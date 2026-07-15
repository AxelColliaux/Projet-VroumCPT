#!/usr/bin/env python3
"""
VroumCPT — Spark batch threshold calculator.

Reads historical vehicle state reports (JSON files or Kafka topic),
computes 99th-percentile error rates for each metric type, and writes
thresholds to config/thresholds.json so that alerts fire only 1% of the time.

Usage (from JSON files):
    spark-submit batch/threshold_calculator.py --input-dir /path/to/data

Usage (from Kafka, full history):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        batch/threshold_calculator.py --source kafka --kafka-broker kafka:9092
"""

import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType


VEHICLE_STATE_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("make", StringType(), True),
    StructField("speed", FloatType(), True),
    StructField("temp", FloatType(), True),
    StructField("errors", ArrayType(StringType()), True),
])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="VroumCPT batch threshold calculator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--source", choices=["files", "kafka"], default="files",
                   help="Data source")
    p.add_argument("--input-dir", default="output/data",
                   help="Directory with JSON data files (files source)")
    p.add_argument("--kafka-broker", default="kafka:9092")
    p.add_argument("--input-topic", default="vehicle-states")
    p.add_argument("--window-duration", default="30 seconds",
                   help="Window size (must match streaming job)")
    p.add_argument("--percentile", type=float, default=0.99,
                   help="Percentile to use as threshold (default: 0.99 → alerts at 1%%)")
    p.add_argument("--output", default="config/thresholds.json",
                   help="Output file for computed thresholds")
    return p.parse_args()


def load_data(spark: SparkSession, args: argparse.Namespace):
    if args.source == "files":
        return spark.read.schema(VEHICLE_STATE_SCHEMA).json(args.input_dir)

    # Kafka batch read (reads all messages from beginning to current end)
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_broker)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    return raw.select(
        F.from_json(F.col("value").cast("string"), VEHICLE_STATE_SCHEMA).alias("data")
    ).select("data.*")


def compute_windowed_rates(df, window_duration: str):
    """Compute per-window error rates for each metric type."""
    df = df.withColumn(
        "event_time", F.to_timestamp(F.col("timestamp"))
    ).withColumn(
        "has_error", F.when(F.size(F.col("errors")) > 0, 1.0).otherwise(0.0)
    )

    window = F.window("event_time", window_duration)

    global_rates = (
        df.groupBy(window)
        .agg(F.mean("has_error").alias("error_rate"))
        .select("error_rate")
    )

    per_make_rates = (
        df.groupBy(window, "make")
        .agg(F.mean("has_error").alias("error_rate"))
        .select("error_rate")
    )

    per_vehicle_rates = (
        df.groupBy(window, "vehicle_id")
        .agg(F.mean("has_error").alias("error_rate"))
        .select("error_rate")
    )

    return global_rates, per_make_rates, per_vehicle_rates


def percentile_value(df, col: str, p: float) -> float:
    row = df.agg(F.percentile_approx(col, p).alias("v")).collect()
    if row and row[0]["v"] is not None:
        return float(row[0]["v"])
    return 0.0


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("VroumCPT-ThresholdCalculator")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = load_data(spark, args)
    total = df.count()

    if total == 0:
        print("No data found. Cannot compute thresholds.")
        return

    print(f"Loaded {total} records. Computing {args.percentile * 100:.0f}th-percentile thresholds...")

    global_rates, per_make_rates, per_vehicle_rates = compute_windowed_rates(
        df, args.window_duration
    )

    global_threshold = percentile_value(global_rates, "error_rate", args.percentile)
    per_make_threshold = percentile_value(per_make_rates, "error_rate", args.percentile)
    per_vehicle_threshold = percentile_value(per_vehicle_rates, "error_rate", args.percentile)

    thresholds = {
        "global": round(global_threshold, 4),
        "per_make": round(per_make_threshold, 4),
        "per_vehicle": round(per_vehicle_threshold, 4),
        "_meta": {
            "percentile": args.percentile,
            "records_analyzed": total,
            "window_duration": args.window_duration,
        },
    }

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(thresholds, f, indent=2)

    print(f"Thresholds written to {args.output}:")
    print(f"  global:      {thresholds['global']}")
    print(f"  per_make:    {thresholds['per_make']}")
    print(f"  per_vehicle: {thresholds['per_vehicle']}")

    spark.stop()


if __name__ == "__main__":
    main()
