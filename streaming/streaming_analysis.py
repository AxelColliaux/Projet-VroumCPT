#!/usr/bin/env python3
"""
VroumCPT — Spark Structured Streaming analysis job.

Reads vehicle state reports from a socket (minimal mode) or Kafka topic
(vehicle-states), computes three error-rate metrics per tumbling window,
and emits alerts to a file (minimal) or Kafka topic (alerts) when thresholds
are exceeded.

Run (socket mode):
    spark-submit streaming_analysis.py --source socket

Run (Kafka mode):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        streaming_analysis.py --source kafka --kafka-broker kafka:9092

For the socket source, pipe generator output via netcat:
    python generator/generator.py --no-kafka --rate 5 | nc -lk 9999
"""

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, FloatType, StringType, StructField, StructType, TimestampType,
)


VEHICLE_STATE_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("make", StringType(), True),
    StructField("speed", FloatType(), True),
    StructField("temp", FloatType(), True),
    StructField("errors", ArrayType(StringType()), True),
])

DEFAULT_THRESHOLDS = {
    "global": 0.15,
    "per_make": 0.25,
    "per_vehicle": 0.50,
}

ALERT_SCHEMA = StructType([
    StructField("alert_type", StringType()),
    StructField("entity", StringType()),
    StructField("metric_value", FloatType()),
    StructField("threshold", FloatType()),
    StructField("window_start", StringType()),
    StructField("window_end", StringType()),
    StructField("generated_at", StringType()),
])


def load_thresholds(path: Optional[str]) -> dict:
    if path and os.path.exists(path):
        with open(path) as f:
            data = json.load(f)
        return {
            "global": float(data.get("global", DEFAULT_THRESHOLDS["global"])),
            "per_make": float(data.get("per_make", DEFAULT_THRESHOLDS["per_make"])),
            "per_vehicle": float(data.get("per_vehicle", DEFAULT_THRESHOLDS["per_vehicle"])),
        }
    return DEFAULT_THRESHOLDS.copy()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="VroumCPT Spark Streaming analysis",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--source", choices=["socket", "kafka"], default="socket",
                   help="Input source type")
    p.add_argument("--socket-host", default="localhost",
                   help="Host for socket source")
    p.add_argument("--socket-port", type=int, default=9999,
                   help="Port for socket source")
    p.add_argument("--kafka-broker", default="kafka:9092",
                   help="Kafka broker address")
    p.add_argument("--input-topic", default="vehicle-states",
                   help="Kafka input topic")
    p.add_argument("--output-topic", default="alerts",
                   help="Kafka output topic (kafka sink only)")
    p.add_argument("--group-id", default="spark-streaming",
                   help="Kafka consumer group id (must match ACLs in secure mode)")
    p.add_argument("--sink", choices=["file", "kafka", "console"], default="file",
                   help="Output sink type")
    p.add_argument("--output-dir", default="output/alerts",
                   help="Directory for file sink output")
    p.add_argument("--thresholds", default=None,
                   help="Path to thresholds.json (uses defaults if absent)")
    p.add_argument("--window-duration", default="30 seconds",
                   help="Tumbling window duration (e.g. '30 seconds', '1 minute')")
    p.add_argument("--trigger-interval", default="10 seconds",
                   help="Streaming trigger interval")
    p.add_argument("--watermark", default="1 minute",
                   help="Watermark for late data handling")
    # Kafka security (passed as Kafka options)
    p.add_argument("--kafka-security-protocol", default="PLAINTEXT")
    p.add_argument("--kafka-sasl-mechanism", default="SCRAM-SHA-256")
    p.add_argument("--kafka-sasl-jaas-config", default=None,
                   help='Full JAAS config string for SASL authentication')
    p.add_argument("--ssl-truststore-location", default=None)
    p.add_argument("--ssl-truststore-password", default=None)
    return p.parse_args()


def build_kafka_options(args: argparse.Namespace, is_sink: bool = False) -> dict:
    opts = {
        "kafka.bootstrap.servers": args.kafka_broker,
        "kafka.security.protocol": args.kafka_security_protocol,
    }
    if "SASL" in args.kafka_security_protocol and args.kafka_sasl_jaas_config:
        opts["kafka.sasl.mechanism"] = args.kafka_sasl_mechanism
        opts["kafka.sasl.jaas.config"] = args.kafka_sasl_jaas_config
    if "SSL" in args.kafka_security_protocol:
        if args.ssl_truststore_location:
            opts["kafka.ssl.truststore.location"] = args.ssl_truststore_location
        if args.ssl_truststore_password:
            opts["kafka.ssl.truststore.password"] = args.ssl_truststore_password
    if not is_sink:
        opts["subscribe"] = args.input_topic
        opts["startingOffsets"] = "latest"
        opts["failOnDataLoss"] = "false"
        opts["kafka.group.id"] = args.group_id
    else:
        opts["topic"] = args.output_topic
    return opts


def read_stream(spark: SparkSession, args: argparse.Namespace):
    if args.source == "socket":
        raw = (
            spark.readStream
            .format("socket")
            .option("host", args.socket_host)
            .option("port", args.socket_port)
            .load()
        )
        return raw.select(
            F.from_json(F.col("value"), VEHICLE_STATE_SCHEMA).alias("data")
        ).select("data.*")

    kafka_opts = build_kafka_options(args)
    raw = spark.readStream.format("kafka")
    for k, v in kafka_opts.items():
        raw = raw.option(k, v)
    raw = raw.load()

    return raw.select(
        F.from_json(F.col("value").cast("string"), VEHICLE_STATE_SCHEMA).alias("data")
    ).select("data.*")


def compute_metrics(df, window_duration: str, watermark: str):
    """Return (global_df, per_make_df, per_vehicle_df) with windowed error rates."""
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp"))
    ).withColumn(
        "has_error",
        F.when(F.size(F.col("errors")) > 0, 1).otherwise(0)
    )

    df_wm = df.withWatermark("event_time", watermark)

    window = F.window("event_time", window_duration)

    global_df = (
        df_wm.groupBy(window)
        .agg(
            F.mean("has_error").alias("error_rate"),
            F.count("*").alias("record_count"),
        )
        .withColumn("entity", F.lit("global"))
        .withColumn("alert_type", F.lit("global"))
    )

    per_make_df = (
        df_wm.groupBy(window, F.col("make").alias("entity"))
        .agg(
            F.mean("has_error").alias("error_rate"),
            F.count("*").alias("record_count"),
        )
        .withColumn("alert_type", F.lit("make"))
    )

    per_vehicle_df = (
        df_wm.groupBy(window, F.col("vehicle_id").alias("entity"))
        .agg(
            F.mean("has_error").alias("error_rate"),
            F.count("*").alias("record_count"),
        )
        .withColumn("alert_type", F.lit("vehicle"))
    )

    return global_df, per_make_df, per_vehicle_df


def apply_threshold(df, threshold: float):
    return df.filter(F.col("error_rate") > threshold)


def format_alerts(df, threshold: float):
    """Convert a metrics DataFrame into alert JSON strings."""
    return df.select(
        F.to_json(F.struct(
            F.col("alert_type"),
            F.col("entity"),
            F.col("error_rate").alias("metric_value"),
            F.lit(threshold).cast(FloatType()).alias("threshold"),
            F.col("window.start").cast(StringType()).alias("window_start"),
            F.col("window.end").cast(StringType()).alias("window_end"),
            F.lit(datetime.now(timezone.utc).isoformat()).alias("generated_at"),
        )).alias("value")
    )


def write_stream(df, args: argparse.Namespace, query_name: str):
    if args.sink == "file":
        return (
            df.writeStream
            .outputMode("append")
            .format("json")
            .option("path", os.path.join(args.output_dir, query_name))
            .option("checkpointLocation", f"/tmp/vroumcpt-checkpoints/{query_name}")
            .trigger(processingTime=args.trigger_interval)
            .queryName(query_name)
            .start()
        )
    if args.sink == "kafka":
        kafka_opts = build_kafka_options(args, is_sink=True)
        writer = df.writeStream.outputMode("append").format("kafka")
        for k, v in kafka_opts.items():
            writer = writer.option(k, v)
        return (
            writer
            .option("checkpointLocation", f"/tmp/vroumcpt-checkpoints/{query_name}")
            .trigger(processingTime=args.trigger_interval)
            .queryName(query_name)
            .start()
        )
    # console
    return (
        df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime=args.trigger_interval)
        .queryName(query_name)
        .start()
    )


def main():
    args = parse_args()
    thresholds = load_thresholds(args.thresholds)

    spark = (
        SparkSession.builder
        .appName("VroumCPT-Streaming")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = read_stream(spark, args)
    global_df, per_make_df, per_vehicle_df = compute_metrics(
        events, args.window_duration, args.watermark
    )

    # Filter by thresholds and format as alert JSON
    global_alerts = format_alerts(
        apply_threshold(global_df, thresholds["global"]), thresholds["global"]
    )
    make_alerts = format_alerts(
        apply_threshold(per_make_df, thresholds["per_make"]), thresholds["per_make"]
    )
    vehicle_alerts = format_alerts(
        apply_threshold(per_vehicle_df, thresholds["per_vehicle"]), thresholds["per_vehicle"]
    )

    q1 = write_stream(global_alerts, args, "global-alerts")
    q2 = write_stream(make_alerts, args, "make-alerts")
    q3 = write_stream(vehicle_alerts, args, "vehicle-alerts")

    print(f"Streaming started. Source={args.source}, Sink={args.sink}", flush=True)
    print(f"Thresholds — global={thresholds['global']}, "
          f"per_make={thresholds['per_make']}, "
          f"per_vehicle={thresholds['per_vehicle']}", flush=True)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping streaming queries...")
        q1.stop()
        q2.stop()
        q3.stop()


if __name__ == "__main__":
    main()
