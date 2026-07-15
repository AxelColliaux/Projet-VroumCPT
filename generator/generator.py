#!/usr/bin/env python3
"""
VroumCPT — Vehicle state report generator.

Generates realistic vehicle telemetry events and either prints them to stdout
or publishes them to a Kafka topic (vehicle-states).
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone


SENSOR_NAMES = ["speed", "temp", "gps", "lidar", "camera"]

CAR_MAKES = [
    "DeLorean", "Tesla", "Renault", "Peugeot", "Citroën",
    "BMW", "Volkswagen", "Ford", "Toyota", "Honda",
    "Audi", "Mercedes", "Volvo", "Nissan", "Hyundai",
]


def build_vehicle_pool(vehicle_count: int, make_count: int,
                       error_vehicle_pct: float, error_make_pct: float) -> list[dict]:
    makes = random.sample(CAR_MAKES, min(make_count, len(CAR_MAKES)))
    high_error_makes = set(random.sample(makes, max(1, int(len(makes) * error_make_pct))))

    vehicles = []
    for i in range(vehicle_count):
        vid = f"V{random.randint(10000, 99999)}"
        make = random.choice(makes)
        is_error_prone = (random.random() < error_vehicle_pct) or (make in high_error_makes)
        vehicles.append({"vehicle_id": vid, "make": make, "error_prone": is_error_prone})

    return vehicles


def generate_record(vehicle: dict, base_error_rate: float, high_error_rate: float) -> dict:
    error_rate = high_error_rate if vehicle["error_prone"] else base_error_rate
    error_count = sum(1 for _ in SENSOR_NAMES if random.random() < error_rate)
    errors = random.sample(SENSOR_NAMES, error_count) if error_count else []

    return {
        "vehicle_id": vehicle["vehicle_id"],
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(),
        "make": vehicle["make"],
        "speed": round(random.uniform(0, 180), 1),
        "temp": round(random.uniform(60, 120), 1),
        "errors": errors,
    }


def make_kafka_producer(broker: str, topic: str, security_config: dict | None = None):
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("kafka-python not installed. Run: pip install kafka-python", file=sys.stderr)
        sys.exit(1)

    config = {
        "bootstrap_servers": broker,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "acks": "all",
    }
    if security_config:
        config.update(security_config)

    return KafkaProducer(**config), topic


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="VroumCPT vehicle state report generator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--rate", type=float, default=1.0,
                   help="Records generated per second")
    p.add_argument("--vehicle-count", type=int, default=50,
                   help="Number of distinct vehicle IDs in the pool")
    p.add_argument("--make-count", type=int, default=5,
                   help="Number of distinct car makes")
    p.add_argument("--error-vehicle-pct", type=float, default=0.2,
                   help="Fraction of vehicles that are error-prone (0-1)")
    p.add_argument("--error-make-pct", type=float, default=0.3,
                   help="Fraction of makes where all vehicles are error-prone (0-1)")
    p.add_argument("--base-error-rate", type=float, default=0.05,
                   help="Per-sensor error probability for normal vehicles (0-1)")
    p.add_argument("--high-error-rate", type=float, default=0.5,
                   help="Per-sensor error probability for error-prone vehicles (0-1)")
    p.add_argument("--kafka-broker", type=str, default="localhost:9092",
                   help="Kafka broker address (host:port)")
    p.add_argument("--topic", type=str, default="vehicle-states",
                   help="Kafka topic to publish to")
    p.add_argument("--no-kafka", action="store_true",
                   help="Print records to stdout instead of sending to Kafka")
    p.add_argument("--duration", type=float, default=0,
                   help="Run for N seconds then exit (0 = run forever)")
    p.add_argument("--seed", type=int, default=None,
                   help="Random seed for reproducibility")
    # Security options
    p.add_argument("--security-protocol", type=str, default=None,
                   choices=["PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL"],
                   help="Kafka security protocol")
    p.add_argument("--sasl-mechanism", type=str, default="SCRAM-SHA-256",
                   help="SASL mechanism (when using SASL_* protocol)")
    p.add_argument("--sasl-username", type=str, default=None)
    p.add_argument("--sasl-password", type=str, default=None)
    p.add_argument("--ssl-cafile", type=str, default=None,
                   help="Path to CA certificate file")
    return p.parse_args()


def build_security_config(args: argparse.Namespace) -> dict | None:
    if not args.security_protocol or args.security_protocol == "PLAINTEXT":
        return None
    cfg: dict = {"security_protocol": args.security_protocol}
    if "SASL" in args.security_protocol:
        cfg["sasl_mechanism"] = args.sasl_mechanism
        cfg["sasl_plain_username"] = args.sasl_username
        cfg["sasl_plain_password"] = args.sasl_password
    if "SSL" in args.security_protocol and args.ssl_cafile:
        cfg["ssl_cafile"] = args.ssl_cafile
    return cfg


def main():
    args = parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    vehicles = build_vehicle_pool(
        args.vehicle_count, args.make_count,
        args.error_vehicle_pct, args.error_make_pct,
    )

    producer = None
    if not args.no_kafka:
        security = build_security_config(args)
        producer, topic = make_kafka_producer(args.kafka_broker, args.topic, security)
        print(f"Connected to Kafka at {args.kafka_broker}, publishing to '{topic}'",
              file=sys.stderr)
    else:
        print("Running in no-Kafka mode (stdout)", file=sys.stderr)

    interval = 1.0 / args.rate if args.rate > 0 else 1.0
    start = time.monotonic()
    count = 0

    try:
        while True:
            vehicle = random.choice(vehicles)
            record = generate_record(vehicle, args.base_error_rate, args.high_error_rate)

            if producer:
                producer.send(topic, value=record)
            else:
                print(json.dumps(record))
                sys.stdout.flush()

            count += 1
            elapsed = time.monotonic() - start
            if args.duration > 0 and elapsed >= args.duration:
                break

            next_tick = start + count * interval
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        pass
    finally:
        if producer:
            producer.flush()
            producer.close()
        print(f"\nGenerated {count} records.", file=sys.stderr)


if __name__ == "__main__":
    main()
