#!/usr/bin/env bash
# Create Kafka topics for VroumCPT (dev/plaintext mode).
# Idempotent — safe to run multiple times.
#
# Usage: ./scripts/create_topics.sh [broker]
# Default broker: localhost:9092

set -euo pipefail

BROKER="${1:-localhost:9092}"

echo "Creating topics on ${BROKER}..."

docker compose exec kafka \
    kafka-topics.sh --bootstrap-server "$BROKER" \
    --create --if-not-exists \
    --topic vehicle-states \
    --partitions 3 \
    --replication-factor 1

docker compose exec kafka \
    kafka-topics.sh --bootstrap-server "$BROKER" \
    --create --if-not-exists \
    --topic alerts \
    --partitions 1 \
    --replication-factor 1

echo "Topics:"
docker compose exec kafka \
    kafka-topics.sh --bootstrap-server "$BROKER" --list
