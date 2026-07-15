# Generator — Documentation

`generator/generator.py` simulates the on-board telemetry system of VroumCPT vehicles.
It produces vehicle state reports and either prints them to stdout or publishes them
to a Kafka topic.

## Output Format

Each record is a JSON object on a single line:

```json
{
  "vehicle_id": "V31337",
  "timestamp": "2026-06-17T11:29:06+02:00",
  "make": "DeLorean",
  "speed": 80.0,
  "temp": 90.5,
  "errors": ["speed", "temp"]
}
```

| Field        | Type       | Description                                       |
|--------------|------------|---------------------------------------------------|
| `vehicle_id` | string     | Unique vehicle identifier (format: `V#####`)      |
| `timestamp`  | ISO-8601   | Time of the reading (local timezone)              |
| `make`       | string     | Vehicle manufacturer                              |
| `speed`      | float      | Speed in km/h (0–180)                             |
| `temp`       | float      | Engine temperature in °C (60–120)                 |
| `errors`     | string[]   | List of faulty sensor names (may be empty)        |

Possible sensor names: `speed`, `temp`, `gps`, `lidar`, `camera`.

## Usage

```
python generator/generator.py [OPTIONS]
```

### Options

| Option                 | Default          | Description                                               |
|------------------------|------------------|-----------------------------------------------------------|
| `--rate FLOAT`         | `1.0`            | Records generated per second                              |
| `--vehicle-count INT`  | `50`             | Number of distinct vehicle IDs in the pool                |
| `--make-count INT`     | `5`              | Number of distinct car makes (sampled from built-in list) |
| `--error-vehicle-pct FLOAT` | `0.2`      | Fraction of vehicles that are error-prone (0–1)           |
| `--error-make-pct FLOAT`    | `0.3`      | Fraction of makes whose vehicles are all error-prone      |
| `--base-error-rate FLOAT`   | `0.05`     | Per-sensor error probability for normal vehicles          |
| `--high-error-rate FLOAT`   | `0.5`      | Per-sensor error probability for error-prone vehicles     |
| `--kafka-broker STR`   | `localhost:9092` | Kafka broker (ignored with `--no-kafka`)                  |
| `--topic STR`          | `vehicle-states` | Kafka topic to publish to                                 |
| `--no-kafka`           | (flag)           | Print to stdout instead of sending to Kafka               |
| `--duration FLOAT`     | `0`              | Stop after N seconds (0 = run forever)                    |
| `--seed INT`           | `None`           | Fix random seed for reproducible output                   |

### Security options (Kafka only)

| Option                          | Description                          |
|---------------------------------|--------------------------------------|
| `--security-protocol`           | `PLAINTEXT` / `SASL_PLAINTEXT` / `SASL_SSL` / `SSL` |
| `--sasl-mechanism`              | `SCRAM-SHA-256` (default)            |
| `--sasl-username`               | SASL username                        |
| `--sasl-password`               | SASL password                        |
| `--ssl-cafile`                  | Path to CA certificate               |

## Examples

### Minimal — print to stdout
```bash
python generator/generator.py --no-kafka
```

### High rate, many vehicles
```bash
python generator/generator.py --no-kafka --rate 100 --vehicle-count 200 --make-count 10
```

### Simulate a bad batch of sensors (many errors)
```bash
python generator/generator.py \
    --no-kafka \
    --error-vehicle-pct 0.8 \
    --high-error-rate 0.9 \
    --rate 5
```

### Publish to Kafka (dev)
```bash
python generator/generator.py \
    --kafka-broker localhost:9092 \
    --rate 10
```

### Publish to Kafka (secure)
```bash
python generator/generator.py \
    --kafka-broker kafka:9092 \
    --security-protocol SASL_SSL \
    --sasl-username generator \
    --sasl-password generator-secret \
    --ssl-cafile config/ssl/ca.crt \
    --rate 10
```

## Error Modelling

At startup, the generator pre-allocates a fixed pool of vehicles and makes.
A configurable fraction of vehicles and makes are marked as **error-prone**.

- A vehicle is error-prone if it was sampled into the error-prone subset, **or** if its make is error-prone.
- For each record, the probability of a sensor being listed in `errors` is `high_error_rate` for error-prone vehicles and `base_error_rate` for normal vehicles.
- Each sensor is evaluated independently, so a single record can have zero to five errors.
