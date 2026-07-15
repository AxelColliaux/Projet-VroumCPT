# Security — Documentation

VroumCPT uses a layered security model: **authentication** (who can connect) and
**encryption** (data in transit and at rest). The approach differs between development
and production environments, with explicit justifications below.

## Threat Model

| Threat                         | Impact                              |
|--------------------------------|-------------------------------------|
| Unauthorized Kafka producer    | Inject false sensor data → wrong alerts |
| Unauthorized Kafka consumer    | Leak vehicle telemetry (GDPR)       |
| Man-in-the-middle on Kafka     | Intercept or tamper with data       |
| Unauthorized Spark UI access   | Expose job internals, code          |

## Dev Environment (`docker-compose.yml`)

**No authentication, no TLS.**

Justification: The dev environment runs on a private local network (Docker bridge)
with no external exposure. The added operational complexity of TLS/SASL in dev would
slow down iteration without adding security value in that context.

Mitigations:
- Kafka is reachable only on the Docker bridge network (`kafka:9092`); the host
  port mappings (`9092`, `29092`) are intended for local development on the
  developer's own machine. Do **not** expose this compose file's ports on a
  shared or public host — use `docker-compose.secure.yml` for any networked deployment.
- No secrets are stored in the codebase; the dev setup uses no credentials at all.

## Production Environment (`docker-compose.secure.yml`)

### Authentication: SASL/SCRAM-SHA-256

Each client (generator, Spark) authenticates with a dedicated username and password
stored in Kafka's internal credential store (not in config files or environment variables
visible in `docker inspect`).

| User        | Permissions                              |
|-------------|------------------------------------------|
| `admin`     | Full access (topic management, ACL mgmt) |
| `generator` | Write to `vehicle-states`               |
| `spark`     | Read from `vehicle-states`, write to `alerts` |

ACLs are enforced by Kafka's `AclAuthorizer`. The principle of least privilege is
applied: the generator cannot read; Spark cannot modify topic configuration.

SCRAM-SHA-256 was chosen over PLAIN because:
- Credentials are not sent in cleartext (even without TLS).
- The server stores a salted, iterated hash (not the password itself).

### Encryption in Transit: TLS (SSL)

All Kafka traffic is encrypted with TLS 1.2+. Self-signed certificates are used in
the provided setup (`scripts/generate_certs.sh`). In production, replace with
certificates signed by your organization's CA or a public CA (Let's Encrypt, etc.).

The truststore (`kafka.truststore.jks`) is distributed to clients so they can verify
the broker's identity and prevent man-in-the-middle attacks.

`ssl.endpoint.identification.algorithm` is set to empty in the dev client properties
to allow hostname `kafka` (Docker internal DNS). In production, set it to `https`
and ensure the certificate CN matches the broker's FQDN.

### Spark Security

**Kafka access (implemented).** The `spark-streaming` service in
`docker-compose.secure.yml` connects to Kafka over **SASL_SSL** using the
dedicated `spark` SCRAM user and the shared truststore. This is the security
boundary that matters for this project: all telemetry and alert traffic between
Spark and Kafka is authenticated and encrypted in transit. The consumer uses the
`spark-streaming` group, which matches the ACL granted in `kafka-init`.

**Spark RPC encryption (not applicable here, enable for a cluster).** The
provided setup runs Spark in **local mode** (a single driver process, no separate
executors), so there is no inter-node RPC traffic to encrypt. When deploying to a
real Spark cluster (standalone/YARN/k8s), enable RPC authentication and encryption
by adding these to the `SparkSession` config or `spark-submit --conf`:

```
spark.authenticate=true
spark.authenticate.secret=<shared-secret>
spark.network.crypto.enabled=true
spark.ssl.enabled=true
```

The Spark Web UI should then be placed behind an authenticating reverse proxy
(nginx + OAuth2 Proxy, or Spark's built-in ACLs).

### Encryption at Rest

Kafka stores data on disk. To encrypt data at rest:
- Use an encrypted filesystem (LUKS on Linux, FileVault on macOS) on the host volume.
- Alternatively, use a managed Kafka service (Confluent Cloud, AWS MSK) that provides
  transparent encryption at rest.

In the current Docker setup, the `kafka_secure_data` volume is not encrypted at rest.
This is acceptable for a development/demo environment but must be addressed before
production deployment with real vehicle data.

## Setting Up the Secure Environment

```bash
# 1. Generate TLS certificates and keystores
./scripts/generate_certs.sh

# 2. Start secure Kafka
docker compose -f docker-compose.secure.yml up -d

# 3. Verify: unauthenticated access must be rejected
docker compose -f docker-compose.secure.yml exec kafka \
    kafka-topics.sh --bootstrap-server localhost:9092 --list
# Expected: authentication error

# 4. Authenticated access succeeds
docker compose -f docker-compose.secure.yml exec kafka \
    kafka-topics.sh --bootstrap-server localhost:9092 \
    --command-config /bitnami/kafka/config/ssl/client-admin.properties \
    --list
```

## Rotating Credentials

To rotate a user's password:
```bash
docker compose exec kafka kafka-configs.sh \
    --bootstrap-server localhost:9092 \
    --command-config config/ssl/client-admin.properties \
    --alter \
    --entity-type users --entity-name generator \
    --add-config 'SCRAM-SHA-256=[password=new-secret]'
```

Update the corresponding `client-generator.properties` and restart the generator.
No Kafka restart required.
