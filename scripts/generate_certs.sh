#!/usr/bin/env bash
# Generates self-signed TLS certificates and Java keystores for Kafka.
# Run once before starting docker-compose.secure.yml.
#
# Requirements: openssl, keytool (JDK)
#
# Output (in config/ssl/):
#   ca.crt                  Self-signed CA certificate
#   kafka.keystore.jks      Kafka broker keystore (cert + private key)
#   kafka.truststore.jks    Truststore containing the CA (used by clients)
#   client-admin.properties Client config for admin operations

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Load local secrets when the script is run directly. Docker Compose also reads
# this file automatically. The file is ignored by Git.
if [[ -f "${PROJECT_ROOT}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_ROOT}/.env"
    set +a
fi

CERT_PASSWORD="${KAFKA_CERTIFICATE_PASSWORD:?Set KAFKA_CERTIFICATE_PASSWORD in .env}"
ADMIN_PASSWORD="${KAFKA_ADMIN_PASSWORD:?Set KAFKA_ADMIN_PASSWORD in .env}"
GENERATOR_PASSWORD="${KAFKA_GENERATOR_PASSWORD:?Set KAFKA_GENERATOR_PASSWORD in .env}"
SPARK_PASSWORD="${KAFKA_SPARK_PASSWORD:?Set KAFKA_SPARK_PASSWORD in .env}"
SSL_DIR="${PROJECT_ROOT}/config/ssl"
VALIDITY_DAYS=3650
CN="kafka"

mkdir -p "$SSL_DIR"
cd "$SSL_DIR"

echo "==> Generating CA key and certificate..."
openssl req -new -newkey rsa:4096 -days "$VALIDITY_DAYS" -x509 -subj "/CN=VroumCPT-CA" \
    -keyout ca.key -out ca.crt -nodes

echo "==> Generating Kafka broker key and CSR..."
keytool -genkey -noprompt \
    -alias kafka-broker \
    -dname "CN=${CN}, O=VroumCPT, L=Paris, C=FR" \
    -keystore kafka.keystore.jks \
    -storetype JKS \
    -keyalg RSA \
    -keysize 4096 \
    -validity "$VALIDITY_DAYS" \
    -storepass "$CERT_PASSWORD" \
    -keypass "$CERT_PASSWORD" \
    -ext "SAN=DNS:kafka,DNS:localhost"

keytool -keystore kafka.keystore.jks \
    -storetype JKS \
    -alias kafka-broker \
    -certreq \
    -file kafka.csr \
    -storepass "$CERT_PASSWORD" \
    -ext "SAN=DNS:kafka,DNS:localhost"

cat > kafka-cert.ext <<EOF
subjectAltName=DNS:kafka,DNS:localhost
extendedKeyUsage=serverAuth
keyUsage=digitalSignature,keyEncipherment
EOF

echo "==> Signing broker certificate with CA..."
openssl x509 -req \
    -CA ca.crt -CAkey ca.key \
    -in kafka.csr \
    -out kafka-signed.crt \
    -days "$VALIDITY_DAYS" \
    -CAcreateserial \
    -extfile kafka-cert.ext

echo "==> Importing CA and signed cert into keystore..."
keytool -keystore kafka.keystore.jks -storetype JKS -alias CARoot -import -file ca.crt \
    -storepass "$CERT_PASSWORD" -noprompt
keytool -keystore kafka.keystore.jks -storetype JKS -alias kafka-broker -import -file kafka-signed.crt \
    -storepass "$CERT_PASSWORD" -noprompt

echo "==> Creating truststore..."
keytool -keystore kafka.truststore.jks -storetype JKS -alias CARoot -import -file ca.crt \
    -storepass "$CERT_PASSWORD" -noprompt

echo "==> Writing client properties files..."

cat > client-admin.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="admin" password="${ADMIN_PASSWORD}";
ssl.truststore.location=/opt/ssl/kafka.truststore.jks
ssl.truststore.password=${CERT_PASSWORD}
ssl.endpoint.identification.algorithm=https
EOF

cat > client-generator.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="generator" password="${GENERATOR_PASSWORD}";
ssl.truststore.location=/opt/ssl/kafka.truststore.jks
ssl.truststore.password=${CERT_PASSWORD}
ssl.endpoint.identification.algorithm=https
EOF

cat > client-spark.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="spark" password="${SPARK_PASSWORD}";
ssl.truststore.location=/opt/ssl/kafka.truststore.jks
ssl.truststore.password=${CERT_PASSWORD}
ssl.endpoint.identification.algorithm=https
EOF

# Client property files contain credentials; keep them readable only by the
# local user. The CA certificate is public and can remain world-readable.
chmod 600 kafka.keystore.jks kafka.truststore.jks \
    client-admin.properties client-generator.properties client-spark.properties
chmod 644 ca.crt

# Cleanup intermediate files
rm -f kafka.csr kafka-signed.crt kafka-cert.ext ca.key ca.srl

echo ""
echo "Done. Certificates generated in: ${SSL_DIR}"
echo "  kafka.keystore.jks    -> Kafka broker"
echo "  kafka.truststore.jks  -> Clients"
echo "  client-admin.properties, client-generator.properties, client-spark.properties"
