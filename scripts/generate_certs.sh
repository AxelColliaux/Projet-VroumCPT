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

SSL_DIR="$(cd "$(dirname "$0")/.." && pwd)/config/ssl"
CERT_PASSWORD="vroumcpt-cert-password"
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
    -keyalg RSA \
    -keysize 4096 \
    -validity "$VALIDITY_DAYS" \
    -storepass "$CERT_PASSWORD" \
    -keypass "$CERT_PASSWORD"

keytool -keystore kafka.keystore.jks \
    -alias kafka-broker \
    -certreq \
    -file kafka.csr \
    -storepass "$CERT_PASSWORD"

echo "==> Signing broker certificate with CA..."
openssl x509 -req \
    -CA ca.crt -CAkey ca.key \
    -in kafka.csr \
    -out kafka-signed.crt \
    -days "$VALIDITY_DAYS" \
    -CAcreateserial

echo "==> Importing CA and signed cert into keystore..."
keytool -keystore kafka.keystore.jks -alias CARoot -import -file ca.crt \
    -storepass "$CERT_PASSWORD" -noprompt
keytool -keystore kafka.keystore.jks -alias kafka-broker -import -file kafka-signed.crt \
    -storepass "$CERT_PASSWORD" -noprompt

echo "==> Creating truststore..."
keytool -keystore kafka.truststore.jks -alias CARoot -import -file ca.crt \
    -storepass "$CERT_PASSWORD" -noprompt

echo "==> Writing client properties files..."

cat > client-admin.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="admin" password="admin-secret";
ssl.truststore.location=${SSL_DIR}/kafka.truststore.jks
ssl.truststore.password=${CERT_PASSWORD}
ssl.endpoint.identification.algorithm=
EOF

cat > client-generator.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="generator" password="generator-secret";
ssl.truststore.location=${SSL_DIR}/kafka.truststore.jks
ssl.truststore.password=${CERT_PASSWORD}
ssl.endpoint.identification.algorithm=
EOF

cat > client-spark.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="spark" password="spark-secret";
ssl.truststore.location=${SSL_DIR}/kafka.truststore.jks
ssl.truststore.password=${CERT_PASSWORD}
ssl.endpoint.identification.algorithm=
EOF

# Cleanup intermediate files
rm -f kafka.csr kafka-signed.crt ca.key ca.srl

echo ""
echo "Done. Certificates generated in: ${SSL_DIR}"
echo "  kafka.keystore.jks    -> Kafka broker"
echo "  kafka.truststore.jks  -> Clients"
echo "  client-admin.properties, client-generator.properties, client-spark.properties"
