#!/bin/sh
set -eu

SECRETS_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../secrets" && pwd)"
KEYSTORE_PATH="$SECRETS_DIR/kafka.keystore.jks"
TRUSTSTORE_PATH="$SECRETS_DIR/kafka.truststore.jks"
GENERATOR_IMAGE="${SIMULPIX_TLS_IMAGE:-eclipse-temurin:17-jre}"
SECRETS_DIR="${SIMULPIX_SECRETS_DIR:-$SECRETS_DIR}"

if [ -f "$KEYSTORE_PATH" ] && [ -f "$TRUSTSTORE_PATH" ]; then
  echo "[simulpix] kafka TLS secrets already present"
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "[simulpix] docker is required to generate kafka TLS secrets" >&2
  exit 1
fi

mkdir -p "$SECRETS_DIR"

docker run --rm \
  -v "$SECRETS_DIR:/work" \
  -w /work \
  "$GENERATOR_IMAGE" \
  sh -lc '
    set -eu
    keytool_bin="$(command -v keytool)"
    if [ -z "$keytool_bin" ]; then
      echo "[simulpix] keytool not found in container image" >&2
      exit 1
    fi

    KEYSTORE_PASSWORD="simulpix-keystore"
    KEY_PASSWORD="simulpix-key"
    TRUSTSTORE_PASSWORD="simulpix-truststore"

    printf "%s\n" "$KEYSTORE_PASSWORD" > keystore_creds
    printf "%s\n" "$KEY_PASSWORD" > key_creds
    printf "%s\n" "$TRUSTSTORE_PASSWORD" > truststore_creds

    "$keytool_bin" -genkeypair \
      -alias kafka \
      -keystore kafka.keystore.jks \
      -storepass "$KEYSTORE_PASSWORD" \
      -keypass "$KEY_PASSWORD" \
      -dname "CN=localhost" \
      -ext "SAN=DNS:localhost,DNS:kafka,IP:127.0.0.1" \
      -keyalg RSA \
      -storetype JKS \
      -validity 3650

    "$keytool_bin" -exportcert \
      -alias kafka \
      -keystore kafka.keystore.jks \
      -storepass "$KEYSTORE_PASSWORD" \
      -rfc \
      -file kafka.server.cert.pem

    cp kafka.server.cert.pem ca-cert.pem
    touch ca-key.pem

    "$keytool_bin" -importcert \
      -alias CARoot \
      -keystore kafka.truststore.jks \
      -storepass "$TRUSTSTORE_PASSWORD" \
      -noprompt \
      -file kafka.server.cert.pem

    "$keytool_bin" -importkeystore \
      -srckeystore kafka.keystore.jks \
      -srcstorepass "$KEYSTORE_PASSWORD" \
      -destkeystore kafka.server.keystore.p12 \
      -deststoretype PKCS12 \
      -deststorepass "$KEYSTORE_PASSWORD" \
      -srcalias kafka \
      -destalias kafka \
      -srckeypass "$KEY_PASSWORD" \
      -destkeypass "$KEY_PASSWORD"

    touch kafka.server.csr
  '

echo "[simulpix] kafka TLS secrets generated in $SECRETS_DIR"
