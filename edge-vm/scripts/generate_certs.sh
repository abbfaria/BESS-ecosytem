#!/usr/bin/env bash
# =============================================================================
# BESS PKI Certificate Generator
# Generates a self-signed CA and device/server certificates for mTLS.
#
# Usage:
#   ./generate_certs.sh [device_id]
#   ./generate_certs.sh bess-edge-01
#
# Outputs:
#   certs/ca.crt         — CA certificate (distribute to ALL components)
#   certs/ca.key         — CA private key (keep secure, not in containers)
#   certs/server.crt     — Cloud MQTT broker certificate
#   certs/server.key     — Cloud MQTT broker private key
#   certs/edge.crt       — Edge device client certificate (CN = device_id)
#   certs/edge.key       — Edge device client private key
# =============================================================================

set -euo pipefail

DEVICE_ID="${1:-bess-edge-01}"
DAYS_CA=3650        # 10 years for CA
DAYS_CERT=825       # ~27 months for leaf certs (Apple/browser policy max)
COUNTRY="UA"
ORG="BESS Project"
CLOUD_CN="${CLOUD_CN:-cloud.bess.local}"

OUT_DIR="$(dirname "$0")/../../certs"
mkdir -p "$OUT_DIR"
pushd "$OUT_DIR" > /dev/null

echo "=== BESS PKI Generator ==="
echo "  CA validity:   ${DAYS_CA} days"
echo "  Leaf validity: ${DAYS_CERT} days"
echo "  Device CN:     ${DEVICE_ID}"
echo "  Server CN:     ${CLOUD_CN}"
echo "  Output dir:    ${OUT_DIR}"
echo ""

# ── 1. Root CA ─────────────────────────────────────────────────────────────────
if [[ ! -f ca.key ]]; then
    echo "[1/5] Generating CA key and self-signed certificate..."
    openssl genrsa -out ca.key 4096
    openssl req -new -x509 -days "$DAYS_CA" -key ca.key -out ca.crt \
        -subj "/C=${COUNTRY}/O=${ORG}/CN=BESS-CA"
    chmod 600 ca.key
    echo "  CA: ca.crt / ca.key"
else
    echo "[1/5] CA key exists — skipping CA generation (delete ca.key to regenerate)"
fi

# ── 2. Cloud MQTT server certificate ──────────────────────────────────────────
echo "[2/5] Generating cloud MQTT server certificate..."
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
    -subj "/C=${COUNTRY}/O=${ORG}/CN=${CLOUD_CN}"

cat > server_ext.cnf <<EOF
[SAN]
subjectAltName=DNS:${CLOUD_CN},DNS:mosquitto,DNS:localhost,IP:127.0.0.1
EOF

openssl x509 -req -days "$DAYS_CERT" -in server.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server.crt \
    -extfile server_ext.cnf -extensions SAN
rm -f server.csr server_ext.cnf
chmod 600 server.key
echo "  Server cert: server.crt / server.key"

# ── 3. Edge device client certificate ─────────────────────────────────────────
echo "[3/5] Generating edge device certificate (CN=${DEVICE_ID})..."
openssl genrsa -out edge.key 2048
openssl req -new -key edge.key -out edge.csr \
    -subj "/C=${COUNTRY}/O=${ORG}/CN=${DEVICE_ID}"
openssl x509 -req -days "$DAYS_CERT" -in edge.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out edge.crt
rm -f edge.csr
chmod 600 edge.key
echo "  Edge cert: edge.crt / edge.key"

# ── 4. Cloud API service account certificate ──────────────────────────────────
echo "[4/5] Generating cloud-api service account certificate..."
openssl genrsa -out cloud-api.key 2048
openssl req -new -key cloud-api.key -out cloud-api.csr \
    -subj "/C=${COUNTRY}/O=${ORG}/CN=cloud-api"
openssl x509 -req -days "$DAYS_CERT" -in cloud-api.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out cloud-api.crt
rm -f cloud-api.csr
chmod 600 cloud-api.key
echo "  Cloud-API cert: cloud-api.crt / cloud-api.key"

# ── 5. Verify chain ────────────────────────────────────────────────────────────
echo "[5/5] Verifying certificate chain..."
openssl verify -CAfile ca.crt server.crt && echo "  server.crt  OK"
openssl verify -CAfile ca.crt edge.crt   && echo "  edge.crt    OK"
openssl verify -CAfile ca.crt cloud-api.crt && echo "  cloud-api.crt OK"

popd > /dev/null
echo ""
echo "=== Certificates generated in: ${OUT_DIR} ==="
echo ""
echo "Next steps:"
echo "  1. Copy certs/ca.crt + certs/edge.crt + certs/edge.key  → edge VM /certs/"
echo "  2. Copy certs/ca.crt + certs/server.crt + certs/server.key → cloud certs/"
echo "  3. Keep certs/ca.key OFFLINE — do not commit to git"
echo "  4. Add certs/ to .gitignore"
echo ""
