#!/bin/sh
set -eu

mkdir -p runtime

ACTIVE="${1:-0}"
PROFILE="${2:-none}"
TARGET_SERVICE="${3:-none}"
INTERFACE_NAME="${4:-eth0}"
DELAY_MS="${5:-0}"
JITTER_MS="${6:-0}"
LOSS_PERCENT="${7:-0}"
RATE_KBIT="${8:-0}"
DESCRIPTION="${9:-none}"
APPLIED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

cat > runtime/current-network.json <<EOF
{
  "active": ${ACTIVE},
  "profile": "${PROFILE}",
  "target_service": "${TARGET_SERVICE}",
  "interface": "${INTERFACE_NAME}",
  "delay_ms": ${DELAY_MS},
  "jitter_ms": ${JITTER_MS},
  "loss_percent": ${LOSS_PERCENT},
  "rate_kbit": ${RATE_KBIT},
  "description": "${DESCRIPTION}",
  "applied_at": "${APPLIED_AT}"
}
EOF
