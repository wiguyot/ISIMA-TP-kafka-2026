#!/bin/sh
set -eu

mkdir -p runtime

RUN_ID="${1:-default}"
PERSISTER_VALID_GROUP="${2:-simulpix-persister-valid-v1}"
PERSISTER_REJECTED_GROUP="${3:-simulpix-persister-rejected-v1}"
PIX_VALIDATOR_GROUP="${4:-simulpix-validator-v1}"
PIX_DECISION_ENGINE_GROUP="${5:-simulpix-decision-engine-v1}"
PIX_OUTCOME_PUBLISHER_GROUP="${6:-simulpix-outcome-publisher-v1}"

cat > runtime/current-run.env <<EOF
SIMULPIX_RUN_ID=${RUN_ID}
SIMULPIX_PIX_VALIDATOR_GROUP=${PIX_VALIDATOR_GROUP}
SIMULPIX_PIX_DECISION_ENGINE_GROUP=${PIX_DECISION_ENGINE_GROUP}
SIMULPIX_PIX_OUTCOME_PUBLISHER_GROUP=${PIX_OUTCOME_PUBLISHER_GROUP}
SIMULPIX_PERSISTER_VALID_GROUP=${PERSISTER_VALID_GROUP}
SIMULPIX_PERSISTER_REJECTED_GROUP=${PERSISTER_REJECTED_GROUP}
EOF

cat > runtime/current-run.json <<EOF
{
  "run_id": "${RUN_ID}",
  "pix_validator_group": "${PIX_VALIDATOR_GROUP}",
  "pix_decision_engine_group": "${PIX_DECISION_ENGINE_GROUP}",
  "pix_outcome_publisher_group": "${PIX_OUTCOME_PUBLISHER_GROUP}",
  "persister_valid_group": "${PERSISTER_VALID_GROUP}",
  "persister_rejected_group": "${PERSISTER_REJECTED_GROUP}"
}
EOF
