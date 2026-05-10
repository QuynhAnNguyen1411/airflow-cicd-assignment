#!/bin/bash
# Trigger DAG run after deployment
set -euo pipefail

: "${COMPOSER_ENV:?Must set COMPOSER_ENV}"
: "${COMPOSER_LOCATION:?Must set COMPOSER_LOCATION}"
: "${DAG_ID:=healthcare_etl_pipeline}"

echo "▶ Waiting 90s for Composer to parse DAG..."
sleep 90

echo "▶ Triggering DAG: $DAG_ID"
gcloud composer environments run "$COMPOSER_ENV" \
  --location="$COMPOSER_LOCATION" \
  dags trigger -- "$DAG_ID"
echo "✅ DAG triggered."