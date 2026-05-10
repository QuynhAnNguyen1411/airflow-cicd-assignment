#!/bin/bash
# Deploy DAG to Cloud Composer GCS bucket
set -euo pipefail

: "${COMPOSER_BUCKET:?Must set COMPOSER_BUCKET}"
: "${DAG_FILE:=dags/example_etl_dag.py}"

echo "▶ Deploying $DAG_FILE → $COMPOSER_BUCKET/dags/"
gsutil cp "$DAG_FILE" "$COMPOSER_BUCKET/dags/"
echo "✅ DAG deployed."