#!/bin/bash
# Validate DAG: syntax check + pytest
set -euo pipefail

echo "▶ Step 1: Python syntax check"
python3 -m py_compile dags/example_etl_dag.py
echo "  ✅ Syntax OK"

echo "▶ Step 2: Run pytest"
pytest tests/ -v
echo "  ✅ Tests passed"

echo "✅ All validations passed."