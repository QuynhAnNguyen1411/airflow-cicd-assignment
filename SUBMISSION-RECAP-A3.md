# Assignment 3 - NPI Ingestion Pipeline

## Info
- GitHub: https://github.com/QuynhAnNguyen1411/airflow-cicd-assignment
- DAG file: dags/npi_ingestion_dag.py

## Checklist
- [x] Read providers from BigQuery (UNION ALL hospital_a + hospital_b)
- [x] Call NPI Registry API for each provider
- [x] Validation: VALID / NAME_MISMATCH / NPI_NOT_FOUND
- [x] Snapshot: full data at dt={ds}
- [x] Change detection: NEW / UNCHANGED / UPDATED
- [x] DAG run success on Cloud Composer (3/4 runs)

## Output paths
- gs://healthcare-analytics-495602-landing/landing/provider_npi_validation/dt=2026-05-11/validation.json
- gs://healthcare-analytics-495602-landing/landing/provider_npi_snapshot/dt=2026-05-11/snapshot.json
- gs://healthcare-analytics-495602-landing/landing/provider_npi_change_detection/dt=2026-05-11/changes.json
