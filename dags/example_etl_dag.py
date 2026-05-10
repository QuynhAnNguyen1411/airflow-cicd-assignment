"""
Healthcare ETL DAG — orchestrate Bronze → Silver → Gold trên BigQuery.

Tận dụng SQL đã viết ở Assignment 1.
Trigger CI/CD: mỗi lần push code mới, Cloud Build deploy DAG này lên Composer.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

# ============================================================
# Config
# ============================================================
PROJECT_ID = "healthcare-analytics-495602"
LOCATION   = "asia-southeast1"
BUCKET     = f"{PROJECT_ID}-landing"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ============================================================
# Helper: build BigQuery task
# ============================================================
def bq_task(task_id: str, sql: str) -> BigQueryInsertJobOperator:
    """Tạo task chạy 1 SQL statement trên BigQuery."""
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": sql,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )


# ============================================================
# DAG definition
# ============================================================
with DAG(
    dag_id="healthcare_etl_pipeline",
    description="Bronze → Silver → Gold ETL on BigQuery (v2 - test CI/CD)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",                 # Chạy hằng ngày, hoặc None để chỉ chạy thủ công
    catchup=False,                     # Không backfill quá khứ
    max_active_runs=1,                 # Chỉ 1 instance chạy cùng lúc
    tags=["healthcare", "etl", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # -------- BRONZE --------
    # External tables đã tạo 1 lần trong setup, không cần loop hằng ngày.
    # Chỉ refresh native tables (claims, cptcodes) phòng có data mới.
    bronze_refresh = bq_task(
        "bronze_refresh_native",
        f"""
        -- Native tables không tự refresh, chỉ touch metadata
        SELECT 'bronze refreshed' AS status,
               (SELECT COUNT(*) FROM `{PROJECT_ID}.bronze_native.claims`) AS claims_count;
        """
    )

    # -------- SILVER DIMS --------
    silver_dim_patient = bq_task("silver_dim_patient", f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.silver_cdm.dim_patient`
        PARTITION BY effective_from CLUSTER BY patient_sk AS
        WITH unified AS (
            SELECT 'hospital-a' AS source_hospital, patientid AS patient_id,
                   firstname AS first_name, lastname AS last_name, middlename AS middle_name,
                   ssn, phonenumber, gender, dob, address, modifieddate
            FROM `{PROJECT_ID}.bronze_raw.hospital_a_patients`
            UNION ALL
            SELECT 'hospital-b', id, f_name, l_name, m_name,
                   ssn, phonenumber, gender, dob, address, modifieddate
            FROM `{PROJECT_ID}.bronze_raw.hospital_b_patients`
        )
        SELECT TO_HEX(SHA256(CONCAT(source_hospital,'|',patient_id))) AS patient_sk,
               patient_id, source_hospital, first_name, last_name, middle_name,
               TRIM(CONCAT(COALESCE(first_name,''),' ',COALESCE(last_name,''))) AS full_name,
               ssn, phonenumber, gender, dob, address,
               CURRENT_DATE() AS effective_from,
               DATE '9999-12-31' AS effective_to,
               TRUE AS is_current,
               CURRENT_TIMESTAMP() AS _silver_loaded_at
        FROM unified
    """)

    silver_dim_provider = bq_task("silver_dim_provider", f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.silver_cdm.dim_provider`
        PARTITION BY effective_from CLUSTER BY provider_sk AS
        WITH unified AS (
            SELECT 'hospital-a' AS source_hospital,
                   REGEXP_REPLACE(providerid, r'^H[12]-', '') AS provider_id,
                   firstname, lastname, specialization, deptid, npi
            FROM `{PROJECT_ID}.bronze_raw.hospital_a_providers`
            UNION ALL
            SELECT 'hospital-b', REGEXP_REPLACE(providerid, r'^H[12]-', ''),
                   firstname, lastname, specialization, deptid, npi
            FROM `{PROJECT_ID}.bronze_raw.hospital_b_providers`
        )
        SELECT TO_HEX(SHA256(CONCAT(source_hospital,'|',provider_id))) AS provider_sk,
               provider_id, source_hospital, firstname AS first_name, lastname AS last_name,
               TRIM(CONCAT(COALESCE(firstname,''),' ',COALESCE(lastname,''))) AS full_name,
               specialization, deptid AS department_id, npi,
               CURRENT_DATE() AS effective_from, DATE '9999-12-31' AS effective_to,
               TRUE AS is_current, CURRENT_TIMESTAMP() AS _silver_loaded_at
        FROM unified
        UNION ALL
        SELECT 'UNKNOWN_PROVIDER_SK','UNKNOWN','unknown',NULL,NULL,'Unknown Provider',
               NULL,NULL,NULL,DATE '1900-01-01',DATE '9999-12-31',TRUE,CURRENT_TIMESTAMP()
    """)

    silver_dim_department = bq_task("silver_dim_department", f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.silver_cdm.dim_department` CLUSTER BY department_sk AS
        WITH unified AS (
            SELECT 'hospital-a' AS source_hospital, deptid AS department_id, name AS department_name
            FROM `{PROJECT_ID}.bronze_raw.hospital_a_departments`
            UNION ALL
            SELECT 'hospital-b', deptid, name FROM `{PROJECT_ID}.bronze_raw.hospital_b_departments`)
        SELECT TO_HEX(SHA256(CONCAT(source_hospital,'|',department_id))) AS department_sk,
               department_id, source_hospital, department_name, CURRENT_TIMESTAMP() AS _silver_loaded_at
        FROM unified
    """)

    # -------- SILVER FACTS --------
    silver_fact_encounter = bq_task("silver_fact_encounter", f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.silver_cdm.fact_encounter`
        PARTITION BY encounter_date CLUSTER BY patient_sk, provider_sk AS
        WITH unified AS (
            SELECT 'hospital-a' AS source_hospital, encounterid, patientid, encounterdate,
                   encountertype, providerid, departmentid, procedurecode
            FROM `{PROJECT_ID}.bronze_raw.hospital_a_encounters`
            UNION ALL
            SELECT 'hospital-b', encounterid, patientid, encounterdate, encountertype,
                   providerid, departmentid, procedurecode
            FROM `{PROJECT_ID}.bronze_raw.hospital_b_encounters`)
        SELECT TO_HEX(SHA256(CONCAT(u.source_hospital,'|',u.encounterid))) AS encounter_sk,
               p.patient_sk, COALESCE(pr.provider_sk,'UNKNOWN_PROVIDER_SK') AS provider_sk,
               d.department_sk, u.source_hospital, u.encounterid AS encounter_id,
               u.encounterdate AS encounter_date, u.encountertype AS encounter_type,
               u.procedurecode AS procedure_code, CURRENT_TIMESTAMP() AS _silver_loaded_at
        FROM unified u
        LEFT JOIN `{PROJECT_ID}.silver_cdm.dim_patient` p
            ON p.patient_id=u.patientid AND p.source_hospital=u.source_hospital
        LEFT JOIN `{PROJECT_ID}.silver_cdm.dim_provider` pr
            ON pr.provider_id=u.providerid AND pr.source_hospital=u.source_hospital
        LEFT JOIN `{PROJECT_ID}.silver_cdm.dim_department` d
            ON d.department_id=u.departmentid AND d.source_hospital=u.source_hospital
    """)

    silver_fact_transaction = bq_task("silver_fact_transaction", f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.silver_cdm.fact_transaction`
        PARTITION BY visit_date CLUSTER BY patient_sk, provider_sk AS
        WITH unified AS (
            SELECT 'hospital-a' AS source_hospital, * EXCEPT(_ingestion_date)
            FROM `{PROJECT_ID}.bronze_raw.hospital_a_transactions`
            UNION ALL
            SELECT 'hospital-b', * EXCEPT(_ingestion_date)
            FROM `{PROJECT_ID}.bronze_raw.hospital_b_transactions`)
        SELECT TO_HEX(SHA256(CONCAT(u.source_hospital,'|',u.transactionid))) AS transaction_sk,
               TO_HEX(SHA256(CONCAT(u.source_hospital,'|',u.encounterid))) AS encounter_sk,
               p.patient_sk, COALESCE(pr.provider_sk,'UNKNOWN_PROVIDER_SK') AS provider_sk,
               d.department_sk, u.source_hospital,
               u.transactionid AS transaction_id, u.visitdate AS visit_date,
               u.amount, u.paidamount AS paid_amount,
               (u.amount - COALESCE(u.paidamount,0)) AS outstanding_amount,
               u.lineofbusiness AS line_of_business, u.payorid AS payor_id,
               CURRENT_TIMESTAMP() AS _silver_loaded_at
        FROM unified u
        LEFT JOIN `{PROJECT_ID}.silver_cdm.dim_patient` p
            ON p.patient_id=u.patientid AND p.source_hospital=u.source_hospital
        LEFT JOIN `{PROJECT_ID}.silver_cdm.dim_provider` pr
            ON pr.provider_id=u.providerid AND pr.source_hospital=u.source_hospital
        LEFT JOIN `{PROJECT_ID}.silver_cdm.dim_department` d
            ON d.department_id=u.deptid AND d.source_hospital=u.source_hospital
    """)

    # -------- GOLD --------
    gold_provider_perf = bq_task("gold_provider_performance", f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.gold_mart.provider_performance` AS
        WITH txn_agg AS (
            SELECT provider_sk, COUNT(DISTINCT encounter_sk) AS encounters,
                   SUM(amount) AS billed, SUM(paid_amount) AS collected
            FROM `{PROJECT_ID}.silver_cdm.fact_transaction`
            WHERE provider_sk!='UNKNOWN_PROVIDER_SK' GROUP BY 1)
        SELECT pr.provider_id, pr.full_name, pr.specialization, pr.source_hospital,
               COALESCE(t.encounters,0) AS total_encounters,
               COALESCE(t.billed,0) AS total_billed,
               ROUND(SAFE_DIVIDE(t.collected,t.billed),4) AS collection_rate
        FROM `{PROJECT_ID}.silver_cdm.dim_provider` pr
        LEFT JOIN txn_agg t ON t.provider_sk=pr.provider_sk
        WHERE pr.provider_sk!='UNKNOWN_PROVIDER_SK'
    """)

    gold_financial = bq_task("gold_financial_metrics", f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.gold_mart.financial_metrics` AS
        SELECT DATE_TRUNC(visit_date,MONTH) AS revenue_month,
               source_hospital, line_of_business, payor_id,
               COUNT(DISTINCT transaction_sk) AS n_transactions,
               SUM(amount) AS total_billed, SUM(paid_amount) AS total_collected,
               ROUND(SAFE_DIVIDE(SUM(paid_amount),SUM(amount)),4) AS collection_rate
        FROM `{PROJECT_ID}.silver_cdm.fact_transaction`
        GROUP BY 1,2,3,4
    """)

    # ============================================================
    # Task dependencies
    # ============================================================
    start >> bronze_refresh

    # 3 dims chạy song song sau bronze
    bronze_refresh >> [silver_dim_patient, silver_dim_provider, silver_dim_department]

    # Facts chỉ chạy sau khi cả 3 dims xong
    [silver_dim_patient, silver_dim_provider, silver_dim_department] >> silver_fact_encounter
    [silver_dim_patient, silver_dim_provider, silver_dim_department] >> silver_fact_transaction

    # Gold chạy sau khi facts xong
    silver_fact_transaction >> [gold_provider_perf, gold_financial]

    # End
    [silver_fact_encounter, gold_provider_perf, gold_financial] >> end