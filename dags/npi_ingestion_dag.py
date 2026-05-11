"""
NPI Ingestion Pipeline
======================
Flow:
  read_providers (BQ)
    → call_npi_registry (API)
       → save_validation_output (GCS output 1)
       → save_snapshot          (GCS output 2)
       → detect_changes         (GCS output 3)
"""

import json
from collections import Counter
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery, storage

PROJECT_ID = "healthcare-analytics-495602"
BUCKET     = f"{PROJECT_ID}-landing"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ============================================================
# HELPER FUNCTIONS (thuần Python, không phụ thuộc Airflow)
# ============================================================

def call_npi_api(npi_number):
    """Gọi NPI Registry API. Trả về dict kết quả."""
    url    = "https://npiregistry.cms.hhs.gov/api/"
    params = {"number": str(npi_number), "version": "2.1"}
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
    except Exception as e:
        return {"validation_status": "API_ERROR", "error": str(e)}

    if data.get("result_count", 0) == 0:
        return {"validation_status": "NPI_NOT_FOUND"}

    p     = data["results"][0]
    basic = p.get("basic", {})
    tax   = next((t for t in p.get("taxonomies", []) if t.get("primary")), {})

    return {
        "npi_id":                p.get("number"),
        "npi_first_name":        basic.get("first_name"),
        "npi_last_name":         basic.get("last_name"),
        "npi_position":          tax.get("desc"),
        "npi_organisation_name": basic.get("organization_name"),
        "npi_last_updated":      basic.get("last_updated"),
        "enumeration_type":      p.get("enumeration_type"),
        "validation_status":     "FOUND",
    }


def name_match(first_a, last_a, first_b, last_b):
    """So sánh tên không phân biệt hoa/thường."""
    if not first_b or not last_b:
        return False
    return first_a.upper() == first_b.upper() and last_a.upper() == last_b.upper()


def gcs_read(bucket_name, path):
    """Đọc JSON từ GCS. Trả về None nếu file không tồn tại."""
    blob = storage.Client().bucket(bucket_name).blob(path)
    return json.loads(blob.download_as_text()) if blob.exists() else None


def gcs_write(data, bucket_name, path):
    """Ghi JSON lên GCS."""
    blob = storage.Client().bucket(bucket_name).blob(path)
    blob.upload_from_string(
        json.dumps(data, indent=2, default=str),
        content_type="application/json"
    )
    n = len(data) if isinstance(data, list) else 1
    print(f"Saved {n} records to gs://{bucket_name}/{path}")


# ============================================================
# TASK FUNCTIONS
# ============================================================

def task_read_providers(**context):
    """Task 1: Đọc providers từ BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"""
        SELECT 'hospital-a'    AS source_hospital,
               providerid,
               firstname       AS internal_firstname,
               lastname        AS internal_lastname,
               specialization  AS internal_specialization,
               deptid          AS internal_deptid,
               npi             AS internal_npi
        FROM `{PROJECT_ID}.bronze_raw.hospital_a_providers`
        UNION ALL
        SELECT 'hospital-b', providerid, firstname, lastname,
               specialization, deptid, npi
        FROM `{PROJECT_ID}.bronze_raw.hospital_b_providers`
    """
    providers = [dict(row) for row in client.query(sql).result()]
    print(f"Read {len(providers)} providers")
    return providers


def task_call_npi_registry(**context):
    """Task 2: Gọi NPI API cho từng provider."""
    ti        = context["ti"]
    providers = ti.xcom_pull(task_ids="read_providers")
    now       = datetime.now().isoformat()
    results   = []

    for prov in providers:
        npi_data = call_npi_api(prov["internal_npi"])

        record = {
            "providerid":              prov["providerid"],
            "source_hospital":         prov["source_hospital"],
            "internal_firstname":      prov["internal_firstname"],
            "internal_lastname":       prov["internal_lastname"],
            "internal_specialization": prov["internal_specialization"],
            "internal_deptid":         prov["internal_deptid"],
            "internal_npi":            str(prov["internal_npi"]),
            "npi_id":                  npi_data.get("npi_id"),
            "npi_first_name":          npi_data.get("npi_first_name"),
            "npi_last_name":           npi_data.get("npi_last_name"),
            "npi_position":            npi_data.get("npi_position"),
            "npi_organisation_name":   npi_data.get("npi_organisation_name"),
            "npi_last_updated":        npi_data.get("npi_last_updated"),
            "enumeration_type":        npi_data.get("enumeration_type"),
            "npi_found":               npi_data["validation_status"] == "FOUND",
            "refreshed_at":            now,
        }

        if npi_data["validation_status"] != "FOUND":
            record["name_match"]        = False
            record["validation_status"] = npi_data["validation_status"]
        else:
            matched = name_match(
                prov["internal_firstname"], prov["internal_lastname"],
                npi_data.get("npi_first_name"), npi_data.get("npi_last_name")
            )
            record["name_match"]        = matched
            record["validation_status"] = "VALID" if matched else "NAME_MISMATCH"

        print(f"  {prov['providerid']} -> {record['validation_status']}")
        results.append(record)

    print(f"Summary: {dict(Counter(r['validation_status'] for r in results))}")
    return results


def task_save_validation_output(**context):
    """Task 3: Output 1 — Validation results."""
    ti      = context["ti"]
    ds      = context["ds"]
    records = ti.xcom_pull(task_ids="call_npi_registry")

    FIELDS = [
        "providerid", "internal_firstname", "internal_lastname",
        "internal_specialization", "internal_deptid", "internal_npi",
        "npi_found", "npi_first_name", "npi_last_name",
        "npi_organisation_name", "npi_position", "npi_last_updated",
        "name_match", "validation_status", "refreshed_at",
    ]
    output = [{k: r.get(k) for k in FIELDS} for r in records]
    gcs_write(output, BUCKET, f"landing/provider_npi_validation/dt={ds}/validation.json")


def task_save_snapshot(**context):
    """Task 4: Output 2 — Full snapshot."""
    ti      = context["ti"]
    ds      = context["ds"]
    records = ti.xcom_pull(task_ids="call_npi_registry")
    gcs_write(records, BUCKET, f"landing/provider_npi_snapshot/dt={ds}/snapshot.json")


def task_detect_changes(**context):
    """Task 5: Output 3 — Change detection (NEW/UNCHANGED/UPDATED)."""
    ti      = context["ti"]
    ds      = context["ds"]
    current = ti.xcom_pull(task_ids="call_npi_registry")

    # Tìm snapshot ngày gần nhất trước hôm nay
    client    = storage.Client()
    all_blobs = list(client.list_blobs(BUCKET, prefix="landing/provider_npi_snapshot/dt="))
    past_dates = sorted(set(
        b.name.split("/")[2].replace("dt=", "")
        for b in all_blobs
        if b.name.endswith("snapshot.json")
        and b.name.split("/")[2].replace("dt=", "") < ds
    ))

    # Lần đầu chạy: chưa có snapshot cũ → tất cả là NEW
    if not past_dates:
        print("No previous snapshot. All records = NEW")
        changes = [
            {**r, "change_type": "NEW",
             "compared_to": None,
             "detected_at": datetime.now().isoformat()}
            for r in current
        ]
        gcs_write(changes, BUCKET,
                  f"landing/provider_npi_change_detection/dt={ds}/changes.json")
        return

    # Load snapshot cũ
    prev_dt   = past_dates[-1]
    prev_data = gcs_read(BUCKET,
                f"landing/provider_npi_snapshot/dt={prev_dt}/snapshot.json") or []
    print(f"Comparing with snapshot dt={prev_dt}")

    KEYS = ["npi_first_name", "npi_last_name", "npi_position",
            "npi_organisation_name", "npi_last_updated", "validation_status"]
    prev_index = {
        r["internal_npi"]: {k: r.get(k) for k in KEYS}
        for r in prev_data
    }

    changes = []
    for r in current:
        key  = r["internal_npi"]
        curr = {k: r.get(k) for k in KEYS}
        if key not in prev_index:
            ct = "NEW"
        elif prev_index[key] == curr:
            ct = "UNCHANGED"
        else:
            ct = "UPDATED"
        changes.append({**r, "change_type": ct,
                         "compared_to": prev_dt,
                         "detected_at": datetime.now().isoformat()})

    print(f"Change summary: {dict(Counter(c['change_type'] for c in changes))}")
    gcs_write(changes, BUCKET,
              f"landing/provider_npi_change_detection/dt={ds}/changes.json")


# ============================================================
# DAG ASSEMBLY
# ============================================================
with DAG(
    dag_id="npi_ingestion_pipeline",
    description="NPI validation + enrichment + snapshot + change detection",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["npi", "enrichment", "provider"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    t1 = PythonOperator(task_id="read_providers",         python_callable=task_read_providers)
    t2 = PythonOperator(task_id="call_npi_registry",      python_callable=task_call_npi_registry)
    t3 = PythonOperator(task_id="save_validation_output", python_callable=task_save_validation_output)
    t4 = PythonOperator(task_id="save_snapshot",          python_callable=task_save_snapshot)
    t5 = PythonOperator(task_id="detect_changes",         python_callable=task_detect_changes)

    start >> t1 >> t2 >> [t3, t4, t5] >> end
