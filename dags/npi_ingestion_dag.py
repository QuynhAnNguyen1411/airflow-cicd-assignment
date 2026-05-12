"""
NPI Ingestion Pipeline (v2 - GCS intermediate storage)
=======================================================
Fix: không dùng XCom cho data lớn.
call_npi_registry → ghi GCS → trả về path (string nhỏ) qua XCom
Các task sau đọc từ GCS path thay vì XCom data.
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
# HELPERS
# ============================================================

def call_npi_api(npi_number):
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


def name_match(fa, la, fb, lb):
    if not fb or not lb:
        return False
    return fa.upper() == fb.upper() and la.upper() == lb.upper()


def gcs_read(bucket_name, path):
    blob = storage.Client().bucket(bucket_name).blob(path)
    return json.loads(blob.download_as_text()) if blob.exists() else None


def gcs_write(data, bucket_name, path):
    blob = storage.Client().bucket(bucket_name).blob(path)
    blob.upload_from_string(
        json.dumps(data, indent=2, default=str),
        content_type="application/json"
    )
    n = len(data) if isinstance(data, list) else 1
    print(f"Saved {n} records to gs://{bucket_name}/{path}")


# ============================================================
# TASKS
# ============================================================

def task_read_providers(**context):
    """Task 1: Đọc providers từ BQ → trả về list qua XCom (nhỏ, OK)."""
    client = bigquery.Client(project=PROJECT_ID)
    sql = f"""
        SELECT 'hospital-a'   AS source_hospital,
               providerid,
               firstname      AS internal_firstname,
               lastname       AS internal_lastname,
               specialization AS internal_specialization,
               deptid         AS internal_deptid,
               npi            AS internal_npi
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
    """
    Task 2: Gọi API → ghi kết quả lên GCS intermediate file.
    
    FIX: Thay vì return data lớn qua XCom,
    ta ghi lên GCS và chỉ return PATH (string nhỏ ~50 chars) qua XCom.
    Tại sao? XCom lưu trong metadata DB của Airflow (PostgreSQL).
    Khi 3 tasks song song cùng pull XCom lớn → DB bị quá tải.
    Dùng GCS tránh được vấn đề này.
    """
    ti        = context["ti"]
    ds        = context["ds"]
    providers = ti.xcom_pull(task_ids="read_providers")
    now       = datetime.now().isoformat()
    results   = []

    for prov in providers:
        npi_data = call_npi_api(prov["internal_npi"])
        record   = {
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
                npi_data.get("npi_first_name"), npi_data.get("npi_last_name"),
            )
            record["name_match"]        = matched
            record["validation_status"] = "VALID" if matched else "NAME_MISMATCH"

        print(f"  {prov['providerid']} -> {record['validation_status']}")
        results.append(record)

    print(f"Summary: {dict(Counter(r['validation_status'] for r in results))}")

    # Ghi lên GCS intermediate file
    intermediate_path = f"landing/provider_npi_intermediate/dt={ds}/enriched.json"
    gcs_write(results, BUCKET, intermediate_path)

    # Chỉ return PATH (string nhỏ) qua XCom — an toàn
    return intermediate_path


def task_save_validation_output(**context):
    """Task 3: Output 1 — Đọc từ GCS path, ghi validation output."""
    ti   = context["ti"]
    ds   = context["ds"]
    path = ti.xcom_pull(task_ids="call_npi_registry")   # chỉ là string path
    records = gcs_read(BUCKET, path)                     # đọc từ GCS

    FIELDS = [
        "providerid", "internal_firstname", "internal_lastname",
        "internal_specialization", "internal_deptid", "internal_npi",
        "npi_found", "npi_first_name", "npi_last_name",
        "npi_organisation_name", "npi_position", "npi_last_updated",
        "name_match", "validation_status", "refreshed_at",
    ]
    output = [{k: r.get(k) for k in FIELDS} for r in records]
    gcs_write(output, BUCKET, f"landing/provider_npi_validation/dt={ds}/validation.json")
    print(f"Validation: {dict(Counter(r['validation_status'] for r in output))}")


def task_save_snapshot(**context):
    """Task 4: Output 2 — Đọc từ GCS path, ghi snapshot đầy đủ."""
    ti      = context["ti"]
    ds      = context["ds"]
    path    = ti.xcom_pull(task_ids="call_npi_registry")
    records = gcs_read(BUCKET, path)
    gcs_write(records, BUCKET, f"landing/provider_npi_snapshot/dt={ds}/snapshot.json")


def task_detect_changes(**context):
    """Task 5: Output 3 — So sánh snapshot mới vs cũ → NEW/UNCHANGED/UPDATED."""
    ti      = context["ti"]
    ds      = context["ds"]
    path    = ti.xcom_pull(task_ids="call_npi_registry")
    current = gcs_read(BUCKET, path)

    # Tìm snapshot ngày gần nhất trước hôm nay
    client     = storage.Client()
    all_blobs  = list(client.list_blobs(BUCKET, prefix="landing/provider_npi_snapshot/dt="))
    past_dates = sorted(set(
        b.name.split("/")[2].replace("dt=", "")
        for b in all_blobs
        if b.name.endswith("snapshot.json")
        and b.name.split("/")[2].replace("dt=", "") < ds
    ))

    if not past_dates:
        print("No previous snapshot. All = NEW")
        changes = [{**r, "change_type": "NEW",
                    "compared_to": None,
                    "detected_at": datetime.now().isoformat()} for r in current]
        gcs_write(changes, BUCKET,
                  f"landing/provider_npi_change_detection/dt={ds}/changes.json")
        return

    prev_dt   = past_dates[-1]
    prev_data = gcs_read(BUCKET,
        f"landing/provider_npi_snapshot/dt={prev_dt}/snapshot.json") or []
    print(f"Comparing with dt={prev_dt}")

    KEYS = ["npi_first_name","npi_last_name","npi_position",
            "npi_organisation_name","npi_last_updated","validation_status"]
    prev_idx = {r["internal_npi"]: {k: r.get(k) for k in KEYS} for r in prev_data}

    changes = []
    for r in current:
        key  = r["internal_npi"]
        curr = {k: r.get(k) for k in KEYS}
        ct   = ("NEW" if key not in prev_idx
                else "UNCHANGED" if prev_idx[key] == curr
                else "UPDATED")
        changes.append({**r, "change_type": ct,
                         "compared_to": prev_dt,
                         "detected_at": datetime.now().isoformat()})

    print(f"Changes: {dict(Counter(c['change_type'] for c in changes))}")
    gcs_write(changes, BUCKET,
              f"landing/provider_npi_change_detection/dt={ds}/changes.json")


# ============================================================
# DAG
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
