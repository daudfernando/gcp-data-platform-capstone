import os
import json
import requests
from datetime import datetime, timezone, timedelta

from google.cloud import storage
from google.cloud import bigquery

def fetch_frankfurter(base: str, start_date: str, end_date: str) -> dict:
    url = f"https://api.frankfurter.app/{start_date}..{end_date}"
    r = requests.get(url, params={"base": base}, timeout=30)
    r.raise_for_status()
    return r.json()

def gcs_upload_text(bucket_name: str, object_path: str, text_data: str, content_type: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_string(data=text_data, content_type=content_type)

def flatten_frankfurter_to_ndjson(payload: dict) -> str:
    base = payload.get("base")
    rates = payload.get("rates", {})
    if not base or not isinstance(rates, dict) or not rates:
        raise ValueError("Format payload tidak sesuai (base/rates kosong).")

    load_ts = datetime.now(timezone.utc).isoformat()
    lines = []
    for fx_date, symbols in rates.items():
        if not isinstance(symbols, dict):
            continue
        for symbol, rate in symbols.items():
            rec = {
                "fx_date": fx_date,
                "base": base,
                "symbol": symbol,
                "rate": rate,
                "load_ts": load_ts,
            }
            lines.append(json.dumps(rec, ensure_ascii=False))
    return "\n".join(lines) + "\n"

def bq_load_ndjson_from_gcs(project_id: str, location: str, gcs_uri: str, dataset: str, table: str):
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("fx_date", "DATE"),
            bigquery.SchemaField("base", "STRING"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("rate", "NUMERIC"),
            bigquery.SchemaField("load_ts", "TIMESTAMP"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # sementara: replace biar gampang belajar
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
        location=location,
    )
    load_job.result()
    return table_id

def bq_build_mart(project_id: str, location: str, stg_ds: str, stg_table: str, mart_ds: str, mart_table: str):
    client = bigquery.Client(project=project_id)
    q = f"""
    CREATE OR REPLACE TABLE `{project_id}.{mart_ds}.{mart_table}`
    PARTITION BY fx_date
    CLUSTER BY base, symbol AS
    SELECT
      fx_date,
      base,
      symbol,
      rate,
      rate - LAG(rate, 1) OVER (PARTITION BY base, symbol ORDER BY fx_date) AS change_1d,
      rate - LAG(rate, 7) OVER (PARTITION BY base, symbol ORDER BY fx_date) AS change_7d,
      CURRENT_TIMESTAMP() AS load_ts
    FROM `{project_id}.{stg_ds}.{stg_table}`
    """
    job = client.query(q, location=location)
    job.result()
    return f"{project_id}.{mart_ds}.{mart_table}"

def bq_count(project_id: str, location: str, full_table: str) -> int:
    client = bigquery.Client(project=project_id)
    q = f"SELECT COUNT(*) AS row_count FROM `{full_table}`"
    job = client.query(q, location=location)
    rows = list(job.result())
    return int(rows[0]["row_count"])

def main():
    project_id = os.getenv("PROJECT_ID")
    bucket = os.getenv("BUCKET")  # tanpa gs://
    base = os.getenv("BASE", "EUR")

    bq_location = os.getenv("BQ_LOCATION", "asia-southeast2")
    stg_ds = os.getenv("BQ_STG_DATASET", "capstone_stg")
    stg_table = os.getenv("BQ_STG_TABLE", "stg_fx_rates")
    mart_ds = os.getenv("BQ_MART_DATASET", "capstone_mart")
    mart_table = os.getenv("BQ_MART_TABLE", "mart_fx_daily")

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=14)

    start_s = start_date.isoformat()
    end_s = end_date.isoformat()

    print("✅ FX pipeline started")
    print("PROJECT_ID:", project_id)
    print("BUCKET:", bucket)
    print("BASE:", base)
    print("Range:", start_s, "to", end_s)
    print("BQ location:", bq_location)

    payload = fetch_frankfurter(base=base, start_date=start_s, end_date=end_s)

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # 1) Upload RAW
    raw_path = f"raw/fx/run_date={run_date}/fx_rates.json"
    gcs_upload_text(
        bucket_name=bucket,
        object_path=raw_path,
        text_data=json.dumps(payload, ensure_ascii=False),
        content_type="application/json; charset=utf-8",
    )
    print(f"✅ Uploaded RAW: gs://{bucket}/{raw_path}")

    # 2) Flatten → NDJSON
    ndjson = flatten_frankfurter_to_ndjson(payload)

    # 3) Upload PROCESSED NDJSON
    processed_path = f"processed/fx/run_date={run_date}/fx_rates_flat.ndjson"
    gcs_upload_text(
        bucket_name=bucket,
        object_path=processed_path,
        text_data=ndjson,
        content_type="application/x-ndjson; charset=utf-8",
    )
    gcs_uri = f"gs://{bucket}/{processed_path}"
    print(f"✅ Uploaded NDJSON: {gcs_uri}")

    # 4) Load NDJSON → BigQuery staging
    stg_full = f"{project_id}.{stg_ds}.{stg_table}"
    bq_load_ndjson_from_gcs(project_id, bq_location, gcs_uri, stg_ds, stg_table)
    stg_count = bq_count(project_id, bq_location, stg_full)
    print(f"✅ Loaded to BQ staging: {stg_full} (rows={stg_count})")

    # 5) Build mart
    mart_full = f"{project_id}.{mart_ds}.{mart_table}"
    bq_build_mart(project_id, bq_location, stg_ds, stg_table, mart_ds, mart_table)
    mart_count = bq_count(project_id, bq_location, mart_full)
    print(f"✅ Built BQ mart: {mart_full} (rows={mart_count})")

    print("Done.")

if __name__ == "__main__":
    main()
