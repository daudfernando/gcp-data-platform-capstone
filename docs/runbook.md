# Runbook — GCP Data Platform Capstone (Local)

Dokumen ini berisi langkah operasional & troubleshooting untuk pipeline lokal:
**Ingest (Frankfurter FX) → Load (Postgres) → Transform (Mart) → Quality Tests → Orchestrate (Airflow)**.

Repo terkait:
- Repo kamu: https://github.com/daudfernando/gcp-data-platform-capstone
- Dataset: Frankfurter FX API (public)

---

## 1) Struktur Komponen (Local)
- **Postgres Capstone (data)**: service `postgres` (DB: `capstone_db`)
- **Airflow Metadata DB**: service `airflow-db` (DB: `airflow`)
- **Airflow services**: `airflow-webserver`, `airflow-scheduler`, `airflow-init`

Tabel utama di Postgres Capstone:
- `raw_fx` (raw payload JSON)
- `stg_fx_rates` (normalized)
- `mart_fx_daily` (analytics-ready + change_1d / change_7d)

---

## 2) Perintah Harian (Quick Commands)

### 2.1 Start stack
```bash
cd "/mnt/c/Data Engineer/github/gcp-data-platform-capstone"
docker compose up -d postgres airflow-db
docker compose up -d airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose ps
```

### 2.2 Stop stack (tanpa hapus data)
```bash
docker compose down
```

### 2.3 Cek koneksi & tabel Postgres capstone
> Gunakan **docker compose exec** supaya tidak bergantung ke nama container.
```bash
docker compose exec postgres psql -U capstone -d capstone_db -c "SELECT 1;"
docker compose exec postgres psql -U capstone -d capstone_db -c "\\dt"
```

### 2.4 Cek rowcount cepat
```bash
docker compose exec postgres psql -U capstone -d capstone_db -c "SELECT COUNT(*) FROM raw_fx;"
docker compose exec postgres psql -U capstone -d capstone_db -c "SELECT COUNT(*) FROM stg_fx_rates;"
docker compose exec postgres psql -U capstone -d capstone_db -c "SELECT COUNT(*) FROM mart_fx_daily;"
```

### 2.5 Trigger DAG (CLI)
```bash
docker exec -it airflow-webserver airflow dags trigger fx_daily_pipeline
docker exec -it airflow-webserver airflow dags list-runs -d fx_daily_pipeline | head -n 10
```

---

## 3) Manual Run Tanpa Airflow (Local)
### 3.1 Ingest raw JSON
Jika modul `-m` bermasalah, selalu aman pakai `PYTHONPATH=$(pwd)`:
```bash
source .venv/bin/activate
PYTHONPATH="$(pwd)" python -m src.ingestion.fx_frankfurter --from 2026-02-01 --to 2026-02-07 --base EUR
```

### 3.2 Load raw → raw_fx dan parse → stg_fx_rates
```bash
source .venv/bin/activate
PYTHONPATH="$(pwd)" python -m src.storage.postgres_loader
```

### 3.3 Transform stg → mart
```bash
docker compose exec -T postgres psql -U capstone -d capstone_db < src/transformations/sql/020_mart_fx_daily.sql
```

### 3.4 Jalankan quality tests (pytest)
```bash
source .venv/bin/activate
pytest -q
```

---

## 4) Data Quality Checks (SQL)
### 4.1 Not null (wajib 0)
```bash
docker compose exec postgres psql -U capstone -d capstone_db -c "
SELECT
  SUM(CASE WHEN fx_date IS NULL THEN 1 ELSE 0 END) AS null_fx_date,
  SUM(CASE WHEN base IS NULL THEN 1 ELSE 0 END) AS null_base,
  SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) AS null_symbol,
  SUM(CASE WHEN rate IS NULL THEN 1 ELSE 0 END) AS null_rate
FROM stg_fx_rates;"
```

### 4.2 Rate harus > 0 (wajib 0)
```bash
docker compose exec postgres psql -U capstone -d capstone_db -c "
SELECT COUNT(*) AS bad_rate
FROM stg_fx_rates
WHERE rate <= 0;"
```

### 4.3 Validasi format symbol 3 huruf (hindari `!~` karena bash history expansion)
```bash
docker compose exec postgres psql -U capstone -d capstone_db -c "
SELECT COUNT(*) AS bad_symbol
FROM stg_fx_rates
WHERE NOT (symbol ~ '^[A-Z]{3}$');"
```

---

## 5) Troubleshooting Paling Sering

### 5.1 `No module named src.ingestion.fx_frankfurter`
Penyebab umum:
- Jalankan bukan dari root project (yang ada folder `src`)
- `__init__.py` belum ada
- `PYTHONPATH` belum diset

Fix cepat:
```bash
cd "/mnt/c/Data Engineer/github/gcp-data-platform-capstone"
touch src/__init__.py src/ingestion/__init__.py src/storage/__init__.py src/utils/__init__.py
source .venv/bin/activate
PYTHONPATH="$(pwd)" python -m src.ingestion.fx_frankfurter --from 2026-02-01 --to 2026-02-07 --base EUR
```

---

### 5.2 `externally-managed-environment` saat pip install
Artinya kamu pakai pip system (PEP 668). Pastikan pip dari venv.
```bash
cd "/mnt/c/Data Engineer/github/gcp-data-platform-capstone"
source .venv/bin/activate
which python
which pip
python -m pip install -U pip
python -m pip install -r requirements.txt
```

---

### 5.3 Airflow login 401 / Invalid credentials
Biasanya user belum dibuat atau init gagal. Buat user manual:
```bash
docker exec -it airflow-webserver airflow users create \
  --username airflow \
  --password airflow \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email admin@example.com
docker exec -it airflow-webserver airflow users list
```

---

### 5.4 Airflow webserver tidak running / status `Created`
Penyebab umum:
- Port UI bentrok (mis. 8080 dipakai stack lain)

Solusi:
- Ubah mapping port di `docker-compose.yml`, contoh:
  - `"8081:8080"` atau `"8082:8080"`
- Lalu restart:
```bash
docker compose up -d airflow-webserver airflow-scheduler
docker compose ps
```

Cek siapa pakai port:
```bash
ss -ltnp | grep ':8080' || true
ss -ltnp | grep ':8081' || true
```

---

### 5.5 Volume Airflow tidak bisa dihapus: `volume is in use`
Artinya masih ada container yang menempel volume.
Stop + remove container terkait, baru hapus volume:
```bash
docker rm -f airflow-scheduler airflow-webserver airflow-init airflow-postgres 2>/dev/null || true
docker volume rm gcp-data-platform-capstone_airflow_pgdata
```

---

### 5.6 Postgres capstone “hilang” (`\\dt` kosong / Did not find any relations)
Biasanya volume DB capstone ke-reset. Recreate tabel:
```bash
docker compose exec -T postgres psql -U capstone -d capstone_db << 'SQL'
CREATE TABLE IF NOT EXISTS raw_fx (
  id BIGSERIAL PRIMARY KEY,
  file_path TEXT NOT NULL,
  raw_payload TEXT NOT NULL,
  load_ts TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS stg_fx_rates (
  fx_date DATE NOT NULL,
  base TEXT NOT NULL,
  symbol TEXT NOT NULL,
  rate NUMERIC NOT NULL,
  load_ts TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT stg_fx_rates_pk PRIMARY KEY (fx_date, base, symbol)
);
CREATE TABLE IF NOT EXISTS mart_fx_daily (
  fx_date DATE NOT NULL,
  base TEXT NOT NULL,
  symbol TEXT NOT NULL,
  rate NUMERIC NOT NULL,
  change_1d NUMERIC,
  change_7d NUMERIC,
  load_ts TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT mart_fx_daily_pk PRIMARY KEY (fx_date, base, symbol)
);
SQL
```
Lalu load ulang:
```bash
source .venv/bin/activate
PYTHONPATH="$(pwd)" python -m src.storage.postgres_loader
docker compose exec -T postgres psql -U capstone -d capstone_db < src/transformations/sql/020_mart_fx_daily.sql
```

---

### 5.7 Task Airflow `load_to_postgres/build_mart/data_quality_tests` error (DB host salah)
Di container Airflow, **DB_HOST harus `postgres`** (service name), bukan `localhost`.

Fix:
- Pastikan task di DAG melakukan `export DB_HOST=postgres ...` sebelum menjalankan script,
atau set env DB di service `airflow-webserver` & `airflow-scheduler` (compose).

Cek cepat koneksi dari scheduler:
```bash
docker exec -it airflow-scheduler python - <<'PY'
import socket
s = socket.create_connection(("postgres", 5432), timeout=5)
print("✅ scheduler can reach postgres:5432")
s.close()
PY
```

---

## 6) Reset yang Aman vs Berbahaya

### Aman (tidak hapus data capstone)
- Restart service:
```bash
docker compose restart postgres airflow-webserver airflow-scheduler
```
- Reset metadata Airflow saja:
```bash
docker rm -f airflow-scheduler airflow-webserver airflow-init airflow-postgres 2>/dev/null || true
docker volume rm gcp-data-platform-capstone_airflow_pgdata
```

### Berbahaya (hapus data capstone)
- `docker compose down -v` akan menghapus **semua** volume di compose ini (termasuk data capstone).
Gunakan dengan sangat hati-hati.

---

## 7) Catatan Operasional
- `change_7d` sering kosong jika data belum cukup “baris” (weekend/libur → gap). Ini normal.
- Untuk kestabilan pipeline, ingestion window 14 hari terakhir lebih aman daripada 7 hari.
