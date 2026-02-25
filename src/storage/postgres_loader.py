import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def latest_raw_file() -> str:
    candidates = list(Path("data/raw/fx").glob("**/fx_rates.json"))
    if not candidates:
        raise FileNotFoundError("Tidak ada file raw: data/raw/fx/**/fx_rates.json")
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    return str(latest)

def get_engine():
    load_dotenv()
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db   = os.getenv("DB_NAME", "capstone_db")
    user = os.getenv("DB_USER", "capstone")
    pwd  = os.getenv("DB_PASSWORD", "capstone")
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)

def parse_frankfurter(payload: dict):
    base = payload.get("base")
    rates = payload.get("rates", {})
    if not base or not rates:
        return []
    rows = []
    for date_str, symbols in rates.items():
        if not isinstance(symbols, dict):
            continue
        for symbol, rate in symbols.items():
            rows.append({
                "fx_date": date_str,
                "base": base,
                "symbol": symbol,
                "rate": rate
            })
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-path", default=None, help="Path file raw fx_rates.json. Jika kosong, ambil yang terbaru.")
    args = ap.parse_args()

    raw_path = args.raw_path or latest_raw_file()
    p = Path(raw_path)
    if not p.exists():
        raise FileNotFoundError(f"Raw file tidak ditemukan: {raw_path}")

    payload = json.loads(p.read_text(encoding="utf-8"))
    raw_payload_text = json.dumps(payload, ensure_ascii=False)

    rows = parse_frankfurter(payload)
    if not rows:
        raise ValueError("Hasil parse kosong. Pastikan file raw benar dari Frankfurter.")

    engine = get_engine()

    insert_raw = text("""
        INSERT INTO raw_fx (file_path, raw_payload)
        VALUES (:file_path, :raw_payload)
    """)

    upsert_stg = text("""
        INSERT INTO stg_fx_rates (fx_date, base, symbol, rate)
        VALUES (:fx_date, :base, :symbol, :rate)
        ON CONFLICT (fx_date, base, symbol)
        DO UPDATE SET
            rate = EXCLUDED.rate,
            load_ts = NOW()
    """)

    with engine.begin() as conn:
        conn.execute(insert_raw, {"file_path": str(p), "raw_payload": raw_payload_text})
        conn.execute(upsert_stg, rows)

    print(f"✅ raw_fx inserted: {p}")
    print(f"✅ stg_fx_rates upserted rows: {len(rows)}")

if __name__ == "__main__":
    main()
