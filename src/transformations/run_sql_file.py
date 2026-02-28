import argparse
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pathlib import Path

def get_engine():
    load_dotenv()
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db   = os.getenv("DB_NAME", "capstone_db")
    user = os.getenv("DB_USER", "capstone")
    pwd  = os.getenv("DB_PASSWORD", "capstone")
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True, help="Path file SQL")
    args = ap.parse_args()

    p = Path(args.path)
    if not p.exists():
        raise FileNotFoundError(f"SQL file not found: {p}")

    sql = p.read_text(encoding="utf-8")
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(f"✅ Executed SQL: {p}")

if __name__ == "__main__":
    main()
