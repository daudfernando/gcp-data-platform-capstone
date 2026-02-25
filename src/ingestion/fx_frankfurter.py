import argparse
import requests
from datetime import datetime
from pathlib import Path
import json

def fetch_fx(start_date: str, end_date: str, base: str):
    url = f"https://api.frankfurter.app/{start_date}..{end_date}"
    r = requests.get(url, params={"base": base}, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start_date", required=True)
    ap.add_argument("--to", dest="end_date", required=True)
    ap.add_argument("--base", required=True)
    args = ap.parse_args()

    data = fetch_fx(args.start_date, args.end_date, args.base)

    run_dt = datetime.now().strftime("%Y-%m-%d")
    out_path = Path(f"data/raw/fx/{run_dt}/fx_rates.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    print(f"✅ Saved raw to {out_path}")

if __name__ == "__main__":
    main()
