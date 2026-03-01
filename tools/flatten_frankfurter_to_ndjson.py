import json
from pathlib import Path
from datetime import datetime, timezone
import argparse

def latest_raw_file() -> Path:
    candidates = list(Path("data/raw/fx").glob("**/fx_rates.json"))
    if not candidates:
        raise FileNotFoundError("Tidak ada file raw: data/raw/fx/**/fx_rates.json")
    return max(candidates, key=lambda p: p.stat().st_mtime)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=None, help="Path fx_rates.json. Jika kosong, pakai yang terbaru.")
    ap.add_argument("--output", default=None, help="Path output .ndjson. Jika kosong, auto.")
    args = ap.parse_args()

    inp = Path(args.input) if args.input else latest_raw_file()
    payload = json.loads(inp.read_text(encoding="utf-8"))

    base = payload.get("base")
    rates = payload.get("rates", {})
    if not base or not isinstance(rates, dict) or not rates:
        raise ValueError("Format JSON tidak sesuai (base/rates kosong).")

    load_ts = datetime.now(timezone.utc).isoformat()
    lines = []

    # rates: { "YYYY-MM-DD": { "USD": 1.18, ... }, ... }
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

    out = Path(args.output) if args.output else Path(
        f"data/processed/bq/fx_rates_flat_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.ndjson"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"✅ input : {inp}")
    print(f"✅ output: {out}")
    print(f"✅ rows  : {len(lines)}")

if __name__ == "__main__":
    main()
