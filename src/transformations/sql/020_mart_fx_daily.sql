INSERT INTO mart_fx_daily (fx_date, base, symbol, rate, change_1d, change_7d)
SELECT
  fx_date,
  base,
  symbol,
  rate,
  rate - LAG(rate, 1) OVER (PARTITION BY base, symbol ORDER BY fx_date) AS change_1d,
  rate - LAG(rate, 7) OVER (PARTITION BY base, symbol ORDER BY fx_date) AS change_7d
FROM stg_fx_rates
ON CONFLICT (fx_date, base, symbol)
DO UPDATE SET
  rate = EXCLUDED.rate,
  change_1d = EXCLUDED.change_1d,
  change_7d = EXCLUDED.change_7d,
  load_ts = NOW();
