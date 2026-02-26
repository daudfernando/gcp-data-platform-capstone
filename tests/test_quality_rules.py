from sqlalchemy import text

def scalar(conn, q):
    return conn.execute(text(q)).scalar()

def test_stg_not_null(db_engine):
    q = """
    SELECT
      SUM(CASE WHEN fx_date IS NULL THEN 1 ELSE 0 END) +
      SUM(CASE WHEN base IS NULL THEN 1 ELSE 0 END) +
      SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) +
      SUM(CASE WHEN rate IS NULL THEN 1 ELSE 0 END) AS nulls
    FROM stg_fx_rates;
    """
    with db_engine.connect() as c:
        assert scalar(c, q) == 0

def test_stg_rate_positive(db_engine):
    q = "SELECT COUNT(*) FROM stg_fx_rates WHERE rate <= 0;"
    with db_engine.connect() as c:
        assert scalar(c, q) == 0

def test_stg_symbol_format(db_engine):
    q = "SELECT COUNT(*) FROM stg_fx_rates WHERE symbol !~ '^[A-Z]{3}$';"
    with db_engine.connect() as c:
        assert scalar(c, q) == 0

def test_no_duplicates_stg_key(db_engine):
    q = """
    SELECT COUNT(*)
    FROM (
      SELECT fx_date, base, symbol, COUNT(*) c
      FROM stg_fx_rates
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
    ) t;
    """
    with db_engine.connect() as c:
        assert scalar(c, q) == 0

def test_mart_rowcount_matches_stg(db_engine):
    q = """
    SELECT
      (SELECT COUNT(*) FROM stg_fx_rates) - (SELECT COUNT(*) FROM mart_fx_daily);
    """
    with db_engine.connect() as c:
        assert scalar(c, q) == 0
