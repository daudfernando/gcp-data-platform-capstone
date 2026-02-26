import os
import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine

@pytest.fixture(scope="session")
def db_engine():
    load_dotenv()
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db   = os.getenv("DB_NAME", "capstone_db")
    user = os.getenv("DB_USER", "capstone")
    pwd  = os.getenv("DB_PASSWORD", "capstone")
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)
