import logging
from pathlib import Path
import duckdb
import pandas as pd
from config.settings import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "models" / "schema.sql"


def get_connection() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))
    return conn


def initialize_schema(conn: duckdb.DuckDBPyConnection) -> None:
    logger.info("Initializing database schema...")
    try:
        conn.execute("CREATE SEQUENCE IF NOT EXISTS price_seq START 1")
    except Exception:
        pass
    schema_sql = SCHEMA_PATH.read_text()
    for statement in schema_sql.split(";"):
        statement = statement.strip()
        if statement:
            conn.execute(statement)
    logger.info("Schema initialized successfully")


def load_coins(conn: duckdb.DuckDBPyConnection, coins_df: pd.DataFrame) -> int:
    logger.info(f"Loading {len(coins_df)} coins into dim_coins...")
    conn.execute("DELETE FROM dim_coins WHERE coin_id IN (SELECT coin_id FROM coins_df)")
    conn.execute("INSERT INTO dim_coins SELECT * FROM coins_df")
    count = conn.execute("SELECT COUNT(*) FROM dim_coins").fetchone()[0]
    logger.info(f"dim_coins now has {count} records")
    return count


def load_prices(conn: duckdb.DuckDBPyConnection, prices_df: pd.DataFrame) -> int:
    logger.info(f"Loading {len(prices_df)} price records into fact_crypto_prices...")
    prices_to_load = prices_df.drop(columns=["market_cap_tier", "price_change_category"], errors="ignore")
    prices_to_load = prices_df.copy()
    conn.execute("""
        INSERT INTO fact_crypto_prices (
            coin_id, price_usd, market_cap, market_cap_rank, total_volume_24h,
            price_change_24h, price_change_1h, price_change_7d,
            high_24h, low_24h, circulating_supply, total_supply,
            ath, ath_change_percentage, price_change_category, market_cap_tier, extracted_at
        )
        SELECT
            coin_id, price_usd, market_cap, market_cap_rank, total_volume_24h,
            price_change_24h, price_change_1h, price_change_7d,
            high_24h, low_24h, circulating_supply, total_supply,
            ath, ath_change_percentage, price_change_category, market_cap_tier, extracted_at
        FROM prices_to_load
    """)
    count = conn.execute("SELECT COUNT(*) FROM fact_crypto_prices").fetchone()[0]
    logger.info(f"fact_crypto_prices now has {count} records")
    return count


def load_historical_prices(conn: duckdb.DuckDBPyConnection, hist_df: pd.DataFrame) -> int:
    logger.info(f"Loading {len(hist_df)} historical price records...")
    conn.execute("""
        INSERT OR REPLACE INTO fact_historical_prices (coin_id, date, price_usd, total_volume, market_cap)
        SELECT coin_id, date, price_usd, total_volume, market_cap FROM hist_df
    """)
    count = conn.execute("SELECT COUNT(*) FROM fact_historical_prices").fetchone()[0]
    logger.info(f"fact_historical_prices now has {count} records")
    return count


def load_date_dimension(conn: duckdb.DuckDBPyConnection, dates_df: pd.DataFrame) -> int:
    logger.info(f"Loading {len(dates_df)} date records into dim_dates...")
    conn.execute("DELETE FROM dim_dates")
    conn.execute("INSERT INTO dim_dates SELECT * FROM dates_df")
    count = conn.execute("SELECT COUNT(*) FROM dim_dates").fetchone()[0]
    logger.info(f"dim_dates now has {count} records")
    return count
