import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
import pandas as pd
from etl.load import initialize_schema, load_coins, load_prices, load_date_dimension

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "models" / "schema.sql"


def get_test_connection():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS price_seq START 1")
    schema_sql = SCHEMA_PATH.read_text()
    for statement in schema_sql.split(";"):
        statement = statement.strip()
        if statement:
            conn.execute(statement)
    return conn


def test_load_coins():
    conn = get_test_connection()
    coins_df = pd.DataFrame({
        "coin_id": ["bitcoin", "ethereum"],
        "symbol": ["BTC", "ETH"],
        "name": ["Bitcoin", "Ethereum"],
        "image_url": ["https://img.com/btc.png", "https://img.com/eth.png"],
        "last_updated": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
    })

    count = load_coins(conn, coins_df)
    assert count == 2

    result = conn.execute("SELECT * FROM dim_coins ORDER BY coin_id").fetchall()
    assert result[0][0] == "bitcoin"
    assert result[1][0] == "ethereum"
    conn.close()


def test_load_prices():
    conn = get_test_connection()

    coins_df = pd.DataFrame({
        "coin_id": ["bitcoin"],
        "symbol": ["BTC"],
        "name": ["Bitcoin"],
        "image_url": ["https://img.com/btc.png"],
        "last_updated": [datetime.now(timezone.utc)],
    })
    conn.execute("INSERT INTO dim_coins SELECT * FROM coins_df")

    now = datetime.now(timezone.utc)
    prices_df = pd.DataFrame({
        "coin_id": ["bitcoin"],
        "price_usd": [65000.00],
        "market_cap": [1.2e12],
        "market_cap_rank": [1],
        "total_volume_24h": [30e9],
        "price_change_24h": [2.5],
        "price_change_1h": [0.3],
        "price_change_7d": [5.1],
        "high_24h": [66000.0],
        "low_24h": [64000.0],
        "circulating_supply": [19000000.0],
        "total_supply": [21000000.0],
        "ath": [69000.0],
        "ath_change_percentage": [-5.8],
        "price_change_category": ["bull"],
        "market_cap_tier": ["large_cap"],
        "extracted_at": [now],
    })

    count = load_prices(conn, prices_df)
    assert count == 1

    result = conn.execute("SELECT price_usd FROM fact_crypto_prices").fetchone()
    assert result[0] == 65000.00
    conn.close()


def test_load_date_dimension():
    conn = get_test_connection()
    dates_df = pd.DataFrame({
        "date": [datetime(2024, 1, 1).date(), datetime(2024, 1, 2).date()],
        "day_of_week": ["Monday", "Tuesday"],
        "day_of_month": [1, 2],
        "month": [1, 1],
        "month_name": ["January", "January"],
        "quarter": [1, 1],
        "year": [2024, 2024],
        "is_weekend": [False, False],
    })

    count = load_date_dimension(conn, dates_df)
    assert count == 2
    conn.close()
