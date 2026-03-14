import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from etl.transform import (
    classify_price_change,
    classify_market_cap,
    transform_market_data,
    transform_historical_prices,
    generate_date_dimension,
)

MOCK_RAW_DATA = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 65000.00,
        "market_cap": 1200000000000,
        "market_cap_rank": 1,
        "total_volume": 30000000000,
        "price_change_percentage_24h": 2.5,
        "high_24h": 66000.00,
        "low_24h": 64000.00,
        "circulating_supply": 19000000,
        "total_supply": 21000000,
        "ath": 69000.00,
        "ath_change_percentage": -5.8,
        "image": "https://example.com/btc.png",
        "last_updated": "2024-01-01T00:00:00.000Z",
    },
]


def test_classify_price_change():
    assert classify_price_change(6.0) == "strong_bull"
    assert classify_price_change(2.0) == "bull"
    assert classify_price_change(0.5) == "stable"
    assert classify_price_change(-2.0) == "bear"
    assert classify_price_change(-6.0) == "strong_bear"
    assert classify_price_change(None) == "unknown"


def test_classify_market_cap():
    assert classify_market_cap(50_000_000_000) == "large_cap"
    assert classify_market_cap(5_000_000_000) == "mid_cap"
    assert classify_market_cap(500_000_000) == "small_cap"
    assert classify_market_cap(50_000_000) == "micro_cap"
    assert classify_market_cap(None) == "unknown"


def test_transform_market_data():
    prices_df, coins_df = transform_market_data(MOCK_RAW_DATA)

    assert len(prices_df) == 1
    assert len(coins_df) == 1
    assert prices_df.iloc[0]["coin_id"] == "bitcoin"
    assert prices_df.iloc[0]["price_usd"] == 65000.00
    assert prices_df.iloc[0]["price_change_category"] == "bull"
    assert prices_df.iloc[0]["market_cap_tier"] == "large_cap"
    assert coins_df.iloc[0]["symbol"] == "BTC"
    assert "extracted_at" in prices_df.columns


def test_transform_historical_prices():
    raw = {
        "prices": [[1704067200000, 65000], [1704153600000, 66000]],
        "total_volumes": [[1704067200000, 30e9], [1704153600000, 31e9]],
        "market_caps": [[1704067200000, 1.2e12], [1704153600000, 1.21e12]],
    }

    df = transform_historical_prices("bitcoin", raw)
    assert len(df) == 2
    assert "coin_id" in df.columns
    assert "date" in df.columns
    assert df.iloc[0]["coin_id"] == "bitcoin"


def test_generate_date_dimension():
    df = generate_date_dimension("2024-01-01", "2024-01-31")
    assert len(df) == 31
    assert "day_of_week" in df.columns
    assert "is_weekend" in df.columns
    assert "quarter" in df.columns
