import logging
from datetime import datetime, timezone
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def classify_price_change(change: float) -> str:
    if pd.isna(change):
        return "unknown"
    if change >= 5:
        return "strong_bull"
    if change >= 1:
        return "bull"
    if change <= -5:
        return "strong_bear"
    if change <= -1:
        return "bear"
    return "stable"


def classify_market_cap(market_cap: float) -> str:
    if pd.isna(market_cap):
        return "unknown"
    if market_cap >= 10_000_000_000:
        return "large_cap"
    if market_cap >= 1_000_000_000:
        return "mid_cap"
    if market_cap >= 100_000_000:
        return "small_cap"
    return "micro_cap"


def transform_market_data(raw_data: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"Transforming market data for {len(raw_data)} coins...")

    df = pd.DataFrame(raw_data)

    now = datetime.now(timezone.utc)

    prices_df = pd.DataFrame({
        "coin_id": df["id"],
        "price_usd": df["current_price"].astype(float),
        "market_cap": df["market_cap"].astype(float),
        "market_cap_rank": df["market_cap_rank"],
        "total_volume_24h": df["total_volume"].astype(float),
        "price_change_24h": df["price_change_percentage_24h"].astype(float),
        "price_change_1h": df.get("price_change_percentage_1h_in_currency", pd.Series([None] * len(df))),
        "price_change_7d": df.get("price_change_percentage_7d_in_currency", pd.Series([None] * len(df))),
        "high_24h": df["high_24h"].astype(float),
        "low_24h": df["low_24h"].astype(float),
        "circulating_supply": df["circulating_supply"],
        "total_supply": df["total_supply"],
        "ath": df["ath"].astype(float),
        "ath_change_percentage": df["ath_change_percentage"].astype(float),
        "price_change_category": df["price_change_percentage_24h"].apply(classify_price_change),
        "market_cap_tier": df["market_cap"].apply(classify_market_cap),
        "extracted_at": now,
    })

    coins_df = pd.DataFrame({
        "coin_id": df["id"],
        "symbol": df["symbol"].str.upper(),
        "name": df["name"],
        "image_url": df["image"],
        "last_updated": pd.to_datetime(df["last_updated"]),
    })

    prices_df = prices_df.drop_duplicates(subset=["coin_id", "extracted_at"])
    coins_df = coins_df.drop_duplicates(subset=["coin_id"])

    logger.info(f"Transformed {len(prices_df)} price records and {len(coins_df)} coin records")

    return prices_df, coins_df


def transform_historical_prices(coin_id: str, raw_data: dict) -> pd.DataFrame:
    logger.info(f"Transforming historical prices for {coin_id}...")

    prices = raw_data.get("prices", [])
    volumes = raw_data.get("total_volumes", [])
    market_caps = raw_data.get("market_caps", [])

    df = pd.DataFrame(prices, columns=["timestamp_ms", "price_usd"])
    df["date"] = pd.to_datetime(df["timestamp_ms"], unit="ms").dt.date
    df["coin_id"] = coin_id

    if volumes:
        vol_df = pd.DataFrame(volumes, columns=["timestamp_ms", "total_volume"])
        df["total_volume"] = vol_df["total_volume"]

    if market_caps:
        mc_df = pd.DataFrame(market_caps, columns=["timestamp_ms", "market_cap"])
        df["market_cap"] = mc_df["market_cap"]

    df = df.drop(columns=["timestamp_ms"])
    df = df.drop_duplicates(subset=["coin_id", "date"])

    logger.info(f"Transformed {len(df)} historical records for {coin_id}")
    return df


def generate_date_dimension(start_date: str = "2020-01-01", end_date: str = None) -> pd.DataFrame:
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    df = pd.DataFrame({
        "date": dates.date,
        "day_of_week": dates.day_name(),
        "day_of_month": dates.day,
        "month": dates.month,
        "month_name": dates.month_name(),
        "quarter": dates.quarter,
        "year": dates.year,
        "is_weekend": dates.dayofweek.isin([5, 6]),
    })

    logger.info(f"Generated date dimension with {len(df)} records")
    return df
