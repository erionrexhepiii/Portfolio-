import sys
import logging
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

from etl.extract import extract_market_data, extract_historical_prices
from etl.transform import transform_market_data, transform_historical_prices, generate_date_dimension
from etl.load import get_connection, initialize_schema, load_coins, load_prices, load_historical_prices, load_date_dimension

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@task(retries=3, retry_delay_seconds=10)
def extract_task() -> list[dict]:
    return extract_market_data()


@task(retries=2, retry_delay_seconds=5)
def extract_history_task(coin_id: str, days: int = 30) -> dict:
    time.sleep(1.5)
    return extract_historical_prices(coin_id, days)


@task
def transform_market_task(raw_data: list[dict]):
    return transform_market_data(raw_data)


@task
def transform_history_task(coin_id: str, raw_data: dict):
    return transform_historical_prices(coin_id, raw_data)


@task
def generate_dates_task():
    return generate_date_dimension()


@task
def load_all_task(prices_df, coins_df, dates_df, historical_dfs):
    conn = get_connection()
    initialize_schema(conn)
    load_date_dimension(conn, dates_df)
    load_coins(conn, coins_df)
    load_prices(conn, prices_df)
    for hist_df in historical_dfs:
        if hist_df is not None and not hist_df.empty:
            load_historical_prices(conn, hist_df)
    conn.close()
    logger.info("All data loaded successfully")


@flow(name="crypto-etl-pipeline", log_prints=True)
def crypto_etl_flow(top_n_history: int = 5, history_days: int = 30):
    logger.info("Starting Crypto ETL Pipeline...")

    raw_market_data = extract_task()

    if not raw_market_data:
        logger.error("No market data extracted. Aborting pipeline.")
        return

    prices_df, coins_df = transform_market_task(raw_market_data)

    dates_df = generate_dates_task()

    top_coins = [coin["id"] for coin in raw_market_data[:top_n_history]]
    historical_dfs = []
    for coin_id in top_coins:
        raw_history = extract_history_task(coin_id, history_days)
        if raw_history:
            hist_df = transform_history_task(coin_id, raw_history)
            historical_dfs.append(hist_df)

    load_all_task(prices_df, coins_df, dates_df, historical_dfs)

    logger.info("Crypto ETL Pipeline completed successfully!")
    print(f"Loaded {len(coins_df)} coins, {len(prices_df)} price snapshots, {len(historical_dfs)} historical series")


if __name__ == "__main__":
    crypto_etl_flow()
