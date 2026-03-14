import time
import logging
import requests
from typing import Optional
from config.settings import (
    MARKETS_ENDPOINT,
    COIN_DETAIL_ENDPOINT,
    MARKETS_PARAMS,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRY_DELAY,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_with_retry(url: str, params: Optional[dict] = None) -> Optional[dict]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Fetching {url} (attempt {attempt}/{MAX_RETRIES})")
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            logger.info(f"Successfully fetched {url}")
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                wait_time = RETRY_DELAY * attempt
                logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                logger.error(f"HTTP error: {e}")
                raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                raise
    return None


def extract_market_data() -> list[dict]:
    logger.info("Extracting market data for top coins...")
    data = fetch_with_retry(MARKETS_ENDPOINT, params=MARKETS_PARAMS)
    if data is None:
        logger.error("Failed to extract market data after all retries")
        return []
    logger.info(f"Extracted data for {len(data)} coins")
    return data


def extract_coin_detail(coin_id: str) -> Optional[dict]:
    url = f"{COIN_DETAIL_ENDPOINT}/{coin_id}"
    params = {
        "localization": "false",
        "tickers": "false",
        "market_data": "true",
        "community_data": "false",
        "developer_data": "false",
        "sparkline": "false",
    }
    logger.info(f"Extracting detail for coin: {coin_id}")
    return fetch_with_retry(url, params=params)


def extract_historical_prices(coin_id: str, days: int = 30) -> Optional[dict]:
    url = f"{COIN_DETAIL_ENDPOINT}/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days, "interval": "daily"}
    logger.info(f"Extracting {days}-day historical prices for {coin_id}")
    return fetch_with_retry(url, params=params)


if __name__ == "__main__":
    data = extract_market_data()
    for coin in data[:5]:
        print(f"{coin['name']}: ${coin['current_price']:,.2f}")
