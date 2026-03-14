import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / os.getenv("DB_PATH", "data/crypto.duckdb")
COINGECKO_BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
TOP_N_COINS = int(os.getenv("TOP_N_COINS", 20))
FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", 3600))

MARKETS_ENDPOINT = f"{COINGECKO_BASE_URL}/coins/markets"
COIN_DETAIL_ENDPOINT = f"{COINGECKO_BASE_URL}/coins"

MARKETS_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": TOP_N_COINS,
    "page": 1,
    "sparkline": False,
    "price_change_percentage": "1h,24h,7d",
}

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 5
