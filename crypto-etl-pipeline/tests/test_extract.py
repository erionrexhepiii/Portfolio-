import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.extract import extract_market_data, fetch_with_retry

MOCK_MARKET_RESPONSE = [
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
    {
        "id": "ethereum",
        "symbol": "eth",
        "name": "Ethereum",
        "current_price": 3500.00,
        "market_cap": 420000000000,
        "market_cap_rank": 2,
        "total_volume": 15000000000,
        "price_change_percentage_24h": -1.2,
        "high_24h": 3600.00,
        "low_24h": 3400.00,
        "circulating_supply": 120000000,
        "total_supply": None,
        "ath": 4800.00,
        "ath_change_percentage": -27.1,
        "image": "https://example.com/eth.png",
        "last_updated": "2024-01-01T00:00:00.000Z",
    },
]


@patch("etl.extract.requests.get")
def test_extract_market_data_success(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = MOCK_MARKET_RESPONSE
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = extract_market_data()

    assert len(result) == 2
    assert result[0]["id"] == "bitcoin"
    assert result[1]["id"] == "ethereum"
    assert result[0]["current_price"] == 65000.00


@patch("etl.extract.requests.get")
def test_extract_market_data_empty(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = extract_market_data()
    assert result == []


@patch("etl.extract.requests.get")
def test_fetch_with_retry_success(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "test"}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = fetch_with_retry("https://example.com/api")
    assert result == {"data": "test"}
    assert mock_get.call_count == 1
