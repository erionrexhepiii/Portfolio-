# Crypto ETL Pipeline

An end-to-end data engineering pipeline that extracts cryptocurrency market data from the CoinGecko API, transforms it into a star schema, loads it into DuckDB, and generates an analytics dashboard.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────┐     ┌───────────┐
│  CoinGecko  │────>│   Extract    │────>│Transform│────>│   Load    │
│    API      │     │  (requests)  │     │(pandas) │     │ (DuckDB)  │
└─────────────┘     └──────────────┘     └─────────┘     └─────┬─────┘
                                                               │
                         ┌─────────────┐     ┌─────────────────┘
                         │  Dashboard   │<────│
                         │   (HTML)     │     │  Analytics
                         └─────────────┘     │  Queries (SQL)
                                             └─────────────────
                    Orchestrated by Prefect
```

## Tech Stack

| Layer          | Technology                |
|----------------|---------------------------|
| Extraction     | Python, Requests          |
| Transformation | Pandas                    |
| Storage        | DuckDB (analytical OLAP)  |
| Orchestration  | Prefect                   |
| Analytics      | SQL                       |
| Dashboard      | HTML/CSS (generated)      |
| Containerization| Docker, docker-compose   |
| Testing        | Pytest                    |

## Data Model (Star Schema)

**Fact Tables:**
- `fact_crypto_prices` — real-time price snapshots with market metrics
- `fact_historical_prices` — daily historical prices per coin

**Dimension Tables:**
- `dim_coins` — coin metadata (name, symbol, image)
- `dim_dates` — date dimension for time-based analytics

## Quick Start

### Prerequisites
- Python 3.10+
- pip

### Setup

```bash
git clone <repo-url>
cd crypto-etl-pipeline

python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

pip install -r requirements.txt
```

### Run the Pipeline

```bash
python orchestration/flows.py
```

### Generate Dashboard

```bash
python dashboard/generate_dashboard.py
```

Then open `dashboard/index.html` in your browser.

### Run Tests

```bash
pytest tests/ -v
```

### Docker

```bash
docker-compose up
```

Run tests in Docker:
```bash
docker-compose --profile test run tests
```

## Project Structure

```
crypto-etl-pipeline/
├── config/settings.py          — Configuration and API settings
├── etl/
│   ├── extract.py              — API data extraction with retry logic
│   ├── transform.py            — Data cleaning and enrichment
│   └── load.py                 — DuckDB loading with schema management
├── models/schema.sql           — Star schema DDL
├── orchestration/flows.py      — Prefect flow orchestration
├── analytics/queries.sql       — Analytical SQL queries
├── dashboard/
│   ├── generate_dashboard.py   — Query DuckDB and generate HTML report
│   ├── style.css               — Dashboard styling
│   └── index.html              — Generated analytics dashboard
├── tests/                      — Unit tests for ETL modules
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Key Features

- **Retry Logic** — Automatic retries with exponential backoff for API calls
- **Rate Limiting** — Respects CoinGecko API limits with built-in delays
- **Star Schema** — Proper dimensional modeling for analytical queries
- **Data Quality** — Deduplication, type casting, null handling, computed fields
- **Orchestration** — Prefect flows with task-level retries and logging
- **Containerized** — Docker setup for reproducible execution
- **Tested** — Unit tests for extract, transform, and load modules

## Author

Erion Rexhepi — Data Engineering Intern
