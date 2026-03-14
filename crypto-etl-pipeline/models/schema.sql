CREATE TABLE IF NOT EXISTS dim_coins (
    coin_id VARCHAR PRIMARY KEY,
    symbol VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    image_url VARCHAR,
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date DATE PRIMARY KEY,
    day_of_week VARCHAR,
    day_of_month INTEGER,
    month INTEGER,
    month_name VARCHAR,
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_crypto_prices (
    id INTEGER PRIMARY KEY DEFAULT(nextval('price_seq')),
    coin_id VARCHAR NOT NULL REFERENCES dim_coins(coin_id),
    price_usd DOUBLE NOT NULL,
    market_cap DOUBLE,
    market_cap_rank INTEGER,
    total_volume_24h DOUBLE,
    price_change_24h DOUBLE,
    price_change_1h DOUBLE,
    price_change_7d DOUBLE,
    high_24h DOUBLE,
    low_24h DOUBLE,
    circulating_supply DOUBLE,
    total_supply DOUBLE,
    ath DOUBLE,
    ath_change_percentage DOUBLE,
    price_change_category VARCHAR,
    market_cap_tier VARCHAR,
    extracted_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_historical_prices (
    coin_id VARCHAR NOT NULL REFERENCES dim_coins(coin_id),
    date DATE NOT NULL,
    price_usd DOUBLE NOT NULL,
    total_volume DOUBLE,
    market_cap DOUBLE,
    PRIMARY KEY (coin_id, date)
);
