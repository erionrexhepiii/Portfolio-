-- Top 10 Coins by Market Cap
SELECT
    c.name,
    c.symbol,
    p.price_usd,
    p.market_cap,
    p.market_cap_rank,
    p.price_change_24h,
    p.market_cap_tier
FROM fact_crypto_prices p
JOIN dim_coins c ON p.coin_id = c.coin_id
WHERE p.extracted_at = (SELECT MAX(extracted_at) FROM fact_crypto_prices)
ORDER BY p.market_cap_rank ASC
LIMIT 10;

-- Biggest 24h Gainers
SELECT
    c.name,
    c.symbol,
    p.price_usd,
    p.price_change_24h,
    p.price_change_category
FROM fact_crypto_prices p
JOIN dim_coins c ON p.coin_id = c.coin_id
WHERE p.extracted_at = (SELECT MAX(extracted_at) FROM fact_crypto_prices)
ORDER BY p.price_change_24h DESC
LIMIT 5;

-- Biggest 24h Losers
SELECT
    c.name,
    c.symbol,
    p.price_usd,
    p.price_change_24h,
    p.price_change_category
FROM fact_crypto_prices p
JOIN dim_coins c ON p.coin_id = c.coin_id
WHERE p.extracted_at = (SELECT MAX(extracted_at) FROM fact_crypto_prices)
ORDER BY p.price_change_24h ASC
LIMIT 5;

-- Market Cap Distribution by Tier
SELECT
    market_cap_tier,
    COUNT(*) as coin_count,
    ROUND(SUM(market_cap) / 1e9, 2) as total_market_cap_billions,
    ROUND(AVG(price_change_24h), 2) as avg_24h_change
FROM fact_crypto_prices
WHERE extracted_at = (SELECT MAX(extracted_at) FROM fact_crypto_prices)
GROUP BY market_cap_tier
ORDER BY total_market_cap_billions DESC;

-- Volume Leaders (Top 10 by 24h Trading Volume)
SELECT
    c.name,
    c.symbol,
    ROUND(p.total_volume_24h / 1e9, 2) as volume_24h_billions,
    p.price_usd,
    p.price_change_24h
FROM fact_crypto_prices p
JOIN dim_coins c ON p.coin_id = c.coin_id
WHERE p.extracted_at = (SELECT MAX(extracted_at) FROM fact_crypto_prices)
ORDER BY p.total_volume_24h DESC
LIMIT 10;

-- Historical Price Trend (30-day average for top coins)
SELECT
    h.coin_id,
    c.name,
    ROUND(AVG(h.price_usd), 2) as avg_price,
    ROUND(MIN(h.price_usd), 2) as min_price,
    ROUND(MAX(h.price_usd), 2) as max_price,
    ROUND(MAX(h.price_usd) - MIN(h.price_usd), 2) as price_range,
    COUNT(*) as data_points
FROM fact_historical_prices h
JOIN dim_coins c ON h.coin_id = c.coin_id
GROUP BY h.coin_id, c.name
ORDER BY avg_price DESC;

-- ATH Distance (How far coins are from All-Time High)
SELECT
    c.name,
    c.symbol,
    p.price_usd as current_price,
    p.ath as all_time_high,
    ROUND(p.ath_change_percentage, 2) as pct_from_ath
FROM fact_crypto_prices p
JOIN dim_coins c ON p.coin_id = c.coin_id
WHERE p.extracted_at = (SELECT MAX(extracted_at) FROM fact_crypto_prices)
ORDER BY p.ath_change_percentage DESC
LIMIT 10;
