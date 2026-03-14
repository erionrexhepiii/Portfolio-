import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
from config.settings import DB_PATH

DASHBOARD_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = DASHBOARD_DIR / "index.html"


def query_db(query: str) -> list[tuple]:
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    conn.close()
    return columns, result


def format_number(n) -> str:
    if n is None:
        return "N/A"
    if isinstance(n, float):
        if abs(n) >= 1e9:
            return f"${n/1e9:,.2f}B"
        if abs(n) >= 1e6:
            return f"${n/1e6:,.2f}M"
        if abs(n) >= 1:
            return f"${n:,.2f}"
        return f"${n:,.6f}"
    return str(n)


def format_pct(n) -> str:
    if n is None:
        return "N/A"
    color = "#00d4aa" if n >= 0 else "#ff4757"
    arrow = "&uarr;" if n >= 0 else "&darr;"
    return f'<span style="color:{color}">{arrow} {n:+.2f}%</span>'


def build_table(columns, rows, formatters=None) -> str:
    html = '<table><thead><tr>'
    for col in columns:
        html += f'<th>{col.replace("_", " ").title()}</th>'
    html += '</tr></thead><tbody>'
    for row in rows:
        html += '<tr>'
        for i, val in enumerate(row):
            if formatters and i in formatters:
                html += f'<td>{formatters[i](val)}</td>'
            else:
                html += f'<td>{val}</td>'
        html += '</tr>'
    html += '</tbody></table>'
    return html


def generate():
    print("Generating dashboard...")

    cols1, top_coins = query_db("""
        SELECT c.name, c.symbol, p.price_usd, p.market_cap, p.price_change_24h, p.market_cap_tier
        FROM fact_crypto_prices p
        JOIN dim_coins c ON p.coin_id = c.coin_id
        WHERE p.extracted_at = (SELECT MAX(extracted_at) FROM fact_crypto_prices)
        ORDER BY p.market_cap_rank ASC
        LIMIT 10
    """)

    cols2, gainers = query_db("""
        SELECT c.name, c.symbol, p.price_usd, p.price_change_24h
        FROM fact_crypto_prices p
        JOIN dim_coins c ON p.coin_id = c.coin_id
        WHERE p.extracted_at = (SELECT MAX(extracted_at) FROM fact_crypto_prices)
        ORDER BY p.price_change_24h DESC
        LIMIT 5
    """)

    cols3, losers = query_db("""
        SELECT c.name, c.symbol, p.price_usd, p.price_change_24h
        FROM fact_crypto_prices p
        JOIN dim_coins c ON p.coin_id = c.coin_id
        WHERE p.extracted_at = (SELECT MAX(extracted_at) FROM fact_crypto_prices)
        ORDER BY p.price_change_24h ASC
        LIMIT 5
    """)

    _, tier_data = query_db("""
        SELECT market_cap_tier, COUNT(*), ROUND(SUM(market_cap)/1e9, 2), ROUND(AVG(price_change_24h), 2)
        FROM fact_crypto_prices
        WHERE extracted_at = (SELECT MAX(extracted_at) FROM fact_crypto_prices)
        GROUP BY market_cap_tier
        ORDER BY 3 DESC
    """)

    _, summary = query_db("""
        SELECT
            COUNT(*) as total_coins,
            ROUND(SUM(market_cap)/1e9, 2) as total_mcap_b,
            ROUND(SUM(total_volume_24h)/1e9, 2) as total_vol_b,
            ROUND(AVG(price_change_24h), 2) as avg_change
        FROM fact_crypto_prices
        WHERE extracted_at = (SELECT MAX(extracted_at) FROM fact_crypto_prices)
    """)

    _, last_run = query_db("SELECT MAX(extracted_at) FROM fact_crypto_prices")

    total_coins, total_mcap, total_vol, avg_change = summary[0]
    last_updated = last_run[0][0] if last_run else "N/A"

    top_coins_table = build_table(
        ["Name", "Symbol", "Price", "Market Cap", "24h Change", "Tier"],
        top_coins,
        {2: format_number, 3: format_number, 4: format_pct}
    )

    gainers_table = build_table(
        ["Name", "Symbol", "Price", "24h Change"],
        gainers,
        {2: format_number, 3: format_pct}
    )

    losers_table = build_table(
        ["Name", "Symbol", "Price", "24h Change"],
        losers,
        {2: format_number, 3: format_pct}
    )

    tier_table = build_table(
        ["Tier", "Count", "Total MCap ($B)", "Avg 24h Change"],
        tier_data,
        {3: format_pct}
    )

    style_path = DASHBOARD_DIR / "style.css"
    style_content = style_path.read_text() if style_path.exists() else ""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Crypto ETL Dashboard</title>
    <style>{style_content}</style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Crypto Market Dashboard</h1>
            <p class="subtitle">Data Pipeline: CoinGecko API &rarr; Python ETL &rarr; DuckDB &rarr; Analytics</p>
            <p class="last-updated">Last updated: {last_updated}</p>
        </header>

        <div class="summary-cards">
            <div class="card">
                <div class="card-label">Coins Tracked</div>
                <div class="card-value">{total_coins}</div>
            </div>
            <div class="card">
                <div class="card-label">Total Market Cap</div>
                <div class="card-value">${total_mcap}B</div>
            </div>
            <div class="card">
                <div class="card-label">24h Volume</div>
                <div class="card-value">${total_vol}B</div>
            </div>
            <div class="card">
                <div class="card-label">Avg 24h Change</div>
                <div class="card-value">{format_pct(avg_change)}</div>
            </div>
        </div>

        <section>
            <h2>Top 10 by Market Cap</h2>
            {top_coins_table}
        </section>

        <div class="two-col">
            <section>
                <h2>Biggest Gainers (24h)</h2>
                {gainers_table}
            </section>
            <section>
                <h2>Biggest Losers (24h)</h2>
                {losers_table}
            </section>
        </div>

        <section>
            <h2>Market Cap Distribution</h2>
            {tier_table}
        </section>

        <footer>
            <p>Built by Erion Rexhepi | Data Engineering Portfolio Project</p>
            <p>Stack: Python &middot; Prefect &middot; DuckDB &middot; CoinGecko API</p>
        </footer>
    </div>
</body>
</html>"""

    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print(f"Dashboard generated at: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate()
