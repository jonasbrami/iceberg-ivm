"""Seed sample trades data and create example config."""
import random
import datetime
from pathlib import Path

import trino


def main():
    conn = trino.dbapi.connect(
        host="localhost", port=18080, catalog="iceberg", schema="default", user="demo"
    )
    cur = conn.cursor()

    # Create schemas
    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.market_data")
    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.analytics")

    # Create trades table
    cur.execute("DROP TABLE IF EXISTS iceberg.market_data.trades")
    cur.execute("""
        CREATE TABLE iceberg.market_data.trades (
            symbol VARCHAR,
            ts TIMESTAMP(6) WITH TIME ZONE,
            price DOUBLE,
            quantity DOUBLE
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['day(ts)']
        )
    """)

    # Generate 3 days of trades for 3 symbols (~1 trade per minute per symbol)
    symbols = {"AAPL": (170.0, 2.0), "GOOG": (140.0, 1.5), "TSLA": (250.0, 5.0)}
    random.seed(42)

    for day_offset in range(3):
        day = datetime.date(2026, 4, 8) + datetime.timedelta(days=day_offset)
        rows = []
        for symbol, (base_price, volatility) in symbols.items():
            price = base_price + day_offset * 0.5
            for hour in range(9, 16):  # market hours
                for minute in range(60):
                    sec = random.randint(0, 59)
                    ms = random.randint(0, 999999)
                    ts = f"{day} {hour:02d}:{minute:02d}:{sec:02d}.{ms:06d}"
                    p = round(price + random.gauss(0, volatility), 2)
                    q = round(random.uniform(10, 1000), 1)
                    rows.append(f"('{symbol}', TIMESTAMP '{ts} UTC', {p}, {q})")

        # Insert in batches
        for i in range(0, len(rows), 200):
            batch = ",".join(rows[i : i + 200])
            cur.execute(f"INSERT INTO iceberg.market_data.trades VALUES {batch}")
        print(f"  {day}: {len(rows)} trades")

    # Verify
    cur.execute("SELECT count(*) FROM iceberg.market_data.trades")
    total = cur.fetchone()[0]
    print(f"\nTotal: {total} trades")

    cur.execute("""
        SELECT CAST(ts AS DATE) AS day, symbol, count(*)
        FROM iceberg.market_data.trades GROUP BY 1, 2 ORDER BY 1, 2
    """)
    for row in cur.fetchall():
        print(f"  {row[0]} {row[1]}: {row[2]}")

    # Write example config pointing at the test Trino
    config = Path("config.yaml")
    config.write_text("""\
server:
  port: 8000
  config_reload_interval_seconds: 30

trino:
  host: localhost
  port: 18080
  catalog: iceberg
  schema: analytics
  user: demo

views:
  - name: ohlcv_1m
    query: |
      SELECT
        symbol,
        date_trunc('minute', ts) AS minute,
        min_by(price, ts) AS open,
        max(price)        AS high,
        min(price)        AS low,
        max_by(price, ts) AS close,
        sum(quantity)     AS volume,
        count(*)          AS trade_count
      FROM iceberg.market_data.trades
      GROUP BY symbol, date_trunc('minute', ts)
    target_table: iceberg.analytics.ohlcv_1m
    target_partitioning: "ARRAY['day(minute)']"
    refresh_interval_seconds: 30
""")
    print(f"\nWrote {config}")
    print("\nRun:  uv run trino-mv-orchestrator -c config.yaml")
    print("Open: http://localhost:8000")

    conn.close()


if __name__ == "__main__":
    main()
