"""Seed sample trades data into the quickstart Trino instance.

Idempotent: drops and recreates `iceberg.market_data.trades` on every run.
Inserts ~30 days of synthetic trades across 3 symbols, batched for speed.
Exits 0 on success so the iceberg-ivm container can gate on it.
"""
import datetime
import os
import random
import sys
import time

import trino


HOST = os.environ.get("TRINO_HOST", "trino")
PORT = int(os.environ.get("TRINO_PORT", "8080"))
USER = os.environ.get("TRINO_USER", "seed")


def wait_for_trino(max_attempts: int = 60) -> None:
    """Poll Trino until it answers SELECT 1 or we give up."""
    for attempt in range(1, max_attempts + 1):
        try:
            conn = trino.dbapi.connect(host=HOST, port=PORT, user=USER, catalog="iceberg", schema="default")
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            conn.close()
            print(f"Trino is up after {attempt} attempt(s)")
            return
        except Exception as e:
            print(f"Waiting for Trino ({attempt}/{max_attempts}): {e}")
            time.sleep(2)
    print("Trino did not become ready in time", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    wait_for_trino()

    conn = trino.dbapi.connect(host=HOST, port=PORT, user=USER, catalog="iceberg", schema="default")
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.market_data")
    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.analytics")

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

    symbols = {"AAPL": (170.0, 2.0), "GOOG": (140.0, 1.5), "TSLA": (250.0, 5.0)}
    random.seed(42)

    days = 30
    start_day = datetime.date(2026, 4, 1)
    total = 0
    for day_offset in range(days):
        day = start_day + datetime.timedelta(days=day_offset)
        rows = []
        for symbol, (base_price, volatility) in symbols.items():
            price = base_price + day_offset * 0.5
            for hour in range(9, 16):
                # 4 trades per minute per symbol — enough to populate
                # higher-granularity views with non-trivial counts.
                for minute in range(60):
                    for _ in range(4):
                        sec = random.randint(0, 59)
                        ms = random.randint(0, 999999)
                        ts = f"{day} {hour:02d}:{minute:02d}:{sec:02d}.{ms:06d}"
                        p = round(price + random.gauss(0, volatility), 2)
                        q = round(random.uniform(10, 1000), 1)
                        rows.append(f"('{symbol}', TIMESTAMP '{ts} UTC', {p}, {q})")

        for i in range(0, len(rows), 200):
            batch = ",".join(rows[i : i + 200])
            cur.execute(f"INSERT INTO iceberg.market_data.trades VALUES {batch}")
            cur.fetchall()
        total += len(rows)
        print(f"  {day}: {len(rows)} trades (cum {total})")

    cur.execute("SELECT count(*) FROM iceberg.market_data.trades")
    final = cur.fetchone()[0]
    print(f"\nDone. Total trades: {final}")
    conn.close()


if __name__ == "__main__":
    main()
