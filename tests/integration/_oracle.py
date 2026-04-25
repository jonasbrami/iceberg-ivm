"""In-memory replica of the OHLCV materialized view.

Mirrors the SQL defined by VIEW_QUERY in test_streaming_refresh.py:

    SELECT symbol, date_trunc('minute', ts),
           min_by(price, ts), max(price), min(price), max_by(price, ts),
           sum(quantity), count(*)
    FROM trades GROUP BY 1, 2

The oracle ingests trades one-by-one as the test inserts them, then
emits a sorted ``list[dict]`` matching the target's ``SELECT * ORDER BY
symbol, minute`` so equality compares row-for-row.

Determinism: scenarios never share ``(symbol, ts)`` between two trades,
so ``min_by(price, ts)`` / ``max_by(price, ts)`` have no ties to break.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class _Bar:
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    count: int = 0
    open_ts: datetime | None = None
    close_ts: datetime | None = None


def _floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


class OhlcvOracle:
    def __init__(self) -> None:
        self._bars: dict[tuple[str, datetime], _Bar] = {}

    def update(self, symbol: str, ts: datetime, price: float, qty: float) -> None:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        key = (symbol, _floor_minute(ts))
        b = self._bars.get(key)
        if b is None:
            b = _Bar(open=price, high=price, low=price, close=price,
                     volume=0.0, count=0, open_ts=ts, close_ts=ts)
            self._bars[key] = b
        if ts < b.open_ts:
            b.open = price
            b.open_ts = ts
        if ts > b.close_ts:
            b.close = price
            b.close_ts = ts
        if price > b.high:
            b.high = price
        if price < b.low:
            b.low = price
        b.volume += qty
        b.count += 1

    def expected_rows(self) -> list[dict]:
        rows = []
        for (symbol, minute), b in self._bars.items():
            rows.append({
                "symbol": symbol,
                "minute": minute,
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume,
                "trade_count": b.count,
            })
        rows.sort(key=lambda r: (r["symbol"], r["minute"]))
        return rows
