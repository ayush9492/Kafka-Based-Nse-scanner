"""NSE Scanner worker — consumes scan requests, fetches OHLCV via yfinance,
runs RS-new-high + buy-signal checks, publishes results back to Kafka.

Usage:
    python scanner_worker.py

Launch multiple instances in separate terminals for horizontal scaling:
    Terminal 1: python scanner_worker.py
    Terminal 2: python scanner_worker.py
    Terminal 3: python scanner_worker.py

Kafka automatically load-balances requests across instances via consumer-group
partitioning. With 8 partitions (default) you can scale up to 8 workers before
diminishing returns.

Per-message flow:
    1. Read {symbol, request_id, index_period} from nse-scanner.scan-requests
    2. Fetch OHLCV via yfinance ({SYMBOL}.NS)
    3. Fetch index OHLCV (once per worker, cached in memory for 2 min)
    4. Run RS-high check (123-day rolling relative strength)
    5. Run buy-signal check (DI diff ≥ 10, volume value > 50M, EMA alignment,
       weekly RSI ≥ 59, SMA uptrend, volume breakout)
    6. Publish {request_id, symbol, rs_high, buy_signal, details, error}
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from threading import Lock
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from kafka import KafkaConsumer, KafkaProducer

from kafka_config import (
    CONSUMER_AUTO_OFFSET_RESET,
    CONSUMER_MAX_POLL_RECORDS,
    INDEX_SYMBOL,
    KAFKA_BOOTSTRAP,
    PRODUCER_ACKS,
    SESSION_TIMEOUT_MS,
    TOPIC_SCAN_REQUESTS,
    TOPIC_SCAN_RESULTS,
    WORKER_GROUP,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scanner-worker")


# ─── Index data cache ─────────────────────────────────────────────────────────
# yfinance calls are expensive; each worker fetches the index once and reuses
# it across many per-stock scans. TTL keeps it fresh during long-running worker
# sessions.
_index_cache: Dict[str, Tuple[pd.DataFrame, float]] = {}
_index_cache_lock = Lock()
INDEX_CACHE_TTL_SEC = 120


def _fetch_index(period: str = "1y") -> Optional[pd.DataFrame]:
    """Fetch and cache index OHLCV. Returns None on failure."""
    import yfinance as yf

    now = time.time()
    with _index_cache_lock:
        cached = _index_cache.get(period)
        if cached and now - cached[1] < INDEX_CACHE_TTL_SEC:
            return cached[0]

    try:
        df = yf.Ticker(INDEX_SYMBOL).history(period=period)
        if df is None or df.empty:
            return None
        with _index_cache_lock:
            _index_cache[period] = (df, now)
        return df
    except Exception as exc:
        log.warning(f"index fetch failed ({period}): {exc}")
        return None


# ─── Scan logic (ported from app.py) ──────────────────────────────────────────
def _safe_array(series: pd.Series) -> np.ndarray:
    return np.asarray(series.values, dtype=np.float64)


def scan_single_stock(symbol: str) -> Dict[str, Any]:
    """Run RS-high + buy-signal checks for one symbol.

    Returns a dict with keys:
        symbol, rs_high (bool), buy_signal (bool), details (dict), error (str|None)
    """
    import talib
    import yfinance as yf

    result: Dict[str, Any] = {
        "symbol": symbol,
        "rs_high": False,
        "buy_signal": False,
        "details": {
            "symbol": symbol,
            "price": 0.0, "change_pct": 0.0, "volume": 0,
            "avg_volume": 0, "volume_ratio": 0.0, "rs_value": 0.0,
        },
        "error": None,
    }

    ticker = f"{symbol}.NS"
    try:
        stock = yf.Ticker(ticker)
        data_1y = stock.history(period="1y")
        data_6mo = stock.history(period="6mo")

        if data_1y.empty or data_6mo.empty:
            result["error"] = "empty OHLCV from yfinance (delisted or wrong symbol?)"
            return result

        index_data_1y = _fetch_index("1y")
        index_data_6mo = _fetch_index("6mo")
        if index_data_1y is None or index_data_6mo is None:
            result["error"] = "index data unavailable"
            return result

        # ── RS new-high check ──
        try:
            close_1y = data_1y["Close"]
            idx_close_1y = index_data_1y["Close"]
            common = close_1y.index.intersection(idx_close_1y.index)
            if len(common) >= 50:
                sc = close_1y.loc[common]
                ic = idx_close_1y.loc[common]
                # Formula preserved verbatim from original app.py
                rs = (sc * 7 * 1000) / ic
                rs_high = rs.rolling(window=123, min_periods=1).max()
                if rs.iloc[-1] > rs_high.shift(1).iloc[-1]:
                    result["rs_high"] = True
        except Exception as exc:
            log.debug(f"{symbol}: RS check error — {exc}")

        # ── Buy-signal check ──
        try:
            common_6 = data_6mo.index.intersection(index_data_6mo.index)
            df = data_6mo.loc[common_6].copy()
            if len(df) < 60:
                raise ValueError("not enough data")

            high = _safe_array(df["High"])
            low = _safe_array(df["Low"])
            close = _safe_array(df["Close"])
            volume = _safe_array(df["Volume"])

            plus_di = talib.PLUS_DI(high, low, close, timeperiod=5)
            ema10 = talib.EMA(close, timeperiod=10)
            ema20 = talib.EMA(close, timeperiod=20)
            ema50 = talib.EMA(close, timeperiod=50)
            sma50 = talib.SMA(close, timeperiod=50)
            vol_sma20 = talib.SMA(volume, timeperiod=20)

            # SMA trending up — 5 consecutive higher values
            sma_trend = (
                not np.isnan(sma50[-1]) and len(sma50) >= 5
                and sma50[-1] > sma50[-2] > sma50[-3] > sma50[-4] > sma50[-5]
            )

            # Weekly RSI
            weekly = stock.history(period="2y", interval="1wk")
            weekly_rsi_arr = talib.RSI(_safe_array(weekly["Close"]), timeperiod=14)
            last_weekly_rsi = float(weekly_rsi_arr[-1]) if len(weekly_rsi_arr) > 0 else 0.0

            di_diff = float(plus_di[-1] - plus_di[-2]) if len(plus_di) >= 2 else 0.0
            vol_value = float(close[-1]) * float(volume[-1])

            cond1 = di_diff >= 10
            cond2 = vol_value > 50_000_000
            cond3 = (
                not np.isnan(ema10[-1]) and not np.isnan(ema20[-1]) and not np.isnan(ema50[-1])
                and float(ema10[-1]) > float(ema20[-1]) > float(ema50[-1])
            )
            cond4 = last_weekly_rsi >= 59
            cond5 = bool(sma_trend)
            cond6 = (
                not np.isnan(vol_sma20[-1])
                and float(volume[-1]) > float(vol_sma20[-1])
            )

            if all([cond1, cond2, cond3, cond4, cond5, cond6]):
                result["buy_signal"] = True
        except Exception as exc:
            log.debug(f"{symbol}: buy-signal check error — {exc}")

        # ── Stock details ──
        try:
            close_1y = data_1y["Close"]
            idx_close_1y = index_data_1y["Close"]
            latest_price = float(close_1y.iloc[-1])
            prev_close = float(close_1y.iloc[-2]) if len(close_1y) > 1 else latest_price
            change_pct = ((latest_price - prev_close) / prev_close * 100) if prev_close else 0.0

            vol_series = data_1y["Volume"].tail(20)
            latest_volume = float(data_1y["Volume"].iloc[-1])
            avg_volume = float(vol_series.mean()) if not vol_series.empty else 0.0

            common = close_1y.index.intersection(idx_close_1y.index)
            if len(common) > 0:
                rs_val = float(
                    (close_1y.loc[common].iloc[-1] * 7 * 1000) / idx_close_1y.loc[common].iloc[-1]
                )
            else:
                rs_val = 0.0

            result["details"] = {
                "symbol": symbol,
                "price": round(latest_price, 2),
                "change_pct": round(change_pct, 2),
                "volume": int(latest_volume),
                "avg_volume": int(avg_volume),
                "volume_ratio": round(latest_volume / avg_volume, 2) if avg_volume else 0.0,
                "rs_value": round(rs_val, 2),
            }
        except Exception as exc:
            log.debug(f"{symbol}: details error — {exc}")

    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
    return result


# ─── Kafka loop ───────────────────────────────────────────────────────────────
def _json_ser(v: Any) -> bytes:
    return json.dumps(v, default=str).encode("utf-8")


def _json_des(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


_shutdown = False


def _handle_signal(signum, frame):  # noqa: ARG001
    global _shutdown
    log.info("shutdown signal received")
    _shutdown = True


def run_worker() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info(f"connecting to Kafka @ {KAFKA_BOOTSTRAP}")
    consumer = KafkaConsumer(
        TOPIC_SCAN_REQUESTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=WORKER_GROUP,
        auto_offset_reset=CONSUMER_AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        session_timeout_ms=SESSION_TIMEOUT_MS,
        value_deserializer=_json_des,
        max_poll_records=CONSUMER_MAX_POLL_RECORDS,
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks=PRODUCER_ACKS,
        value_serializer=_json_ser,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    log.info(f"ready — subscribed to '{TOPIC_SCAN_REQUESTS}', group '{WORKER_GROUP}'")
    processed = 0

    try:
        while not _shutdown:
            batch = consumer.poll(timeout_ms=1000)
            for tp_records in batch.values():
                if _shutdown:
                    break
                for record in tp_records:
                    if _shutdown:
                        break
                    msg = record.value
                    if not isinstance(msg, dict) or "symbol" not in msg:
                        log.warning(f"skipping malformed msg: {msg!r}")
                        continue

                    t0 = time.monotonic()
                    scan_result = scan_single_stock(msg["symbol"])
                    elapsed_ms = (time.monotonic() - t0) * 1000

                    envelope = {
                        "request_id": msg.get("request_id"),
                        "symbol": msg["symbol"],
                        "processed_at": datetime.utcnow().isoformat(),
                        "elapsed_ms": round(elapsed_ms, 1),
                        **scan_result,
                    }
                    producer.send(TOPIC_SCAN_RESULTS, key=msg["symbol"], value=envelope)

                    processed += 1
                    if scan_result["error"]:
                        tag = "✗"
                    elif scan_result["rs_high"] and scan_result["buy_signal"]:
                        tag = "★"
                    elif scan_result["rs_high"] or scan_result["buy_signal"]:
                        tag = "✓"
                    else:
                        tag = "·"
                    log.info(
                        f"{tag} {msg['symbol']:<12} "
                        f"rs={scan_result['rs_high']!s:<5} buy={scan_result['buy_signal']!s:<5} "
                        f"in {elapsed_ms:>5.0f}ms  (n={processed})"
                    )
            producer.flush(timeout=5)
    finally:
        log.info("closing consumer + producer")
        consumer.close()
        producer.close(timeout=5)
        log.info(f"worker stopped. processed {processed} message(s).")


if __name__ == "__main__":
    run_worker()
