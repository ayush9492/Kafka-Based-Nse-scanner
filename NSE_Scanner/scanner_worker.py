"""NSE/USA Scanner worker — consumes scan requests, fetches OHLCV via yfinance,
runs RS-new-high + buy-signal checks, publishes results back to Kafka.

Optimizations vs naive version:
  - 1 yfinance call per stock (was 3): fetch 2y daily, derive 1y/6mo/weekly by slicing/resampling
  - Retry with exponential backoff on empty/rate-limited responses
  - Configurable inter-stock delay to stay under Yahoo rate limits
  - Index data cached per (symbol, period) with 2-min TTL

Usage:
    python scanner_worker.py
    # Launch up to DEFAULT_PARTITIONS instances for full parallelism
"""
from __future__ import annotations

import json
import logging
import signal
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
    INDEX_SYMBOL_USA,
    INTER_STOCK_DELAY_SEC,
    KAFKA_BOOTSTRAP,
    PRODUCER_ACKS,
    SESSION_TIMEOUT_MS,
    TOPIC_SCAN_REQUESTS,
    TOPIC_SCAN_RESULTS,
    VOL_THRESHOLD_INDIA,
    VOL_THRESHOLD_USA,
    WORKER_GROUP,
    YFINANCE_RETRIES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scanner-worker")


# ─── Index cache ──────────────────────────────────────────────────────────────
_index_cache: Dict[str, Tuple[pd.DataFrame, float]] = {}
_index_cache_lock = Lock()
INDEX_CACHE_TTL_SEC = 120


def _fetch_index(index_sym: str, period: str = "2y") -> Optional[pd.DataFrame]:
    """Fetch and cache index OHLCV. Returns None on failure."""
    import yfinance as yf

    key = f"{index_sym}:{period}"
    now = time.time()
    with _index_cache_lock:
        cached = _index_cache.get(key)
        if cached and now - cached[1] < INDEX_CACHE_TTL_SEC:
            return cached[0]

    for attempt in range(YFINANCE_RETRIES):
        try:
            df = yf.Ticker(index_sym).history(period=period)
            if df is not None and not df.empty:
                with _index_cache_lock:
                    _index_cache[key] = (df, time.time())
                return df
        except Exception as exc:
            log.warning(f"index fetch attempt {attempt+1} failed ({index_sym}): {exc}")
        time.sleep(2 ** attempt)

    return None


def _fetch_stock(ticker: str) -> Optional[pd.DataFrame]:
    """Fetch 2y daily OHLCV for a stock with retry. Returns None on failure."""
    import yfinance as yf

    for attempt in range(YFINANCE_RETRIES):
        try:
            df = yf.Ticker(ticker).history(period="2y")
            if df is not None and not df.empty:
                return df
        except Exception as exc:
            log.debug(f"stock fetch attempt {attempt+1} failed ({ticker}): {exc}")
        time.sleep(2 ** attempt)

    return None


# ─── Scan logic ───────────────────────────────────────────────────────────────
def _safe_array(series: pd.Series) -> np.ndarray:
    return np.asarray(series.values, dtype=np.float64)


def scan_single_stock(symbol: str, market: str = "india") -> Dict[str, Any]:
    """Run RS-high + buy-signal checks for one symbol.

    Uses a single 2y yfinance fetch; derives 1y/6mo/weekly by slicing/resampling.
    """
    import talib

    ticker     = f"{symbol}.NS" if market == "india" else symbol
    index_sym  = INDEX_SYMBOL if market == "india" else INDEX_SYMBOL_USA
    vol_threshold = VOL_THRESHOLD_INDIA if market == "india" else VOL_THRESHOLD_USA

    result: Dict[str, Any] = {
        "symbol": symbol,
        "market": market,
        "rs_high": False,
        "buy_signal": False,
        "details": {
            "symbol": symbol,
            "price": 0.0, "change_pct": 0.0, "volume": 0,
            "avg_volume": 0, "volume_ratio": 0.0, "rs_value": 0.0,
        },
        "error": None,
    }

    try:
        # ── Single fetch: 2y daily ──────────────────────────────────────────
        data_2y = _fetch_stock(ticker)
        if data_2y is None or data_2y.empty:
            result["error"] = "empty OHLCV (rate-limited, delisted, or wrong symbol)"
            return result

        idx_2y = _fetch_index(index_sym, "2y")
        if idx_2y is None:
            result["error"] = "index data unavailable"
            return result

        # Derive shorter windows from 2y data
        n = len(data_2y)
        data_1y  = data_2y.iloc[max(0, n - 252):]
        data_6mo = data_2y.iloc[max(0, n - 126):]

        n_idx = len(idx_2y)
        idx_1y = idx_2y.iloc[max(0, n_idx - 252):]

        # Weekly close via resampling (avoids a separate API call)
        weekly_close = data_2y["Close"].resample("W").last().dropna()

        # ── RS new-high check ─────────────────────────────────────────────
        try:
            close_1y     = data_1y["Close"]
            idx_close_1y = idx_1y["Close"]
            common = close_1y.index.intersection(idx_close_1y.index)
            if len(common) >= 50:
                sc = close_1y.loc[common]
                ic = idx_close_1y.loc[common]
                rs = (sc * 7 * 1000) / ic
                rs_high = rs.rolling(window=123, min_periods=1).max()
                if rs.iloc[-1] > rs_high.shift(1).iloc[-1]:
                    result["rs_high"] = True
        except Exception as exc:
            log.debug(f"{symbol}: RS check — {exc}")

        # ── Buy-signal check ──────────────────────────────────────────────
        try:
            idx_6mo = idx_2y.iloc[max(0, n_idx - 126):]
            common_6 = data_6mo.index.intersection(idx_6mo.index)
            df6 = data_6mo.loc[common_6].copy()
            if len(df6) < 60:
                raise ValueError("not enough 6mo data")

            high   = _safe_array(df6["High"])
            low    = _safe_array(df6["Low"])
            close  = _safe_array(df6["Close"])
            volume = _safe_array(df6["Volume"])

            plus_di  = talib.PLUS_DI(high, low, close, timeperiod=5)
            ema10    = talib.EMA(close, timeperiod=10)
            ema20    = talib.EMA(close, timeperiod=20)
            ema50    = talib.EMA(close, timeperiod=50)
            sma50    = talib.SMA(close, timeperiod=50)
            vol_sma20 = talib.SMA(volume, timeperiod=20)

            sma_trend = (
                not np.isnan(sma50[-1]) and len(sma50) >= 5
                and sma50[-1] > sma50[-2] > sma50[-3] > sma50[-4] > sma50[-5]
            )

            weekly_rsi_arr  = talib.RSI(_safe_array(weekly_close), timeperiod=14)
            last_weekly_rsi = float(weekly_rsi_arr[-1]) if len(weekly_rsi_arr) > 0 else 0.0

            di_diff   = float(plus_di[-1] - plus_di[-2]) if len(plus_di) >= 2 else 0.0
            vol_value = float(close[-1]) * float(volume[-1])

            cond1 = di_diff >= 10
            cond2 = vol_value > vol_threshold
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
            log.debug(f"{symbol}: buy-signal check — {exc}")

        # ── Stock details ─────────────────────────────────────────────────
        try:
            close_1y     = data_1y["Close"]
            idx_close_1y = idx_1y["Close"]
            latest_price = float(close_1y.iloc[-1])
            prev_close   = float(close_1y.iloc[-2]) if len(close_1y) > 1 else latest_price
            change_pct   = ((latest_price - prev_close) / prev_close * 100) if prev_close else 0.0

            vol_series     = data_1y["Volume"].tail(20)
            latest_volume  = float(data_1y["Volume"].iloc[-1])
            avg_volume     = float(vol_series.mean()) if not vol_series.empty else 0.0

            common = close_1y.index.intersection(idx_close_1y.index)
            rs_val = (
                float((close_1y.loc[common].iloc[-1] * 7 * 1000) / idx_close_1y.loc[common].iloc[-1])
                if len(common) > 0 else 0.0
            )

            result["details"] = {
                "symbol":       symbol,
                "price":        round(latest_price, 2),
                "change_pct":   round(change_pct, 2),
                "volume":       int(latest_volume),
                "avg_volume":   int(avg_volume),
                "volume_ratio": round(latest_volume / avg_volume, 2) if avg_volume else 0.0,
                "rs_value":     round(rs_val, 2),
            }
        except Exception as exc:
            log.debug(f"{symbol}: details — {exc}")

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

                    market = msg.get("market", "india")
                    t0 = time.monotonic()
                    scan_result = scan_single_stock(msg["symbol"], market=market)
                    elapsed_ms = (time.monotonic() - t0) * 1000

                    envelope = {
                        "request_id":  msg.get("request_id"),
                        "symbol":      msg["symbol"],
                        "market":      market,
                        "processed_at": datetime.utcnow().isoformat(),
                        "elapsed_ms":  round(elapsed_ms, 1),
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
                        f"[{market}] {tag} {msg['symbol']:<14} "
                        f"rs={scan_result['rs_high']!s:<5} buy={scan_result['buy_signal']!s:<5} "
                        f"in {elapsed_ms:>6.0f}ms  (n={processed})"
                    )

                    # Throttle to stay under Yahoo rate limits
                    if INTER_STOCK_DELAY_SEC > 0:
                        time.sleep(INTER_STOCK_DELAY_SEC)

            producer.flush(timeout=5)
    finally:
        log.info("closing consumer + producer")
        consumer.close()
        producer.close(timeout=5)
        log.info(f"worker stopped. processed {processed} message(s).")


if __name__ == "__main__":
    run_worker()
