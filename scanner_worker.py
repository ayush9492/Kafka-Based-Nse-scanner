"""NSE/USA Scanner worker — Kafka + threading + bulk OHLCV fetch.

Pipeline per poll:
    consumer.poll(max_poll_records=N)
       ├─ group msgs by market
       ├─ bulk fetch ALL symbols in one call (Method A or B, see fetcher.py)
       ├─ ThreadPoolExecutor runs TA-Lib checks in parallel per symbol
       └─ batch-produce results to Kafka

Speedup vs old (1 HTTP per stock + 0.15s sleep):
    India 221 stocks: ~10 min  →  ~20-40 s
    USA  6713 stocks: ~hours   →  ~2-4 min  (with ≥4 worker procs)
"""
from __future__ import annotations

import json
import logging
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

CST = ZoneInfo("America/Chicago")


def _now_cst() -> datetime:
    return datetime.now(CST)

import numpy as np
import pandas as pd

from kafka import KafkaConsumer, KafkaProducer

from fetcher import fetch_batch, fetch_index
from kafka_config import (
    CONSUMER_AUTO_OFFSET_RESET,
    CONSUMER_MAX_POLL_RECORDS,
    FETCH_BATCH_SIZE,
    FETCH_METHOD,
    FETCH_THREADS,
    INDEX_SYMBOL,
    INDEX_SYMBOL_USA,
    INTER_STOCK_DELAY_SEC,
    KAFKA_BOOTSTRAP,
    PRODUCER_ACKS,
    SESSION_TIMEOUT_MS,
    TA_THREADS,
    TOPIC_SCAN_REQUESTS,
    TOPIC_SCAN_RESULTS,
    VOL_THRESHOLD_INDIA,
    VOL_THRESHOLD_USA,
    WORKER_GROUP,
    WORKER_THREADS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S %Z",
)
logging.Formatter.converter = lambda *args: datetime.now(CST).timetuple()
log = logging.getLogger("scanner-worker")


# ─── Pure-CPU TA on already-fetched DataFrame ─────────────────────────────────
def _safe_array(series: pd.Series) -> np.ndarray:
    return np.asarray(series.values, dtype=np.float64)


def _empty_result(symbol: str, market: str, err: Optional[str]) -> Dict[str, Any]:
    return {
        "symbol": symbol, "market": market,
        "rs_high": False, "buy_signal": False,
        "details": {
            "symbol": symbol, "price": 0.0, "change_pct": 0.0,
            "volume": 0, "avg_volume": 0, "volume_ratio": 0.0, "rs_value": 0.0,
        },
        "error": err,
    }


def scan_with_data(symbol: str, market: str,
                   data_2y: Optional[pd.DataFrame],
                   idx_2y: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """RS-high + buy-signal checks given already-fetched OHLCV."""
    import talib

    if data_2y is None or data_2y.empty:
        return _empty_result(symbol, market, "empty OHLCV (rate-limited / delisted / wrong sym)")
    if idx_2y is None or idx_2y.empty:
        return _empty_result(symbol, market, "index data unavailable")

    vol_threshold = VOL_THRESHOLD_INDIA if market == "india" else VOL_THRESHOLD_USA
    result = _empty_result(symbol, market, None)

    try:
        n = len(data_2y)
        data_1y  = data_2y.iloc[max(0, n - 252):]
        data_6mo = data_2y.iloc[max(0, n - 126):]
        n_idx    = len(idx_2y)
        idx_1y   = idx_2y.iloc[max(0, n_idx - 252):]
        weekly_close = data_2y["Close"].resample("W").last().dropna()

        # ── RS new-high ───────────────────────────────────────────────
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
            log.debug(f"{symbol}: RS — {exc}")

        # ── Buy-signal ────────────────────────────────────────────────
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
            log.debug(f"{symbol}: buy — {exc}")

        # ── Details ───────────────────────────────────────────────────
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


def _ticker_for(symbol: str, market: str) -> str:
    return f"{symbol}.NS" if market == "india" else symbol


def _process_market_batch(market: str, msgs: List[dict], producer: KafkaProducer) -> int:
    """Bulk fetch + threaded TA for all msgs of one market. Returns count processed."""
    if not msgs:
        return 0

    index_sym = INDEX_SYMBOL if market == "india" else INDEX_SYMBOL_USA
    idx_2y = fetch_index(index_sym, method=FETCH_METHOD, period="2y")

    # Build ticker → original symbol mapping
    sym_by_ticker: Dict[str, str] = {}
    msg_by_sym:    Dict[str, dict] = {}
    for m in msgs:
        s = m["symbol"]
        sym_by_ticker[_ticker_for(s, market)] = s
        msg_by_sym[s] = m

    tickers = list(sym_by_ticker.keys())

    # Bulk fetch in chunks (avoid mega URLs / yfinance limits)
    all_data: Dict[str, pd.DataFrame] = {}
    for i in range(0, len(tickers), FETCH_BATCH_SIZE):
        chunk = tickers[i:i + FETCH_BATCH_SIZE]
        all_data.update(fetch_batch(chunk, method=FETCH_METHOD,
                                    period="2y", max_workers=FETCH_THREADS))
        if _shutdown:
            break

    # Threaded TA + send
    def _one(symbol: str) -> Dict[str, Any]:
        ticker = _ticker_for(symbol, market)
        df = all_data.get(ticker)
        t0 = time.monotonic()
        res = scan_with_data(symbol, market, df, idx_2y)
        elapsed_ms = (time.monotonic() - t0) * 1000
        msg = msg_by_sym[symbol]
        envelope = {
            "request_id":   msg.get("request_id"),
            "symbol":       symbol,
            "market":       market,
            "processed_at": _now_cst().isoformat(),
            "elapsed_ms":   round(elapsed_ms, 1),
            **res,
        }
        producer.send(TOPIC_SCAN_RESULTS, key=symbol, value=envelope)
        return res

    processed = 0
    with ThreadPoolExecutor(max_workers=TA_THREADS) as pool:
        for symbol, res in zip(msg_by_sym.keys(), pool.map(_one, msg_by_sym.keys())):
            if res["error"]:
                tag = "x"
            elif res["rs_high"] and res["buy_signal"]:
                tag = "*"
            elif res["rs_high"] or res["buy_signal"]:
                tag = "+"
            else:
                tag = "."
            log.info(f"[{market}] {tag} {symbol:<14} "
                     f"rs={res['rs_high']!s:<5} buy={res['buy_signal']!s:<5}")
            processed += 1

    return processed


_total_lock = threading.Lock()
_total_processed = 0


def _consumer_thread_loop(thread_id: int, producer: KafkaProducer) -> None:
    """One consumer thread. Joins WORKER_GROUP — Kafka assigns partitions across threads."""
    global _total_processed

    consumer = KafkaConsumer(
        TOPIC_SCAN_REQUESTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=WORKER_GROUP,
        client_id=f"worker-thread-{thread_id}",
        auto_offset_reset=CONSUMER_AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        session_timeout_ms=SESSION_TIMEOUT_MS,
        value_deserializer=_json_des,
        max_poll_records=CONSUMER_MAX_POLL_RECORDS,
    )
    log.info(f"[t{thread_id}] subscribed, polling…")

    try:
        while not _shutdown:
            batch = consumer.poll(timeout_ms=1000)
            if not batch:
                continue

            by_market: Dict[str, List[dict]] = {}
            for tp_records in batch.values():
                for record in tp_records:
                    msg = record.value
                    if not isinstance(msg, dict) or "symbol" not in msg:
                        log.warning(f"[t{thread_id}] skip malformed: {msg!r}")
                        continue
                    by_market.setdefault(msg.get("market", "india"), []).append(msg)

            t0 = time.monotonic()
            n = 0
            for market, msgs in by_market.items():
                n += _process_market_batch(market, msgs, producer)
                if _shutdown:
                    break

            producer.flush(timeout=10)
            with _total_lock:
                _total_processed += n
                grand_total = _total_processed
            elapsed = time.monotonic() - t0
            log.info(f"[t{thread_id}] batch: {n} stocks in {elapsed:.1f}s "
                     f"({n/max(elapsed,0.001):.1f} stk/s) — grand total {grand_total}")

            if INTER_STOCK_DELAY_SEC > 0:
                time.sleep(INTER_STOCK_DELAY_SEC)
    except Exception as exc:
        log.error(f"[t{thread_id}] crashed: {exc}")
    finally:
        consumer.close()
        log.info(f"[t{thread_id}] stopped")


def run_worker() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info(f"connecting to Kafka @ {KAFKA_BOOTSTRAP}")
    log.info(f"WORKER_THREADS={WORKER_THREADS}  FETCH_METHOD={FETCH_METHOD}  "
             f"BATCH={FETCH_BATCH_SIZE}  FETCH_THREADS={FETCH_THREADS}  TA_THREADS={TA_THREADS}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks=PRODUCER_ACKS,
        value_serializer=_json_ser,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=20,
        batch_size=64 * 1024,
    )

    threads: List[threading.Thread] = []
    for i in range(WORKER_THREADS):
        t = threading.Thread(
            target=_consumer_thread_loop,
            args=(i, producer),
            name=f"worker-{i}",
            daemon=True,
        )
        t.start()
        threads.append(t)
    log.info(f"spawned {WORKER_THREADS} consumer threads")

    try:
        while not _shutdown:
            time.sleep(0.5)
    finally:
        log.info("shutting down…")
        for t in threads:
            t.join(timeout=10)
        producer.close(timeout=5)
        log.info(f"all stopped. grand total {_total_processed} messages.")


if __name__ == "__main__":
    run_worker()
