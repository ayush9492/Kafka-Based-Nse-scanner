"""Stock Scanner API — Kafka-powered, dual-market (India / USA).

Architecture:
    POST /scan?market=india|usa  ──► Kafka nse-scanner.scan-requests
    workers consume, run checks, publish to nse-scanner.scan-results
    background collector thread updates per-market scan state

    GET /status?market=...  ──► live progress snapshot
    GET /results?market=... ──► rs_highs, buy_signals, final_stocks
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List

import yfinance as yf
from fastapi import BackgroundTasks, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from kafka_config import (
    CONSUMER_AUTO_OFFSET_RESET,
    INDEX_SYMBOL,
    INDEX_SYMBOL_USA,
    KAFKA_BOOTSTRAP,
    PRODUCER_ACKS,
    STOCKS_FILE_USA,
    TOPIC_SCAN_REQUESTS,
    TOPIC_SCAN_RESULTS,
    load_stocks,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scanner-api")

app = FastAPI(title="Stock Scanner API (Kafka)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Stock lists ──────────────────────────────────────────────────────────────
STOCK_LIST_INDIA: List[str] = load_stocks()
log.info(f"loaded {len(STOCK_LIST_INDIA)} India stocks from stocks.txt")

try:
    STOCK_LIST_USA: List[str] = load_stocks(STOCKS_FILE_USA)
    log.info(f"loaded {len(STOCK_LIST_USA)} USA stocks from stock_usa.txt")
except FileNotFoundError:
    log.warning("stock_usa.txt not found — USA market disabled (create stock_usa.txt to enable)")
    STOCK_LIST_USA = []

STOCK_LISTS: Dict[str, List[str]] = {
    "india": STOCK_LIST_INDIA,
    "usa": STOCK_LIST_USA,
}


# ─── Per-market scan state ────────────────────────────────────────────────────
def _fresh_state(total: int) -> Dict[str, Any]:
    return {
        "status": "idle",
        "progress": 0,
        "current_stock": "",
        "scanned_count": 0,
        "total_stocks": total,
        "rs_highs": [],
        "buy_signals": [],
        "final_stocks": [],
        "stock_details": {},
        "last_scan_time": None,
        "error": None,
        "request_id": None,
        "kafka_connected": False,
    }


SCAN_STATES: Dict[str, Dict[str, Any]] = {
    "india": _fresh_state(len(STOCK_LIST_INDIA)),
    "usa": _fresh_state(len(STOCK_LIST_USA)),
}
SCAN_LOCKS: Dict[str, threading.Lock] = {
    "india": threading.Lock(),
    "usa": threading.Lock(),
}


# ─── Kafka producer ───────────────────────────────────────────────────────────
_producer = None
_producer_lock = threading.Lock()


def get_producer():
    global _producer
    from kafka import KafkaProducer
    with _producer_lock:
        if _producer is not None:
            return _producer
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            acks=PRODUCER_ACKS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            request_timeout_ms=30_000,
        )
        log.info(f"producer connected to {KAFKA_BOOTSTRAP}")
    return _producer


# ─── Result collector ─────────────────────────────────────────────────────────
def _apply_result(result: Dict[str, Any]) -> None:
    symbol = result.get("symbol")
    rid = result.get("request_id")
    market = result.get("market", "india")
    if not symbol or market not in SCAN_STATES:
        return

    state = SCAN_STATES[market]
    lock = SCAN_LOCKS[market]

    with lock:
        if state["status"] != "scanning":
            return
        if rid and state.get("request_id") and rid != state["request_id"]:
            log.debug(f"[{market}] drop stale result {symbol} rid={str(rid)[:8]}…")
            return

        if result.get("rs_high") and symbol not in state["rs_highs"]:
            state["rs_highs"].append(symbol)
        if result.get("buy_signal") and symbol not in state["buy_signals"]:
            state["buy_signals"].append(symbol)
        if result.get("details"):
            state["stock_details"][symbol] = result["details"]

        seen = state.setdefault("_seen_symbols", set())
        if symbol in seen:
            return
        seen.add(symbol)

        state["scanned_count"] = len(seen)
        state["current_stock"] = symbol
        total = state["total_stocks"] or 1
        state["progress"] = int((len(seen) / total) * 100)
        log.info(f"[{market}] {len(seen)}/{total} ({state['progress']}%) — {symbol}")

        if len(seen) >= total:
            final = sorted(set(state["rs_highs"]) & set(state["buy_signals"]))
            state["final_stocks"] = final
            state["status"] = "complete"
            state["last_scan_time"] = datetime.utcnow().isoformat()
            state["progress"] = 100
            log.info(
                f"[{market}] scan complete: {len(state['rs_highs'])} RS highs, "
                f"{len(state['buy_signals'])} buy signals, {len(final)} final"
            )


def _collector_loop():
    from kafka import KafkaConsumer
    consumer = None
    while True:
        try:
            if consumer is None:
                consumer = KafkaConsumer(
                    TOPIC_SCAN_RESULTS,
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    group_id="nse-scanner-api-collector",
                    auto_offset_reset=CONSUMER_AUTO_OFFSET_RESET,
                    enable_auto_commit=True,
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                    consumer_timeout_ms=2000,
                )
                for state in SCAN_STATES.values():
                    state["kafka_connected"] = True
                log.info("collector connected")

            batch = consumer.poll(timeout_ms=1000)
            for tp_records in batch.values():
                for record in tp_records:
                    val = record.value
                    if isinstance(val, dict):
                        _apply_result(val)
        except Exception as exc:
            log.warning(f"collector error: {exc}; retrying in 5s")
            for state in SCAN_STATES.values():
                state["kafka_connected"] = False
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
                consumer = None
            time.sleep(5)


_collector_thread = threading.Thread(target=_collector_loop, daemon=True, name="result-collector")
_collector_thread.start()


# ─── Scan launcher ────────────────────────────────────────────────────────────
def publish_scan(symbols: List[str], request_id: str, market: str) -> int:
    producer = get_producer()
    sent = 0
    for sym in symbols:
        msg = {"request_id": request_id, "symbol": sym, "market": market, "ts": time.time()}
        try:
            producer.send(TOPIC_SCAN_REQUESTS, key=sym, value=msg)
            sent += 1
        except Exception as exc:
            log.error(f"failed to publish {sym}: {exc}")
    producer.flush(timeout=10)
    return sent


def start_scan_async(market: str = "india"):
    stocks = STOCK_LISTS.get(market, [])
    if not stocks:
        with SCAN_LOCKS[market]:
            SCAN_STATES[market]["status"] = "error"
            SCAN_STATES[market]["error"] = (
                f"No stocks loaded for '{market}'. "
                f"{'Create stock_usa.txt with one symbol per line.' if market == 'usa' else ''}"
            )
        return

    request_id = str(uuid.uuid4())
    with SCAN_LOCKS[market]:
        SCAN_STATES[market].update({
            "status": "scanning",
            "progress": 0,
            "current_stock": "",
            "scanned_count": 0,
            "total_stocks": len(stocks),
            "rs_highs": [],
            "buy_signals": [],
            "final_stocks": [],
            "stock_details": {},
            "last_scan_time": None,
            "error": None,
            "request_id": request_id,
            "_seen_symbols": set(),
        })

    try:
        sent = publish_scan(stocks, request_id, market)
        log.info(f"[{market}] published {sent}/{len(stocks)} requests (rid={request_id[:8]}…)")
        if sent == 0:
            with SCAN_LOCKS[market]:
                SCAN_STATES[market]["status"] = "error"
                SCAN_STATES[market]["error"] = "nothing published to Kafka"
    except Exception as exc:
        log.error(f"[{market}] scan launch failed: {exc}")
        with SCAN_LOCKS[market]:
            SCAN_STATES[market]["status"] = "error"
            SCAN_STATES[market]["error"] = f"Kafka unavailable: {exc}"


# ─── API routes ───────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "message": "Stock Scanner API (Kafka) is running",
        "markets": {
            m: {"stock_count": len(STOCK_LISTS[m]), "status": SCAN_STATES[m]["status"]}
            for m in ("india", "usa")
        },
        "kafka_connected": SCAN_STATES["india"]["kafka_connected"],
    }


@app.post("/scan")
def trigger_scan(background_tasks: BackgroundTasks, market: str = Query("india")):
    if market not in SCAN_STATES:
        return {"error": f"Unknown market '{market}'. Use 'india' or 'usa'."}
    with SCAN_LOCKS[market]:
        if SCAN_STATES[market]["status"] == "scanning":
            return {"message": "scan already in progress", "status": "scanning", "market": market}
    background_tasks.add_task(start_scan_async, market)
    return {
        "message": "scan started",
        "status": "scanning",
        "market": market,
        "stock_count": len(STOCK_LISTS[market]),
    }


@app.get("/status")
def get_status(market: str = Query("india")):
    if market not in SCAN_STATES:
        return {"error": f"Unknown market '{market}'"}
    with SCAN_LOCKS[market]:
        s = SCAN_STATES[market]
        return {
            "status": s["status"],
            "progress": s["progress"],
            "current_stock": s["current_stock"],
            "scanned_count": s["scanned_count"],
            "total_stocks": s["total_stocks"],
            "kafka_connected": s["kafka_connected"],
            "market": market,
        }


@app.get("/results")
def get_results(market: str = Query("india")):
    import math
    if market not in SCAN_STATES:
        return {"error": f"Unknown market '{market}'"}

    def _sanitize(obj):
        if isinstance(obj, dict):
            return {k: _sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_sanitize(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return 0.0
        try:
            import numpy as np
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                return float(obj)
        except ImportError:
            pass
        return obj

    with SCAN_LOCKS[market]:
        s = SCAN_STATES[market]
        final_so_far = (
            sorted(set(s["rs_highs"]) & set(s["buy_signals"]))
            if s["status"] == "scanning"
            else s["final_stocks"]
        )
        return {
            "status": s["status"],
            "market": market,
            "rs_highs": s["rs_highs"],
            "buy_signals": s["buy_signals"],
            "final_stocks": final_so_far,
            "stock_details": _sanitize(s["stock_details"]),
            "last_scan_time": s["last_scan_time"],
            "total_rs": len(s["rs_highs"]),
            "total_buy": len(s["buy_signals"]),
            "total_final": len(final_so_far),
        }


@app.get("/stocks")
def list_stocks(market: str = Query("india")):
    stocks = STOCK_LISTS.get(market, [])
    return {"market": market, "count": len(stocks), "symbols": stocks}


@app.get("/stock/{symbol}")
def get_stock_chart(symbol: str, market: str = Query("india")):
    try:
        ticker_sym = f"{symbol}.NS" if market == "india" else symbol
        index_sym = INDEX_SYMBOL if market == "india" else INDEX_SYMBOL_USA
        df = yf.Ticker(ticker_sym).history(period="1y")
        index_df = yf.Ticker(index_sym).history(period="1y")

        common = df.index.intersection(index_df.index)
        rs_line = ((df["Close"].loc[common] * 7000) / index_df["Close"].loc[common]).tolist()

        return {
            "symbol": symbol,
            "market": market,
            "dates": [d.strftime("%Y-%m-%d") for d in df.index],
            "close": df["Close"].tolist(),
            "high": df["High"].tolist(),
            "low": df["Low"].tolist(),
            "open": df["Open"].tolist(),
            "volume": df["Volume"].tolist(),
            "rs_dates": [d.strftime("%Y-%m-%d") for d in common],
            "rs_line": rs_line,
        }
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/app", response_class=FileResponse)
def serve_frontend():
    for path in ["index.html", "static/index.html", "../frontend/index.html"]:
        if os.path.exists(path):
            return FileResponse(path)
    return {"error": "Frontend not found"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
