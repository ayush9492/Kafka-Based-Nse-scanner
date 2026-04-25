"""Stock Scanner API — Kafka-powered version.

FastAPI backend that publishes scan requests to Kafka and consumes results
via a background thread. Preserves the original REST API contract (/scan,
/status, /results, /stock/{symbol}) so the React frontend works unchanged.

Architecture:
    POST /scan  ──► publishes N messages to Kafka  ──► workers consume
                                                        and publish results
    background thread ◄──  consumes nse-scanner.scan-results
                            and updates scan_state progressively

    GET /status  ──► returns scan_state snapshot (progress, etc.)
    GET /results ──► returns final rs_highs, buy_signals, stock_details
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
from fastapi import BackgroundTasks, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from kafka_config import (
    CONSUMER_AUTO_OFFSET_RESET,
    INDEX_SYMBOL,
    KAFKA_BOOTSTRAP,
    PRODUCER_ACKS,
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


# ─── Stock list (loaded once at startup) ─────────────────────────────────────
STOCK_LIST: List[str] = load_stocks()
log.info(f"loaded {len(STOCK_LIST)} stocks from stocks.txt")


# ─── Global scan state ────────────────────────────────────────────────────────
scan_state: Dict[str, Any] = {
    "status": "idle",              # idle | scanning | complete | error
    "progress": 0,                 # 0-100
    "current_stock": "",
    "scanned_count": 0,
    "total_stocks": len(STOCK_LIST),
    "rs_highs": [],                # list[str]
    "buy_signals": [],             # list[str]
    "final_stocks": [],
    "stock_details": {},           # symbol -> details dict
    "last_scan_time": None,
    "error": None,
    "request_id": None,
    "kafka_connected": False,
}
scan_lock = threading.Lock()


# ─── Kafka producer + background consumer ─────────────────────────────────────
_producer = None
_producer_lock = threading.Lock()


def get_producer():
    """Lazy-init a module-level producer."""
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


def _collector_loop():
    """Long-lived thread that consumes results and updates scan_state."""
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
                with scan_lock:
                    scan_state["kafka_connected"] = True
                log.info("collector connected")

            batch = consumer.poll(timeout_ms=1000)
            for tp_records in batch.values():
                for record in tp_records:
                    val = record.value
                    if not isinstance(val, dict):
                        continue
                    _apply_result(val)
        except Exception as exc:
            log.warning(f"collector error: {exc}; retrying in 5s")
            with scan_lock:
                scan_state["kafka_connected"] = False
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
                consumer = None
            time.sleep(5)


def _apply_result(result: Dict[str, Any]) -> None:
    """Fold one result envelope into scan_state."""
    symbol = result.get("symbol")
    rid = result.get("request_id")
    if not symbol:
        return

    with scan_lock:
        if scan_state["status"] != "scanning":
            return
        if rid and scan_state.get("request_id") and rid != scan_state["request_id"]:
            log.debug(f"drop stale result for {symbol} (rid={rid[:8]}… != {scan_state['request_id'][:8]}…)")
            return

        if result.get("rs_high") and symbol not in scan_state["rs_highs"]:
            scan_state["rs_highs"].append(symbol)
        if result.get("buy_signal") and symbol not in scan_state["buy_signals"]:
            scan_state["buy_signals"].append(symbol)
        if result.get("details"):
            scan_state["stock_details"][symbol] = result["details"]

        seen = scan_state.setdefault("_seen_symbols", set())
        if symbol in seen:
            return
        seen.add(symbol)

        scan_state["scanned_count"] = len(seen)
        scan_state["current_stock"] = symbol
        total = scan_state["total_stocks"] or 1
        scan_state["progress"] = int((len(seen) / total) * 100)
        log.info(f"progress: {len(seen)}/{total} ({scan_state['progress']}%) — {symbol}")

        if len(seen) >= total:
            final = sorted(set(scan_state["rs_highs"]) & set(scan_state["buy_signals"]))
            scan_state["final_stocks"] = final
            scan_state["status"] = "complete"
            scan_state["last_scan_time"] = datetime.utcnow().isoformat()
            scan_state["progress"] = 100
            log.info(
                f"scan complete: {len(scan_state['rs_highs'])} RS highs, "
                f"{len(scan_state['buy_signals'])} buy signals, "
                f"{len(final)} final"
            )


# Launch the collector thread on import so it's always listening
_collector_thread = threading.Thread(target=_collector_loop, daemon=True, name="result-collector")
_collector_thread.start()


# ─── Scan launcher ────────────────────────────────────────────────────────────
def publish_scan(symbols: List[str], request_id: str) -> int:
    producer = get_producer()
    sent = 0
    for sym in symbols:
        msg = {"request_id": request_id, "symbol": sym, "ts": time.time()}
        try:
            producer.send(TOPIC_SCAN_REQUESTS, key=sym, value=msg)
            sent += 1
        except Exception as exc:
            log.error(f"failed to publish {sym}: {exc}")
    producer.flush(timeout=10)
    return sent


def start_scan_async():
    request_id = str(uuid.uuid4())
    with scan_lock:
        scan_state.update({
            "status": "scanning",
            "progress": 0,
            "current_stock": "",
            "scanned_count": 0,
            "total_stocks": len(STOCK_LIST),
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
        sent = publish_scan(STOCK_LIST, request_id)
        log.info(f"published {sent}/{len(STOCK_LIST)} requests (rid={request_id[:8]}…)")
        if sent == 0:
            with scan_lock:
                scan_state["status"] = "error"
                scan_state["error"] = "nothing was published to Kafka"
    except Exception as exc:
        log.error(f"scan launch failed: {exc}")
        with scan_lock:
            scan_state["status"] = "error"
            scan_state["error"] = f"Kafka unavailable: {exc}"


# ─── API ROUTES ───────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "message": "Stock Scanner API (Kafka) is running",
        "stock_count": len(STOCK_LIST),
        "kafka_connected": scan_state["kafka_connected"],
    }


@app.post("/scan")
def trigger_scan(background_tasks: BackgroundTasks):
    with scan_lock:
        if scan_state["status"] == "scanning":
            return {"message": "scan already in progress", "status": "scanning"}
    background_tasks.add_task(start_scan_async)
    return {"message": "scan started", "status": "scanning", "stock_count": len(STOCK_LIST)}


@app.get("/status")
def get_status():
    with scan_lock:
        return {
            "status": scan_state["status"],
            "progress": scan_state["progress"],
            "current_stock": scan_state["current_stock"],
            "scanned_count": scan_state["scanned_count"],
            "total_stocks": scan_state["total_stocks"],
            "kafka_connected": scan_state["kafka_connected"],
        }


@app.get("/results")
def get_results():
    import math
    def _sanitize(obj):
        if isinstance(obj, dict):
            return {k: _sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_sanitize(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return 0.0
        try:
            import numpy as np
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj)
        except ImportError:
            pass
        return obj

    with scan_lock:
        if scan_state["status"] == "scanning":
            final_so_far = sorted(set(scan_state["rs_highs"]) & set(scan_state["buy_signals"]))
        else:
            final_so_far = scan_state["final_stocks"]
        return {
            "status": scan_state["status"],
            "rs_highs": scan_state["rs_highs"],
            "buy_signals": scan_state["buy_signals"],
            "final_stocks": final_so_far,
            "stock_details": _sanitize(scan_state["stock_details"]),
            "last_scan_time": scan_state["last_scan_time"],
            "total_rs": len(scan_state["rs_highs"]),
            "total_buy": len(scan_state["buy_signals"]),
            "total_final": len(final_so_far),
        }


@app.get("/stocks")
def list_stocks():
    """Return contents of stocks.txt."""
    return {"count": len(STOCK_LIST), "symbols": STOCK_LIST}


@app.get("/stock/{symbol}")
def get_stock_chart(symbol: str):
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        df = ticker.history(period="1y")
        index_df = yf.Ticker(INDEX_SYMBOL).history(period="1y")

        common = df.index.intersection(index_df.index)
        rs_line = ((df["Close"].loc[common] * 7000) / index_df["Close"].loc[common]).tolist()

        return {
            "symbol": symbol,
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
