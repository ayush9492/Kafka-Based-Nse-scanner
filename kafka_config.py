"""Shared configuration for the Kafka-powered NSE Scanner.

All topic names, bootstrap servers, and constants live here so producer,
workers, and result-collector can't drift out of sync.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List


# ─── Kafka ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

TOPIC_SCAN_REQUESTS = "nse-scanner.scan-requests"
TOPIC_SCAN_RESULTS = "nse-scanner.scan-results"

WORKER_GROUP = "nse-scanner-workers"

# More partitions = more workers can process in parallel. Tune to your fleet.
DEFAULT_PARTITIONS = 16
DEFAULT_REPLICATION = 1  # single-broker dev cluster

# Durability / performance knobs
PRODUCER_ACKS = "all"                  # wait for full ISR
CONSUMER_AUTO_OFFSET_RESET = "earliest"
CONSUMER_MAX_POLL_RECORDS = 50         # bigger poll → bigger fetch batches
INTER_STOCK_DELAY_SEC = 0.0            # 0 — bulk fetch + curl_cffi avoids 429s
YFINANCE_RETRIES = 2                   # retry attempts on empty/failed fetch
REQUEST_TIMEOUT_MS = 30_000
SESSION_TIMEOUT_MS = 30_000

# Bulk-fetch tuning
FETCH_METHOD = os.environ.get("FETCH_METHOD", "B")   # "A" = yf.download batch, "B" = curl_cffi raw
FETCH_BATCH_SIZE = int(os.environ.get("FETCH_BATCH_SIZE", "50"))   # tickers per batch fetch
FETCH_THREADS   = int(os.environ.get("FETCH_THREADS",   "20"))     # threads for Method B
TA_THREADS      = int(os.environ.get("TA_THREADS",      "8"))      # threads for TA-Lib calc


# ─── Scanner ──────────────────────────────────────────────────────────────────
INDEX_SYMBOL = "^NSEI"
INDEX_SYMBOL_USA = "^GSPC"

STOCKS_FILE = Path(__file__).parent / "stocks.txt"
STOCKS_FILE_USA = Path(__file__).parent / "stock_usa.txt"

VOL_THRESHOLD_INDIA = 50_000_000   # ₹5 Crore
VOL_THRESHOLD_USA   = 3_000_000    # $3 Million


def load_stocks(path: Path = STOCKS_FILE) -> List[str]:
    """Load symbols from stocks.txt. Strips comments, blank lines, and any
    `.NS` suffix — workers append it when querying yfinance.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Stock list not found at {path}. "
            "Create stocks.txt with one symbol per line."
        )

    symbols: List[str] = []
    seen: set = set()
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        sym = line.split("#", 1)[0].strip().upper()
        # Normalize away yfinance suffixes — workers add them back
        for suffix in (".NS", ".NSE", ".BO", ".BSE"):
            if sym.endswith(suffix):
                sym = sym[: -len(suffix)]
                break
        if sym and sym not in seen:
            seen.add(sym)
            symbols.append(sym)
    return symbols


if __name__ == "__main__":
    stocks = load_stocks()
    print(f"Loaded {len(stocks)} unique symbols from {STOCKS_FILE}")
    print("First 5:", stocks[:5])
    print("Last 5:", stocks[-5:])
    print(f"\nKafka bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"Topics: {TOPIC_SCAN_REQUESTS}, {TOPIC_SCAN_RESULTS}")
