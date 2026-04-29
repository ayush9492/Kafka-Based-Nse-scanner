"""Bulk OHLCV fetcher.

Two methods:
    A) yf.download(tickers=[...], threads=True)  — batch yfinance, simple.
    B) Raw Yahoo chart API + curl_cffi (Chrome impersonation) — fastest.

Pick via FETCH_METHOD in kafka_config.
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger("fetcher")


# ─── Method A: yf.download batch ──────────────────────────────────────────────
def _fetch_batch_yf(tickers: List[str], period: str = "2y") -> Dict[str, pd.DataFrame]:
    """Single yf.download call for many tickers. Returns {ticker: DataFrame}."""
    import yfinance as yf

    if not tickers:
        return {}

    out: Dict[str, pd.DataFrame] = {}
    try:
        data = yf.download(
            tickers=tickers,
            period=period,
            interval="1d",
            group_by="ticker",
            threads=True,
            progress=False,
            auto_adjust=False,
        )
    except Exception as exc:
        log.warning(f"yf.download batch failed ({len(tickers)} tickers): {exc}")
        return out

    if data is None or data.empty:
        return out

    if len(tickers) == 1:
        t = tickers[0]
        if not data.empty:
            out[t] = data.dropna(how="all")
        return out

    for t in tickers:
        try:
            df = data[t].dropna(how="all")
            if not df.empty:
                out[t] = df
        except KeyError:
            continue
    return out


# ─── Method B: raw Yahoo chart API + curl_cffi ────────────────────────────────
_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
_session = None
_session_lock = Lock()


def _get_session():
    """Lazily build a curl_cffi Chrome-impersonating Session."""
    global _session
    with _session_lock:
        if _session is not None:
            return _session
        from curl_cffi import requests as creq
        _session = creq.Session(impersonate="chrome120", timeout=15)
    return _session


def _parse_chart_json(j: dict) -> Optional[pd.DataFrame]:
    try:
        result = j["chart"]["result"][0]
        ts = result["timestamp"]
        q = result["indicators"]["quote"][0]
        idx = pd.to_datetime(ts, unit="s", utc=True).tz_convert(None)
        df = pd.DataFrame({
            "Open":   q.get("open"),
            "High":   q.get("high"),
            "Low":    q.get("low"),
            "Close":  q.get("close"),
            "Volume": q.get("volume"),
        }, index=idx)
        df = df.dropna(how="all")
        return df if not df.empty else None
    except (KeyError, TypeError, IndexError):
        return None


def _fetch_one_curl(ticker: str, period: str = "2y") -> Tuple[str, Optional[pd.DataFrame]]:
    sess = _get_session()
    try:
        r = sess.get(
            _CHART_URL.format(ticker=ticker),
            params={"range": period, "interval": "1d", "includePrePost": "false"},
        )
        if r.status_code != 200:
            return ticker, None
        return ticker, _parse_chart_json(r.json())
    except Exception as exc:
        log.debug(f"curl_cffi {ticker}: {exc}")
        return ticker, None


def _fetch_batch_curl(tickers: List[str], period: str = "2y",
                      max_workers: int = 20) -> Dict[str, pd.DataFrame]:
    """ThreadPool of curl_cffi requests to raw Yahoo chart API."""
    out: Dict[str, pd.DataFrame] = {}
    if not tickers:
        return out

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_fetch_one_curl, t, period) for t in tickers]
        for fut in as_completed(futures):
            t, df = fut.result()
            if df is not None:
                out[t] = df
    return out


# ─── Public API ───────────────────────────────────────────────────────────────
def fetch_batch(tickers: List[str], method: str = "B", period: str = "2y",
                max_workers: int = 20) -> Dict[str, pd.DataFrame]:
    """Bulk-fetch OHLCV. method='A' (yf.download) or 'B' (curl_cffi)."""
    t0 = time.monotonic()
    if method.upper() == "A":
        out = _fetch_batch_yf(tickers, period=period)
    else:
        out = _fetch_batch_curl(tickers, period=period, max_workers=max_workers)
    elapsed = time.monotonic() - t0
    log.info(f"fetch_batch[{method}] {len(out)}/{len(tickers)} in {elapsed:.1f}s "
             f"({len(out)/max(elapsed,0.001):.1f} stk/s)")
    return out


# ─── Index cache (shared across batches) ──────────────────────────────────────
_index_cache: Dict[str, Tuple[pd.DataFrame, float]] = {}
_index_cache_lock = Lock()
INDEX_CACHE_TTL_SEC = 300


def fetch_index(index_sym: str, method: str = "B", period: str = "2y") -> Optional[pd.DataFrame]:
    key = f"{index_sym}:{period}"
    now = time.time()
    with _index_cache_lock:
        cached = _index_cache.get(key)
        if cached and now - cached[1] < INDEX_CACHE_TTL_SEC:
            return cached[0]

    res = fetch_batch([index_sym], method=method, period=period, max_workers=1)
    df = res.get(index_sym)
    if df is not None and not df.empty:
        with _index_cache_lock:
            _index_cache[key] = (df, time.time())
        return df
    return None
