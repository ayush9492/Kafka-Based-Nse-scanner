# NSE Scanner — Kafka-Powered

This is the original NSE Scanner (FastAPI backend + React frontend + TA-Lib indicators) with the sequential scan loop replaced by a **Kafka-based distributed pipeline**. Scans ~216 stocks in parallel across N worker processes instead of one at a time.

## What changed

| Before | After |
|---|---|
| Stock list hardcoded in `app.py` | `stocks.txt` — one symbol per line |
| `for symbol in STOCK_LIST:` sequential loop | `producer.send()` to `nse-scanner.scan-requests` topic |
| Single process doing all yfinance fetches | N worker processes consuming from the topic in parallel |
| `scan_state` updated inline | `scan_state` updated by a background collector thread reading `nse-scanner.scan-results` |

API routes (`/scan`, `/status`, `/results`, `/stock/{symbol}`) are unchanged — the React frontend works without modification.

## Files

```
NSE_Scanner/
├── app.py                  # FastAPI backend (Kafka-powered)
├── scanner_worker.py       # Kafka worker — run N instances for parallelism
├── kafka_config.py         # Shared config + stocks.txt loader
├── kafka_admin.py          # One-time topic creator
├── docker-compose.yml      # Kafka broker (KRaft mode) + Kafka UI
├── stocks.txt              # 216 NSE symbols, edit freely
├── index.html              # React frontend (unchanged)
├── requirements.txt
├── Dockerfile
├── railway.toml
├── backtest.py             # Unchanged — independent backtesting
└── backtest_output/
```

## Local setup

### 1. Start Kafka

```powershell
docker compose up -d
```

Wait ~30s for the broker to become healthy:

```powershell
docker compose ps
```

Should show `nse-scanner-kafka` and `nse-scanner-kafka-ui` both `Up (healthy)`.

Kafka UI is at [http://localhost:8090](http://localhost:8090) for inspecting topics/messages.

### 2. Install Python deps

```powershell
pip install -r requirements.txt
```

> **TA-Lib install on Windows:** this usually needs a pre-built wheel. If `pip install TA-Lib` fails, grab the correct wheel from [here](https://github.com/cgohlke/talib-build/releases) for your Python version and install via `pip install TA_Lib-0.4.xx-cp3xx-...whl`.

### 3. Create Kafka topics

```powershell
python kafka_admin.py
```

Expected: `Created: nse-scanner.scan-requests, nse-scanner.scan-results (8 partitions each)` (or "Already exist" on re-runs).

### 4. Start workers

**Open at least one terminal** and keep it running:

```powershell
python scanner_worker.py
```

Expected output:
```
HH:MM:SS [INFO] connecting to Kafka @ localhost:9092
HH:MM:SS [INFO] ready — subscribed to 'nse-scanner.scan-requests', group 'nse-scanner-workers'
```

**For parallelism, launch more workers in more terminals.** 4-8 is a reasonable sweet spot; the default topic has 8 partitions so workers beyond 8 just sit idle.

### 5. Start the API

In a **separate** terminal:

```powershell
python app.py
```

Or with uvicorn directly:

```powershell
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

Browse to [http://localhost:8000/app](http://localhost:8000/app) for the React UI. Click **Run Scan**; requests fan out to your workers and results stream back.

## Editing the stock list

Just edit `stocks.txt`. Lines starting with `#` are comments. The `.NS` suffix is optional — workers append it when calling yfinance.

```
# stocks.txt
RELIANCE
TCS
INFY
# Excluded until earnings:
# HDFCBANK
ICICIBANK
```

Restart `app.py` after editing — the list is loaded once at startup.

## Scaling

**Horizontal scaling is the whole point of the Kafka layer:**

| Workers | Stocks/sec (approx) | Time for 216 stocks |
|---|---|---|
| 1 | ~1 | ~3-4 min |
| 4 | ~3-4 | ~60-90 sec |
| 8 | ~5-7 | ~30-45 sec |

Beyond 8 you hit yfinance rate limits; Yahoo starts returning 429s. Add `time.sleep(0.2)` inside the worker loop if you see that pattern.

## Monitoring

- **API status:** `GET http://localhost:8000/status` → progress %, current stock, total, Kafka connection health
- **Kafka UI:** [http://localhost:8090](http://localhost:8090) → topic messages, consumer group lag, per-partition offsets
- **Worker logs:** per-stock scan outcome with icons:
  - `★` = passed both checks (final signal)
  - `✓` = passed one check
  - `·` = processed, no signal
  - `✗` = error

## Resetting

```powershell
# Stop everything
docker compose down

# Wipe topic data too
docker compose down -v
```

## Railway deploy (production)

The existing `railway.toml` and `Dockerfile` still work for the API. For Kafka in production, you'd typically use a **managed broker** — Confluent Cloud has a free tier:

1. Sign up at [confluent.cloud](https://confluent.cloud)
2. Create a cluster → get bootstrap server + API key
3. Set env vars on Railway:
   - `KAFKA_BOOTSTRAP=pkc-xxxxx.confluent.cloud:9092`
   - Add SASL auth in `kafka_config.py` (see kafka-python docs for `sasl_plain_username`/`sasl_plain_password`)
4. Workers deploy as a separate Railway service from the same repo with command `python scanner_worker.py`

Out of scope for this commit — local Docker works for development and portfolio demos.

## Why Kafka here?

Honest answer: for 216 stocks this is architecturally overkill — a `ThreadPoolExecutor` would give you 90% of the throughput with 10% of the infrastructure. Kafka earns its complexity when:

- You want **true horizontal scaling** across machines (each machine runs its own worker process, broker coordinates them)
- You want **crash resilience** — a worker dying mid-scan doesn't lose jobs; they're held in the topic until consumed
- You want the **scan result stream** to be tappable by other consumers (analytics, alerts, a trading bot)
- You want the architecture on your resume as a real-world Kafka use case

For an ad-hoc local scan, `docker compose up -d && python app.py` and get going.
