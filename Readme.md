# NSE Scanner — Kafka-Powered RS + Buy Signal

Real-time stock scanner for 200+ NSE stocks. Publishes scan jobs to Kafka, processes them in parallel across N worker processes, and surfaces stocks hitting **Relative Strength New High** × **Buy Signal** simultaneously.

**Live App:** [nsescanner-production.up.railway.app/app](https://nsescanner-production.up.railway.app/app)

> **Disclaimer:** Educational purposes only. Not financial advice.

---

## How It Works

```
POST /scan
    │
    ▼
app.py  ──► nse-scanner.scan-requests (Kafka topic, 8 partitions)
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
      worker 1  worker 2 ... worker 8   (scanner_worker.py × N)
          │         │         │
          └─────────┼─────────┘
                    ▼
        nse-scanner.scan-results (Kafka topic)
                    │
                    ▼
         app.py background collector
                    │
                    ▼
         scan_state (progress, rs_highs, buy_signals)
```

Each worker independently fetches OHLCV via yfinance, runs all indicator checks, and publishes results. The API's collector thread folds results into live scan state as they arrive.

---

## Scan Criteria

**Final Picks** = stocks passing **both** conditions:

| # | Filter | Condition |
|---|--------|-----------|
| RS | **Relative Strength New High** | RS line (Stock/Nifty50 × 7000) making new 123-day high |
| C1 | **DI+ Jump** | Plus DI increases ≥ 10 in one day (5-period) |
| C2 | **Volume Value** | Trade value > ₹5 Crore (price × volume > 50M) |
| C3 | **EMA Stack** | EMA 10 > EMA 20 > EMA 50 |
| C4 | **Weekly RSI** | Weekly RSI ≥ 59 |
| C5 | **SMA Uptrend** | 50-day SMA rising 5 consecutive days |
| C6 | **Volume Breakout** | Volume above 20-day SMA |

---

## Backtest Results (3 Years | 1:2 Risk-Reward)

Tested on 200+ NSE stocks, 2023–2026. Rules: max 5 concurrent positions, 20% capital per position, no re-entry while holding.

| Strategy | Win Rate | Expectancy | Profit Factor | Max Drawdown | Avg Hold |
|----------|----------|------------|---------------|--------------|----------|
| SL 3% → TGT 6% | 42.2% | +0.80% | 1.46x | -7.6% | 6 days |
| SL 5% → TGT 10% | 44.4% | +1.65% | 1.60x | -11.8% | 15 days |
| SL 7% → TGT 14% | 47.4% | +2.93% | 1.80x | -21.7% | 27 days |
| **SL 10% → TGT 20%** | **51.5%** | **+5.35%** | **2.13x** | -43.5% | 50 days |

**Key insight:** SL 5%/TGT 10% gives best risk-adjusted returns (-11.8% drawdown vs +1.65% expectancy). SL 10%/TGT 20% has highest absolute returns but -43.5% drawdown requires strong conviction.

---

## Local Setup

### Prerequisites

- Python 3.11+
- Docker Desktop (running)
- TA-Lib C binary ([Windows wheel](https://github.com/cgohlke/talib-build/releases) — pick your Python version)

### 1. Install dependencies

```bash
# Windows: install TA-Lib wheel first
pip install TA_Lib-0.4.xx-cp3xx-cp3xx-win_amd64.whl

pip install -r requirements.txt
```

### 2. Start Kafka

```bash
docker-compose up -d
```

Wait ~20 seconds, verify broker is ready:

```bash
docker-compose logs kafka | Select-Object -Last 5
# Look for: [KafkaServer id=1] started
```

### 3. Create Kafka topics

```bash
python kafka_admin.py
# Output: Created: nse-scanner.scan-requests, nse-scanner.scan-results (8 partitions each)
```

### 4. Start workers

Open up to 8 terminals (one per partition for max throughput):

```bash
python scanner_worker.py
```

Expected:
```
HH:MM:SS [INFO] connecting to Kafka @ localhost:9092
HH:MM:SS [INFO] ready — subscribed to 'nse-scanner.scan-requests', group 'nse-scanner-workers'
```

### 5. Start the API

```bash
python app.py
```

Open [http://localhost:8000/app](http://localhost:8000/app) → click **Run Scan**.

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/scan` | Trigger scan (publishes all symbols to Kafka) |
| `GET` | `/status` | Live progress: scanned count, current stock, Kafka health |
| `GET` | `/results` | Final results: rs_highs, buy_signals, final_stocks, details |
| `GET` | `/stocks` | List all symbols from stocks.txt |
| `GET` | `/stock/{symbol}` | OHLCV + RS line data for charting |
| `GET` | `/app` | Serve React frontend |
| `GET` | `/docs` | Swagger UI |

---

## Scaling

| Workers | Approx speed | Time for 216 stocks |
|---------|-------------|---------------------|
| 1 | ~1 stock/sec | ~3–4 min |
| 4 | ~3–4 stocks/sec | ~60–90 sec |
| 8 | ~5–7 stocks/sec | ~30–45 sec |

Max useful workers = number of partitions (default: 8). Beyond 8, extra workers sit idle. Beyond ~8 workers you may also hit yfinance rate limits (429s) — add `time.sleep(0.2)` in the worker loop if that happens.

---

## Worker Log Icons

| Icon | Meaning |
|------|---------|
| `★` | Passed both RS High + Buy Signal (final pick) |
| `✓` | Passed one of the two checks |
| `·` | Processed, no signal |
| `✗` | Error (yfinance failure, insufficient data, etc.) |

---

## Project Structure

```
NSE_Scanner/
├── app.py              # FastAPI backend + Kafka producer + result collector
├── scanner_worker.py   # Kafka consumer worker — run N instances
├── kafka_config.py     # Shared constants, topic names, stocks.txt loader
├── kafka_admin.py      # One-time topic creation (idempotent)
├── docker-compose.yml  # Zookeeper + Kafka broker
├── stocks.txt          # ~216 NSE symbols (edit freely)
├── index.html          # React frontend (single file, no build step)
├── backtest.py         # 3-year backtest engine (1:2 RR, 4 SL levels)
├── Dockerfile          # Docker build for Railway/cloud deploy
├── railway.toml        # Railway deployment config
└── backtest_output/
    ├── summary.csv
    ├── backtest_1to2.png
    ├── equity_SL3_TGT6.png
    ├── equity_SL5_TGT10.png
    ├── equity_SL7_TGT14.png
    ├── equity_SL10_TGT20.png
    ├── equity_all_combined.png
    └── trades_SL*_TGT*.csv
```

---

## Editing the Stock List

Edit `stocks.txt` — one symbol per line. `.NS` suffix optional (workers append it).

```
RELIANCE
TCS
INFY
# Excluded:
# HDFCBANK
ICICIBANK
```

Restart `app.py` after editing (list loads once at startup).

---

## Run the Backtest

```bash
python backtest.py
# Takes ~10–15 min (downloads 3yr data for 200+ stocks)
```

Outputs saved to `backtest_output/`:

| File | Description |
|------|-------------|
| `summary.csv` | All metrics in one table |
| `equity_SL*.png` | Equity curve per SL/target combo |
| `equity_all_combined.png` | All 4 strategies overlaid |
| `backtest_1to2.png` | Win rate / expectancy / profit factor chart |
| `trades_SL*_TGT*.csv` | Individual trade log per strategy |

---

## Resetting Kafka

```bash
# Stop containers (keep data)
docker-compose down

# Stop + wipe all topic data
docker-compose down -v
```

---

## Deploy to Railway

### API (existing Dockerfile works)

1. Push repo to GitHub
2. [railway.app](https://railway.app) → New Project → Deploy from GitHub
3. Settings → Networking → Generate Domain
4. App at `https://your-app.up.railway.app/app`

### Kafka in Production (Confluent Cloud free tier)

1. Sign up at [confluent.cloud](https://confluent.cloud) → create cluster → get bootstrap server + API key
2. Set env vars on Railway:
   - `KAFKA_BOOTSTRAP=pkc-xxxxx.confluent.cloud:9092`
3. Add SASL auth in `kafka_config.py` (see kafka-python docs for `sasl_plain_username` / `sasl_plain_password`)
4. Deploy workers as a separate Railway service: `python scanner_worker.py`

---

## Tech Stack

| Layer | Tech |
|-------|------|
| Backend | Python, FastAPI, uvicorn |
| Indicators | TA-Lib (C), pandas, NumPy |
| Market data | yfinance (Yahoo Finance, `.NS` suffix for NSE) |
| Message broker | Apache Kafka (via kafka-python) |
| Frontend | React (single HTML file, no build step) |
| Containerization | Docker, docker-compose |
| Deploy | Railway (Dockerfile), Confluent Cloud (managed Kafka) |

---

## License

MIT — use freely, modify as needed.
