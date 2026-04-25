# ⚡ NSE Scanner — RS New High × Buy Signal

A real-time stock scanner + backtested trading strategy for 200+ NSE stocks. Scans for **Relative Strength New Highs** combined with multi-factor **Buy Signals**, then surfaces the intersection as final picks.

**🔴 Live App:** [nsescanner-production.up.railway.app/app](https://nsescanner-production.up.railway.app/app)

> ⚠️ **Disclaimer**: For educational purposes only. Not financial advice.

---

## 📋 Scan Criteria

**Final Picks** = Stocks hitting BOTH conditions simultaneously:

| # | Filter | Condition |
|---|--------|-----------|
| RS | **Relative Strength New High** | RS line (Stock/Nifty50) making a new 123-day high |
| C1 | **DI+ Jump** | Plus DI increases by ≥ 10 in a single day (5-period) |
| C2 | **Volume Value** | Trade value > ₹5 Crore |
| C3 | **EMA Stack** | EMA 10 > EMA 20 > EMA 50 (bullish alignment) |
| C4 | **Weekly RSI** | Weekly RSI ≥ 59 (momentum confirmation) |
| C5 | **SMA Trend** | 50-day SMA rising for 5 consecutive days |
| C6 | **Volume Spike** | Volume above 20-day average |

---

## 📊 Backtest Results (3 Years | 1:2 Risk-Reward)

Backtested over 2023–2026 on 200+ NSE stocks with **max 5 concurrent positions**, **1 position per stock** (no re-entry while holding), and **20% capital per position**.

### Strategy Comparison

| Strategy | Win Rate | Expectancy | Profit Factor | Max Drawdown | Avg Holding |
|----------|----------|------------|---------------|--------------|-------------|
| SL 3% → Target 6% | 42.2% | +0.80% | 1.46x | -7.6% | 6 days |
| SL 5% → Target 10% | 44.4% | +1.65% | 1.60x | -11.8% | 15 days |
| SL 7% → Target 14% | 47.4% | +2.93% | 1.80x | -21.7% | 27 days |
| **SL 10% → Target 20%** | **51.5%** | **+5.35%** | **2.13x** | -43.5% | 50 days |

### Equity Curves

**SL 10% / Target 20% (Best Expectancy)**

![Equity SL10 TGT20](backtest_results/equity_SL10_TGT20.png)

**All Strategies Combined**

![All Strategies](backtest_results/equity_all_combined.png)

**Strategy Comparison Charts**

![Backtest Comparison](backtest_results/backtest_1to2.png)

### Position Management Rules

- **Max 5 stocks** held at the same time
- **1 position per stock** — if scanner signals a stock you already hold, skip it
- **No re-entry** until position is closed (target or stop hit)
- **Once closed**, the stock is immediately available for re-entry on next signal
- **20% capital** allocated per position (5 × 20% = 100%)

### Key Insights

- All 4 RR setups are profitable — the strategy has a genuine edge
- Wider stops = higher win rate (more room to breathe) but deeper drawdowns
- **SL 5% / TGT 10%** offers the best risk-adjusted balance: decent expectancy with manageable -11.8% max drawdown
- **SL 10% / TGT 20%** has the highest returns but -43.5% drawdown — requires strong conviction
- Strategy works best in trending markets; bleeds during choppy/bearish phases (late 2024 – early 2026)

---

## 🚀 Quick Start

### Run Locally

```bash
# 1. Install TA-Lib C library
# macOS:
brew install ta-lib
# Ubuntu:
sudo apt-get install -y build-essential wget
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib && ./configure --prefix=/usr && make && sudo make install

# 2. Install Python deps
pip install -r requirements.txt

# 3. Run
python app.py

# 4. Open browser
# http://localhost:8000/app
```

### Deploy to Railway

1. Push repo to GitHub
2. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub**
3. Select your repo (Railway auto-detects the Dockerfile)
4. After deploy → **Settings** → **Networking** → **Generate Domain**
5. Access at `https://your-app.up.railway.app/app`

---

## 🧪 Run the Backtest

```bash
# Make sure TA-Lib is installed (see above)
pip install yfinance TA-Lib pandas numpy matplotlib

# Run backtest (takes ~10-15 min to download 3yr data for 200+ stocks)
python backtest.py
```

**Outputs saved to `backtest_output/`:**

| File | Description |
|------|-------------|
| `equity_SL3_TGT6.png` | Equity curve — 3% SL / 6% target |
| `equity_SL5_TGT10.png` | Equity curve — 5% SL / 10% target |
| `equity_SL7_TGT14.png` | Equity curve — 7% SL / 14% target |
| `equity_SL10_TGT20.png` | Equity curve — 10% SL / 20% target |
| `equity_all_combined.png` | All 4 strategies overlaid |
| `backtest_1to2.png` | Win rate, expectancy, profit factor comparison |
| `summary.csv` | All metrics in one table |
| `trades_SL*_TGT*.csv` | Individual trade logs per strategy |

---

## 📁 Project Structure

```
NSE_Scanner/
├── app.py                  # FastAPI backend + scanner logic
├── index.html              # React frontend (single file, no build step)
├── backtest.py             # Backtesting engine (1:2 RR)
├── requirements.txt        # Python dependencies
├── Dockerfile              # Docker build for Railway/cloud deploy
├── railway.toml            # Railway deployment config
├── Readme.md
└── backtest_results/       # Backtest output charts + CSVs
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

## 🔌 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/scan` | Start a background scan |
| `GET` | `/status` | Get scan progress |
| `GET` | `/results` | Get scan results |
| `GET` | `/stock/{symbol}` | Get chart data for a stock |
| `GET` | `/app` | Serve the frontend |

---

## 🛠️ Customization

**Add/Remove Stocks** — Edit `STOCK_LIST` in `app.py`

**Change Scan Parameters** in `app.py`:
- `window=123` — RS lookback period
- `timeperiod=5` — DI period
- `timeperiod=10/20/50` — EMA periods
- `>= 59` — Weekly RSI threshold
- `> 50_000_000` — Minimum volume value (₹5Cr)

**Change Backtest Settings** in `backtest.py`:
- `BACKTEST_YEARS = 3` — History to test
- `MAX_POSITIONS = 5` — Concurrent positions
- `RR_PAIRS` — Stop-loss / target combinations

---

## 🧰 Tech Stack

- **Backend**: Python, FastAPI, TA-Lib, yfinance, pandas, NumPy
- **Frontend**: React (single HTML file, no build step)
- **Deploy**: Docker, Railway
- **Data**: Yahoo Finance API (NSE stocks via `.NS` suffix)

---

## 📝 License

MIT — Use freely, modify as you like.