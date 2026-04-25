"""
═══════════════════════════════════════════════════════════════════════════════
  BACKTEST — RS New High + Buy Signal | 1:2 Risk-Reward
  
  Entry:  When BOTH RS New High and Buy Signal fire
  Exit:   Stop-Loss at -X% or Target at +2X% (whichever hits first)
  
  Tests SL levels: 3%, 5%, 7%, 10% with 1:2 RR targets

  Run: pip install yfinance TA-Lib pandas numpy matplotlib
       python backtest.py
═══════════════════════════════════════════════════════════════════════════════
"""

import yfinance as yf
import talib
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
import os

warnings.filterwarnings('ignore')

# ── CONFIG ────────────────────────────────────────────────────────────────────

INDEX_SYMBOL = "^NSEI"
BACKTEST_YEARS = 3
MIN_DATA_DAYS = 200
INITIAL_CAPITAL = 1_000_000

RR_PAIRS = [
    (3, 6),
    (5, 10),
    (7, 14),
    (10, 20),
]

STOCK_LIST = [
    "CAMS.NS", "CONCOR.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS",
    "DLF.NS", "DABUR.NS", "DALBHARAT.NS", "DEEPAKNTR.NS", "DELHIVERY.NS",
    "DIVISLAB.NS", "DIXON.NS", "DRREDDY.NS", "ETERNAL.NS", "EICHERMOT.NS",
    "ESCORTS.NS", "EXIDEIND.NS", "NYKAA.NS", "GAIL.NS", "GMRAIRPORT.NS",
    "GLENMARK.NS", "GODREJCP.NS", "GODREJPROP.NS", "GRANULES.NS", "GRASIM.NS",
    "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HFCL.NS",
    "HAVELLS.NS", "HEROMOTOCO.NS", "HINDALCO.NS", "HAL.NS", "HINDCOPPER.NS",
    "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "HUDCO.NS", "ICICIBANK.NS",
    "ICICIGI.NS", "ICICIPRULI.NS", "IDFCFIRSTB.NS", "IRB.NS",
    "ITC.NS", "INDIANB.NS", "IEX.NS", "IOC.NS", "IRCTC.NS",
    "IRFC.NS", "IREDA.NS", "IGL.NS", "INDUSTOWER.NS", "INDUSINDBK.NS",
    "NAUKRI.NS", "INFY.NS", "INOXWIND.NS", "INDIGO.NS", "JSWENERGY.NS",
    "JSWSTEEL.NS", "JSL.NS", "JINDALSTEL.NS", "JIOFIN.NS", "JUBLFOOD.NS",
    "KEI.NS", "KPITTECH.NS", "KALYANKJIL.NS", "KOTAKBANK.NS", "LTF.NS",
    "LICHSGFIN.NS", "LTIM.NS", "LT.NS", "LAURUSLABS.NS", "LICI.NS",
    "LUPIN.NS", "MRF.NS", "LODHA.NS", "MGL.NS", "M&MFIN.NS",
    "M&M.NS", "MANAPPURAM.NS", "MARICO.NS", "MARUTI.NS", "MFSL.NS",
    "MAXHEALTH.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NBCC.NS",
    "NCC.NS", "NHPC.NS", "NMDC.NS", "NTPC.NS", "NATIONALUM.NS",
    "NESTLEIND.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "PAYTM.NS",
    "OFSS.NS", "POLICYBZR.NS", "PIIND.NS", "PNBHOUSING.NS", "PAGEIND.NS",
    "PATANJALI.NS", "PERSISTENT.NS", "PETRONET.NS", "PIDILITIND.NS", "PEL.NS",
    "POLYCAB.NS", "POONAWALLA.NS", "PFC.NS", "POWERGRID.NS", "PRESTIGE.NS",
    "PNB.NS", "RBLBANK.NS", "RECLTD.NS", "RELIANCE.NS", "SBICARD.NS",
    "SBILIFE.NS", "SHREECEM.NS", "SJVN.NS", "SRF.NS", "MOTHERSON.NS",
    "SHRIRAMFIN.NS", "SIEMENS.NS", "SOLARINDS.NS", "SONACOMS.NS", "SBIN.NS",
    "SAIL.NS", "SUNPHARMA.NS", "SUPREMEIND.NS", "SYNGENE.NS", "TATACONSUM.NS",
    "TITAGARH.NS", "TVSMOTOR.NS", "TATACHEM.NS", "TATACOMM.NS", "TCS.NS",
    "TATAELXSI.NS", "TATAMOTORS.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TATATECH.NS",
    "TECHM.NS", "FEDERALBNK.NS", "INDHOTEL.NS", "PHOENIXLTD.NS", "RAMCOCEM.NS",
    "TITAN.NS", "TORNTPHARM.NS", "TORNTPOWER.NS", "TRENT.NS", "TIINDIA.NS",
    "UPL.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UNITDSPR.NS", "VBL.NS",
    "VEDL.NS", "IDEA.NS", "VOLTAS.NS", "WIPRO.NS", "YESBANK.NS", "ZYDUSLIFE.NS",
    "BIOCON.NS", "APOLLOHOSP.NS", "AUBANK.NS", "BAJAJ-AUTO.NS", "BALKRISIND.NS",
    "BOSCHLTD.NS", "BRITANNIA.NS", "BHARTIARTL.NS", "APOLLOTYRE.NS", "AXISBANK.NS",
    "ASIANPAINT.NS", "CHAMBLFERT.NS", "BEL.NS", "BAJAJFINSV.NS", "COLPAL.NS",
    "CIPLA.NS", "ACC.NS", "BSE.NS", "ALKEM.NS", "ASHOKLEY.NS",
    "BANDHANBNK.NS", "CESC.NS", "COALINDIA.NS", "APLAPOLLO.NS", "DMART.NS",
    "CGPOWER.NS", "ABCAPITAL.NS", "ADANIENT.NS", "BHARATFORG.NS", "ABFRL.NS",
    "AMBUJACEM.NS", "BPCL.NS", "BAJFINANCE.NS", "CDSL.NS", "AUROPHARMA.NS",
    "ANGELONE.NS", "ABB.NS", "AARTIIND.NS", "BHEL.NS",
    "ADANIPORTS.NS", "ASTRAL.NS", "BSOFT.NS", "BANKBARODA.NS", "COFORGE.NS",
    "CHOLAFIN.NS", "ADANIGREEN.NS", "CANBK.NS", "ATGL.NS", "BANKINDIA.NS",
    "AMBER.NS",
]
STOCK_LIST = list(set(STOCK_LIST))

def safe_array(arr):
    return np.asarray(arr, dtype=np.float64)

def strip_tz(index):
    return index.tz_localize(None) if index.tz else index


# ── DOWNLOAD ──────────────────────────────────────────────────────────────────

def download_data():
    print(f"\n{'═'*70}\n  DOWNLOADING DATA\n{'═'*70}")
    end = datetime.now()
    start = end - timedelta(days=BACKTEST_YEARS * 365 + 400)
    print(f"  Period: {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}")
    print(f"  Stocks: {len(STOCK_LIST)}\n")

    print(f"  Downloading {INDEX_SYMBOL}...", end=" ", flush=True)
    idx = yf.Ticker(INDEX_SYMBOL).history(start=start, end=end)
    idx.index = strip_tz(idx.index)
    print(f"✓ ({len(idx)} days)")

    print(f"  Downloading {INDEX_SYMBOL} weekly...", end=" ", flush=True)
    idx_w = yf.Ticker(INDEX_SYMBOL).history(start=start, end=end, interval="1wk")
    idx_w.index = strip_tz(idx_w.index)
    print(f"✓ ({len(idx_w)} weeks)")

    stocks, stocks_w, failed = {}, {}, []
    for i, sym in enumerate(STOCK_LIST):
        print(f"\r  [{int((i+1)/len(STOCK_LIST)*100):3d}%] Downloading {sym:<20s}", end="", flush=True)
        try:
            d = yf.Ticker(sym).history(start=start, end=end)
            w = yf.Ticker(sym).history(start=start, end=end, interval="1wk")
            d.index = strip_tz(d.index)
            w.index = strip_tz(w.index)
            if len(d) >= MIN_DATA_DAYS:
                stocks[sym] = d
                stocks_w[sym] = w
            else:
                failed.append(sym)
        except:
            failed.append(sym)

    print(f"\r  ✅ Downloaded: {len(stocks)} stocks | Failed: {len(failed)}              ")
    return idx, idx_w, stocks, stocks_w


# ── GENERATE SIGNALS ──────────────────────────────────────────────────────────

def generate_signals(idx_data, stocks, stocks_w):
    print(f"\n{'═'*70}\n  GENERATING SIGNALS\n{'═'*70}")
    backtest_start = pd.Timestamp(datetime.now() - timedelta(days=BACKTEST_YEARS * 365))
    signals = []
    diag = {'total': 0, 'rs': 0, 'c1': 0, 'c2': 0, 'c3': 0, 'c4': 0, 'c5': 0, 'c6': 0, 'all': 0,
            'rs_and_c1': 0}
    idx_close = idx_data['Close']

    for n, sym in enumerate(stocks.keys()):
        print(f"\r  [{int((n+1)/len(stocks)*100):3d}%] {sym:<20s}", end="", flush=True)
        df = stocks[sym].copy()
        wdf = stocks_w.get(sym)
        if wdf is None or len(wdf) < 20:
            continue

        common = df.index.intersection(idx_close.index)
        if len(common) < MIN_DATA_DAYS:
            continue
        df = df.loc[common]
        idx_al = idx_close.loc[common]

        h, l, c, v = safe_array(df['High']), safe_array(df['Low']), safe_array(df['Close']), safe_array(df['Volume'])
        plus_di = talib.PLUS_DI(h, l, c, timeperiod=5)
        ema10, ema20, ema50 = talib.EMA(c, 10), talib.EMA(c, 20), talib.EMA(c, 50)
        sma50 = talib.SMA(c, 50)
        vol_sma = talib.SMA(v, 20)

        rs = (df['Close'] * 7000) / idx_al
        rs_high = rs.rolling(window=123, min_periods=1).max()

        w_rsi_arr = talib.RSI(safe_array(wdf['Close']), 14)
        w_rsi_s = pd.Series(w_rsi_arr, index=wdf.index)

        dates = df.index
        for i in range(MIN_DATA_DAYS, len(dates)):
            dt = dates[i]
            if dt < backtest_start:
                continue
            diag['total'] += 1

            try:
                # RS New High
                if np.isnan(rs.iloc[i]) or np.isnan(rs_high.shift(1).iloc[i]):
                    continue
                if rs.iloc[i] <= rs_high.shift(1).iloc[i]:
                    continue
                diag['rs'] += 1

                # C1: DI+ jump >= 10
                if np.isnan(plus_di[i]) or np.isnan(plus_di[i-1]):
                    continue
                di_diff = plus_di[i] - plus_di[i-1]
                c1 = di_diff >= 10
                if c1:
                    diag['c1'] += 1
                    diag['rs_and_c1'] += 1

                # C2: Vol value > 5Cr
                c2 = (c[i] * v[i]) > 50_000_000
                if c2: diag['c2'] += 1

                # C3: EMA stack
                if any(np.isnan(x) for x in [ema10[i], ema20[i], ema50[i]]):
                    continue
                c3 = ema10[i] > ema20[i] > ema50[i]
                if c3: diag['c3'] += 1

                # C4: Weekly RSI >= 59
                match = w_rsi_s.index[w_rsi_s.index <= dt]
                if len(match) == 0:
                    continue
                wrsi = w_rsi_s.loc[match[-1]]
                c4 = not np.isnan(wrsi) and wrsi >= 59
                if c4: diag['c4'] += 1

                # C5: SMA50 up 5 days
                if i < 4 or any(np.isnan(sma50[i-j]) for j in range(5)):
                    continue
                c5 = sma50[i] > sma50[i-1] > sma50[i-2] > sma50[i-3] > sma50[i-4]
                if c5: diag['c5'] += 1

                # C6: Volume > avg
                if np.isnan(vol_sma[i]):
                    continue
                c6 = v[i] > vol_sma[i]
                if c6: diag['c6'] += 1

                if all([c1, c2, c3, c4, c5, c6]):
                    diag['all'] += 1
                    signals.append({
                        'date': dt, 'symbol': sym,
                        'entry_price': float(c[i]),
                        'di_diff': round(float(di_diff), 1),
                        'weekly_rsi': round(float(wrsi), 1),
                        'vol_ratio': round(float(v[i]/vol_sma[i]), 2),
                    })
            except:
                continue

    signals.sort(key=lambda x: x['date'])

    # Diagnostics
    print(f"\n\n  {'─'*55}")
    print(f"  CONDITION DIAGNOSTICS  ({diag['total']:,} stock-days scanned)")
    print(f"  {'─'*55}")
    print(f"  {'RS New High:':<35s} {diag['rs']:>6,}")
    print(f"  {'  + DI+ jump ≥ 10 (C1):':<35s} {diag['c1']:>6,}  ← {'⚠️ BOTTLENECK' if diag['c1'] < 50 else ''}")
    print(f"  {'  + Vol Value > ₹5Cr (C2):':<35s} {diag['c2']:>6,}")
    print(f"  {'  + EMA 10>20>50 (C3):':<35s} {diag['c3']:>6,}")
    print(f"  {'  + Weekly RSI ≥ 59 (C4):':<35s} {diag['c4']:>6,}")
    print(f"  {'  + SMA50 uptrend (C5):':<35s} {diag['c5']:>6,}")
    print(f"  {'  + Volume > avg (C6):':<35s} {diag['c6']:>6,}")
    print(f"  {'─'*55}")
    print(f"  {'ALL CONDITIONS MET:':<35s} {diag['all']:>6,}")
    print(f"  {'─'*55}")
    print(f"  ✅ {len(signals)} signals across {len(set(s['symbol'] for s in signals)) if signals else 0} stocks")

    return signals, diag


# ── SIMULATE 1:2 RR ──────────────────────────────────────────────────────────

MAX_POSITIONS = 5      # Max 5 concurrent open positions
POSITION_SIZE_PCT = 20 # Each position = 20% of capital (5 x 20% = 100%)


def simulate_trades(signals, stocks, stop_pct, target_pct):
    """
    Realistic simulator:
    - 1 position per stock: if holding, skip all new signals for that stock
    - Once target/stop is hit, stock is free to be entered again on next signal
    - Max 5 concurrent positions
    """
    trades = []
    open_positions = []
    skipped_holding = 0
    skipped_full = 0

    sorted_sigs = sorted(signals, key=lambda x: x['date'])
    all_dates = sorted(set(d for df in stocks.values() for d in df.index.tolist()))

    signal_idx = 0
    equity = INITIAL_CAPITAL
    equity_curve = [{'date': all_dates[0], 'equity': equity}]

    for date in all_dates:
        # ── CHECK EXITS ──
        closed = []
        for pos in open_positions:
            df = stocks.get(pos['symbol'])
            if df is None or date not in df.index:
                continue

            lo = float(df.loc[date, 'Low'])
            hi = float(df.loc[date, 'High'])
            pos_size = INITIAL_CAPITAL * POSITION_SIZE_PCT / 100

            if lo <= pos['stop']:
                equity += pos_size * (-stop_pct / 100)
                pos.update({'exit_date': date, 'exit_price': pos['stop'],
                            'pnl_pct': round(-stop_pct, 2), 'result': 'STOP',
                            'days': len(df[(df.index > pos['entry_date']) & (df.index <= date)])})
                closed.append(pos)
            elif hi >= pos['target']:
                equity += pos_size * (target_pct / 100)
                pos.update({'exit_date': date, 'exit_price': pos['target'],
                            'pnl_pct': round(target_pct, 2), 'result': 'TARGET',
                            'days': len(df[(df.index > pos['entry_date']) & (df.index <= date)])})
                closed.append(pos)

        for pos in closed:
            open_positions.remove(pos)
            trades.append({
                'symbol': pos['symbol'].replace('.NS', ''),
                'entry_date': pos['entry_date'].strftime('%Y-%m-%d'),
                'exit_date': pos['exit_date'].strftime('%Y-%m-%d'),
                'entry': round(pos['entry_price'], 2),
                'target': round(pos['target'], 2),
                'stop': round(pos['stop'], 2),
                'exit': round(pos['exit_price'], 2),
                'pnl_pct': pos['pnl_pct'],
                'days': pos['days'],
                'result': pos['result'],
            })

        # ── CHECK NEW ENTRIES ──
        while signal_idx < len(sorted_sigs) and sorted_sigs[signal_idx]['date'] <= date:
            sig = sorted_sigs[signal_idx]
            signal_idx += 1
            if sig['date'] != date:
                continue

            sym = sig['symbol']

            # Already holding this stock? Skip until position closes
            if any(p['symbol'] == sym for p in open_positions):
                skipped_holding += 1
                continue

            # All 5 slots full?
            if len(open_positions) >= MAX_POSITIONS:
                skipped_full += 1
                continue

            # ENTER — 1 position in this stock
            open_positions.append({
                'symbol': sym,
                'entry_date': date,
                'entry_price': sig['entry_price'],
                'target': sig['entry_price'] * (1 + target_pct / 100),
                'stop': sig['entry_price'] * (1 - stop_pct / 100),
            })

        equity_curve.append({'date': date, 'equity': equity})

    # Close remaining open positions at last price
    for pos in open_positions:
        df = stocks.get(pos['symbol'])
        if df is not None and len(df) > 0:
            ep = float(df['Close'].iloc[-1])
            ed = df.index[-1]
            pnl = ((ep - pos['entry_price']) / pos['entry_price']) * 100
            equity += (INITIAL_CAPITAL * POSITION_SIZE_PCT / 100) * (pnl / 100)
            trades.append({
                'symbol': pos['symbol'].replace('.NS', ''),
                'entry_date': pos['entry_date'].strftime('%Y-%m-%d'),
                'exit_date': ed.strftime('%Y-%m-%d'),
                'entry': round(pos['entry_price'], 2),
                'target': round(pos['target'], 2),
                'stop': round(pos['stop'], 2),
                'exit': round(ep, 2),
                'pnl_pct': round(pnl, 2),
                'days': len(df[(df.index > pos['entry_date']) & (df.index <= ed)]),
                'result': 'open',
            })

    print(f"    Already holding: {skipped_holding} skipped | Slots full: {skipped_full} skipped | Trades taken: {len(trades)}")
    return trades, equity_curve


# ── STATS ─────────────────────────────────────────────────────────────────────

def compute_stats(trades, label):
    if not trades:
        return {'label': label, 'total': 0}
    pnls = [t['pnl_pct'] for t in trades]
    w = [p for p in pnls if p > 0]
    l = [p for p in pnls if p <= 0]
    tgt = len([t for t in trades if t['result']=='TARGET'])
    stp = len([t for t in trades if t['result']=='STOP'])
    opn = len([t for t in trades if t['result']=='open'])
    aw = np.mean(w) if w else 0
    al = abs(np.mean(l)) if l else 0
    return {
        'label': label, 'total': len(trades), 'winners': len(w), 'losers': len(l),
        'targets': tgt, 'stops': stp, 'open': opn,
        'win_rate': round(len(w)/len(trades)*100, 1),
        'avg_ret': round(np.mean(pnls),2), 'total_ret': round(sum(pnls),2),
        'avg_win': round(aw,2), 'avg_loss': round(-al,2),
        'best': round(max(pnls),2), 'worst': round(min(pnls),2),
        'pf': round(sum(w)/abs(sum(l)),2) if l else float('inf'),
        'expectancy': round((len(w)/len(trades)*aw) - (len(l)/len(trades)*al), 2),
        'avg_days': round(np.mean([t['days'] for t in trades]),1),
    }

def print_results(s):
    if s['total'] == 0:
        print(f"  {s['label']}: No trades"); return
    v = "✅ EDGE EXISTS" if s['expectancy'] > 0 else "❌ NO EDGE"
    print(f"""
  ┌────────────────────────────────────────────────────┐
  │  {s['label']:<50s}│
  ├────────────────────────────────────────────────────┤
  │  Trades: {s['total']:>4}  │ Targets: {s['targets']:>4}  │ Stops: {s['stops']:>4}    │
  │  Win Rate:     {s['win_rate']:>6.1f}%                          │
  │  Avg Return:  {s['avg_ret']:>+7.2f}%                           │
  │  Total Return:{s['total_ret']:>+8.2f}%                          │
  │  Avg Winner:  {s['avg_win']:>+7.2f}%  │ Avg Loser: {s['avg_loss']:>+7.2f}%     │
  │  Profit Factor: {s['pf']:>5.2f}x                            │
  │  Expectancy:  {s['expectancy']:>+7.2f}%/trade                      │
  │  Avg Holding:  {s['avg_days']:>5.1f} days                        │
  │  Final Equity: ₹{s['final_equity']:>12,.0f}  ({s['portfolio_ret']:>+.1f}%)     │
  │  Verdict: {v:<42s}│
  └────────────────────────────────────────────────────┘""")


# ── CHARTS ────────────────────────────────────────────────────────────────────

def plot_results(all_stats, all_trades, all_curves):
    os.makedirs("backtest_output", exist_ok=True)
    valid = [s for s in all_stats if s['total'] > 0]
    if not valid:
        print("  No trades to chart."); return

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle('1:2 Risk-Reward Backtest — RS New High + Buy Signal',
                 fontsize=14, fontweight='bold', color='#e2e8f0', y=1.02)
    fig.patch.set_facecolor('#0a0e17')
    for ax in axes:
        ax.set_facecolor('#111827')
        ax.tick_params(colors='#94a3b8')
        ax.title.set_color('#e2e8f0')
        for sp in ax.spines.values(): sp.set_color('#1e293b')

    labels = [s['label'] for s in valid]

    # Win Rate
    wr = [s['win_rate'] for s in valid]
    axes[0].barh(labels, wr, color=['#22c55e' if w>=50 else '#ef4444' for w in wr], height=0.5)
    axes[0].set_title('Win Rate %')
    axes[0].axvline(50, color='#f59e0b', ls='--', alpha=0.5)
    for i,v in enumerate(wr): axes[0].text(v+0.5, i, f'{v:.1f}%', va='center', color='#e2e8f0')

    # Expectancy
    exp = [s['expectancy'] for s in valid]
    axes[1].barh(labels, exp, color=['#22c55e' if e>0 else '#ef4444' for e in exp], height=0.5)
    axes[1].set_title('Expectancy %/trade')
    axes[1].axvline(0, color='#f59e0b', ls='--', alpha=0.5)
    for i,v in enumerate(exp): axes[1].text(v+0.1, i, f'{v:+.2f}%', va='center', color='#e2e8f0')

    # Profit Factor
    pf = [min(s['pf'],5) for s in valid]
    axes[2].barh(labels, pf, color=['#22c55e' if p>=1 else '#ef4444' for p in pf], height=0.5)
    axes[2].set_title('Profit Factor')
    axes[2].axvline(1, color='#f59e0b', ls='--', alpha=0.5)
    for i,v in enumerate(pf): axes[2].text(v+0.05, i, f'{valid[i]["pf"]:.2f}x', va='center', color='#e2e8f0')

    plt.tight_layout()
    plt.savefig('backtest_output/backtest_1to2.png', dpi=150, facecolor='#0a0e17', bbox_inches='tight')
    plt.close()
    print(f"  📊 Saved: backtest_output/backtest_1to2.png")

    # Equity curve from real portfolio tracking
    best_i = max(range(len(valid)), key=lambda i: valid[i]['expectancy'])

    colors = ['#f59e0b', '#3b82f6', '#a855f7', '#22c55e']

    # ── Individual equity curve for EACH strategy ──
    for i, (stats, eq_data) in enumerate(zip(valid, all_curves)):
        if not eq_data or len(eq_data) < 10:
            continue
        fig, ax = plt.subplots(figsize=(14, 5))
        fig.patch.set_facecolor('#0a0e17'); ax.set_facecolor('#111827')
        dates = [e['date'] for e in eq_data]
        curve = [e['equity'] for e in eq_data]
        ax.plot(dates, curve, color=colors[i], lw=1.5)
        ax.fill_between(dates, INITIAL_CAPITAL, curve,
                        where=[e >= INITIAL_CAPITAL for e in curve], alpha=0.12, color=colors[i])
        ax.fill_between(dates, INITIAL_CAPITAL, curve,
                        where=[e < INITIAL_CAPITAL for e in curve], alpha=0.12, color='#ef4444')
        ax.axhline(INITIAL_CAPITAL, color='#f59e0b', ls='--', alpha=0.4)
        ret = (curve[-1]-INITIAL_CAPITAL)/INITIAL_CAPITAL*100
        ax.set_title(f"Portfolio Equity — {stats['label']} | {MAX_POSITIONS} pos max, 1 per stock | ₹{curve[-1]:,.0f} ({ret:+.1f}%)",
                     color='#e2e8f0', fontsize=12, fontweight='bold')
        ax.set_xlabel('Date', color='#94a3b8'); ax.set_ylabel('₹', color='#94a3b8')
        ax.tick_params(colors='#94a3b8')
        for sp in ax.spines.values(): sp.set_color('#1e293b')
        plt.tight_layout()
        # Filename from label: "SL 3% → Target 6%" -> "equity_SL3_TGT6.png"
        fname = stats['label'].replace('SL ','SL').replace('% → Target ','_TGT').replace('%','')
        plt.savefig(f'backtest_output/equity_{fname}.png', dpi=150, facecolor='#0a0e17', bbox_inches='tight')
        plt.close()
        print(f"  📈 Saved: backtest_output/equity_{fname}.png")

    # ── Combined overlay of all 4 strategies ──
    fig, ax = plt.subplots(figsize=(16, 6))
    fig.patch.set_facecolor('#0a0e17'); ax.set_facecolor('#111827')
    for i, (stats, eq_data) in enumerate(zip(valid, all_curves)):
        if not eq_data or len(eq_data) < 10:
            continue
        dates = [e['date'] for e in eq_data]
        curve = [e['equity'] for e in eq_data]
        ret = (curve[-1]-INITIAL_CAPITAL)/INITIAL_CAPITAL*100
        ax.plot(dates, curve, color=colors[i], lw=1.5, label=f"{stats['label']} → ₹{curve[-1]:,.0f} ({ret:+.1f}%)")
    ax.axhline(INITIAL_CAPITAL, color='#64748b', ls='--', alpha=0.4)
    ax.set_title(f"All Strategies Compared | {MAX_POSITIONS} pos max, 1 per stock, ₹{INITIAL_CAPITAL:,.0f} start",
                 color='#e2e8f0', fontsize=13, fontweight='bold')
    ax.set_xlabel('Date', color='#94a3b8'); ax.set_ylabel('₹', color='#94a3b8')
    ax.tick_params(colors='#94a3b8')
    ax.legend(facecolor='#111827', edgecolor='#1e293b', labelcolor='#e2e8f0', fontsize=10, loc='upper left')
    for sp in ax.spines.values(): sp.set_color('#1e293b')
    plt.tight_layout()
    plt.savefig('backtest_output/equity_all_combined.png', dpi=150, facecolor='#0a0e17', bbox_inches='tight')
    plt.close()
    print(f"  📈 Saved: backtest_output/equity_all_combined.png")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'═'*70}")
    print(f"  BACKTEST — RS New High + Buy Signal | 1:2 Risk-Reward")
    print(f"{'═'*70}")
    print(f"  ₹{INITIAL_CAPITAL:,.0f} capital | {BACKTEST_YEARS}yr | {len(STOCK_LIST)} stocks")
    print(f"  Max {MAX_POSITIONS} positions | 1 position per stock | {POSITION_SIZE_PCT}% per position")

    idx, idx_w, stocks, stocks_w = download_data()
    signals, diag = generate_signals(idx, stocks, stocks_w)

    if not signals:
        print(f"\n  ⚠️  0 signals found. The DI+ ≥ 10 condition is the likely bottleneck.")
        print(f"  💡 To get signals, try lowering DI+ threshold in your scanner (e.g., ≥ 5)")
        print(f"     or reduce RSI threshold (e.g., ≥ 50).\n")
        return

    print(f"\n{'═'*70}\n  SIMULATING 1:2 RISK-REWARD (max {MAX_POSITIONS} positions, 1 per stock)\n{'═'*70}")
    all_stats, all_trades, all_curves = [], [], []
    for sl, tgt in RR_PAIRS:
        print(f"\n  Testing SL {sl}% → Target {tgt}%:")
        trades, eq_curve = simulate_trades(signals, stocks, sl, tgt)
        final_eq = eq_curve[-1]['equity'] if eq_curve else INITIAL_CAPITAL
        stats = compute_stats(trades, f"SL {sl}% → Target {tgt}%")
        stats['final_equity'] = round(final_eq)
        stats['portfolio_ret'] = round((final_eq - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100, 1)
        all_stats.append(stats)
        all_trades.append(trades)
        all_curves.append(eq_curve)
        print_results(stats)

    # Best
    valid = [s for s in all_stats if s['total'] > 0]
    if valid:
        best = max(valid, key=lambda x: x['expectancy'])
        print(f"\n  🏆 BEST: {best['label']} | WR {best['win_rate']}% | Exp {best['expectancy']:+.2f}% | PF {best['pf']}x")

    # Last signals
    print(f"\n  📋 LAST 10 SIGNALS:")
    print(f"  {'Date':<12} {'Stock':<14} {'Price':>10} {'DI+Δ':>7} {'RSI':>6} {'Vol':>6}")
    for s in signals[-10:]:
        print(f"  {s['date'].strftime('%Y-%m-%d')}  {s['symbol'].replace('.NS',''):<14} ₹{s['entry_price']:>9,.2f} {s['di_diff']:>+6.1f} {s['weekly_rsi']:>5.1f} {s['vol_ratio']:>5.1f}x")

    # Save
    print(f"\n  📊 GENERATING CHARTS...")
    plot_results(all_stats, all_trades, all_curves)
    os.makedirs("backtest_output", exist_ok=True)
    for i,(sl,tgt) in enumerate(RR_PAIRS):
        if all_trades[i]:
            pd.DataFrame(all_trades[i]).to_csv(f"backtest_output/trades_SL{sl}_TGT{tgt}.csv", index=False)
    pd.DataFrame(all_stats).to_csv("backtest_output/summary.csv", index=False)
    print(f"  💾 Saved to backtest_output/")
    print(f"\n{'═'*70}\n  ✅ DONE\n{'═'*70}\n")

if __name__ == "__main__":
    main()