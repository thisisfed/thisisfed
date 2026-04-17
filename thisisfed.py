import yfinance as yf
import pandas as pd
import numpy as np
import random
import warnings
from datetime import datetime, time as dt_time
import matplotlib.pyplot as plt
import os
import json
import time
import statistics
from itertools import product
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import requests
from collections import defaultdict, Counter
import threading
from dotenv import load_dotenv
import pandas_market_calendars as mcal
import argparse
import traceback
import sys
import signal
import schedule

warnings.filterwarnings("ignore")
random.seed(42)
np.random.seed(42)

# ========================== CLI ARGUMENTS ==========================
parser = argparse.ArgumentParser(description="Momentum Bot v11.5")
parser.add_argument("--force", action="store_true", help="Force run even on non-trading days")
parser.add_argument("mode", nargs="?", choices=["test", "live"], default="live",
                    help="Run mode: 'test' for full WFO + Monte Carlo, 'live' for normal operation")
args = parser.parse_args()

FORCE_RUN = args.force
TEST_MODE = args.mode == "test"

# ========================== LOAD .ENV ==========================
load_dotenv()

# ========================== CONFIG ==========================
FILES_DIR = "files"
STOCKS_DIR = os.path.join(FILES_DIR, "stocks")

DYNAMIC_WATCHLIST_FILE = os.path.join(STOCKS_DIR, "dynamic_watchlist.json")
OPEN_POSITIONS_FILE = os.path.join(FILES_DIR, "open_positions.json")
BEST_PARAMS_FILE = os.path.join(FILES_DIR, "walk_forward", "best_params.json")
SECTOR_MAP_FILE = os.path.join(STOCKS_DIR, "sector_map.json")

WFO_DIR = os.path.join(FILES_DIR, "walk_forward")
WFO_CACHE_DIR = os.path.join(WFO_DIR, "cache")

for d in [FILES_DIR, STOCKS_DIR, WFO_DIR, WFO_CACHE_DIR]:
    os.makedirs(d, exist_ok=True)

# ========================== DUAL SCAN CONFIG ==========================
LATE_AFTERNOON_SCAN_TIME = "15:40"
EVENING_SCAN_TIME = "22:30"
LAST_HOUR_STRENGTH_FILTER = True

# ========================== CONTROL FLAGS ==========================
LIVE_MODE = True
AGGRESSIVE_MODE = True
SEND_TRADE_NOTIFICATIONS = False

RUN_MONTE_CARLO_LIVE = False
NUM_MONTE_CARLO_LIVE = 5000

# ========================== TELEGRAM ==========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("Missing TELEGRAM_TOKEN or TELEGRAM_CHAT_ID in .env file!")

print("✅ Telegram credentials loaded from .env")

# ========================== MARKET CALENDAR (NYSE) ==========================
nyse = mcal.get_calendar('NYSE')

def is_trading_day(current_date=None):
    if current_date is None:
        current_date = pd.Timestamp.now().normalize()
    else:
        current_date = pd.to_datetime(current_date).normalize()
    schedule = nyse.schedule(start_date=current_date, end_date=current_date)
    return not schedule.empty

# ========================== DYNAMIC WATCHLIST ==========================
DYNAMIC_WATCHLIST_ENABLED = True
WATCHLIST_REFRESH_MONTHS = 3
DYNAMIC_TARGET_SIZE = 180

ADDITIONAL_UNIVERSE = [
    "AEP", "APD", "ABBV", "ACN", "AIG", "BA", "BAC", "BLK", "BRK-B", "BSX", "BX",
    "CSCO", "CAT", "CHTR", "CI", "CMCSA", "CMI", "COF", "COP", "CVX", "DE", "DIS",
    "DOW", "DUK", "ECL", "ELV", "EMR", "EOG", "ETN", "GD", "GE", "GILD", "GS",
    "HCA", "HON", "HUM", "IBM", "INTC", "ITW", "JNJ", "JPM", "KHC", "KMI", "LIN",
    "LMT", "LYB", "MPC", "MRK", "NEE", "NOC", "NVO", "OKE", "ORCL", "PCAR", "PFE",
    "PG", "PH", "PGR", "PM", "PSX", "RTX", "SHW", "SLB", "SO", "SYF", "SYK",
    "T", "TMO", "TMUS", "UNH", "URI", "VLO", "VZ", "WBD", "WM", "XOM", "ZTS"
]

STATIC_WATCHLIST = [
    "NVDA", "AVGO", "SMCI", "AMD", "ARM", "TSM", "MU", "CRWD", "PANW", "NOW",
    "TSLA", "META", "AMZN", "NFLX", "SHOP", "UBER", "HOOD", "COIN", "PLTR",
    "LLY", "APP", "ASTS", "AAPL", "MSFT", "GOOGL", "GOOG", "ADBE", "CRM",
    "INTU", "SNOW", "DDOG", "ZS", "NET", "MDB", "TEAM", "WDAY", "HUBS",
    "CDNS", "KLAC", "LRCX", "ASML", "AMAT", "TXN", "QCOM", "MRVL",
    "NXPI", "ADI", "MPWR", "ON", "SWKS", "V", "MA", "PYPL", "AFRM",
    "SOFI", "UPST", "RBLX", "EA", "TTWO", "BKNG", "ABNB", "MAR", "HLT",
    "RCL", "CCL", "NCLH", "DAL", "UAL", "LUV", "CMG", "DPZ", "YUM", "MCD",
    "SBUX", "KO", "PEP", "MNST", "KDP", "COST", "WMT", "TGT", "HD", "LOW",
    "NKE", "LULU", "DECK", "ULTA", "EL", "CLX", "PG", "CL", "KMB", "GIS",
    "KHC", "MDLZ", "MO", "PM", "VRTX", "REGN", "MRNA", "BIIB", "INCY",
    "SRPT", "TECH", "IDXX", "DXCM", "PODD", "ISRG", "MDT", "ABT", "JNJ",
    "PFE", "MRK", "UNH", "CVS", "CI", "HUM", "ELV", "AXP", "COF",
    "SYF", "BAC", "JPM", "WFC", "GS", "MS", "BLK", "CAT", "DE", "URI",
    "PCAR", "CMI", "ETN", "PH", "ITW", "EMR", "HON", "GE", "RTX", "LMT",
    "BA", "GD", "NOC", "WM", "RSG", "CTAS"
]

LARGE_UNIVERSE = sorted(list(set(STATIC_WATCHLIST + ADDITIONAL_UNIVERSE)))

# ========================== ROBINHOOD UK FEES ==========================
FX_TICKER = "GBPUSD=X"
STARTING_CAPITAL_GBP = 22000.0
MONTHLY_DEPOSIT_GBP = 600.0

ROBINHOOD_NORMAL_FX_PCT = 0.0010
ROBINHOOD_FRIDAY_SPIKE_PCT = 0.0030

PER_SIDE_COMMISSION_PCT = 0.0
BASE_SLIPPAGE = 0.0008

TRADING_ACTIVITY_FEE_PER_SHARE = 0.000195
REGULATORY_FEE_PER_MILLION = 20.60
REGULATORY_FEE_MIN_NOTIONAL = 500.0
TAF_MAX_FEE = 9.79
TAF_SHARE_EXEMPTION = 50

# ========================== CASH SLEEVE ==========================
ENABLE_CASH_SLEEVE = True
FORCE_CLOSE_ON_REGIME_OFF = True
VIX_FILTER_ENABLED = True

# ========================== STRATEGY PARAMETERS ==========================
NUM_MONTE_CARLO = 30000
WFO_REOPTIMIZE_EVERY_MONTHS = 6
WFO_ANCHOR_DATE = "2018-01-01"
WFO_IS_MONTHS = 36
WFO_OOS_MONTHS = 9
WFO_STEP_MONTHS = 6

BACKTEST_START_DATE = "2018-01-01"
MAX_HOLD_DAYS = 180
MAX_POSITION_SIZE_PCT = 0.40
ATR_STOP_MULT = 2.0

MAX_LOSS_PER_TRADE_R = 3.0
RUNNER_TRAIL_BASE_PCT = 0.38
MIN_TRAIL_PCT = 0.22
TRAIL_MULT = 3.3

RS_MONTHS = 6
TOP_N = 10              
MAX_PER_SECTOR = 2
VOL_PARITY_FLOOR = 0.30
VOL_PARITY_CEIL = 1.3

WFO_PARAM_GRID = {
    "risk_per_trade_pct": [0.009, 0.011, 0.013],
    "runner_trail_base_pct": [0.36, 0.42],
    "max_position_size_pct": [0.38, 0.45],
    "trail_atr_mult": [3.0, 3.6],
    "trail_tighten_div": [110],
    "abs_mom_weight": [1.95],
    "top_n": [9, 12],
    "vix_threshold": [34, 40],
    "sma200_required": [False],
}

print(f"WFO_PARAM_GRID size: {len(list(product(*[WFO_PARAM_GRID[k] for k in WFO_PARAM_GRID.keys()])))} combinations")

if AGGRESSIVE_MODE:
    print("AGGRESSIVE_MODE ENABLED → v11.5 Live (4R partial + 8% pyramiding)")

# ========================== TELEGRAM HELPERS ==========================
def send_telegram_message(message: str, parse_mode="HTML", force=False):
    if not (LIVE_MODE and (SEND_TRADE_NOTIFICATIONS or force)):
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": parse_mode}, timeout=10)
    except:
        pass

def nice_reason(reason: str) -> str:
    mapping = {
        "partial_4R": "Partial 4R profit taken",
        "stop": "Stop loss hit",
        "max_hold": "Maximum hold period reached",
        "max_loss": "Maximum loss per trade hit",
        "runner_trail": "Runner trail stop triggered",
        "regime_off_cash_sleeve": "Regime off - moved to cash sleeve",
        "rebalance_out": "Rebalanced out of position",
    }
    return mapping.get(reason, reason.replace("_", " ").title())

# ========================== ALERTS ==========================
def send_alert(title: str, details: str, is_crash: bool = True):
    emoji = "🚨" if is_crash else "🛑"
    header = "BOT CRASHED — IMMEDIATE ATTENTION REQUIRED" if is_crash else "BOT SHUT DOWN GRACEFULLY"
    
    msg = f"{emoji} <b>{header}</b> {emoji}\n\n" \
          f"{details}\n\n" \
          f"Time: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"

    print(f"\n{emoji} {header}")
    send_telegram_message(msg, force=True)

def send_crash_alert(exctype, value, tb):
    traceback_str = "".join(traceback.format_exception(exctype, value, tb))
    if len(traceback_str) > 2800:
        traceback_str = traceback_str[:2800] + "\n\n... (truncated)"
    
    details = f"<b>Error:</b> {exctype.__name__}\n" \
              f"<b>Message:</b> {value}\n\n" \
              f"<b>Traceback:</b>\n<pre>{traceback_str}</pre>"
    
    send_alert("CRASH", details, is_crash=True)

def graceful_shutdown(signum, frame):
    signame = signal.Signals(signum).name
    details = f"<b>Type:</b> Graceful Shutdown\n" \
              f"<b>Signal:</b> {signame}\n\n" \
              f"Bot was stopped cleanly. No crash occurred."
    
    send_alert("SHUTDOWN", details, is_crash=False)
    sys.exit(0)

# ========================== SHARED VARIABLES ==========================
open_trades_shared = []
next_wfo_months_global = 0
data = {}
DYNAMIC_WATCHLIST = []
SECTOR_MAP = {}

# ========================== DAILY START MESSAGE ==========================
def send_daily_start_message(equity_gbp: float, scan_type: str):
    if not (LIVE_MODE and SEND_TRADE_NOTIFICATIONS):
        return

    regime_ok = True
    vix_high = False
    if "SPY" in data and not data["SPY"].empty and len(data["SPY"]) > 200:
        spy_close = float(data["SPY"]["Close"].iloc[-1])
        spy_sma200 = float(data["SPY"]["SMA200"].iloc[-1]) if "SMA200" in data["SPY"].columns else 0
        if spy_close < spy_sma200:
            regime_ok = False
    vix_df = get_data("^VIX")
    if not vix_df.empty:
        current_vix = float(vix_df["Close"].iloc[-1])
        if current_vix > 32:
            vix_high = True

    status = "🟢 Looking for entries" if regime_ok and not vix_high else "🔴 Cash sleeve active"

    open_list = ""
    if open_trades_shared:
        open_list = "\n\n<b>Open Positions:</b>"
        for t in open_trades_shared[:8]:
            entry = t.get("entry_price", 0)
            current = t.get("current_price", entry)
            pct = ((current / entry) - 1) * 100 if entry > 0 else 0
            open_list += f"\n• {t.get('ticker')} ({pct:+.1f}%)"
        if len(open_trades_shared) > 8:
            open_list += f"\n... and {len(open_trades_shared)-8} more"
    else:
        open_list = "\n\nNo open positions"

    msg = f"<b>🤖 {scan_type} Scan Started - v11.5</b>\n\n" \
          f"Equity: £{equity_gbp:,.0f}\n" \
          f"Open Positions: {len(open_trades_shared)}\n" \
          f"Status: {status}\n" \
          f"Time: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}" \
          f"{open_list}"

    send_telegram_message(msg, force=True)

# ========================== TELEGRAM STATUS POLLER ==========================
def telegram_status_poller():
    global open_trades_shared, next_wfo_months_global
    last_update_id = 0
    print("📡 Telegram status poller started → send 'status' anytime")

    send_telegram_message("✅ Status poller is now ACTIVE and responsive.\nYou will get a reply every time you send 'status'.", force=True)

    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates?offset={last_update_id + 1}&timeout=30&allowed_updates=message"
            response = requests.get(url, timeout=40).json()

            if response.get("ok") and response.get("result"):
                for update in response["result"]:
                    last_update_id = max(last_update_id, update.get("update_id", 0))

                    if "message" in update and "text" in update["message"]:
                        text = update["message"]["text"].strip().lower()
                        chat_id = str(update["message"]["chat"]["id"])

                        print(f"📨 Received message: '{text}' from chat {chat_id}")

                        if text == "status" and chat_id == TELEGRAM_CHAT_ID:
                            num_open = len(open_trades_shared)
                            
                            open_list = "\n".join([
                                f"• {t.get('ticker')} ({((t.get('current_price', t.get('entry_price', 0)) / t.get('entry_price', 1)) - 1) * 100:+.1f}%)"
                                for t in open_trades_shared[:12]
                            ]) if num_open > 0 else "No open positions"

                            if len(open_trades_shared) > 12:
                                open_list += f"\n... and {len(open_trades_shared)-12} more"

                            equity_display = STARTING_CAPITAL_GBP
                            try:
                                equity_display = float(open_trades_shared[0].get("equity_gbp", STARTING_CAPITAL_GBP)) if open_trades_shared else STARTING_CAPITAL_GBP
                            except:
                                pass

                            msg = f"<b>🤖 STRATEGY STATUS — LIVE</b>\n\n" \
                                  f"Equity: £{equity_display:,.0f}\n" \
                                  f"Open Positions: {num_open}\n" \
                                  f"Next WFO in: {next_wfo_months_global} months\n" \
                                  f"Time: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n" \
                                  f"<b>Open Trades:</b>\n{open_list}"

                            send_telegram_message(msg, force=True)
                            print(f"✅ Replied to 'status' at {datetime.now().strftime('%H:%M:%S')}")

        except Exception as e:
            print(f"⚠️ Telegram poller thread crashed: {type(e).__name__} - {e}")
            time.sleep(5)

        time.sleep(1.0)

# ========================== HELPERS ==========================
progress_lock = threading.Lock()

def progress_bar(title: str, completed: int, total: int, bar_length: int = 40):
    if total == 0:
        return
    percent = completed / total
    filled_length = int(bar_length * percent)
    bar = '█' * filled_length + '░' * (bar_length - filled_length)
    with progress_lock:
        print(f"\r{title}: [{bar}] {completed}/{total} ({percent:.1%})", end='', flush=True)
        if completed >= total:
            print()

def get_data(ticker: str, interval="1d"):
    cache_file = os.path.join(STOCKS_DIR, f"{ticker}_{interval}.pkl")
    if os.path.exists(cache_file):
        try:
            df = pd.read_pickle(cache_file)
            if not df.empty:
                last_date = pd.to_datetime(df.index[-1]).date()
                now_date = datetime.now().date()
                if (now_date - last_date).days <= 3:
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.get_level_values(0)
                    for col in ["Open", "High", "Low", "Close", "ATR", "SMA200", "200d_low"]:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    return df
        except:
            pass

    print(f"Downloading/Updating {ticker}...")
    max_retries = 3
    for attempt in range(max_retries):
        try:
            df = yf.download(
                ticker,
                start=BACKTEST_START_DATE,
                interval=interval,
                progress=False,
                auto_adjust=True,
                timeout=15
            )
            if df is not None and not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
                
                for col in ["Open", "High", "Low", "Close", "Volume"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df.to_pickle(cache_file)
                return df
        except Exception as e:
            print(f"  → Error downloading {ticker}: {type(e).__name__}")
            if attempt < max_retries - 1:
                time.sleep(2 + attempt * 2)
    
    return pd.DataFrame()

def build_sector_map(tickers):
    if os.path.exists(SECTOR_MAP_FILE):
        try:
            with open(SECTOR_MAP_FILE) as f:
                return json.load(f)
        except:
            pass

    print("\n=== Building sector map (robust version) ===\n")
    sector_map = {}
    failed = []

    for t in tickers:
        t_clean = t.strip().upper()
        if not t_clean:
            continue
        try:
            ticker_obj = yf.Ticker(t_clean)
            info = ticker_obj.info or {}
            sector = (info.get('sector') or info.get('industry') or info.get('longBusinessSummary') or "UNKNOWN")
            clean = str(sector).upper().replace(" ", "_").replace("-", "_").replace("&", "AND").replace("/", "_").replace(",", "").replace("'", "")
            sector_map[t_clean] = clean
            print(f"{t_clean:8} → {clean}")
        except Exception as e:
            sector_map[t_clean] = "UNKNOWN"
            failed.append(t_clean)
            print(f"{t_clean:8} → UNKNOWN (error: {type(e).__name__})")
        time.sleep(0.8)

    with open(SECTOR_MAP_FILE, "w") as f:
        json.dump(sector_map, f, indent=2)

    print(f"\nSector map built. {len(sector_map)} tickers mapped. Failed: {len(failed)}")
    if failed:
        print("Failed tickers:", failed[:30], "..." if len(failed) > 30 else "")

    return sector_map

def build_dynamic_watchlist(universe, data, target_size=DYNAMIC_TARGET_SIZE, verbose=True, save_to_file=True):
    if verbose:
        print("\n=== Building dynamic watchlist (v11.0 + Quality SMA200 filter) ===")
    if "SPY" not in data or data["SPY"].empty:
        if verbose:
            print("→ SPY missing, falling back to static watchlist")
        return STATIC_WATCHLIST[:target_size]

    spy_df = data["SPY"]
    spy_close = spy_df.get("Close")
    if isinstance(spy_close, pd.DataFrame):
        spy_close = spy_close.iloc[:, 0]

    candidates = []
    for ticker in universe:
        if ticker == "SPY" or ticker not in data or data[ticker].empty:
            continue
        df = data[ticker]
        if len(df) < 150:
            continue

        vol_col = df.get("Volume")
        if isinstance(vol_col, pd.DataFrame):
            vol_col = vol_col.iloc[:, 0]
        vol_ma = vol_col.rolling(20).mean()
        if vol_ma.empty:
            continue
        last_vol_raw = vol_ma.iloc[-1]
        last_vol = float(last_vol_raw.item() if hasattr(last_vol_raw, 'item') else last_vol_raw)
        if pd.isna(last_vol) or last_vol < 400000:
            continue

        price_col = df.get("Close")
        if isinstance(price_col, pd.DataFrame):
            price_col = price_col.iloc[:, 0]
        price_raw = price_col.iloc[-1]
        price = float(price_raw.item() if hasattr(price_raw, 'item') else price_raw)
        if pd.isna(price) or price < 5.0:
            continue

        sma200_col = df.get("SMA200")
        if isinstance(sma200_col, pd.DataFrame):
            sma200_col = sma200_col.iloc[:, 0]
        sma_val = float(sma200_col.iloc[-1]) if len(sma200_col) > 0 and not pd.isna(sma200_col.iloc[-1]) else 0
        close_now = float(price_col.iloc[-1])
        if close_now <= sma_val:
            continue

        period = RS_MONTHS * 21
        if len(df) < period + 10:
            continue

        close_past = float(price_col.iloc[-period] if len(price_col) > period else price_col.iloc[0])
        stock_ret = close_now / close_past if close_past > 0 else 0.0

        spy_now = float(spy_close.iloc[-1])
        spy_past = float(spy_close.iloc[-period] if len(spy_close) >= period else spy_close.iloc[0])
        spy_ret = spy_now / spy_past if spy_past > 0 else 1.0

        rs = stock_ret / spy_ret if spy_ret > 0 else 0.0
        if rs > 0.8:
            candidates.append((ticker, rs))

    candidates.sort(key=lambda x: x[1], reverse=True)
    dynamic_list = [t[0] for t in candidates[:target_size]]

    if save_to_file:
        with open(DYNAMIC_WATCHLIST_FILE, "w") as f:
            json.dump({"watchlist": dynamic_list, "timestamp": datetime.now().isoformat(), "size": len(dynamic_list)}, f, indent=2)

    if verbose:
        print(f"→ Selected {len(dynamic_list)} stocks for dynamic watchlist (quality SMA200 filter applied)")
    return dynamic_list

def save_best_params(best_params):
    data = {"params": best_params, "timestamp": datetime.now().isoformat()}
    with open(BEST_PARAMS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def load_best_params():
    if os.path.exists(BEST_PARAMS_FILE):
        try:
            with open(BEST_PARAMS_FILE) as f:
                return json.load(f).get("params", {})
        except:
            pass
    return {
        "runner_trail_base_pct": 0.38,
        "trail_atr_mult": 3.3,
        "risk_per_trade_pct": 0.011,
        "max_position_size_pct": 0.45,
        "trail_tighten_div": 110,
        "abs_mom_weight": 1.95,
        "sma200_required": False,
        "top_n": 11,
        "vix_threshold": 38,
    }

def get_window_hash(start_date, end_date):
    key = f"{start_date.date()}_{end_date.date()}"
    return hashlib.md5(key.encode()).hexdigest()

def load_cached_best_params(window_hash):
    cache_file = os.path.join(WFO_CACHE_DIR, f"{window_hash}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file) as f:
                return json.load(f)
        except:
            pass
    return None

def save_cached_best_params(window_hash, best_params):
    cache_file = os.path.join(WFO_CACHE_DIR, f"{window_hash}.json")
    try:
        with open(cache_file, "w") as f:
            json.dump(best_params, f, indent=2)
    except:
        pass

def build_param_grid():
    return list(product(*[WFO_PARAM_GRID[k] for k in WFO_PARAM_GRID.keys()]))

def get_optimization_score(trades, final_equity, equity_curve, end_date):
    num_trades = len(trades)
    if num_trades < 10:
        return -1500.0
    years = (end_date - pd.to_datetime(BACKTEST_START_DATE)).days / 365.25
    if years <= 0 or final_equity <= 0:
        return 0.0
    try:
        cagr = ((final_equity / STARTING_CAPITAL_GBP) ** (1 / years) - 1) * 100
        cagr = float(np.real(cagr))
    except:
        cagr = 0.0
    rolling_max = np.maximum.accumulate(equity_curve)
    mdd = ((equity_curve / rolling_max) - 1).min() * 100
    calmar = cagr / abs(mdd) if mdd != 0 else 0.0

    win_rate = sum(1 for t in trades if t.get("return_%", 0) > 0) / num_trades * 100 if num_trades > 0 else 0
    gross_profit = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0

    sortino = 0.0
    if len(equity_curve) > 1:
        daily_returns = pd.Series(equity_curve).pct_change().dropna().values
        downside_returns = daily_returns[daily_returns < 0]
        if len(downside_returns) > 0:
            downside_dev = np.std(downside_returns) * np.sqrt(252)
            sortino = cagr / downside_dev if downside_dev != 0 else float('inf')
        else:
            sortino = float('inf')

    score = (calmar * (profit_factor ** 1.4) * (win_rate / 45.0) *
             (sortino / 1.8 if sortino != float('inf') else 3.0))
    trade_density_penalty = max(0, 15 - num_trades) * 0.12
    return score - trade_density_penalty

def should_run_wfo():
    if not os.path.exists(BEST_PARAMS_FILE):
        print("No best_params.json found → Running initial full WFO...")
        return True, 0
    try:
        with open(BEST_PARAMS_FILE) as f:
            saved = json.load(f)
        saved_date = datetime.fromisoformat(saved["timestamp"])
        months_old = (datetime.now() - saved_date).days / 30.44
        next_due_in = max(0, WFO_REOPTIMIZE_EVERY_MONTHS - int(months_old))
        today = datetime.now()
        is_month_start = today.day <= 3
        if months_old >= WFO_REOPTIMIZE_EVERY_MONTHS and is_month_start:
            print(f"Best params are {months_old:.1f} months old → Running full WFO now")
            return True, 0
        else:
            print(f"Best params are {months_old:.1f} months old → Using cached parameters. Next WFO due in {next_due_in} months")
            return False, next_due_in
    except:
        print("Could not read best_params.json → Running full WFO...")
        return True, 0

def get_robinhood_fx_fee(current_date: pd.Timestamp) -> float:
    if current_date.weekday() != 4:
        return ROBINHOOD_NORMAL_FX_PCT
    sim_time = dt_time(random.randint(0, 23), random.randint(0, 59))
    if dt_time(17, 0) <= sim_time <= dt_time(21, 0):
        return ROBINHOOD_FRIDAY_SPIKE_PCT
    return ROBINHOOD_NORMAL_FX_PCT

def calculate_sell_fees(shares: float, sell_price: float, current_date: pd.Timestamp) -> float:
    sell_value = shares * sell_price
    total_fees = 0.0

    if shares > TAF_SHARE_EXEMPTION:
        taf = min(shares * TRADING_ACTIVITY_FEE_PER_SHARE, TAF_MAX_FEE)
        total_fees += taf

    if sell_value > REGULATORY_FEE_MIN_NOTIONAL:
        sec_fee = (sell_value / 1_000_000) * REGULATORY_FEE_PER_MILLION
        total_fees += sec_fee

    return total_fees

# ========================== BUY AND HOLD SPY ==========================
def run_buy_and_hold_spy(data, common_dates, starting_capital=None, monthly_deposit=None):
    if starting_capital is None: starting_capital = STARTING_CAPITAL_GBP
    if monthly_deposit is None: monthly_deposit = MONTHLY_DEPOSIT_GBP
    if "SPY" not in data or data["SPY"].empty or len(common_dates) == 0:
        return np.full(len(common_dates), starting_capital, dtype=float), common_dates

    df_spy = data["SPY"].reindex(common_dates).ffill()
    close_series = df_spy.get("Close")
    if isinstance(close_series, pd.DataFrame):
        close_series = close_series.iloc[:, 0]
    closes = pd.to_numeric(close_series, errors='coerce').values.astype(float)

    n = len(common_dates)
    equity_curve = np.full(n, starting_capital, dtype=float)
    equity = float(starting_capital)
    last_month = None
    for i in range(1, n):
        current_date = common_dates[i]
        close_price = closes[i] if not pd.isna(closes[i]) else closes[i-1]
        current_month = current_date.strftime("%Y-%m")
        if current_month != last_month and current_date.day <= 7:
            equity += monthly_deposit
            last_month = current_month
        prev_close = closes[i-1]
        daily_ret = close_price / prev_close if prev_close > 0 and not pd.isna(close_price) else 1.0
        equity *= daily_ret
        equity_curve[i] = equity
    return equity_curve, pd.DatetimeIndex(common_dates)

# ========================== SCORING ==========================
def get_score(value, metric_type):
    if metric_type == "net_profit":
        if value > 45000: return "Excellent"
        elif value > 36500: return "Very Good"
        elif value > 28000: return "Good"
        elif value > 21500: return "Solid"
        elif value > 15000: return "Fair"
        else: return "Bad"
    elif metric_type == "total_return":
        if value > 220: return "Excellent"
        elif value > 180: return "Very Good"
        elif value > 140: return "Good"
        elif value > 105: return "Solid"
        elif value > 70: return "Fair"
        else: return "Bad"
    elif metric_type == "cagr":
        if value > 18: return "Excellent"
        elif value > 15.5: return "Very Good"
        elif value > 13: return "Good"
        elif value > 10.5: return "Solid"
        elif value > 8: return "Fair"
        else: return "Bad"
    elif metric_type == "max_dd":
        if value > -18: return "Excellent"
        elif value > -23: return "Very Good"
        elif value > -28: return "Good"
        elif value > -34: return "Solid"
        elif value > -40: return "Fair"
        else: return "Bad"
    elif metric_type == "calmar":
        if value > 0.9: return "Excellent"
        elif value > 0.725: return "Very Good"
        elif value > 0.55: return "Good"
        elif value > 0.425: return "Solid"
        elif value > 0.3: return "Fair"
        else: return "Bad"
    elif metric_type == "sortino":
        if value > 2.5: return "Excellent"
        elif value > 2.0: return "Very Good"
        elif value > 1.5: return "Good"
        elif value > 1.15: return "Solid"
        elif value > 0.8: return "Fair"
        else: return "Bad"
    elif metric_type == "sharpe":
        if value > 1.8: return "Excellent"
        elif value > 1.55: return "Very Good"
        elif value > 1.3: return "Good"
        elif value > 1.05: return "Solid"
        elif value > 0.8: return "Fair"
        else: return "Bad"
    elif metric_type == "expectancy":
        if value > 500: return "Excellent"
        elif value > 425: return "Very Good"
        elif value > 350: return "Good"
        elif value > 275: return "Solid"
        elif value > 200: return "Fair"
        else: return "Bad"
    elif metric_type == "profit_factor":
        if value > 2.2: return "Excellent"
        elif value > 1.9: return "Very Good"
        elif value > 1.6: return "Good"
        elif value > 1.4: return "Solid"
        elif value > 1.2: return "Fair"
        else: return "Bad"
    elif metric_type == "win_rate":
        if value > 58: return "Excellent"
        elif value > 55: return "Very Good"
        elif value > 52: return "Good"
        elif value > 49: return "Solid"
        elif value > 46: return "Fair"
        else: return "Bad"
    elif metric_type == "total_trades":
        if value > 280: return "Excellent"
        elif value > 230: return "Very Good"
        elif value > 180: return "Good"
        elif value > 140: return "Solid"
        elif value > 100: return "Fair"
        else: return "Bad"
    elif metric_type == "median_final":
        if value > 65000: return "Excellent"
        elif value > 56500: return "Very Good"
        elif value > 48000: return "Good"
        elif value > 40000: return "Solid"
        elif value > 32000: return "Fair"
        else: return "Bad"
    elif metric_type == "p5_final":
        if value > 28000: return "Excellent"
        elif value > 23000: return "Very Good"
        elif value > 18000: return "Good"
        elif value > 14000: return "Solid"
        elif value > 10000: return "Fair"
        else: return "Bad"
    elif metric_type == "p95_final":
        if value > 110000: return "Excellent"
        elif value > 92500: return "Very Good"
        elif value > 75000: return "Good"
        elif value > 62500: return "Solid"
        elif value > 50000: return "Fair"
        else: return "Bad"
    elif metric_type == "mc_median_dd":
        if value > -22: return "Excellent"
        elif value > -27: return "Very Good"
        elif value > -32: return "Good"
        elif value > -38.5: return "Solid"
        elif value > -45: return "Fair"
        else: return "Bad"
    elif metric_type == "prob_ruin":
        if value < 8: return "Excellent"
        elif value < 13: return "Very Good"
        elif value < 18: return "Good"
        elif value < 26.5: return "Solid"
        elif value < 35: return "Fair"
        else: return "Bad"
    elif metric_type == "prob_30dd":
        if value < 25: return "Excellent"
        elif value < 32.5: return "Very Good"
        elif value < 40: return "Good"
        elif value < 50: return "Solid"
        elif value < 60: return "Fair"
        else: return "Bad"
    return "Fair"

def print_scored_metrics(trades, final_equity, equity_curve, end_date, mc_results=None):
    total_trades = len(trades)
    if total_trades == 0:
        print("No trades executed.")
        return

    win_rate = sum(1 for t in trades if t.get("return_%", 0) > 0) / total_trades * 100
    gross_profit = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0

    net_profit = final_equity - STARTING_CAPITAL_GBP
    total_return = (final_equity / STARTING_CAPITAL_GBP - 1) * 100
    years = (end_date - pd.to_datetime(BACKTEST_START_DATE)).days / 365.25
    cagr = ((final_equity / STARTING_CAPITAL_GBP) ** (1 / years) - 1) * 100 if years > 0 else 0.0

    rolling_max = np.maximum.accumulate(equity_curve)
    mdd = ((equity_curve / rolling_max) - 1).min() * 100
    calmar = cagr / abs(mdd) if mdd != 0 else 0

    sortino = 0.0
    if len(equity_curve) > 1:
        daily_returns = pd.Series(equity_curve).pct_change().dropna().values
        downside_returns = daily_returns[daily_returns < 0]
        if len(downside_returns) > 0:
            downside_dev = np.std(downside_returns) * np.sqrt(252)
            sortino = cagr / downside_dev if downside_dev != 0 else float('inf')
        else:
            sortino = float('inf')

    sharpe = 0.0
    if len(equity_curve) > 1:
        daily_returns = pd.Series(equity_curve).pct_change().dropna().values
        if len(daily_returns) > 0:
            mean_ret = np.mean(daily_returns)
            std_ret = np.std(daily_returns)
            if std_ret > 0:
                sharpe = (mean_ret / std_ret) * np.sqrt(252)

    expectancy_gbp = net_profit / total_trades if total_trades > 0 else 0.0

    print("\n" + "="*110)
    print("v11.5 SAFER DUAL MOMENTUM + 4R PARTIAL + 8% PYRAMIDING + VIX REGIME + QUALITY")
    print("="*110)

    print(f"Net Profit          : £{net_profit:,.0f}          → {get_score(net_profit, 'net_profit')}")
    print(f"Total Return        : {total_return:.1f}%           → {get_score(total_return, 'total_return')}")
    print(f"CAGR                : {cagr:.2f}%            → {get_score(cagr, 'cagr')}")
    print(f"Max Drawdown        : {mdd:.2f}%           → {get_score(mdd, 'max_dd')}")
    print(f"Calmar Ratio        : {calmar:.2f}             → {get_score(calmar, 'calmar')}")
    print(f"Sortino Ratio       : {sortino:.2f}             → {get_score(sortino, 'sortino')}")
    print(f"Sharpe Ratio        : {sharpe:.2f}             → {get_score(sharpe, 'sharpe')}")
    print(f"Expectancy          : £{expectancy_gbp:,.0f} per trade → {get_score(expectancy_gbp, 'expectancy')}")
    print(f"Profit Factor       : {profit_factor:.2f}            → {get_score(profit_factor, 'profit_factor')}")
    print(f"Win Rate            : {win_rate:.1f}%            → {get_score(win_rate, 'win_rate')}")
    print(f"Total Trades        : {total_trades}               → {get_score(total_trades, 'total_trades')}")

    if mc_results:
        print("\n--- Monte Carlo Simulation Scoring ---")
        print(f"Median Final Equity     : £{mc_results['median_final']:,.0f}     → {get_score(mc_results['median_final'], 'median_final')}")
        print(f"5th Percentile Equity   : £{mc_results['p5_final']:,.0f}      → {get_score(mc_results['p5_final'], 'p5_final')}")
        print(f"95th Percentile Equity  : £{mc_results['p95_final']:,.0f}     → {get_score(mc_results['p95_final'], 'p95_final')}")
        print(f"Median Max DD (MC)      : {mc_results['median_dd']:.1f}%       → {get_score(mc_results['median_dd'], 'mc_median_dd')}")
        print(f"Prob. losing >50%       : {mc_results['prob_ruin_50pct']:.1f}%      → {get_score(mc_results['prob_ruin_50pct'], 'prob_ruin')}")
        print(f"Prob. >30% DD           : {mc_results['prob_dd_gt_30pct']:.1f}%      → {get_score(mc_results['prob_dd_gt_30pct'], 'prob_30dd')}")

    print("="*110)

# ========================== CORE BACKTEST ==========================
def run_single_backtest(data, weekly_data=None, slippage=BASE_SLIPPAGE, params=None, dynamic_watchlist=None, save_positions=False, scan_type="Evening"):
    if params is None:
        params = load_best_params()

    equity_usd = float(STARTING_CAPITAL_GBP)
    open_trades = []
    trades = []
    equity_curve_usd = [equity_usd]
    all_dates_list = []

    for t in list(data.keys()):
        if not data[t].empty:
            data[t] = data[t][~data[t].index.duplicated(keep='first')]

    all_date_sets = [df.index for df in data.values() if not df.empty]
    common_dates = sorted(set.union(*[set(dates) for dates in all_date_sets]))
    common_dates = pd.DatetimeIndex([d for d in common_dates if d >= pd.to_datetime(BACKTEST_START_DATE)])

    if len(common_dates) < 30:
        return [], equity_usd, np.array([equity_usd]), pd.DatetimeIndex([]), common_dates[-1] if len(common_dates) > 0 else pd.to_datetime("2026-04-11"), 0

    aligned_data = {t: data[t].reindex(common_dates).ffill() for t in data if not data[t].empty}

    np_data = {}
    for ticker, df in aligned_data.items():
        def safe_get(col: str, default=np.nan):
            s = df.get(col)
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
            elif not isinstance(s, pd.Series):
                s = pd.Series(default, index=df.index)
            return pd.to_numeric(s, errors='coerce').values.astype(float)

        np_data[ticker] = {
            "Open": safe_get("Open"),
            "High": safe_get("High"),
            "Low": safe_get("Low"),
            "Close": safe_get("Close"),
            "ATR": safe_get("ATR"),
        }

    rs_6m = {}
    abs_mom_6m = {}
    if "SPY" in np_data:
        spy_close = np_data["SPY"]["Close"]
        period = RS_MONTHS * 21
        for ticker in np_data:
            if ticker == "SPY": continue
            close = np_data[ticker]["Close"]
            rs = np.zeros(len(close))
            abs_m = np.zeros(len(close))
            for i in range(period, len(close)):
                if close[i - period] > 0 and spy_close[i - period] > 0:
                    ret_t = close[i] / close[i - period]
                    ret_s = spy_close[i] / spy_close[i - period]
                    rs[i] = ret_t / ret_s
                    abs_m[i] = (close[i] / close[i - period]) - 1
            rs_6m[ticker] = rs
            abs_mom_6m[ticker] = abs_m

    vix_data = get_data("^VIX")
    vix_series = None
    if not vix_data.empty:
        vix_series = vix_data["Close"].reindex(common_dates).ffill()

    watchlist_to_use = dynamic_watchlist if dynamic_watchlist is not None else DYNAMIC_WATCHLIST

    last_rebalance_month = None
    backtest_end_date = common_dates[-1]

    for i in range(1, len(common_dates)):
        current_date = common_dates[i]
        prev_idx = i - 1
        daily_pnl = 0.0

        current_month_str = current_date.strftime("%Y-%m")
        is_new_month = (last_rebalance_month is None or current_month_str != last_rebalance_month)

        if is_new_month and current_date.day <= 7:
            equity_usd += MONTHLY_DEPOSIT_GBP
            last_rebalance_month = current_month_str

        for trade in open_trades[:]:
            ticker = trade["ticker"]
            nd = np_data.get(ticker)
            if nd is None: continue

            close_val = float(nd["Close"][i]) if not pd.isna(nd["Close"][i]) else np.nan
            open_val = float(nd["Open"][i]) if not pd.isna(nd["Open"][i]) else np.nan
            if pd.isna(close_val) or pd.isna(open_val):
                continue

            close = close_val * (1 + np.random.uniform(-slippage, slippage))
            open_p = open_val * (1 + np.random.uniform(-slippage, slippage))

            if close > trade.get("highest_since_partial", trade.get("entry_price", 0)):
                trade["highest_since_partial"] = close

            days_held = (current_date - trade["entry_date"]).days
            exit_price = None
            exit_reason = None

            if days_held >= MAX_HOLD_DAYS:
                exit_price = close
                exit_reason = "max_hold"

            if exit_price is None and trade.get("stop_price") and open_p <= trade["stop_price"]:
                exit_price = open_p
                exit_reason = "stop"

            if exit_price is None:
                initial_risk = trade.get("initial_risk_distance", 1.0)
                unrealized_r = (close - trade["entry_price"]) / initial_risk if initial_risk > 0 else 0
                if unrealized_r <= -MAX_LOSS_PER_TRADE_R:
                    exit_price = close
                    exit_reason = "max_loss"

            if exit_price is None:
                entry = trade["entry_price"]
                initial_risk = trade.get("initial_risk_distance", 1.0)
                unrealized_r = (close - entry) / initial_risk

                if not trade.get("partial_taken", False) and unrealized_r >= 4.0:
                    partial_shares = trade["shares"] / 2
                    partial_price = close
                    gross_partial = partial_shares * (partial_price - entry)
                    comm_partial = partial_shares * partial_price * PER_SIDE_COMMISSION_PCT
                    reg_partial = calculate_sell_fees(partial_shares, partial_price, current_date)
                    fx_partial = get_robinhood_fx_fee(current_date)
                    fx_cost_partial = (partial_shares * partial_price) * fx_partial
                    total_fees_partial = comm_partial + reg_partial + fx_cost_partial

                    daily_pnl += gross_partial - total_fees_partial

                    trades.append({
                        "return_%": round(((partial_price / entry) - 1) * 100, 2),
                        "pnl": round(gross_partial - total_fees_partial, 2),
                        "exit_reason": "partial_4R",
                        "ticker": ticker,
                        "sleeve": "main"
                    })

                    trade["shares"] = partial_shares
                    trade["partial_taken"] = True
                    trade["highest_since_partial"] = partial_price

                if (AGGRESSIVE_MODE and not trade.get("pyramid_added", False) and unrealized_r >= 5.0):
                    regime_ok = True
                    if "SPY" in np_data and prev_idx >= 200:
                        spy_close_val = float(np_data["SPY"]["Close"][prev_idx])
                        spy_df = aligned_data.get("SPY")
                        spy_sma200 = float(spy_df["SMA200"].iloc[prev_idx]) if "SMA200" in spy_df.columns else 0
                        current_vix = float(vix_series.iloc[i]) if vix_series is not None and not pd.isna(vix_series.iloc[i]) else 20
                        if spy_close_val < spy_sma200 or current_vix > 32:
                            regime_ok = False

                    if regime_ok:
                        pyramid_shares = round(trade["shares"] * 0.08, 4)
                        current_pos_value = trade["shares"] * close
                        pyramid_value = pyramid_shares * close
                        max_pos_value = equity_usd * params.get("max_position_size_pct", MAX_POSITION_SIZE_PCT)

                        if current_pos_value + pyramid_value <= max_pos_value:
                            trade["shares"] += pyramid_shares
                            trade["pyramid_added"] = True
                            trade["highest_since_partial"] = close
                            print(f"Pyramiding +8% on {ticker} at +5R")

                tighten_div = params.get("trail_tighten_div", 110)
                trail_pct = max(params.get("min_trail_pct", MIN_TRAIL_PCT),
                                params.get("runner_trail_base_pct", RUNNER_TRAIL_BASE_PCT) - (unrealized_r / tighten_div))
                atr_trail = trade.get("initial_risk_distance", 1.0) * params.get("trail_atr_mult", TRAIL_MULT)
                trail_stop = max(
                    trade["highest_since_partial"] * (1 - trail_pct),
                    trade["highest_since_partial"] - atr_trail
                )

                if close <= trail_stop:
                    exit_price = trail_stop
                    exit_reason = "runner_trail"

            if exit_price is not None:
                gross = trade["shares"] * (exit_price - trade["entry_price"])
                comm = trade["shares"] * exit_price * PER_SIDE_COMMISSION_PCT
                regulatory = calculate_sell_fees(trade["shares"], exit_price, current_date)
                fx_fee = get_robinhood_fx_fee(current_date)
                fx_cost = (trade["shares"] * exit_price) * fx_fee
                total_fees = comm + regulatory + fx_cost

                daily_pnl += gross - total_fees

                trades.append({
                    "return_%": round(((exit_price / trade["entry_price"]) - 1) * 100, 2),
                    "pnl": round(gross - total_fees, 2),
                    "exit_reason": exit_reason,
                    "ticker": ticker,
                    "sleeve": "main"
                })

                open_trades.remove(trade)

        equity_usd += daily_pnl
        equity_usd = max(equity_usd, 1000.0)
        equity_curve_usd.append(equity_usd)
        all_dates_list.append(current_date)

        regime_ok = True
        vix_high = False
        if "SPY" in np_data and prev_idx >= 200:
            spy_close_val = float(np_data["SPY"]["Close"][prev_idx]) if not pd.isna(np_data["SPY"]["Close"][prev_idx]) else np.nan
            if pd.isna(spy_close_val):
                regime_ok = False
            else:
                spy_df = aligned_data.get("SPY")
                spy_sma200 = float(spy_df["SMA200"].iloc[prev_idx]) if not spy_df.empty and "SMA200" in spy_df.columns and not pd.isna(spy_df["SMA200"].iloc[prev_idx]) else np.nan
                if np.isnan(spy_sma200) or spy_close_val < spy_sma200:
                    regime_ok = False

        if VIX_FILTER_ENABLED and vix_series is not None:
            current_vix = float(vix_series.iloc[i]) if not pd.isna(vix_series.iloc[i]) else 20.0
            if current_vix > 32:
                vix_high = True

        risk_scale = 1.0
        if vix_high or not regime_ok:
            risk_scale = 0.5

        if ENABLE_CASH_SLEEVE and not regime_ok and FORCE_CLOSE_ON_REGIME_OFF:
            should_rebalance = is_new_month and current_date.day <= 5
            if should_rebalance:
                for trade in open_trades[:]:
                    ticker = trade["ticker"]
                    nd = np_data.get(ticker)
                    if nd is None: continue
                    close_val = float(nd["Close"][i]) if not pd.isna(nd["Close"][i]) else np.nan
                    if pd.isna(close_val): continue
                    close_val = close_val * (1 + np.random.uniform(-slippage, slippage))

                    gross = trade["shares"] * (close_val - trade["entry_price"])
                    comm = trade["shares"] * close_val * PER_SIDE_COMMISSION_PCT
                    regulatory = calculate_sell_fees(trade["shares"], close_val, current_date)
                    fx_fee = get_robinhood_fx_fee(current_date)
                    fx_cost = (trade["shares"] * close_val) * fx_fee
                    total_fees = comm + regulatory + fx_cost

                    equity_usd += gross - total_fees
                    trades.append({
                        "return_%": round(((close_val / trade["entry_price"]) - 1) * 100, 2),
                        "pnl": round(gross - total_fees, 2),
                        "exit_reason": "regime_off_cash_sleeve",
                        "ticker": ticker,
                        "sleeve": "main"
                    })

                    open_trades.remove(trade)

        if ENABLE_CASH_SLEEVE and not regime_ok:
            continue

        current_day = current_date.day
        should_rebalance = is_new_month and current_day <= 5

        if should_rebalance or len(open_trades) == 0:
            ranking = []
            for ticker in watchlist_to_use:
                if ticker not in rs_6m or prev_idx >= len(rs_6m[ticker]): continue
                rs_val = rs_6m[ticker][prev_idx]
                abs_ret = abs_mom_6m.get(ticker, np.zeros(len(rs_6m[ticker])))[prev_idx] if ticker in abs_mom_6m else -1.0
                
                close_prev = float(np_data[ticker]["Close"][prev_idx])
                sma200_val = float(aligned_data[ticker]["SMA200"].iloc[prev_idx]) if "SMA200" in aligned_data[ticker].columns else 0
                min_rs = params.get("min_rs", 1.05)
                min_abs = params.get("min_abs_mom", 0.05)
                sma_required = params.get("sma200_required", False)
                abs_weight = params.get("abs_mom_weight", 1.95)

                if rs_val > min_rs and abs_ret > min_abs and (not sma_required or close_prev > sma200_val):
                    combined_score = rs_val * (1 + abs_weight * abs_ret)
                    ranking.append((ticker, combined_score))

            ranking.sort(key=lambda x: x[1], reverse=True)

            sector_counts = defaultdict(int)
            top_tickers = []
            top_n = params.get("top_n", TOP_N)
            for ticker, score in ranking:
                sector = SECTOR_MAP.get(ticker, "UNKNOWN")
                if sector_counts[sector] < params.get("max_per_sector", MAX_PER_SECTOR):
                    top_tickers.append(ticker)
                    sector_counts[sector] += 1
                if len(top_tickers) >= top_n:
                    break

            atr_pcts = []
            extra_candidates = [t[0] for t in ranking[:30] if t[0] not in top_tickers][:10]
            for ticker in top_tickers + extra_candidates:
                if ticker not in np_data or prev_idx >= len(np_data[ticker]["ATR"]): continue
                atr = float(np_data[ticker]["ATR"][prev_idx])
                close = float(np_data[ticker]["Close"][prev_idx])
                if close > 1 and atr > 0:
                    atr_pcts.append(atr / close)
            median_atr_pct = np.median(atr_pcts) if atr_pcts else 0.02

            for trade in open_trades[:]:
                if trade["ticker"] not in top_tickers:
                    close_val = float(np_data[trade["ticker"]]["Close"][i])
                    if close_val <= 0: continue
                    close_val *= (1 + np.random.uniform(-slippage, slippage))

                    gross = trade["shares"] * (close_val - trade["entry_price"])
                    comm = trade["shares"] * close_val * PER_SIDE_COMMISSION_PCT
                    regulatory = calculate_sell_fees(trade["shares"], close_val, current_date)
                    fx_fee = get_robinhood_fx_fee(current_date)
                    fx_cost = (trade["shares"] * close_val) * fx_fee
                    total_fees = comm + regulatory + fx_cost

                    equity_usd += gross - total_fees
                    trades.append({
                        "return_%": round(((close_val / trade["entry_price"]) - 1) * 100, 2),
                        "pnl": round(gross - total_fees, 2),
                        "exit_reason": "rebalance_out",
                        "ticker": trade["ticker"],
                        "sleeve": "main"
                    })

                    open_trades.remove(trade)

            current_held = {t["ticker"] for t in open_trades}

            if not vix_high:
                for ticker in top_tickers:
                    if ticker in current_held: continue
                    nd = np_data.get(ticker)
                    if nd is None or prev_idx >= len(nd["ATR"]): continue

                    atr = float(nd["ATR"][prev_idx])
                    close_prev = float(nd["Close"][prev_idx])
                    if atr <= 0 or close_prev <= 0: continue

                    this_atr_pct = atr / close_prev
                    vol_factor = median_atr_pct / max(this_atr_pct, 0.005)

                    if scan_type == "Late-Afternoon":
                        entry_price = close_prev * (1 + np.random.uniform(-slippage*1.5, slippage*1.5))
                        if LAST_HOUR_STRENGTH_FILTER and i > 0:
                            last_hour_start = max(0, i - 6)
                            last_hour_close = float(nd["Close"][i])
                            last_hour_open = float(nd["Open"][last_hour_start]) if last_hour_start < i else close_prev
                            if last_hour_close <= last_hour_open:
                                continue
                    else:
                        next_open = float(nd["Open"][i])
                        if next_open <= 0: continue
                        entry_price = next_open * (1 + np.random.uniform(-slippage*2, slippage*2))

                    risk_dist = atr * ATR_STOP_MULT

                    risk_amount = equity_usd * params.get("risk_per_trade_pct", 0.009) * vol_factor * risk_scale
                    risk_amount = max(risk_amount, equity_usd * params.get("risk_per_trade_pct", 0.009) * VOL_PARITY_FLOOR)
                    risk_amount = min(risk_amount, equity_usd * params.get("risk_per_trade_pct", 0.009) * VOL_PARITY_CEIL)

                    shares = risk_amount / risk_dist
                    position_value = shares * entry_price

                    max_pos_pct = params.get("max_position_size_pct", MAX_POSITION_SIZE_PCT)
                    if position_value > equity_usd * max_pos_pct:
                        shares = (equity_usd * max_pos_pct) / entry_price

                    fx_fee = get_robinhood_fx_fee(current_date)
                    equity_usd -= shares * entry_price * PER_SIDE_COMMISSION_PCT * (1 + fx_fee)

                    open_trades.append({
                        "ticker": ticker,
                        "entry_price": entry_price,
                        "shares": round(shares, 4),
                        "highest_since_partial": entry_price,
                        "initial_risk_distance": risk_dist,
                        "stop_price": entry_price - risk_dist,
                        "entry_date": current_date,
                        "breakeven_moved": False,
                        "partial_taken": False,
                        "pyramid_added": False
                    })

    if save_positions:
        with open(OPEN_POSITIONS_FILE, "w") as f:
            json.dump({"strategy": open_trades}, f, indent=2, default=str)

    return trades, equity_usd, np.array(equity_curve_usd[1:]), pd.DatetimeIndex(all_dates_list), backtest_end_date, len(trades)

# ========================== WFO ==========================
def run_walk_forward(data, weekly_data=None):
    print("\n=== Rolling Walk-Forward Optimization v11.5 (96 combos) ===")
    start_time = time.time()
    print(f"WFO Anchor: {WFO_ANCHOR_DATE} | IS: {WFO_IS_MONTHS} months | OOS: {WFO_OOS_MONTHS} months | Step: {WFO_STEP_MONTHS} months\n")

    all_dates = pd.concat([df.index.to_series() for df in data.values() if not df.empty]).unique()
    all_dates = sorted(pd.to_datetime(all_dates))
    all_dates = pd.DatetimeIndex([d for d in all_dates if d >= pd.to_datetime(BACKTEST_START_DATE)])

    wfo_start = pd.to_datetime(WFO_ANCHOR_DATE)
    current_start = max(all_dates[0], wfo_start)

    is_delta = pd.DateOffset(months=WFO_IS_MONTHS)
    oos_delta = pd.DateOffset(months=WFO_OOS_MONTHS)
    step_delta = pd.DateOffset(months=WFO_STEP_MONTHS)

    all_trades = []
    equity = STARTING_CAPITAL_GBP
    param_history = []
    window_num = 0

    while current_start + is_delta + oos_delta <= all_dates[-1]:
        window_num += 1
        is_end = current_start + is_delta
        oos_start = is_end
        oos_end = min(oos_start + oos_delta, all_dates[-1])

        if (oos_end - oos_start).days < 60:
            current_start += step_delta
            continue

        train_data = {t: df[df.index < is_end].copy() for t, df in data.items() if not df.empty}
        test_data = {t: df[(df.index >= oos_start) & (df.index <= oos_end)].copy() for t, df in data.items() if not df.empty}

        print(f"\nWindow {window_num}: IS {current_start.date()} → {is_end.date()} | OOS {oos_start.date()} → {oos_end.date()}")

        is_watchlist = build_dynamic_watchlist(LARGE_UNIVERSE, train_data, verbose=False, save_to_file=False)
        print(f"  → Frozen IS dynamic watchlist: {len(is_watchlist)} stocks")

        window_hash = get_window_hash(current_start, is_end)
        cached_params = load_cached_best_params(window_hash)

        if cached_params is not None:
            best_param_dict = cached_params
            best_score = "cached"
        else:
            grid_candidates = build_param_grid()
            print(f"  Grid size: {len(grid_candidates)} combinations")

            best_score = -np.inf
            best_param_dict = None
            best_trades_is = 0

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(_evaluate_param_combo, dict(zip(WFO_PARAM_GRID.keys(), combo)), train_data, weekly_data, is_watchlist)
                           for combo in grid_candidates]

                completed = 0
                for future in as_completed(futures):
                    completed += 1
                    progress_bar(f"    Evaluating combos", completed, len(grid_candidates))
                    param_dict, score, num_trades = future.result()
                    if score > best_score:
                        best_score = score
                        best_param_dict = param_dict.copy()
                        best_trades_is = num_trades

            print(f"  IS Trades with best params: {best_trades_is}")
            save_cached_best_params(window_hash, best_param_dict)

        print(f"  → Best IS params: {best_param_dict} (score: {best_score})")

        param_history.append(best_param_dict)

        trades, final_eq, curve_oos, _, oos_end_date, _ = run_single_backtest(test_data, weekly_data, params=best_param_dict, dynamic_watchlist=is_watchlist, save_positions=False)
        oos_score = get_optimization_score(trades, final_eq, curve_oos, oos_end_date)

        if len(trades) >= 12:
            print(f"  OOS Trades: {len(trades)} | OOS Score: {oos_score:.2f}")
            all_trades.extend(trades)
            equity = final_eq
        else:
            print(f"  OOS Trades: {len(trades)} | Too few trades")

        current_start += step_delta

    if param_history:
        final_best_params = {}
        last_five = param_history[-5:]
        for key in WFO_PARAM_GRID:
            vals = [p.get(key) for p in last_five if p.get(key) is not None]
            if vals:
                if key == "sma200_required":
                    final_best_params[key] = Counter(vals).most_common(1)[0][0]
                else:
                    final_best_params[key] = round(statistics.median(vals), 3 if key == "risk_per_trade_pct" else 2)
        print(f"\nFinal optimized params: {final_best_params}")
    else:
        final_best_params = load_best_params()

    save_best_params(final_best_params)
    print(f"WFO completed in {time.time() - start_time:.1f} seconds")
    return all_trades, equity, final_best_params

def _evaluate_param_combo(param_dict, train_data, weekly_data, dynamic_watchlist):
    trades_t, eq_t, curve_t, _, end_date_t, _ = run_single_backtest(train_data, weekly_data, params=param_dict, dynamic_watchlist=dynamic_watchlist, save_positions=False)
    score = get_optimization_score(trades_t, eq_t, curve_t, end_date_t)
    return param_dict.copy(), score, len(trades_t)

# ========================== MONTE CARLO ==========================
def run_monte_carlo(data, weekly_data=None, num_sims=NUM_MONTE_CARLO):
    print(f"\nRunning {num_sims:,} Monte Carlo simulations...")
    _, _, equity_curve, _, _, _ = run_single_backtest(data, weekly_data, save_positions=False)
    base_daily_ret = pd.Series(equity_curve).pct_change().dropna().values
    n_days = len(base_daily_ret)
    if n_days == 0:
        print("No data for Monte Carlo")
        return {}

    base_daily_ret = np.clip(base_daily_ret, -0.5, 2.0)
    rng = np.random.default_rng(42)

    block_size = 21
    blocks = [base_daily_ret[i:i+block_size] for i in range(0, n_days - block_size + 1, block_size)]
    if not blocks:
        blocks = [base_daily_ret]

    equity_paths = np.zeros((num_sims, n_days))
    for sim in range(num_sims):
        path = []
        while len(path) < n_days:
            block = rng.choice(blocks)
            path.extend(block)
        path = np.array(path[:n_days])

        crash_prob_per_day = 0.0004
        crashes = rng.random(n_days) < crash_prob_per_day
        path[crashes] *= 0.85

        equity_paths[sim] = STARTING_CAPITAL_GBP * np.cumprod(1 + path)

    final_equities = equity_paths[:, -1]
    rolling_max = np.maximum.accumulate(equity_paths, axis=1)
    drawdowns = (equity_paths / rolling_max - 1)
    max_dds = np.min(drawdowns, axis=1) * 100

    prob_ruin = np.mean(final_equities < STARTING_CAPITAL_GBP * 0.50) * 100
    prob_severe_dd = np.mean(max_dds < -30) * 100

    results = {
        "median_final": float(np.median(final_equities)),
        "p5_final": float(np.percentile(final_equities, 5)),
        "p95_final": float(np.percentile(final_equities, 95)),
        "median_dd": float(np.median(max_dds)),
        "p5_dd": float(np.percentile(max_dds, 5)),
        "p95_dd": float(np.percentile(max_dds, 95)),
        "prob_ruin_50pct": float(prob_ruin),
        "prob_dd_gt_30pct": float(prob_severe_dd),
    }

    print("\nMonte Carlo Results:")
    print(f"  Median Final Equity     : £{results['median_final']:,.0f}")
    print(f"  5th Percentile Equity   : £{results['p5_final']:,.0f}")
    print(f"  95th Percentile Equity  : £{results['p95_final']:,.0f}")
    print(f"  Median Max Drawdown     : {results['median_dd']:.1f}%")
    print(f"  Prob. losing >50%       : {results['prob_ruin_50pct']:.1f}%")
    print(f"  Prob. >30% DD           : {results['prob_dd_gt_30pct']:.1f}%")
    return results

def plot_equity_curve(equity_curve, dates, final_equity, equity_curve_bh, spy_final):
    plt.figure(figsize=(12, 7))
    plt.plot(dates, equity_curve, label='Strategy v11.5 Live', color='blue', linewidth=2)
    plt.plot(dates, equity_curve_bh, label='SPY Buy & Hold', color='red', linewidth=2, alpha=0.85)
    plt.title('Equity Curve Comparison - v11.5 Live')
    plt.xlabel('Date')
    plt.ylabel('Equity (£)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('equity_curve_v11.5_live.png', dpi=200)
    print("Equity curve saved as equity_curve_v11.5_live.png")

# ========================== DAILY BOT RUN ==========================
def run_bot_once(scan_type="Evening"):
    global data, open_trades_shared, next_wfo_months_global, DYNAMIC_WATCHLIST, SECTOR_MAP

    print(f"\n=== v11.5 {scan_type} Scan Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

    run_wfo, next_wfo_months = should_run_wfo()
    global next_wfo_months_global
    next_wfo_months_global = next_wfo_months

    start_time = time.time()

    if run_wfo or not LIVE_MODE:
        SEND_TRADE_NOTIFICATIONS = False
    else:
        SEND_TRADE_NOTIFICATIONS = True

    print("Downloading historical GBP/USD rates...")
    fx_data = get_data(FX_TICKER)

    print("Loading market data for large universe...")
    data = {}
    tickers_to_load = LARGE_UNIVERSE + ["SPY", "^VIX"]
    for i, ticker in enumerate(tickers_to_load):
        df = get_data(ticker)
        if df is not None and not df.empty:
            if "ATR" not in df.columns and ticker != "^VIX":
                high_low = df["High"] - df["Low"]
                high_close = np.abs(df["High"] - df["Close"].shift())
                low_close = np.abs(df["Low"] - df["Close"].shift())
                tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
                df["ATR"] = tr.rolling(14).mean()
            if "SMA200" not in df.columns and ticker != "^VIX":
                df["SMA200"] = df["Close"].rolling(200).mean()
            if "200d_low" not in df.columns and ticker != "^VIX":
                df["200d_low"] = df["Close"].rolling(200).min()
            data[ticker] = df
        else:
            print(f"Warning: No data loaded for {ticker}")

        if i % 8 == 0 and i > 0:
            time.sleep(1.0)

    weekly_data = None

    if DYNAMIC_WATCHLIST_ENABLED:
        refresh_needed = True
        if os.path.exists(DYNAMIC_WATCHLIST_FILE):
            try:
                with open(DYNAMIC_WATCHLIST_FILE) as f:
                    saved = json.load(f)
                saved_time = datetime.fromisoformat(saved["timestamp"])
                months_old = (datetime.now() - saved_time).days / 30.44
                if months_old < WATCHLIST_REFRESH_MONTHS:
                    refresh_needed = False
                    DYNAMIC_WATCHLIST = saved["watchlist"]
                    print(f"Loaded cached dynamic watchlist ({len(DYNAMIC_WATCHLIST)} stocks)")
            except:
                pass
        if refresh_needed:
            DYNAMIC_WATCHLIST = build_dynamic_watchlist(LARGE_UNIVERSE, data, verbose=True, save_to_file=True)
    else:
        DYNAMIC_WATCHLIST = STATIC_WATCHLIST

    SECTOR_MAP = build_sector_map(LARGE_UNIVERSE + ["SPY"])

    try:
        with open(OPEN_POSITIONS_FILE) as f:
            open_trades_shared = json.load(f).get("strategy", [])
    except:
        open_trades_shared = []

    if LIVE_MODE and SEND_TRADE_NOTIFICATIONS:
        send_daily_start_message(STARTING_CAPITAL_GBP, scan_type)

    if run_wfo:
        print("\n=== RUNNING FULL ANALYSIS (WFO + Monte Carlo) ===")
        wf_trades, wf_equity, optimized_params = run_walk_forward(data, weekly_data)
    else:
        optimized_params = load_best_params()
        print(f"Using cached optimized params: {optimized_params}")

    print("\nRunning Deterministic Backtest on full history...")
    trades, final_equity_usd, equity_curve_usd, date_index, backtest_end_date, _ = run_single_backtest(
        data, weekly_data, save_positions=True, scan_type=scan_type
    )

    try:
        with open(OPEN_POSITIONS_FILE) as f:
            open_trades_shared = json.load(f).get("strategy", [])
    except:
        open_trades_shared = []

    final_equity_usd = float(final_equity_usd)
    equity_curve_usd = np.asarray(equity_curve_usd, dtype=float).flatten()

    if not fx_data.empty and len(date_index) > 0:
        close_col = fx_data["Close"]
        if isinstance(close_col, pd.DataFrame):
            close_col = close_col.squeeze()
        fx_series = close_col.reindex(date_index).ffill()
        last_fx = float(fx_series.iloc[-1]) if not pd.isna(fx_series.iloc[-1]) else 0.78
        equity_curve_gbp = equity_curve_usd / fx_series.values
        final_equity_gbp = final_equity_usd / last_fx
    else:
        equity_curve_gbp = equity_curve_usd * 0.78
        final_equity_gbp = final_equity_usd * 0.78

    equity_curve_bh, _ = run_buy_and_hold_spy(data, date_index)
    equity_curve_bh = np.asarray(equity_curve_bh, dtype=float).flatten()

    if len(equity_curve_bh) > 0:
        if not fx_data.empty and len(date_index) > 0:
            equity_curve_bh_gbp = equity_curve_bh / fx_series.values
        else:
            equity_curve_bh_gbp = equity_curve_bh * 0.78
        spy_final_gbp = float(equity_curve_bh_gbp[-1])
    else:
        spy_final_gbp = 0.0

    print(f"\nFinal Strategy Equity (GBP): £{final_equity_gbp:,.0f}")
    print(f"SPY Buy & Hold (GBP)       : £{spy_final_gbp:,.0f}")

    if run_wfo or RUN_MONTE_CARLO_LIVE:
        sims = NUM_MONTE_CARLO if run_wfo else NUM_MONTE_CARLO_LIVE
        mc_result = run_monte_carlo(data, weekly_data, num_sims=sims)
    else:
        mc_result = None

    print_scored_metrics(trades, final_equity_gbp, equity_curve_gbp, backtest_end_date, mc_result)

    plot_equity_curve(equity_curve_gbp, date_index, final_equity_gbp, equity_curve_bh_gbp, spy_final_gbp)

    duration = time.time() - start_time
    print(f"\n=== {scan_type} Scan Complete in {duration:.1f} seconds ===\n")

# ========================== SCHEDULER (DUAL) ==========================
def main_scheduler():
    schedule.every().day.at(LATE_AFTERNOON_SCAN_TIME).do(lambda: run_bot_once(scan_type="Late-Afternoon"))
    schedule.every().day.at(EVENING_SCAN_TIME).do(lambda: run_bot_once(scan_type="Evening"))
    
    print("🕒 Dual Scheduler activated:")
    print(f"   • {LATE_AFTERNOON_SCAN_TIME} → Late-Afternoon Scan (Same-day close entries + last-hour filter)")
    print(f"   • {EVENING_SCAN_TIME} → Evening Rebalance Scan (full rebalancing)")

    while True:
        schedule.run_pending()
        time.sleep(60)

# ========================== MAIN ==========================
if __name__ == "__main__":
    sys.excepthook = send_crash_alert
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)
    print("🛡️ Crash handler + graceful shutdown alerts activated")

    if TEST_MODE:
        print("\n🚀 TEST MODE ACTIVATED")
        print("→ Forcing FULL WFO + Monte Carlo + Backtest")
        print("→ Notifications disabled | Trading day check bypassed\n")

        LIVE_MODE = False
        SEND_TRADE_NOTIFICATIONS = False
        FORCE_RUN = True
        RUN_MONTE_CARLO_LIVE = True

        run_bot_once(scan_type="Evening")
        
        print("\n✅ TEST MODE COMPLETE — Full WFO + Monte Carlo executed.")
        sys.exit(0)

    # ====================== NORMAL LIVE MODE ======================
    if LIVE_MODE:
        poller_thread = threading.Thread(target=telegram_status_poller, daemon=False)
        poller_thread.start()
        print("✅ Telegram status poller is now ACTIVE")

    try:
        main_scheduler()
    except KeyboardInterrupt:
        print("\nScheduler stopped by user.")
        graceful_shutdown(signal.SIGINT, None)
    except Exception as e:
        send_crash_alert(type(e), e, e.__traceback__)
