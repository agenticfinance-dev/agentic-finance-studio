import os
import logging
import asyncio
import json
import traceback
from datetime import datetime, timedelta
from collections import deque
from typing import Dict, List, Optional, Tuple
import hmac
import hashlib
import time
import sqlite3
from contextlib import contextmanager

import aiohttp
import websockets
import numpy as np

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

# ================= CONFIG =================
TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_TOKEN environment variable is not set!")

SOSO_API_KEY = os.getenv("SOSO_API_KEY")
ADMIN_CHAT_ID = os.getenv("CHAT_ID")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
SODEX_API_KEY = os.getenv("SODEX_API_KEY")

if ADMIN_CHAT_ID:
    try:
        ADMIN_CHAT_ID = int(ADMIN_CHAT_ID)
    except ValueError:
        ADMIN_CHAT_ID = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)

HEADERS = {"User-Agent": "AgenticFinanceStudio/3.0"}
TIMEOUT = aiohttp.ClientTimeout(total=15)

COINS = ["btc", "eth", "xrp", "sol"]
COIN_NAMES = {"btc": "bitcoin", "eth": "ethereum", "xrp": "ripple", "sol": "solana"}

SECTORS = {
    "AI": ["eth", "sol"],
    "PAYFI": ["xrp"],
    "RWA": ["btc"],
    "DEFI": ["eth", "sol"],
    "GAMING": ["sol"],
    "LAYER2": ["eth"]
}

# ================= DATABASE =================
DB_PATH = "bot_database.db"

def init_database():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            subscribed INTEGER DEFAULT 0,
            auto_trade_enabled INTEGER DEFAULT 0,
            max_position REAL DEFAULT 100,
            risk_per_trade REAL DEFAULT 2,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            symbol TEXT,
            side TEXT,
            entry_price REAL,
            exit_price REAL,
            tp REAL,
            sl REAL,
            pnl REAL,
            result TEXT,
            opened_at TIMESTAMP,
            closed_at TIMESTAMP
        )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            bias TEXT,
            confidence INTEGER,
            entry_price REAL,
            tp REAL,
            sl REAL,
            reasoning TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()
        logging.info("✅ Database initialized")

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

# ================= PERSISTENT STORAGE =================
performance_log = {coin: deque(maxlen=100) for coin in COINS}
signal_history = deque(maxlen=500)
paper_trade_history = []
paper_positions = {}
subscribed_users = set()
auto_trade_config = {}

PORTFOLIO_FILE = "paper_portfolio.json"
HISTORY_FILE = "trade_history.json"
SUBSCRIPTIONS_FILE = "subscribed_users.json"
ANALYTICS_FILE = "analytics.json"
AUTO_TRADE_FILE = "auto_trade.json"

# Load data
analytics = {"signals_generated": 0, "alerts_sent": 0, "scanner_alerts": 0, "executions": 0, "auto_trades": 0}
if os.path.exists(ANALYTICS_FILE):
    try:
        with open(ANALYTICS_FILE, "r") as f:
            analytics = json.load(f)
    except:
        pass

if os.path.exists(PORTFOLIO_FILE):
    try:
        with open(PORTFOLIO_FILE, "r") as f:
            paper_positions = json.load(f)
    except:
        paper_positions = {}

if os.path.exists(HISTORY_FILE):
    try:
        with open(HISTORY_FILE, "r") as f:
            loaded = json.load(f)
            paper_trade_history = loaded if isinstance(loaded, list) else []
    except:
        paper_trade_history = []

if os.path.exists(SUBSCRIPTIONS_FILE):
    try:
        with open(SUBSCRIPTIONS_FILE, "r") as f:
            subscribed_users = set(json.load(f))
    except:
        subscribed_users = set()

if os.path.exists(AUTO_TRADE_FILE):
    try:
        with open(AUTO_TRADE_FILE, "r") as f:
            auto_trade_config = json.load(f)
    except:
        auto_trade_config = {}

def save_analytics():
    try:
        temp = ANALYTICS_FILE + ".tmp"
        with open(temp, "w") as f:
            json.dump(analytics, f)
        os.replace(temp, ANALYTICS_FILE)
    except Exception as e:
        logging.error(f"Analytics save failed: {e}")

def save_portfolio():
    try:
        temp = PORTFOLIO_FILE + ".tmp"
        with open(temp, "w") as f:
            json.dump(paper_positions, f)
        os.replace(temp, PORTFOLIO_FILE)
    except Exception as e:
        logging.error(f"Portfolio save failed: {e}")

def save_trade_history():
    try:
        temp = HISTORY_FILE + ".tmp"
        with open(temp, "w") as f:
            json.dump(paper_trade_history[-500:], f)
        os.replace(temp, HISTORY_FILE)
    except Exception as e:
        logging.error(f"History save failed: {e}")

def save_subscriptions():
    try:
        temp = SUBSCRIPTIONS_FILE + ".tmp"
        with open(temp, "w") as f:
            json.dump(list(subscribed_users), f)
        os.replace(temp, SUBSCRIPTIONS_FILE)
    except Exception as e:
        logging.error(f"Subscriptions save failed: {e}")

def save_auto_trade_config():
    try:
        temp = AUTO_TRADE_FILE + ".tmp"
        with open(temp, "w") as f:
            json.dump(auto_trade_config, f)
        os.replace(temp, AUTO_TRADE_FILE)
    except Exception as e:
        logging.error(f"Auto trade config save failed: {e}")

def save_user_to_db(user_id: int, username: str = None, first_name: str = None):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO users (user_id, username, first_name, last_active)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ''', (user_id, username, first_name))

def save_signal_to_db(signal: Dict):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO signals (symbol, bias, confidence, entry_price, tp, sl, reasoning)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (signal['symbol'], signal['bias'], signal['confidence'],
              signal['entry'], signal['tp'], signal['sl'], signal['reasoning']))

user_last_interaction = {}
price_cache = {}
last_scanner_alert = {}
last_fng = {"score": 50, "mood": "Neutral", "time": datetime.now()}

api_semaphore = asyncio.Semaphore(10)
price_semaphore = asyncio.Semaphore(20)

start_time = datetime.now()

BYBIT_REFERRAL_LINK = "https://www.bybit.com/invite?ref=N8GY3B&medium=referral&utm_campaign=evergreen"

def escape_markdown(text: str) -> str:
    if not text:
        return ""
    special = r'_*[]()\~`>#+-=|{}.!'
    return ''.join('\\' + c if c in special else c for c in str(text))

def format_price(price: float) -> str:
    if price is None:
        return "0.00"
    if price < 0.01:
        return f"{price:.8f}"
    elif price < 1:
        return f"{price:.6f}"
    return f"{price:,.2f}"

def build_main_menu():
    keyboard = [
        [InlineKeyboardButton("📊 BTC", callback_data="signal_btc"), InlineKeyboardButton("📊 ETH", callback_data="signal_eth")],
        [InlineKeyboardButton("📊 XRP", callback_data="signal_xrp"), InlineKeyboardButton("📊 SOL", callback_data="signal_sol")],
        [InlineKeyboardButton("📈 Sector Map", callback_data="sectors"), InlineKeyboardButton("🐳 Whale Radar", callback_data="whale")],
        [InlineKeyboardButton("📊 ETF Flows", callback_data="etf"), InlineKeyboardButton("🧠 Intelligence", callback_data="news")],
        [InlineKeyboardButton("📈 Performance", callback_data="performance"), InlineKeyboardButton("💼 Portfolio", callback_data="portfolio")],
        [InlineKeyboardButton("📜 Signal History", callback_data="history"), InlineKeyboardButton("🔍 Diagnostics", callback_data="diagnostics")],
        [InlineKeyboardButton("📊 Backtest", callback_data="backtest"), InlineKeyboardButton("🔔 Alerts", callback_data="toggle_alerts")],
        [InlineKeyboardButton("📋 Daily Report", callback_data="daily_report"), InlineKeyboardButton("🤖 Auto-Trade", callback_data="auto_trade_menu")],
        [InlineKeyboardButton("⚡ SoDEX Execute", callback_data="sodex_menu"), InlineKeyboardButton("⭐ Premium", callback_data="premium")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ================= TECHNICAL INDICATORS =================
def calculate_rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    if down == 0:
        return 100.0
    for i in range(period, len(deltas)):
        delta = deltas[i]
        upval = delta if delta > 0 else 0
        downval = -delta if delta < 0 else 0
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        if down == 0:
            return 100.0
    rs = up / down
    rsi = 100 - (100 / (1 + rs))
    return max(0, min(100, rsi))

def calculate_macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Dict:
    if len(closes) < slow:
        return {"macd": 0, "signal": 0, "histogram": 0, "bullish": False}
    def ema(data, period):
        alpha = 2 / (period + 1)
        result = [data[0]]
        for price in data[1:]:
            result.append(alpha * price + (1 - alpha) * result[-1])
        return result
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = ema(macd_line, signal)
    histogram = [m - s for m, s in zip(macd_line, signal_line)]
    return {
        "macd": macd_line[-1],
        "signal": signal_line[-1],
        "histogram": histogram[-1],
        "bullish": macd_line[-1] > signal_line[-1] and histogram[-1] > 0
    }

def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 0.02 * closes[-1] if closes else 100
    trs = []
    for i in range(1, len(highs)):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i-1])
        lc = abs(lows[i] - closes[i-1])
        trs.append(max(hl, hc, lc))
    return sum(trs[-period:]) / period if trs else 0.02 * closes[-1]

def calculate_support_resistance(closes: List[float], highs: List[float], lows: List[float]) -> Tuple[float, float]:
    if len(closes) < 50:
        return closes[-1] * 0.95 if closes else 0, closes[-1] * 1.05 if closes else 0
    resistance = max(highs[-50:])
    support = min(lows[-50:])
    return support, resistance

# ================= GLOBAL SESSION =================
async def get_session(app):
    if "session" not in app.bot_data or app.bot_data["session"].closed:
        connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
        app.bot_data["session"] = aiohttp.ClientSession(timeout=TIMEOUT, connector=connector, headers=HEADERS)
    return app.bot_data["session"]

async def post_shutdown(app):
    logging.info("🛑 Shutting down...")
    for task_name in ["scanner_task", "bybit_ws_task", "monitor_task", "heartbeat_task"]:
        task = app.bot_data.get(task_name)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    if "session" in app.bot_data and not app.bot_data["session"].closed:
        await app.bot_data["session"].close()
    save_analytics()
    save_portfolio()
    save_trade_history()
    save_subscriptions()
    save_auto_trade_config()
    logging.info("✅ Graceful shutdown completed")

# ================= PRICE ENGINE =================
async def fetch_with_retry(session, url, headers=None, retries=3):
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers or HEADERS, timeout=10) as r:
                if r.status == 200:
                    return await r.json()
        except:
            await asyncio.sleep(0.5 * (attempt + 1))
    return None

async def get_cached_price(session, symbol: str) -> Dict:
    now = datetime.now()
    if symbol in price_cache and (now - price_cache[symbol]["time"]) < timedelta(seconds=20):
        return price_cache[symbol]["data"]
    
    result = {"price": None, "change": 0, "volume": 0, "high": 0, "low": 0, "source": "No Data"}
    
    if SOSO_API_KEY:
        try:
            headers = {"x-soso-api-key": SOSO_API_KEY, **HEADERS}
            data = await fetch_with_retry(session, f"https://openapi.sosovalue.com/openapi/v1/asset/market/current-price?symbol={symbol.upper()}", headers)
            if data and data.get("data"):
                item = data["data"][0] if isinstance(data["data"], list) else data["data"]
                result = {
                    "price": float(item.get("price", 0)),
                    "change": float(item.get("change24h", 0)),
                    "volume": float(item.get("volume", 0)),
                    "high": float(item.get("high24h", 0)),
                    "low": float(item.get("low24h", 0)),
                    "source": "🥇 SoSoValue"
                }
                if result["price"] > 0:
                    price_cache[symbol] = {"data": result, "time": now}
                    return result
        except Exception as e:
            logging.debug(f"SoSoValue error: {e}")
    
    try:
        cg_id = COIN_NAMES.get(symbol)
        if cg_id:
            data = await fetch_with_retry(session, f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true", retries=2)
            if data and cg_id in data:
                result = {
                    "price": float(data[cg_id].get("usd", 0)),
                    "change": float(data[cg_id].get("usd_24h_change", 0)),
                    "volume": float(data[cg_id].get("usd_24h_vol", 0)),
                    "source": "🥈 CoinGecko"
                }
                if result["price"] > 0:
                    price_cache[symbol] = {"data": result, "time": now}
                    return result
    except Exception as e:
        logging.debug(f"CoinGecko error: {e}")
    
    try:
        async with price_semaphore:
            async with session.get(f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol.upper()}USDT", timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = {
                        "price": float(data.get("lastPrice", 0)),
                        "change": float(data.get("priceChangePercent", 0)),
                        "volume": float(data.get("volume", 0)),
                        "source": "🥉 Binance"
                    }
                    if result["price"] > 0:
                        price_cache[symbol] = {"data": result, "time": now}
                        return result
    except Exception as e:
        logging.debug(f"Binance error: {e}")
    
    return result

async def get_historical_klines(session, symbol: str, interval: str = "1h", limit: int = 100):
    try:
        async with price_semaphore:
            async with session.get(f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}USDT&interval={interval}&limit={limit}", timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "closes": [float(c[4]) for c in data],
                        "highs": [float(c[2]) for c in data],
                        "lows": [float(c[3]) for c in data]
                    }
    except:
        return None

# ================= SSI =================
async def get_ssi(session) -> Tuple[int, str]:
    global last_fng
    if (datetime.now() - last_fng["time"]) < timedelta(minutes=30):
        return last_fng["score"], last_fng["mood"]
    
    if SOSO_API_KEY:
        try:
            headers = {"x-soso-api-key": SOSO_API_KEY, **HEADERS}
            data = await fetch_with_retry(session, "https://openapi.sosovalue.com/openapi/v1/market/sentiment", headers)
            if data and data.get("data"):
                score = int(data["data"].get("sentimentScore", 50))
                mood = data["data"].get("mood", "Neutral")
                last_fng = {"score": score, "mood": mood, "time": datetime.now()}
                return score, mood
        except:
            pass
    
    try:
        async with session.get("https://api.alternative.me/fng/", timeout=10) as resp:
            fng = await resp.json()
            if fng and fng.get("data"):
                score = int(fng["data"][0]["value"])
                mood = fng["data"][0]["value_classification"]
                last_fng = {"score": score, "mood": mood, "time": datetime.now()}
                return score, mood
    except:
        pass
    return last_fng["score"], last_fng["mood"]

# ================= SIGNAL GENERATION =================
async def generate_signal(session, symbol: str) -> Dict:
    price_data = await get_cached_price(session, symbol)
    current_price = price_data.get("price") or 50000.0
    
    klines = await get_historical_klines(session, symbol)
    if not klines or len(klines["closes"]) < 50:
        change = price_data.get("change", 0)
        bias = "LONG" if change > 1 else "SHORT" if change < -1 else "NEUTRAL"
        return {
            "symbol": symbol.upper(),
            "price": current_price,
            "entry": current_price,
            "tp": current_price * 1.04,
            "sl": current_price * 0.96,
            "bias": bias,
            "confidence": 60,
            "reasoning": "Simple mode",
            "rr_ratio": 2.0,
            "source": price_data["source"]
        }
    
    closes = klines["closes"]
    highs = klines["highs"]
    lows = klines["lows"]
    
    rsi = calculate_rsi(closes)
    macd = calculate_macd(closes)
    atr = calculate_atr(highs, lows, closes)
    support, resistance = calculate_support_resistance(closes, highs, lows)
    ssi_score, ssi_mood = await get_ssi(session)
    
    confidence = 50 + (ssi_score - 50) * 0.4
    if rsi < 35:
        confidence += 20
    if macd["bullish"]:
        confidence += 15
    if price_data.get("change", 0) > 2:
        confidence += 10
    
    confidence = max(35, min(95, int(confidence)))
    
    if "LONG" in (bias := "LONG" if rsi < 50 or macd["bullish"] else "SHORT" if rsi > 60 else "NEUTRAL"):
        entry = round(current_price * 0.995, 6)
        tp = round(entry + (atr * 2.5), 6)
        sl = round(entry - (atr * 1.2), 6)
    else:
        entry = round(current_price * 1.005, 6)
        tp = round(entry - (atr * 2.5), 6)
        sl = round(entry + (atr * 1.2), 6)
    
    rr = abs((tp - entry) / (entry - sl)) if (entry - sl) != 0 else 2.0
    
    signal = {
        "symbol": symbol.upper(),
        "price": current_price,
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "bias": bias,
        "confidence": confidence,
        "reasoning": f"SSI({ssi_mood} {ssi_score}) | RSI({rsi:.0f}) | MACD Bullish: {macd['bullish']}",
        "rr_ratio": round(rr, 1),
        "source": price_data["source"]
    }
    
    analytics["signals_generated"] += 1
    signal_history.append(signal)
    save_analytics()
    return signal

# ================= EIP-712 PAYLOAD (FIXED) =================
def generate_eip712_payload(from_token: str, to_token: str, amount: float, user_address: str = "0xYourWalletAddress"):
    domain = {
        "name": "SoDEX",
        "version": "1",
        "chainId": 8453,
        "verifyingContract": "0x0000000000000000000000000000000000000000"  # placeholder
    }
    # Return a simple dict – you can extend with actual EIP-712 structure
    return {
        "domain": domain,
        "message": {
            "fromToken": from_token,
            "toToken": to_token,
            "amount": amount,
            "user": user_address
        },
        "primaryType": "Swap",
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"}
            ],
            "Swap": [
                {"name": "fromToken", "type": "address"},
                {"name": "toToken", "type": "address"},
                {"name": "amount", "type": "uint256"},
                {"name": "user", "type": "address"}
            ]
        }
    }

# ================= HANDLERS =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_user_to_db(user.id, user.username, user.first_name)
    await update.message.reply_text(
        f"🚀 Welcome {user.first_name}!\n"
        "I am your AI trading assistant. Use the menu below to explore.\n\n"
        "🔹 /help – show commands\n"
        "🔹 /menu – show main menu",
        reply_markup=build_main_menu()
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Available Commands:*\n"
        "/start – start the bot\n"
        "/help – this message\n"
        "/menu – show main menu\n"
        "/signal <coin> – get signal (e.g. /signal btc)\n"
        "/portfolio – view paper portfolio\n"
        "/backtest – run backtest\n"
        "/daily – daily report\n"
        "/alerts – toggle alerts\n"
        "/autotrade – auto-trade settings\n"
        "/sodex – execute SoDEX swap\n"
        "/premium – premium features",
        parse_mode=ParseMode.MARKDOWN
    )

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 Main Menu:", reply_markup=build_main_menu())

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    # Placeholder responses – extend as needed
    if data.startswith("signal_"):
        symbol = data.split("_")[1]
        session = await get_session(context.application)
        signal = await generate_signal(session, symbol)
        msg = (
            f"📈 *{signal['symbol']} Signal*\n"
            f"Bias: {signal['bias']}\n"
            f"Confidence: {signal['confidence']}%\n"
            f"Entry: ${signal['entry']:.2f}\n"
            f"TP: ${signal['tp']:.2f}\n"
            f"SL: ${signal['sl']:.2f}\n"
            f"RR: {signal['rr_ratio']}\n"
            f"Reasoning: {signal['reasoning']}"
        )
        await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN)
    else:
        await query.edit_message_text(f"🛠️ Feature '{data}' is under development.\nUse /help for available commands.")

# ================= BACKGROUND TASKS (Placeholders) =================
async def scanner_task(app):
    while True:
        await asyncio.sleep(60)  # run every minute
        # Implement scanner logic here

async def heartbeat_task(app):
    while True:
        await asyncio.sleep(30)
        # Send heartbeat to admin if needed

# ================= MAIN =================
async def main():
    # Initialize database
    init_database()

    # Build application
    application = ApplicationBuilder().token(TOKEN).build()

    # Register handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("menu", menu))
    application.add_handler(CallbackQueryHandler(button_callback))

    # Add shutdown hook
    application.on_shutdown.append(post_shutdown)

    # Start background tasks
    loop = asyncio.get_running_loop()
    application.bot_data["scanner_task"] = loop.create_task(scanner_task(application))
    application.bot_data["heartbeat_task"] = loop.create_task(heartbeat_task(application))
    # Add other tasks as needed

    # Start polling
    logging.info("🤖 Bot is starting...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    # Keep running until interrupted
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Received shutdown signal...")
    finally:
        await application.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}\n{traceback.format_exc()}")
