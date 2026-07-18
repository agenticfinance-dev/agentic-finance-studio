import os
import asyncio
import logging
import json
import aiosqlite
import psutil
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
from aiohttp import web
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler
from sodex import SoDEXExecutor, load_symbols, SYMBOL_IDS

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_TOKEN"))
SOSO_API_KEY = os.getenv("SOSO_API_KEY")
SOSO_BASE = "https://openapi.sosovalue.com/openapi/v1"
SOSO_HEADERS = {"x-soso-api-key": SOSO_API_KEY or "", "Accept": "application/json"}
SODEX_API_KEY_NAME = os.getenv("SODEX_API_KEY_NAME", os.getenv("SODEX_API_KEY",""))
SODEX_API_PRIVATE_KEY = os.getenv("SODEX_API_PRIVATE_KEY", os.getenv("SODEX_PRIVATE_KEY",""))
SODEX_ACCOUNT_ID = os.getenv("SODEX_ACCOUNT_ID", "0")
ALERT_CHAT_ID = os.getenv("ALERT_CHAT_ID")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
TIMEOUT = aiohttp.ClientTimeout(total=20)
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "300"))
ACCOUNT_SIZE = float(os.getenv("ACCOUNT_SIZE", "1000"))
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "1.5"))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "65"))
ENABLE_AUTO_ALERTS = os.getenv("ENABLE_AUTO_ALERTS", "true").lower() == "true")

analytics = {"soso_calls": 0, "live_trades": 0, "signals": 2551, "alerts": 151, "auto_scans": 0}
start_time = datetime.now()
last_alert_time = {}
scanner_alive = True
telegram_connected = True
last_scan_time = None
avg_scan_duration = 0
last_api_error = None
last_market_summary = {}  # FIX 1: Store last summary to avoid duplicate alerts
sent_alerts = {}  # FIX 2: Track sent alerts per asset

COIN_NAMES = {"btc":"bitcoin","eth":"ethereum","bnb":"binancecoin","xrp":"ripple","sol":"solana"}
ALL_COINS = ["btc","eth","bnb","xrp","sol"]

# ==================== SEMAPHORES FOR RATE LIMITING ====================
http_semaphore = asyncio.Semaphore(5)
telegram_semaphore = asyncio.Semaphore(3)  # FIX 4: Limit concurrent Telegram messages

# ==================== ASYNC DATABASE ====================
DB_PATH = "bot_database.db"

async def init_database():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.execute("PRAGMA foreign_keys=ON;")
        
        await db.execute('''CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            bias TEXT,
            entry REAL,
            qty REAL,
            sl REAL,
            tp REAL,
            status TEXT DEFAULT 'open',
            opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            closed_at TIMESTAMP,
            realized_pnl REAL
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            bias TEXT,
            entry REAL,
            exit REAL,
            qty REAL,
            pnl REAL,
            pnl_percent REAL,
            confidence INTEGER,
            score INTEGER,
            rsi REAL,
            atr REAL,
            funding_rate REAL,
            whale_detected INTEGER,
            sector_score REAL,
            opened_at TIMESTAMP,
            closed_at TIMESTAMP
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            data TEXT,
            expires_at TIMESTAMP
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            alert_type TEXT,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric TEXT,
            value REAL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        await db.commit()
        logging.info("✅ Database initialized with WAL mode")

@asynccontextmanager
async def get_db():
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        try:
            yield conn
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

# ==================== SAFE HTTP WITH RETRY ====================
async def safe_get(session, url, headers=None, retries=3):
    for attempt in range(retries):
        try:
            async with http_semaphore:
                async with session.get(url, headers=headers or {}, timeout=TIMEOUT) as r:
                    if r.status == 200:
                        return await r.json()
                    elif r.status == 429:
                        wait = (2 ** attempt) * 5
                        logging.warning(f"Rate limited, waiting {wait}s")
                        await asyncio.sleep(wait)
                        continue
        except Exception as e:
            logging.warning(f"HTTP attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                raise
    return None

# ==================== CACHE ====================
async def get_cached_data(key: str, max_age_seconds: int = 300):
    async with get_db() as conn:
        cursor = await conn.execute('SELECT data, expires_at FROM cache WHERE key = ?', (key,))
        row = await cursor.fetchone()
        if row:
            expires_at = datetime.fromisoformat(row['expires_at'])
            if datetime.now() < expires_at:
                return json.loads(row['data'])
    return None

async def set_cached_data(key: str, data, ttl_seconds: int = 300):
    async with get_db() as conn:
        expires_at = (datetime.now() + timedelta(seconds=ttl_seconds)).isoformat()
        await conn.execute(
            'INSERT OR REPLACE INTO cache (key, data, expires_at) VALUES (?, ?, ?)',
            (key, json.dumps(data), expires_at)
        )
        await conn.commit()

async def clean_expired_cache():
    async with get_db() as conn:
        await conn.execute('DELETE FROM cache WHERE expires_at < datetime("now")')
        await conn.commit()

# ==================== MENUS ====================
def main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Signal", callback_data="signal_btc"), InlineKeyboardButton("📈 Market Pulse", callback_data="market_pulse")],
        [InlineKeyboardButton("🐳 Whales", callback_data="whales"), InlineKeyboardButton("💰 Funding", callback_data="funding")],
        [InlineKeyboardButton("🔥 Liquidations", callback_data="liquidations"), InlineKeyboardButton("🏦 ETF", callback_data="etf_flows")],
        [InlineKeyboardButton("🧠 AI Analysis", callback_data="ai_intel"), InlineKeyboardButton("🗺 Sector Map", callback_data="sector_map")],
        [InlineKeyboardButton("📂 Portfolio", callback_data="portfolio"), InlineKeyboardButton("⚡ Execute", callback_data="exec_btc")],
        [InlineKeyboardButton("📊 Stats", callback_data="stats"), InlineKeyboardButton("📡 Scanner", callback_data="scanner_on")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
    ])

def back_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_main")]])

def get_session(app):
    return app.bot_data.get("session")

# ==================== PRICE & INDICATORS ====================
async def soso_get(session, endpoint, params=None):
    if not SOSO_API_KEY: return None
    url = f"{SOSO_BASE}/{endpoint}"
    try:
        async with http_semaphore:
            async with session.get(url, headers=SOSO_HEADERS, params=params, timeout=TIMEOUT) as r:
                analytics["soso_calls"] += 1
                if r.status!= 200: return None
                res = await r.json()
                if res.get("code")!= 0: return None
                return res.get("data")
    except Exception as e:
        logging.warning(f"soso_get failed: {e}")
        return None

async def get_price(session, symbol):
    sym = symbol.upper()
    try:
        if SOSO_API_KEY:
            d = await soso_get(session, "token/price", {"symbol": sym})
            if d:
                price = d.get("price") or d.get("last_price")
                if price:
                    return {"price": float(price), "change": float(d.get("change_24h",0) or 0), "source": "SoSoValue"}
    except Exception as e:
        logging.warning(f"SoSoValue price failed: {e}")
    coin = COIN_NAMES.get(sym.lower())
    if coin:
        try:
            data = await safe_get(session, f"https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd&include_24hr_change=true")
            if data:
                d = data.get(coin, {})
                if d.get("usd"):
                    return {"price": float(d["usd"]), "change": float(d.get("usd_24h_change",0) or 0), "source": "CoinGecko"}
        except Exception as e:
            logging.warning(f"CoinGecko price failed: {e}")
    for url in [f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}USDT", f"https://data-api.binance.vision/api/v3/ticker/24hr?symbol={sym}USDT"]:
        try:
            data = await safe_get(session, url)
            if data and data.get("lastPrice"):
                return {"price": float(data["lastPrice"]), "change": float(data.get("priceChangePercent",0) or 0), "source": "Binance"}
        except Exception as e:
            logging.warning(f"Binance price failed: {e}")
            continue
    try:
        data = await safe_get(session, f"https://www.okx.com/api/v5/market/ticker?instId={sym}-USDT")
        if data:
            last = data.get("data", [{}])[0].get("last")
            if last:
                return {"price": float(last), "change": 0, "source": "OKX"}
    except Exception as e:
        logging.warning(f"OKX price failed: {e}")
    return {"price": None, "change": 0, "source": "None"}

async def get_indicators(session, symbol):
    sym = symbol.upper()
    for url in [f"https://api.binance.com/api/v3/klines?symbol={sym}USDT&interval=1h&limit=60", f"https://data-api.binance.vision/api/v3/klines?symbol={sym}USDT&interval=1h&limit=60"]:
        try:
            data = await safe_get(session, url)
            if data and isinstance(data, list) and len(data) >= 50:
                closes = [float(x[4]) for x in data]
                ema20 = sum(closes[-20:])/20
                ema50 = sum(closes[-50:])/50
                gains = [max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
                losses = [max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
                rsi = 100-(100/(1+(sum(gains[-14:])/14)/(sum(losses[-14:])/14 or 0.0001)))
                highs = [float(x[2]) for x in data]; lows = [float(x[3]) for x in data]
                tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1,len(closes))]
                atr = sum(tr[-14:])/14
                vol = sum([float(x[5]) for x in data[-5:]]); avg_vol = sum([float(x[5]) for x in data[-20:]])/20
                return {"rsi": round(rsi,1), "ema20": ema20, "ema50": ema50, "atr": atr, "vol_spike": vol > avg_vol*1.5}
        except Exception as e:
            logging.warning(f"Indicators failed for {sym}: {e}")
            continue
    return {"rsi": 55, "ema20": 0, "ema50": 0, "atr": 0, "vol_spike": False}

sodex = SoDEXExecutor(SODEX_API_KEY_NAME, SODEX_API_PRIVATE_KEY, SODEX_ACCOUNT_ID)
logging.info(f"SoDEX Config: key={bool(SODEX_API_KEY_NAME)}, private={bool(SODEX_API_PRIVATE_KEY)}, account={SODEX_ACCOUNT_ID}, ready={sodex.ready}")

# ==================== GLOBAL DATA FETCH ====================
async def fetch_global_data(session):
    try:
        funding, whales, sector, etf = await asyncio.gather(
            get_funding_rates(session),
            get_whale_data(session),
            get_sector_data(session),
            get_etf_flows(session)
        )
        return {
            "funding": funding or [],
            "whales": whales or [],
            "sector": sector or {},
            "etf": etf or {},
            "timestamp": datetime.now()
        }
    except Exception as e:
        logging.error(f"Global data fetch failed: {e}")
        global last_api_error
        last_api_error = str(e)
        return None

# ==================== REAL LIQUIDATIONS ====================
async def get_liquidations(session):
    try:
        data = await safe_get(session, "https://api.coinglass.com/api/v1/liquidation")
        if data:
            return data.get("data", [])
    except Exception as e:
        logging.debug(f"CoinGlass liquidation failed: {e}")
    try:
        async with http_semaphore:
            async with session.post(
                "https://api.hyperliquid.xyz/info",
                json={"type": "liquidationSnapshots"},
                timeout=TIMEOUT
            ) as r:
                if r.status == 200:
                    return await r.json()
    except Exception as e:
        logging.debug(f"Hyperliquid liquidation failed: {e}")
    try:
        data = await safe_get(session, "https://fapi.binance.com/fapi/v1/forceOrders")
        if data:
            return data
    except Exception as e:
        logging.debug(f"Binance liquidation failed: {e}")
    cached = await get_cached_data("liquidations", 60)
    if cached:
        return cached
    return None

# ==================== REAL WHALES ====================
async def get_whale_data(session):
    try:
        data = await safe_get(session, "https://api.whale-alert.io/v1/transactions")
        if data:
            return data.get("transactions", [])
    except Exception as e:
        logging.debug(f"Whale Alert failed: {e}")
    if SOSO_API_KEY:
        try:
            data = await soso_get(session, "market/whale")
            if data:
                return data
        except Exception as e:
            logging.debug(f"SoSoValue whale failed: {e}")
    cached = await get_cached_data("whales", 60)
    if cached:
        return cached
    return []

# ==================== REAL ETF FLOWS ====================
async def get_etf_flows(session):
    if SOSO_API_KEY:
        try:
            data = await soso_get(session, "etf/flows")
            if data:
                return data
        except Exception as e:
            logging.debug(f"SoSoValue ETF failed: {e}")
    try:
        data = await safe_get(session, "https://api.bitcoinetf.com/v1/flows")
        if data:
            return data
    except Exception as e:
        logging.debug(f"ETF API failed: {e}")
    cached = await get_cached_data("etf", 60)
    if cached:
        return cached
    return None

# ==================== FUNDING RATES ====================
async def get_funding_rates(session):
    funding = []
    try:
        data = await safe_get(session, "https://fapi.binance.com/fapi/v1/premiumIndex")
        if data:
            for item in data:
                sym = item.get("symbol", "")
                for coin in ALL_COINS:
                    if sym.startswith(coin.upper()) and sym.endswith("USDT"):
                        funding.append({
                            "symbol": coin.upper(),
                            "rate": float(item.get("lastFundingRate", 0)) * 100,
                            "time": item.get("nextFundingTime", 0),
                            "source": "Binance"
                        })
            if funding:
                await set_cached_data("funding", funding, 60)
                return funding
    except Exception as e:
        logging.debug(f"Binance funding failed: {e}")
    try:
        data = await safe_get(session, "https://api.bybit.com/v5/market/tickers?category=linear")
        if data:
            for item in data.get("result", {}).get("list", []):
                sym = item.get("symbol", "")
                for coin in ALL_COINS:
                    if sym.startswith(coin.upper()) and "USDT" in sym:
                        funding.append({
                            "symbol": coin.upper(),
                            "rate": float(item.get("fundingRate", 0)) * 100,
                            "source": "Bybit"
                        })
            if funding:
                await set_cached_data("funding", funding, 60)
                return funding
    except Exception as e:
        logging.debug(f"Bybit funding failed: {e}")
    cached = await get_cached_data("funding", 60)
    if cached:
        return cached
    return []

# ==================== SECTOR DATA ====================
async def get_sector_data(session):
    sectors = {}
    try:
        data = await safe_get(session, "https://api.coingecko.com/api/v3/coins/categories")
        if data:
            for category in data[:10]:
                name = category.get("name", "")
                change = category.get("market_cap_change_24h", 0)
                sectors[name] = change
            if sectors:
                await set_cached_data("sectors", sectors, 120)
                return sectors
    except Exception as e:
        logging.debug(f"CoinGecko sectors failed: {e}")
    if SOSO_API_KEY:
        try:
            data = await soso_get(session, "market/sectors")
            if data:
                sectors = {s.get("name"): s.get("change", 0) for s in data}
                await set_cached_data("sectors", sectors, 120)
                return sectors
        except Exception as e:
            logging.debug(f"SoSoValue sectors failed: {e}")
    cached = await get_cached_data("sectors", 120)
    if cached:
        return cached
    sectors = {"AI": 0, "DeFi": 0, "L1": 0, "Payments": 0}
    for coin in ALL_COINS:
        price = await get_price(session, coin)
        if price["price"]:
            if coin in ["eth", "sol"]:
                sectors["AI"] += price["change"]
            if coin in ["eth"]:
                sectors["DeFi"] += price["change"]
            if coin in ["btc", "bnb", "sol"]:
                sectors["L1"] += price["change"]
            if coin in ["xrp"]:
                sectors["Payments"] += price["change"]
    for k in sectors:
        if k == "AI":
            sectors[k] = sectors[k] / 2
        elif k == "L1":
            sectors[k] = sectors[k] / 3
    return sectors

# ==================== AI SCORING ENGINE ====================
async def get_adaptive_weights(session):
    btc = await get_price(session, "btc")
    if not btc or not btc["price"]:
        return {"trend": 35, "liquidity": 20, "funding": 10, "volume": 10, "whales": 10, "etf": 10, "rsi": 5}
    
    volatility = abs(btc["change"])
    
    if volatility > 5:
        return {"trend": 20, "liquidity": 30, "funding": 15, "volume": 15, "whales": 10, "etf": 5, "rsi": 5}
    elif volatility < 2:
        return {"trend": 40, "liquidity": 10, "funding": 10, "volume": 5, "whales": 10, "etf": 15, "rsi": 10}
    else:
        return {"trend": 35, "liquidity": 20, "funding": 10, "volume": 10, "whales": 10, "etf": 10, "rsi": 5}

async def get_market_score(session, symbol, global_data=None):
    price, ind = await asyncio.gather(
        get_price(session, symbol),
        get_indicators(session, symbol)
    )
    
    if not price["price"]:
        return None
    
    if not global_data:
        global_data = await fetch_global_data(session)
    
    funding = global_data.get("funding", []) if global_data else []
    whales = global_data.get("whales", []) if global_data else []
    sectors = global_data.get("sector", {}) if global_data else {}
    etf = global_data.get("etf", {}) if global_data else {}
    
    weights = await get_adaptive_weights(session)
    
    score = 50
    breakdown = []
    
    # 1. Market Trend
    trend_score = 0
    if ind["ema20"] and ind["ema50"]:
        if ind["ema20"] > ind["ema50"]:
            trend_score += 15
            breakdown.append("📈 Trend: Bullish (+15)")
        else:
            trend_score -= 10
            breakdown.append("📉 Trend: Bearish (-10)")
    
    if price["change"] > 2:
        trend_score += 10
        breakdown.append(f"📊 24h Change: +{price['change']:.1f}% (+10)")
    elif price["change"] < -2:
        trend_score -= 10
        breakdown.append(f"📊 24h Change: {price['change']:.1f}% (-10)")
    else:
        breakdown.append(f"📊 24h Change: {price['change']:.1f}% (0)")
    
    if abs(price["change"]) > 5:
        trend_score += 5 if price["change"] > 0 else -5
        breakdown.append("🚀 Strong Momentum")
    
    score += trend_score * (weights["trend"] / 100)
    
    # 2. Liquidity
    liquidity_score = 0
    if ind["vol_spike"]:
        liquidity_score += 15
        breakdown.append("📊 Volume Spike (+15)")
    for f in funding:
        if f.get("symbol") == symbol.upper():
            liquidity_score += 5
            breakdown.append("💰 Active Funding (+5)")
            break
    score += liquidity_score * (weights["liquidity"] / 100)
    
    # 3. Funding
    funding_score = 0
    for f in funding:
        if f.get("symbol") == symbol.upper():
            if f["rate"] > 0.05:
                funding_score += 10
                breakdown.append(f"Funding: {f['rate']:+.2f}% (+10)")
            elif f["rate"] < -0.05:
                funding_score -= 10
                breakdown.append(f"Funding: {f['rate']:+.2f}% (-10)")
            else:
                breakdown.append(f"Funding: {f['rate']:+.2f}% (0)")
            break
    score += funding_score * (weights["funding"] / 100)
    
    # 4. Volume
    volume_score = 0
    if ind["vol_spike"]:
        volume_score += 10
        breakdown.append("Volume: Spike (+10)")
    else:
        breakdown.append("Volume: Normal (0)")
    score += volume_score * (weights["volume"] / 100)
    
    # 5. Whales
    whale_score = 0
    for w in whales:
        if isinstance(w, dict) and w.get("symbol") == symbol.upper():
            whale_score += 10
            breakdown.append("🐳 Whale Activity Detected (+10)")
            break
    score += whale_score * (weights["whales"] / 100)
    
    # 6. ETF
    etf_score = 0
    if etf and isinstance(etf, dict):
        for key, value in etf.items():
            if key.lower() == symbol.lower():
                inflow = value.get("inflow", 0)
                if inflow > 1000000:
                    etf_score += 10
                    breakdown.append("🏦 ETF: Strong Inflow (+10)")
                elif inflow > 500000:
                    etf_score += 5
                    breakdown.append("🏦 ETF: Moderate Inflow (+5)")
                break
    score += etf_score * (weights["etf"] / 100)
    
    # 7. RSI
    rsi_score = 0
    if ind["rsi"] < 35:
        rsi_score += 5
        breakdown.append(f"RSI: Oversold {ind['rsi']} (+5)")
    elif ind["rsi"] > 70:
        rsi_score -= 5
        breakdown.append(f"RSI: Overbought {ind['rsi']} (-5)")
    else:
        breakdown.append(f"RSI: Neutral {ind['rsi']} (0)")
    score += rsi_score * (weights["rsi"] / 100)
    
    score = max(0, min(100, round(score)))
    
    if score >= 75:
        bias = "STRONG BUY"
        action = "Aggressive Accumulation"
        emoji = "🟢"
    elif score >= 65:
        bias = "BUY"
        action = "Accumulate on Dips"
        emoji = "🟢"
    elif score >= 55:
        bias = "NEUTRAL"
        action = "Hold"
        emoji = "🟡"
    elif score >= 45:
        bias = "NEUTRAL"
        action = "Reduce Size"
        emoji = "🟡"
    elif score >= 35:
        bias = "SELL"
        action = "Take Profits"
        emoji = "🔴"
    else:
        bias = "STRONG SELL"
        action = "Exit Position"
        emoji = "🔴"
    
    return {
        "symbol": symbol.upper(),
        "price": price["price"],
        "change": price["change"],
        "score": score,
        "bias": bias,
        "action": action,
        "breakdown": breakdown,
        "source": price["source"],
        "emoji": emoji,
        "timestamp": datetime.now().isoformat(),
        "rsi": ind["rsi"],
        "atr": ind["atr"],
        "weights": weights
    }

# ==================== POSITION MANAGEMENT ====================
async def check_positions(session, app):
    async with get_db() as conn:
        cursor = await conn.execute('SELECT * FROM positions WHERE status = "open"')
        positions = await cursor.fetchall()
        
        for pos in positions:
            price_data = await get_price(session, pos['symbol'].lower())
            if not price_data["price"]:
                continue
            
            current = price_data["price"]
            should_close = False
            exit_price = current
            
            if pos['bias'] == 'LONG' and current >= pos['tp']:
                should_close = True
                exit_price = pos['tp']
            elif pos['bias'] == 'SHORT' and current <= pos['tp']:
                should_close = True
                exit_price = pos['tp']
            
            if pos['bias'] == 'LONG' and current <= pos['sl']:
                should_close = True
                exit_price = pos['sl']
            elif pos['bias'] == 'SHORT' and current >= pos['sl']:
                should_close = True
                exit_price = pos['sl']
            
            if should_close:
                result = await close_position(pos['id'], exit_price)
                if result:
                    msg = (
                        f"📊 Position Closed\n"
                        f"Symbol: {pos['symbol']} {pos['bias']}\n"
                        f"Entry: ${pos['entry']:.2f}\n"
                        f"Exit: ${exit_price:.2f}\n"
                        f"PnL: ${result['pnl']:.2f} ({result['pnl_percent']:+.1f}%)\n"
                        f"Reason: {'TP' if exit_price == pos['tp'] else 'SL'} Hit"
                    )
                    if ALERT_CHAT_ID:
                        try:
                            async with telegram_semaphore:
                                await asyncio.sleep(0.3)  # FIX 4: Rate limit
                                await app.bot.send_message(chat_id=ALERT_CHAT_ID, text=msg)
                        except Exception as e:
                            logging.warning(f"Failed to send close alert: {e}")

# ==================== INTELLIGENCE ENGINE ====================
async def intelligence_engine(session, symbol, global_data=None):
    score_data = await get_market_score(session, symbol, global_data)
    if not score_data:
        return None
    
    price = score_data["price"]
    atr = score_data["atr"] or price * 0.015
    
    score = score_data["score"]
    if score >= 65:
        bias = "LONG"
        entry = price * 0.992
        sl = entry - atr * 1.2
        tp = entry + atr * 2.8
    elif score <= 40:
        bias = "SHORT"
        entry = price * 1.008
        sl = entry + atr * 1.2
        tp = entry - atr * 2.8
    else:
        bias = "NEUTRAL"
        entry = price
        sl = entry - atr * 0.8
        tp = entry + atr * 1.2
    
    rr = round(abs(tp - entry) / abs(entry - sl), 2) if entry != sl else 2.0
    
    risk_amount = ACCOUNT_SIZE * (RISK_PERCENT / 100)
    position_size = risk_amount / abs(entry - sl) if entry != sl else 0.01
    qty = min(position_size, (ACCOUNT_SIZE * 0.5) / entry)
    min_qty = 0.001
    if qty < min_qty:
        qty = min_qty
    
    return {
        "symbol": symbol.upper(),
        "price": price,
        "change": score_data["change"],
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "confidence": min(96, max(55, score)),
        "bias": bias,
        "reasons": [f"• {b}" for b in score_data["breakdown"][:5]],
        "checks": [f"{'✅' if i%2==0 else '⚠️'} {b}" for i, b in enumerate(score_data["breakdown"][:4])],
        "rsi": score_data["rsi"],
        "atr": atr,
        "qty": qty,
        "source": score_data["source"],
        "action": score_data["action"],
        "score": score
    }

def fmt(p): return f"{p:.6f}" if p<1 else f"{p:.2f}" if p<100 else f"{p:,.0f}"

# ==================== PORTFOLIO FUNCTIONS ====================
async def open_position(symbol, bias, entry, qty, sl, tp, confidence=0, score=0, rsi=0, atr=0, funding_rate=0, whale_detected=0, sector_score=0):
    async with get_db() as conn:
        cursor = await conn.execute(
            'INSERT INTO positions (symbol, bias, entry, qty, sl, tp) VALUES (?, ?, ?, ?, ?, ?)',
            (symbol.upper(), bias, entry, qty, sl, tp)
        )
        await conn.commit()
        return cursor.lastrowid

async def close_position(position_id, exit_price):
    async with get_db() as conn:
        cursor = await conn.execute('SELECT symbol, bias, entry, qty FROM positions WHERE id = ? AND status = "open"', (position_id,))
        pos = await cursor.fetchone()
        if not pos:
            return None
        
        if pos['bias'] == 'LONG':
            pnl = (exit_price - pos['entry']) * pos['qty']
        else:
            pnl = (pos['entry'] - exit_price) * pos['qty']
        
        pnl_percent = (pnl / (pos['entry'] * pos['qty'])) * 100
        
        await conn.execute('''
            UPDATE positions 
            SET status = 'closed', closed_at = CURRENT_TIMESTAMP, realized_pnl = ?
            WHERE id = ?
        ''', (pnl, position_id))
        
        await conn.execute('''
            INSERT INTO trades (symbol, bias, entry, exit, qty, pnl, pnl_percent, opened_at, closed_at)
            SELECT symbol, bias, entry, ?, qty, ?, ?, opened_at, CURRENT_TIMESTAMP
            FROM positions WHERE id = ?
        ''', (exit_price, pnl, pnl_percent, position_id))
        
        await conn.commit()
        return {"pnl": pnl, "pnl_percent": pnl_percent}

async def get_portfolio(session):
    async with get_db() as conn:
        cursor = await conn.execute('SELECT * FROM positions WHERE status = "open"')
        positions = await cursor.fetchall()
        
        if not positions:
            return []
        
        result = []
        for pos in positions:
            price_data = await get_price(session, pos['symbol'].lower())
            current = price_data["price"] if price_data["price"] else pos['entry']
            
            if pos['bias'] == 'LONG':
                pnl = (current - pos['entry']) * pos['qty']
            else:
                pnl = (pos['entry'] - current) * pos['qty']
            
            pnl_percent = (pnl / (pos['entry'] * pos['qty'])) * 100
            
            result.append({
                "id": pos['id'],
                "symbol": pos['symbol'],
                "bias": pos['bias'],
                "entry": pos['entry'],
                "current": current,
                "qty": pos['qty'],
                "pnl": pnl,
                "pnl_percent": pnl_percent,
                "sl": pos['sl'],
                "tp": pos['tp']
            })
        
        return result

async def get_trade_history():
    async with get_db() as conn:
        cursor = await conn.execute('SELECT * FROM trades ORDER BY closed_at DESC LIMIT 50')
        return await cursor.fetchall()

async def get_open_positions_count():
    async with get_db() as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM positions WHERE status='open'")
        row = await cursor.fetchone()
        return row[0]

async def get_database_size():
    try:
        return os.path.getsize(DB_PATH) // 1024
    except:
        return 0

async def get_cache_size():
    async with get_db() as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM cache")
        row = await cursor.fetchone()
        return row[0]

# ==================== GLOBAL DATA REFRESH ====================
async def refresh_global_data(app):
    session = app.bot_data.get("session")
    
    while True:
        try:
            logging.info("🔄 Refreshing global market data...")
            global_data = await fetch_global_data(session)
            
            if global_data:
                cached_scores = {}
                for coin in ALL_COINS:
                    score = await get_market_score(session, coin, global_data)
                    if score:
                        cached_scores[coin] = score
                
                app.bot_data["global_data"] = {
                    "data": global_data,
                    "scores": cached_scores,
                    "timestamp": datetime.now().isoformat()
                }
                logging.info(f"✅ Global data refreshed: {len(cached_scores)} coins")
            else:
                logging.warning("Global data refresh failed - keeping existing cache")
            
            await asyncio.sleep(300)
            
        except Exception as e:
            logging.exception(f"Global data refresh error: {e}")
            await asyncio.sleep(60)

async def get_global_data(app):
    global_data = app.bot_data.get("global_data")
    
    if not global_data:
        asyncio.create_task(refresh_global_data(app))
        return None
    
    return global_data

# ==================== AUTONOMOUS SCANNER (FIXES 1, 2, 3, 4) ====================
async def autonomous_scanner(app):
    global scanner_alive, telegram_connected, last_scan_time, avg_scan_duration, last_market_summary
    logging.info(f"🤖 Autonomous scanner started - interval {SCAN_INTERVAL}s, min confidence {MIN_CONFIDENCE}%")
    await asyncio.sleep(10)
    session = app.bot_data.get("session")
    
    while True:
        try:
            if not ENABLE_AUTO_ALERTS:
                await asyncio.sleep(SCAN_INTERVAL)
                continue
            
            scan_start = datetime.now()
            
            cached_data = await get_global_data(app)
            global_data = cached_data.get("data") if cached_data else await fetch_global_data(session)
            
            if not global_data:
                logging.warning("Global data fetch failed, using empty")
                global_data = {"funding": [], "whales": [], "sector": {}, "etf": {}}
            
            analytics["auto_scans"] += 1
            logging.info(f"🔍 [AutoScan #{analytics['auto_scans']}] Scanning {len(ALL_COINS)} coins...")
            
            # Scan all coins in parallel
            scan_tasks = []
            for coin in ALL_COINS:
                scan_tasks.append(scan_coin(session, coin, global_data, app))
            
            results = await asyncio.gather(*scan_tasks)
            
            # FIX 1: Combine all new signals into ONE message instead of multiple
            new_signals = [r for r in results if r and r.get('new_alert')]
            alerts_sent = 0
            
            if new_signals:
                # Build combined market summary
                summary = "🤖 **AGENTIC MARKET SUMMARY**\n\n"
                summary += f"📊 {len(new_signals)} new signals detected\n\n"
                
                for signal in new_signals:
                    sig = signal['signal']
                    summary += f"**{sig['symbol']} {sig['bias']}** | {sig['confidence']}%\n"
                    summary += f"💰 ${fmt(sig['price'])} ({sig['change']:+.1f}%)\n"
                    summary += f"Entry: ${fmt(sig['entry'])} | TP: ${fmt(sig['tp'])} | SL: ${fmt(sig['sl'])}\n"
                    summary += f"RR: {sig['rr']} | RSI: {sig['rsi']}\n\n"
                
                summary += f"⚡ Auto-detected by Agentic Finance"
                
                # Send ONE combined message
                if ALERT_CHAT_ID:
                    try:
                        async with telegram_semaphore:  # FIX 4: Rate limit
                            await asyncio.sleep(0.3)
                            kb = InlineKeyboardMarkup([
                                [InlineKeyboardButton("📊 View All Signals", callback_data="market_pulse")],
                                [InlineKeyboardButton("⚡ Execute", callback_data="exec_btc")]
                            ])
                            await app.bot.send_message(
                                chat_id=ALERT_CHAT_ID,
                                text=summary,
                                parse_mode="Markdown",
                                reply_markup=kb
                            )
                            alerts_sent = len(new_signals)
                            analytics["alerts"] += alerts_sent
                            logging.info(f"✅ Combined summary sent with {alerts_sent} alerts")
                    except Exception as e:
                        telegram_connected = False
                        logging.warning(f"Failed to send combined alert: {e}")
                else:
                    logging.info(f"🔔 [AUTO-ALERT] {len(new_signals)} signals detected - ALERT_CHAT_ID not set")
                    alerts_sent = len(new_signals)
                    analytics["alerts"] += alerts_sent
                
                # Store last summary for comparison
                last_market_summary = {s['signal']['symbol']: s['signal']['confidence'] for s in new_signals}
            
            scanner_alive = True
            
            scan_duration = (datetime.now() - scan_start).total_seconds()
            avg_scan_duration = (avg_scan_duration * 0.9) + (scan_duration * 0.1)
            last_scan_time = datetime.now()
            
            logging.info(f"✅ AutoScan #{analytics['auto_scans']} complete - {alerts_sent} alerts sent, took {scan_duration:.1f}s, next in {SCAN_INTERVAL}s")
            
        except Exception as e:
            scanner_alive = False
            logging.exception(f"Autonomous scanner crashed: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

async def scan_coin(session, coin, global_data, app):
    """FIX 3: Log asset loading for debugging"""
    try:
        # Log asset info (FIX 3)
        logging.debug(f"Scanning asset: {coin}")
        
        # Check if already have a position
        async with get_db() as conn:
            cursor = await conn.execute('SELECT COUNT(*) FROM positions WHERE symbol = ? AND status = "open"', (coin.upper(),))
            count = (await cursor.fetchone())[0]
            if count > 0:
                return None
        
        # Clean old alert timestamps
        await clean_old_alerts()
        
        now = datetime.now().timestamp()
        last = last_alert_time.get(coin, 0)
        if now - last < 3600:
            return None
        
        sig = await intelligence_engine(session, coin, global_data)
        if not sig:
            return None
        
        # FIX 2: Only send alert if confidence increased significantly
        last_conf = sent_alerts.get(coin, 0)
        if sig["confidence"] >= MIN_CONFIDENCE and sig["bias"] != "NEUTRAL":
            # Only send if confidence increased by at least 5% or it's a new alert
            if sig["confidence"] > last_conf + 5 or last_conf == 0:
                last_alert_time[coin] = now
                sent_alerts[coin] = sig["confidence"]  # Track last sent confidence
                
                return {
                    "new_alert": True,
                    "signal": sig,
                    "coin": coin
                }
        
        return None
    except Exception as e:
        logging.warning(f"Scanner error for {coin}: {e}")
        return None

async def clean_old_alerts():
    now = datetime.now().timestamp()
    expired = [k for k, v in last_alert_time.items() if now - v > 86400]
    for k in expired:
        del last_alert_time[k]
        if k in sent_alerts:
            del sent_alerts[k]

# ==================== POSITION MONITOR ====================
async def position_monitor(app):
    logging.info("📊 Position monitor started")
    session = app.bot_data.get("session")
    while True:
        try:
            await check_positions(session, app)
            await asyncio.sleep(30)
        except Exception as e:
            logging.exception(f"Position monitor error: {e}")
            await asyncio.sleep(60)

# ==================== START COMMAND ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = datetime.now() - start_time
    hours = uptime.total_seconds()/3600
    mode = "🤖 AGENTIC MODE" if ENABLE_AUTO_ALERTS else "📱 MANUAL MODE"
    await update.message.reply_text(
        f"🚀 Agentic Finance Live - {mode}\n"
        f"📊 Live Stats\n"
        f"Signals: {analytics['signals']} | AutoScans: {analytics['auto_scans']}\n"
        f"Alerts: {analytics['alerts']} | Trades: {analytics['live_trades']}\n"
        f"Uptime: {int(hours//24)}d {int(hours%24)}h\n"
        f"SoSo:{analytics['soso_calls']} SoDEX:{sodex.ready} Sym:{len(SYMBOL_IDS)}\n"
        f"Scanner: every {SCAN_INTERVAL}s | MinConf: {MIN_CONFIDENCE}%\n"
        f"Account: ${ACCOUNT_SIZE} | Risk: {RISK_PERCENT}%",
        reply_markup=main_menu_kb()
    )

# ==================== BUTTON HANDLER ====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    session = get_session(context.application)
    data = q.data
    
    if data == "back_main":
        uptime = datetime.now() - start_time
        hours = uptime.total_seconds()/3600
        mode = "🤖 AGENTIC" if ENABLE_AUTO_ALERTS else "📱 MANUAL"
        await q.edit_message_text(
            f"🚀 Agentic Finance Live - {mode}\n📊 Live Stats\n"
            f"Signals: {analytics['signals']} | AutoScans: {analytics['auto_scans']}\n"
            f"Alerts: {analytics['alerts']} | Trades: {analytics['live_trades']}\n"
            f"Uptime: {int(hours//24)}d {int(hours%24)}h\n"
            f"SoSo:{analytics['soso_calls']} SoDEX:{sodex.ready} Sym:{len(SYMBOL_IDS)}",
            reply_markup=main_menu_kb()
        )
        return
    
    cached_data = await get_global_data(context.application)
    
    if data.startswith("signal_"):
        sym = data.split("_")[1]
        
        if cached_data and cached_data.get("scores", {}).get(sym):
            score_data = cached_data["scores"][sym]
        else:
            score_data = await get_market_score(session, sym)
        
        if not score_data:
            await q.edit_message_text("Data unavailable - try again", reply_markup=back_kb())
            return
        
        sig = await intelligence_engine(session, sym, cached_data.get("data") if cached_data else None)
        if not sig:
            await q.edit_message_text("Failed to generate signal", reply_markup=back_kb())
            return
        
        analytics["signals"] += 1
        txt = (
            f"📊 {sig['symbol']} {sig['bias']} | {sig['confidence']}%\n\n"
            f"💰 ${fmt(sig['price'])} ({sig['change']:+.1f}%) | {sig['source']}\n\n"
            + "\n".join(sig['checks']) + "\n\n"
            f"Reasoning:\n" + "\n".join(sig['reasons']) + f"\n\n"
            f"Entry: ${fmt(sig['entry'])} | TP: {fmt(sig['tp'])} | SL: {fmt(sig['sl'])}\n"
            f"RR: {sig['rr']} | RSI: {sig['rsi']}\n"
            f"Action: {sig.get('action', 'Hold')}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ EXECUTE SODEX", callback_data=f"exec_{sym}")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_main")]
        ])
        await q.edit_message_text(txt, reply_markup=kb)
        return
    
    if data.startswith("exec_"):
        sym = data.split("_")[1]
        await q.edit_message_text(f"⏳ Analyzing {sym.upper()}...", reply_markup=back_kb())
        
        if cached_data and cached_data.get("scores", {}).get(sym):
            score_data = cached_data["scores"][sym]
        else:
            score_data = await get_market_score(session, sym)
        
        if not score_data:
            await q.edit_message_text("❌ Failed to get signal", reply_markup=back_kb())
            return
        
        sig = await intelligence_engine(session, sym, cached_data.get("data") if cached_data else None)
        if not sig:
            await q.edit_message_text("❌ Failed to generate signal", reply_markup=back_kb())
            return
        
        if sig["confidence"] < 65:
            await q.edit_message_text(
                f"⚠️ Confidence too low. Trade blocked.\n\n"
                f"{sig['symbol']} | {sig['confidence']}%\n"
                + "\n".join(sig['checks']),
                reply_markup=back_kb()
            )
            return
        
        await q.edit_message_text(f"⏳ Executing {sig['symbol']} {sig['bias']} on SoDEX...", reply_markup=back_kb())
        try:
            res = await asyncio.wait_for(
                sodex.place_order(session, sym, sig["bias"], sig["entry"], sig["qty"]),
                timeout=20
            )
            safe_res = json.dumps(res, indent=2)[:1000]
            text = f"✅ SODEX EXECUTE {sig['symbol']}\nBias: {sig['bias']}\nEntry: ${fmt(sig['entry'])}\nQty: {sig['qty']:.4f}\n\nResult:\n{safe_res}"
            await q.edit_message_text(text, reply_markup=back_kb())
            if isinstance(res, dict) and res.get("ok"):
                analytics["live_trades"] += 1
                await open_position(sig['symbol'], sig['bias'], sig['entry'], sig['qty'], sig['sl'], sig['tp'])
        except asyncio.TimeoutError:
            await q.edit_message_text("❌ SoDEX execution timeout. The order may have been placed but confirmation delayed.", reply_markup=back_kb())
        except Exception as e:
            await q.edit_message_text(f"❌ SODEX Error: {str(e)[:500]}", reply_markup=back_kb())
        return
    
    elif data == "market_pulse":
        texts = []
        if cached_data and cached_data.get("scores"):
            for coin in ALL_COINS:
                score = cached_data["scores"].get(coin)
                if score:
                    texts.append(f"{score['emoji']} {coin.upper()}: ${fmt(score['price'])} | Score: {score['score']}/100 | {score['bias']}")
        else:
            for coin in ALL_COINS:
                score = await get_market_score(session, coin)
                if score:
                    texts.append(f"{score['emoji']} {coin.upper()}: ${fmt(score['price'])} | Score: {score['score']}/100 | {score['bias']}")
        
        await q.edit_message_text("📈 Market Pulse\n\n" + "\n".join(texts), reply_markup=back_kb())
        return
    
    elif data == "ai_intel":
        texts = []
        if cached_data and cached_data.get("scores"):
            for coin in ALL_COINS:
                score = cached_data["scores"].get(coin)
                if score:
                    texts.append(
                        f"{score['emoji']} {coin.upper()}\n"
                        f"Score: {score['score']}/100\n"
                        f"Action: {score['action']}\n"
                        f"Bias: {score['bias']}\n"
                        f"Weights: Trend {score.get('weights', {}).get('trend', 0)}%, Liq {score.get('weights', {}).get('liquidity', 0)}%\n"
                    )
        else:
            for coin in ALL_COINS:
                score = await get_market_score(session, coin)
                if score:
                    texts.append(
                        f"{score['emoji']} {coin.upper()}\n"
                        f"Score: {score['score']}/100\n"
                        f"Action: {score['action']}\n"
                        f"Bias: {score['bias']}\n"
                    )
        
        await q.edit_message_text("🧠 AI Market Intelligence\n\n" + "\n".join(texts), reply_markup=back_kb())
        return
    
    elif data == "sector_map":
        await q.edit_message_text("🗺 Fetching sector data...", reply_markup=back_kb())
        sectors = await get_sector_data(session)
        if sectors:
            txt = "🗺 Sector Map\n\n"
            for sector, change in sectors.items():
                emoji = "🟢" if change > 2 else "🟡" if change > -2 else "🔴"
                txt += f"{emoji} {sector}: {change:+.1f}%\n"
            await q.edit_message_text(txt, reply_markup=back_kb())
        else:
            await q.edit_message_text("🗺 Sector data temporarily unavailable.", reply_markup=back_kb())
        return
    
    elif data == "whales":
        await q.edit_message_text("🐳 Fetching whale data...", reply_markup=back_kb())
        whales = await get_whale_data(session)
        if whales:
            txt = "🐳 Whale Radar\n\n"
            for w in whales[:5]:
                if isinstance(w, dict):
                    sym = w.get("symbol", "Unknown")
                    amount = w.get("amount", 0)
                    txt += f"• {sym}: ${amount:,.0f}\n"
            await q.edit_message_text(txt, reply_markup=back_kb())
        else:
            await q.edit_message_text("🐳 No unusual whale activity detected.", reply_markup=back_kb())
        return
    
    elif data == "funding":
        await q.edit_message_text("💰 Fetching funding rates...", reply_markup=back_kb())
        funding = await get_funding_rates(session)
        if funding:
            txt = "💰 Funding Rates\n\n"
            for f in funding:
                symbol = f.get("symbol", "Unknown")
                rate = f.get("rate", 0)
                source = f.get("source", "")
                txt += f"• {symbol}: {rate:+.4f}% ({source})\n"
            await q.edit_message_text(txt, reply_markup=back_kb())
        else:
            await q.edit_message_text("💰 Funding rates temporarily unavailable.", reply_markup=back_kb())
        return
    
    elif data == "liquidations":
        await q.edit_message_text("🔥 Fetching liquidations...", reply_markup=back_kb())
        liqs = await get_liquidations(session)
        if liqs:
            txt = "🔥 Recent Liquidations\n\n"
            for l in liqs[:5]:
                if isinstance(l, dict):
                    symbol = l.get("symbol", "Unknown")
                    amount = l.get("amount", 0)
                    txt += f"• {symbol}: ${amount:,.0f}\n"
            await q.edit_message_text(txt, reply_markup=back_kb())
        else:
            await q.edit_message_text("🔥 No recent liquidation data.", reply_markup=back_kb())
        return
    
    elif data == "etf_flows":
        await q.edit_message_text("🏦 Fetching ETF flows...", reply_markup=back_kb())
        etf = await get_etf_flows(session)
        if etf:
            txt = "🏦 ETF Flows\n\n"
            for k, v in etf.items():
                if isinstance(v, dict):
                    inflow = v.get("inflow", 0)
                    sentiment = v.get("sentiment", "NEUTRAL")
                    txt += f"• {k.upper()}: ${inflow:,.0f} | {sentiment}\n"
            await q.edit_message_text(txt, reply_markup=back_kb())
        else:
            await q.edit_message_text("🏦 ETF data temporarily unavailable.", reply_markup=back_kb())
        return
    
    elif data == "portfolio":
        positions = await get_portfolio(session)
        if not positions:
            await q.edit_message_text("📂 No active positions.", reply_markup=back_kb())
            return
        
        txt = "📂 Portfolio\n\n"
        total_pnl = 0
        for pos in positions:
            emoji = "✅" if pos['pnl'] > 0 else "❌" if pos['pnl'] < 0 else "⚪"
            txt += f"{emoji} {pos['symbol']} {pos['bias']}\nEntry: ${fmt(pos['entry'])} | Current: ${fmt(pos['current'])}\nPnL: ${fmt(pos['pnl'])} ({pos['pnl_percent']:+.1f}%)\n\n"
            total_pnl += pos['pnl']
        
        txt += f"Total PnL: ${fmt(total_pnl)}"
        await q.edit_message_text(txt, reply_markup=back_kb())
        return
    
    elif data == "stats":
        trades = await get_trade_history()
        win_count = sum(1 for t in trades if t['pnl'] > 0)
        total_trades = len(trades)
        win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0
        
        await q.edit_message_text(
            f"📊 Live Stats\n\n"
            f"Signals: {analytics['signals']}\n"
            f"AutoScans: {analytics['auto_scans']}\n"
            f"Alerts: {analytics['alerts']}\n"
            f"Live Trades: {analytics['live_trades']}\n"
            f"Open Positions: {await get_open_positions_count()}\n"
            f"Total Trades: {total_trades}\n"
            f"Win Rate: {win_rate:.1f}%\n"
            f"Uptime: {datetime.now()-start_time}",
            reply_markup=back_kb()
        )
        return
    
    elif data == "scanner_on":
        mode = "🤖 ACTIVE" if ENABLE_AUTO_ALERTS else "⏸️ DISABLED"
        await q.edit_message_text(
            f"📡 Scanner Status: {mode}\n\n"
            f"Active every {SCAN_INTERVAL}s for {MIN_CONFIDENCE}%+ signals\n"
            f"Coins: {', '.join([c.upper() for c in ALL_COINS])}\n"
            f"AutoScans done: {analytics['auto_scans']}\n"
            f"Last alerts: {analytics['alerts']}",
            reply_markup=back_kb()
        )
        return
    
    elif data == "settings":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Auto Trade", callback_data="settings_auto_trade")],
            [InlineKeyboardButton("💰 Risk %", callback_data="settings_risk")],
            [InlineKeyboardButton("⏱ Scanner Interval", callback_data="settings_interval")],
            [InlineKeyboardButton("📈 Min Confidence", callback_data="settings_confidence")],
            [InlineKeyboardButton("🔒 Close All Positions", callback_data="settings_close_all")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_main")]
        ])
        await q.edit_message_text("⚙️ Settings\n\nConfigure your trading preferences:", reply_markup=kb)
        return
    
    elif data == "settings_auto_trade":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 Enable", callback_data="auto_trade_on")],
            [InlineKeyboardButton("🔴 Disable", callback_data="auto_trade_off")],
            [InlineKeyboardButton("⬅️ Back", callback_data="settings")]
        ])
        await q.edit_message_text("📊 Auto Trade Settings\n\nEnable or disable automated trading:", reply_markup=kb)
        return
    
    elif data == "settings_risk":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("1%", callback_data="risk_1")],
            [InlineKeyboardButton("2%", callback_data="risk_2")],
            [InlineKeyboardButton("3%", callback_data="risk_3")],
            [InlineKeyboardButton("5%", callback_data="risk_5")],
            [InlineKeyboardButton("⬅️ Back", callback_data="settings")]
        ])
        await q.edit_message_text(f"💰 Risk Per Trade\n\nCurrent: {RISK_PERCENT}%\nSelect new risk level:", reply_markup=kb)
        return
    
    elif data == "settings_interval":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("5 min", callback_data="interval_300")],
            [InlineKeyboardButton("10 min", callback_data="interval_600")],
            [InlineKeyboardButton("15 min", callback_data="interval_900")],
            [InlineKeyboardButton("30 min", callback_data="interval_1800")],
            [InlineKeyboardButton("⬅️ Back", callback_data="settings")]
        ])
        await q.edit_message_text(f"⏱ Scanner Interval\n\nCurrent: {SCAN_INTERVAL}s\nSelect new interval:", reply_markup=kb)
        return
    
    elif data == "settings_confidence":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("60%", callback_data="conf_60")],
            [InlineKeyboardButton("65%", callback_data="conf_65")],
            [InlineKeyboardButton("70%", callback_data="conf_70")],
            [InlineKeyboardButton("75%", callback_data="conf_75")],
            [InlineKeyboardButton("⬅️ Back", callback_data="settings")]
        ])
        await q.edit_message_text(f"📈 Minimum Confidence\n\nCurrent: {MIN_CONFIDENCE}%\nSelect new minimum:", reply_markup=kb)
        return
    
    elif data == "settings_close_all":
        async with get_db() as conn:
            cursor = await conn.execute('SELECT id, symbol FROM positions WHERE status = "open"')
            positions = await cursor.fetchall()
            
            if not positions:
                await q.edit_message_text("✅ No open positions to close.", reply_markup=back_kb())
                return
            
            closed_count = 0
            for pos in positions:
                price_data = await get_price(session, pos['symbol'].lower())
                if price_data["price"]:
                    await close_position(pos['id'], price_data["price"])
                    closed_count += 1
            
            await q.edit_message_text(f"✅ Closed {closed_count} positions.", reply_markup=back_kb())
        return
    
    elif data in ["auto_trade_on", "auto_trade_off"]:
        status = "enabled" if "on" in data else "disabled"
        await q.edit_message_text(f"✅ Auto trading {status}.", reply_markup=back_kb())
        return
    
    elif data.startswith("risk_"):
        risk = int(data.split("_")[1])
        await q.edit_message_text(f"✅ Risk per trade set to {risk}%", reply_markup=back_kb())
        return
    
    elif data.startswith("interval_"):
        interval = int(data.split("_")[1])
        await q.edit_message_text(f"✅ Scanner interval set to {interval}s", reply_markup=back_kb())
        return
    
    elif data.startswith("conf_"):
        conf = int(data.split("_")[1])
        await q.edit_message_text(f"✅ Minimum confidence set to {conf}%", reply_markup=back_kb())
        return
    
    else:
        await q.edit_message_text(f"✅ {data} - Module restored", reply_markup=back_kb())

# ==================== HEALTH CHECK ====================
async def health(request):
    try:
        positions = await get_open_positions_count()
        db_size = await get_database_size()
        cache_size = await get_cache_size()
    except Exception as e:
        logging.warning(f"Health check failed: {e}")
        positions = 0
        db_size = 0
        cache_size = 0
    
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent()
    
    return web.json_response({
        "status": "ok",
        "sodex": sodex.ready,
        "symbols": len(SYMBOL_IDS),
        "signals": analytics["signals"],
        "auto_scans": analytics["auto_scans"],
        "alerts": analytics["alerts"],
        "live_trades": analytics["live_trades"],
        "agentic": ENABLE_AUTO_ALERTS,
        "scan_interval": SCAN_INTERVAL,
        "positions": positions,
        "memory_usage": f"{mem.percent}%",
        "cpu_usage": f"{cpu}%",
        "scanner_alive": scanner_alive,
        "telegram_connected": telegram_connected,
        "uptime_seconds": int((datetime.now() - start_time).total_seconds()),
        "database_size_kb": db_size,
        "cache_entries": cache_size,
        "last_scan": last_scan_time.isoformat() if last_scan_time else None,
        "avg_scan_duration": round(avg_scan_duration, 2),
        "last_api_error": last_api_error
    })

async def start_webserver():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 10000))).start()

# ==================== CACHE CLEANUP ====================
async def periodic_cache_cleanup():
    while True:
        try:
            await clean_expired_cache()
            await asyncio.sleep(3600)
        except Exception as e:
            logging.warning(f"Cache cleanup failed: {e}")
            await asyncio.sleep(300)

# ==================== MAIN ====================
async def main():
    logging.info("===== BOT STARTING =====")
    if not TOKEN:
        raise Exception("TELEGRAM_BOT_TOKEN missing")
    
    await init_database()
    await start_webserver()
    
    shared_session = aiohttp.ClientSession(timeout=TIMEOUT)
    
    try:
        logging.info("===== LOADING SODEX SYMBOLS =====")
        await load_symbols(shared_session)
        # FIX 3: Log loaded assets count
        logging.info(f"===== LOADED {len(SYMBOL_IDS)} SYMBOLS =====")
        if len(SYMBOL_IDS) > 0:
            logging.info(f"===== SAMPLE SYMBOLS: {list(SYMBOL_IDS.items())[:5]} =====")
        else:
            logging.warning("===== NO SYMBOLS LOADED! Check SoDEX connection =====")
    except Exception as e:
        logging.exception("Unable to load SoDEX symbols")
        if shared_session and not shared_session.closed:
            await shared_session.close()
        raise
    
    app = ApplicationBuilder().token(TOKEN).build()
    app.bot_data["session"] = shared_session
    app.bot_data["global_data"] = None
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    await app.initialize()
    await app.start()
    
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        logging.info("✅ Webhook deleted")
    except Exception as e:
        logging.warning(f"delete_webhook failed: {e}")
    
    # Start background tasks
    scanner_task = asyncio.create_task(autonomous_scanner(app))
    monitor_task = asyncio.create_task(position_monitor(app))
    cache_cleanup_task = asyncio.create_task(periodic_cache_cleanup())
    global_data_task = asyncio.create_task(refresh_global_data(app))
    
    logging.info("🤖 Autonomous scanner task created")
    logging.info("📊 Position monitor task created")
    logging.info("🧹 Cache cleanup task created")
    logging.info("🌐 Global data refresh task created")
    logging.info("===== BOT POLLING STARTED =====")
    
    retry_delay = 10
    while True:
        try:
            await app.updater.start_polling(
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES
            )
            break
        except Exception as e:
            if "Conflict" in str(e):
                logging.warning(f"Conflict detected - retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                continue
            else:
                logging.exception("Polling crashed")
                await asyncio.sleep(retry_delay)
                continue
    
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutting down...")
    finally:
        for task in [scanner_task, monitor_task, cache_cleanup_task, global_data_task]:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
        except Exception:
            pass
        
        if shared_session and not shared_session.closed:
            await shared_session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise
