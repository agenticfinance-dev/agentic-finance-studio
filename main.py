import os
import asyncio
import logging
import json
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from collections import OrderedDict
import time

import aiosqlite
import psutil
from dotenv import load_dotenv
from aiohttp import web, ClientTimeout, ClientSession
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler

from sodex import SoDEXExecutor, load_symbols, SYMBOL_IDS

load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_TOKEN"))
    SOSO_API_KEY = os.getenv("SOSO_API_KEY")
    SOSO_BASE = "https://openapi.sosovalue.com/openapi/v1"
    SODEX_API_KEY_NAME = os.getenv("SODEX_API_KEY_NAME", os.getenv("SODEX_API_KEY", ""))
    SODEX_API_PRIVATE_KEY = os.getenv("SODEX_API_PRIVATE_KEY", os.getenv("SODEX_PRIVATE_KEY", ""))
    SODEX_ACCOUNT_ID = os.getenv("SODEX_ACCOUNT_ID", "0")
    ALERT_CHAT_ID = int(os.getenv("ALERT_CHAT_ID", "0"))
    HEALTH_API_KEY = os.getenv("HEALTH_API_KEY")
    SIGNER_URL = os.getenv("SIGNER_URL", "https://agenticfinance-signer.onrender.com")
    
    SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "300"))
    ACCOUNT_SIZE = float(os.getenv("ACCOUNT_SIZE", "1000"))
    RISK_PERCENT = float(os.getenv("RISK_PERCENT", "1.5"))
    MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "65"))
    ENABLE_AUTO_ALERTS = os.getenv("ENABLE_AUTO_ALERTS", "true").lower() == "true"
    MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "3"))
    MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))
    VOLATILITY_MIN = float(os.getenv("VOLATILITY_MIN", "0.5"))
    VOLATILITY_MAX = float(os.getenv("VOLATILITY_MAX", "8"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    PORT = int(os.getenv("PORT", "10000"))

    REQUIRED = [
        ("TELEGRAM_BOT_TOKEN", TOKEN),
        ("SOSO_API_KEY", SOSO_API_KEY),
        ("SODEX_API_KEY_NAME", SODEX_API_KEY_NAME),
        ("SODEX_API_PRIVATE_KEY", SODEX_API_PRIVATE_KEY),
    ]


# Validate required environment variables
missing = [name for name, value in Config.REQUIRED if not value]
if missing:
    raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS
# ============================================================================

COINS = ["btc", "eth", "bnb", "xrp", "sol"]
COIN_NAMES = {"btc": "bitcoin", "eth": "ethereum", "bnb": "binancecoin", "xrp": "ripple", "sol": "solana"}
COIN_SYMBOLS = {"btc": "₿", "eth": "Ξ", "bnb": "🟡", "xrp": "✕", "sol": "◎"}
SECTOR_MAP = {
    "AI": ["ETH", "SOL"],
    "L1": ["BTC", "BNB", "SOL"],
    "Payments": ["XRP"],
    "Exchange": ["BNB"]
}
SOSO_HEADERS = {"x-soso-api-key": Config.SOSO_API_KEY or "", "Accept": "application/json"}
DB_PATH = "bot_database.db"
TIMEOUT = ClientTimeout(total=20)

# ============================================================================
# IN-MEMORY CACHE (Faster than SQLite)
# ============================================================================

class TTLCache:
    """Thread-safe TTL cache with max size."""
    def __init__(self, maxsize=1000, ttl=300):
        self._cache = OrderedDict()
        self._maxsize = maxsize
        self._ttl = ttl
        self._lock = asyncio.Lock()
    
    async def get(self, key):
        async with self._lock:
            if key not in self._cache:
                return None
            data, timestamp = self._cache[key]
            if time.monotonic() - timestamp > self._ttl:
                del self._cache[key]
                return None
            self._cache.move_to_end(key)
            return data
    
    async def set(self, key, value):
        async with self._lock:
            if len(self._cache) >= self._maxsize:
                self._cache.popitem(last=False)
            self._cache[key] = (value, time.monotonic())
            self._cache.move_to_end(key)

# Global cache instance
_cache = TTLCache(maxsize=1000, ttl=300)

async def get_cached(key: str, max_age: int = 300):
    """Get cached data from memory."""
    data = await _cache.get(key)
    if data:
        # Check if expired
        if isinstance(data, dict) and "expires" in data:
            if time.monotonic() > data["expires"]:
                return None
        return data
    return None

async def set_cached(key: str, data, ttl: int = 300):
    """Cache data in memory with TTL."""
    data["expires"] = time.monotonic() + ttl
    await _cache.set(key, data)

# ============================================================================
# STATE
# ============================================================================

class State:
    start_time = datetime.now()
    scanner_alive = True
    signer_ready = False
    emergency_stop = False
    last_scan_time = None
    avg_scan_duration = 0
    daily_signals = 0
    daily_alerts = 0
    last_alert_sent = None
    
    analytics = {
        "soso_calls": 0,
        "live_trades": 0,
        "signals": 0,
        "alerts": 0,
        "auto_scans": 0,
        "failed_signals": 0
    }
    last_alert_time = {}
    sent_alerts = {}
    
    # Shared database connection pool
    db_conn = None
    db_lock = asyncio.Lock()
    
    # Order execution lock (prevents race conditions)
    order_lock = asyncio.Lock()
    
    # Signal cache (reuse signals for 60 seconds)
    signal_cache = {}
    signal_cache_time = {}

# ============================================================================
# HELPERS
# ============================================================================

def fmt(p):
    """Format a number for display."""
    if p is None:
        return "N/A"
    try:
        p = float(p)
    except (ValueError, TypeError):
        return str(p)
    if p < 1:
        return f"{p:.6f}"
    elif p < 100:
        return f"{p:.2f}"
    return f"{p:,.0f}"

# ============================================================================
# DATABASE (Connection Pool)
# ============================================================================

async def get_db_conn():
    """Get shared database connection."""
    if State.db_conn is None:
        State.db_conn = await aiosqlite.connect(DB_PATH)
        State.db_conn.row_factory = aiosqlite.Row
    return State.db_conn

@asynccontextmanager
async def get_db():
    """Get database connection with auto-commit and rollback."""
    conn = await get_db_conn()
    try:
        yield conn
    except Exception:
        await conn.rollback()
        raise
    finally:
        await conn.commit()

async def init_db():
    """Initialize database with tables and indexes."""
    conn = await get_db_conn()
    await conn.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA foreign_keys=ON;
        
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, bias TEXT, entry REAL, qty REAL, original_qty REAL,
            sl REAL, tp REAL,
            status TEXT DEFAULT 'open',
            opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            closed_at TIMESTAMP, realized_pnl REAL,
            trail_start REAL, partial_tp1 REAL, partial_tp2 REAL,
            tp1_done INTEGER DEFAULT 0, tp2_done INTEGER DEFAULT 0,
            order_id TEXT, tx_hash TEXT, fill_price REAL, fees REAL
        );
        
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, bias TEXT, entry REAL, exit REAL, qty REAL,
            pnl REAL, pnl_percent REAL, confidence INTEGER, score INTEGER,
            rsi REAL, atr REAL, funding_rate REAL, whale_detected INTEGER,
            sector_score REAL, order_id TEXT, tx_hash TEXT,
            opened_at TIMESTAMP, closed_at TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY, data TEXT, expires_at TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, bias TEXT, entry REAL, exit REAL, qty REAL, pnl REAL,
            confidence INTEGER, score INTEGER, rsi REAL, atr REAL,
            funding_rate REAL, whale_detected INTEGER, sector_score REAL,
            reason TEXT, order_id TEXT, tx_hash TEXT,
            closed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
        CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
        CREATE INDEX IF NOT EXISTS idx_trades_closed ON trades(closed_at);
        CREATE INDEX IF NOT EXISTS idx_cache_key ON cache(key);
    """)
    await conn.commit()
    log.info("✅ Database ready")

async def get_setting(key: str, default: str = None) -> str:
    """Get a setting from the database."""
    async with get_db() as conn:
        row = await conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
        r = await row.fetchone()
        return r["value"] if r else default

async def set_setting(key: str, value: str) -> None:
    """Set a setting in the database."""
    async with get_db() as conn:
        await conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        # Also update memory for emergency stop
        if key == "emergency_stop":
            State.emergency_stop = value.lower() == "true"

# ============================================================================
# INDICATORS
# ============================================================================

def wilder_rsi(closes, period=14):
    """Calculate Wilder's RSI (exponential smoothing)."""
    if not closes or len(closes) < period + 1:
        return None
    
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(diff if diff > 0 else 0)
        losses.append(-diff if diff < 0 else 0)
    
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))

def ema(data, period):
    """Calculate Exponential Moving Average."""
    if not data or len(data) < period:
        return None
    alpha = 2 / (period + 1)
    result = data[0]
    for price in data[1:]:
        result = alpha * price + (1 - alpha) * result
    return result

def wilder_atr(highs, lows, closes, period=14):
    """Calculate Wilder's ATR (exponential smoothing)."""
    if not closes or len(closes) < period + 1:
        return None
    
    trs = []
    for i in range(1, len(highs)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        trs.append(tr)
    
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr

# ============================================================================
# API HELPERS
# ============================================================================

_semaphore = asyncio.Semaphore(10)
_coingecko_cache = None
_coingecko_cache_time = 0

async def safe_get(session, url, headers=None, retries=3):
    """Make HTTP request with retry and rate limiting."""
    for attempt in range(retries):
        try:
            async with _semaphore:
                async with session.get(url, headers=headers or {}, timeout=TIMEOUT) as r:
                    if r.status == 200:
                        return await r.json()
                    # Retry on rate limit and server errors
                    if r.status in (429, 500, 502, 503, 504):
                        wait = (2 ** attempt) * (5 if r.status == 429 else 2)
                        log.warning(f"HTTP {r.status}, retrying in {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    return None
        except Exception as e:
            log.warning(f"HTTP attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                raise
    return None

async def get_coingecko_batch(session):
    """Fetch all coin prices in one request."""
    global _coingecko_cache, _coingecko_cache_time
    
    now = time.monotonic()
    if _coingecko_cache and now - _coingecko_cache_time < 60:
        return _coingecko_cache
    
    try:
        data = await safe_get(
            session,
            f"https://api.coingecko.com/api/v3/simple/price?ids={','.join(COIN_NAMES.values())}&vs_currencies=usd&include_24hr_change=true"
        )
        if data:
            _coingecko_cache = data
            _coingecko_cache_time = now
            return data
    except Exception as e:
        log.debug(f"CoinGecko batch failed: {e}")
    return None

# ============================================================================
# PRICE & INDICATORS
# ============================================================================

async def get_price(session, symbol, use_cache=False):
    """Get current price with fallbacks."""
    sym = symbol.upper()
    if use_cache:
        cached = await get_cached(f"price_{sym}")
        if cached:
            return cached
    
    # Try SoSoValue
    if Config.SOSO_API_KEY:
        try:
            async with _semaphore:
                async with session.get(
                    f"{Config.SOSO_BASE}/token/price",
                    headers=SOSO_HEADERS,
                    params={"symbol": sym},
                    timeout=TIMEOUT
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data.get("code") == 0 and data.get("data"):
                            d = data["data"]
                            price = d.get("price") or d.get("last_price")
                            if price:
                                result = {
                                    "price": float(price),
                                    "change": float(d.get("change_24h", 0) or 0),
                                    "source": "SoSoValue"
                                }
                                await set_cached(f"price_{sym}", result, 60)
                                return result
        except Exception as e:
            log.debug(f"SoSoValue price failed: {e}")
    
    # Try CoinGecko (batch request)
    try:
        data = await get_coingecko_batch(session)
        if data:
            coin = COIN_NAMES.get(sym.lower())
            if coin and data.get(coin, {}).get("usd"):
                result = {
                    "price": float(data[coin]["usd"]),
                    "change": float(data[coin].get("usd_24h_change", 0) or 0),
                    "source": "CoinGecko"
                }
                await set_cached(f"price_{sym}", result, 60)
                return result
    except Exception as e:
        log.debug(f"CoinGecko price failed: {e}")
    
    # Try Binance
    try:
        data = await safe_get(session, f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}USDT")
        if data and data.get("lastPrice"):
            result = {
                "price": float(data["lastPrice"]),
                "change": float(data.get("priceChangePercent", 0) or 0),
                "source": "Binance"
            }
            await set_cached(f"price_{sym}", result, 60)
            return result
    except Exception as e:
        log.debug(f"Binance price failed: {e}")
    
    return await get_cached(f"price_{sym}")

async def get_indicators(session, symbol, use_cache=False):
    """Get technical indicators."""
    sym = symbol.upper()
    if use_cache:
        cached = await get_cached(f"indicators_{sym}")
        if cached:
            return cached
    
    try:
        data = await safe_get(session, f"https://api.binance.com/api/v3/klines?symbol={sym}USDT&interval=1h&limit=60")
        if data and len(data) >= 50:
            closes = [float(x[4]) for x in data]
            highs = [float(x[2]) for x in data]
            lows = [float(x[3]) for x in data]
            
            rsi = wilder_rsi(closes)
            ema20 = ema(closes, 20)
            ema50 = ema(closes, 50)
            atr = wilder_atr(highs, lows, closes)
            vol = sum(float(x[5]) for x in data[-5:])
            avg_vol = sum(float(x[5]) for x in data[-20:]) / 20
            
            if rsi is None or ema20 is None or ema50 is None or atr is None:
                return None
            
            result = {
                "rsi": rsi,
                "ema20": ema20,
                "ema50": ema50,
                "atr": atr,
                "vol_spike": vol > avg_vol * 1.5
            }
            await set_cached(f"indicators_{sym}", result, 120)
            return result
    except Exception as e:
        log.debug(f"Indicators failed: {e}")
    return None

# ============================================================================
# SODEX
# ============================================================================

sodex = SoDEXExecutor(
    Config.SODEX_API_KEY_NAME,
    Config.SODEX_API_PRIVATE_KEY,
    Config.SODEX_ACCOUNT_ID
)
log.info(f"SoDEX ready: {sodex.ready}")

# ============================================================================
# SIGNER
# ============================================================================

async def check_signer(session):
    """Check if the signer service is online."""
    try:
        async with session.get(f"{Config.SIGNER_URL}/health", timeout=5) as r:
            State.signer_ready = r.status == 200
            log.info(f"✅ Signer online: {State.signer_ready}")
    except Exception as e:
        State.signer_ready = False
        log.warning(f"⚠️ Signer unreachable: {e}")

async def execute_order(session, symbol, bias, entry, qty, sl, tp):
    """Execute an order via the signer service with race protection."""
    async with State.order_lock:
        if not State.signer_ready:
            return {"ok": False, "error": "Signer offline"}
        if State.emergency_stop:
            return {"ok": False, "error": "Emergency stop active"}
        if await has_open_position(symbol):
            return {"ok": False, "error": f"Position already open for {symbol}"}
        
        payload = {
            "symbol": symbol,
            "side": bias,
            "price": entry,
            "quantity": qty,
            "stop_loss": sl,
            "take_profit": tp
        }
        
        for attempt in range(Config.MAX_RETRIES):
            try:
                log.info(f"📤 Executing {symbol} {bias} (attempt {attempt+1})")
                async with session.post(
                    f"{Config.SIGNER_URL}/execute",
                    json=payload,
                    timeout=ClientTimeout(total=30)
                ) as r:
                    result = await r.json()
                    if r.status == 200 and result.get("success"):
                        data = result.get("data", {})
                        await open_position_full(
                            symbol, bias, entry, qty, sl, tp,
                            data.get("order_id"),
                            data.get("tx_hash"),
                            data.get("fill_price", entry),
                            data.get("fees", 0)
                        )
                        log.info(f"✅ Order filled: {symbol} {bias}")
                        return {"ok": True, **data}
                    
                    log.warning(f"Attempt {attempt+1} failed: {result.get('error')}")
                    if attempt < Config.MAX_RETRIES - 1:
                        await asyncio.sleep(2 ** attempt * 2)
            except Exception as e:
                log.warning(f"Attempt {attempt+1} error: {e}")
                if attempt < Config.MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** attempt * 2)
        
        return {"ok": False, "error": "Max retries exceeded"}

# ============================================================================
# POSITIONS (with transactions)
# ============================================================================

async def open_position_full(symbol, bias, entry, qty, sl, tp, order_id=None, tx_hash=None, fill_price=None, fees=0):
    """Open a position with full details."""
    async with get_db() as conn:
        trail = entry
        partial1 = entry + (tp - entry) * 0.4 if bias == "LONG" else entry - (entry - tp) * 0.4
        partial2 = entry + (tp - entry) * 0.7 if bias == "LONG" else entry - (entry - tp) * 0.7
        cursor = await conn.execute("""
            INSERT INTO positions (
                symbol, bias, entry, qty, original_qty, sl, tp,
                trail_start, partial_tp1, partial_tp2,
                order_id, tx_hash, fill_price, fees
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol.upper(), bias, entry, qty, qty, sl, tp,
            trail, partial1, partial2,
            order_id, tx_hash, fill_price or entry, fees
        ))
        return cursor.lastrowid

async def has_open_position(symbol):
    """Check if ANY position is open for this symbol."""
    async with get_db() as conn:
        row = await conn.execute(
            "SELECT COUNT(*) FROM positions WHERE symbol = ? AND status='open'",
            (symbol.upper(),)
        )
        return (await row.fetchone())[0] > 0

async def close_position(pos_id, exit_price, reason=""):
    """Close a position and record the trade (atomic transaction)."""
    async with get_db() as conn:
        # Use transaction for consistency
        await conn.execute("BEGIN IMMEDIATE")
        
        row = await conn.execute(
            "SELECT symbol, bias, entry, qty, order_id, tx_hash FROM positions WHERE id = ? AND status='open'",
            (pos_id,)
        )
        pos = await row.fetchone()
        if not pos:
            await conn.execute("ROLLBACK")
            return None
        
        pnl = (exit_price - pos["entry"]) * pos["qty"] if pos["bias"] == "LONG" else (pos["entry"] - exit_price) * pos["qty"]
        pnl_pct = (pnl / (pos["entry"] * pos["qty"])) * 100
        
        await conn.execute(
            "UPDATE positions SET status='closed', closed_at=CURRENT_TIMESTAMP, realized_pnl=? WHERE id=?",
            (pnl, pos_id)
        )
        await conn.execute("""
            INSERT INTO trades (
                symbol, bias, entry, exit, qty, pnl, pnl_percent,
                opened_at, closed_at, order_id, tx_hash
            )
            SELECT symbol, bias, entry, ?, qty, ?, ?,
                   opened_at, CURRENT_TIMESTAMP, order_id, tx_hash
            FROM positions WHERE id=?
        """, (exit_price, pnl, pnl_pct, pos_id))
        await conn.execute("""
            INSERT INTO journal (
                symbol, bias, entry, exit, qty, pnl, reason,
                order_id, tx_hash, closed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (pos["symbol"], pos["bias"], pos["entry"], exit_price, pos["qty"], pnl, reason, pos["order_id"], pos["tx_hash"]))
        
        await conn.execute("COMMIT")
        return {"pnl": pnl, "pnl_percent": pnl_pct}

async def get_open_positions():
    """Get all open positions."""
    async with get_db() as conn:
        rows = await conn.execute(
            "SELECT symbol, bias, entry, qty, original_qty, tp, sl FROM positions WHERE status='open'"
        )
        return await rows.fetchall()

async def get_open_positions_count():
    """Get count of open positions."""
    async with get_db() as conn:
        row = await conn.execute("SELECT COUNT(*) FROM positions WHERE status='open'")
        return (await row.fetchone())[0]

async def get_daily_pnl():
    """Get today's realized PnL."""
    today = datetime.now().date().isoformat()
    async with get_db() as conn:
        row = await conn.execute("SELECT SUM(pnl) FROM trades WHERE date(closed_at) = ?", (today,))
        return (await row.fetchone())[0] or 0

# ============================================================================
# MARKET DATA
# ============================================================================

async def get_funding_rates(session):
    """Get funding rates from Binance."""
    cached = await get_cached("funding")
    if cached:
        return cached
    
    try:
        data = await safe_get(session, "https://fapi.binance.com/fapi/v1/premiumIndex")
        if data:
            rates = []
            for item in data:
                sym = item.get("symbol", "")
                for coin in COINS:
                    if sym.startswith(coin.upper()) and sym.endswith("USDT"):
                        rates.append({
                            "symbol": coin.upper(),
                            "rate": float(item.get("lastFundingRate", 0)) * 100,
                            "source": "Binance"
                        })
            if rates:
                await set_cached("funding", rates, 60)
                return rates
    except Exception as e:
        log.debug(f"Funding failed: {e}")
    return []

async def get_etf_flows(session):
    """Get ETF flows from SoSoValue."""
    cached = await get_cached("etf")
    if cached:
        return cached
    
    if Config.SOSO_API_KEY:
        try:
            data = await safe_get(session, f"{Config.SOSO_BASE}/etf/flows", headers=SOSO_HEADERS)
            if data and data.get("code") == 0:
                etf_data = data.get("data", {})
                await set_cached("etf", etf_data, 60)
                return etf_data
        except Exception:
            pass
    return {}

async def get_whale_data(session):
    """Get whale activity data."""
    cached = await get_cached("whales")
    if cached:
        return cached
    
    if Config.SOSO_API_KEY:
        try:
            data = await safe_get(session, f"{Config.SOSO_BASE}/market/whale", headers=SOSO_HEADERS)
            if data and data.get("code") == 0:
                whale_data = data.get("data", [])
                await set_cached("whales", whale_data, 60)
                return whale_data
        except Exception:
            pass
    return []

# ============================================================================
# SCORING ENGINE
# ============================================================================

def calc_position_size(entry, sl, account=Config.ACCOUNT_SIZE, risk=Config.RISK_PERCENT):
    """Calculate position size with risk management."""
    if entry is None or sl is None or entry == sl:
        return None
    risk_amount = account * (risk / 100)
    stop_dist = abs(entry - sl)
    if stop_dist == 0:
        return None
    position_value = risk_amount / (stop_dist / entry)
    qty = position_value / entry
    min_qty, step = 0.001, 0.001
    max_qty = (account * 0.95) / entry
    qty = max(min_qty, min(qty, max_qty))
    return round(qty / step) * step

async def get_signal(session, symbol, global_data=None, use_cache=False, price=None, indicators=None):
    """Generate a trading signal with detailed logging."""
    # Check signal cache
    cache_key = f"{symbol}_{int(time.monotonic() / 60)}"
    if use_cache and cache_key in State.signal_cache:
        return State.signal_cache[cache_key]
    
    try:
        # Use provided price/indicators or fetch them
        if price is None or indicators is None:
            price, indicators = await asyncio.gather(
                get_price(session, symbol, use_cache),
                get_indicators(session, symbol, use_cache)
            )
        
        if not price:
            log.warning(f"{symbol}: price unavailable")
            return None
        
        if not indicators:
            log.warning(f"{symbol}: indicators unavailable")
            return None
        
        if indicators.get("atr") is None:
            log.warning(f"{symbol}: ATR unavailable")
            return None
        
        if global_data:
            funding = global_data.get("funding", [])
            whales = global_data.get("whales", [])
            etf = global_data.get("etf", {})
        else:
            funding, whales, etf = await asyncio.gather(
                get_funding_rates(session),
                get_whale_data(session),
                get_etf_flows(session)
            )
        
        score = 50
        breakdown = []
        
        # Trend (35%)
        trend = 0
        if indicators.get("ema20") is not None and indicators.get("ema50") is not None:
            trend += 15 if indicators["ema20"] > indicators["ema50"] else -10
            breakdown.append("📈 Bullish" if indicators["ema20"] > indicators["ema50"] else "📉 Bearish")
        
        if price.get("change") is not None:
            if price["change"] > 2:
                trend += 10
                breakdown.append(f"📊 +{price['change']:.1f}%")
            elif price["change"] < -2:
                trend -= 10
                breakdown.append(f"📊 {price['change']:.1f}%")
        score += trend * 0.35
        
        # Volume (15%)
        vol = 15 if indicators.get("vol_spike") else 0
        if vol:
            breakdown.append("📊 Volume Spike")
        score += vol * 0.15
        
        # RSI (10%)
        if indicators.get("rsi") is not None:
            rsi = indicators["rsi"]
            if rsi < 35:
                score += 10
                breakdown.append(f"📊 RSI Oversold {rsi:.0f}")
            elif rsi > 70:
                score -= 10
                breakdown.append(f"📊 RSI Overbought {rsi:.0f}")
            else:
                breakdown.append(f"📊 RSI Neutral {rsi:.0f}")
            score += (10 if rsi < 35 else -10 if rsi > 70 else 0) * 0.10
        
        # Funding (10%)
        for f in funding:
            if f.get("symbol") == symbol.upper():
                if f["rate"] > 0.05:
                    score += 10
                    breakdown.append(f"💰 {f['rate']:+.2f}%")
                elif f["rate"] < -0.05:
                    score -= 10
                    breakdown.append(f"💰 {f['rate']:+.2f}%")
                break
        
        # Whale (10%)
        if any(w.get("symbol") == symbol.upper() for w in whales if isinstance(w, dict)):
            score += 10
            breakdown.append("🐳 Whale Activity")
        
        # ETF (10%)
        if etf:
            for key, value in etf.items():
                if key.lower() == symbol.lower():
                    inflow = value.get("inflow", 0)
                    if inflow > 1_000_000:
                        score += 10
                        breakdown.append("🏦 Strong Inflow")
                    elif inflow > 500_000:
                        score += 5
                        breakdown.append("🏦 Moderate Inflow")
                    break
        
        score = max(0, min(100, round(score)))
        
        # Determine bias
        if score >= 65:
            bias, action, emoji = "LONG", "Accumulate", "🟢"
            entry = price["price"] * 0.992
            sl = entry - indicators["atr"] * 1.2
            tp = entry + indicators["atr"] * 2.8
        elif score <= 40:
            bias, action, emoji = "SHORT", "Reduce", "🔴"
            entry = price["price"] * 1.008
            sl = entry + indicators["atr"] * 1.2
            tp = entry - indicators["atr"] * 2.8
        else:
            return None
        
        if indicators["atr"] is None or indicators["atr"] <= 0:
            return None
        
        rr = abs(tp - entry) / abs(entry - sl) if entry != sl else 0
        if rr < 1.5:
            log.info(f"{symbol}: RR too low ({rr:.2f})")
            return None
        
        qty = calc_position_size(entry, sl)
        if qty is None or qty <= 0:
            log.warning(f"{symbol}: position size failed")
            return None
        
        result = {
            "symbol": symbol.upper(),
            "price": price["price"],
            "change": price.get("change"),
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "rr": rr,
            "confidence": score,
            "bias": bias,
            "action": action,
            "emoji": emoji,
            "qty": qty,
            "rsi": indicators.get("rsi"),
            "breakdown": breakdown[:4],
            "score": score,
            "source": price.get("source", "Unknown")
        }
        
        # Cache signal for 60 seconds
        if use_cache:
            State.signal_cache[cache_key] = result
        
        return result
    except Exception as e:
        log.exception(f"Signal failed for {symbol}: {e}")
        State.analytics["failed_signals"] += 1
        return None

# ============================================================================
# POSITION MANAGEMENT
# ============================================================================

async def manage_positions(session, app):
    """Monitor and manage open positions."""
    async with get_db() as conn:
        rows = await conn.execute("SELECT * FROM positions WHERE status='open'")
        positions = await rows.fetchall()
        
        if not positions:
            return
        
        # Fetch all prices in parallel
        prices = await asyncio.gather(
            *[get_price(session, p["symbol"].lower()) for p in positions]
        )
        
        for pos, price_data in zip(positions, prices):
            if not price_data or price_data.get("price") is None:
                continue
            
            current = price_data["price"]
            entry = pos["entry"]
            sl = pos["sl"]
            tp = pos["tp"]
            p1 = pos["partial_tp1"]
            p2 = pos["partial_tp2"]
            tp1_done = pos["tp1_done"]
            tp2_done = pos["tp2_done"]
            original_qty = pos["original_qty"]
            
            if pos["bias"] == "LONG":
                # Trailing stop
                if current - entry >= (entry - sl) and sl < entry:
                    await conn.execute("UPDATE positions SET sl = ? WHERE id = ?", (entry, pos["id"]))
                    log.info(f"📊 Trailing stop: {pos['symbol']} -> breakeven")
                
                # Partial TP1 (40% of original) - only once
                if current >= p1 and not tp1_done and pos["qty"] > 0.001:
                    qty_close = original_qty * 0.4
                    qty_remaining = max(0, pos["qty"] - qty_close)
                    await conn.execute(
                        "UPDATE positions SET qty = ?, tp1_done = 1 WHERE id = ?",
                        (qty_remaining, pos["id"])
                    )
                    log.info(f"📊 TP1: {pos['symbol']} at ${p1:.2f}")
                
                # Partial TP2 (30% of original) - only once
                if current >= p2 and not tp2_done and pos["qty"] > 0.001:
                    qty_close = original_qty * 0.3
                    qty_remaining = max(0, pos["qty"] - qty_close)
                    await conn.execute(
                        "UPDATE positions SET qty = ?, tp2_done = 1 WHERE id = ?",
                        (qty_remaining, pos["id"])
                    )
                    log.info(f"📊 TP2: {pos['symbol']} at ${p2:.2f}")
                
                # Full TP (remaining)
                if current >= tp and pos["qty"] > 0.001:
                    await close_position(pos["id"], tp, "TP Hit")
                    log.info(f"✅ {pos['symbol']} TP hit")
                    await send_telegram_message(app, f"✅ {pos['symbol']} TP hit at ${tp:.2f}")
                    continue
                
                # SL
                if current <= sl:
                    await close_position(pos["id"], sl, "SL Hit")
                    log.info(f"❌ {pos['symbol']} SL hit")
                    await send_telegram_message(app, f"❌ {pos['symbol']} SL hit at ${sl:.2f}")
                    continue
            
            else:  # SHORT
                # Trailing stop
                if entry - current >= (entry - sl) and sl > entry:
                    await conn.execute("UPDATE positions SET sl = ? WHERE id = ?", (entry, pos["id"]))
                    log.info(f"📊 Trailing stop: {pos['symbol']} -> breakeven")
                
                # Partial TP1 (40% of original) - only once
                if current <= p1 and not tp1_done and pos["qty"] > 0.001:
                    qty_close = original_qty * 0.4
                    qty_remaining = max(0, pos["qty"] - qty_close)
                    await conn.execute(
                        "UPDATE positions SET qty = ?, tp1_done = 1 WHERE id = ?",
                        (qty_remaining, pos["id"])
                    )
                    log.info(f"📊 TP1: {pos['symbol']} at ${p1:.2f}")
                
                # Partial TP2 (30% of original) - only once
                if current <= p2 and not tp2_done and pos["qty"] > 0.001:
                    qty_close = original_qty * 0.3
                    qty_remaining = max(0, pos["qty"] - qty_close)
                    await conn.execute(
                        "UPDATE positions SET qty = ?, tp2_done = 1 WHERE id = ?",
                        (qty_remaining, pos["id"])
                    )
                    log.info(f"📊 TP2: {pos['symbol']} at ${p2:.2f}")
                
                # Full TP (remaining)
                if current <= tp and pos["qty"] > 0.001:
                    await close_position(pos["id"], tp, "TP Hit")
                    log.info(f"✅ {pos['symbol']} TP hit")
                    await send_telegram_message(app, f"✅ {pos['symbol']} TP hit at ${tp:.2f}")
                    continue
                
                # SL
                if current >= sl:
                    await close_position(pos["id"], sl, "SL Hit")
                    log.info(f"❌ {pos['symbol']} SL hit")
                    await send_telegram_message(app, f"❌ {pos['symbol']} SL hit at ${sl:.2f}")
                    continue

async def send_telegram_message(app, text):
    """Send Telegram message with retry."""
    if not Config.ALERT_CHAT_ID:
        return
    try:
        await app.bot.send_message(Config.ALERT_CHAT_ID, text)
    except Exception as e:
        log.warning(f"Failed to send Telegram message: {e}")

# ============================================================================
# SCANNER
# ============================================================================

async def refresh_data(app):
    """Refresh global market data in background."""
    session = app.bot_data["session"]
    while True:
        try:
            start_time = time.monotonic()
            log.info("🔄 Refreshing market data...")
            
            funding, whales, etf = await asyncio.gather(
                get_funding_rates(session),
                get_whale_data(session),
                get_etf_flows(session)
            )
            
            global_data = {"funding": funding, "whales": whales, "etf": etf}
            
            # Fetch all coin data in parallel
            tasks = []
            for coin in COINS:
                tasks.append(get_price(session, coin, use_cache=True))
                tasks.append(get_indicators(session, coin, use_cache=True))
            results = await asyncio.gather(*tasks)
            
            market = {}
            for i, coin in enumerate(COINS):
                market[coin] = {
                    "price": results[i * 2],
                    "indicators": results[i * 2 + 1]
                }
            
            # Generate signals in parallel
            signals = await asyncio.gather(*[
                get_signal(session, coin, global_data, use_cache=True, 
                          price=market[coin]["price"], 
                          indicators=market[coin]["indicators"])
                for coin in COINS
            ])
            
            scores = {}
            for coin, sig in zip(COINS, signals):
                if sig:
                    scores[coin] = sig
            
            app.bot_data["global_data"] = {
                "market": market,
                "scores": scores,
                "funding": funding,
                "whales": whales,
                "etf": etf,
                "timestamp": datetime.now().isoformat()
            }
            
            elapsed = time.monotonic() - start_time
            log.info(f"✅ Data refreshed: {len(scores)} signals, {len(market)} market entries ({elapsed:.1f}s)")
        except Exception as e:
            log.exception(f"Refresh error: {e}")
        
        await asyncio.sleep(300)

async def scanner_loop(app):
    """Main scanner loop for signal generation and auto-execution."""
    await asyncio.sleep(10)
    session = app.bot_data["session"]
    
    while True:
        try:
            if not Config.ENABLE_AUTO_ALERTS:
                await asyncio.sleep(Config.SCAN_INTERVAL)
                continue
            
            scan_start = time.monotonic()
            
            # Check emergency stop (sync from database)
            State.emergency_stop = (await get_setting("emergency_stop", "false")) == "true"
            
            # Check signer
            if not State.signer_ready or not sodex.ready:
                await asyncio.sleep(60)
                continue
            
            # Check daily loss
            daily_loss = await get_daily_pnl()
            if daily_loss < -Config.ACCOUNT_SIZE * (Config.MAX_DAILY_LOSS / 100):
                if not State.emergency_stop:
                    State.emergency_stop = True
                    await set_setting("emergency_stop", "true")
                    log.warning(f"⚠️ Emergency stop: Daily loss ${fmt(daily_loss)}")
                    await send_telegram_message(app, f"⚠️ Emergency stop: Daily loss ${fmt(daily_loss)}")
                await asyncio.sleep(Config.SCAN_INTERVAL)
                continue
            
            # Check max positions
            if await get_open_positions_count() >= Config.MAX_POSITIONS:
                await asyncio.sleep(Config.SCAN_INTERVAL)
                continue
            
            global_data = app.bot_data.get("global_data", {})
            State.analytics["auto_scans"] += 1
            
            # Scan all coins in parallel
            signals = await asyncio.gather(*[
                get_signal(session, coin, global_data, use_cache=True)
                for coin in COINS
            ])
            
            valid = [s for s in signals if s and s["confidence"] >= Config.MIN_CONFIDENCE]
            
            if valid:
                State.daily_signals += len(valid)
                State.analytics["signals"] += len(valid)
                State.daily_alerts += 1
                State.last_alert_sent = datetime.now()
                
                # Send summary
                if Config.ALERT_CHAT_ID:
                    summary = "🤖 **AGENTIC ALERT**\n\n"
                    for s in sorted(valid, key=lambda x: x["confidence"], reverse=True)[:5]:
                        summary += f"{s['symbol']} {s['bias']} | {s['confidence']}%\n"
                        summary += f"💰 ${fmt(s['price'])} ({fmt(s['change'])}%)\n"
                        summary += f"Entry: ${fmt(s['entry'])} | TP: ${fmt(s['tp'])} | SL: ${fmt(s['sl'])}\n\n"
                    
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("📊 View", callback_data="dashboard")]])
                    try:
                        await app.bot.send_message(
                            Config.ALERT_CHAT_ID,
                            summary,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=kb
                        )
                        State.analytics["alerts"] += len(valid)
                    except Exception as e:
                        log.warning(f"Failed to send alert: {e}")
                
                # Auto-execute best signal
                top = valid[0]
                log.info(f"🎯 Top signal: {top['symbol']} {top['score']} {top['confidence']}%")
                
                if top["confidence"] >= 75 and not await has_open_position(top["symbol"]):
                    log.info(f"🤖 Auto-executing {top['symbol']} {top['bias']}")
                    result = await execute_order(
                        session,
                        top["symbol"],
                        top["bias"],
                        top["entry"],
                        top["qty"],
                        top["sl"],
                        top["tp"]
                    )
                    if result.get("ok"):
                        log.info(f"✅ Auto-execution successful: {top['symbol']}")
                        State.analytics["live_trades"] += 1
                        await send_telegram_message(app, f"✅ **Auto-Executed**: {top['symbol']} {top['bias']}\nEntry: ${fmt(top['entry'])}")
            
            State.last_scan_time = datetime.now()
            elapsed = time.monotonic() - scan_start
            State.avg_scan_duration = State.avg_scan_duration * 0.9 + elapsed * 0.1
            State.scanner_alive = True
            
            # Schedule next scan accounting for elapsed time
            sleep_time = max(0, Config.SCAN_INTERVAL - elapsed)
            await asyncio.sleep(sleep_time)
            
        except Exception as e:
            State.scanner_alive = False
            log.exception(f"Scanner error: {e}")
            await asyncio.sleep(Config.SCAN_INTERVAL)

# ============================================================================
# TELEGRAM
# ============================================================================

def main_menu():
    """Main menu keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Dashboard", callback_data="dashboard")],
        [InlineKeyboardButton("⚡ Execute", callback_data="execute_menu"), InlineKeyboardButton("📂 Portfolio", callback_data="portfolio")],
        [InlineKeyboardButton("🐳 Whales", callback_data="whales"), InlineKeyboardButton("💰 Funding", callback_data="funding")],
        [InlineKeyboardButton("🔥 Liquidations", callback_data="liquidations"), InlineKeyboardButton("🏦 ETF", callback_data="etf_flows")],
        [InlineKeyboardButton("🧠 AI Analysis", callback_data="ai_intel"), InlineKeyboardButton("🗺 Sector Map", callback_data="sector_map")],
        [InlineKeyboardButton("📊 Stats", callback_data="stats"), InlineKeyboardButton("📡 Scanner", callback_data="scanner_on")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
    ])

def execute_menu():
    """Execute menu keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{COIN_SYMBOLS['btc']} BTC", callback_data="exec_btc"),
         InlineKeyboardButton(f"{COIN_SYMBOLS['eth']} ETH", callback_data="exec_eth")],
        [InlineKeyboardButton(f"{COIN_SYMBOLS['bnb']} BNB", callback_data="exec_bnb"),
         InlineKeyboardButton(f"{COIN_SYMBOLS['xrp']} XRP", callback_data="exec_xrp")],
        [InlineKeyboardButton(f"{COIN_SYMBOLS['sol']} SOL", callback_data="exec_sol")],
        [InlineKeyboardButton("⬅️ Back", callback_data="back_main")]
    ])

def back_kb():
    """Back button keyboard."""
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_main")]])

def risk_kb():
    """Risk settings keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1%", callback_data="risk_1"), InlineKeyboardButton("2%", callback_data="risk_2"),
         InlineKeyboardButton("3%", callback_data="risk_3"), InlineKeyboardButton("5%", callback_data="risk_5")],
        [InlineKeyboardButton("⬅️ Back", callback_data="settings")]
    ])

def interval_kb():
    """Interval settings keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("5m", callback_data="interval_300"), InlineKeyboardButton("10m", callback_data="interval_600"),
         InlineKeyboardButton("15m", callback_data="interval_900"), InlineKeyboardButton("30m", callback_data="interval_1800")],
        [InlineKeyboardButton("⬅️ Back", callback_data="settings")]
    ])

def confidence_kb():
    """Confidence settings keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("60%", callback_data="conf_60"), InlineKeyboardButton("65%", callback_data="conf_65"),
         InlineKeyboardButton("70%", callback_data="conf_70"), InlineKeyboardButton("75%", callback_data="conf_75")],
        [InlineKeyboardButton("⬅️ Back", callback_data="settings")]
    ])

def maxpos_kb():
    """Max positions settings keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1", callback_data="maxpos_1"), InlineKeyboardButton("2", callback_data="maxpos_2"),
         InlineKeyboardButton("3", callback_data="maxpos_3"), InlineKeyboardButton("5", callback_data="maxpos_5")],
        [InlineKeyboardButton("⬅️ Back", callback_data="settings")]
    ])

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    await update.message.reply_text(
        "🚀 **Agentic Finance**\n\nInstitutional-grade trading intelligence.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_menu()
    )

# ============================================================================
# BUTTON HANDLER
# ============================================================================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all callback queries."""
    q = update.callback_query
    await q.answer()
    session = context.application.bot_data["session"]
    data = q.data
    
    if data == "back_main":
        await q.edit_message_text("🚀 **Agentic Finance**", parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu())
        return
    
    # Dashboard
    if data == "dashboard":
        global_data = context.application.bot_data.get("global_data", {})
        market = global_data.get("market", {})
        scores = global_data.get("scores", {})
        
        txt = "📊 **Dashboard**\n\n"
        for coin in COINS:
            p = market.get(coin, {}).get("price")
            sig = scores.get(coin)
            
            txt += f"**{coin.upper()}**\n"
            if p:
                txt += f"💰 ${fmt(p['price'])} ({fmt(p.get('change', 0))}%)\n"
                txt += f"📡 {p.get('source', 'Unknown')}\n"
            else:
                txt += "💰 No price data\n"
            
            if sig:
                txt += f"📊 {sig['bias']} ({sig['score']}/100)\n"
                txt += f"Entry: ${fmt(sig['entry'])} | TP: ${fmt(sig['tp'])}\n"
            else:
                txt += "📊 NEUTRAL\n"
            
            txt += "\n"
        
        txt += f"📈 Signals: {State.daily_signals} | Alerts: {State.daily_alerts}\n"
        txt += f"🖊 Signer: {'🟢 Online' if State.signer_ready else '🔴 Offline'}\n"
        txt += f"🛑 Stop: {'🔴 Active' if State.emergency_stop else '🟢 Normal'}"
        await q.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb())
        return
    
    # Execute
    if data == "execute_menu":
        await q.edit_message_text("⚡ **Select Asset**", parse_mode=ParseMode.MARKDOWN, reply_markup=execute_menu())
        return
    
    if data.startswith("exec_"):
        sym = data.split("_")[1]
        await q.edit_message_text(f"⏳ Analyzing {sym.upper()}...", reply_markup=back_kb())
        
        if State.emergency_stop:
            await q.edit_message_text("⚠️ Emergency stop active", reply_markup=back_kb())
            return
        
        if await get_daily_pnl() < -Config.ACCOUNT_SIZE * (Config.MAX_DAILY_LOSS / 100):
            await q.edit_message_text(f"⚠️ Daily loss limit reached ({Config.MAX_DAILY_LOSS}%)", reply_markup=back_kb())
            return
        
        if await get_open_positions_count() >= Config.MAX_POSITIONS:
            await q.edit_message_text(f"⚠️ Max positions ({Config.MAX_POSITIONS})", reply_markup=back_kb())
            return
        
        if not State.signer_ready:
            await q.edit_message_text("⚠️ Signer offline", reply_markup=back_kb())
            return
        
        sig = await get_signal(session, sym)
        if not sig:
            await q.edit_message_text("❌ No signal available", reply_markup=back_kb())
            return
        
        if sig["confidence"] < Config.MIN_CONFIDENCE:
            await q.edit_message_text(f"⚠️ Confidence {sig['confidence']}% < {Config.MIN_CONFIDENCE}%", reply_markup=back_kb())
            return
        
        result = await execute_order(session, sym, sig["bias"], sig["entry"], sig["qty"], sig["sl"], sig["tp"])
        
        if result.get("ok"):
            txt = f"✅ **Filled**\n\n{sig['symbol']} {sig['bias']}\nEntry: ${fmt(sig['entry'])}\nQty: {sig['qty']:.4f}\nOrder: {result.get('order_id', 'N/A')}"
        else:
            txt = f"❌ **Failed**\n\n{result.get('error')}"
        
        await q.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb())
        return
    
    # Portfolio
    if data == "portfolio":
        positions = await get_open_positions()
        if not positions:
            txt = "📂 No open positions."
        else:
            txt = "📂 **Open Positions**\n\n"
            for r in positions:
                txt += (
                    f"**{r['symbol']} {r['bias']}**\n"
                    f"Entry: ${fmt(r['entry'])}\n"
                    f"Qty: {r['qty']:.4f}\n"
                    f"TP: ${fmt(r['tp'])}\n"
                    f"SL: ${fmt(r['sl'])}\n\n"
                )
        
        await q.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb())
        return
    
    # Stats
    if data == "stats":
        await q.edit_message_text(
            f"📊 **Stats**\n\n"
            f"Trades: {State.analytics['live_trades']}\n"
            f"Positions: {await get_open_positions_count()}\n"
            f"Daily PnL: ${fmt(await get_daily_pnl())}\n"
            f"Scans: {State.analytics['auto_scans']}\n"
            f"Signals: {State.analytics['signals']}\n"
            f"Failed Signals: {State.analytics['failed_signals']}\n"
            f"Alerts: {State.analytics['alerts']}\n"
            f"Signer: {'🟢 Online' if State.signer_ready else '🔴 Offline'}\n"
            f"Stop: {'🔴 Active' if State.emergency_stop else '🟢 Normal'}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_kb()
        )
        return
    
    # Quick data views
    if data == "whales":
        whales = await get_whale_data(session)
        txt = "🐳 **Whales**\n\n" + "\n".join([f"• {w.get('symbol', 'Unknown')}: ${w.get('amount', 0):,.0f}" for w in whales[:5]]) if whales else "🐳 No whale activity."
        await q.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb())
        return
    
    if data == "funding":
        funding = await get_funding_rates(session)
        txt = "💰 **Funding**\n\n" + "\n".join([f"• {f['symbol']}: {f['rate']:+.2f}%" for f in funding[:5]]) if funding else "💰 No funding data."
        await q.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb())
        return
    
    if data == "etf_flows":
        etf = await get_etf_flows(session)
        if etf:
            txt = "🏦 **ETF Flows**\n\n" + "\n".join([f"• {k.upper()}: Net ${v.get('inflow', 0):,.0f}" for k, v in etf.items() if isinstance(v, dict)])
            await q.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb())
        else:
            await q.edit_message_text("🏦 ETF data unavailable.", reply_markup=back_kb())
        return
    
    if data == "ai_intel":
        global_data = context.application.bot_data.get("global_data", {})
        market = global_data.get("market", {})
        
        txt = "🧠 **AI Analysis**\n\n"
        for coin in COINS:
            p = market.get(coin, {}).get("price")
            ind = market.get(coin, {}).get("indicators")
            
            txt += f"**{coin.upper()}**\n"
            if p:
                txt += f"💰 ${fmt(p['price'])} ({p.get('source', 'Unknown')})\n"
                txt += f"📊 24h Change: {fmt(p.get('change', 0))}%\n"
            if ind:
                txt += f"📈 RSI: {fmt(ind.get('rsi'))}\n"
                txt += f"📊 Vol Spike: {'Yes' if ind.get('vol_spike') else 'No'}\n"
            txt += "\n"
        
        await q.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb())
        return
    
    if data == "sector_map":
        txt = "🗺 **Sector Map**\n\n"
        for sector, coins in SECTOR_MAP.items():
            txt += f"**{sector}**\n"
            for c in coins:
                p = await get_price(session, c.lower(), use_cache=True)
                if p:
                    txt += f"  {c}: ${fmt(p['price'])} ({p.get('source', 'Unknown')})\n"
                else:
                    txt += f"  {c}: No price\n"
            txt += "\n"
        
        await q.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb())
        return
    
    if data == "liquidations":
        await q.edit_message_text("🔥 Liquidation data unavailable.\n\nCoinGlass integration coming soon.", reply_markup=back_kb())
        return
    
    if data == "scanner_on":
        await q.edit_message_text(
            f"📡 **Scanner**\n\n"
            f"Status: {'ACTIVE' if Config.ENABLE_AUTO_ALERTS else 'DISABLED'}\n"
            f"Interval: {Config.SCAN_INTERVAL}s\n"
            f"Confidence: {Config.MIN_CONFIDENCE}%\n"
            f"Max Positions: {Config.MAX_POSITIONS}\n"
            f"Daily Loss: {Config.MAX_DAILY_LOSS}%\n"
            f"Last Scan: {State.last_scan_time.strftime('%H:%M:%S') if State.last_scan_time else 'Never'}\n"
            f"Avg Duration: {State.avg_scan_duration:.1f}s\n"
            f"Signer: {'🟢 Online' if State.signer_ready else '🔴 Offline'}\n"
            f"Stop: {'🔴 Active' if State.emergency_stop else '🟢 Normal'}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_kb()
        )
        return
    
    if data == "settings":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Risk", callback_data="settings_risk")],
            [InlineKeyboardButton("⏱ Interval", callback_data="settings_interval")],
            [InlineKeyboardButton("📈 Confidence", callback_data="settings_confidence")],
            [InlineKeyboardButton("🤖 Auto Trade", callback_data="settings_auto_trade")],
            [InlineKeyboardButton("📊 Max Positions", callback_data="settings_max_pos")],
            [InlineKeyboardButton("🛑 Emergency", callback_data="settings_emergency")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_main")]
        ])
        await q.edit_message_text(
            f"⚙️ **Settings**\n\n"
            f"Risk: {Config.RISK_PERCENT}%\n"
            f"Interval: {Config.SCAN_INTERVAL}s\n"
            f"Confidence: {Config.MIN_CONFIDENCE}%\n"
            f"Auto: {'ON' if Config.ENABLE_AUTO_ALERTS else 'OFF'}\n"
            f"Max Positions: {Config.MAX_POSITIONS}\n"
            f"Stop: {'🔴 ACTIVE' if State.emergency_stop else '🟢 NORMAL'}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb
        )
        return
    
    # Settings submenus
    if data == "settings_risk":
        await q.edit_message_text(f"💰 **Risk**\n\nCurrent: {Config.RISK_PERCENT}%", parse_mode=ParseMode.MARKDOWN, reply_markup=risk_kb())
        return
    
    if data == "settings_interval":
        await q.edit_message_text(f"⏱ **Interval**\n\nCurrent: {Config.SCAN_INTERVAL}s", parse_mode=ParseMode.MARKDOWN, reply_markup=interval_kb())
        return
    
    if data == "settings_confidence":
        await q.edit_message_text(f"📈 **Confidence**\n\nCurrent: {Config.MIN_CONFIDENCE}%", parse_mode=ParseMode.MARKDOWN, reply_markup=confidence_kb())
        return
    
    if data == "settings_auto_trade":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ ON", callback_data="auto_on"), InlineKeyboardButton("❌ OFF", callback_data="auto_off")],
            [InlineKeyboardButton("⬅️ Back", callback_data="settings")]
        ])
        await q.edit_message_text(f"🤖 **Auto Trade**\n\nCurrent: {'ON' if Config.ENABLE_AUTO_ALERTS else 'OFF'}", parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        return
    
    if data == "settings_max_pos":
        await q.edit_message_text(f"📊 **Max Positions**\n\nCurrent: {Config.MAX_POSITIONS}", parse_mode=ParseMode.MARKDOWN, reply_markup=maxpos_kb())
        return
    
    if data == "settings_emergency":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔴 Activate", callback_data="emergency_on")],
            [InlineKeyboardButton("🟢 Deactivate", callback_data="emergency_off")],
            [InlineKeyboardButton("⬅️ Back", callback_data="settings")]
        ])
        await q.edit_message_text(
            f"🛑 **Emergency Stop**\n\nCurrent: {'🔴 ACTIVE' if State.emergency_stop else '🟢 NORMAL'}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb
        )
        return
    
    # Settings actions
    if data.startswith("risk_"):
        Config.RISK_PERCENT = float(data.split("_")[1])
        await set_setting("risk_percent", str(Config.RISK_PERCENT))
        await q.edit_message_text(f"✅ Risk set to {Config.RISK_PERCENT}%", reply_markup=back_kb())
        return
    
    if data.startswith("interval_"):
        Config.SCAN_INTERVAL = int(data.split("_")[1])
        await set_setting("scan_interval", str(Config.SCAN_INTERVAL))
        await q.edit_message_text(f"✅ Interval set to {Config.SCAN_INTERVAL}s", reply_markup=back_kb())
        return
    
    if data.startswith("conf_"):
        Config.MIN_CONFIDENCE = int(data.split("_")[1])
        await set_setting("min_confidence", str(Config.MIN_CONFIDENCE))
        await q.edit_message_text(f"✅ Confidence set to {Config.MIN_CONFIDENCE}%", reply_markup=back_kb())
        return
    
    if data.startswith("maxpos_"):
        Config.MAX_POSITIONS = int(data.split("_")[1])
        await set_setting("max_positions", str(Config.MAX_POSITIONS))
        await q.edit_message_text(f"✅ Max positions set to {Config.MAX_POSITIONS}", reply_markup=back_kb())
        return
    
    if data in ["auto_on", "auto_off"]:
        Config.ENABLE_AUTO_ALERTS = data == "auto_on"
        await set_setting("auto_trade", str(Config.ENABLE_AUTO_ALERTS).lower())
        await q.edit_message_text(f"✅ Auto trade {'ON' if Config.ENABLE_AUTO_ALERTS else 'OFF'}", reply_markup=back_kb())
        return
    
    if data in ["emergency_on", "emergency_off"]:
        State.emergency_stop = data == "emergency_on"
        await set_setting("emergency_stop", str(State.emergency_stop).lower())
        await q.edit_message_text(
            f"🛑 Emergency Stop {'ACTIVATED' if State.emergency_stop else 'DEACTIVATED'}",
            reply_markup=back_kb()
        )
        return
    
    await q.edit_message_text("✅ Done", reply_markup=back_kb())

# ============================================================================
# HEALTH
# ============================================================================

async def health(request):
    """Health check endpoint."""
    if request.path == "/health":
        return web.json_response({"status": "ok"})
    
    if not Config.HEALTH_API_KEY:
        return web.json_response({"status": "error", "message": "Not configured"}, status=500)
    
    if request.headers.get("X-API-Key") != Config.HEALTH_API_KEY:
        return web.json_response({"status": "unauthorized"}, status=401)
    
    return web.json_response({
        "status": "ok",
        "positions": await get_open_positions_count(),
        "daily_pnl": f"${fmt(await get_daily_pnl())}",
        "signals": State.analytics["signals"],
        "alerts": State.analytics["alerts"],
        "scanner": State.scanner_alive,
        "signer": State.signer_ready,
        "emergency_stop": State.emergency_stop,
        "sodex": sodex.ready,
        "uptime": str(datetime.now() - State.start_time)
    })

# ============================================================================
# MAIN
# ============================================================================

async def main():
    """Main entry point."""
    log.info("🚀 Starting Agentic Finance...")
    
    # Initialize database FIRST
    await init_db()
    
    # Load settings from database
    Config.RISK_PERCENT = float(await get_setting("risk_percent", str(Config.RISK_PERCENT)))
    Config.SCAN_INTERVAL = int(await get_setting("scan_interval", str(Config.SCAN_INTERVAL)))
    Config.MIN_CONFIDENCE = int(await get_setting("min_confidence", str(Config.MIN_CONFIDENCE)))
    Config.ENABLE_AUTO_ALERTS = (await get_setting("auto_trade", str(Config.ENABLE_AUTO_ALERTS))) == "true"
    Config.MAX_POSITIONS = int(await get_setting("max_positions", str(Config.MAX_POSITIONS)))
    State.emergency_stop = (await get_setting("emergency_stop", "false")) == "true"
    
    # Web server
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", Config.PORT).start()
    log.info(f"✅ Web server on port {Config.PORT}")
    
    # HTTP session
    session = ClientSession(timeout=TIMEOUT)
    
    # Load symbols
    try:
        await load_symbols(session)
        log.info(f"✅ Loaded {len(SYMBOL_IDS)} symbols")
    except Exception as e:
        log.exception("Symbol load failed")
        await session.close()
        raise
    
    # Check signer
    await check_signer(session)
    
    # Telegram app
    bot = ApplicationBuilder().token(Config.TOKEN).build()
    bot.bot_data["session"] = session
    bot.bot_data["global_data"] = {}
    
    bot.add_handler(CommandHandler("start", start_cmd))
    bot.add_handler(CallbackQueryHandler(button_handler))
    
    await bot.initialize()
    await bot.start()
    await bot.bot.delete_webhook(drop_pending_updates=True)
    
    # Background tasks
    refresh_task = asyncio.create_task(refresh_data(bot))
    scanner_task = asyncio.create_task(scanner_loop(bot))
    monitor_task = asyncio.create_task(manage_positions_loop(bot))
    signer_task = asyncio.create_task(signer_health_loop(bot))
    
    log.info("✅ Bot ready")
    
    try:
        await bot.updater.start_polling(drop_pending_updates=True)
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        for task in [refresh_task, scanner_task, monitor_task, signer_task]:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        await bot.updater.stop()
        await bot.stop()
        await bot.shutdown()
        await runner.cleanup()
        await session.close()
    
    log.info("✅ Shutdown complete")

async def manage_positions_loop(bot):
    """Background loop for position management."""
    session = bot.bot_data["session"]
    while True:
        try:
            await manage_positions(session, bot)
        except Exception as e:
            log.exception(f"Position management error: {e}")
        await asyncio.sleep(15)

async def signer_health_loop(bot):
    """Background loop for signer health checks."""
    session = bot.bot_data["session"]
    while True:
        try:
            await check_signer(session)
        except Exception as e:
            log.debug(f"Signer check error: {e}")
        await asyncio.sleep(60)

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot stopped")
    except Exception as e:
        log.error(f"Fatal: {e}")
        raise
