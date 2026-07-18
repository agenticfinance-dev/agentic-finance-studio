import os
import asyncio
import logging
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
from aiohttp import web
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler
from sodex import SoDEXExecutor, load_symbols, SYMBOL_IDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_TOKEN"))
if not TOKEN:
    raise Exception("TELEGRAM_BOT_TOKEN not found")

SOSO_API_KEY = os.getenv("SOSO_API_KEY")
SOSO_BASE = "https://openapi.sosovalue.com/openapi/v1"
SOSO_HEADERS = {"x-soso-api-key": SOSO_API_KEY or "", "Accept": "application/json"}

SODEX_API_KEY_NAME = os.getenv("SODEX_API_KEY_NAME", os.getenv("SODEX_API_KEY",""))
SODEX_API_PRIVATE_KEY = os.getenv("SODEX_API_PRIVATE_KEY", os.getenv("SODEX_PRIVATE_KEY",""))
SODEX_ACCOUNT_ID = os.getenv("SODEX_ACCOUNT_ID", "0")
ALERT_CHAT_ID = int(os.getenv("ALERT_CHAT_ID", "0"))

TIMEOUT = aiohttp.ClientTimeout(total=30)
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "300"))
ACCOUNT_SIZE = float(os.getenv("ACCOUNT_SIZE", "1000"))
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "1.5"))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "65"))
ENABLE_AUTO_ALERTS = os.getenv("ENABLE_AUTO_ALERTS", "true").lower() == "true"
MIN_NOTIONAL = 10.0

analytics = {"soso_calls": 0, "live_trades": 0, "signals": 0, "alerts": 0, "auto_scans": 0}
start_time = datetime.now()
last_alert_time = {}
price_cache = {}
CACHE_TTL = 60

COIN_NAMES = {"btc":"bitcoin","eth":"ethereum","bnb":"binancecoin","xrp":"ripple","sol":"solana"}
ALL_COINS = ["btc","eth","bnb","xrp","sol"]

# ============================================================================
# SODEX INITIALIZATION WITH FALLBACK
# ============================================================================

try:
    sodex = SoDEXExecutor(
        SODEX_API_KEY_NAME,
        SODEX_API_PRIVATE_KEY,
        SODEX_ACCOUNT_ID
    )
    logging.info(f"SoDEX ready: {sodex.ready} account={SODEX_ACCOUNT_ID}")
except Exception as e:
    logging.warning(f"SoDEX initialization failed: {e}")
    
    class DummySoDEX:
        ready = False
        
        async def place_order(self, *args, **kwargs):
            return {"err": "SoDEX unavailable"}
        
        async def get_positions(self, *args, **kwargs):
            return []
        
        async def verify_order(self, *args, **kwargs):
            return {"status": "N/A"}
    
    sodex = DummySoDEX()

# ============================================================================
# CACHE CLEANUP
# ============================================================================

def cleanup_cache():
    """Remove expired entries from price cache."""
    now = datetime.now()
    expired = [
        k for k, v in price_cache.items()
        if now - v["time"] > timedelta(minutes=5)
    ]
    for k in expired:
        del price_cache[k]

# ============================================================================
# SAFE HTTP GET WITH RETRY
# ============================================================================

async def safe_get(session, url, **kwargs):
    """Make HTTP request with retry logic."""
    for attempt in range(3):
        try:
            async with session.get(url, **kwargs) as r:
                if r.status == 200:
                    return await r.json()
        except Exception:
            pass
        await asyncio.sleep(1)
    return None

# ============================================================================
# MENUS
# ============================================================================

def main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 BTC", callback_data="signal_btc"), InlineKeyboardButton("📊 ETH", callback_data="signal_eth"), InlineKeyboardButton("📊 BNB", callback_data="signal_bnb")],
        [InlineKeyboardButton("💎 XRP", callback_data="signal_xrp"), InlineKeyboardButton("📊 SOL", callback_data="signal_sol"), InlineKeyboardButton("🗺 Sectors", callback_data="sectors")],
        [InlineKeyboardButton("🐳 Whales", callback_data="whales"), InlineKeyboardButton("🧠 AI Intel", callback_data="ai_intel")],
        [InlineKeyboardButton("🗺 Sector Map", callback_data="sector_map"), InlineKeyboardButton("🐋 Whale Radar", callback_data="whale_radar")],
        [InlineKeyboardButton("📈 ETF Flows", callback_data="etf_flows"), InlineKeyboardButton("🧠 Intelligence", callback_data="intelligence")],
        [InlineKeyboardButton("📊 Performance", callback_data="performance"), InlineKeyboardButton("📊 Stats", callback_data="stats")],
        [InlineKeyboardButton("📡 Scanner Status", callback_data="scanner_on"), InlineKeyboardButton("⚡ Trade Now", callback_data="exec_btc")],
        [InlineKeyboardButton("💼 Portfolio", callback_data="portfolio"), InlineKeyboardButton("🏭 INST. FLOW", callback_data="inst_flow")],
    ])

def back_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="back_main")]])

def get_session(app):
    return app.bot_data.get("session")

# ============================================================================
# EXECUTION LOG
# ============================================================================

def log_execution(trade_data):
    """Save trade execution to audit log."""
    try:
        with open("executions.json", "a") as f:
            f.write(json.dumps(trade_data) + "\n")
    except Exception:
        logging.exception("Execution log failed")

# ============================================================================
# SOSO VALUE HELPERS
# ============================================================================

async def fetch_soso_etf(session):
    if not SOSO_API_KEY:
        return None
    try:
        data = await soso_get(session, "etf/flows")
        if data:
            return data
    except Exception:
        pass
    return None

async def fetch_soso_whale(session):
    if not SOSO_API_KEY:
        return None
    try:
        data = await soso_get(session, "market/whale")
        if data:
            return data
    except Exception:
        pass
    return None

async def fetch_soso_news(session):
    if not SOSO_API_KEY:
        return None
    try:
        data = await soso_get(session, "market/news")
        if data:
            return data
    except Exception:
        pass
    return None

async def fetch_soso_sectors(session):
    if not SOSO_API_KEY:
        return None
    try:
        data = await soso_get(session, "market/sectors")
        if data:
            return data
    except Exception:
        pass
    return None

async def get_top_performers(session, limit=3):
    results = []
    for coin in ALL_COINS:
        price = await get_price(session, coin)
        if price and price.get("price"):
            results.append({
                "symbol": coin.upper(),
                "change": price.get("change", 0)
            })
    results.sort(key=lambda x: x["change"], reverse=True)
    return results[:limit]

async def get_volume_anomalies(session):
    anomalies = []
    for coin in ALL_COINS:
        ind = await get_indicators(session, coin)
        price = await get_price(session, coin)
        score = 0
        
        if ind.get("vol_spike"):
            score += 2
        if abs(price.get("change", 0)) > 3:
            score += 1
        if ind.get("rsi", 50) > 70 or ind.get("rsi", 50) < 30:
            score += 1
        
        if score >= 2:
            anomalies.append({
                "symbol": coin.upper(),
                "score": score,
                "change": price.get("change", 0)
            })
    
    return sorted(anomalies, key=lambda x: x["score"], reverse=True)

# ============================================================================
# API HELPERS
# ============================================================================

async def soso_get(session, endpoint, params=None):
    if not SOSO_API_KEY:
        return None
    url = f"{SOSO_BASE}/{endpoint}"
    try:
        async with session.get(url, headers=SOSO_HEADERS, params=params, timeout=TIMEOUT) as r:
            analytics["soso_calls"] += 1
            if r.status != 200:
                return None
            res = await r.json()
            if res.get("code") != 0:
                return None
            return res.get("data")
    except Exception:
        return None

# ============================================================================
# PRICE & INDICATORS
# ============================================================================

def get_cached_price(symbol):
    sym = symbol.lower()
    if sym in price_cache:
        entry = price_cache[sym]
        if datetime.now() - entry["time"] < timedelta(seconds=CACHE_TTL):
            if entry["data"] and entry["data"].get("price") is not None:
                return entry["data"]
    return None

def set_cached_price(symbol, data):
    if data and data.get("price") is not None:
        price_cache[symbol.lower()] = {"data": data, "time": datetime.now()}

async def get_price(session, symbol):
    sym = symbol.upper()
    cached = get_cached_price(sym)
    if cached:
        return cached
    
    try:
        if SOSO_API_KEY:
            d = await soso_get(session, "token/price", {"symbol": sym})
            if d:
                raw = d.get("price") or d.get("last_price")
                if raw is not None:
                    result = {"price": float(raw), "change": float(d.get("change_24h",0) or 0), "source": "SoSoValue"}
                    set_cached_price(sym, result)
                    return result
    except Exception:
        pass
    
    coin = COIN_NAMES.get(sym.lower())
    if coin:
        try:
            data = await safe_get(session, f"https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd&include_24hr_change=true", timeout=TIMEOUT)
            if data:
                d = data.get(coin, {})
                if d.get("usd") is not None:
                    result = {"price": float(d["usd"]), "change": float(d.get("usd_24h_change",0) or 0), "source": "CoinGecko"}
                    set_cached_price(sym, result)
                    return result
        except Exception:
            pass
    
    for url in [f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}USDT", f"https://data-api.binance.vision/api/v3/ticker/24hr?symbol={sym}USDT"]:
        try:
            data = await safe_get(session, url, timeout=TIMEOUT)
            if data and data.get("lastPrice") is not None:
                result = {"price": float(data["lastPrice"]), "change": float(data.get("priceChangePercent",0) or 0), "source": "Binance"}
                set_cached_price(sym, result)
                return result
        except Exception:
            continue
    
    return {"price": None, "change": 0, "source": "None"}

# ============================================================================
# INDICATORS
# ============================================================================

def wilder_rsi(closes, period=14):
    if not closes or len(closes) < period + 1:
        return 55
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

def wilder_atr(highs, lows, closes, period=14):
    if not closes or len(closes) < period + 1:
        return closes[-1]*0.015 if closes else 0
    trs = []
    for i in range(1, len(highs)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr

async def get_indicators(session, symbol):
    sym = symbol.upper()
    for url in [f"https://api.binance.com/api/v3/klines?symbol={sym}USDT&interval=1h&limit=100"]:
        try:
            data = await safe_get(session, url, timeout=TIMEOUT)
            if data and isinstance(data, list) and len(data) >= 60:
                closes = [float(x[4]) for x in data]
                highs = [float(x[2]) for x in data]
                lows = [float(x[3]) for x in data]
                ema20 = sum(closes[-20:])/20
                ema50 = sum(closes[-50:])/50
                rsi = wilder_rsi(closes)
                atr = wilder_atr(highs, lows, closes)
                vol = sum([float(x[5]) for x in data[-5:]])
                avg_vol = sum([float(x[5]) for x in data[-20:]])/20
                return {"rsi": round(rsi,1), "ema20": ema20, "ema50": ema50, "atr": atr, "vol_spike": vol > avg_vol*1.5}
        except Exception:
            continue
    return {"rsi": 55, "ema20": 0, "ema50": 0, "atr": 0, "vol_spike": False}

# ============================================================================
# SCORING ENGINE
# ============================================================================

def calculate_confidence(price_data, ind):
    score = 55  # Start higher for better signal quality
    
    if ind["ema20"] and ind["ema50"]:
        score += 15 if ind["ema20"] > ind["ema50"] else -10
    if ind["rsi"] < 30:
        score += 20
    elif ind["rsi"] < 40:
        score += 10
    elif ind["rsi"] > 70:
        score -= 15
    if ind["vol_spike"]:
        score += 8
    change = price_data.get("change",0)
    if abs(change) > 3:
        score += 7 if change > 0 else -7
    
    # Boost score if using high-quality data source
    if price_data.get("source") == "SoSoValue":
        score += 5
    
    return max(10, min(95, int(score)))

def calc_position_size(entry, sl):
    if entry is None or sl is None or entry == sl:
        return 0.001
    risk_amount = ACCOUNT_SIZE * (RISK_PERCENT / 100)
    stop_distance = abs(entry - sl)
    if stop_distance == 0:
        return 0.001
    qty = risk_amount / stop_distance
    max_position = (ACCOUNT_SIZE * 0.5) / entry
    qty = min(qty, max_position)
    qty = max(qty, 0.001)  # Prevent tiny orders
    qty = round(qty, 6)
    return qty

def format_price_precision(entry):
    if entry >= 1000:
        return round(entry, 2)
    elif entry >= 100:
        return round(entry, 3)
    elif entry >= 1:
        return round(entry, 4)
    else:
        return round(entry, 6)

# ============================================================================
# INTELLIGENCE ENGINE
# ============================================================================

async def intelligence_engine(session, symbol):
    price_data = await get_price(session, symbol)
    ind = await get_indicators(session, symbol)
    if not price_data["price"]:
        return None
    
    confidence = calculate_confidence(price_data, ind)
    reasons=[]; checks=[]
    
    if ind["vol_spike"]:
        checks.append("✅ Volume Spike")
    else:
        checks.append("❌ No volume confirmation")
    
    if ind["rsi"] < 35:
        reasons.append(f"• RSI oversold {ind['rsi']} indicates potential bottom")
        checks.append(f"✅ RSI oversold {ind['rsi']}")
    elif ind["rsi"] > 70:
        reasons.append(f"• RSI overbought {ind['rsi']} suggests potential top")
        checks.append(f"❌ RSI overbought {ind['rsi']}")
    else:
        reasons.append(f"• RSI neutral {ind['rsi']} - waiting for momentum")
        checks.append(f"⚠️ RSI {ind['rsi']}")
    
    if ind["ema20"] and ind["ema50"]:
        if ind["ema20"] > ind["ema50"]:
            reasons.append("• Price above EMA50 - uptrend confirmed")
            checks.append("✅ Price above EMA50")
        else:
            reasons.append("• Price below EMA50 - downtrend risk")
            checks.append("❌ Price below EMA50")
    
    price = price_data["price"]
    atr = max(ind["atr"], price * 0.01)  # Prevent zero ATR
    
    if confidence >= MIN_CONFIDENCE:
        bias="LONG"; entry=price*1.002; sl=entry-atr*1.2; tp=entry+atr*2.8
    elif confidence <= (100-MIN_CONFIDENCE):
        bias="SHORT"; entry=price*0.998; sl=entry+atr*1.2; tp=entry-atr*2.8
    else:
        bias="NEUTRAL"; entry=price; sl=entry-atr*0.8; tp=entry+atr*1.2
    
    entry = format_price_precision(entry)
    sl = format_price_precision(sl)
    tp = format_price_precision(tp)
    rr = round(abs(tp-entry)/abs(entry-sl),2) if entry!=sl else 2.0
    qty = calc_position_size(entry, sl)
    qty = round(qty, 6)
    
    return {
        "symbol": symbol.upper(),
        "price": price,
        "change": price_data["change"],
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "confidence": confidence,
        "bias": bias,
        "reasons": reasons,
        "checks": checks,
        "rsi": ind["rsi"],
        "atr": atr,
        "qty": qty,
        "source": price_data["source"]
    }

def fmt(p):
    return f"{p:.6f}" if p and p<1 else f"{p:.2f}" if p and p<100 else f"{p:,.0f}" if p else "N/A"

# ============================================================================
# AUTONOMOUS SCANNER (PARALLEL WITH ERROR PROTECTION)
# ============================================================================

async def autonomous_scanner(app):
    logging.info(f"🤖 Scanner started interval {SCAN_INTERVAL}s threshold {MIN_CONFIDENCE}%")
    await asyncio.sleep(10)
    session = app.bot_data.get("session")
    
    while True:
        try:
            if not ENABLE_AUTO_ALERTS:
                await asyncio.sleep(SCAN_INTERVAL)
                continue
            
            # Cleanup cache before scanning
            cleanup_cache()
            
            analytics["auto_scans"] += 1
            
            # FIX: Protect scanner from crashing
            try:
                signals = await asyncio.gather(
                    *(intelligence_engine(session, coin) for coin in ALL_COINS),
                    return_exceptions=True,
                )
            except Exception:
                signals = []
            
            for sig in signals:
                if isinstance(sig, Exception) or not sig:
                    continue
                
                coin = sig["symbol"].lower()
                
                if sig["confidence"] >= MIN_CONFIDENCE and sig["bias"] != "NEUTRAL":
                    now = datetime.now()
                    last = last_alert_time.get(coin)
                    
                    should_alert = False
                    if last is None:
                        should_alert = True
                    elif last["bias"] != sig["bias"]:
                        should_alert = True
                    elif now - last["time"] > timedelta(hours=1):
                        should_alert = True
                    
                    if not should_alert:
                        continue
                    
                    last_alert_time[coin] = {
                        "time": now,
                        "bias": sig["bias"],
                        "confidence": sig["confidence"],
                    }
                    
                    analytics["alerts"] += 1
                    
                    if ALERT_CHAT_ID:
                        txt = (
                            f"🤖 ALERT - {sig['symbol']} {sig['bias']} {sig['confidence']}%\n"
                            f"💰 ${fmt(sig['price'])} ({sig['change']:+.1f}%)\n"
                            f"Entry: ${fmt(sig['entry'])}\n"
                            f"TP: ${fmt(sig['tp'])}\n"
                            f"SL: ${fmt(sig['sl'])}\n"
                            f"Qty: {sig['qty']:.6f}"
                        )
                        
                        kb = InlineKeyboardMarkup([
                            [InlineKeyboardButton(
                                f"⚡ EXECUTE {sig['symbol']}",
                                callback_data=f"exec_{coin}"
                            )]
                        ])
                        
                        try:
                            await app.bot.send_message(
                                ALERT_CHAT_ID,
                                txt,
                                reply_markup=kb
                            )
                        except Exception:
                            pass
        
        except Exception as e:
            logging.exception(f"Scanner crashed: {e}")
        
        await asyncio.sleep(SCAN_INTERVAL)

# ============================================================================
# TELEGRAM HANDLERS
# ============================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"🚀 Agentic Finance Live\nSignals: {analytics['signals']} | Scans: {analytics['auto_scans']}\nAlerts: {analytics['alerts']} | Trades: {analytics['live_trades']}\nSoDEX:{sodex.ready} Sym:{len(SYMBOL_IDS)}",
        reply_markup=main_menu_kb()
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    session = get_session(context.application)
    data = q.data
    
    if data == "back_main":
        await q.edit_message_text("🚀 Agentic Finance Live", reply_markup=main_menu_kb())
        return
    
    # ===== SIGNAL =====
    if data.startswith("signal_"):
        sym = data.split("_")[1]
        sig = await intelligence_engine(session, sym)
        if not sig:
            await q.edit_message_text("Price fetch failed", reply_markup=back_kb())
            return
        
        analytics["signals"] += 1
        txt = f"📊 {sig['symbol']} {sig['bias']} | {sig['confidence']}%\n💰 ${fmt(sig['price'])} ({sig['change']:+.1f}%) | {sig['source']}\n\n" + "\n".join(sig['checks']) + "\n\n" + "\n".join(sig['reasons']) + f"\n\nEntry: ${fmt(sig['entry'])} | TP: ${fmt(sig['tp'])} | SL: ${fmt(sig['sl'])}\nRR: {sig['rr']} | RSI: {sig['rsi']}\nQty: {sig['qty']:.6f} = ${sig['qty']*sig['entry']:.2f}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⚡ EXECUTE SODEX", callback_data=f"exec_{sym}")],[InlineKeyboardButton("⬅️ Back", callback_data="back_main")]])
        await q.edit_message_text(txt, reply_markup=kb)
    
    # ===== EXECUTE =====
    elif data.startswith("exec_"):
        sym = data.split("_")[1]
        await q.edit_message_text(f"⏳ Analyzing {sym.upper()}...", reply_markup=back_kb())
        
        sig = await intelligence_engine(session, sym)
        if not sig:
            await q.edit_message_text("❌ Failed to get signal", reply_markup=back_kb())
            return
        
        if sig["bias"] == "NEUTRAL":
            await q.edit_message_text(f"⚠️ No trade executed. Signal is NEUTRAL.\n\n{sig['symbol']} | {sig['confidence']}%\n" + "\n".join(sig['checks']), reply_markup=back_kb())
            return
        
        if sig["confidence"] < MIN_CONFIDENCE:
            await q.edit_message_text(f"⚠️ Confidence {sig['confidence']}% < {MIN_CONFIDENCE}% threshold", reply_markup=back_kb())
            return
        
        notional = sig["qty"] * sig["entry"]
        if notional < MIN_NOTIONAL:
            await q.edit_message_text(f"⚠️ Notional too small: ${notional:.2f} < ${MIN_NOTIONAL} min", reply_markup=back_kb())
            return
        
        # Check SoDEX readiness before execution
        if not sodex.ready:
            await q.edit_message_text(
                "❌ SoDEX is not connected or symbols failed to load.",
                reply_markup=back_kb()
            )
            return
        
        await q.edit_message_text(f"⏳ Executing {sig['symbol']} {sig['bias']} on SoDEX...\nEntry: ${fmt(sig['entry'])} (mkt ${fmt(sig['price'])})\nQty: {sig['qty']:.6f} = ${notional:.2f}", reply_markup=back_kb())
        
        try:
            res = await sodex.place_order(
                session=session,
                symbol=sym.upper(),
                side=sig["bias"],
                qty=float(sig["qty"]),
                price=float(sig["entry"])
            )
            
            if "err" in res:
                await q.edit_message_text(f"❌ SoDEX rejected\n\n{res['err'][:1000]}", reply_markup=back_kb())
                return
            
            analytics["live_trades"] += 1
            
            order_id = (
                res.get("orderId")
                or res.get("id")
                or res.get("txHash")
                or res.get("hash")
                or "N/A"
            )
            
            status = res.get("status") or "Submitted"
            exchange = res.get("exchange") or "SoDEX"
            
            # Build professional confirmation
            text = f"""
✅ ORDER SUBMITTED

Exchange:
{exchange}

Order ID:
{order_id}

Status:
{status}

Symbol:
{sig['symbol']}

Side:
{sig['bias']}

Entry:
${fmt(sig['entry'])}

Quantity:
{sig['qty']:.6f}

Confidence:
{sig['confidence']}%
"""
            
            # Protect verify_order call
            if hasattr(sodex, "verify_order"):
                try:
                    verify = await sodex.verify_order(
                        session=session,
                        order_id=order_id
                    )
                    if verify:
                        text += f"\n\nVerification:\n{verify.get('status', 'Pending')}"
                except Exception:
                    pass
            
            await q.edit_message_text(text, reply_markup=back_kb())
            
            # Log execution
            log_execution({
                "time": datetime.utcnow().isoformat(),
                "symbol": sig["symbol"],
                "side": sig["bias"],
                "entry": sig["entry"],
                "qty": sig["qty"],
                "confidence": sig["confidence"],
                "order": order_id,
                "exchange": exchange,
                "status": status
            })
            
        except Exception as e:
            await q.edit_message_text(f"❌ SODEX Error: {str(e)[:500]}", reply_markup=back_kb())
    
    # ===== SECTORS =====
    elif data == "sectors":
        eth = await get_price(session, "eth")
        sol = await get_price(session, "sol")
        bnb = await get_price(session, "bnb")
        ai = (eth["change"] + sol["change"]) / 2 if eth["price"] and sol["price"] else 0
        defi = (eth["change"] + bnb["change"]) / 2 if eth["price"] and bnb["price"] else 0
        ai_state = "Bullish" if ai > 0 else "Bearish"
        defi_state = "Bullish" if defi > 0 else "Bearish"
        text = "🗺 MARKET SECTORS\n\n" f"AI : {ai_state} ({ai:+.2f}%)\n" f"DeFi : {defi_state} ({defi:+.2f}%)"
        await q.edit_message_text(text, reply_markup=back_kb())
    
    # ===== WHALES =====
    elif data == "whales":
        anomalies = await get_volume_anomalies(session)
        if anomalies:
            rows = []
            for a in anomalies[:5]:
                emoji = "🐳" if a["score"] >= 3 else "🐋"
                rows.append(f"{emoji} {a['symbol']} {a['change']:+.2f}%")
            text = "🐳 WHALE ACTIVITY\n\n" + "\n".join(rows)
        else:
            text = "🐳 WHALE ACTIVITY\n\nNo unusual activity detected."
        await q.edit_message_text(text, reply_markup=back_kb())
    
    # ===== SECTOR MAP =====
    elif data == "sector_map":
        sectors = await fetch_soso_sectors(session)
        if sectors:
            rows = []
            for s in sectors[:5]:
                if isinstance(s, dict):
                    rows.append(f"{s.get('name', 'Unknown')}: {s.get('change', 0):+.2f}%")
            text = "🗺 SECTOR MAP\n\n" + "\n".join(rows) + "\n\nSource: SoSoValue"
        else:
            top = await get_top_performers(session)
            text = "🗺 SECTOR MAP\n\n" + "\n".join([f"{p['symbol']}: {p['change']:+.2f}%" for p in top]) + "\n\nSource: CoinGecko"
        await q.edit_message_text(text, reply_markup=back_kb())
    
    # ===== WHALE RADAR =====
    elif data == "whale_radar":
        whales = await fetch_soso_whale(session)
        if whales:
            rows = []
            for w in whales[:8]:
                if isinstance(w, dict):
                    rows.append(f"🐳 {w.get('symbol', 'Unknown')} ${w.get('amount', 0):,.0f}")
            text = "🐋 LIVE WHALE ACTIVITY\n\n" + "\n".join(rows) + "\n\nSource: SoSoValue"
        else:
            anomalies = await get_volume_anomalies(session)
            if anomalies:
                text = "🐋 Whale Radar (Fallback)\n\n"
                for a in anomalies[:5]:
                    text += f"🐳 {a['symbol']} {a['change']:+.2f}%\n"
            else:
                text = "🐋 Whale Radar\n\nNo abnormal activity detected."
        await q.edit_message_text(text, reply_markup=back_kb())
    
    # ===== ETF FLOWS =====
    elif data == "etf_flows":
        etf = await fetch_soso_etf(session)
        if etf:
            if isinstance(etf, list):
                rows = []
                for x in etf[:5]:
                    if isinstance(x, dict):
                        rows.append(f"{x.get('name', 'ETF')}: ${x.get('netFlow', 'N/A')}")
                text = "📈 ETF FLOWS\n\n" + "\n".join(rows) + "\n\nSource: SoSoValue"
            else:
                text = "📈 ETF FLOWS\n\n" + json.dumps(etf, indent=2)[:1500]
        else:
            top = await get_top_performers(session)
            text = "📈 ETF API unavailable\nShowing institutional momentum fallback.\n\n" + "\n".join([f"{p['symbol']} {p['change']:+.2f}%" for p in top]) + "\n\nFallback: CoinGecko + Binance"
        await q.edit_message_text(text, reply_markup=back_kb())
    
    # ===== INTELLIGENCE =====
    elif data == "intelligence":
        btc = await intelligence_engine(session, "btc")
        eth = await intelligence_engine(session, "eth")
        if not btc or not eth:
            await q.edit_message_text("🧠 Intelligence\n\nUnable to fetch market intelligence.", reply_markup=back_kb())
            return
        
        avg = (btc["confidence"] + eth["confidence"]) / 2
        if avg >= 70:
            sentiment = "🟢 Bullish"
        elif avg <= 40:
            sentiment = "🔴 Bearish"
        else:
            sentiment = "🟡 Neutral"
        
        text = f"🧠 MARKET INTELLIGENCE\n\nOverall Sentiment: {sentiment}\n\nBTC Confidence: {btc['confidence']}%\nETH Confidence: {eth['confidence']}%\n\nSource:\n• SoSoValue\n• Binance\n• CoinGecko"
        
        # Try to add news
        news = await fetch_soso_news(session)
        if news:
            headlines = []
            for n in news[:3]:
                if isinstance(n, dict):
                    headlines.append(f"• {n.get('title', '')}")
            if headlines:
                text += "\n\nLatest Intelligence\n" + "\n".join(headlines)
        
        await q.edit_message_text(text, reply_markup=back_kb())
    
    # ===== AI INTEL =====
    elif data == "ai_intel":
        text = "🧠 AI INTELLIGENCE\n\n" f"Scanner Status : {'🟢 ACTIVE' if ENABLE_AUTO_ALERTS else '🔴 OFF'}\n" f"Confidence Threshold : {MIN_CONFIDENCE}%\n" f"Scan Interval : {SCAN_INTERVAL}s\n\n" f"Assets Monitored:\n" + "\n".join([f"• {c.upper()}" for c in ALL_COINS])
        await q.edit_message_text(text, reply_markup=back_kb())
    
    # ===== PERFORMANCE =====
    elif data == "performance":
        await q.edit_message_text(f"📊 Performance\n\nSignals: {analytics['signals']}\nAlerts: {analytics['alerts']}\nLive Trades: {analytics['live_trades']}\nAutoScans: {analytics['auto_scans']}\nUptime: {datetime.now()-start_time}", reply_markup=back_kb())
    
    # ===== STATS =====
    elif data == "stats":
        await q.edit_message_text(f"📊 Live Stats\n\nSignals: {analytics['signals']}\nAlerts: {analytics['alerts']}\nUptime: {datetime.now()-start_time}", reply_markup=back_kb())
    
    # ===== SCANNER =====
    elif data == "scanner_on":
        await q.edit_message_text(f"📡 Scanner active every {SCAN_INTERVAL}s for {MIN_CONFIDENCE}%+ signals\nCoins: {', '.join([c.upper() for c in ALL_COINS])}\nAutoAlerts: {ENABLE_AUTO_ALERTS}\nSoDEX:{sodex.ready}", reply_markup=back_kb())
    
    # ===== PORTFOLIO =====
    elif data == "portfolio":
        try:
            positions = await sodex.get_positions(session=session)
        except Exception:
            positions = []
        
        if positions:
            text = "💼 PORTFOLIO\n\n"
            for pos in positions[:10]:
                if isinstance(pos, dict):
                    text += f"{pos.get('symbol', 'Unknown')} {pos.get('side', '')}\nEntry: ${fmt(pos.get('entry', 0))}\nQty: {pos.get('qty', 0):.4f}\n\n"
            await q.edit_message_text(text, reply_markup=back_kb())
            return
        
        text = "💼 PORTFOLIO\n\nExchange : SoDEX\n" f"Status : {'Connected' if sodex.ready else 'Disconnected'}\n\nOpen positions are not available from the current SoDEX API."
        await q.edit_message_text(text, reply_markup=back_kb())
    
    # ===== INSTITUTIONAL FLOW =====
    elif data == "inst_flow":
        texts = []
        for s in ALL_COINS:
            p = await get_price(session, s)
            if p["price"]:
                ind = await get_indicators(session, s)
                confidence = calculate_confidence(p, ind)
                trend = "Institution Buying" if confidence > 70 else "Institution Selling" if confidence < 40 else "Neutral"
                texts.append(f"{s.upper()}: ${fmt(p['price'])} ({p['change']:+.2f}%) • {trend}")
        await q.edit_message_text("🏭 INSTITUTIONAL FLOW REPORT\n\n" + "\n".join(texts), reply_markup=back_kb())
    
    else:
        await q.edit_message_text(f"✅ {data} - Module restored", reply_markup=back_kb())

# ============================================================================
# HEALTH
# ============================================================================

async def health(request):
    return web.json_response({
        "status": "online",
        "sodex": sodex.ready,
        "symbols": len(SYMBOL_IDS),
        "scans": analytics["auto_scans"],
        "signals": analytics["signals"],
        "alerts": analytics["alerts"],
        "trades": analytics["live_trades"],
        "uptime": str(datetime.now() - start_time)
    })

async def start_webserver():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 10000))).start()

# ============================================================================
# MAIN
# ============================================================================

async def main():
    logging.info("===== BOT STARTING =====")
    await start_webserver()
    
    shared_session = aiohttp.ClientSession(timeout=TIMEOUT)
    
    try:
        logging.info("===== LOADING SODEX SYMBOLS =====")
        try:
            await load_symbols(shared_session)
            
            if not SYMBOL_IDS:
                logging.warning("No SoDEX symbols loaded. Trading disabled.")
                sodex.ready = False
            else:
                logging.info(f"SoDEX symbols loaded: {len(SYMBOL_IDS)}")
        
        except Exception as e:
            logging.exception("Failed loading SoDEX symbols")
            sodex.ready = False
        
        try:
            async with shared_session.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=TIMEOUT) as r:
                txt = await r.text()
                logging.info(f"Delete webhook: {txt[:200]}")
        except Exception as e:
            logging.warning(f"deleteWebhook failed: {e}")
        await asyncio.sleep(3)
    except Exception as e:
        logging.exception(f"Startup failed: {e}")
    
    app = ApplicationBuilder().token(TOKEN).build()
    app.bot_data["session"] = shared_session
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    await app.initialize()
    
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    
    await app.start()
    
    scanner_task = asyncio.create_task(autonomous_scanner(app))
    app.bot_data["scanner_task"] = scanner_task
    
    await app.updater.start_polling(drop_pending_updates=True)
    
    logging.info("Bot started successfully.")
    
    try:
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        if "Conflict" in str(e):
            logging.warning("Conflict detected - another instance running")
            await asyncio.sleep(10)
        else:
            logging.exception("Polling crashed")
    finally:
        task = app.bot_data.get("scanner_task")
        if task:
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
        await shared_session.close()

if __name__ == "__main__":
    asyncio.run(main())
