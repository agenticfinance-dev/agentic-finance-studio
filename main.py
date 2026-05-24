import os
import logging
import asyncio
from datetime import datetime, timedelta
from collections import deque

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler

# ================= CONFIG =================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SOSO_API_KEY = os.getenv("SOSO_API_KEY")

ADMIN_CHAT_ID = os.getenv("CHAT_ID")
if ADMIN_CHAT_ID:
    try:
        ADMIN_CHAT_ID = int(ADMIN_CHAT_ID)
    except ValueError:
        ADMIN_CHAT_ID = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

HEADERS = {"User-Agent": "AgenticFinanceStudio"}
TIMEOUT = aiohttp.ClientTimeout(total=12)

COINS = ["btc", "eth", "xrp", "sol"]
COIN_NAMES = {"btc": "bitcoin", "eth": "ethereum", "xrp": "ripple", "sol": "solana"}

SECTORS = {
    "AI": ["eth", "sol"],
    "PAYFI": ["xrp"],
    "RWA": ["btc"],
    "DEFI": ["eth", "sol"]
}

performance_log = {coin: deque(maxlen=100) for coin in COINS}
user_last_interaction = {}
price_cache = {}
rsi_cache = {}
last_alerts = {}

alert_lock = asyncio.Lock()
api_semaphore = asyncio.Semaphore(8)

analytics = {"signals_generated": 0, "alerts_sent": 0}
start_time = datetime.now()

def format_price(price: float) -> str:
    if price < 1:
        return f"{price:.6f}"
    elif price < 100:
        return f"{price:.2f}"
    return f"{price:,.0f}"

def build_main_menu():
    keyboard = [
        [InlineKeyboardButton("📊 BTC", callback_data="signal_btc"), InlineKeyboardButton("📊 ETH", callback_data="signal_eth")],
        [InlineKeyboardButton("📊 XRP", callback_data="signal_xrp"), InlineKeyboardButton("📊 SOL", callback_data="signal_sol")],
        [InlineKeyboardButton("📈 Sector Map", callback_data="sectors"), InlineKeyboardButton("🐳 Whale Radar", callback_data="whale")],
        [InlineKeyboardButton("📊 ETF Flows", callback_data="etf"), InlineKeyboardButton("📰 Intelligence", callback_data="news")],
        [InlineKeyboardButton("📈 Performance", callback_data="performance"), InlineKeyboardButton("📊 Stats", callback_data="stats")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ================= GLOBAL SESSION + SHUTDOWN =================
async def get_session(app):
    if "session" not in app.bot_data:
        connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
        app.bot_data["session"] = aiohttp.ClientSession(timeout=TIMEOUT, connector=connector)
    return app.bot_data["session"]

async def post_shutdown(app):
    for task_name in ["alert_task", "cleanup_task"]:
        task = app.bot_data.get(task_name)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    session = app.bot_data.get("session")
    if session and not session.closed:
        await session.close()
        logging.info("✅ Session closed cleanly.")

# ================= HELPERS =================
async def fetch_with_retry(session, url, headers=None, retries=3):
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers or HEADERS, timeout=TIMEOUT) as r:
                if r.status == 200:
                    return await r.json()
                logging.warning(f"HTTP {r.status} for {url}")
        except Exception as e:
            logging.error(f"Attempt {attempt+1} failed: {e}")
            await asyncio.sleep(1.2)
    return None

async def fetch_price(session, symbol):
    async with api_semaphore:
        display = symbol.upper()
        price = None
        change = 0.0
        source = "N/A"

        if SOSO_API_KEY:
            data = await fetch_with_retry(session, f"https://openapi.sosovalue.com/openapi/v1/asset/market/current-price?symbol={display}", {"x-soso-api-key": SOSO_API_KEY, **HEADERS})
            if data and isinstance(data.get("data"), list) and len(data["data"]) > 0:
                item = data["data"][0]
                price = float(item.get("price"))
                change = float(item.get("change24h", 0))
                source = "🥇 SoSoValue"

        if price is None:
            cg_id = COIN_NAMES[symbol]
            data = await fetch_with_retry(session, f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd&include_24hr_change=true")
            if data and cg_id in data:
                price = data[cg_id]["usd"]
                change = float(data[cg_id].get("usd_24h_change", 0))
                if source == "N/A": source = "🥈 CoinGecko"

        if price is None:
            data = await fetch_with_retry(session, f"https://api.binance.com/api/v3/ticker/24hr?symbol={display}USDT")
            if data:
                price = float(data["lastPrice"])
                change = float(data["priceChangePercent"])
                if source == "N/A": source = "🥉 Binance"

        confidence = 95 if "SoSoValue" in source else 75 if "CoinGecko" in source else 60
        return {"price": price, "change": change, "source": source, "confidence": confidence}

async def get_cached_price(session, symbol):
    now = datetime.now()
    if symbol in price_cache and now - price_cache[symbol]["time"] < timedelta(seconds=25):
        return price_cache[symbol]["data"]
    data = await fetch_price(session, symbol)
    price_cache[symbol] = {"data": data, "time": now}
    return data

async def get_atr(session, symbol, period=14):
    try:
        async with api_semaphore:
            async with session.get(f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}USDT&interval=1h&limit={period+2}", timeout=TIMEOUT) as resp:
                data = await resp.json()
                if len(data) < period + 1: return None
                trs = []
                for i in range(1, len(data)):
                    high = float(data[i][2])
                    low = float(data[i][3])
                    prev = float(data[i-1][4])
                    tr = max(high - low, abs(high - prev), abs(low - prev))
                    trs.append(tr)
                return sum(trs) / len(trs)
    except Exception as e:
        logging.error(f"ATR error {symbol}: {e}")
        return None

async def get_rsi(session, symbol):
    key = f"rsi_{symbol}"
    now = datetime.now()
    if key in rsi_cache and now - rsi_cache[key]["time"] < timedelta(seconds=180):
        return rsi_cache[key]["value"]
    try:
        async with api_semaphore:
            async with session.get(f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}USDT&interval=1h&limit=20", timeout=TIMEOUT) as resp:
                data = await resp.json()
                if len(data) < 15: return None
                closes = [float(c[4]) for c in data]
                gains = [max(closes[i] - closes[i-1], 0) for i in range(1, len(closes))]
                losses = [max(closes[i-1] - closes[i], 0) for i in range(1, len(closes))]
                avg_gain = sum(gains[-14:]) / 14
                avg_loss = sum(losses[-14:]) / 14 if sum(losses[-14:]) > 0 else 0.0001
                rsi = 100 - (100 / (1 + avg_gain / avg_loss))
                rsi_value = round(rsi, 1)
                rsi_cache[key] = {"value": rsi_value, "time": now}
                return rsi_value
    except Exception as e:
        logging.error(f"RSI error {symbol}: {e}")
        return None

# ================= SIGNAL WITH PULLBACK =================
async def generate_signal(session, symbol):
    data = await get_cached_price(session, symbol)
    if data["price"] is None:
        return None

    rsi = await get_rsi(session, symbol)
    atr = await get_atr(session, symbol)

    price = data["price"]
    change = data["change"]

    direction = "LONG" if change > 1.5 else "SHORT" if change < -1.5 else "NEUTRAL"

    if not atr:
        atr = price * 0.015

    if direction == "LONG":
        sentiment, emoji = "Bullish", "🚀"
        entry = price * 0.992
        sl = entry - (atr * 1.2)
        tp = entry + (atr * 2.5)
    elif direction == "SHORT":
        sentiment, emoji = "Bearish", "📉"
        entry = price * 1.008
        sl = entry + (atr * 1.2)
        tp = entry - (atr * 2.5)
    else:
        sentiment, emoji = "Neutral", "🟡"
        entry = price * 0.995
        sl = entry - (atr * 0.8)
        tp = entry + (atr * 1.5)

    confidence = 58
    if abs(change) > 5: confidence += 20
    elif abs(change) > 3: confidence += 14
    if rsi and 35 < rsi < 70: confidence += 18
    if "SoSoValue" in data["source"]: confidence += 22
    confidence = min(98, confidence)

    reasoning = "Strong Momentum" if abs(change) > 4 else "Multi-source Convergence"

    signal = {
        **data,
        "symbol": symbol.upper(),
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "sentiment": sentiment,
        "emoji": emoji,
        "bias": f"{direction} Signal",
        "rsi": rsi,
        "confidence": confidence,
        "reasoning": reasoning,
        "timeframe": "1H"
    }

    analytics["signals_generated"] += 1
    performance_log[symbol].append(signal)
    return signal

# ================= DYNAMIC FEATURES =================
async def get_sector_map(session):
    msg = "📈 **Sector Intelligence Map**\n\n"
    for sector, coins in SECTORS.items():
        changes = []
        for c in coins:
            data = await get_cached_price(session, c)
            if data and data.get("price") is not None:
                changes.append(data["change"])
        avg = sum(changes) / len(changes) if changes else 0
        emoji = "🟢" if avg > 1.5 else "🔴" if avg < -1.5 else "🟡"
        msg += f"{emoji} **{sector}**: {avg:+.2f}%\n"
    return msg

async def get_whale_radar(session):
    msg = "🐳 **Whale Radar (Extreme Moves)**\n\n"
    found = False
    for symbol in COINS:
        data = await get_cached_price(session, symbol)
        if data and data.get("price") is not None and abs(data["change"]) >= 3.0:
            msg += f"• {symbol.upper()}: **{data['change']:+.2f}%** (High Momentum)\n"
            found = True
    if not found:
        msg += "No extreme whale activity detected."
    return msg

async def fetch_etf_flows(session):
    btc = await get_cached_price(session, "btc")
    eth = await get_cached_price(session, "eth")
    btc_change = btc["change"] if btc and btc.get("price") else 0
    eth_change = eth["change"] if eth and eth.get("price") else 0
    bias = "Bullish on BTC" if btc_change > eth_change else "Bullish on ETH"
    return f"📊 **ETF Intelligence**\n\nBTC 24h: {btc_change:+.2f}%\nETH 24h: {eth_change:+.2f}%\n\nInstitutional Bias: **{bias}**"

async def get_intelligence_feed(session):
    total = 0
    count = 0
    strongest = "N/A"
    max_change = -100
    for symbol in COINS:
        data = await get_cached_price(session, symbol)
        if data and data.get("price") is not None:
            total += data["change"]
            count += 1
            if data["change"] > max_change:
                max_change = data["change"]
                strongest = symbol.upper()
    avg = total / count if count > 0 else 0

    # Real SSI First
    try:
        data = await fetch_with_retry(session, "https://api.alternative.me/fng/")
        if data and data.get("data"):
            ssi = int(data["data"][0]["value"])
            mood = data["data"][0]["value_classification"]
        else:
            ssi = max(0, min(100, round(50 + (avg * 7))))
            mood = "Extreme Greed" if ssi >= 80 else "Greed" if ssi >= 65 else "Neutral" if ssi >= 45 else "Fear" if ssi >= 25 else "Extreme Fear"
    except:
        ssi = max(0, min(100, round(50 + (avg * 7))))
        mood = "Extreme Greed" if ssi >= 80 else "Greed" if ssi >= 65 else "Neutral" if ssi >= 45 else "Fear" if ssi >= 25 else "Extreme Fear"

    return f"🧠 **Market Intelligence Feed**\n\n" \
           f"**SSI Score**: {ssi}/100\n" \
           f"Mood: **{mood}**\n" \
           f"Avg Change: {avg:+.2f}%\n" \
           f"Strongest Asset: **{strongest}**"

async def get_performance():
    msg = "📈 **Performance Tracker**\n\n"
    for coin in COINS:
        logs = performance_log[coin]
        msg += f"{coin.upper()}: {len(logs)} signals\n"
    return msg

async def get_stats():
    uptime = str(datetime.now() - start_time).split('.')[0]
    return f"📊 **Live Stats**\n\nSignals Generated: {analytics['signals_generated']}\nUptime: {uptime}"

# ================= BUTTON HANDLER =================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    now = datetime.now()

    if user_id in user_last_interaction and (now - user_last_interaction[user_id]) < timedelta(seconds=2):
        await query.answer("⏳ Please wait 2 seconds.", show_alert=True)
        return

    user_last_interaction[user_id] = now
    await query.answer()

    action = query.data
    session = await get_session(context.application)

    try:
        if action.startswith("signal_"):
            symbol = action.split("_")[1]
            sig = await generate_signal(session, symbol)
            if sig:
                msg = f"🧠 **{sig['symbol']} SIGNAL** — {sig['confidence']}% Confidence\n\n" \
                      f"💰 Current: **${format_price(sig['price'])}**\n" \
                      f"🎯 Entry: **${format_price(sig['entry'])}**\n" \
                      f"🏆 TP: **${format_price(sig['tp'])}**\n" \
                      f"🛑 SL: **${format_price(sig['sl'])}**\n\n" \
                      f"{sig['emoji']} {sig['sentiment']} | RSI: {sig.get('rsi', 'N/A')}\n" \
                      f"🔍 {sig['reasoning']}\n" \
                      f"🔗 {sig['source']}"
                await query.edit_message_text(msg, reply_markup=build_main_menu(), parse_mode=ParseMode.MARKDOWN)

        elif action == "sectors":
            msg = await get_sector_map(session)
            await query.edit_message_text(msg, reply_markup=build_main_menu(), parse_mode=ParseMode.MARKDOWN)

        elif action == "whale":
            msg = await get_whale_radar(session)
            await query.edit_message_text(msg, reply_markup=build_main_menu(), parse_mode=ParseMode.MARKDOWN)

        elif action == "etf":
            msg = await fetch_etf_flows(session)
            await query.edit_message_text(msg, reply_markup=build_main_menu(), parse_mode=ParseMode.MARKDOWN)

        elif action == "news":
            msg = await get_intelligence_feed(session)
            await query.edit_message_text(msg, reply_markup=build_main_menu(), parse_mode=ParseMode.MARKDOWN)

        elif action == "performance":
            msg = await get_performance()
            await query.edit_message_text(msg, reply_markup=build_main_menu(), parse_mode=ParseMode.MARKDOWN)

        elif action == "stats":
            msg = await get_stats()
            await query.edit_message_text(msg, reply_markup=build_main_menu(), parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        logging.error(f"Button error: {e}")
        await query.edit_message_text("⚠️ Temporary error. Please try again.", reply_markup=build_main_menu())

# ================= START =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 **Agentic Finance Studio**\n\n"
        "SoSoValue Powered • ATR Signals • Institutional Intelligence\n\n"
        "Real-time signals with pullback entries, sector analysis, whale detection, ETF flows, and live SSI sentiment.\n\n"
        "⚠️ Not financial advice. Trade at your own risk.\n\n"
        "Tap any button below:",
        reply_markup=build_main_menu(),
        parse_mode=ParseMode.MARKDOWN
    )

def main():
    if not TOKEN:
        logging.error("❌ TELEGRAM_TOKEN not set!")
        return

    async def _internal_post_init(app):
        await get_session(app)
        logging.info("✅ Global session ready.")

    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .post_init(_internal_post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))

    logging.info("🚀 Agentic Finance Studio is running...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
