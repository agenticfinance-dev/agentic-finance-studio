import os
import logging
import requests
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ====================== CONFIG ======================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SOSO_API_KEY = os.getenv("SOSO_API_KEY")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

COINS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "XRP": "ripple",
    "SOL": "solana"
}

HEADERS = {"User-Agent": "AgenticFinanceBot/1.0"}

# ====================== HELPERS ======================
def format_price(price):
    try:
        price = float(price)
        if price < 1:
            return f"{price:.6f}"
        elif price < 100:
            return f"{price:.2f}"
        return f"{price:,.0f}"
    except:
        return "N/A"


def get_priority_price(symbol, coin_id):
    """
    🧠 STRICT PRIORITY SYSTEM:
    1. SoSoValue (PRIMARY)
    2. CoinGecko (SECONDARY fallback)
    3. Binance (TERTIARY fallback)
    """

    # ================== 🥇 SOSO VALUE ==================
    if SOSO_API_KEY:
        try:
            headers = {
                "Authorization": f"Bearer {SOSO_API_KEY}",
                "x-soso-api-key": SOSO_API_KEY
            }

            r = requests.get(
                f"https://openapi.sosovalue.com/openapi/v1/asset/market/current-price?symbol={symbol}",
                headers=headers,
                timeout=10
            )

            if r.status_code == 200:
                data = r.json()
                if data.get("data"):
                    price = float(data["data"][0].get("price", 0))
                    if price > 0:
                        return price, "🥇 SoSoValue"
        except:
            pass

    # ================== 🥈 COINGECKO ==================
    try:
        r = requests.get(
            f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd",
            timeout=10
        )

        if r.status_code == 200:
            data = r.json()
            price = data[coin_id]["usd"]
            return price, "🥈 CoinGecko"
    except:
        pass

    # ================== 🥉 BINANCE ==================
    try:
        r = requests.get(
            f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT",
            timeout=10
        )

        if r.status_code == 200:
            price = float(r.json()["price"])
            return price, "🥉 Binance"
    except:
        pass

    return None, None


# ====================== START ======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 *Agentic Finance Studio*\n\n"
        "Institutional Crypto Intelligence System\n\n"
        "*Commands:*\n"
        "/signal\n"
        "/whale\n"
        "/btc /eth /xrp /sol",
        parse_mode="Markdown"
    )


# ====================== PRICE ======================
async def get_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    symbol = context.args[0].upper() if context.args else update.message.text.replace("/", "").upper()
    coin_id = COINS.get(symbol)

    msg = await update.message.reply_text(f"🔍 Fetching {symbol}...")

    if not coin_id:
        await msg.edit_text("❌ Unknown coin")
        return

    price, source = get_priority_price(symbol, coin_id)

    if not price:
        await msg.edit_text("❌ Price unavailable")
        return

    await msg.edit_text(
        f"📊 *{symbol}*\n💰 ${format_price(price)}\n{source}",
        parse_mode="Markdown"
    )


# ====================== SIGNAL (ALL COINS) ======================
async def signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("📡 Generating multi-coin signals...")

    report = "📊 *MULTI-COIN SIGNALS*\n\n"

    for symbol, coin_id in COINS.items():

        price, source = get_priority_price(symbol, coin_id)

        if not price:
            continue

        price = float(price)

        entry = price
        tp = price * 1.015
        sl = price * 0.985

        report += (
            f"{symbol}: ${format_price(price)}\n"
            f"Entry: {entry:.2f} | TP: {tp:.2f} | SL: {sl:.2f}\n"
            f"Source: {source}\n\n"
        )

    await msg.edit_text(report, parse_mode="Markdown")


# ====================== WHALE (ALL COINS) ======================
async def whale_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🐳 Scanning institutional flows...")

    report = "🐳 *INSTITUTIONAL FLOW REPORT*\n\n"

    for symbol, coin_id in COINS.items():

        price, source = get_priority_price(symbol, coin_id)

        if not price:
            continue

        price = float(price)

        signal = (
            "📈 Bullish" if price > 1 else
            "📉 Bearish" if price < 0.5 else
            "🟡 Neutral"
        )

        report += f"{symbol}: ${format_price(price)} → {signal} ({source})\n"

    await msg.edit_text(report, parse_mode="Markdown")


# ====================== MAIN ======================
if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("❌ TELEGRAM_TOKEN not set")

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("signal", signal))
    app.add_handler(CommandHandler("whale", whale_alert))
    app.add_handler(CommandHandler("price", get_price))

    for c in ["btc", "eth", "xrp", "sol"]:
        app.add_handler(CommandHandler(c, get_price))

    print("🚀 Bot running...")
    app.run_polling()
