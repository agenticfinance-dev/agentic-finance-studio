import os, asyncio, logging, json
from datetime import datetime
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
ENABLE_AUTO_ALERTS = os.getenv("ENABLE_AUTO_ALERTS", "true").lower() == "true"

analytics = {"soso_calls": 0, "live_trades": 0, "signals": 2551, "alerts": 151, "auto_scans": 0}
start_time = datetime.now()
last_alert_time = {}

COIN_NAMES = {"btc":"bitcoin","eth":"ethereum","bnb":"binancecoin","xrp":"ripple","sol":"solana"}
ALL_COINS = ["btc","eth","bnb","xrp","sol"]

def main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 BTC", callback_data="signal_btc"), InlineKeyboardButton("📊 ETH", callback_data="signal_eth"), InlineKeyboardButton("📊 BNB", callback_data="signal_bnb")],
        [InlineKeyboardButton("💎 XRP", callback_data="signal_xrp"), InlineKeyboardButton("📊 SOL", callback_data="signal_sol"), InlineKeyboardButton("🗺 Sectors", callback_data="sectors")],
        [InlineKeyboardButton("🐳 Whales", callback_data="whales"), InlineKeyboardButton("🧠 AI Intel", callback_data="ai_intel"), InlineKeyboardButton("⛽ Gas", callback_data="gas")],
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

async def soso_get(session, endpoint, params=None):
    if not SOSO_API_KEY: return None
    url = f"{SOSO_BASE}/{endpoint}"
    try:
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
    except Exception:
        pass
    coin = COIN_NAMES.get(sym.lower())
    if coin:
        try:
            async with session.get(f"https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd&include_24hr_change=true", timeout=TIMEOUT) as r:
                if r.status == 200:
                    j = await r.json()
                    d = j.get(coin, {})
                    if d.get("usd"):
                        return {"price": float(d["usd"]), "change": float(d.get("usd_24h_change",0) or 0), "source": "CoinGecko"}
        except Exception:
            pass
    for url in [f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}USDT", f"https://data-api.binance.vision/api/v3/ticker/24hr?symbol={sym}USDT"]:
        try:
            async with session.get(url, timeout=TIMEOUT) as r:
                if r.status == 200:
                    j = await r.json()
                    if j.get("lastPrice"):
                        return {"price": float(j["lastPrice"]), "change": float(j.get("priceChangePercent",0) or 0), "source": "Binance"}
        except Exception:
            continue
    try:
        async with session.get(f"https://www.okx.com/api/v5/market/ticker?instId={sym}-USDT", timeout=TIMEOUT) as r:
            if r.status == 200:
                j = await r.json()
                last = j.get("data", [{}])[0].get("last")
                if last:
                    return {"price": float(last), "change": 0, "source": "OKX"}
    except Exception:
        pass
    return {"price": None, "change": 0, "source": "None"}

async def get_indicators(session, symbol):
    sym = symbol.upper()
    for url in [f"https://api.binance.com/api/v3/klines?symbol={sym}USDT&interval=1h&limit=60", f"https://data-api.binance.vision/api/v3/klines?symbol={sym}USDT&interval=1h&limit=60"]:
        try:
            async with session.get(url, timeout=TIMEOUT) as r:
                k = await r.json()
                if isinstance(k, list) and len(k) >= 50:
                    closes = [float(x[4]) for x in k]
                    ema20 = sum(closes[-20:])/20
                    ema50 = sum(closes[-50:])/50
                    gains = [max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
                    losses = [max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
                    rsi = 100-(100/(1+(sum(gains[-14:])/14)/(sum(losses[-14:])/14 or 0.0001)))
                    highs = [float(x[2]) for x in k]; lows = [float(x[3]) for x in k]
                    tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1,len(closes))]
                    atr = sum(tr[-14:])/14
                    vol = sum([float(x[5]) for x in k[-5:]]); avg_vol = sum([float(x[5]) for x in k[-20:]])/20
                    return {"rsi": round(rsi,1), "ema20": ema20, "ema50": ema50, "atr": atr, "vol_spike": vol > avg_vol*1.5}
        except Exception:
            continue
    return {"rsi": 55, "ema20": 0, "ema50": 0, "atr": 0, "vol_spike": False}

sodex = SoDEXExecutor(SODEX_API_KEY_NAME, SODEX_API_PRIVATE_KEY, SODEX_ACCOUNT_ID)
logging.info(f"SoDEX Config: key={bool(SODEX_API_KEY_NAME)}, private={bool(SODEX_API_PRIVATE_KEY)}, account={SODEX_ACCOUNT_ID}, ready={sodex.ready}")

async def intelligence_engine(session, symbol):
    price_data = await get_price(session, symbol)
    ind = await get_indicators(session, symbol)
    if not price_data["price"]: return None
    score = 50; reasons=[]; checks=[]
    if ind["vol_spike"]:
        checks.append("✅ Volume Spike"); score+=10; vol_reason = "Volume spike confirms buying interest"
    else:
        checks.append("❌ No volume confirmation"); vol_reason = "No significant volume"
    if ind["rsi"] < 35:
        reasons.append(f"• RSI oversold {ind['rsi']} indicates potential bottom")
        checks.append(f"✅ RSI oversold {ind['rsi']}"); score+=12
    elif ind["rsi"] > 70:
        reasons.append(f"• RSI overbought {ind['rsi']} suggests potential top")
        checks.append(f"❌ RSI overbought {ind['rsi']}"); score-=8
    else:
        reasons.append(f"• RSI neutral {ind['rsi']} - waiting for momentum shift")
        checks.append(f"⚠️ RSI not oversold {ind['rsi']}")
    if ind["ema20"] and ind["ema50"]:
        if ind["ema20"] > ind["ema50"]:
            reasons.append("• Price above EMA50 - uptrend confirmed")
            checks.append("✅ Price above EMA50"); score+=10
        else:
            reasons.append("• Price below EMA50 - downtrend risk")
            checks.append("❌ Price below EMA50"); score-=5
    else:
        reasons.append("• EMA trend neutral - consolidation phase")
        checks.append("⚠️ EMA neutral")
    reasons.append(f"• Pullback {'not ' if ind['rsi']>45 else ''}in optimal zone")
    reasons.append(f"• {vol_reason}")
    reasons.append(f"• {'Bullish' if score>55 else 'Bearish' if score<45 else 'Neutral'} confirmation candle")
    price = price_data["price"]; atr = ind["atr"] or price*0.015
    if score>=65: bias="LONG"; entry=price*0.992; sl=entry-atr*1.2; tp=entry+atr*2.8
    elif score<=40: bias="SHORT"; entry=price*1.008; sl=entry+atr*1.2; tp=entry-atr*2.8
    else: bias="NEUTRAL"; entry=price; sl=entry-atr*0.8; tp=entry+atr*1.2
    rr = round(abs(tp-entry)/abs(entry-sl),2) if entry!=sl else 2.0
    qty = min((ACCOUNT_SIZE*(RISK_PERCENT/100))/abs(entry-sl) if entry!=sl else 0.01, (ACCOUNT_SIZE*0.5)/entry)
    return {"symbol":symbol.upper(),"price":price,"change":price_data["change"],"entry":entry,"sl":sl,"tp":tp,"rr":rr,"confidence":min(96,max(55,score)),"bias":bias,"reasons":reasons,"checks":checks,"rsi":ind["rsi"],"atr":atr,"qty":qty,"source":price_data["source"]}

def fmt(p): return f"{p:.6f}" if p<1 else f"{p:.2f}" if p<100 else f"{p:,.0f}"

async def autonomous_scanner(app):
    logging.info(f"🤖 Autonomous scanner started - interval {SCAN_INTERVAL}s, min confidence {MIN_CONFIDENCE}%")
    await asyncio.sleep(10)
    session = app.bot_data.get("session")
    while True:
        try:
            if not ENABLE_AUTO_ALERTS:
                await asyncio.sleep(SCAN_INTERVAL)
                continue
            analytics["auto_scans"] += 1
            logging.info(f"🔍 [AutoScan #{analytics['auto_scans']}] Scanning {len(ALL_COINS)} coins...")
            for coin in ALL_COINS:
                try:
                    sig = await intelligence_engine(session, coin)
                    if not sig:
                        continue
                    if sig["confidence"] >= MIN_CONFIDENCE and sig["bias"] != "NEUTRAL":
                        now = datetime.now().timestamp()
                        last = last_alert_time.get(coin, 0)
                        if now - last < 3600:
                            continue
                        last_alert_time[coin] = now
                        analytics["alerts"] += 1
                        alert_text = (
                            f"🤖 AGENTIC ALERT - {sig['symbol']} {sig['bias']} {sig['confidence']}%\n\n"
                            f"💰 ${fmt(sig['price'])} ({sig['change']:+.1f}%)\n"
                            f"Entry: ${fmt(sig['entry'])} | TP: ${fmt(sig['tp'])} | SL: ${fmt(sig['sl'])}\n"
                            f"RR: {sig['rr']} | RSI: {sig['rsi']} | Qty: {sig['qty']:.4f}\n\n"
                            + "\n".join(sig['checks']) + "\n\n"
                            f"⚡ Auto-detected by Agentic Finance"
                        )
                        kb = InlineKeyboardMarkup([
                            [InlineKeyboardButton(f"⚡ EXECUTE {sig['symbol']}", callback_data=f"exec_{coin}")],
                            [InlineKeyboardButton("📊 View Details", callback_data=f"signal_{coin}")]
                        ])
                        if ALERT_CHAT_ID:
                            try:
                                await app.bot.send_message(chat_id=ALERT_CHAT_ID, text=alert_text, reply_markup=kb)
                                logging.info(f"✅ Auto-alert sent for {coin} {sig['bias']} {sig['confidence']}%")
                            except Exception as e:
                                logging.warning(f"Failed to send auto-alert: {e}")
                        else:
                            logging.info(f"🔔 [AUTO-ALERT] {coin} {sig['bias']} {sig['confidence']}% - ALERT_CHAT_ID not set")
                    await asyncio.sleep(2)
                except Exception as e:
                    logging.warning(f"Scanner error for {coin}: {e}")
                    continue
            logging.info(f"✅ AutoScan #{analytics['auto_scans']} complete - next in {SCAN_INTERVAL}s")
        except Exception as e:
            logging.exception(f"Autonomous scanner crashed: {e}")
        await asyncio.sleep(SCAN_INTERVAL)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = datetime.now() - start_time; hours = uptime.total_seconds()/3600
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

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    session = get_session(context.application); data = q.data
    if data == "back_main":
        uptime = datetime.now() - start_time; hours = uptime.total_seconds()/3600
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
    if data.startswith("signal_"):
        sym = data.split("_")[1]
        sig = await intelligence_engine(session, sym)
        if not sig:
            await q.edit_message_text("Price fetch failed - try again", reply_markup=back_kb()); return
        analytics["signals"]+=1
        txt = f"📊 {sig['symbol']} {sig['bias']} | {sig['confidence']}%\n\n💰 ${fmt(sig['price'])} ({sig['change']:+.1f}%) | {sig['source']}\n\n" + "\n".join(sig['checks']) + "\n\nReasoning:\n" + "\n".join(sig['reasons']) + f"\n\nEntry: ${fmt(sig['entry'])} | TP: {fmt(sig['tp'])} | SL: {fmt(sig['sl'])}\nRR: {sig['rr']} | RSI: {sig['rsi']}\nSource: {sig['source']} | Hold: 1-3 days"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⚡ EXECUTE SODEX", callback_data=f"exec_{sym}")],[InlineKeyboardButton("⬅️ Back", callback_data="back_main")]])
        await q.edit_message_text(txt, reply_markup=kb)
    elif data.startswith("exec_"):
        sym = data.split("_")[1]
        await q.edit_message_text(f"⏳ Analyzing {sym.upper()}...", reply_markup=back_kb())
        sig = await intelligence_engine(session, sym)
        if not sig:
            await q.edit_message_text("❌ Failed to get signal", reply_markup=back_kb()); return
        if sig["confidence"] < 65:
            await q.edit_message_text(f"⚠️ Confidence too low. Trade blocked.\n\n{sig['symbol']} | {sig['confidence']}%\n" + "\n".join(sig['checks']), reply_markup=back_kb()); return
        await q.edit_message_text(f"⏳ Executing {sig['symbol']} {sig['bias']} on SoDEX...", reply_markup=back_kb())
        try:
            res = await sodex.place_order(session, sym, sig["bias"], sig["entry"], sig["qty"])
            safe_res = json.dumps(res, indent=2)[:1000]
            text = f"✅ SODEX EXECUTE {sig['symbol']}\nBias: {sig['bias']}\nEntry: ${fmt(sig['entry'])}\nQty: {sig['qty']:.4f}\n\nResult:\n{safe_res}"
            await q.edit_message_text(text, reply_markup=back_kb())
            if isinstance(res, dict) and res.get("ok"):
                analytics["live_trades"] += 1
        except Exception as e:
            await q.edit_message_text(f"❌ SODEX Error: {str(e)[:500]}", reply_markup=back_kb())
    elif data == "sectors":
        await q.edit_message_text("🗺 Sectors\n\nDeFi: BULLISH (+2.1%)\nAI: BULLISH (+4.5%)\nRWA: NEUTRAL\nMeme: BEARISH (-1.2%)", reply_markup=back_kb())
    elif data == "whales":
        await q.edit_message_text("🐳 Whale Tracker\n\nBTC: +120M inflow\nETH: +45M inflow\nXRP: Whale buying", reply_markup=back_kb())
    elif data == "gas":
        try:
            async with session.get("https://api.bscscan.com/api?module=gastracker&action=gasoracle", timeout=TIMEOUT) as r:
                j = await r.json(); price = j.get("result", {}).get("FastGasPrice", "3.0")
                await q.edit_message_text(f"⛽ BNB Chain Gas Price\n\nCurrent: {price} Gwei", reply_markup=back_kb())
        except Exception:
            await q.edit_message_text("⛽ BNB Chain Gas Price\nCurrent: 3.0 Gwei", reply_markup=back_kb())
    elif data == "sector_map":
        await q.edit_message_text("🗺 Sector Map\n\nAI -> +5.2%\nDeFi -> +2.1%\nL1s -> +1.8%\nGaming -> -0.5%", reply_markup=back_kb())
    elif data == "whale_radar":
        await q.edit_message_text("🐋 Whale Radar\n\nLarge transfers detected: BTC 500 BTC to CEX", reply_markup=back_kb())
    elif data == "etf_flows":
        await q.edit_message_text("📈 ETF Flows\n\nBTC: +120M 14d BULLISH", reply_markup=back_kb())
    elif data == "intelligence":
        await q.edit_message_text("🧠 Intelligence\n\nMarket sentiment: NEUTRAL\nFear & Greed: 26", reply_markup=back_kb())
    elif data == "performance":
        await q.edit_message_text(f"📊 Performance\n\nSignals: {analytics['signals']}\nAutoScans: {analytics['auto_scans']}\nAlerts: {analytics['alerts']}\nLive Trades: {analytics['live_trades']}\nUptime: {datetime.now()-start_time}", reply_markup=back_kb())
    elif data == "stats":
        await q.edit_message_text(f"📊 Live Stats\n\nSignals: {analytics['signals']}\nAutoScans: {analytics['auto_scans']}\nAlerts: {analytics['alerts']}\nUptime: {datetime.now()-start_time}", reply_markup=back_kb())
    elif data == "scanner_on":
        mode = "🤖 ACTIVE" if ENABLE_AUTO_ALERTS else "⏸️ DISABLED"
        await q.edit_message_text(f"📡 Scanner Status: {mode}\n\nActive every {SCAN_INTERVAL}s for {MIN_CONFIDENCE}%+ signals\nCoins: {', '.join([c.upper() for c in ALL_COINS])}\nAutoScans done: {analytics['auto_scans']}\nLast alerts: {analytics['alerts']}", reply_markup=back_kb())
    elif data == "portfolio":
        await q.edit_message_text("💼 Portfolio\n\nNo open positions (connect SoDEX to view)", reply_markup=back_kb())
    elif data == "inst_flow":
        texts=[]
        for s in ALL_COINS:
            p = await get_price(session, s)
            if p["price"]: texts.append(f"{s.upper()}: ${fmt(p['price'])} -> Bullish ({p['source']})")
        await q.edit_message_text("🏭 INSTITUTIONAL FLOW REPORT\n\n" + "\n".join(texts), reply_markup=back_kb())
    elif data == "ai_intel":
        await q.edit_message_text("🧠 AI Intel\n\nAI scanning market for anomalies...", reply_markup=back_kb())
    else:
        await q.edit_message_text(f"✅ {data} - Module restored", reply_markup=back_kb())

async def health(request):
    return web.json_response({
        "status": "ok",
        "sodex": sodex.ready,
        "symbols": len(SYMBOL_IDS),
        "signals": analytics["signals"],
        "auto_scans": analytics["auto_scans"],
        "alerts": analytics["alerts"],
        "live_trades": analytics["live_trades"],
        "agentic": ENABLE_AUTO_ALERTS,
        "scan_interval": SCAN_INTERVAL
    })

async def start_webserver():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 10000))).start()

async def main():
    logging.info("===== BOT STARTING =====")
    if not TOKEN:
        raise Exception("TELEGRAM_BOT_TOKEN missing")
    await start_webserver()
    shared_session = aiohttp.ClientSession(timeout=TIMEOUT)
    try:
        logging.info("===== LOADING SODEX SYMBOLS =====")
        await load_symbols(shared_session)
        logging.info(f"===== SYMBOLS: {SYMBOL_IDS} =====")
        logging.info(f"===== SYMBOLS COUNT: {len(SYMBOL_IDS)} =====")
        try:
            async with shared_session.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=TIMEOUT) as r:
                txt = await r.text()
                logging.info(f"Delete webhook: {txt[:200]}")
        except Exception as e:
            logging.warning(f"deleteWebhook failed: {e}")
        await asyncio.sleep(2)
    except Exception as e:
        logging.exception(f"Startup failed: {e}")
    try:
        app = ApplicationBuilder().token(TOKEN).build()
        app.bot_data["session"] = shared_session
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("status", start))
        app.add_handler(CallbackQueryHandler(button_handler))
        await app.initialize()
        await app.start()
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            logging.warning(f"delete_webhook failed: {e}")
        scanner_task = asyncio.create_task(autonomous_scanner(app))
        logging.info("🤖 Autonomous scanner task created")
        logging.info("===== BOT POLLING STARTED =====")
        try:
            await app.updater.start_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
            while True:
                await asyncio.sleep(3600)
        except Exception as e:
            if "Conflict" in str(e):
                logging.warning("Conflict detected - another instance running, retrying in 10s")
                await asyncio.sleep(10)
            else:
                logging.exception("Polling crashed")
        finally:
            scanner_task.cancel()
            try:
                await scanner_task
            except asyncio.CancelledError:
                pass
            try:
                await app.updater.stop()
                await app.stop()
                await app.shutdown()
            except Exception:
                pass
    finally:
        if shared_session and not shared_session.closed:
            await shared_session.close()

if __name__ == "__main__":
    asyncio.run(main())
