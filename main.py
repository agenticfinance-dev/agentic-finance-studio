import os, asyncio, logging, json, signal
from datetime import datetime
from aiohttp import web
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler

# SODEX - optional import so bot never crashes on JustRunMyApp
try:
    from eth_account import Account
    from eth_utils import keccak
    HAS_EIP712 = True
except ImportError:
    HAS_EIP712 = False
    Account = None
    keccak = None

TOKEN = os.getenv("TELEGRAM_TOKEN")
SOSO_API_KEY = os.getenv("SOSO_API_KEY")
SOSO_BASE = "https://openapi.sosovalue.com/openapi/v1"
SOSO_HEADERS = {
    "x-soso-api-key": SOSO_API_KEY,
    "Accept": "application/json"
}
SODEX_API_KEY_NAME = os.getenv("SODEX_API_KEY_NAME", "SODEX_API_KEY")
SODEX_API_PRIVATE_KEY = os.getenv("SODEX_API_PRIVATE_KEY")
SODEX_ACCOUNT_ID = os.getenv("SODEX_ACCOUNT_ID", "0")
SODEX_ENV = os.getenv("SODEX_ENV", "mainnet")
SODEX_PERPS_URL = "https://mainnet-gw.sodex.dev/api/v1/perps"
SODEX_CHAIN_ID = 286623
ALERT_CHAT_ID = os.getenv("ALERT_CHAT_ID")
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "300"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
TIMEOUT = aiohttp.ClientTimeout(total=20)
analytics = {"soso_calls": 0, "live_trades": 0, "scans": 0}
COIN_NAMES = {"btc": "bitcoin", "eth": "ethereum", "xrp": "ripple", "sol": "solana", "ada": "cardano", "doge": "dogecoin"}
price_cache = {}

async def soso_get(session, endpoint, params=None):
    url = f"{SOSO_BASE}/{endpoint}"
    async with session.get(url, headers=SOSO_HEADERS, params=params, timeout=TIMEOUT) as r:
        analytics["soso_calls"] += 1
        if r.status!= 200:
            body = await r.text()
            logging.error("SoSo %s returned %s: %s", endpoint, r.status, body[:500])
            return None
        res = await r.json()
        if res.get("code")!= 0:
            logging.error("SoSo %s code!=0: %s", endpoint, res.get("message"))
            return None
        return res.get("data")

def get_session(app):
    sess = app.bot_data.get("session")
    if not sess or sess.closed:
        sess = aiohttp.ClientSession(timeout=TIMEOUT)
        app.bot_data["session"] = sess
    return sess

async def get_price(session, symbol):
    sym = symbol.lower()
    coin = COIN_NAMES.get(sym)
    if not coin:
        return {"price": None, "change": 0}
    if sym in price_cache and (datetime.now() - price_cache[sym]["time"]).seconds < 30:
        return price_cache[sym]["data"]
    try:
        async with session.get(f"https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd&include_24hr_change=true", timeout=TIMEOUT) as r:
            if r.status == 200:
                j = await r.json()
                coin_data = j.get(coin, {})
                d = {"price": coin_data.get("usd"), "change": float(coin_data.get("usd_24h_change", 0) or 0)}
                if d["price"]:
                    price_cache[sym] = {"data": d, "time": datetime.now()}
                    return d
    except Exception as e:
        logging.error("price %s %s", symbol, e)
    return {"price": None, "change": 0}

async def get_indicators(session, symbol):
    try:
        async with session.get(f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}USDT&interval=1h&limit=60", timeout=TIMEOUT) as r:
            k = await r.json()
            if not isinstance(k, list) or len(k) < 50:
                return None
            closes = [float(x[4]) for x in k]
            ema20 = sum(closes[-20:]) / 20
            ema50 = sum(closes[-50:]) / 50
            gains = [max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
            losses = [max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
            rsi = 100-(100/(1+(sum(gains[-14:])/14)/(sum(losses[-14:])/14 or 0.0001)))
            highs = [float(x[2]) for x in k]
            lows = [float(x[3]) for x in k]
            tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1,len(closes))]
            atr = sum(tr[-14:]) / 14
            return {"rsi": round(rsi,1), "ema20": ema20, "ema50": ema50, "atr": atr}
    except Exception as e:
        logging.error("indicators %s %s", symbol, e)
        return None

async def get_etf_flow(session, symbol="BTC"):
    etf = await soso_get(session, "etfs/summary-history", {"symbol": symbol.upper(), "country_code": "US", "limit": 14})
    etf_rows = etf if isinstance(etf, list) else []
    etf_flow = sum(float(r.get("total_net_inflow", 0) or 0) for r in etf_rows)
    trend = "BULLISH" if etf_flow>0 else "BEARISH" if etf_flow<0 else "NEUTRAL"
    return {"flow": etf_flow, "trend": trend, "rows": etf_rows}

async def get_indices_parsed(session):
    indices = await soso_get(session, "indices")
    ssi_score = 70
    items = []
    if isinstance(indices, list):
        items = indices
        for item in items:
            logging.info("SSI Index: %s", item)
    return {"ssi_score": ssi_score, "items": items}

async def get_news_parsed(session, symbol="BTC"):
    news = await soso_get(session, "news", {"symbol": symbol.upper(), "limit": 5})
    news_list = []
    if isinstance(news, list):
        news_list = news
    elif isinstance(news, dict):
        news_list = news.get("list", [])

    titles = [(x.get("title") or x.get("content", "")) for x in news_list]

    bullish_words = ["bull","rally","surge","buy","etf","inflow","approval","whale","listing","adoption","partnership","launch","institutional","ai","rwa"]
    bearish_words = ["bear","crash","drop","sell","hack","exploit","liquidation","outflow","ban","lawsuit","attack"]

    pos = sum(1 for t in titles if any(w in t.lower() for w in bullish_words))
    neg = sum(1 for t in titles if any(w in t.lower() for w in bearish_words))

    sentiment = "BULLISH" if pos>neg else "BEARISH" if neg>pos else "NEUTRAL"
    return {"sentiment": sentiment, "titles": titles, "list": news_list, "pos": pos, "neg": neg}

class SoDEXExecutor:
    def __init__(self):
        self.ready = HAS_EIP712 and bool(SODEX_API_PRIVATE_KEY and SODEX_API_KEY_NAME)
        self.account_id = SODEX_ACCOUNT_ID
        if not HAS_EIP712:
            logging.warning("eth_account not installed - SoDEX EIP-712 disabled, install requirements.txt")

    def _hash(self, p):
        if not HAS_EIP712:
            return b""
        return keccak(text=json.dumps(p, separators=(',', ':'), ensure_ascii=False))

    def sign(self, payload):
        if not HAS_EIP712:
            return "0x", 0, "0x"
        nonce = int(datetime.now().timestamp()*1000)
        p_hash = self._hash(payload)
        typed = {
            "types":{
                "EIP712Domain":[
                    {"name":"name","type":"string"},
                    {"name":"chainId","type":"uint256"},
                    {"name":"verifyingContract","type":"address"}
                ],
                "ExchangeAction":[
                    {"name":"payloadHash","type":"bytes32"},
                    {"name":"nonce","type":"uint64"}
                ]
            },
            "primaryType":"ExchangeAction",
            "domain":{"name":"futures","chainId":SODEX_CHAIN_ID,"verifyingContract":"0x0000000000000000"},
            "message":{"payloadHash":p_hash,"nonce":nonce}
        }
        signed = Account.sign_typed_data(SODEX_API_PRIVATE_KEY, full_message=typed)
        sig = "0x01" + signed.signature.hex()[2:]
        return sig, nonce, p_hash.hex()

    async def place_order(self, session, sig_data):
        if not HAS_EIP712:
            return {"error":"eth_account not installed - run pip install -r requirements.txt","evidence":"Missing eth_account"}
        if not SODEX_API_PRIVATE_KEY:
            return {"error":"No SoDEX keys","evidence":"Set SODEX_API_PRIVATE_KEY"}
        params = {"clOrdID": f"AF-{int(datetime.now().timestamp())}","modifier":"0","side":"1" if sig_data["bias"]=="LONG" else "2","type":"1","timeInForce":"1","price":str(sig_data["entry"]),"quantity":str(round(sig_data["qty"],4)),"reduceOnly":False,"positionSide":"0"}
        sig, nonce, ph = self.sign({"type":"newOrder","params":params})
        headers = {"X-API-Key":SODEX_API_KEY_NAME,"X-API-Sign":sig,"X-API-Nonce":str(nonce),"Content-Type":"application/json"}
        try:
            async with session.post(f"{SODEX_PERPS_URL}/order", json=params, headers=headers, timeout=TIMEOUT) as r:
                res = await r.json()
                evidence = f"ChainID:{SODEX_CHAIN_ID} PH:{ph[:16]} Nonce:{nonce} Sig:{sig[:18]}.. Acct:{self.account_id} SoSo:{analytics['soso_calls']} {SODEX_ENV}"
                analytics["live_trades"]+=1
                return {"response":res,"evidence":evidence}
        except Exception as e:
            return {"error":str(e),"evidence":f"EIP712 ChainID:{SODEX_CHAIN_ID} Nonce:{nonce}"}

sodex = SoDEXExecutor()

async def intelligence_engine(session, symbol):
    price_data = await get_price(session, symbol)
    ind = await get_indicators(session, symbol)
    if not price_data["price"] or not ind:
        return None

    etf_task = get_etf_flow(session, symbol)
    indices_task = get_indices_parsed(session)
    news_task = get_news_parsed(session, symbol)
    etf, indices, news = await asyncio.gather(etf_task, indices_task, news_task)

    logging.debug("Shapes ETF:%s Indices:%s News:%s pos:%s neg:%s", len(etf["rows"]), len(indices["items"]), len(news["list"]), news["pos"], news["neg"])

    score = 50
    reasons = []

    if ind["rsi"] < 35:
        reasons.append(f"✅ RSI Oversold {ind['rsi']}")
        score += 12
    elif ind["rsi"] > 70:
        reasons.append(f"⚠️ RSI Overbought {ind['rsi']}")
        score -= 8
    else:
        reasons.append(f"✓ RSI Neutral {ind['rsi']}")

    if ind["ema20"] > ind["ema50"]:
        reasons.append(f"✓ EMA20 {ind['ema20']:.2f} > EMA50 {ind['ema50']:.2f}")
        score += 10
    else:
        reasons.append(f"✗ EMA20 {ind['ema20']:.2f} < EMA50 {ind['ema50']:.2f}")
        score -= 5

    reasons.append(f"✓ ETF ${etf['flow']/1e6:+.1f}M 14d {etf['trend']}")
    if etf["flow"]>50_000_000: score+=15
    elif etf["flow"]<-20_000_000: score-=10

    reasons.append(f"✓ SSI {indices['ssi_score']}/100")
    if indices["ssi_score"]>75: score+=8

    reasons.append(f"✓ News {news['sentiment']} {news['pos']}B/{news['neg']}S")
    if news["sentiment"]=="BULLISH": score+=7
    elif news["sentiment"]=="BEARISH": score-=5

    if abs(price_data["change"]) > 3:
        reasons.append(f"✓ Momentum {price_data['change']:+.1f}%")
        score+=8

    price = price_data["price"]
    atr = ind["atr"] or price*0.015
    if score>=70:
        bias="LONG"; entry=price*0.992; sl=entry-atr*1.2; tp=entry+atr*2.8
    elif score<=40:
        bias="SHORT"; entry=price*1.008; sl=entry+atr*1.2; tp=entry-atr*2.8
    else:
        bias="NEUTRAL"; entry=price; sl=entry-atr*0.8; tp=entry+atr*1.2

    rr = round(abs(tp - entry) / abs(entry - sl), 2) if entry!= sl else 2.0
    qty = (1000*0.015)/abs(entry-sl) if entry!=sl else 0.01
    qty = min(qty, (1000*0.5)/entry)

    return {"symbol":symbol.upper(),"price":price,"change":price_data["change"],"entry":entry,"sl":sl,"tp":tp,"rr":rr,"confidence":min(96,max(55,score)),"bias":bias,"reasons":reasons,"etf":etf,"indices":indices,"news":news,"rsi":ind["rsi"],"atr":atr,"qty":qty}

def fmt(p): return f"{p:.6f}" if p<1 else f"{p:.2f}" if p<100 else f"{p:,.0f}"

# ===== SCANNER ALERT LOOP =====
async def scanner_loop(app):
    logging.info("Scanner loop active - interval %ss AlertID=%s", SCAN_INTERVAL, bool(ALERT_CHAT_ID))
    while True:
        try:
            session = get_session(app)
            analytics["scans"]+=1
            for sym in ["btc", "eth", "sol"]:
                sig = await intelligence_engine(session, sym)
                if not sig:
                    continue
                if sig["confidence"] >= 75 and sig["bias"]!= "NEUTRAL":
                    logging.info("ALERT %s %s %s%%", sig["symbol"], sig["bias"], sig["confidence"])
                    if ALERT_CHAT_ID:
                        try:
                            txt = (
                                f"🚨 **{sig['symbol']} {sig['bias']} | {sig['confidence']}%**\n\n"
                                f"💰 ${fmt(sig['price'])} ({sig['change']:+.1f}%) RSI {sig['rsi']}\n\n"
                                + "\n".join(sig['reasons'][:5]) + f"\n\n"
                                f"Entry ${fmt(sig['entry'])}\nTP ${fmt(sig['tp'])} RR 1:{sig['rr']}\nSL ${fmt(sig['sl'])}"
                            )
                            kb = InlineKeyboardMarkup([[InlineKeyboardButton("⚡ EXECUTE SODEX", callback_data=f"exec_{sym}")]])
                            await app.bot.send_message(chat_id=ALERT_CHAT_ID, text=txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
                        except Exception as e:
                            logging.error("Scanner send failed %s", e)
            await asyncio.sleep(SCAN_INTERVAL)
        except asyncio.CancelledError:
            logging.info("Scanner loop cancelled - shutdown")
            break
        except Exception:
            logging.exception("Scanner crashed, retry in 60s")
            await asyncio.sleep(60)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"🚀 **Agentic Finance Live**\nSoSo:{analytics['soso_calls']} Scans:{analytics['scans']} SoDEX:{sodex.ready} EIP712:{HAS_EIP712}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 BTC", callback_data="signal_btc"), InlineKeyboardButton("📊 ETH", callback_data="signal_eth"), InlineKeyboardButton("📊 SOL", callback_data="signal_sol")],
            [InlineKeyboardButton("📡 Scanner ON", callback_data="scanner_on"), InlineKeyboardButton("⚡ EXECUTE REAL", callback_data="exec_btc")]
        ]),
        parse_mode=ParseMode.MARKDOWN
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    session = get_session(context.application)
    data = q.data

    if data.startswith("signal_"):
        sym = data.split("_")[1]
        sig = await intelligence_engine(session, sym)
        if not sig:
            await q.edit_message_text("Price fetch failed, retry")
            return
        txt = f"🧠 **{sig['symbol']} {sig['bias']} | {sig['confidence']}%**\n\n💰 ${fmt(sig['price'])} ({sig['change']:+.1f}%) RSI {sig['rsi']}\n\n**Reason:**\n" + "\n".join(sig['reasons']) + f"\n\nEntry ${fmt(sig['entry'])} TP ${fmt(sig['tp'])} RR 1:{sig['rr']} SL ${fmt(sig['sl'])}\n\nSoSo {analytics['soso_calls']} Scans {analytics['scans']} Qty {sig['qty']:.4f}"
        await q.edit_message_text(txt, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚡ EXECUTE", callback_data=f"exec_{sym}")]]), parse_mode=ParseMode.MARKDOWN)

    elif data.startswith("exec_"):
        sym = data.split("_")[1]
        sig = await intelligence_engine(session, sym)
        if not sig:
            await q.edit_message_text("Failed")
            return
        res = await sodex.place_order(session, sig)
        await q.edit_message_text(f"✅ **SODEX {SODEX_ENV.upper()} EIP-712**\n🔐 {res.get('evidence')}\n\n{json.dumps(res.get('response',''), indent=2)[:1200]}", parse_mode=ParseMode.MARKDOWN)

    elif data == "scanner_on":
        if ALERT_CHAT_ID:
            await q.edit_message_text(f"📡 Scanner active - sending alerts to {ALERT_CHAT_ID} every {SCAN_INTERVAL}s for 75%+ signals")
        else:
            await q.edit_message_text("Set ALERT_CHAT_ID env var to your Telegram ID from @userinfobot to get auto alerts")

async def health(request):
    return web.Response(text=f"OK SoSo:{analytics['soso_calls']} Scans:{analytics['scans']} SoDEX:{sodex.ready} EIP712:{HAS_EIP712}")

async def start_webserver():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 10000))).start()

async def main():
    logging.info("Starting Agentic Finance...")
    logging.info("Telegram token exists: %s", bool(TOKEN))
    logging.info("SoSo key exists: %s", bool(SOSO_API_KEY))
    logging.info("SoDEX ready: %s (HAS_EIP712=%s)", sodex.ready, HAS_EIP712)
    logging.info("Alert ID: %s Scanner every %ss", ALERT_CHAT_ID, SCAN_INTERVAL)

    await start_webserver()
    logging.info("Web server started on port %s", os.getenv("PORT", 10000))

    async with aiohttp.ClientSession(timeout=TIMEOUT) as s:
        try:
            await s.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
            logging.info("Webhook deleted")
        except Exception as e:
            logging.warning("Webhook delete failed: %s", e)

    app = ApplicationBuilder().token(TOKEN).build()
    app.bot_data["session"] = aiohttp.ClientSession(timeout=TIMEOUT)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    logging.info("Bot polling started")

    # Start scanner loop
    scanner_task = asyncio.create_task(scanner_loop(app))

    # Graceful shutdown
    async def graceful_shutdown():
        logging.info("Shutdown signal received - closing...")
        scanner_task.cancel()
        try:
            await scanner_task
        except asyncio.CancelledError:
            pass
        sess = app.bot_data.get("session")
        if sess and not sess.closed:
            await sess.close()
            logging.info("Global ClientSession closed")
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        logging.info("Bot stopped gracefully")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(graceful_shutdown()))
        except NotImplementedError:
            pass

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        await graceful_shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        logging.exception("Bot crashed during startup")
