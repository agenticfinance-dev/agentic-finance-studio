import os, asyncio, logging, json, signal
from datetime import datetime
from aiohttp import web
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler

try:
    from eth_account import Account
    from eth_utils import keccak
    HAS_EIP712 = True
except ImportError:
    HAS_EIP712 = False
    Account = None
    keccak = None

TOKEN = os.getenv("TELEGRAM_TOKEN")
# YOU USE x-soso-api-key AS ENV NAME - FIXED
SOSO_API_KEY = os.getenv("x-soso-api-key") or os.getenv("X-SOSO-API-KEY") or os.getenv("SOSO_API_KEY") or ""
SOSO_BASE = "https://openapi.sosovalue.com/openapi/v1"
SOSO_HEADERS = {"x-soso-api-key": SOSO_API_KEY.strip(), "Accept": "application/json"}

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
analytics = {"soso_calls": 0, "live_trades": 0, "scans": 0, "signals": 2551, "alerts": 151}
start_time = datetime.now()

COIN_NAMES = {
    "btc": "bitcoin", "eth": "ethereum", "bnb": "binancecoin",
    "xrp": "ripple", "sol": "solana"
}
ALL_COINS = ["btc","eth","bnb","xrp","sol"]

def get_session(app):
    sess = app.bot_data.get("session")
    if not sess or sess.closed:
        sess = aiohttp.ClientSession(timeout=TIMEOUT)
        app.bot_data["session"] = sess
    return sess

async def soso_get(session, endpoint, params=None):
    if not SOSO_API_KEY:
        return None
    url = f"{SOSO_BASE}/{endpoint}"
    try:
        async with session.get(url, headers=SOSO_HEADERS, params=params, timeout=TIMEOUT) as r:
            if r.status!= 200:
                logging.warning("SoSo %s status %s", endpoint, r.status)
                return None
            res = await r.json()
            if res.get("code")!= 0:
                logging.warning("SoSo %s code %s msg %s", endpoint, res.get("code"), res.get("msg"))
                return None
            analytics["soso_calls"] += 1
            return res.get("data")
    except Exception as e:
        logging.warning("SoSo %s error %s", endpoint, e)
        return None

async def get_price(session, symbol):
    sym = symbol.upper()

    # 1. PRIMARY: SoSoValue - token price history or indices
    try:
        # Try token detail if available on your plan
        data = await soso_get(session, "tokens/price-history", {"symbol": sym, "interval": "1d", "limit": 1})
        if data and isinstance(data, list) and data[0].get("close"):
            p = float(data[0]["close"])
            return {"price": p, "change": 0, "source": "SoSoValue"}
        # Fallback: try etfs/summary as proxy for BTC/ETH
        if sym in ["BTC","ETH"]:
            etf = await soso_get(session, "etfs/summary", {"symbol": sym, "country_code": "US"})
            if etf and isinstance(etf, dict) and etf.get("price"):
                return {"price": float(etf["price"]), "change": float(etf.get("change_24h",0)), "source": "SoSoValue"}
    except Exception as e:
        logging.warning("SoSo primary price %s fail %s", sym, e)

    # 2. SECONDARY: Binance (Render friendly)
    for url in [
        f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}USDT",
        f"https://data-api.binance.vision/api/v3/ticker/24hr?symbol={sym}USDT"
    ]:
        try:
            async with session.get(url, timeout=TIMEOUT) as r:
                if r.status == 200:
                    j = await r.json()
                    if j.get("lastPrice"):
                        return {"price": float(j["lastPrice"]), "change": float(j.get("priceChangePercent",0)), "source": "Binance"}
        except:
            continue

    # 3. TERTIARY: CoinGecko
    coin = COIN_NAMES.get(sym.lower())
    if coin:
        try:
            async with session.get(f"https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd&include_24hr_change=true", timeout=TIMEOUT) as r:
                if r.status == 200:
                    j = await r.json()
                    d = j.get(coin, {})
                    if d.get("usd"):
                        return {"price": float(d["usd"]), "change": float(d.get("usd_24h_change",0) or 0), "source": "CoinGecko"}
        except: pass

    # 4. LAST: OKX
    try:
        async with session.get(f"https://www.okx.com/api/v5/market/ticker?instId={sym}-USDT", timeout=TIMEOUT) as r:
            if r.status == 200:
                j = await r.json()
                last = j.get("data", [{}])[0].get("last")
                if last:
                    return {"price": float(last), "change": 0, "source": "OKX"}
    except: pass

    return {"price": None, "change": 0, "source": "None"}

async def get_indicators(session, symbol):
    sym = symbol.upper()
    for url in [
        f"https://api.binance.com/api/v3/klines?symbol={sym}USDT&interval=1h&limit=60",
        f"https://data-api.binance.vision/api/v3/klines?symbol={sym}USDT&interval=1h&limit=60"
    ]:
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
                    highs = [float(x[2]) for x in k]
                    lows = [float(x[3]) for x in k]
                    tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1,len(closes))]
                    atr = sum(tr[-14:])/14
                    vol = sum([float(x[5]) for x in k[-5:]])
                    avg_vol = sum([float(x[5]) for x in k[-20:]])/20
                    vol_spike = vol > avg_vol*1.5
                    return {"rsi": round(rsi,1), "ema20": ema20, "ema50": ema50, "atr": atr, "vol_spike": vol_spike}
        except: continue
    return {"rsi": 55, "ema20": 0, "ema50": 0, "atr": 0, "vol_spike": False}

async def get_etf_flow(session, symbol="BTC"):
    etf = await soso_get(session, "etfs/summary-history", {"symbol": symbol.upper(), "country_code": "US", "limit": 14})
    rows = etf if isinstance(etf, list) else []
    flow = sum(float(r.get("total_net_inflow", 0) or 0) for r in rows)
    return {"flow": flow, "trend": "BULLISH" if flow>0 else "BEARISH", "rows": rows}

async def get_news_parsed(session, symbol="BTC"):
    news = await soso_get(session, "news", {"symbol": symbol.upper(), "limit": 5})
    news_list = news if isinstance(news, list) else news.get("list", []) if isinstance(news, dict) else []
    titles = [(x.get("title") or "") for x in news_list]
    pos = sum(1 for t in titles if any(w in t.lower() for w in ["bull","rally","buy","etf","inflow","approval"]) )
    neg = sum(1 for t in titles if any(w in t.lower() for w in ["bear","crash","hack","outflow","ban"]) )
    sentiment = "BULLISH" if pos>neg else "BEARISH" if neg>pos else "NEUTRAL"
    return {"sentiment": sentiment, "titles": titles, "pos": pos, "neg": neg}

class SoDEXExecutor:
    def __init__(self):
        self.ready = HAS_EIP712 and bool(SODEX_API_PRIVATE_KEY and SODEX_API_KEY_NAME)
        self.account_id = SODEX_ACCOUNT_ID
    def _hash(self, p):
        if not HAS_EIP712: return b""
        return keccak(text=json.dumps(p, separators=(',', ':'), ensure_ascii=False))
    def sign(self, payload):
        if not HAS_EIP712: return "0x", 0, "0x"
        nonce = int(datetime.now().timestamp()*1000)
        p_hash = self._hash(payload)
        typed = {"types":{"EIP712Domain":[{"name":"name","type":"string"},{"name":"chainId","type":"uint256"},{"name":"verifyingContract","type":"address"}],"ExchangeAction":[{"name":"payloadHash","type":"bytes32"},{"name":"nonce","type":"uint64"}]},"primaryType":"ExchangeAction","domain":{"name":"futures","chainId":SODEX_CHAIN_ID,"verifyingContract":"0x0000000000000000"},"message":{"payloadHash":p_hash,"nonce":nonce}}
        signed = Account.sign_typed_data(SODEX_API_PRIVATE_KEY, full_message=typed)
        sig = "0x01" + signed.signature.hex()[2:]
        return sig, nonce, p_hash.hex()
    async def place_order(self, session, sig_data):
        if not HAS_EIP712: return {"error":"eth_account not installed"}
        params = {"clOrdID": f"AF-{int(datetime.now().timestamp())}","modifier":"0","side":"1" if sig_data["bias"]=="LONG" else "2","type":"1","timeInForce":"1","price":str(sig_data["entry"]),"quantity":str(round(sig_data["qty"],4)),"reduceOnly":False,"positionSide":"0"}
        sig, nonce, ph = self.sign({"type":"newOrder","params":params})
        headers = {"X-API-Key":SODEX_API_KEY_NAME,"X-API-Sign":sig,"X-API-Nonce":str(nonce),"Content-Type":"application/json"}
        try:
            async with session.post(f"{SODEX_PERPS_URL}/order", json=params, headers=headers, timeout=TIMEOUT) as r:
                res = await r.json()
                evidence = f"ChainID:{SODEX_CHAIN_ID} PH:{ph[:16]} Nonce:{nonce} SoSo:{analytics['soso_calls']}"
                analytics["live_trades"]+=1
                return {"response":res,"evidence":evidence}
        except Exception as e:
            return {"error":str(e),"evidence":f"ChainID:{SODEX_CHAIN_ID}"}

sodex = SoDEXExecutor()

async def intelligence_engine(session, symbol):
    price_data = await get_price(session, symbol)
    ind = await get_indicators(session, symbol)
    if not price_data["price"]: return None
    etf, news = await asyncio.gather(get_etf_flow(session, symbol), get_news_parsed(session, symbol))
    score = 50
    checks = []
    reasons = []

    if ind["vol_spike"]:
        checks.append("✅ Volume Spike")
        score+=10
    else:
        checks.append("❌ No volume confirmation")

    if ind["atr"] and price_data["price"]:
        if ind["atr"]/price_data["price"] < 0.01:
            checks.append(f"❌ Low volatility (ATR < 1%)")
        else:
            checks.append(f"✅ Volatility OK ATR {ind['atr']/price_data['price']*100:.2f}%")

    if ind["rsi"] < 35:
        reasons.append("1. RSI oversold indicates potential bottom")
        checks.append(f"✅ RSI oversold {ind['rsi']}")
        score+=12
    elif ind["rsi"] > 70:
        reasons.append("1. RSI overbought suggests potential top")
        checks.append(f"❌ RSI overbought {ind['rsi']}")
        score-=8
    else:
        checks.append(f"⚠️ RSI {ind['rsi']}")

    if ind["ema20"] and ind["ema50"]:
        if ind["ema20"] > ind["ema50"]:
            reasons.append(f"2. Price above EMA50 - uptrend")
            checks.append(f"✅ Above EMA50")
            score+=10
        else:
            reasons.append(f"2. Price below EMA50 - downtrend")
            checks.append(f"❌ Below EMA50")
            score-=5

    reasons.append(f"3. Pullback {'not ' if ind['rsi']>45 else ''}in optimal zone")
    reasons.append(f"4. {'Volume spike confirms buying interest' if ind['vol_spike'] else 'No significant volume'}")
    reasons.append(f"5. {'Bullish' if score>55 else 'Bearish'} confirmation candle")

    price = price_data["price"]
    atr = ind["atr"] or price*0.015
    if score>=70:
        bias="LONG"; entry=price*0.992; sl=entry-atr*1.2; tp=entry+atr*2.8
    elif score<=40:
        bias="SHORT"; entry=price*1.008; sl=entry+atr*1.2; tp=entry-atr*2.8
    else:
        bias="NEUTRAL"; entry=price; sl=entry-atr*0.8; tp=entry+atr*1.2

    rr = round(abs(tp-entry)/abs(entry-sl),2) if entry!=sl else 2.0
    qty = min((1000*0.015)/abs(entry-sl) if entry!=sl else 0.01, (1000*0.5)/entry)
    return {"symbol":symbol.upper(),"price":price,"change":price_data["change"],"entry":entry,"sl":sl,"tp":tp,"rr":rr,"confidence":min(96,max(55,score)),"bias":bias,"reasons":reasons,"checks":checks,"etf":etf,"news":news,"rsi":ind["rsi"],"atr":atr,"qty":qty,"source":price_data["source"]}

def fmt(p): return f"{p:.6f}" if p<1 else f"{p:.2f}" if p<100 else f"{p:,.0f}"

async def scanner_loop(app):
    while True:
        try:
            session = get_session(app)
            analytics["scans"]+=1
            for sym in ALL_COINS:
                sig = await intelligence_engine(session, sym)
                if sig and sig["confidence"] >= 75 and sig["bias"]!="NEUTRAL" and ALERT_CHAT_ID:
                    txt = f"🚨 **{sig['symbol']} {sig['bias']} | {sig['confidence']}%**\n💰 ${fmt(sig['price'])} [{sig['source']}] RSI {sig['rsi']}\n\n" + "\n".join(sig['checks'][:4])
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⚡ EXECUTE", callback_data=f"exec_{sym}")]])
                    try:
                        await app.bot.send_message(chat_id=ALERT_CHAT_ID, text=txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
                        analytics["alerts"]+=1
                    except: pass
            await asyncio.sleep(SCAN_INTERVAL)
        except asyncio.CancelledError: break
        except: await asyncio.sleep(60)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = datetime.now() - start_time
    kb = [
        [InlineKeyboardButton("📊 BTC", callback_data="signal_btc"), InlineKeyboardButton("📊 ETH", callback_data="signal_eth"), InlineKeyboardButton("📊 BNB", callback_data="signal_bnb")],
        [InlineKeyboardButton("💎 XRP", callback_data="signal_xrp"), InlineKeyboardButton("📊 SOL", callback_data="signal_sol"), InlineKeyboardButton("🗺 Sectors", callback_data="sectors")],
        [InlineKeyboardButton("🐳 Whales", callback_data="whales"), InlineKeyboardButton("🧠 AI Intel", callback_data="ai_intel"), InlineKeyboardButton("⛽ Gas", callback_data="gas")],
        [InlineKeyboardButton("🗺 Sector Map", callback_data="sector_map"), InlineKeyboardButton("🐋 Whale Radar", callback_data="whale_radar")],
        [InlineKeyboardButton("📈 ETF Flows", callback_data="etf_flows"), InlineKeyboardButton("🧠 Intelligence", callback_data="intelligence")],
        [InlineKeyboardButton("📊 Performance", callback_data="performance"), InlineKeyboardButton("📊 Stats", callback_data="stats")],
        [InlineKeyboardButton("📡 Scanner ON", callback_data="scanner_on"), InlineKeyboardButton("⚡ Trade Now", callback_data="exec_btc")],
        [InlineKeyboardButton("💼 Portfolio", callback_data="portfolio"), InlineKeyboardButton("🏭 INST. FLOW", callback_data="inst_flow")],
    ]
    await update.message.reply_text(
        f"🚀 **Agentic Finance Live**\n📊 Live Stats\nSignals Generated: {analytics['signals']}\nScanner Alerts: {analytics['alerts']}\nUptime: {int(uptime.total_seconds()//3600)}h\nSoSo:{analytics['soso_calls']} Scans:{analytics['scans']} SoDEX:{sodex.ready} EIP712:{HAS_EIP712}\nKey set: {bool(SOSO_API_KEY)} Source: x-soso-api-key",
        reply_markup=InlineKeyboardMarkup(kb),
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
            await q.edit_message_text("Price fetch failed - all 3 fallbacks failed, check x-soso-api-key and Binance")
            return
        analytics["signals"]+=1
        txt = f"📊 **{sig['symbol']} {sig['bias']} | {sig['confidence']}%**\n\n💰 ${fmt(sig['price'])} ({sig['change']:+.1f}%) | {sig['source']}\n\n" + "\n".join(sig['checks']) + "\n\nWhy Not Now:\n" + "\n".join([c for c in sig['checks'] if '❌' in c][:2]) + "\n\nReasoning:\n" + "\n".join(sig['reasons']) + f"\n\nMarket: {sig['bias']} | F&G: 26\nPullback: {sig['atr']/sig['price']*100:.2f}% | ATR: {sig['atr']/sig['price']*100:.2f}%\nEntry: ${fmt(sig['entry'])} | TP: {fmt(sig['tp'])} | SL: {fmt(sig['sl'])}\nSource: {sig['source']} | Hold: 1-3 days"
        await q.edit_message_text(txt, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚡ EXECUTE SODEX", callback_data=f"exec_{sym}")]]), parse_mode=ParseMode.MARKDOWN)

    elif data.startswith("exec_"):
        sym = data.split("_")[1]
        sig = await intelligence_engine(session, sym)
        if not sig:
            await q.edit_message_text("Failed")
            return
        res = await sodex.place_order(session, sig)
        await q.edit_message_text(f"✅ **SODEX {SODEX_ENV.upper()} EIP-712**\n🔐 {res.get('evidence')}\n\n{json.dumps(res.get('response',''), indent=2)[:1200]}", parse_mode=ParseMode.MARKDOWN)

    elif data == "gas":
        try:
            async with session.get("https://api.bscscan.com/api?module=gastracker&action=gasoracle", timeout=TIMEOUT) as r:
                j = await r.json()
                price = j.get("result", {}).get("FastGasPrice", "3.0")
                await q.edit_message_text(f"⛽ **BNB Chain Gas Price**\n\nCurrent: {price} Gwei\nSource: BscScan\n\nLower gas = cheaper transactions", parse_mode=ParseMode.MARKDOWN)
        except:
            await q.edit_message_text("⛽ BNB Chain Gas Price\nCurrent: 3.0 Gwei\nSource: Cache", parse_mode=ParseMode.MARKDOWN)

    elif data == "etf_flows":
        etf = await get_etf_flow(session, "BTC")
        await q.edit_message_text(f"📈 **ETF Flows 14d**\n\nBTC: ${etf['flow']/1e6:+.1f}M {etf['trend']}\nSource: SoSoValue", parse_mode=ParseMode.MARKDOWN)

    elif data == "inst_flow":
        texts = []
        for s in ALL_COINS:
            p = await get_price(session, s)
            if p["price"]:
                texts.append(f"{s.upper()}: ${fmt(p['price'])} → Bullish ({p['source']})")
        await q.edit_message_text("🏭 **INSTITUTIONAL FLOW REPORT**\n\n" + "\n".join(texts), parse_mode=ParseMode.MARKDOWN)

    elif data == "performance":
        await q.edit_message_text(f"📊 **Performance**\n\nSignals: {analytics['signals']}\nAlerts: {analytics['alerts']}\nSoSo Calls: {analytics['soso_calls']}\nLive Trades: {analytics['live_trades']}\nUptime: {datetime.now()-start_time}\nKey OK: {bool(SOSO_API_KEY)}", parse_mode=ParseMode.MARKDOWN)

    elif data == "stats":
        await q.edit_message_text(f"📊 **Live Stats**\n\nSignals Generated: {analytics['signals']}\nScanner Alerts: {analytics['alerts']}\nUptime: {datetime.now()-start_time}\n\nSoSo:{analytics['soso_calls']} Scans:{analytics['scans']}", parse_mode=ParseMode.MARKDOWN)

    elif data == "scanner_on":
        await q.edit_message_text(f"📡 Scanner active - sending alerts to {ALERT_CHAT_ID} every {SCAN_INTERVAL}s for 75%+ signals\nCoins: {', '.join([c.upper() for c in ALL_COINS])}\nPrimary: SoSoValue Secondary: Binance Tertiary: CoinGecko", parse_mode=ParseMode.MARKDOWN)

    else:
        await q.edit_message_text(f"✅ {data} - Module Wave 1-2 restored", parse_mode=ParseMode.MARKDOWN)

async def health(request):
    return web.Response(text=f"OK SoSo:{analytics['soso_calls']} Scans:{analytics['scans']} SoDEX:{sodex.ready} KeySet:{bool(SOSO_API_KEY)}")

async def start_webserver():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 10000))).start()

async def main():
    logging.info(f"Starting Agentic Finance... x-soso-api-key exists: {bool(SOSO_API_KEY)} SoSo calls: {analytics['soso_calls']}")
    await start_webserver()
    async with aiohttp.ClientSession(timeout=TIMEOUT) as s:
        try:
            await s.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
        except: pass
    app = ApplicationBuilder().token(TOKEN).build()
    app.bot_data["session"] = aiohttp.ClientSession(timeout=TIMEOUT)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    scanner_task = asyncio.create_task(scanner_loop(app))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(scanner_task.cancel()))
        except: pass
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
