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
SOSO_API_KEY = (os.getenv("x-soso-api-key") or os.getenv("SOSO_API_KEY") or "").strip()
SOSO_BASE = "https://openapi.sosovalue.com/openapi/v1"
SOSO_HEADERS = {"x-soso-api-key": SOSO_API_KEY, "Accept": "application/json"}

SODEX_API_KEY_NAME = os.getenv("SODEX_API_KEY_NAME")
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
start_time = datetime.now()

COIN_NAMES = {"btc":"bitcoin","eth":"ethereum","bnb":"binancecoin","xrp":"ripple","sol":"solana"}
ALL_COINS = ["btc","eth","bnb","xrp","sol"]

def get_session(app):
    sess = app.bot_data.get("session")
    if not sess or sess.closed:
        sess = aiohttp.ClientSession(timeout=TIMEOUT)
        app.bot_data["session"] = sess
    return sess

def back_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="home")]])

def fmt(p):
    return f"{p:.6f}" if p<1 else f"{p:.2f}" if p<100 else f"{p:,.2f}"

async def soso_get(session, endpoint, params=None):
    if not SOSO_API_KEY: return None
    url = f"{SOSO_BASE}/{endpoint}"
    try:
        async with session.get(url, headers=SOSO_HEADERS, params=params, timeout=TIMEOUT) as r:
            if r.status!= 200:
                logging.warning(f"SoSo {endpoint} {r.status}")
                return None
            res = await r.json()
            if res.get("code")!= 0:
                logging.warning(f"SoSo {endpoint} code {res.get('code')} msg {res.get('msg')}")
                return None
            analytics["soso_calls"] += 1
            return res.get("data")
    except Exception as e:
        logging.warning(f"SoSo {endpoint} err {e}")
        return None

async def get_price(session, symbol):
    sym = symbol.upper()
    # 1. PRIMARY SoSoValue
    try:
        data = await soso_get(session, "tokens/price-history", {"symbol": sym, "interval": "1d", "limit": 1})
        if data and isinstance(data, list) and len(data)>0 and data[0].get("close"):
            return {"price": float(data[0]["close"]), "change": float(data[0].get("change",0) or 0), "source": "SoSoValue"}
    except: pass
    # 2. SECONDARY Binance
    for url in [f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}USDT", f"https://data-api.binance.vision/api/v3/ticker/24hr?symbol={sym}USDT"]:
        try:
            async with session.get(url, timeout=TIMEOUT) as r:
                if r.status == 200:
                    j = await r.json()
                    if j.get("lastPrice"):
                        return {"price": float(j["lastPrice"]), "change": float(j.get("priceChangePercent",0)), "source": "Binance"}
        except: continue
    # 3. TERTIARY CoinGecko
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
                    highs = [float(x[2]) for x in k]
                    lows = [float(x[3]) for x in k]
                    tr = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1,len(closes))]
                    atr = sum(tr[-14:])/14
                    vol = sum([float(x[5]) for x in k[-5:]])
                    avg_vol = sum([float(x[5]) for x in k[-20:]])/20
                    pullback = abs(closes[-1]-ema20)/ema20*100
                    return {"rsi": round(rsi,1), "ema20": ema20, "ema50": ema50, "atr": atr, "vol_spike": vol > avg_vol*1.5, "pullback": pullback, "price": closes[-1]}
        except: continue
    return None

async def get_etf_flow(session, symbol="BTC"):
    etf = await soso_get(session, "etfs/summary-history", {"symbol": symbol.upper(), "country_code": "US", "limit": 14})
    rows = etf if isinstance(etf, list) else []
    flow = sum(float(r.get("total_net_inflow", 0) or 0) for r in rows)
    return {"flow": flow, "trend": "BULLISH" if flow>0 else "BEARISH"}

class SoDEXExecutor:
    def __init__(self):
        self.ready = HAS_EIP712 and bool(SODEX_API_PRIVATE_KEY and SODEX_API_KEY_NAME)
        logging.info(f"SoDEX ready:{self.ready} HAS_EIP712:{HAS_EIP712} KeyName:{bool(SODEX_API_KEY_NAME)} Priv:{bool(SODEX_API_PRIVATE_KEY)} Chain:{SODEX_CHAIN_ID}")
    def _hash(self, p):
        return keccak(text=json.dumps(p, separators=(',', ':'), ensure_ascii=False))
    def sign(self, payload):
        nonce = int(datetime.now().timestamp()*1000)
        p_hash = self._hash(payload)
        # FIXED DOMAIN - Sodex spec
        typed = {
            "types": {
                "EIP712Domain": [{"name":"name","type":"string"},{"name":"chainId","type":"uint256"},{"name":"verifyingContract","type":"address"}],
                "ExchangeAction": [{"name":"payloadHash","type":"bytes32"},{"name":"nonce","type":"uint64"}]
            },
            "primaryType": "ExchangeAction",
            "domain": {"name":"Sodex","chainId":SODEX_CHAIN_ID,"verifyingContract":"0x0000000000000000000000000"},
            "message": {"payloadHash":p_hash,"nonce":nonce}
        }
        key = SODEX_API_PRIVATE_KEY if SODEX_API_PRIVATE_KEY.startswith("0x") else "0x"+SODEX_API_PRIVATE_KEY
        signed = Account.sign_typed_data(key, full_message=typed)
        sig = "0x01" + signed.signature.hex()[2:]
        return sig, nonce, p_hash.hex()
    async def place_order(self, session, sig_data):
        if not self.ready:
            return {"error": f"SoDEX not configured HAS_EIP712={HAS_EIP712} API_KEY_NAME={bool(SODEX_API_KEY_NAME)} PRIV={bool(SODEX_API_PRIVATE_KEY)}"}
        try:
            qty = round(float(sig_data["qty"]), 4)
            if qty <=0: qty = 0.01
            params = {
                "accountId": int(SODEX_ACCOUNT_ID),
                "clOrdId": f"AF-{int(datetime.now().timestamp()*1000)}",
                "symbol": f"{sig_data['symbol']}-USDT-PERP",
                "side": 1 if sig_data["bias"]=="LONG" else 2,
                "orderType": 1,
                "timeInForce": 1,
                "price": str(sig_data["entry"]),
                "quantity": str(qty)
            }
            sig, nonce, ph = self.sign(params)
            headers = {"X-API-KEY": SODEX_API_KEY_NAME, "X-API-SIGN": sig, "X-API-NONCE": str(nonce), "Content-Type": "application/json"}
            logging.info(f"SoDEX placing {params['symbol']} {params['side']} qty {qty} nonce {nonce}")
            async with session.post(f"{SODEX_PERPS_URL}/order", json=params, headers=headers, timeout=TIMEOUT) as r:
                txt = await r.text()
                try: res = json.loads(txt)
                except: res = {"raw": txt, "status": r.status}
                logging.info(f"SoDEX resp {r.status} {txt[:500]}")
                if r.status in [200,201]:
                    analytics["live_trades"]+=1
                    return {"response": res, "evidence": f"Chain:{SODEX_CHAIN_ID} Nonce:{nonce} PH:{ph[:10]} SoSo:{analytics['soso_calls']}"}
                else:
                    return {"error": f"{r.status} {txt[:800]}", "evidence": f"Chain:{SODEX_CHAIN_ID} Nonce:{nonce}"}
        except Exception as e:
            logging.exception("SoDEX exec")
            return {"error": str(e), "evidence": "sign error"}

sodex = SoDEXExecutor()

async def intelligence_engine(session, symbol):
    price_info = await get_price(session, symbol)
    ind = await get_indicators(session, symbol)
    if not price_info["price"] or not ind: return None
    etf = await get_etf_flow(session, symbol)
    checks=[]; reasons=[]; score=50
    if ind["vol_spike"]:
        checks.append("✅ Volume Spike"); reasons.append("• Volume spike confirms buying interest"); score+=12
    else:
        checks.append("❌ No volume confirmation"); reasons.append("• No significant volume")
    atr_pct = ind["atr"]/ind["price"]*100
    if atr_pct < 1:
        checks.append(f"❌ Low volatility (ATR {atr_pct:.2f}%)"); reasons.append(f"• Low volatility ATR {atr_pct:.2f}%")
    else:
        checks.append(f"✅ Volatility OK (ATR {atr_pct:.2f}%)"); reasons.append(f"• Volatility ATR {atr_pct:.2f}%")
    if ind["rsi"] < 35:
        checks.append(f"✅ RSI oversold {ind['rsi']}"); reasons.append(f"• RSI {ind['rsi']} oversold indicates potential bottom"); score+=12
    elif ind["rsi"] > 70:
        checks.append(f"❌ RSI overbought {ind['rsi']}"); reasons.append(f"• RSI {ind['rsi']} overbought suggests potential top"); score-=8
    else:
        checks.append(f"⚠️ RSI {ind['rsi']}"); reasons.append(f"• RSI {ind['rsi']} neutral")
    if ind["ema20"] > ind["ema50"]:
        checks.append("✅ Above EMA50"); reasons.append("• Price above EMA50 - uptrend"); score+=10
    else:
        checks.append("❌ Below EMA50"); reasons.append("• Price below EMA50 - downtrend"); score-=5
    reasons.append(f"• Pullback {ind['pullback']:.2f}%")
    price = price_info["price"]; atr = ind["atr"]
    if score>=70: bias="LONG"; entry=price*0.992; sl=entry-atr*1.2; tp=entry+atr*2.8
    elif score<=40: bias="SHORT"; entry=price*1.008; sl=entry+atr*1.2; tp=entry-atr*2.8
    else: bias="NEUTRAL"; entry=price; sl=entry-atr*0.8; tp=entry+atr*1.2
    qty = min((1000*0.015)/abs(entry-sl) if entry!=sl else 0.01, (1000*0.5)/entry)
    return {"symbol":symbol.upper(),"price":price,"change":price_info["change"],"entry":entry,"sl":sl,"tp":tp,"confidence":min(96,max(40,score)),"bias":bias,"checks":checks,"reasons":reasons,"etf":etf,"qty":qty,"source":price_info["source"]}

async def scanner_loop(app):
    while True:
        try:
            session = get_session(app); analytics["scans"]+=1
            for sym in ALL_COINS:
                sig = await intelligence_engine(session, sym)
                if sig and sig["confidence"] >= 75 and sig["bias"]!="NEUTRAL" and ALERT_CHAT_ID:
                    txt = f"🚨 {sig['symbol']} {sig['bias']} | {sig['confidence']}%\n💰 ${fmt(sig['price'])} [{sig['source']}]\n\n" + "\n".join(sig['checks'])
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⚡ EXECUTE", callback_data=f"exec_{sym}")]])
                    try: await app.bot.send_message(chat_id=ALERT_CHAT_ID, text=txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN); analytics["live_trades"]+=0
                    except: pass
            await asyncio.sleep(SCAN_INTERVAL)
        except asyncio.CancelledError: break
        except: await asyncio.sleep(60)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("📊 BTC", callback_data="signal_btc"), InlineKeyboardButton("📊 ETH", callback_data="signal_eth"), InlineKeyboardButton("📊 BNB", callback_data="signal_bnb")],
        [InlineKeyboardButton("💎 XRP", callback_data="signal_xrp"), InlineKeyboardButton("📊 SOL", callback_data="signal_sol"), InlineKeyboardButton("🗺 Sectors", callback_data="sectors")],
        [InlineKeyboardButton("🐳 Whales", callback_data="whales"), InlineKeyboardButton("⛽ Gas", callback_data="gas"), InlineKeyboardButton("📈 ETF", callback_data="etf_flows")],
        [InlineKeyboardButton("🗺 Sector Map", callback_data="sector_map"), InlineKeyboardButton("🐋 Whale Radar", callback_data="whale_radar")],
        [InlineKeyboardButton("📡 Scanner", callback_data="scanner_on"), InlineKeyboardButton("🏭 INST FLOW", callback_data="inst_flow")],
    ]
    await update.message.reply_text(f"🚀 **Agentic Finance**\nSoSo:{analytics['soso_calls']} SoDEX:{sodex.ready} EIP712:{HAS_EIP712}\nKey:{bool(SOSO_API_KEY)}", reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    session = get_session(context.application); data = q.data

    if data == "home":
        kb = [
            [InlineKeyboardButton("📊 BTC", callback_data="signal_btc"), InlineKeyboardButton("📊 ETH", callback_data="signal_eth"), InlineKeyboardButton("📊 BNB", callback_data="signal_bnb")],
            [InlineKeyboardButton("💎 XRP", callback_data="signal_xrp"), InlineKeyboardButton("📊 SOL", callback_data="signal_sol"), InlineKeyboardButton("🗺 Sectors", callback_data="sectors")],
            [InlineKeyboardButton("🐳 Whales", callback_data="whales"), InlineKeyboardButton("⛽ Gas", callback_data="gas"), InlineKeyboardButton("📈 ETF", callback_data="etf_flows")],
            [InlineKeyboardButton("🗺 Sector Map", callback_data="sector_map"), InlineKeyboardButton("🐋 Whale Radar", callback_data="whale_radar")],
            [InlineKeyboardButton("📡 Scanner", callback_data="scanner_on"), InlineKeyboardButton("🏭 INST FLOW", callback_data="inst_flow")],
        ]
        await q.edit_message_text(f"🚀 **Agentic Finance**\nSoSo:{analytics['soso_calls']} SoDEX:{sodex.ready} EIP712:{HAS_EIP712}", reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
        return

    if data.startswith("signal_"):
        sym = data.split("_")[1]
        sig = await intelligence_engine(session, sym)
        if not sig:
            await q.edit_message_text("Failed - all 3 fallbacks failed", reply_markup=back_kb())
            return
        txt = f"💰 ${fmt(sig['price'])} ({sig['change']:+.2f}%) | {sig['source']}\n\n" + "\n".join(sig['checks']) + "\n\n" + "\n".join(sig['reasons']) + f"\n\nEntry ${fmt(sig['entry'])} | TP ${fmt(sig['tp'])} | SL ${fmt(sig['sl'])}"
        await q.edit_message_text(txt, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚡ EXECUTE SODEX", callback_data=f"exec_{sym}")],[InlineKeyboardButton("⬅️ Back", callback_data="home")]]), parse_mode=ParseMode.MARKDOWN)

    elif data.startswith("exec_"):
        sym = data.split("_")[1]
        sig = await intelligence_engine(session, sym)
        if not sig:
            await q.edit_message_text("No signal", reply_markup=back_kb()); return
        await q.edit_message_text(f"⏳ Executing {sym.upper()} on SoDEX EIP712...")
        res = await sodex.place_order(session, sig)
        if "error" in res:
            await q.edit_message_text(f"❌ **SoDEX Failed**\n{res['error'][:800]}\n\n{res.get('evidence','')}", reply_markup=back_kb(), parse_mode=ParseMode.MARKDOWN)
        else:
            await q.edit_message_text(f"✅ **Executed**\n{json.dumps(res['response'], indent=2)[:800]}\n\n{res['evidence']}", reply_markup=back_kb(), parse_mode=ParseMode.MARKDOWN)

    elif data == "sectors" or data == "sector_map":
        sectors = await soso_get(session, "sector/market", {})
        if sectors and isinstance(sectors, list):
            txt = "🗺 **Sectors (SoSoValue)**\n\n"
            for s in sectors[:8]:
                txt += f"{s.get('sector','')} {s.get('change_24h','')}\n"
            await q.edit_message_text(txt, reply_markup=back_kb(), parse_mode=ParseMode.MARKDOWN)
        else:
            await q.edit_message_text("🗺 Sectors - no data", reply_markup=back_kb())

    elif data in ["whales","whale_radar"]:
        whales = await soso_get(session, "whales/transactions", {"limit":5})
        if whales and isinstance(whales, list):
            txt = "🐋 **Whale Radar (SoSoValue)**\n\n"
            for w in whales[:5]:
                txt += f"{w.get('symbol','')} {w.get('amount','')}\n"
            await q.edit_message_text(txt, reply_markup=back_kb())
        else:
            await q.edit_message_text("🐋 No whale data", reply_markup=back_kb())

    elif data == "etf_flows":
        etf = await get_etf_flow(session, "BTC")
        await q.edit_message_text(f"📈 **ETF Flows 14d**\nBTC ${etf['flow']/1e6:+.2f}M {etf['trend']}\nSoSoValue", reply_markup=back_kb(), parse_mode=ParseMode.MARKDOWN)

    elif data == "gas":
        try:
            async with session.get("https://api.bscscan.com/api?module=gastracker&action=gasoracle", timeout=TIMEOUT) as r:
                j = await r.json()
                price = j.get("result", {}).get("FastGasPrice", "N/A")
                await q.edit_message_text(f"⛽ **BNB Gas** {price} Gwei (BscScan)", reply_markup=back_kb(), parse_mode=ParseMode.MARKDOWN)
        except:
            await q.edit_message_text("⛽ Gas failed", reply_markup=back_kb())

    elif data == "inst_flow":
        texts=[]
        for s in ALL_COINS:
            p=await get_price(session, s)
            if p["price"]: texts.append(f"{s.upper()}: ${fmt(p['price'])} | {p['source']}")
        await q.edit_message_text("🏭 **INST FLOW**\n\n" + "\n".join(texts), reply_markup=back_kb(), parse_mode=ParseMode.MARKDOWN)

    elif data == "scanner_on":
        await q.edit_message_text(f"📡 Scanner active {SCAN_INTERVAL}s Alerts to {ALERT_CHAT_ID}\nCoins {', '.join([c.upper() for c in ALL_COINS])}\nFallback: SoSo->Binance->CoinGecko", reply_markup=back_kb())

    else:
        await q.edit_message_text(f"{data}", reply_markup=back_kb())

async def health(request): return web.Response(text=f"OK SoSo:{analytics['soso_calls']} SoDEX:{sodex.ready} EIP712:{HAS_EIP712}")
async def start_webserver():
    app = web.Application()
    app.router.add_get("/", health); app.router.add_get("/health", health)
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 10000))).start()

async def main():
    logging.info(f"Start KeyExists:{bool(SOSO_API_KEY)} SoDEX:{sodex.ready} EIP712:{HAS_EIP712}")
    await start_webserver()
    async with aiohttp.ClientSession(timeout=TIMEOUT) as s:
        try: await s.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
        except: pass
    app = ApplicationBuilder().token(TOKEN).build()
    app.bot_data["session"] = aiohttp.ClientSession(timeout=TIMEOUT)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    await app.initialize(); await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    asyncio.create_task(scanner_loop(app))
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
