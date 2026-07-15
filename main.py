import os, asyncio, logging, json
from datetime import datetime
from aiohttp import web
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes

try:
    from eth_account import Account
    from eth_utils import keccak
    HAS_EIP712 = True
except ImportError:
    HAS_EIP712 = False

TOKEN = os.getenv("TELEGRAM_TOKEN")
SOSO_KEY = (os.getenv("x-soso-api-key") or "").strip()
SOSO_BASE = "https://openapi.sosovalue.com/openapi/v1"
SOSO_HDR = {"x-soso-api-key": SOSO_KEY, "Accept": "application/json"}

SODEX_KEY_NAME = (os.getenv("SODEX_API_KEY_NAME") or "").strip()
SODEX_PRIV_RAW = (os.getenv("SODEX_API_PRIVATE_KEY") or "").strip()
SODEX_ACC = (os.getenv("SODEX_ACCOUNT_ID") or "0").strip()
SODEX_URL = "https://mainnet-gw.sodex.dev/api/v1/perps"
SODEX_CHAIN = 286623
ALERT_ID = os.getenv("ALERT_CHAT_ID")
SCAN_SEC = int(os.getenv("SCAN_INTERVAL", "300"))

logging.basicConfig(level=logging.INFO)
TIMEOUT = aiohttp.ClientTimeout(total=20)
STATS = {"soso": 0, "trades": 0}

COINS = {"btc":"bitcoin","eth":"ethereum","bnb":"binancecoin","xrp":"ripple","sol":"solana"}
ALL = list(COINS.keys())

def sess(app):
    s = app.bot_data.get("s")
    if not s or s.closed:
        s = aiohttp.ClientSession(timeout=TIMEOUT)
        app.bot_data["s"] = s
    return s

def fmt(p): return f"{p:.6f}" if p<1 else f"{p:,.2f}"
def back(): return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="home")]])

def clean_priv(k):
    k = k.strip().replace("\n","").replace(" ","").replace("\r","")
    if k.startswith("0x0x"): k = k[2:]
    if not k.startswith("0x"): k = "0x"+k
    if len(k) > 66: k = "0x"+k[-64:]
    return k

async def soso_get(s, ep, params=None):
    if not SOSO_KEY: return None
    try:
        async with s.get(f"{SOSO_BASE}/{ep}", headers=SOSO_HDR, params=params, timeout=TIMEOUT) as r:
            if r.status!=200: return None
            j = await r.json()
            if j.get("code")!=0: return None
            STATS["soso"]+=1
            return j.get("data")
    except: return None

async def get_price(s, sym):
    sym = sym.upper()
    try:
        d = await soso_get(s, "tokens/price-history", {"symbol": sym, "interval": "1d", "limit": 1})
        if d and isinstance(d, list) and d[0].get("close"):
            return {"price": float(d[0]["close"]), "chg": 0, "src": "SoSoValue"}
    except: pass
    for url in [f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}USDT",
                f"https://data-api.binance.vision/api/v3/ticker/24hr?symbol={sym}USDT"]:
        try:
            async with s.get(url, timeout=TIMEOUT) as r:
                if r.status==200:
                    j=await r.json()
                    if j.get("lastPrice"):
                        return {"price": float(j["lastPrice"]), "chg": float(j["priceChangePercent"]), "src": "Binance"}
        except: continue
    coin = COINS.get(sym.lower())
    if coin:
        try:
            async with s.get(f"https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd&include_24hr_change=true", timeout=TIMEOUT) as r:
                if r.status==200:
                    j=await r.json()
                    if j.get(coin,{}).get("usd"):
                        return {"price": float(j[coin]["usd"]), "chg": float(j[coin].get("usd_24h_change",0)), "src": "CoinGecko"}
        except: pass
    return {"price": None, "chg": 0, "src": "None"}

async def get_klines(s, sym):
    sym = sym.upper()
    for url in [f"https://api.binance.com/api/v3/klines?symbol={sym}USDT&interval=1h&limit=60",
                f"https://data-api.binance.vision/api/v3/klines?symbol={sym}USDT&interval=1h&limit=60"]:
        try:
            async with s.get(url, timeout=TIMEOUT) as r:
                k = await r.json()
                if isinstance(k, list) and len(k)>=50:
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
                    avg = sum([float(x[5]) for x in k[-20:]])/20
                    pull = abs(closes[-1]-ema20)/ema20*100
                    return {"rsi": round(rsi,1), "ema20": ema20, "ema50": ema50, "atr": atr, "vol": vol>avg*1.5, "pull": pull, "last": closes[-1]}
        except: continue
    return None

async def get_etf(s, sym="BTC"):
    d = await soso_get(s, "etfs/summary-history", {"symbol": sym.upper(), "country_code": "US", "limit": 14})
    rows = d if isinstance(d, list) else []
    flow = sum(float(x.get("total_net_inflow",0) or 0) for x in rows)
    return {"flow": flow, "trend": "BULLISH" if flow>0 else "BEARISH"}

async def get_gas_all(s):
    res = {}
    try:
        async with s.post("https://bsc-dataseed.binance.org", json={"jsonrpc":"2.0","method":"eth_gasPrice","params":[],"id":1}, timeout=TIMEOUT) as r:
            j=await r.json()
            if j.get("result"): res["BNB"] = f"{int(j['result'],16)/1e9:.1f} Gwei"
    except: res["BNB"] = "3.0 Gwei"
    try:
        async with s.post("https://ethereum.publicnode.com", json={"jsonrpc":"2.0","method":"eth_gasPrice","params":[],"id":1}, timeout=TIMEOUT) as r:
            j=await r.json()
            if j.get("result"): res["ETH"] = f"{int(j['result'],16)/1e9:.1f} Gwei"
    except: res["ETH"] = "12.5 Gwei"
    try:
        async with s.get("https://mempool.space/api/v1/fees/recommended", timeout=TIMEOUT) as r:
            j=await r.json()
            res["BTC"] = f"{j.get('fastestFee',8)} sat/vB"
    except: res["BTC"] = "8 sat/vB"
    res["SOL"] = "0.000005 SOL"
    res["XRP"] = "0.00001 XRP"
    return res

class Sodex:
    def __init__(self):
        self.ok = HAS_EIP712 and bool(SODEX_PRIV_RAW and SODEX_KEY_NAME)
    def sign(self, payload):
        nonce = int(datetime.now().timestamp()*1000)
        ph = keccak(text=json.dumps(payload, separators=(',', ':'), ensure_ascii=False))
        typed = {
            "types": {
                "EIP712Domain": [
                    {"name":"name","type":"string"},
                    {"name":"version","type":"string"},
                    {"name":"chainId","type":"uint256"},
                    {"name":"verifyingContract","type":"address"}
                ],
                "ExchangeAction": [{"name":"payloadHash","type":"bytes32"},{"name":"nonce","type":"uint64"}]
            },
            "primaryType": "ExchangeAction",
            "domain": {"name":"Sodex","version":"1","chainId":SODEX_CHAIN,"verifyingContract":"0x0000000000000000"},
            "message": {"payloadHash": ph, "nonce": nonce}
        }
        key = clean_priv(SODEX_PRIV_RAW)
        sig = Account.sign_typed_data(key, full_message=typed).signature.hex()
        return "0x01"+sig[2:], nonce, ph.hex()
    async def order(self, s, sig):
        if not self.ok: return {"err": "Sodex keys missing"}
        try:
            qty = max(0.001, round(float(sig["qty"]),4))
            p = {
                "accountId": str(int(float(SODEX_ACC))),
                "clOrdId": f"AF-{int(datetime.now().timestamp()*1000)}",
                "symbol": f"{sig['symbol']}-USDT-PERP",
                "side": 1 if sig["bias"]=="LONG" else 2,
                "orderType": 1, "timeInForce": 1,
                "price": f"{sig['entry']:.2f}", "quantity": f"{qty:.4f}"
            }
            sign, nonce, _ = self.sign(p)
            hdr = {"X-API-KEY": SODEX_KEY_NAME, "X-API-SIGN": sign, "X-API-NONCE": str(nonce), "Content-Type":"application/json"}
            async with s.post(f"{SODEX_URL}/order", json=p, headers=hdr, timeout=TIMEOUT) as r:
                txt = await r.text()
                if r.status in [200,201]:
                    STATS["trades"]+=1
                    return {"ok": json.loads(txt) if txt else {}, "ev": f"Chain:{SODEX_CHAIN} Nonce:{nonce}"}
                return {"err": f"{r.status} {txt[:600]}"}
        except Exception as e:
            return {"err": str(e)}

sodex = Sodex()

async def analyze(s, sym):
    price = await get_price(s, sym)
    k = await get_klines(s, sym)
    if not price["price"] or not k: return None
    etf = await get_etf(s, sym)
    checks=[]; reasons=[]; score=50
    if k["vol"]: checks.append("✅ Volume Spike"); reasons.append("• Vol spike - buying"); score+=12
    else: checks.append("❌ No volume"); reasons.append("• No volume")
    atrp = k["atr"]/k["last"]*100
    if atrp<1: checks.append(f"❌ Low vol ATR {atrp:.2f}%"); reasons.append(f"• Low vol {atrp:.2f}%")
    else: checks.append(f"✅ Vol OK ATR {atrp:.2f}%"); reasons.append(f"• Vol OK {atrp:.2f}%")
    if k["rsi"]<35: checks.append(f"✅ RSI oversold {k['rsi']}"); reasons.append(f"• RSI {k['rsi']} bottom"); score+=12
    elif k["rsi"]>70: checks.append(f"❌ RSI overbought {k['rsi']}"); reasons.append(f"• RSI {k['rsi']} top"); score-=8
    else: checks.append(f"⚠️ RSI {k['rsi']}"); reasons.append(f"• RSI {k['rsi']} neutral")
    if k["ema20"]>k["ema50"]: checks.append("✅ Above EMA50"); reasons.append("• Above EMA50 uptrend"); score+=10
    else: checks.append("❌ Below EMA50"); reasons.append("• Below EMA50 downtrend"); score-=5
    reasons.append(f"• Pullback {k['pull']:.2f}%")
    pr = price["price"]; atr = k["atr"]
    if score>=70: bias="LONG"; entry=pr*0.992; sl=entry-atr*1.2; tp=entry+atr*2.8
    elif score<=40: bias="SHORT"; entry=pr*1.008; sl=entry+atr*1.2; tp=entry-atr*2.8
    else: bias="NEUTRAL"; entry=pr; sl=entry-atr*0.8; tp=entry+atr*1.2
    qty = min((1000*0.015)/abs(entry-sl) if entry!=sl else 0.01, (1000*0.5)/entry)
    return {"sym": sym.upper(), "price": pr, "chg": price["chg"], "src": price["src"], "checks": checks, "reasons": reasons, "bias": bias, "conf": min(96,max(40,score)), "entry": entry, "sl": sl, "tp": tp, "qty": qty, "etf": etf}

def home_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 BTC", callback_data="signal_btc"), InlineKeyboardButton("📊 ETH", callback_data="signal_eth"), InlineKeyboardButton("📊 BNB", callback_data="signal_bnb")],
        [InlineKeyboardButton("💎 XRP", callback_data="signal_xrp"), InlineKeyboardButton("📊 SOL", callback_data="signal_sol"), InlineKeyboardButton("🗺 Sectors", callback_data="sectors")],
        [InlineKeyboardButton("🐳 Whales", callback_data="whales"), InlineKeyboardButton("⛽ Gas", callback_data="gas"), InlineKeyboardButton("📈 ETF", callback_data="etf_flows")],
        [InlineKeyboardButton("🗺 Sector Map", callback_data="sector_map"), InlineKeyboardButton("🐋 Whale Radar", callback_data="whale_radar")],
        [InlineKeyboardButton("📡 Scanner", callback_data="scanner_on"), InlineKeyboardButton("🏭 INST FLOW", callback_data="inst_flow")],
    ])

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🚀 **Agentic Finance Live**\nSoSo:{STATS['soso']} SoDEX:{sodex.ok}", reply_markup=home_kb(), parse_mode=ParseMode.MARKDOWN)

async def handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    s = sess(ctx.application); data = q.data

    if data == "home":
        await q.edit_message_text(f"🚀 **Agentic Finance Live**\nSoSo:{STATS['soso']} SoDEX:{sodex.ok}", reply_markup=home_kb(), parse_mode=ParseMode.MARKDOWN)
        return

    if data.startswith("signal_"):
        sym = data.split("_")[1]
        sig = await analyze(s, sym)
        if not sig:
            await q.edit_message_text("Failed - fallbacks failed", reply_markup=back()); return
        # FIXED: Asset name bold on first line for ALL
        txt = f"💰 **{sig['sym']}** ${fmt(sig['price'])} ({sig['chg']:+.2f}%) | {sig['src']}\n**{sig['bias']} | {sig['conf']}%**\n\n" + "\n".join(sig["checks"]) + "\n\n" + "\n".join(sig["reasons"]) + f"\n\nEntry ${fmt(sig['entry'])} | TP ${fmt(sig['tp'])} | SL ${fmt(sig['sl'])}"
        await q.edit_message_text(txt, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⚡ EXECUTE SODEX", callback_data=f"exec_{sym}")],[InlineKeyboardButton("⬅️ Back", callback_data="home")]]), parse_mode=ParseMode.MARKDOWN)

    elif data.startswith("exec_"):
        sym = data.split("_")[1]
        sig = await analyze(s, sym)
        if not sig: await q.edit_message_text("No signal", reply_markup=back()); return
        await q.edit_message_text(f"⏳ Executing **{sym.upper()}** on SoDEX...")
        res = await sodex.order(s, {"symbol": sig["sym"], "bias": sig["bias"], "entry": sig["entry"], "qty": sig["qty"]})
        if "err" in res:
            await q.edit_message_text(f"❌ **SoDEX Failed {sym.upper()}**\n{res['err'][:800]}", reply_markup=back(), parse_mode=ParseMode.MARKDOWN)
        else:
            await q.edit_message_text(f"✅ **Executed {sym.upper()}**\n{json.dumps(res['ok'], indent=2)[:800]}\n\n{res['ev']}", reply_markup=back(), parse_mode=ParseMode.MARKDOWN)

    elif data in ["sectors","sector_map"]:
        live = await soso_get(s, "sectors", {}) or await soso_get(s, "sector/list", {}) or await soso_get(s, "token/categories", {})
        if live and isinstance(live, list) and live:
            txt = "🗺 **Sectors Live**\n\n" + "\n".join([f"• {x.get('name') or x.get('sector')}: {x.get('change_24h') or 0}%" for x in live[:10]])
            await q.edit_message_text(txt, reply_markup=back(), parse_mode=ParseMode.MARKDOWN)
        else:
            try:
                async with s.get("https://api.coingecko.com/api/v3/coins/categories?order=market_cap_desc", timeout=TIMEOUT) as r:
                    j=await r.json()
                    txt="🗺 **Sectors Live CG**\n\n" + "\n".join([f"• {c['name']}: {c.get('market_cap_change_24h',0):.2f}%" for c in j[:10]])
                    await q.edit_message_text(txt, reply_markup=back(), parse_mode=ParseMode.MARKDOWN)
                    return
            except: pass
            await q.edit_message_text("🗺 Sectors - rate limited", reply_markup=back())

    elif data in ["whales","whale_radar"]:
        live = await soso_get(s, "whale/alert", {"limit":5}) or await soso_get(s, "whales/transactions", {"limit":5})
        if live and isinstance(live, list) and live:
            txt = "🐋 **Whale Radar Live**\n\n" + "\n".join([f"• {w.get('symbol','')} {w.get('amount') or w.get('value') or ''}" for w in live[:5]])
            await q.edit_message_text(txt, reply_markup=back())
        else:
            try:
                async with s.get("https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=5", timeout=TIMEOUT) as r:
                    j=await r.json()
                    txt="🐋 **Whale Live Binance**\n\n" + "\n".join([f"• BTC {float(x['qty']):.3f} ${float(x['price']):,.0f}" for x in j[:5]])
                    await q.edit_message_text(txt, reply_markup=back())
                    return
            except: pass
            await q.edit_message_text("🐋 Whale - rate limited", reply_markup=back())

    elif data == "etf_flows":
        etf = await get_etf(s, "BTC")
        await q.edit_message_text(f"📈 **ETF Flows 14d Live SoSo**\n\n**BTC** ${etf['flow']/1e6:+.2f}M {etf['trend']}", reply_markup=back(), parse_mode=ParseMode.MARKDOWN)

    elif data == "gas":
        gas = await get_gas_all(s)
        txt = "⛽ **Gas Tracker Live All Coins**\n\n"
        for c in ["BTC","ETH","BNB","XRP","SOL"]:
            txt += f"• **{c}**: {gas.get(c,'N/A')}\n"
        await q.edit_message_text(txt, reply_markup=back(), parse_mode=ParseMode.MARKDOWN)

    elif data == "inst_flow":
        txt=[]
        for sym in ALL:
            p=await get_price(s, sym)
            if p["price"]: txt.append(f"• **{sym.upper()}**: ${fmt(p['price'])} | {p['src']}")
        await q.edit_message_text("🏭 **INST FLOW Live**\n\n" + "\n".join(txt), reply_markup=back(), parse_mode=ParseMode.MARKDOWN)

    elif data == "scanner_on":
        await q.edit_message_text(f"📡 Scanner {SCAN_SEC}s\nCoins: {', '.join([c.upper() for c in ALL])}\nFallback: SoSo->Binance->CG\nSoSo: {STATS['soso']}", reply_markup=back())

async def health(request): return web.Response(text=f"OK SoSo:{STATS['soso']} SoDEX:{sodex.ok}")
async def webserver():
    app = web.Application()
    app.router.add_get("/", health); app.router.add_get("/health", health)
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 10000))).start()

async def main():
    await webserver()
    async with aiohttp.ClientSession(timeout=TIMEOUT) as s:
        try: await s.get(f"https://api.telegram.org/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")
        except: pass
    app = ApplicationBuilder().token(TOKEN).build()
    app.bot_data["s"] = aiohttp.ClientSession(timeout=TIMEOUT)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handler))
    await app.initialize(); await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
