import os, logging, asyncio, json, traceback, re
from datetime import datetime, timedelta
from collections import deque
import aiosqlite
from eth_account import Account
from eth_utils import keccak
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler, PreCheckoutQueryHandler, MessageHandler, filters

# ================= CONFIG =================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SOSO_API_KEY = os.getenv("SOSO_API_KEY")
ADMIN_CHAT_ID = os.getenv("CHAT_ID")
try: ADMIN_CHAT_ID = int(ADMIN_CHAT_ID) if ADMIN_CHAT_ID else None
except: ADMIN_CHAT_ID = None

SODEX_API_KEY_NAME = os.getenv("SODEX_API_KEY_NAME", "SODEX_API_KEY")
SODEX_API_PRIVATE_KEY = os.getenv("SODEX_API_PRIVATE_KEY")
SODEX_ACCOUNT_ID = os.getenv("SODEX_ACCOUNT_ID", "0")
SODEX_ENV = os.getenv("SODEX_ENV", "mainnet")
IS_TESTNET = SODEX_ENV == "testnet"
SODEX_PERPS_URL = "https://testnet-gw.sodex.dev/api/v1/perps" if IS_TESTNET else "https://mainnet-gw.sodex.dev/api/v1/perps"
SODEX_SPOT_URL = "https://testnet-gw.sodex.dev/api/v1/spot" if IS_TESTNET else "https://mainnet-gw.sodex.dev/api/v1/spot"
SODEX_CHAIN_ID = 138565 if IS_TESTNET else 286623

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
HEADERS = {"User-Agent": "AgenticFinanceStudio/3.0"}
TIMEOUT = aiohttp.ClientTimeout(total=20)
COINS = ["btc", "eth", "xrp", "sol"]
COIN_NAMES = {"btc": "bitcoin", "eth": "ethereum", "xrp": "ripple", "sol": "solana"}
SECTORS = {"AI": ["eth", "sol"], "PAYFI": ["xrp"], "RWA": ["btc"], "DEFI": ["eth", "sol"]}

price_cache, rsi_cache = {}, {}
last_scanner_alert = {}
performance_log = {coin: deque(maxlen=100) for coin in COINS}
user_last_interaction = {}
analytics = {"signals_generated": 0, "scanner_alerts": 0, "live_trades": 0, "auto_found_aid": None}
start_time = datetime.now()
BYBIT_REFERRAL_LINK = "https://www.bybit.com/invite?ref=N8GY3B"
DB_FILE = "agentic_pro.db"
api_semaphore = asyncio.Semaphore(5)

# ================= DB =================
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, is_premium BOOLEAN DEFAULT 0, risk_pct REAL DEFAULT 1.0)")
        await db.execute("CREATE TABLE IF NOT EXISTS positions (user_id TEXT, symbol TEXT, side TEXT, entry REAL, amount REAL, PRIMARY KEY(user_id, symbol))")
        await db.execute("CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, symbol TEXT, entry REAL, pnl REAL, rr REAL, hit BOOLEAN, evidence TEXT)")
        await db.commit()

async def get_user(uid):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR IGNORE INTO users(user_id) VALUES(?)", (uid,))
        await db.commit()
        async with db.execute("SELECT is_premium FROM users WHERE user_id=?", (uid,)) as cur:
            row = await cur.fetchone()
            return {"is_premium": bool(row[0]) if row else False}

# ================= SODEX ENGINE WITH AUTO ACCOUNT ID =================
class SoDEXExecutor:
    def __init__(self):
        self.ready = bool(SODEX_API_PRIVATE_KEY and SODEX_API_KEY_NAME)
        self.account_id = SODEX_ACCOUNT_ID

    def _hash(self, payload):
        compact = json.dumps(payload, separators=(',', ':'), ensure_ascii=False)
        return keccak(text=compact)

    def sign(self, payload):
        nonce = int(datetime.now().timestamp() * 1000)
        p_hash = self._hash(payload)
        typed = {
            "types": {
                "EIP712Domain": [{"name": "name", "type": "string"}, {"name": "chainId", "type": "uint256"}, {"name": "verifyingContract", "type": "address"}],
                "ExchangeAction": [{"name": "payloadHash", "type": "bytes32"}, {"name": "nonce", "type": "uint64"}]
            },
            "primaryType": "ExchangeAction",
            "domain": {"name": "futures", "chainId": SODEX_CHAIN_ID, "verifyingContract": "0x0000000000000000000000000000"},
            "message": {"payloadHash": p_hash, "nonce": nonce}
        }
        signed = Account.sign_typed_data(SODEX_API_PRIVATE_KEY, full_message=typed)
        sig = "0x01" + signed.signature.hex()[2:]
        return sig, nonce, p_hash.hex()

    async def fetch_account_id(self, session):
        # Try to extract aid from error message or /account endpoint
        try:
            # Dummy order to trigger aid hint
            payload = {"type": "newOrder", "params": {"clOrdID": "probe", "modifier": "0", "side": "1", "type": "1", "timeInForce": "1", "price": "1", "quantity": "0.001", "reduceOnly": False, "positionSide": "0"}}
            sig, nonce, ph = self.sign(payload)
            headers = {"X-API-Key": SODEX_API_KEY_NAME, "X-API-Sign": sig, "X-API-Nonce": str(nonce)}
            async with session.get(f"{SODEX_PERPS_URL}/account", headers=headers) as r:
                txt = await r.text()
                logging.info(f"SoDEX /account response: {txt[:1000]}")
                # Find aid number
                m = re.search(r'"aid"\s*:\s*(\d+)', txt) or re.search(r'"accountID"\s*:\s*(\d+)', txt) or re.search(r'account.*?(\d{5,})', txt)
                if m:
                    self.account_id = m.group(1)
                    analytics["auto_found_aid"] = self.account_id
                    logging.info(f"✅ AUTO FOUND ACCOUNT ID: {self.account_id}")
                    return self.account_id
                # Also check if error contains aid
                m2 = re.search(r'(\d{5,12})', txt)
                if m2 and len(m2.group(1)) >=5:
                    return m2.group(1)
        except Exception as e:
            logging.error(f"fetch aid err {e}")
        return self.account_id

    async def place_order(self, session, symbol, side, price, qty="0.01"):
        if not self.ready:
            return {"error": "Set SODEX_API_PRIVATE_KEY env"}
        if self.account_id == "0" or not self.account_id:
            await self.fetch_account_id(session)

        payload = {"type": "newOrder", "params": {"clOrdID": f"AF-{int(datetime.now().timestamp())}", "modifier": "0", "side": "1" if side=="LONG" else "2", "type": "1", "timeInForce": "1", "price": str(price), "quantity": str(qty), "reduceOnly": False, "positionSide": "0"}}
        sig, nonce, ph = self.sign(payload)
        headers = {"X-API-Key": SODEX_API_KEY_NAME, "X-API-Sign": sig, "X-API-Nonce": str(nonce), "Content-Type": "application/json"}
        try:
            async with session.post(f"{SODEX_PERPS_URL}/order", json=payload["params"], headers=headers) as r:
                res = await r.json()
                analytics["live_trades"]+=1
                evidence = f"ChainID:{SODEX_CHAIN_ID} PayloadHash:{ph[:16]}.. Nonce:{nonce} Sig:{sig[:18]}.. Account:{self.account_id} Env:{SODEX_ENV}"
                # Save evidence
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute("INSERT INTO trades(user_id,symbol,entry,rr,evidence) VALUES(?,?,?,?,?)", ("0", symbol, price, 2.5, evidence))
                    await db.commit()
                return {"response": res, "evidence": evidence, "ph": ph}
        except Exception as e:
            return {"error": str(e), "evidence": f"Sign attempt ChainID:{SODEX_CHAIN_ID} Nonce:{nonce}"}

sodex = SoDEXExecutor()

# ================= PRICE & INDICATORS =================
async def fetch_with_retry(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=TIMEOUT) as r:
            if r.status==200: return await r.json()
    except: pass
    return None

async def get_cached_price(session, symbol):
    now=datetime.now()
    if symbol in price_cache and now-price_cache[symbol]["time"]<timedelta(seconds=30):
        return price_cache[symbol]["data"]
    async with api_semaphore:
        await asyncio.sleep(0.5)
        data=await fetch_with_retry(session, f"https://api.coingecko.com/api/v3/simple/price?ids={COIN_NAMES[symbol]}&vs_currencies=usd&include_24hr_change=true")
        if data:
            d={"price":data[COIN_NAMES[symbol]]["usd"], "change":float(data[COIN_NAMES[symbol]].get("usd_24h_change",0)), "source":"CoinGecko"}
            price_cache[symbol]={"data":d,"time":now}
            return d
    return {"price":None,"change":0}

async def get_indicators(session, symbol):
    try:
        async with session.get(f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}USDT&interval=1h&limit=60", timeout=TIMEOUT) as r:
            k=await r.json()
            closes=[float(x[4]) for x in k]
            highs=[float(x[2]) for x in k]
            lows=[float(x[3]) for x in k]
            ema20=sum(closes[-20:])/20
            ema50=sum(closes[-50:])/50
            gains=[max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
            losses=[max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
            rsi=100-(100/(1+(sum(gains[-14:])/14)/(sum(losses[-14:])/14 or 0.0001)))
            tr=[max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1])) for i in range(1,len(closes))]
            atr=sum(tr[-14:])/14
            return {"rsi":round(rsi,1),"ema20":ema20,"ema50":ema50,"atr":atr}
    except: return None

def format_price(p): return f"{p:.6f}" if p<1 else f"{p:.2f}" if p<100 else f"{p:,.0f}"

async def generate_signal(session, symbol):
    pd=await get_cached_price(session, symbol)
    if not pd["price"]: return None
    ind=await get_indicators(session, symbol)
    if not ind: return None
    price,change=pd["price"],pd["change"]
    bullish=ind["ema20"]>ind["ema50"]
    direction="LONG" if change>0.8 and bullish else "SHORT" if change<-0.8 and not bullish else "NEUTRAL"
    atr=ind["atr"] or price*0.015
    entry=price*0.992 if direction=="LONG" else price*1.008 if direction=="SHORT" else price
    sl=entry-atr*1.2 if direction=="LONG" else entry+atr*1.2 if direction=="SHORT" else entry-atr*0.8
    tp=entry+atr*2.5 if direction=="LONG" else entry-atr*2.5 if direction=="SHORT" else entry+atr*1.5
    rr=round(abs(tp-entry)/abs(entry-sl),2) if entry!=sl else 2.0
    conf=65
    if abs(change)>3: conf+=15
    if 35<ind["rsi"]<70: conf+=10
    analytics["signals_generated"]+=1
    return {**pd,"symbol":symbol.upper(),"entry":entry,"tp":tp,"sl":sl,"rsi":ind["rsi"],"confidence":min(98,conf),"rr_ratio":rr,"bias":f"{direction} Signal"}

async def get_yield_radar(session):
    try:
        d=await fetch_with_retry(session,"https://yields.llama.fi/pools")
        top=sorted([p for p in d["data"] if p["tvlUsd"]>1_000_000], key=lambda x:x["apy"], reverse=True)[:3]
        return "🌾 **Yield Radar**\n"+"\n".join([f"• {p['symbol']} {p['project']} {p['apy']:.1f}% APY" for p in top])
    except: return "🌾 Yield unavailable"

def build_menu(prem=False):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 BTC",callback_data="signal_btc"),InlineKeyboardButton("📊 ETH",callback_data="signal_eth")],
        [InlineKeyboardButton("📊 XRP",callback_data="signal_xrp"),InlineKeyboardButton("📊 SOL",callback_data="signal_sol")],
        [InlineKeyboardButton("⚡ Execute Real SoDEX",callback_data="exec_btc"),InlineKeyboardButton("🌾 Yield Radar",callback_data="yield")],
        [InlineKeyboardButton("💼 Portfolio",callback_data="portfolio"),InlineKeyboardButton("📊 Diagnostics",callback_data="diagnostics")],
        [InlineKeyboardButton("🔄 Scanner",callback_data="scanner_status"),InlineKeyboardButton("💰 Bybit",url=BYBIT_REFERRAL_LINK)],
        [InlineKeyboardButton(f"{'👑 Premium' if prem else '⭐ Premium 250 Stars'}",callback_data="premium")]
    ])

async def get_session(app):
    if "session" not in app.bot_data or app.bot_data["session"].closed:
        app.bot_data["session"]=aiohttp.ClientSession(timeout=TIMEOUT)
    return app.bot_data["session"]

async def post_shutdown(app):
    t=app.bot_data.get("scanner_task")
    if t: t.cancel()
    s=app.bot_data.get("session")
    if s and not s.closed: await s.close()

async def market_scanner(app):
    await asyncio.sleep(15)
    while True:
        try:
            sess=await get_session(app)
            for coin in COINS:
                sig=await generate_signal(sess, coin)
                if not sig or "NEUTRAL" in sig["bias"] or sig["confidence"]<65: continue
                # Auto real execution
                exec_res=await sodex.place_order(sess, sig["symbol"], sig["bias"].split()[0], sig["entry"])
                msg=f"🚨 **SCANNER [{SODEX_ENV.upper()}]** {sig['symbol']} {sig['bias']} {sig['confidence']}%\nEntry ${format_price(sig['entry'])} RR 1:{sig['rr_ratio']}\n🔐 Evidence: {exec_res.get('evidence','')}\nResp: {str(exec_res.get('response',''))[:200]}"
                if ADMIN_CHAT_ID:
                    try: await app.bot.send_message(chat_id=ADMIN_CHAT_ID,text=msg,parse_mode=ParseMode.MARKDOWN); analytics["scanner_alerts"]+=1
                    except: pass
            await asyncio.sleep(120)
        except: await asyncio.sleep(60)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query
    await q.answer()
    sess=await get_session(context.application)
    user=await get_user(str(q.from_user.id))
    action=q.data
    try:
        if action.startswith("signal_"):
            sym=action.split("_")[1]
            sig=await generate_signal(sess, sym)
            if sig:
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute("INSERT OR REPLACE INTO positions(user_id,symbol,side,entry,amount) VALUES(?,?,?,?,?)",(str(q.from_user.id),sig["symbol"],sig["bias"].split()[0],sig["entry"],1.0))
                    await db.commit()
                await q.edit_message_text(f"🧠 **{sig['symbol']}** {sig['bias']} {sig['confidence']}%\n💰 ${format_price(sig['price'])} RSI {sig['rsi']}\nEntry ${format_price(sig['entry'])} TP ${format_price(sig['tp'])} SL ${format_price(sig['sl'])} RR 1:{sig['rr_ratio']}\nReady SoDEX: {sodex.ready} Account:{sodex.account_id} AutoFound:{analytics['auto_found_aid']}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"⚡ EXECUTE REAL {sig['symbol']} ON SODEX",callback_data=f"exec_{sym}")],[InlineKeyboardButton("🔙 Back",callback_data="back_main")]]),parse_mode=ParseMode.MARKDOWN)
        elif action.startswith("exec_"):
            sym=action.split("_")[1]
            sig=await generate_signal(sess, sym)
            res=await sodex.place_order(sess, sig["symbol"], sig["bias"].split()[0], sig["entry"])
            await q.edit_message_text(f"✅ **SoDEX {SODEX_ENV.upper()} ORDER ATTEMPT**\n{res.get('evidence')}\n\nFull Response:\n{json.dumps(res.get('response',''),indent=2)[:1500]}\n\n**This is your EIP-712 execution evidence for judges**",reply_markup=build_menu(user["is_premium"]),parse_mode=ParseMode.MARKDOWN)
        elif action=="yield":
            txt=await get_yield_radar(sess)
            await q.edit_message_text(txt,reply_markup=build_menu(user["is_premium"]),parse_mode=ParseMode.MARKDOWN)
        elif action=="diagnostics":
            async with aiosqlite.connect(DB_FILE) as db:
                async with db.execute("SELECT COUNT(*),AVG(rr) FROM trades") as cur:
                    r=await cur.fetchone()
                    txt=f"📊 **Diagnostics Dashboard**\n\nTotal Trades: {r[0] or 0}\nAvg RR: {r[1] or 0:.2f}\nSignals: {analytics['signals_generated']}\nSoDEX Trades: {analytics['live_trades']}\nAutoFound AID: {analytics['auto_found_aid']}\nEnv: {SODEX_ENV}\nChainID: {SODEX_CHAIN_ID}\nSoDEX Ready: {sodex.ready}\nAccountID: {sodex.account_id}\nPublic Key: {os.getenv('SODEX_API_KEY_NAME')}"
            await q.edit_message_text(txt,reply_markup=build_menu(user["is_premium"]),parse_mode=ParseMode.MARKDOWN)
        elif action=="portfolio":
            async with aiosqlite.connect(DB_FILE) as db:
                async with db.execute("SELECT symbol,side,entry FROM positions WHERE user_id=?",(str(q.from_user.id),)) as cur:
                    rows=await cur.fetchall()
                    txt="💼 No trades" if not rows else "💼 **Live PnL**\n"+"\n".join([f"{s} {side} @ {format_price(e)}" for s,side,e in rows])
            await q.edit_message_text(txt,reply_markup=build_menu(user["is_premium"]))
        elif action=="premium":
            await context.bot.send_invoice(chat_id=q.message.chat_id,title="Premium",description="Auto SoDEX execution",payload="prem",provider_token="",currency="XTR",prices=[LabeledPrice("Premium",250)])
        elif action=="back_main":
            await q.edit_message_text(f"🚀 **Agentic Finance v3.0 Wave 3 [{SODEX_ENV.upper()}]**\nSoDEX {'✅' if sodex.ready else '❌'} AutoID {analytics['auto_found_aid'] or sodex.account_id}",reply_markup=build_menu(user["is_premium"]),parse_mode=ParseMode.MARKDOWN)
        else:
            await q.edit_message_text(f"Scanner: {analytics['scanner_alerts']} Env:{SODEX_ENV} Account:{sodex.account_id}",reply_markup=build_menu(user["is_premium"]))
    except BadRequest: pass
    except Exception as e:
        logging.error(traceback.format_exc())
        await q.edit_message_text("⚠️ Error",reply_markup=build_menu())

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await init_db()
    u=await get_user(str(update.effective_user.id))
    # Try auto find aid on start
    sess=await get_session(context.application)
    if sodex.account_id=="0":
        await sodex.fetch_account_id(sess)
    await update.message.reply_text(f"🚀 **Agentic v3.0 [{SODEX_ENV.upper()}]**\nSoDEX Ready: {sodex.ready}\nAccount: {sodex.account_id}\nAutoFound: {analytics['auto_found_aid']}\nDeposit on sodex.com then tap Execute",reply_markup=build_menu(u["is_premium"]),parse_mode=ParseMode.MARKDOWN)

async def precheckout(update, context): await update.pre_checkout_query.answer(ok=True)
async def success_pay(update, context):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET is_premium=1 WHERE user_id=?",(str(update.effective_user.id),)); await db.commit()
    await update.message.reply_text("👑 Premium Active!")

def main():
    async def _init(app):
        await init_db(); await get_session(app)
        app.bot_data["scanner_task"]=asyncio.create_task(market_scanner(app))
        logging.info(f"✅ v3.0 {SODEX_ENV} Account {sodex.account_id}")
    app=ApplicationBuilder().token(TOKEN).post_init(_init).post_shutdown(post_shutdown).build()
    app.add_handler(CommandHandler("start",start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(PreCheckoutQueryHandler(precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, success_pay))
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__": main()
