import json
import logging
import time
import aiohttp
from decimal import Decimal

SODEX_CHAIN_ID = 286623
SODEX_PERPS_URL = "https://mainnet-gw.sodex.dev/api/v1/perps"
SYMBOL_IDS = {}

_last_nonce = 0

def next_nonce() -> int:
    global _last_nonce
    now = int(time.time() * 1000)
    if now <= _last_nonce:
        now = _last_nonce + 1
    _last_nonce = now
    return now

async def load_symbols(session):
    global SYMBOL_IDS
    try:
        url = f"{SODEX_PERPS_URL}/markets/symbols"
        async with session.get(url) as r:
            txt = await r.text()
            if r.status!= 200:
                return
            j = json.loads(txt)
            data = j.get("data", j) if isinstance(j, dict) else j
            for item in data:
                if not isinstance(item, dict):
                    continue
                sid = item.get("symbolID") or item.get("id")
                sym = item.get("symbol") or item.get("displayName") or item.get("name")
                if sid and sym:
                    SYMBOL_IDS[sym.strip()] = int(sid)
            logging.info(f"[SoDEX] Loaded {len(SYMBOL_IDS)} symbols")
    except Exception:
        logging.exception("[SoDEX] load_symbols failed")

def find_symbol_id(symbol: str):
    sym = symbol.upper().strip()

    for cand in [f"{sym}-USD", f"{sym}-USDT", sym]:
        if cand in SYMBOL_IDS:
            return SYMBOL_IDS[cand], cand

    for k, v in SYMBOL_IDS.items():
        if k.upper().startswith(sym + "-"):
            return v, k

    return None, None

class SoDEXExecutor:
    def __init__(self, api_key_name: str, private_key: str, account_id: str):
        self.api_key_name = (api_key_name or "").strip()
        self.private_key_raw = (private_key or "").strip()
        self.account_id = (account_id or "0").strip()
        self.ready = bool(self.api_key_name and self.account_id)

    async def sign(self, session, payload):
        async with session.post(
            "https://agenticfinance-signer.onrender.com/sign-order",
            json=payload,
        ) as r:
            if r.status != 200:
                raise Exception(await r.text())
            result = await r.json()
            if not result.get("success"):
                raise Exception(result.get("error", "Signing failed"))
            return result["signature"]

    async def place_order(self, session, symbol: str, bias: str, entry: float, qty: float):
        if not self.ready:
            return {"err": "SoDEX not configured"}
        symbol_id, found_name = find_symbol_id(symbol)
        if symbol_id is None:
            return {"err": f"symbolID not found for {symbol}"}
        nonce = next_nonce()
        price_str = format(Decimal(str(entry)), "f").rstrip("0").rstrip(".")
        qty_str = format(Decimal(str(qty)), "f").rstrip("0").rstrip(".")

        order = {
            "clOrdID": f"AF-{nonce}",
            "modifier": 1,
            "side": 1 if bias == "LONG" else 2,
            "type": 1,
            "timeInForce": 1,
            "price": price_str,
            "quantity": qty_str,
            "reduceOnly": False,
            "positionSide": 1 if bias == "LONG" else 2
        }

        payload = {
            "accountID": int(float(self.account_id)),
            "symbolID": symbol_id,
            "orders": [order]
        }

        try:
            sign_payload = {
                "accountID": int(float(self.account_id)),
                "symbolID": symbol_id,
                "nonce": nonce,
                "side": "BUY" if bias == "LONG" else "SELL",
                "positionSide": "LONG" if bias == "LONG" else "SHORT",
                "price": price_str,
                "quantity": qty_str,
                "clOrdID": f"AF-{nonce}",
            }
            sig = await self.sign(session, sign_payload)
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-API-Sign": sig,
                "X-API-Nonce": str(nonce),
                "X-API-Chain": str(SODEX_CHAIN_ID),
            }
            if self.api_key_name:
                headers["X-API-Key"] = self.api_key_name

            async with session.post(f"{SODEX_PERPS_URL}/trade/orders", json=payload, headers=headers) as r:
                txt = await r.text()
                logging.info(f"[SoDEX] {found_name} ID={symbol_id} {r.status} {txt[:800]}")
                if r.status in [200, 201]:
                    try:
                        data = json.loads(txt) if txt else {}
                        if isinstance(data, dict) and data.get("error"):
                            return {"err": f"{data.get('error')}", "used_symbol": found_name, "raw": data}
                        return {"ok": data, "used_symbol": found_name}
                    except:
                        return {"ok": {"raw": txt}, "used_symbol": found_name}
                return {"err": f"{r.status} {txt[:800]}", "used_symbol": found_name}
        except Exception as e:
            logging.exception("[SoDEX] place_order failed")
            return {"err": str(e)}
