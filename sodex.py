import json
import logging
from datetime import datetime

try:
    from eth_account import Account
    from eth_utils import keccak
    from hexbytes import HexBytes
    HAS_EIP712 = True
except ImportError:
    HAS_EIP712 = False

SODEX_CHAIN_ID = 286623
SODEX_PERPS_URL = "https://mainnet-gw.sodex.dev/api/v1/perps"
SYMBOL_IDS = {}

def clean_priv(k: str) -> str:
    k = k.strip().replace("\n","").replace(" ","").replace("\r","")
    if k.startswith("0x0x"): k = k[2:]
    if not k.startswith("0x"): k = "0x"+k
    if len(k)!= 66:
        raise ValueError(f"Invalid private key length {len(k)} expected 66")
    return k

async def load_symbols(session):
    global SYMBOL_IDS
    try:
        async with session.get(f"{SODEX_PERPS_URL}/symbols") as r:
            if r.status == 200:
                j = await r.json()
                data = j if isinstance(j, list) else j.get("data", [])
                for item in data:
                    sid = item.get("symbolID") or item.get("id") or item.get("symbolId")
                    sym = item.get("symbol") or item.get("symbolName") or item.get("name")
                    if sid and sym:
                        SYMBOL_IDS[sym] = int(sid)
                logging.info(f"[SoDEX] symbols loaded: {SYMBOL_IDS}")
    except Exception as e:
        logging.warning(f"[SoDEX] load_symbols failed: {e}")

class SoDEXExecutor:
    def __init__(self, api_key_name: str, private_key: str, account_id: str):
        self.api_key_name = (api_key_name or "").strip()
        self.private_key_raw = (private_key or "").strip()
        self.account_id = (account_id or "0").strip()
        self.ready = HAS_EIP712 and bool(self.private_key_raw and self.api_key_name)

    def _payload_hash(self, action_payload: dict) -> bytes:
        j = json.dumps(action_payload, separators=(",", ":"), ensure_ascii=False).encode()
        return keccak(j)

    def sign(self, action_payload: dict):
        nonce = int(datetime.now().timestamp()*1000)
        p_hash_bytes = self._payload_hash(action_payload)

        # CLEAN - no try/except, keccak always returns valid bytes
        payload_hash_field = HexBytes(p_hash_bytes)

        typed = {
            "types": {
                "EIP712Domain": [
                    {"name":"name","type":"string"},
                    {"name":"version","type":"string"},
                    {"name":"chainId","type":"uint256"},
                    {"name":"verifyingContract","type":"address"}
                ],
                "ExchangeAction": [
                    {"name":"payloadHash","type":"bytes32"},
                    {"name":"nonce","type":"uint64"}
                ]
            },
            "primaryType": "ExchangeAction",
            "domain": {
                "name": "futures",
                "version": "1",
                "chainId": SODEX_CHAIN_ID,
                "verifyingContract": "0x" + "0"*40
            },
            "message": {
                "payloadHash": payload_hash_field,
                "nonce": nonce
            }
        }
        key = clean_priv(self.private_key_raw)
        signed = Account.sign_typed_data(key, full_message=typed)
        sig = "0x" + (b"\x01" + signed.signature).hex()
        return sig, nonce, p_hash_bytes.hex()

    async def place_order(self, session, symbol: str, bias: str, entry: float, qty: float):
        if not self.ready:
            return {"err": "SoDEX not configured"}

        qty = max(0.001, float(qty))

        symbol_str = f"{symbol.upper()}-USDT-PERP"
        symbol_id = SYMBOL_IDS.get(symbol_str)
        if symbol_id is None:
            return {"err": f"symbolID not loaded for {symbol_str}. Call load_symbols() at startup. Loaded: {SYMBOL_IDS}"}

        raw_order = {
            "clOrdID": f"AF-{int(datetime.now().timestamp()*1000)}",
            "modifier": 1,
            "side": 1 if bias=="LONG" else 2,
            "type": 1,
            "timeInForce": 1,
            "price": f"{entry:.2f}",
            "quantity": f"{qty:.4f}",
            "reduceOnly": False,
            "positionSide": 1
        }
        params = {
            "accountID": int(float(self.account_id)),
            "symbolID": symbol_id,
            "orders": [raw_order]
        }
        action_payload = {"type": "newOrder", "params": params}
        try:
            sig, nonce, _ = self.sign(action_payload)
            headers = {"X-API-KEY": self.api_key_name, "X-API-SIGN": sig, "X-API-NONCE": str(nonce), "Content-Type":"application/json"}
            async with session.post(f"{SODEX_PERPS_URL}/order", json=params, headers=headers) as r:
                txt = await r.text()
                logging.info(f"[SoDEX] {r.status} {txt[:800]}")
                if r.status in [200,201]:
                    return {"ok": json.loads(txt) if txt else {}}
                return {"err": f"{r.status} {txt[:800]}"}
        except Exception as e:
            logging.exception("place_order")
            return {"err": str(e)}

    async def cancel_order(self, session, symbol: str, order_id: int = None, cl_ord_id: str = None):
        if not self.ready:
            return {"err": "SoDEX not configured"}
        try:
            symbol_id = SYMBOL_IDS.get(f"{symbol.upper()}-USDT-PERP")
            if symbol_id is None:
                return {"err": "symbolID not loaded"}
            cancel = {"symbolID": symbol_id}
            if order_id is not None:
                cancel["orderID"] = int(order_id)
            if cl_ord_id is not None:
                cancel["clOrdID"] = cl_ord_id
            params = {"accountID": int(float(self.account_id)), "cancels": [cancel]}
            action_payload = {"type": "cancelOrder", "params": params}
            sig, nonce, _ = self.sign(action_payload)
            headers = {"X-API-KEY": self.api_key_name, "X-API-SIGN": sig, "X-API-NONCE": str(nonce), "Content-Type":"application/json"}
            async with session.post(f"{SODEX_PERPS_URL}/cancel", json=params, headers=headers) as r:
                txt = await r.text()
                if r.status in [200,201]:
                    return {"ok": json.loads(txt) if txt else {}}
                return {"err": f"{r.status} {txt[:800]}"}
        except Exception as e:
            return {"err": str(e)}
