import json
import logging
import time
from decimal import Decimal
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

_last_nonce = 0

def next_nonce() -> int:
    global _last_nonce
    now = int(time.time() * 1000)
    if now <= _last_nonce:
        now = _last_nonce + 1
    _last_nonce = now
    return now

def clean_priv(k: str) -> str:
    if not k:
        raise ValueError("Private key empty")
    k = k.strip().replace("\n","").replace(" ","").replace("\r","")
    if k.startswith("0x0x"):
        k = k[2:]
    if not k.startswith("0x"):
        k = "0x" + k
    if len(k) == 42:
        raise ValueError(f"Pasted ADDRESS {k} not PRIVATE KEY")
    if len(k)!= 66:
        raise ValueError(f"Invalid key length {len(k)} expected 66")
    return k

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
        self.ready = HAS_EIP712 and bool(self.private_key_raw and self.api_key_name)

    def _payload_hash(self, action_payload: dict) -> bytes:
        j = json.dumps(
            action_payload,
            separators=(",", ":"),
            ensure_ascii=False,
            sort_keys=False
        ).encode()
        return keccak(j)

    def sign(self, action_payload: dict, nonce: int):
        p_hash_bytes = self._payload_hash(action_payload)
        typed = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"}
                ],
                "ExchangeAction": [
                    {"name": "payloadHash", "type": "bytes32"},
                    {"name": "nonce", "type": "uint64"}
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
                "payloadHash": HexBytes(p_hash_bytes),
                "nonce": nonce
            }
        }
        key = clean_priv(self.private_key_raw)
        signed = Account.sign_typed_data(key, full_message=typed)
        return "0x" + (b"\x01" + signed.signature).hex()

    async def place_order(self, session, symbol: str, bias: str, entry: float, qty: float):
        if not self.ready:
            return {"err": "SoDEX not configured"}

        symbol_id, found_name = find_symbol_id(symbol)
        if symbol_id is None:
            return {"err": f"symbolID not found for {symbol}. Have: {list(SYMBOL_IDS.keys())[:15]}"}

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
            "positionSide": 2 if bias == "LONG" else 3
        }

        payload = {
            "accountID": int(float(self.account_id)),
            "symbolID": symbol_id,
            "orders": [order]
        }

        action_payload = {
            "type": "newOrder",
            "params": payload
        }

        try:
            sig = self.sign(action_payload, nonce)

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
                logging.info(f"[SoDEX] Place {found_name} ID={symbol_id} {r.status} {txt[:800]}")
                if r.status in [200, 201]:
                    try:
                        return {"ok": json.loads(txt) if txt else {}, "used_symbol": found_name}
                    except:
                        return {"ok": {"raw": txt}, "used_symbol": found_name}
                return {"err": f"{r.status} {txt[:800]}", "used_symbol": found_name}
        except Exception as e:
            logging.exception("[SoDEX] place_order failed")
            return {"err": str(e)}
