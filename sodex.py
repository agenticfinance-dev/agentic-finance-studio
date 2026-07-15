import json, logging
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
    if not k: raise ValueError("Private key empty")
    k = k.strip().replace("\n","").replace(" ","").replace("\r","")
    if k.startswith("0x0x"): k = k[2:]
    if not k.startswith("0x"): k = "0x"+k
    # Address is 42 chars, private key is 66
    if len(k)==42:
        raise ValueError(f"You pasted ADDRESS {k} not PRIVATE KEY. Private key must be 66 chars (0x + 64 hex). Check Render env vars.")
    if len(k)!=66:
        raise ValueError(f"Invalid private key length {len(k)} - expected 66 (0x + 64 hex), got: {k[:10]}... - make sure you pasted full private key")
    return k

async def load_symbols(session):
    global SYMBOL_IDS
    try:
        url = f"{SODEX_PERPS_URL}/markets/symbols"
        async with session.get(url) as r:
            txt = await r.text()
            if r.status!=200: return
            j = json.loads(txt)
            data = j.get("data", j) if isinstance(j, dict) else j
            for item in data:
                if not isinstance(item, dict): continue
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
        j = json.dumps(action_payload, separators=(",", ":"), ensure_ascii=False).encode()
        return keccak(j)

    def sign(self, action_payload: dict):
        nonce = int(datetime.now().timestamp()*1000)
        p_hash_bytes = self._payload_hash(action_payload)
        payload_hash_field = HexBytes(p_hash_bytes)
        typed = {
            "types": {"EIP712Domain": [{"name":"name","type":"string"},{"name":"version","type":"string"},{"name":"chainId","type":"uint256"},{"name":"verifyingContract","type":"address"}],"ExchangeAction": [{"name":"payloadHash","type":"bytes32"},{"name":"nonce","type":"uint64"}]},
            "primaryType": "ExchangeAction",
            "domain": {"name": "futures","version": "1","chainId": SODEX_CHAIN_ID,"verifyingContract": "0x" + "0"*40},
            "message": {"payloadHash": payload_hash_field, "nonce": nonce}
        }
        key = clean_priv(self.private_key_raw)
        signed = Account.sign_typed_data(key, full_message=typed)
        sig = "0x" + (b"\x01" + signed.signature).hex()
        return sig, nonce, p_hash_bytes.hex()

    async def place_order(self, session, symbol: str, bias: str, entry: float, qty: float):
        if not self.ready: return {"err": "SoDEX not configured - check API key and private key"}
        qty = max(0.001, float(qty))
        symbol_id, found_name = find_symbol_id(symbol)
        if symbol_id is None:
            return {"err": f"symbolID not found for {symbol}. Have {list(SYMBOL_IDS.keys())[:10]}"}
        raw_order = {"clOrdID": f"AF-{int(datetime.now().timestamp()*1000)}","modifier": 1,"side": 1 if bias=="LONG" else 2,"type": 1,"timeInForce": 1,"price": f"{entry:.2f}","quantity": f"{qty:.4f}","reduceOnly": False,"positionSide": 1}
        params = {"accountID": int(float(self.account_id)), "symbolID": symbol_id, "orders": [raw_order]}
        action_payload = {"type": "newOrder", "params": params}
        try:
            sig, nonce, _ = self.sign(action_payload)
            headers = {"X-API-Key": self.api_key_name, "X-API-Sign": sig, "X-API-Nonce": str(nonce), "Content-Type": "application/json"}
            async with session.post(f"{SODEX_PERPS_URL}/trade/orders", json=params, headers=headers) as r:
                txt = await r.text()
                if r.status in [200,201]: return {"ok": json.loads(txt) if txt else {}, "used_symbol": found_name}
                return {"err": f"{r.status} {txt[:800]}", "used_symbol": found_name}
        except Exception as e:
            logging.exception("place_order")
            return {"err": str(e)}
