import os, json
from datetime import datetime, timezone
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()
RPC_URL = os.environ["RPC_URL"]
w3 = Web3(Web3.HTTPProvider(RPC_URL))

PAIR = Web3.to_checksum_address("0xC09bf2B1Bc8725903C509e8CAeef9190857215A8")

UNIV2_PAIR_ABI = [
    {"name": "token0", "outputs": [{"type": "address"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "token1", "outputs": [{"type": "address"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "getReserves", "outputs": [{"type": "uint112"}, {"type": "uint112"}, {"type": "uint32"}],
     "inputs": [], "stateMutability": "view", "type": "function"},
]

ERC20_ABI = [
    {"name": "symbol", "outputs": [{"type": "string"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "decimals", "outputs": [{"type": "uint8"}], "inputs": [], "stateMutability": "view", "type": "function"},
]

pair = w3.eth.contract(address=PAIR, abi=UNIV2_PAIR_ABI)
t0 = Web3.to_checksum_address(pair.functions.token0().call())
t1 = Web3.to_checksum_address(pair.functions.token1().call())
r0, r1, ts_last = pair.functions.getReserves().call()  # reserves of token0/token1 [web:73]

token0 = w3.eth.contract(address=t0, abi=ERC20_ABI)
token1 = w3.eth.contract(address=t1, abi=ERC20_ABI)

sym0 = token0.functions.symbol().call()
sym1 = token1.functions.symbol().call()
dec0 = token0.functions.decimals().call()
dec1 = token1.functions.decimals().call()

latest_block = w3.eth.block_number
run_ts = datetime.now(timezone.utc).isoformat()

out = {
    "run_ts_utc": run_ts,
    "chain_id": w3.eth.chain_id,
    "latest_block": latest_block,
    "pair": str(PAIR),
    "token0": {"address": t0, "symbol": sym0, "decimals": dec0, "reserve_raw": str(r0)},
    "token1": {"address": t1, "symbol": sym1, "decimals": dec1, "reserve_raw": str(r1)},
    "blockTimestampLast_raw": str(ts_last),
}

os.makedirs("data/raw", exist_ok=True)
with open("data/raw/univ2_pair_metadata.json", "w") as f:
    json.dump(out, f, indent=2)

print(json.dumps(out, indent=2))
