import os, json
from datetime import datetime, timezone
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()
RPC_URL = os.environ.get("RPC_URL")
if not RPC_URL:
    raise RuntimeError("Missing RPC_URL in .env")

w3 = Web3(Web3.HTTPProvider(RPC_URL))
if not w3.is_connected():
    raise RuntimeError("Cannot connect to RPC_URL")

run_ts = datetime.now(timezone.utc).isoformat()
chain_id = w3.eth.chain_id
latest_block = w3.eth.block_number

ADDRESSES = {
    "univ2_pair": "0xC09bf2B1Bc8725903C509e8CAeef9190857215A8",
    "arrakis_vault": "0x90bde935ce7feb6636afd5a1a0340af45eeae600",
    "univ4_quoter": "0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203",
}

def is_contract(addr: str) -> tuple[str, bool, int]:
    checksum = Web3.to_checksum_address(addr)
    code = w3.eth.get_code(checksum)  # eth_getCode under the hood [web:46][web:54]
    code_hex_len = len(code.hex())    # includes "0x..."
    return checksum, (code_hex_len > 2), code_hex_len

report = {
    "run_ts_utc": run_ts,
    "rpc_url_redacted": RPC_URL.split("?")[0],
    "chain_id": chain_id,
    "latest_block": latest_block,
    "contracts": {},
}

for name, addr in ADDRESSES.items():
    checksum, ok, code_len = is_contract(addr)
    report["contracts"][name] = {
        "address": checksum,
        "is_contract": ok,
        "bytecode_hex_len": code_len,
    }

os.makedirs("data/raw", exist_ok=True)
with open("data/raw/address_verification.json", "w") as f:
    json.dump(report, f, indent=2)

print(json.dumps(report, indent=2))

if chain_id != 1:
    raise RuntimeError(f"Not mainnet: chain_id={chain_id}")

for name, meta in report["contracts"].items():
    if not meta["is_contract"]:
        raise RuntimeError(f"{name} is not a contract on this chain: {meta['address']}")
