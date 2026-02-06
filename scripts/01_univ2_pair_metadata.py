import os
import json
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from web3 import Web3


PAIR = "0xC09bf2B1Bc8725903C509e8CAeef9190857215A8"

UNIV2_PAIR_ABI = [
    {
        "name": "token0",
        "outputs": [{"type": "address"}],
        "inputs": [],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "token1",
        "outputs": [{"type": "address"}],
        "inputs": [],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "getReserves",
        "outputs": [{"type": "uint112"}, {"type": "uint112"}, {"type": "uint32"}],
        "inputs": [],
        "stateMutability": "view",
        "type": "function",
    },
]

ERC20_ABI = [
    {
        "name": "symbol",
        "outputs": [{"type": "string"}],
        "inputs": [],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "decimals",
        "outputs": [{"type": "uint8"}],
        "inputs": [],
        "stateMutability": "view",
        "type": "function",
    },
]


def main() -> None:
    load_dotenv()
    rpc_url = os.environ.get("RPC_URL")
    if not rpc_url:
        raise RuntimeError("Missing RPC_URL in .env")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError("Cannot connect to RPC_URL")

    if w3.eth.chain_id != 1:
        raise RuntimeError(f"Not Ethereum mainnet: chain_id={w3.eth.chain_id}")

    pair_addr = Web3.to_checksum_address(PAIR)

    # Ensure it's a contract
    if len(w3.eth.get_code(pair_addr).hex()) <= 2:
        raise RuntimeError(f"PAIR has no bytecode: {pair_addr}")

    block_number_used = w3.eth.block_number

    pair = w3.eth.contract(address=pair_addr, abi=UNIV2_PAIR_ABI)
    t0 = Web3.to_checksum_address(pair.functions.token0().call(block_identifier=block_number_used))
    t1 = Web3.to_checksum_address(pair.functions.token1().call(block_identifier=block_number_used))
    r0, r1, ts_last = pair.functions.getReserves().call(block_identifier=block_number_used)

    token0 = w3.eth.contract(address=t0, abi=ERC20_ABI)
    token1 = w3.eth.contract(address=t1, abi=ERC20_ABI)

    sym0 = token0.functions.symbol().call(block_identifier=block_number_used)
    sym1 = token1.functions.symbol().call(block_identifier=block_number_used)
    dec0 = token0.functions.decimals().call(block_identifier=block_number_used)
    dec1 = token1.functions.decimals().call(block_identifier=block_number_used)

    out = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "chain_id": w3.eth.chain_id,
        "latest_block": w3.eth.block_number,
        "block_number_used": int(block_number_used),
        "pair": str(pair_addr),
        "token0": {"address": t0, "symbol": sym0, "decimals": int(dec0), "reserve_raw": str(r0)},
        "token1": {"address": t1, "symbol": sym1, "decimals": int(dec1), "reserve_raw": str(r1)},
        "blockTimestampLast_raw": str(ts_last),
    }

    root = Path(__file__).resolve().parents[1]
    out_path = root / "data" / "raw" / "univ2_pair_metadata.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
