import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from requests.exceptions import HTTPError
from web3 import Web3


def get_logs_safe(w3: Web3, address: str, topic0: str, start: int, end: int, min_range: int = 50, sleep: float = 0.12):
    params = {"address": address, "topics": [topic0], "fromBlock": start, "toBlock": end}
    try:
        return w3.eth.get_logs(params)
    except HTTPError:
        if end - start <= min_range:
            raise
        mid = (start + end) // 2
        left = get_logs_safe(w3, address, topic0, start, mid, min_range=min_range, sleep=sleep)
        time.sleep(sleep)
        right = get_logs_safe(w3, address, topic0, mid + 1, end, min_range=min_range, sleep=sleep)
        return left + right


def fetch_topic(w3: Web3, address: str, topic0: str, start: int, end: int, step: int = 2000):
    logs_all = []
    for s in range(start, end + 1, step):
        e = min(s + step - 1, end)
        logs = get_logs_safe(w3, address, topic0, s, e)
        logs_all.extend(logs)
    return logs_all


def main():
    load_dotenv()
    rpc_url = os.environ.get("RPC_URL")
    if not rpc_url:
        raise RuntimeError("Missing RPC_URL in .env")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError("Cannot connect to RPC_URL")
    if w3.eth.chain_id != 1:
        raise RuntimeError(f"Not Ethereum mainnet: chain_id={w3.eth.chain_id}")

    root = Path(__file__).resolve().parents[1]
    out_dir = root / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)

    pair = Web3.to_checksum_address("0xC09bf2B1Bc8725903C509e8CAeef9190857215A8")

    mig_json = root / "data" / "processed" / "migration_regime_change.json"
    meta_json = root / "data" / "raw" / "univ2_pair_metadata.json"
    mig = json.loads(mig_json.read_text())
    meta = json.loads(meta_json.read_text())

    mig_block = int(mig["best_change_point"]["migration_block_estimate"])
    window = 50_000  # use the wider window directly, but chunked safely
    from_block = mig_block - window
    to_block = mig_block + window

    sym0 = meta["token0"]["symbol"]
    sym1 = meta["token1"]["symbol"]
    dec0 = int(meta["token0"]["decimals"])
    dec1 = int(meta["token1"]["decimals"])

    # Event topics (Uniswap V2 Pair)
    mint_topic0 = w3.keccak(text="Mint(address,uint256,uint256)").hex()
    burn_topic0 = w3.keccak(text="Burn(address,uint256,uint256,address)").hex()
    swap_topic0 = w3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
    sync_topic0 = w3.keccak(text="Sync(uint112,uint112)").hex()

    ts_from = w3.eth.get_block(from_block)["timestamp"]
    ts_to = w3.eth.get_block(to_block)["timestamp"]

    print(f"Pair: {pair}")
    print(f"Regime-change migration estimate block: {mig_block}")
    print(f"Scanning blocks: {from_block} -> {to_block} (±{window})")
    print("from_block time UTC:", datetime.fromtimestamp(ts_from, tz=timezone.utc))
    print("to_block time UTC:", datetime.fromtimestamp(ts_to, tz=timezone.utc))

    print("Fetching Sync logs (chunked)...")
    logs_sync = fetch_topic(w3, pair, sync_topic0, from_block, to_block, step=2000)
    print("Sync:", len(logs_sync))

    print("Fetching Swap logs (chunked)...")
    logs_swap = fetch_topic(w3, pair, swap_topic0, from_block, to_block, step=2000)
    print("Swap:", len(logs_swap))

    print("Fetching Burn logs (chunked)...")
    logs_burn = fetch_topic(w3, pair, burn_topic0, from_block, to_block, step=2000)
    print("Burn:", len(logs_burn))

    print("Fetching Mint logs (chunked)...")
    logs_mint = fetch_topic(w3, pair, mint_topic0, from_block, to_block, step=2000)
    print("Mint:", len(logs_mint))

    # Decode burns
    burn_rows = []
    for lg in logs_burn:
        data_hex = lg["data"].hex()
        amount0 = int(data_hex[0:64], 16)
        amount1 = int(data_hex[64:128], 16)
        burn_rows.append(
            {
                "block_number": int(lg["blockNumber"]),
                "tx_hash": lg["transactionHash"].hex(),
                "log_index": int(lg["logIndex"]),
                f"amount0_{sym0}": amount0 / (10**dec0),
                f"amount1_{sym1}": amount1 / (10**dec1),
            }
        )
    burn_df = pd.DataFrame(burn_rows)
    if not burn_df.empty:
        burn_df = burn_df.sort_values(["block_number", "log_index"]).reset_index(drop=True)
        burn_df.to_csv(out_dir / "migration03b_burns.csv", index=False)

    # Decode swaps
    swap_rows = []
    for lg in logs_swap:
        data_hex = lg["data"].hex()
        amount0_in = int(data_hex[0:64], 16)
        amount1_in = int(data_hex[64:128], 16)
        amount0_out = int(data_hex[128:192], 16)
        amount1_out = int(data_hex[192:256], 16)
        swap_rows.append(
            {
                "block_number": int(lg["blockNumber"]),
                "tx_hash": lg["transactionHash"].hex(),
                "log_index": int(lg["logIndex"]),
                f"amount0_in_{sym0}": amount0_in / (10**dec0),
                f"amount1_in_{sym1}": amount1_in / (10**dec1),
                f"amount0_out_{sym0}": amount0_out / (10**dec0),
                f"amount1_out_{sym1}": amount1_out / (10**dec1),
                f"abs_{sym1}": max(amount1_in, amount1_out) / (10**dec1),
            }
        )
    swap_df = pd.DataFrame(swap_rows)
    if not swap_df.empty:
        swap_df = swap_df.sort_values(["block_number", "log_index"]).reset_index(drop=True)
        swap_df.to_csv(out_dir / "migration03b_swaps.csv", index=False)

    summary = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "pair": pair,
        "regime_change_block_estimate": mig_block,
        "window_blocks": window,
        "from_block": int(from_block),
        "to_block": int(to_block),
        "event_counts": {
            "Mint": len(logs_mint),
            "Burn": len(logs_burn),
            "Swap": len(logs_swap),
            "Sync": len(logs_sync),
        },
        "token0": {"symbol": sym0, "decimals": dec0},
        "token1": {"symbol": sym1, "decimals": dec1},
        "interpretation": "If Burn events cluster around the regime-change time, that supports liquidity removal/migration; otherwise the regime shift may be driven by trading flow or liquidity moved outside this pool.",
    }

    (out_dir / "migration03b_summary.json").write_text(json.dumps(summary, indent=2))
    print(f"\nWrote {out_dir / 'migration03b_summary.json'}")

    if not burn_df.empty:
        print("\nTop 10 burns by token1 (usually WETH):")
        print(burn_df.sort_values(by=f"amount1_{sym1}", ascending=False).head(10).to_string(index=False))
    else:
        print("\nNo Burn events in the scanned window.")

    if not swap_df.empty:
        print("\nTop 10 swaps by abs(token1) (usually WETH):")
        print(swap_df.sort_values(by=f"abs_{sym1}", ascending=False).head(10).to_string(index=False))


if __name__ == "__main__":
    main()
