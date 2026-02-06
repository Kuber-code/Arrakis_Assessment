import os
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from web3 import Web3


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

    pair = Web3.to_checksum_address("0xC09bf2B1Bc8725903C509e8CAeef9190857215A8")
    migration_json = root / "data" / "processed" / "migration_candidate.json"
    meta_json = root / "data" / "raw" / "univ2_pair_metadata.json"

    mig = json.loads(migration_json.read_text())
    meta = json.loads(meta_json.read_text())

    cand_block = int(mig["candidate_migration_block"])
    window = 2000
    from_block = cand_block - window
    to_block = cand_block + window

    dec0 = int(meta["token0"]["decimals"])
    dec1 = int(meta["token1"]["decimals"])
    sym0 = meta["token0"]["symbol"]
    sym1 = meta["token1"]["symbol"]

    # Event topics (Uniswap V2 Pair)
    mint_topic0 = w3.keccak(text="Mint(address,uint256,uint256)").hex()
    burn_topic0 = w3.keccak(text="Burn(address,uint256,uint256,address)").hex()
    swap_topic0 = w3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
    sync_topic0 = w3.keccak(text="Sync(uint112,uint112)").hex()

    print(f"Pair: {pair}")
    print(f"Candidate migration block: {cand_block}")
    print(f"Scanning blocks: {from_block} -> {to_block} (±{window})")

    def fetch(topic0: str):
        return w3.eth.get_logs(
            {"fromBlock": from_block, "toBlock": to_block, "address": pair, "topics": [topic0]}
        )

    logs_mint = fetch(mint_topic0)
    logs_burn = fetch(burn_topic0)
    logs_swap = fetch(swap_topic0)
    logs_sync = fetch(sync_topic0)

    print(f"Mint events: {len(logs_mint)}")
    print(f"Burn events: {len(logs_burn)}")
    print(f"Swap events: {len(logs_swap)}")
    print(f"Sync events: {len(logs_sync)}")

    out_dir = root / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---- Decode burns (if any) ----
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
                f"amount0_raw_{sym0}": amount0,
                f"amount1_raw_{sym1}": amount1,
                f"amount0_{sym0}": amount0 / (10**dec0),
                f"amount1_{sym1}": amount1 / (10**dec1),
            }
        )

    burn_df = pd.DataFrame(burn_rows)
    if not burn_df.empty:
        burn_df = burn_df.sort_values(["block_number", "log_index"]).reset_index(drop=True)
        burn_df.to_csv(out_dir / "migration_confirm_burns.csv", index=False)
        top_burns = burn_df.sort_values(by=f"amount1_{sym1}", ascending=False).head(10)
    else:
        top_burns = None

    # ---- Decode swaps (to see if it was a big trade) ----
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
                # proxy "size in WETH"
                f"abs_weth_{sym1}": max(amount1_in, amount1_out) / (10**dec1),
            }
        )

    swap_df = pd.DataFrame(swap_rows)
    if not swap_df.empty:
        swap_df = swap_df.sort_values(["block_number", "log_index"]).reset_index(drop=True)
        swap_df.to_csv(out_dir / "migration_confirm_swaps.csv", index=False)
        top_swaps = swap_df.sort_values(by=f"abs_weth_{sym1}", ascending=False).head(10)
    else:
        top_swaps = None

    summary = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "pair": pair,
        "candidate_block": cand_block,
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
        "interpretation_hint": "If large Burn(s) occur near candidate block, it suggests liquidity removal/migration; if only Swaps dominate, it may be a large trade rather than liquidity migration.",
    }

    (out_dir / "migration_confirm_summary.json").write_text(json.dumps(summary, indent=2))
    print(f"Wrote {out_dir / 'migration_confirm_summary.json'}")

    if top_burns is not None:
        print("\nTop 10 burns by token1 (usually WETH):")
        print(top_burns.to_string(index=False))
    else:
        print("\nNo Burn events in ±2000 blocks.")

    if top_swaps is not None:
        print("\nTop 10 swaps by abs(token1) (usually WETH):")
        print(top_swaps.to_string(index=False))

    print("\nFiles written (if non-empty):")
    print(f"- {out_dir / 'migration_confirm_burns.csv'} (if any burns)")
    print(f"- {out_dir / 'migration_confirm_swaps.csv'} (if any swaps)")


if __name__ == "__main__":
    main()
