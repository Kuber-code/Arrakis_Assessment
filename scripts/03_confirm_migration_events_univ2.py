import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from requests.exceptions import HTTPError
from web3 import Web3


def get_logs_safe(
    w3: Web3,
    address: str,
    topic0: str,
    start: int,
    end: int,
    min_range: int = 50,
    sleep: float = 0.12,
):
    params = {"address": address, "topics": [topic0], "fromBlock": start, "toBlock": end}
    try:
        return w3.eth.get_logs(params)
    except HTTPError as e:
        body = getattr(e.response, "text", "") if hasattr(e, "response") else ""
        if body:
            print("eth_getLogs HTTPError body:", body[:200])
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


def decode_u256_pair_from_data(lg) -> tuple[int, int]:
    data = lg["data"]
    # data is 64 bytes: amount0 (32) + amount1 (32)
    amount0 = int.from_bytes(data[0:32], "big")
    amount1 = int.from_bytes(data[32:64], "big")
    return amount0, amount1


def decode_swap_from_data(lg) -> tuple[int, int, int, int]:
    data = lg["data"]
    # 4 * 32 bytes
    a0_in = int.from_bytes(data[0:32], "big")
    a1_in = int.from_bytes(data[32:64], "big")
    a0_out = int.from_bytes(data[64:96], "big")
    a1_out = int.from_bytes(data[96:128], "big")
    return a0_in, a1_in, a0_out, a1_out


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

    migration_json = out_dir / "migration_candidate.json"
    meta_json = root / "data" / "raw" / "univ2_pair_metadata.json"

    mig = json.loads(migration_json.read_text(encoding="utf-8"))
    meta = json.loads(meta_json.read_text(encoding="utf-8"))

    cand_block = int(mig["candidate_migration_block"])
    window = int(os.environ.get("MIGRATION_CONFIRM_WINDOW_BLOCKS", "2000"))

    from_block = max(0, cand_block - window)
    to_block = cand_block + window

    sym0 = meta["token0"]["symbol"]
    sym1 = meta["token1"]["symbol"]
    dec0 = int(meta["token0"]["decimals"])
    dec1 = int(meta["token1"]["decimals"])

    mint_topic0 = w3.keccak(text="Mint(address,uint256,uint256)").hex()
    burn_topic0 = w3.keccak(text="Burn(address,uint256,uint256,address)").hex()
    swap_topic0 = w3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
    sync_topic0 = w3.keccak(text="Sync(uint112,uint112)").hex()

    print(f"Pair: {pair}")
    print(f"Candidate migration block: {cand_block}")
    print(f"Scanning blocks: {from_block} -> {to_block} (±{window})")

    logs_mint = fetch_topic(w3, pair, mint_topic0, from_block, to_block, step=2000)
    logs_burn = fetch_topic(w3, pair, burn_topic0, from_block, to_block, step=2000)
    logs_swap = fetch_topic(w3, pair, swap_topic0, from_block, to_block, step=2000)
    logs_sync = fetch_topic(w3, pair, sync_topic0, from_block, to_block, step=2000)

    print(f"Mint events: {len(logs_mint)}")
    print(f"Burn events: {len(logs_burn)}")
    print(f"Swap events: {len(logs_swap)}")
    print(f"Sync events: {len(logs_sync)}")

    # Burns
    burn_rows = []
    for lg in logs_burn:
        amount0, amount1 = decode_u256_pair_from_data(lg)
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
        burn_df.to_csv(out_dir / "migration_confirm_burns.csv", index=False)

    # Swaps
    swap_rows = []
    for lg in logs_swap:
        a0_in, a1_in, a0_out, a1_out = decode_swap_from_data(lg)
        swap_rows.append(
            {
                "block_number": int(lg["blockNumber"]),
                "tx_hash": lg["transactionHash"].hex(),
                "log_index": int(lg["logIndex"]),
                f"amount0_in_{sym0}": a0_in / (10**dec0),
                f"amount1_in_{sym1}": a1_in / (10**dec1),
                f"amount0_out_{sym0}": a0_out / (10**dec0),
                f"amount1_out_{sym1}": a1_out / (10**dec1),
                f"abs_{sym1}": max(a1_in, a1_out) / (10**dec1),
            }
        )
    swap_df = pd.DataFrame(swap_rows)
    if not swap_df.empty:
        swap_df = swap_df.sort_values(["block_number", "log_index"]).reset_index(drop=True)
        swap_df.to_csv(out_dir / "migration_confirm_swaps.csv", index=False)

    summary = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "pair": pair,
        "candidate_block": cand_block,
        "candidate_datetime_utc": mig.get("candidate_datetime_utc"),
        "delta_reserve1_weth": mig.get("delta_reserve1_weth"),
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
        "interpretation_hint": (
            "Large Burn(s) near candidate block supports liquidity removal/migration; "
            "if only Swaps dominate, it may be a large trade rather than migration."
        ),
        "files_written": {
            "burns_csv": str(out_dir / "migration_confirm_burns.csv"),
            "swaps_csv": str(out_dir / "migration_confirm_swaps.csv"),
        },
    }

    (out_dir / "migration_confirm_summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(f"Wrote {out_dir / 'migration_confirm_summary.json'}")

    if not burn_df.empty:
        print("\nTop 10 burns by token1 (usually WETH):")
        print(burn_df.sort_values(by=f"amount1_{sym1}", ascending=False).head(10).to_string(index=False))
    else:
        print("\nNo Burn events in the window.")

    if not swap_df.empty:
        print("\nTop 10 swaps by abs(token1) (usually WETH):")
        print(swap_df.sort_values(by=f"abs_{sym1}", ascending=False).head(10).to_string(index=False))


if __name__ == "__main__":
    main()
