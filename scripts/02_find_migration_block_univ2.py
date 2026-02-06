import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from requests.exceptions import HTTPError
from web3 import Web3


def get_logs_safe(w3: Web3, address: str, topic0: str, start: int, end: int, min_range: int = 20, sleep: float = 0.15):
    """
    Fetch logs for [start,end]. If provider rejects (often 400 on eth_getLogs),
    split the range until it works.
    """
    params = {"address": address, "topics": [topic0], "fromBlock": start, "toBlock": end}

    try:
        return w3.eth.get_logs(params)
    except HTTPError as e:
        body = getattr(e.response, "text", "")
        if body:
            print("eth_getLogs HTTPError body:", body[:200])

        if end - start <= min_range:
            raise

        mid = (start + end) // 2
        left = get_logs_safe(w3, address, topic0, start, mid, min_range=min_range, sleep=sleep)
        time.sleep(sleep)
        right = get_logs_safe(w3, address, topic0, mid + 1, end, min_range=min_range, sleep=sleep)
        return left + right


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

    pair = Web3.to_checksum_address("0xC09bf2B1Bc8725903C509e8CAeef9190857215A8")

    # Sync(uint112,uint112) event topic0 = keccak("Sync(uint112,uint112)")
    sync_topic0 = w3.keccak(text="Sync(uint112,uint112)").hex()

    root = Path(__file__).resolve().parents[1]
    raw_meta = root / "data" / "raw" / "univ2_pair_metadata.json"
    out_csv = root / "data" / "processed" / "univ2_sync_timeseries.csv"
    out_json = root / "data" / "processed" / "migration_candidate.json"
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    meta = json.loads(raw_meta.read_text())
    dec0 = int(meta["token0"]["decimals"])
    dec1 = int(meta["token1"]["decimals"])

    latest = w3.eth.block_number

    # Start small (~30 days) to confirm pipeline works; expand later to 90/180 days.
    #220_000 blocks ~= 30 days
    #650_000 blocks ~= 90 days
    #1_300_000 blocks ~= 180 days
    from_block = max(0, latest - 650_000)
    to_block = latest
    step = 2_000

    print(f"Scanning Sync logs for {pair}")
    print(f"Blocks: {from_block} -> {to_block} (step={step})")

    rows = []
    for start in range(from_block, to_block + 1, step):
        end = min(start + step - 1, to_block)
        print(f"Fetching logs {start}->{end} ...")
        logs = get_logs_safe(w3, pair, sync_topic0, start, end)
        print(f"  got {len(logs)} logs")

        for lg in logs:
            data_hex = lg["data"].hex()
            r0 = int(data_hex[0:64], 16)
            r1 = int(data_hex[64:128], 16)
            rows.append(
                {
                    "block_number": int(lg["blockNumber"]),
                    "tx_hash": lg["transactionHash"].hex(),
                    "log_index": int(lg["logIndex"]),
                    "reserve0_raw": r0,
                    "reserve1_raw": r1,
                }
            )

    df = pd.DataFrame(rows).sort_values(["block_number", "log_index"]).reset_index(drop=True)

    if df.empty:
        raise RuntimeError("No Sync logs found in selected block range. Expand window or check address/topic.")

    uniq_blocks = df["block_number"].unique()
    print(f"Resolving timestamps for {len(uniq_blocks)} unique blocks ...")

    block_ts = {}
    for i, b in enumerate(uniq_blocks, start=1):
        block_ts[int(b)] = int(w3.eth.get_block(int(b))["timestamp"])
        if i % 200 == 0:
            print(f"  {i}/{len(uniq_blocks)} blocks")

    df["timestamp"] = df["block_number"].map(block_ts)
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    df["reserve0"] = df["reserve0_raw"] / (10**dec0)
    df["reserve1"] = df["reserve1_raw"] / (10**dec1)

    # Heuristic: largest negative jump in token1 reserves (token1 is WETH for this pair)
    df["delta_reserve1"] = df["reserve1"].diff()
    candidate = df.loc[df["delta_reserve1"].idxmin()]

    result = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "pair": pair,
        "from_block": int(from_block),
        "to_block": int(to_block),
        "candidate_migration_block": int(candidate["block_number"]),
        "candidate_datetime_utc": str(candidate["datetime_utc"]),
        "delta_reserve1_weth": float(candidate["delta_reserve1"]),
        "note": "Candidate chosen as largest negative change in token1 (WETH) reserves between consecutive Sync events.",
    }

    df.to_csv(out_csv, index=False)
    out_json.write_text(json.dumps(result, indent=2))

    print(f"Wrote {out_csv}")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
