import os
import json
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from requests.exceptions import HTTPError
from web3 import Web3


FEED_ETH_USD = Web3.to_checksum_address("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419")  # ETH/USD mainnet [web:241]


def get_logs_safe(w3: Web3, params: dict, start: int, end: int, min_range: int = 200, sleep: float = 0.12):
    req = dict(params)
    req["fromBlock"] = start
    req["toBlock"] = end
    try:
        return w3.eth.get_logs(req)
    except HTTPError:
        if end - start <= min_range:
            raise
        mid = (start + end) // 2
        left = get_logs_safe(w3, params, start, mid, min_range=min_range, sleep=sleep)
        time.sleep(sleep)
        right = get_logs_safe(w3, params, mid + 1, end, min_range=min_range, sleep=sleep)
        return left + right


def fetch_logs_chunked(w3: Web3, params: dict, from_block: int, to_block: int, step: int = 20_000):
    all_logs = []
    for s in range(from_block, to_block + 1, step):
        e = min(s + step - 1, to_block)
        logs = get_logs_safe(w3, params, s, e)
        all_logs.extend(logs)
        if (s - from_block) // step % 10 == 0:
            print(f"Fetched blocks {s}->{e}, logs={len(logs)}, total={len(all_logs)}")
    return all_logs


def call_uint8(w3: Web3, to: str, selector_hex: str) -> int:
    out = w3.eth.call({"to": to, "data": selector_hex}).hex()
    return int(out, 16)


def call_get_round_data(w3: Web3, feed: str, round_id: int):
    # AggregatorV3Interface.getRoundData(uint80)
    # selector = bytes4(keccak256("getRoundData(uint80)"))
    # We hardcode selector for simplicity using common ABI: 0x9a6fc8f5
    selector = "0x9a6fc8f5"
    rid = round_id.to_bytes(32, "big").hex()
    data = selector + rid
    raw = w3.eth.call({"to": feed, "data": data})
    hx = raw.hex()[2:]

    # returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
    def word(i):  # 32-byte word as hex
        return hx[i * 64 : (i + 1) * 64]

    round_id_out = int(word(0), 16)
    answer = int.from_bytes(bytes.fromhex(word(1)), "big", signed=True)
    started_at = int(word(2), 16)
    updated_at = int(word(3), 16)
    answered_in_round = int(word(4), 16)
    return round_id_out, answer, started_at, updated_at, answered_in_round


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
    processed = root / "data" / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    sync_csv = processed / "univ2_sync_timeseries.csv"
    if not sync_csv.exists():
        raise RuntimeError("Missing data/processed/univ2_sync_timeseries.csv. Run script 02 first.")

    sync = pd.read_csv(sync_csv, usecols=["block_number"])
    from_block = int(sync["block_number"].min())
    to_block = int(sync["block_number"].max())

    # Use NewRound logs to discover historical rounds, then getRoundData(roundId) for answers.
    # NewRound(uint256 indexed roundId, address indexed startedBy, uint256 startedAt)
    newround_topic0 = w3.keccak(text="NewRound(uint256,address,uint256)").hex()

    params = {"address": FEED_ETH_USD, "topics": [newround_topic0]}

    print(f"ETH/USD feed: {FEED_ETH_USD}")
    print(f"Fetching NewRound logs in blocks {from_block} -> {to_block}")

    logs = fetch_logs_chunked(w3, params, from_block, to_block, step=20_000)
    print(f"Total NewRound logs: {len(logs)}")

    if len(logs) == 0:
        raise RuntimeError(
            "No NewRound logs fetched. Possible RPC limitation on this address/topics, or wrong feed address."
        )

    # roundId is topics[1] (indexed uint256)
    round_ids = sorted({int(lg["topics"][1].hex(), 16) for lg in logs})
    print(f"Unique rounds discovered: {len(round_ids)}")

    # decimals() selector = 0x313ce567 (standard) [web:246]
    decimals = call_uint8(w3, FEED_ETH_USD, "0x313ce567")
    print("Feed decimals:", decimals)

    rows = []
    for i, rid in enumerate(round_ids, start=1):
        try:
            rid_out, answer, started_at, updated_at, answered_in_round = call_get_round_data(w3, FEED_ETH_USD, rid)
        except Exception as e:
            # some rounds might be missing; skip but record
            continue

        rows.append(
            {
                "round_id": int(rid_out),
                "answer": int(answer),
                "started_at": int(started_at),
                "updated_at": int(updated_at),
                "answered_in_round": int(answered_in_round),
                "decimals": int(decimals),
                "eth_usd": float(answer) / (10**decimals),
            }
        )

        if i % 200 == 0:
            print(f"Resolved rounds {i}/{len(round_ids)}")

    df = pd.DataFrame(rows).sort_values("updated_at").reset_index(drop=True)
    if df.empty:
        raise RuntimeError("Resolved 0 rounds via getRoundData; something is wrong with calls or RPC.")

    out_csv = processed / "eth_usd_chainlink_answers.csv"
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")
    print(df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
