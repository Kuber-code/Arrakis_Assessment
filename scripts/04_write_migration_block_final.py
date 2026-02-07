import os
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from web3 import Web3


def read_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def try_fetch_block_timestamp_utc(w3: Web3, block_number: int):
    try:
        ts = int(w3.eth.get_block(int(block_number))["timestamp"])
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except Exception:
        return None


def main():
    root = Path(__file__).resolve().parents[1]
    processed = root / "data" / "processed"
    raw = root / "data" / "raw"
    processed.mkdir(parents=True, exist_ok=True)

    cand_path = raw / "migration_candidate.json"
    burns_path = processed / "migration_confirm_burns.csv"
    out_path = raw / "migration_block_final.json"

    cand = read_json(cand_path)
    if not cand:
        raise RuntimeError(f"Missing required input: {cand_path}")

    # Optional RPC (only to enrich output with exact timestamp)
    load_dotenv()
    rpc_url = os.environ.get("RPC_URL")
    w3 = Web3(Web3.HTTPProvider(rpc_url)) if rpc_url else None
    if w3 and (not w3.is_connected()):
        w3 = None

    chosen = {
        "selected_by": None,
        "migration_block_final": None,
        "migration_time_utc": None,
        "details": {},
    }

    # 1) Prefer largest Burn (amount1_WETH) from the tight confirmation window
    if burns_path.exists():
        burns_df = pd.read_csv(burns_path)
        if not burns_df.empty and {"block_number", "amount1_WETH"}.issubset(burns_df.columns):
            burns_df = burns_df.copy()
            burns_df["block_number"] = burns_df["block_number"].astype(int)
            burns_df["amount1_WETH"] = burns_df["amount1_WETH"].astype(float)

            top = burns_df.sort_values("amount1_WETH", ascending=False).iloc[0]
            block_final = int(top["block_number"])

            chosen["selected_by"] = "largest_burn_confirm_window"
            chosen["migration_block_final"] = block_final
            chosen["migration_time_utc"] = try_fetch_block_timestamp_utc(w3, block_final) if w3 else None
            chosen["details"] = {
                "burns_csv": str(burns_path.relative_to(root)),
                "burns_rows": int(len(burns_df)),
                "top_burn_block": block_final,
                "top_burn_amount1_weth": float(top["amount1_WETH"]),
            }

    # 2) Fallback: largest Sync reserve drop candidate
    if chosen["migration_block_final"] is None:
        block_final = int(cand["candidate_migration_block"])
        chosen["selected_by"] = "largest_sync_drop_candidate"
        chosen["migration_block_final"] = block_final
        chosen["migration_time_utc"] = cand.get("candidate_datetime_utc") or (
            try_fetch_block_timestamp_utc(w3, block_final) if w3 else None
        )
        chosen["details"] = {
            "candidate_json": str(cand_path.relative_to(root)),
            "delta_reserve1_weth": cand.get("delta_reserve1_weth"),
        }

    out = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "rule": {
            "priority": [
                "largest_burn_confirm_window (amount1_WETH from migration_confirm_burns.csv)",
                "largest_sync_drop_candidate (delta_reserve1_weth from migration_candidate.json)",
            ],
            "notes": (
                "This script selects a conservative migration block for splitting pre/post analysis. "
                "Primary evidence is Uniswap V2 Burn events near the candidate block."
            ),
        },
        "inputs": {
            "migration_candidate_json": str(cand_path.relative_to(root)),
            "migration_confirm_burns_csv": str(burns_path.relative_to(root)) if burns_path.exists() else None,
        },
        "selected": chosen,
        "supporting": {
            "candidate_block": int(cand["candidate_migration_block"]),
            "candidate_time_utc": cand.get("candidate_datetime_utc"),
            "candidate_delta_reserve1_weth": cand.get("delta_reserve1_weth"),
        },
        "next_step": (
            "Use migration_block_final to define pre-migration UniV2 window and post-migration UniV4 window "
            "for execution-quality (slippage) and vault-performance analyses."
        ),
    }

    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote {out_path}")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
