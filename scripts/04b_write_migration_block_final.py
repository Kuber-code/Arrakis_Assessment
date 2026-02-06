import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def read_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def safe_read_csv(path: Path):
    if not path.exists():
        return None, "missing"
    try:
        df = pd.read_csv(path)
        if df.empty:
            return df, "empty"
        return df, "ok"
    except pd.errors.EmptyDataError:
        return pd.DataFrame(), "empty"
    except Exception as e:
        return None, f"error: {type(e).__name__}: {e}"


def main():
    root = Path(__file__).resolve().parents[1]
    processed = root / "data" / "processed"

    cand_path = processed / "migration_candidate.json"
    regime_path = processed / "migration_regime_change.json"
    burns_path = processed / "migration03b_burns.csv"

    out_path = processed / "migration_block_final.json"
    processed.mkdir(parents=True, exist_ok=True)

    cand = read_json(cand_path)
    regime = read_json(regime_path)
    burns_df, burns_status = safe_read_csv(burns_path)

    # --- Extract candidates ---
    cand_block = None
    cand_time = None
    cand_delta_weth = None
    if cand:
        cand_block = int(cand.get("candidate_migration_block")) if cand.get("candidate_migration_block") is not None else None
        cand_time = cand.get("candidate_datetime_utc")
        cand_delta_weth = cand.get("delta_reserve1_weth")

    regime_block = None
    regime_time = None
    regime_delta_weth = None
    regime_before = None
    regime_after = None
    regime_persistence = None
    if regime and regime.get("best_change_point"):
        bcp = regime["best_change_point"]
        regime_block = int(bcp.get("migration_block_estimate")) if bcp.get("migration_block_estimate") is not None else None
        regime_time = bcp.get("bin_time_utc")
        regime_delta_weth = bcp.get("delta_weth")
        regime_before = bcp.get("before_level_weth")
        regime_after = bcp.get("after_level_weth_24h")
        regime_persistence = bcp.get("persistence_score")

    # --- Rule: choose final block ---
    chosen = {
        "source": None,
        "block": None,
        "time_utc": None,
        "details": {},
    }

    # 1) Prefer largest Burn (amount1_WETH)
    if burns_df is not None and burns_status == "ok" and len(burns_df) > 0:
        # Expect columns: block_number, amount1_WETH (from your script output)
        if "amount1_WETH" in burns_df.columns and "block_number" in burns_df.columns:
            burns_df = burns_df.copy()
            burns_df["amount1_WETH"] = burns_df["amount1_WETH"].astype(float)
            burns_df["block_number"] = burns_df["block_number"].astype(int)

            top = burns_df.sort_values("amount1_WETH", ascending=False).iloc[0]
            chosen["source"] = "largest_burn"
            chosen["block"] = int(top["block_number"])
            chosen["time_utc"] = None  # optional, we don't fetch block timestamp here
            chosen["details"] = {
                "burns_csv": str(burns_path),
                "top_burn_block": int(top["block_number"]),
                "top_burn_amount1_weth": float(top["amount1_WETH"]),
                "burns_rows": int(len(burns_df)),
            }

    # 2) Fallback: regime-change estimate
    if chosen["block"] is None and regime_block is not None:
        chosen["source"] = "regime_change"
        chosen["block"] = int(regime_block)
        chosen["time_utc"] = regime_time
        chosen["details"] = {
            "regime_json": str(regime_path),
            "before_level_weth": regime_before,
            "after_level_weth_24h": regime_after,
            "delta_weth": regime_delta_weth,
            "persistence_score": regime_persistence,
        }

    # 3) Fallback: largest negative jump between consecutive Sync events
    if chosen["block"] is None and cand_block is not None:
        chosen["source"] = "largest_sync_drop"
        chosen["block"] = int(cand_block)
        chosen["time_utc"] = cand_time
        chosen["details"] = {
            "candidate_json": str(cand_path),
            "delta_reserve1_weth": cand_delta_weth,
        }

    if chosen["block"] is None:
        raise RuntimeError(
            "Could not determine migration block. Missing/invalid inputs: "
            f"{cand_path.name}={cand is not None}, {regime_path.name}={regime is not None}, "
            f"{burns_path.name}={burns_status}"
        )

    out = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "rule": {
            "priority": [
                "largest_burn (amount1_WETH)",
                "regime_change (reserve1 median shift + 24h persistence)",
                "largest_sync_drop (delta_reserve1_weth between consecutive Sync events)",
            ],
            "notes": "This script selects a conservative 'migration block' for separating pre/post analyses. Supporting artifacts are included for traceability.",
        },
        "inputs": {
            "migration_candidate_json": str(cand_path),
            "migration_regime_change_json": str(regime_path),
            "migration03b_burns_csv": str(burns_path),
            "burns_csv_status": burns_status,
        },
        "selected": {
            "migration_block_final": int(chosen["block"]),
            "migration_time_utc": chosen["time_utc"],
            "selected_by": chosen["source"],
            "details": chosen["details"],
        },
        "supporting": {
            "candidate_block": cand_block,
            "candidate_time_utc": cand_time,
            "candidate_delta_reserve1_weth": cand_delta_weth,
            "regime_change_block_estimate": regime_block,
            "regime_change_time_utc": regime_time,
            "regime_change_delta_weth": regime_delta_weth,
        },
        "next_step": "Use migration_block_final to define pre-migration UniV2 window and post-migration UniV4 window for execution-quality (slippage) analysis.",
    }

    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote {out_path}")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
