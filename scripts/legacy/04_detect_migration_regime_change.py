import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


def main():
    root = Path(__file__).resolve().parents[1]

    inp = root / "data" / "processed" / "univ2_sync_timeseries.csv"
    out_csv = root / "data" / "processed" / "univ2_reserve1_binned_30m.csv"
    out_json = root / "data" / "processed" / "migration_regime_change.json"

    df = pd.read_csv(inp)

    required_cols = {"block_number", "timestamp", "datetime_utc", "reserve1"}
    missing = required_cols - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing columns in {inp}: {missing}")

    # Ensure types
    df["block_number"] = df["block_number"].astype(int)
    df["timestamp"] = df["timestamp"].astype(int)
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df["reserve1"] = df["reserve1"].astype(float)

    # ---- 1) Aggregate to 30-minute bins (median reduces noise/outliers) ----
    df = df.sort_values("datetime_utc").reset_index(drop=True)
    df["bin_30m"] = df["datetime_utc"].dt.floor("30min")

    g = (
        df.groupby("bin_30m", as_index=False)
        .agg(
            reserve1_median=("reserve1", "median"),
            reserve1_min=("reserve1", "min"),
            reserve1_max=("reserve1", "max"),
            block_median=("block_number", "median"),
            block_min=("block_number", "min"),
            block_max=("block_number", "max"),
            n_obs=("reserve1", "size"),
        )
        .sort_values("bin_30m")
        .reset_index(drop=True)
    )

    if len(g) < 120:
        # 120 bins = 60 hours at 30min resolution
        print("Warning: very short series; consider expanding the window in step 02.")
    g.to_csv(out_csv, index=False)

    # ---- 2) Detect regime shift: search for split point with biggest level change ----
    # We want a change that persists for at least 24h => 48 bins after the change.
    persist_bins = 48  # 24h at 30min bins
    min_bins_each_side = 48  # require at least 24h of history before change too

    x = g["reserve1_median"].to_numpy()

    n = len(x)
    candidates = []
    for i in range(min_bins_each_side, n - persist_bins):
        before = x[:i]
        after = x[i : i + persist_bins]  # only the first 24h after the change

        before_level = float(np.median(before))
        after_level = float(np.median(after))

        # magnitude of shift
        delta = after_level - before_level
        delta_abs = abs(delta)

        # persistence check: after period should stay closer to after_level than before_level on average
        after_dev = float(np.median(np.abs(after - after_level)))
        before_dev = float(np.median(np.abs(after - before_level)))
        persistence_score = before_dev - after_dev  # >0 means "after looks more like after_level"

        candidates.append(
            {
                "i": i,
                "bin_time": str(g.loc[i, "bin_30m"]),
                "before_level": before_level,
                "after_level_24h": after_level,
                "delta": delta,
                "delta_abs": delta_abs,
                "persistence_score": persistence_score,
                "block_min_at_bin": int(g.loc[i, "block_min"]),
                "block_max_at_bin": int(g.loc[i, "block_max"]),
            }
        )

    cand_df = pd.DataFrame(candidates)
    if cand_df.empty:
        raise RuntimeError("No candidates for change point (series too short). Expand scan window in step 02.")

    # Filter: persistence_score must be positive (after window resembles new level)
    filtered = cand_df[cand_df["persistence_score"] > 0].copy()
    if filtered.empty:
        # fallback: take max delta_abs anyway, but mark as weak
        best = cand_df.sort_values(["delta_abs"], ascending=False).iloc[0].to_dict()
        confidence = "weak"
        reason = "No candidate passed persistence_score>0; taking max absolute level shift as fallback."
    else:
        # Choose the biggest level shift among persistent candidates
        best = filtered.sort_values(["delta_abs", "persistence_score"], ascending=False).iloc[0].to_dict()
        confidence = "medium"
        reason = "Selected as largest persistent 24h level shift in reserve1_median (WETH) at 30min resolution."

    # ---- 3) Build output recommendation ----
    # migration block estimate: take block_min at the bin where change starts
    migration_block_est = int(best["block_min_at_bin"])
    migration_time_est = best["bin_time"]

    out = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "method": {
            "signal": "UniswapV2 reserve1 (token1=WETH) median",
            "binning": "30min bins, median aggregation",
            "persistence_rule": "post-change median level must persist for 24h (48 bins)",
            "min_history": "require >=24h of bins before candidate",
        },
        "input_csv": str(inp),
        "binned_csv": str(out_csv),
        "best_change_point": {
            "bin_time_utc": migration_time_est,
            "migration_block_estimate": migration_block_est,
            "before_level_weth": float(best["before_level"]),
            "after_level_weth_24h": float(best["after_level_24h"]),
            "delta_weth": float(best["delta"]),
            "abs_delta_weth": float(best["delta_abs"]),
            "persistence_score": float(best["persistence_score"]),
            "block_range_at_bin": [int(best["block_min_at_bin"]), int(best["block_max_at_bin"])],
        },
        "confidence": confidence,
        "note": reason,
        "next_step": "Validate this estimate by expanding to 90/180d window and by checking for Mint/Burn/large reserve drop around the estimated block.",
    }

    out_json.write_text(json.dumps(out, indent=2))
    print(f"Wrote {out_json}")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
