import json
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def ensure_dirs(root: Path):
    (root / "data" / "raw").mkdir(parents=True, exist_ok=True)
    (root / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (root / "figures").mkdir(parents=True, exist_ok=True)


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def get_slippage_col(df: pd.DataFrame) -> str:
    # prefer your standard column
    if "slippage_excl_fees_pct_raw" in df.columns:
        return "slippage_excl_fees_pct_raw"
    # fallback variants (if you ever rename)
    for c in df.columns:
        if "slippage" in c and "fees" in c and "pct" in c:
            return c
    raise RuntimeError("Could not find a slippage (excluding fees) column in dataframe.")


def pick_migration_datetime(v4_df: pd.DataFrame, migration_block: int) -> pd.Timestamp:
    # Prefer exact migration block row in UniV4 post dataset
    if "block_number" in v4_df.columns:
        hit = v4_df[v4_df["block_number"] == migration_block]
        if not hit.empty:
            return hit["dt"].min()
    # Fallback: first timestamp in UniV4 post dataset
    return v4_df["dt"].min()


def plot_direction(df_all: pd.DataFrame, direction: str, migration_dt: pd.Timestamp, out_path: Path):
    d = df_all[df_all["direction"] == direction].copy()
    if d.empty:
        raise RuntimeError(f"No rows for direction={direction}")

    # Use consistent order in legend
    notionals = sorted(d["usd_notional_in"].unique())

    fig, ax = plt.subplots(figsize=(12.8, 6.2))

    for usd in notionals:
        dd = d[d["usd_notional_in"] == usd].sort_values("dt")
        ax.plot(
            dd["dt"],
            dd["slippage_excl_fees_pct_raw"].astype(float),
            linewidth=1.6,
            label=f"${int(usd):,}",
        )

    ax.axvline(migration_dt, color="black", linestyle="--", linewidth=1.2, alpha=0.85, label="Migration")

    ax.set_title(f"Execution quality over time (slippage excl. fees) — {direction}")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Slippage excl. fees (%)")
    ax.grid(True, alpha=0.25)

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))

    ax.legend(frameon=False, ncol=3)
    fig.tight_layout()

    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    print(f"Wrote {out_path}")


def main():
    root = Path(__file__).resolve().parents[1]
    ensure_dirs(root)

    raw = root / "data" / "raw"
    processed = root / "data" / "processed"
    figures = root / "figures"

    v2_csv = processed / "univ2_slippage_pre_usd.csv"
    v4_csv = processed / "univ4_slippage_post_usd.csv"
    mig_json = raw / "migration_block_final.json"

    if not v2_csv.exists():
        raise RuntimeError(f"Missing {v2_csv}. Run scripts/05_univ2_slippage_pre_usd.py first.")
    if not v4_csv.exists():
        raise RuntimeError(f"Missing {v4_csv}. Run scripts/06_univ4_slippage_post_usd.py first.")
    if not mig_json.exists():
        raise RuntimeError(f"Missing {mig_json}. Run scripts/04_write_migration_block_final.py first.")

    mig = load_json(mig_json)
    migration_block = int(mig["selected"]["migration_block_final"])

    v2 = pd.read_csv(v2_csv)
    v4 = pd.read_csv(v4_csv)

    # Normalize datetime
    for df in (v2, v4):
        df["dt"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        df.dropna(subset=["dt"], inplace=True)

    if v2.empty or v4.empty:
        raise RuntimeError("One of the slippage datasets is empty after datetime parsing.")

    # Ensure period column exists and is consistent
    if "period" not in v2.columns:
        v2["period"] = "UniV2 pre"
    if "period" not in v4.columns:
        v4["period"] = "UniV4 post"

    # Pick slippage column (and normalize name)
    v2_slip = get_slippage_col(v2)
    v4_slip = get_slippage_col(v4)
    v2 = v2.rename(columns={v2_slip: "slippage_excl_fees_pct_raw"})
    v4 = v4.rename(columns={v4_slip: "slippage_excl_fees_pct_raw"})

    # Combine
    df_all = pd.concat([v2, v4], ignore_index=True)
    df_all["usd_notional_in"] = df_all["usd_notional_in"].astype(float)

    # Migration line datetime from UniV4 post data
    migration_dt = pick_migration_datetime(v4, migration_block)

    # Summary table (median, p90, n)
    grp = (
        df_all.groupby(["period", "direction", "usd_notional_in"], dropna=False)["slippage_excl_fees_pct_raw"]
        .agg(
            median=lambda s: float(np.nanmedian(s.astype(float).values)),
            p90=lambda s: float(np.nanpercentile(s.astype(float).values, 90)),
            n=lambda s: int(np.isfinite(s.astype(float).values).sum()),
        )
        .reset_index()
        .sort_values(["period", "direction", "usd_notional_in"])
        .reset_index(drop=True)
    )

    out_summary = processed / "execution_quality_summary.csv"
    grp.to_csv(out_summary, index=False)
    print(f"Wrote {out_summary}")
    print(grp.to_string(index=False))

    # Plots (two directions)
    out_eth = figures / "execution_quality_slippage_ETH_to_IXS.png"
    out_ixs = figures / "execution_quality_slippage_IXS_to_ETH.png"

    plot_direction(df_all, "ETH->IXS", migration_dt, out_eth)
    plot_direction(df_all, "IXS->ETH", migration_dt, out_ixs)


if __name__ == "__main__":
    main()
