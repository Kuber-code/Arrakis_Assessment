from pathlib import Path
import json

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.lines import Line2D


METRIC_COL = "slippage_excl_fees_pct_raw"  # definition-required metric


def load_csv(path: Path, period_label: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # Robust datetime parsing; keep everything in UTC
    if "datetime_utc" in df.columns:
        df["dt"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    else:
        df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True, errors="coerce")

    # Normalize direction labels (treat WETH as ETH for presentation)
    if "direction" not in df.columns:
        raise RuntimeError(f"Missing 'direction' in {path}")

    df["direction"] = (
        df["direction"]
        .astype(str)
        .str.replace("WETH", "ETH", regex=False)
        .str.replace("weth", "ETH", regex=False)
    )

    if "usd_notional_in" not in df.columns:
        raise RuntimeError(f"Missing 'usd_notional_in' in {path}")
    df["usd_notional_in"] = df["usd_notional_in"].astype(float)

    if METRIC_COL not in df.columns:
        raise RuntimeError(f"Missing '{METRIC_COL}' in {path}. (Expected new slippage output schema.)")
    df[METRIC_COL] = pd.to_numeric(df[METRIC_COL], errors="coerce")

    df["period"] = period_label

    # Keep only valid rows
    df = df.dropna(subset=["dt", METRIC_COL, "direction", "usd_notional_in"]).copy()
    return df


def load_migration_time(root: Path):
    mig_path = root / "data" / "processed" / "migration_block_final.json"
    if not mig_path.exists():
        return None
    mig = json.loads(mig_path.read_text(encoding="utf-8"))
    t = mig.get("selected", {}).get("migration_time_utc")
    if not t:
        return None
    dt = pd.to_datetime(t, utc=True, errors="coerce")
    if pd.isna(dt):
        return None
    return dt


def _legend_handles(notionals, colors, period_styles):
    # Custom legend: one legend for notionals (colors), one for period (linestyles)
    handles_sizes = [
        Line2D([0], [0], color=colors[n], lw=2, linestyle="-", label=f"${int(n):,}")
        for n in notionals
    ]
    handles_periods = [
        Line2D([0], [0], color="black", lw=2, linestyle=ls, label=lbl)
        for lbl, ls in period_styles.items()
    ]
    return handles_sizes, handles_periods


def plot_direction(df_all: pd.DataFrame, direction: str, out_path: Path, mig_dt=None):
    df = df_all[df_all["direction"] == direction].copy()
    if df.empty:
        raise RuntimeError(f"No rows for direction={direction}")

    notionals = sorted(df["usd_notional_in"].unique())
    periods = ["UniV2 pre", "UniV4 post"]

    # Color by notional, linestyle by period
    palette = plt.cm.tab10(np.linspace(0, 1, max(3, len(notionals))))
    colors = {n: palette[i % len(palette)] for i, n in enumerate(notionals)}
    period_styles = {"UniV2 pre": "-", "UniV4 post": "--"}

    fig, ax = plt.subplots(figsize=(12.5, 6.5))

    for n in notionals:
        for p in periods:
            d = df[(df["usd_notional_in"] == n) & (df["period"] == p)].sort_values("dt")
            if d.empty:
                continue
            ax.plot(
                d["dt"],
                d[METRIC_COL],
                linestyle=period_styles.get(p, "-"),
                color=colors[n],
                linewidth=1.3,
                alpha=0.9,
            )

    if mig_dt is not None:
        ax.axvline(mig_dt, color="grey", linewidth=1.0, alpha=0.7)
        ax.text(
            mig_dt,
            0.98,
            "Migration",
            transform=ax.get_xaxis_transform(),
            ha="left",
            va="top",
            fontsize=9,
            color="grey",
        )

    ax.set_title(f"Execution quality over time — {direction}")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Slippage excluding fees (%)")

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
    fig.autofmt_xdate()

    ax.grid(True, which="major", alpha=0.25)

    # Two-part legend
    handles_sizes, handles_periods = _legend_handles(notionals, colors, period_styles)
    leg1 = ax.legend(handles=handles_sizes, loc="upper left", fontsize=9, frameon=False, title="Trade size")
    ax.add_artist(leg1)
    ax.legend(handles=handles_periods, loc="upper right", fontsize=9, frameon=False, title="Period")

    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def summary_table(df_all: pd.DataFrame) -> pd.DataFrame:
    def p90(x):
        return float(np.nanquantile(x, 0.90))

    g = (
        df_all.groupby(["period", "direction", "usd_notional_in"])[METRIC_COL]
        .agg(median="median", p90=p90, n="count")
        .reset_index()
        .sort_values(["direction", "usd_notional_in", "period"])
        .reset_index(drop=True)
    )
    return g


def main():
    root = Path(__file__).resolve().parents[1]
    processed = root / "data" / "processed"
    figures = root / "figures"

    v2_csv = processed / "univ2_slippage_pre_usd.csv"
    v4_csv = processed / "univ4_slippage_post_usd.csv"
    if not v2_csv.exists():
        raise RuntimeError(f"Missing {v2_csv}")
    if not v4_csv.exists():
        raise RuntimeError(f"Missing {v4_csv}")

    df_v2 = load_csv(v2_csv, "UniV2 pre")
    df_v4 = load_csv(v4_csv, "UniV4 post")
    df_all = pd.concat([df_v2, df_v4], ignore_index=True)

    # Persist merged data for convenience
    merged_csv = processed / "execution_quality_all.csv"
    df_all.to_csv(merged_csv, index=False)
    print(f"Wrote {merged_csv}")

    # Summary stats for report
    stats = summary_table(df_all)
    stats_csv = processed / "execution_quality_summary.csv"
    stats.to_csv(stats_csv, index=False)
    print(f"Wrote {stats_csv}")
    print(stats.head(12).to_string(index=False))

    mig_dt = load_migration_time(root)

    # One plot per direction
    directions = sorted(df_all["direction"].unique())
    for direction in directions:
        safe_name = direction.replace("->", "_to_").replace("/", "_")
        out_png = figures / f"execution_quality_slippage_{safe_name}.png"
        plot_direction(df_all, direction, out_png, mig_dt=mig_dt)
        print(f"Wrote {out_png}")


if __name__ == "__main__":
    main()
