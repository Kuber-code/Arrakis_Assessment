from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def load_csv(path: Path, venue: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # Robust datetime parsing; keep everything in UTC
    if "datetime_utc" in df.columns:
        df["dt"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")  # [web:518]
    else:
        df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True, errors="coerce")  # [web:518]

    df["venue_period"] = venue
    df["usd_notional_in"] = df["usd_notional_in"].astype(float)
    df["slippage_pct"] = pd.to_numeric(df["slippage_pct"], errors="coerce")
    df = df.dropna(subset=["dt", "slippage_pct", "direction", "usd_notional_in"]).copy()
    return df


def plot_direction(df_all: pd.DataFrame, direction: str, out_path: Path):
    df = df_all[df_all["direction"] == direction].copy()
    if df.empty:
        raise RuntimeError(f"No rows for direction={direction}")

    notionals = sorted(df["usd_notional_in"].unique())
    venues = ["UniV2_pre", "UniV4_post"]

    fig, ax = plt.subplots(figsize=(12, 6))

    # Style: color by notional, line style by venue
    colors = plt.cm.tab10(np.linspace(0, 1, max(3, len(notionals))))
    notional_to_color = {n: colors[i % len(colors)] for i, n in enumerate(notionals)}
    venue_style = {"UniV2_pre": "-", "UniV4_post": "--"}

    for n in notionals:
        for v in venues:
            d = df[(df["usd_notional_in"] == n) & (df["venue_period"] == v)].sort_values("dt")
            if d.empty:
                continue
            ax.plot(
                d["dt"],
                d["slippage_pct"],
                linestyle=venue_style.get(v, "-"),
                color=notional_to_color[n],
                linewidth=1.3,
                alpha=0.9,
                label=f"${int(n):,} {v}",
            )

    ax.set_title(f"Slippage over time — {direction}")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Slippage (%)")

    # Make dates readable
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
    fig.autofmt_xdate()  # rotate/align labels for readability [web:525]

    ax.grid(True, which="major", alpha=0.25)
    ax.legend(ncol=2, fontsize=8, frameon=False)

    fig.tight_layout()
    fig.savefig(out_path, dpi=160)  # save plot to file [web:514]
    plt.close(fig)


def summary_table(df_all: pd.DataFrame) -> pd.DataFrame:
    # Median and p90 by venue/direction/notional
    def p90(x):
        return float(np.nanquantile(x, 0.90))

    g = (
        df_all.groupby(["venue_period", "direction", "usd_notional_in"])["slippage_pct"]
        .agg(median="median", p90=p90, n="count")
        .reset_index()
        .sort_values(["direction", "usd_notional_in", "venue_period"])
    )
    return g


def main():
    root = Path(__file__).resolve().parents[1]
    processed = root / "data" / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    v2_csv = processed / "univ2_slippage_pre_usd.csv"
    v4_csv = processed / "univ4_slippage_post_usd.csv"
    if not v2_csv.exists():
        raise RuntimeError(f"Missing {v2_csv}")
    if not v4_csv.exists():
        raise RuntimeError(f"Missing {v4_csv}")

    df_v2 = load_csv(v2_csv, "UniV2_pre")
    df_v4 = load_csv(v4_csv, "UniV4_post")
    df_all = pd.concat([df_v2, df_v4], ignore_index=True)

    # Write summary stats for report
    stats = summary_table(df_all)
    stats_csv = processed / "execution_quality_summary.csv"
    stats.to_csv(stats_csv, index=False)
    print(f"Wrote {stats_csv}")
    print(stats.head(12).to_string(index=False))

    # Two plots (one per direction) into processed/
    directions = sorted(df_all["direction"].unique())
    for direction in directions:
        safe_name = direction.replace("->", "_to_")
        out_png = processed / f"execution_quality_slippage_{safe_name}.png"
        plot_direction(df_all, direction, out_png)
        print(f"Wrote {out_png}")

    # Optional: also write merged data for convenience
    merged_csv = processed / "execution_quality_all.csv"
    df_all.to_csv(merged_csv, index=False)
    print(f"Wrote {merged_csv}")


if __name__ == "__main__":
    main()
