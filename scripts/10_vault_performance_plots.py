from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def ensure_dirs(root: Path):
    (root / "data" / "raw").mkdir(parents=True, exist_ok=True)
    (root / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (root / "figures").mkdir(parents=True, exist_ok=True)


def main():
    root = Path(__file__).resolve().parents[1]
    ensure_dirs(root)

    processed = root / "data" / "processed"
    figures = root / "figures"

    ts_csv = processed / "vault_timeseries.csv"
    if not ts_csv.exists():
        raise RuntimeError(f"Missing {ts_csv}. Run scripts/09_vault_timeseries.py first.")

    df = pd.read_csv(ts_csv)
    df["dt"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["dt"]).copy()

    if df.empty:
        raise RuntimeError("vault_timeseries.csv has no valid datetimes.")

    # ---- 1) Underlying amounts over time ----
    fig, ax1 = plt.subplots(figsize=(12.5, 6.0))
    ax2 = ax1.twinx()

    l1 = ax1.plot(df["dt"], df["amt_eth"].astype(float), label="ETH amount", linewidth=1.8, color="#1f77b4")
    l2 = ax2.plot(df["dt"], df["amt_ixs"].astype(float), label="IXS amount", linewidth=1.8, color="#ff7f0e")

    ax1.set_title("Arrakis vault underlying amounts over time")
    ax1.set_xlabel("Time (UTC)")
    ax1.set_ylabel("ETH")
    ax2.set_ylabel("IXS")
    ax1.grid(True, alpha=0.25)

    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax1.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax1.xaxis.get_major_locator()))

    lines = l1 + l2
    labels = [ln.get_label() for ln in lines]
    ax1.legend(lines, labels, frameon=False, loc="upper left")

    fig.tight_layout()
    out1 = figures / "vault_token_amounts_over_time.png"
    fig.savefig(out1, dpi=180)
    plt.close(fig)
    print(f"Wrote {out1}")

    # ---- 2) Value composition over time (USD) ----
    fig, ax = plt.subplots(figsize=(12.5, 6.0))

    eth_val = df["value_eth_usd"].astype(float).values
    ixs_val = df["value_ixs_usd"].astype(float).values

    ax.stackplot(
        df["dt"].values,
        eth_val,
        ixs_val,
        labels=["ETH value (USD)", "IXS value (USD)"],
        alpha=0.85,
    )

    ax.set_title("Vault composition over time (USD value of ETH vs IXS)")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("USD value")
    ax.grid(True, alpha=0.25)
    ax.legend(frameon=False, loc="upper left")

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))

    fig.tight_layout()
    out2 = figures / "vault_value_composition_over_time.png"
    fig.savefig(out2, dpi=180)
    plt.close(fig)
    print(f"Wrote {out2}")

    # ---- 3) Performance vs hold + full-range baseline (indexed) ----
    fig, ax = plt.subplots(figsize=(12.5, 6.0))

    ax.plot(df["dt"], df["vault_value_index"].astype(float), label="Vault (index)", linewidth=2.2)
    ax.plot(df["dt"], df["hold_value_index"].astype(float), label="Hold initial amounts (index)", linewidth=1.8)

    if "full_range_lp_value_index" in df.columns and df["full_range_lp_value_index"].notna().any():
        ax.plot(
            df["dt"],
            df["full_range_lp_value_index"].astype(float),
            label="Full-range LP (no fees) baseline (index)",
            linewidth=1.8,
        )

    ax.set_title("Vault performance vs hold and full-range LP baseline (indexed)")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Value index (t0 = 1.0)")
    ax.grid(True, alpha=0.25)
    ax.legend(frameon=False)

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))

    fig.tight_layout()
    out3 = figures / "vault_performance_index.png"
    fig.savefig(out3, dpi=180)
    plt.close(fig)
    print(f"Wrote {out3}")

    # ---- 4) Compact summary table ----
    last = df.iloc[-1]
    first = df.iloc[0]

    summary = pd.DataFrame(
        [
            {
                "t0_datetime_utc": str(first["datetime_utc"]),
                "t1_datetime_utc": str(last["datetime_utc"]),
                "vault_value_usd_t0": float(first["value_total_usd"]),
                "vault_value_usd_t1": float(last["value_total_usd"]),
                "vault_index_t1": float(last["vault_value_index"]),
                "hold_index_t1": float(last["hold_value_index"]),
                "full_range_lp_index_t1": float(last["full_range_lp_value_index"])
                if np.isfinite(float(last.get("full_range_lp_value_index", np.nan)))
                else np.nan,
                "amt_eth_t0": float(first["amt_eth"]),
                "amt_eth_t1": float(last["amt_eth"]),
                "amt_ixs_t0": float(first["amt_ixs"]),
                "amt_ixs_t1": float(last["amt_ixs"]),
                "mapping_mode": str(df["underlying_mapping_mode"].mode().iloc[0])
                if "underlying_mapping_mode" in df.columns and not df["underlying_mapping_mode"].isna().all()
                else "",
            }
        ]
    )

    out_sum = processed / "vault_performance_summary.csv"
    summary.to_csv(out_sum, index=False)
    print(f"Wrote {out_sum}")


if __name__ == "__main__":
    main()
