import json
from pathlib import Path

import numpy as np
import pandas as pd


def cpmm_amount_out_no_fee(amount_in: float, reserve_in: float, reserve_out: float) -> float:
    if amount_in <= 0:
        return 0.0
    if reserve_in <= 0 or reserve_out <= 0:
        return 0.0
    k = reserve_in * reserve_out
    new_reserve_in = reserve_in + amount_in
    new_reserve_out = k / new_reserve_in
    out = reserve_out - new_reserve_out
    return max(0.0, out)


def slippage_pct(spot_price: float, avg_exec_price: float) -> float:
    if spot_price <= 0:
        return np.nan
    return (avg_exec_price - spot_price) / spot_price * 100.0


def main():
    root = Path(__file__).resolve().parents[1]
    processed = root / "data" / "processed"
    raw = root / "data" / "raw"

    sync_csv = processed / "univ2_sync_timeseries.csv"
    meta_json = raw / "univ2_pair_metadata.json"
    mig_final_json = processed / "migration_block_final.json"
    eth_usd_csv = processed / "eth_usd_chainlink_answers.csv"

    out_csv = processed / "univ2_slippage_pre_usd.csv"

    df = pd.read_csv(sync_csv)
    meta = json.loads(meta_json.read_text(encoding="utf-8"))
    mig = json.loads(mig_final_json.read_text(encoding="utf-8"))
    eth = pd.read_csv(eth_usd_csv)

    mig_block = int(mig["selected"]["migration_block_final"])
    sym0 = meta["token0"]["symbol"]  # IXS
    sym1 = meta["token1"]["symbol"]  # WETH

    # Pre-migration only
    df["block_number"] = df["block_number"].astype(int)
    df = df[df["block_number"] < mig_block].copy()
    if df.empty:
        raise RuntimeError("No pre-migration rows after filtering. Check migration_block_final vs CSV range.")

    # Align ETH/USD series by block_number using asof (previous known price)
    eth = eth.sort_values("block_number").reset_index(drop=True)
    eth = eth[["block_number", "eth_usd"]].dropna()
    if eth.empty:
        raise RuntimeError("ETH/USD Chainlink series is empty.")

    df = df.sort_values("block_number").reset_index(drop=True)
    df = pd.merge_asof(df, eth, on="block_number", direction="backward")
    if df["eth_usd"].isna().all():
        raise RuntimeError("Could not asof-merge ETH/USD into UniV2 rows (all NaN).")

    # Sample for speed (optional)
    max_points = 800
    if len(df) > max_points:
        idx = np.linspace(0, len(df) - 1, max_points).astype(int)
        df = df.iloc[idx].reset_index(drop=True)

    usd_sizes = [1000, 5000, 10000, 50000]

    rows = []
    for _, r in df.iterrows():
        reserve0 = float(r["reserve0"])  # IXS
        reserve1 = float(r["reserve1"])  # WETH
        block = int(r["block_number"])
        ts = int(r["timestamp"])
        dt = r.get("datetime_utc", None)
        eth_usd = float(r["eth_usd"])

        # Spot prices
        # spot IXS->WETH: WETH per IXS = reserve1/reserve0
        spot_ixs_to_weth = reserve1 / reserve0 if reserve0 > 0 else np.nan
        # spot IXS/USD = (WETH per IXS) * (USD per WETH)
        spot_ixs_usd = spot_ixs_to_weth * eth_usd if np.isfinite(spot_ixs_to_weth) else np.nan

        for usd_notional in usd_sizes:
            # Direction: IXS -> WETH, amount_in is USD/ (IXS/USD)
            amount_in_ixs = (usd_notional / spot_ixs_usd) if (spot_ixs_usd and spot_ixs_usd > 0) else np.nan
            out_weth = cpmm_amount_out_no_fee(amount_in_ixs, reserve0, reserve1) if np.isfinite(amount_in_ixs) else np.nan

            # prices in USD terms for slippage:
            # spot price (USD per WETH) or (USD per IXS) can be used, but we keep it consistent:
            # Compare average execution USD/WETH vs spot USD/WETH for IXS->WETH:
            # - avg exec: USD_notional / out_weth = USD per WETH
            # - spot: eth_usd = USD per WETH
            avg_exec_usd_per_weth = (usd_notional / out_weth) if (out_weth and out_weth > 0) else np.nan
            spot_usd_per_weth = eth_usd
            slip = slippage_pct(spot_usd_per_weth, avg_exec_usd_per_weth)

            rows.append(
                {
                    "block_number": block,
                    "timestamp": ts,
                    "datetime_utc": dt,
                    "direction": f"{sym0}->{sym1}",
                    "usd_notional_in": float(usd_notional),
                    "amount_in": float(amount_in_ixs) if np.isfinite(amount_in_ixs) else np.nan,
                    "amount_in_unit": sym0,
                    "amount_out": float(out_weth) if np.isfinite(out_weth) else np.nan,
                    "amount_out_unit": sym1,
                    "spot_price": float(spot_usd_per_weth),
                    "spot_price_unit": "USD_per_WETH",
                    "avg_exec_price": float(avg_exec_usd_per_weth) if np.isfinite(avg_exec_usd_per_weth) else np.nan,
                    "avg_exec_price_unit": "USD_per_WETH",
                    "slippage_pct": float(slip) if np.isfinite(slip) else np.nan,
                    "fees_included": False,
                }
            )

            # Direction: WETH -> IXS, amount_in is USD/(USD per WETH)
            amount_in_weth = usd_notional / eth_usd if eth_usd > 0 else np.nan
            out_ixs = cpmm_amount_out_no_fee(amount_in_weth, reserve1, reserve0) if np.isfinite(amount_in_weth) else np.nan

            # Compare avg exec USD/IXS vs spot USD/IXS:
            # - avg exec: USD_notional / out_ixs = USD per IXS
            # - spot: spot_ixs_usd = USD per IXS
            avg_exec_usd_per_ixs = (usd_notional / out_ixs) if (out_ixs and out_ixs > 0) else np.nan
            spot_usd_per_ixs = spot_ixs_usd
            slip2 = slippage_pct(spot_usd_per_ixs, avg_exec_usd_per_ixs) if (spot_usd_per_ixs and spot_usd_per_ixs > 0) else np.nan

            rows.append(
                {
                    "block_number": block,
                    "timestamp": ts,
                    "datetime_utc": dt,
                    "direction": f"{sym1}->{sym0}",
                    "usd_notional_in": float(usd_notional),
                    "amount_in": float(amount_in_weth) if np.isfinite(amount_in_weth) else np.nan,
                    "amount_in_unit": sym1,
                    "amount_out": float(out_ixs) if np.isfinite(out_ixs) else np.nan,
                    "amount_out_unit": sym0,
                    "spot_price": float(spot_usd_per_ixs) if (spot_usd_per_ixs and np.isfinite(spot_usd_per_ixs)) else np.nan,
                    "spot_price_unit": "USD_per_IXS",
                    "avg_exec_price": float(avg_exec_usd_per_ixs) if np.isfinite(avg_exec_usd_per_ixs) else np.nan,
                    "avg_exec_price_unit": "USD_per_IXS",
                    "slippage_pct": float(slip2) if np.isfinite(slip2) else np.nan,
                    "fees_included": False,
                }
            )

    out = pd.DataFrame(rows)
    out.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")
    print(out.head(8).to_string(index=False))


if __name__ == "__main__":
    main()
