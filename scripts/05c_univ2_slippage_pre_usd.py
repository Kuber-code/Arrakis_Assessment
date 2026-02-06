import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

# Uniswap V2 swap fee is 0.30% (0.003)
UNIV2_FEE = 0.003


def cpmm_amount_out_with_fee(amount_in: float, reserve_in: float, reserve_out: float, fee: float = UNIV2_FEE) -> float:
    """
    Constant product AMM quote with fee taken from input amount (Uniswap V2 style).
    amount_out = reserve_out * amount_in_eff / (reserve_in + amount_in_eff)
    """
    if not np.isfinite(amount_in) or amount_in <= 0:
        return 0.0
    if not np.isfinite(reserve_in) or not np.isfinite(reserve_out) or reserve_in <= 0 or reserve_out <= 0:
        return 0.0

    amount_in_eff = amount_in * (1.0 - fee)
    out = reserve_out * amount_in_eff / (reserve_in + amount_in_eff)
    return float(max(0.0, out))


def slippage_excl_fees_pct(spot_out_per_in: float, avg_out_per_in: float, fee: float = UNIV2_FEE) -> tuple[float, float]:
    """
    Assessment definition:
      slippage_excl_fees = |spot - avg| / spot * 100 - fee*100
    We return: (gross_slippage_pct, slippage_excl_fees_pct_raw)

    Notes:
    - spot_out_per_in uses the instantaneous pool price (reserve_out/reserve_in).
    - avg_out_per_in uses quoted execution including fees and price impact.
    - Subtracting fee isolates price impact (may go slightly negative for tiny trades; keep raw value).
    """
    if not np.isfinite(spot_out_per_in) or spot_out_per_in <= 0:
        return (np.nan, np.nan)
    if not np.isfinite(avg_out_per_in) or avg_out_per_in <= 0:
        return (np.nan, np.nan)

    gross = abs(spot_out_per_in - avg_out_per_in) / spot_out_per_in * 100.0
    net_raw = gross - fee * 100.0
    return (float(gross), float(net_raw))


def main():
    root = Path(__file__).resolve().parents[1]
    processed = root / "data" / "processed"
    raw = root / "data" / "raw"

    sync_csv = processed / "univ2_sync_timeseries.csv"
    meta_json = raw / "univ2_pair_metadata.json"
    mig_final_json = processed / "migration_block_final.json"
    eth_usd_csv = processed / "eth_usd_univ3_slot0.csv"  # expected columns: block_number, eth_usd

    out_csv = processed / "univ2_slippage_pre_usd.csv"
    processed.mkdir(parents=True, exist_ok=True)

    if not sync_csv.exists():
        raise RuntimeError(f"Missing {sync_csv}. Run script 02 first.")
    if not meta_json.exists():
        raise RuntimeError(f"Missing {meta_json}. Run script 01 first.")
    if not mig_final_json.exists():
        raise RuntimeError(f"Missing {mig_final_json}. Run script 04b first.")
    if not eth_usd_csv.exists():
        raise RuntimeError(f"Missing {eth_usd_csv}. Generate ETH/USD series first (UniV3 slot0 script).")

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

    # ETH/USD as-of merge by block_number (previous known)
    if "eth_usd" not in eth.columns:
        raise RuntimeError("ETH/USD CSV missing 'eth_usd' column.")
    eth["block_number"] = eth["block_number"].astype(int)
    eth = eth.sort_values("block_number")[["block_number", "eth_usd"]].dropna()
    if eth.empty:
        raise RuntimeError("ETH/USD series is empty after selecting (block_number, eth_usd).")

    df = df.sort_values("block_number").reset_index(drop=True)
    df = pd.merge_asof(df, eth, on="block_number", direction="backward")
    df = df.dropna(subset=["eth_usd"]).copy()
    if df.empty:
        raise RuntimeError("No rows left after merging ETH/USD. Check block ranges / ETH/USD coverage.")

    # Optional sampling for speed/readability
    max_points = int(os.environ.get("UNIV2_MAX_POINTS", "800"))
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

        if reserve0 <= 0 or reserve1 <= 0 or eth_usd <= 0:
            continue

        # Pool spot prices (out per in)
        spot_weth_per_ixs = reserve1 / reserve0          # WETH per IXS
        spot_ixs_per_weth = reserve0 / reserve1          # IXS per WETH

        # For sizing trades in USD:
        spot_ixs_usd = spot_weth_per_ixs * eth_usd       # USD per IXS (using pool spot + ETH/USD)

        for usd_notional in usd_sizes:
            usdN = float(usd_notional)

            # -------- Direction 1: IXS -> WETH --------
            amount_in_ixs = usdN / spot_ixs_usd if spot_ixs_usd > 0 else np.nan
            out_weth = cpmm_amount_out_with_fee(amount_in_ixs, reserve0, reserve1, fee=UNIV2_FEE)

            avg_weth_per_ixs = (out_weth / amount_in_ixs) if (np.isfinite(amount_in_ixs) and amount_in_ixs > 0 and out_weth > 0) else np.nan
            gross1, net1_raw = slippage_excl_fees_pct(spot_weth_per_ixs, avg_weth_per_ixs, fee=UNIV2_FEE)

            rows.append(
                {
                    "block_number": block,
                    "timestamp": ts,
                    "datetime_utc": dt,
                    "direction": f"{sym0}->{sym1}",
                    "usd_notional_in": usdN,
                    "eth_usd": eth_usd,
                    "amount_in": float(amount_in_ixs) if np.isfinite(amount_in_ixs) else np.nan,
                    "amount_in_unit": sym0,
                    "amount_out": float(out_weth) if np.isfinite(out_weth) else np.nan,
                    "amount_out_unit": sym1,
                    "spot_price": float(spot_weth_per_ixs),
                    "spot_price_unit": f"{sym1}_per_{sym0}",
                    "avg_exec_price": float(avg_weth_per_ixs) if np.isfinite(avg_weth_per_ixs) else np.nan,
                    "avg_exec_price_unit": f"{sym1}_per_{sym0}",
                    "gross_slippage_pct": gross1,
                    "slippage_excl_fees_pct_raw": net1_raw,
                    "fee_rate": UNIV2_FEE,
                }
            )

            # -------- Direction 2: WETH -> IXS --------
            amount_in_weth = usdN / eth_usd if eth_usd > 0 else np.nan
            out_ixs = cpmm_amount_out_with_fee(amount_in_weth, reserve1, reserve0, fee=UNIV2_FEE)

            avg_ixs_per_weth = (out_ixs / amount_in_weth) if (np.isfinite(amount_in_weth) and amount_in_weth > 0 and out_ixs > 0) else np.nan
            gross2, net2_raw = slippage_excl_fees_pct(spot_ixs_per_weth, avg_ixs_per_weth, fee=UNIV2_FEE)

            rows.append(
                {
                    "block_number": block,
                    "timestamp": ts,
                    "datetime_utc": dt,
                    "direction": f"{sym1}->{sym0}",
                    "usd_notional_in": usdN,
                    "eth_usd": eth_usd,
                    "amount_in": float(amount_in_weth) if np.isfinite(amount_in_weth) else np.nan,
                    "amount_in_unit": sym1,
                    "amount_out": float(out_ixs) if np.isfinite(out_ixs) else np.nan,
                    "amount_out_unit": sym0,
                    "spot_price": float(spot_ixs_per_weth),
                    "spot_price_unit": f"{sym0}_per_{sym1}",
                    "avg_exec_price": float(avg_ixs_per_weth) if np.isfinite(avg_ixs_per_weth) else np.nan,
                    "avg_exec_price_unit": f"{sym0}_per_{sym1}",
                    "gross_slippage_pct": gross2,
                    "slippage_excl_fees_pct_raw": net2_raw,
                    "fee_rate": UNIV2_FEE,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("Produced 0 slippage rows. Check inputs and ETH/USD coverage.")

    out = out.sort_values(["block_number", "direction", "usd_notional_in"]).reset_index(drop=True)
    out.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")
    print(out.head(12).to_string(index=False))


if __name__ == "__main__":
    main()
