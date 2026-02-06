import json
from pathlib import Path

import numpy as np
import pandas as pd


def cpmm_amount_out_no_fee(amount_in: float, reserve_in: float, reserve_out: float) -> float:
    """
    Constant product AMM quote WITHOUT fees (pure x*y=k).
    out = reserve_out - k/(reserve_in + amount_in)
    """
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
    """
    slippage% = (avg_exec - spot) / spot * 100
    """
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

    out_csv = processed / "univ2_slippage_pre.csv"

    df = pd.read_csv(sync_csv)
    meta = json.loads(meta_json.read_text(encoding="utf-8"))
    mig = json.loads(mig_final_json.read_text(encoding="utf-8"))

    mig_block = int(mig["selected"]["migration_block_final"])

    sym0 = meta["token0"]["symbol"]  # IXS
    sym1 = meta["token1"]["symbol"]  # WETH

    # Pre-migration only
    df["block_number"] = df["block_number"].astype(int)
    df = df[df["block_number"] < mig_block].copy()
    if df.empty:
        raise RuntimeError("No pre-migration rows after filtering. Check migration_block_final vs CSV range.")

    # We'll sample at most N points for speed/readability (you can increase later)
    # Use evenly spaced indices across the pre window
    max_points = 600  # ~enough for a smooth line; keeps runtime reasonable
    df = df.sort_values(["block_number", "log_index"]).reset_index(drop=True)
    if len(df) > max_points:
        idx = np.linspace(0, len(df) - 1, max_points).astype(int)
        df = df.iloc[idx].reset_index(drop=True)

    # Trade sizes given in the prompt (assume "1000,5000,..." are in USD notionals).
    # For now, we interpret them as TOKEN-IN UNITS after converting using spot price:
    # - If trading IXS->WETH: 1000 means $1000 of IXS -> need IXS amount; we don't have USD price here.
    # => Instead we implement "notional in WETH" approach for comparability:
    # - sizes are in WETH units (converted later in reporting) OR we can treat as raw token amounts.
    #
    # To avoid inventing USD prices, we implement sizes in *WETH* and *IXS* separately:
    # - For IXS->WETH direction: input sizes are [1000, 5000, 10000, 50000] IXS
    # - For WETH->IXS direction: input sizes are [0.1, 0.5, 1, 5] WETH (scaled set)
    #
    # IMPORTANT: In the report you must explain this assumption OR later replace with USD-based sizes using on-chain price.
    sizes_ixs_in = [1000, 5000, 10000, 50000]
    sizes_weth_in = [0.1, 0.5, 1.0, 5.0]

    rows = []
    for _, r in df.iterrows():
        reserve0 = float(r["reserve0"])  # IXS
        reserve1 = float(r["reserve1"])  # WETH
        ts = int(r["timestamp"])
        dt = r.get("datetime_utc", None)
        block = int(r["block_number"])

        # Direction 1: IXS -> WETH
        spot_ixs_to_weth = reserve1 / reserve0  # WETH per IXS [web:227]
        for amount_in_ixs in sizes_ixs_in:
            out_weth = cpmm_amount_out_no_fee(amount_in_ixs, reserve0, reserve1)
            avg_exec = (amount_in_ixs / out_weth) if out_weth > 0 else np.nan  # IXS per WETH
            # Convert spot to same units (IXS per WETH)
            spot_in_same_units = 1.0 / spot_ixs_to_weth if spot_ixs_to_weth > 0 else np.nan
            slip = slippage_pct(spot_in_same_units, avg_exec)

            rows.append(
                {
                    "block_number": block,
                    "timestamp": ts,
                    "datetime_utc": dt,
                    "direction": f"{sym0}->{sym1}",
                    "amount_in": float(amount_in_ixs),
                    "amount_in_unit": sym0,
                    "amount_out": float(out_weth),
                    "amount_out_unit": sym1,
                    "spot_price": float(spot_in_same_units),
                    "spot_price_unit": f"{sym0}_per_{sym1}",
                    "avg_exec_price": float(avg_exec),
                    "avg_exec_price_unit": f"{sym0}_per_{sym1}",
                    "slippage_pct": float(slip),
                    "fees_included": False,
                }
            )

        # Direction 2: WETH -> IXS
        spot_weth_to_ixs = reserve0 / reserve1  # IXS per WETH
        for amount_in_weth in sizes_weth_in:
            out_ixs = cpmm_amount_out_no_fee(amount_in_weth, reserve1, reserve0)
            avg_exec = (amount_in_weth / out_ixs) if out_ixs > 0 else np.nan  # WETH per IXS
            spot_in_same_units = 1.0 / spot_weth_to_ixs if spot_weth_to_ixs > 0 else np.nan  # WETH per IXS
            slip = slippage_pct(spot_in_same_units, avg_exec)

            rows.append(
                {
                    "block_number": block,
                    "timestamp": ts,
                    "datetime_utc": dt,
                    "direction": f"{sym1}->{sym0}",
                    "amount_in": float(amount_in_weth),
                    "amount_in_unit": sym1,
                    "amount_out": float(out_ixs),
                    "amount_out_unit": sym0,
                    "spot_price": float(spot_in_same_units),
                    "spot_price_unit": f"{sym1}_per_{sym0}",
                    "avg_exec_price": float(avg_exec),
                    "avg_exec_price_unit": f"{sym1}_per_{sym0}",
                    "slippage_pct": float(slip),
                    "fees_included": False,
                }
            )

    out = pd.DataFrame(rows)
    out.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")
    print(out.head(5).to_string(index=False))
    print("\nNOTE: Trade-size units are currently token amounts (IXS sizes, WETH sizes).")
    print("If the case requires USD notionals (1000/5000/...), next step is to convert using on-chain price at each timestamp.")


if __name__ == "__main__":
    main()
