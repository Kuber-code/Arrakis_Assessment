import os
import json
import math
from dataclasses import dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

import matplotlib.pyplot as plt

getcontext().prec = 70

CHAIN_ID_MAINNET = 1

ARRAKIS_VAULT = Web3.to_checksum_address("0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600")
UNIV4_QUOTER = Web3.to_checksum_address("0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203")
ADDRESS_ZERO = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

# --- ABIs ---
VAULT_ABI = [
    {"name": "module", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
]

MODULE_ABI = [
    {
        "name": "poolKey",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [
            {"name": "currency0", "type": "address"},
            {"name": "currency1", "type": "address"},
            {"name": "fee", "type": "uint24"},
            {"name": "tickSpacing", "type": "int24"},
            {"name": "hooks", "type": "address"},
        ],
    },
    # Arrakis module typically exposes ranges; keep it narrow
    {
        "name": "getRanges",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [
            {
                "name": "",
                "type": "tuple[]",
                "components": [
                    {"name": "tickLower", "type": "int24"},
                    {"name": "tickUpper", "type": "int24"},
                ],
            }
        ],
    },
]

ERC20_ABI = [
    {"name": "decimals", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint8"}]},
    {"name": "symbol", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "string"}]},
]

V4_QUOTER_ABI = [
    {
        "name": "quoteExactInputSingle",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {
                "name": "params",
                "type": "tuple",
                "components": [
                    {
                        "name": "poolKey",
                        "type": "tuple",
                        "components": [
                            {"name": "currency0", "type": "address"},
                            {"name": "currency1", "type": "address"},
                            {"name": "fee", "type": "uint24"},
                            {"name": "tickSpacing", "type": "int24"},
                            {"name": "hooks", "type": "address"},
                        ],
                    },
                    {"name": "zeroForOne", "type": "bool"},
                    {"name": "exactAmount", "type": "uint128"},
                    {"name": "hookData", "type": "bytes"},
                ],
            }
        ],
        "outputs": [{"name": "amountOut", "type": "uint256"}, {"name": "gasEstimate", "type": "uint256"}],
    }
]


@dataclass(frozen=True)
class PoolKey:
    currency0: str
    currency1: str
    fee: int
    tick_spacing: int
    hooks: str

    def as_tuple(self):
        return (self.currency0, self.currency1, int(self.fee), int(self.tick_spacing), self.hooks)


def ensure_dirs(root: Path) -> Tuple[Path, Path, Path]:
    raw = root / "data" / "raw"
    processed = root / "data" / "processed"
    figures = root / "figures"
    raw.mkdir(parents=True, exist_ok=True)
    processed.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)
    return raw, processed, figures


def fee_to_rate(fee_u24: int) -> Decimal:
    return Decimal(int(fee_u24)) / Decimal(1_000_000)


def from_raw(amount_raw: int, decimals: int) -> Decimal:
    return Decimal(int(amount_raw)) / (Decimal(10) ** decimals)


def to_raw(amount: Decimal, decimals: int) -> int:
    if amount <= 0:
        return 0
    return int((amount * (Decimal(10) ** decimals)).to_integral_value(rounding="ROUND_FLOOR"))


def currency_meta(w3: Web3, currency: str) -> Tuple[str, int]:
    c = Web3.to_checksum_address(currency)
    if c.lower() == ADDRESS_ZERO.lower():
        return ("ETH", 18)
    t = w3.eth.contract(address=c, abi=ERC20_ABI)
    return (t.functions.symbol().call(), int(t.functions.decimals().call()))


def quote_exact_in_single(quoter, pool_key: PoolKey, token_in: str, amount_in_raw: int, block_number: int) -> Tuple[int, int]:
    if amount_in_raw <= 0:
        return 0, 0
    amount_in_raw = int(min(int(amount_in_raw), 2**128 - 1))

    token_in = Web3.to_checksum_address(token_in)
    if token_in.lower() == pool_key.currency0.lower():
        zero_for_one = True
    elif token_in.lower() == pool_key.currency1.lower():
        zero_for_one = False
    else:
        raise ValueError("token_in not in pool currencies")

    params = (pool_key.as_tuple(), bool(zero_for_one), int(amount_in_raw), b"")
    amount_out, gas_est = quoter.functions.quoteExactInputSingle(params).call(block_identifier=int(block_number))
    return int(amount_out), int(gas_est)


def spot_out_per_in_from_microquote(
    quoter,
    pool_key: PoolKey,
    token_in: str,
    token_out_dec: int,
    token_in_dec: int,
    fee_rate: Decimal,
    block_number: int,
    tiny_amount_in: Decimal,
) -> Optional[Decimal]:
    tiny_in_raw = max(1, to_raw(tiny_amount_in, token_in_dec))
    try:
        out_raw, _ = quote_exact_in_single(quoter, pool_key, token_in=token_in, amount_in_raw=tiny_in_raw, block_number=block_number)
    except Exception:
        return None

    out_amt = from_raw(int(out_raw), token_out_dec)
    in_amt = from_raw(int(tiny_in_raw), token_in_dec)
    if out_amt <= 0 or in_amt <= 0:
        return None

    denom = (Decimal(1) - fee_rate)
    if denom <= 0:
        return None

    return (out_amt / in_amt) / denom


def tick_from_price_ratio_token1_per_token0(price_1_per_0: Decimal) -> float:
    # Uniswap tick definition: price = 1.0001^tick
    # tick = log(price)/log(1.0001)
    p = float(price_1_per_0)
    if not np.isfinite(p) or p <= 0:
        return float("nan")
    return math.log(p) / math.log(1.0001)


def snap_tick(tick: int, tick_spacing: int) -> int:
    if tick_spacing <= 0:
        return int(tick)
    return int(round(int(tick) / int(tick_spacing)) * int(tick_spacing))


def main():
    load_dotenv()
    rpc_url = os.environ.get("RPC_URL")
    if not rpc_url:
        raise RuntimeError("Missing RPC_URL in environment or .env")

    # choose a stable reference block (latest)
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError("Cannot connect to RPC_URL")
    if int(w3.eth.chain_id) != CHAIN_ID_MAINNET:
        raise RuntimeError(f"Not Ethereum mainnet: chain_id={w3.eth.chain_id}")

    root = Path(__file__).resolve().parents[1]
    raw, processed, figures = ensure_dirs(root)

    latest = int(w3.eth.block_number)

    # Discover poolKey from Arrakis vault module
    vault = w3.eth.contract(address=ARRAKIS_VAULT, abi=VAULT_ABI)
    module_addr = Web3.to_checksum_address(vault.functions.module().call())
    module = w3.eth.contract(address=module_addr, abi=MODULE_ABI)

    c0, c1, fee_u24, tick_spacing, hooks = module.functions.poolKey().call()
    c0 = Web3.to_checksum_address(c0)
    c1 = Web3.to_checksum_address(c1)
    hooks = Web3.to_checksum_address(hooks)

    pool_key = PoolKey(currency0=c0, currency1=c1, fee=int(fee_u24), tick_spacing=int(tick_spacing), hooks=hooks)
    fee_rate = fee_to_rate(pool_key.fee)

    sym0, dec0 = currency_meta(w3, c0)
    sym1, dec1 = currency_meta(w3, c1)

    # Ranges from module
    ranges = module.functions.getRanges().call()
    if not ranges:
        raise RuntimeError("module.getRanges() returned 0 ranges.")

    # Normalize ranges list
    norm_ranges: List[Dict[str, int]] = []
    for r in ranges:
        tl = int(r[0]) if not isinstance(r, dict) else int(r["tickLower"])
        tu = int(r[1]) if not isinstance(r, dict) else int(r["tickUpper"])
        norm_ranges.append({"tickLower": tl, "tickUpper": tu})

    # Snapshot JSON (raw)
    snapshot = {
        "block_number": latest,
        "vault": ARRAKIS_VAULT,
        "module": module_addr,
        "poolKey": {
            "currency0": c0,
            "currency1": c1,
            "fee_uint24": int(pool_key.fee),
            "tick_spacing": int(pool_key.tick_spacing),
            "hooks": pool_key.hooks,
            "symbol0": sym0,
            "symbol1": sym1,
            "decimals0": int(dec0),
            "decimals1": int(dec1),
        },
        "ranges": norm_ranges,
    }

    snap_path = raw / "univ4_liquidity_distribution_snapshot.json"
    snap_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    print(f"Wrote {snap_path}")

    # CSV: active ranges (processed)
    df_ranges = pd.DataFrame(norm_ranges)
    df_ranges["width"] = df_ranges["tickUpper"] - df_ranges["tickLower"]
    df_ranges = df_ranges.sort_values(["tickLower", "tickUpper"]).reset_index(drop=True)

    out_ranges_csv = processed / "univ4_active_ranges.csv"
    df_ranges.to_csv(out_ranges_csv, index=False)
    print(f"Wrote {out_ranges_csv}")

    # Estimate current tick via spot micro-quote currency0 -> currency1
    tiny_c0 = Decimal(os.environ.get("V4_SPOT_TINY_C0", "0.000001"))
    quoter = w3.eth.contract(address=UNIV4_QUOTER, abi=V4_QUOTER_ABI)

    spot_1_per_0 = spot_out_per_in_from_microquote(
        quoter=quoter,
        pool_key=pool_key,
        token_in=pool_key.currency0,
        token_out_dec=dec1,
        token_in_dec=dec0,
        fee_rate=fee_rate,
        block_number=latest,
        tiny_amount_in=tiny_c0,
    )

    est_tick = None
    est_tick_snapped = None
    if spot_1_per_0 is not None and spot_1_per_0 > 0:
        # tick in terms of price token1/token0 (approx)
        # This ignores decimals adjustments; that's OK here because we use it only as a visual marker.
        # It will still be locally consistent for plotting "where spot roughly sits" relative to ranges.
        est_tick = int(round(np.log(float(spot_1_per_0)) / np.log(1.0001)))
        est_tick_snapped = snap_tick(est_tick, int(pool_key.tick_spacing))

    # Coverage across tick bins
    min_tick = int(df_ranges["tickLower"].min())
    max_tick = int(df_ranges["tickUpper"].max())
    spacing = int(pool_key.tick_spacing)

    # Build bins aligned to tickSpacing
    start = snap_tick(min_tick, spacing)
    end = snap_tick(max_tick, spacing)
    ticks = list(range(start, end + spacing, spacing))

    coverage = []
    for t in ticks:
        c = 0
        for _, r in df_ranges.iterrows():
            if int(r["tickLower"]) <= t < int(r["tickUpper"]):
                c += 1
        coverage.append(c)

    df_cov = pd.DataFrame({"tick": ticks, "active_range_coverage_count": coverage})
    out_cov_csv = processed / "univ4_range_coverage.csv"
    df_cov.to_csv(out_cov_csv, index=False)
    print(f"Wrote {out_cov_csv}")

    # Plot
    fig, ax = plt.subplots(figsize=(12.8, 6.2))
    ax.plot(df_cov["tick"], df_cov["active_range_coverage_count"], linewidth=2.0, label="Active range coverage (count)")

    # full-range "baseline" (conceptual): 1 range everywhere over [min,max]
    ax.hlines(
        y=1,
        xmin=df_cov["tick"].min(),
        xmax=df_cov["tick"].max(),
        colors="gray",
        linestyles=":",
        linewidth=1.6,
        label="Theoretical full-range baseline (conceptual)",
    )

    if est_tick_snapped is not None:
        ax.axvline(est_tick_snapped, color="gray", linestyle="--", linewidth=1.4, alpha=0.9, label="Estimated current tick (snapped)")
    elif est_tick is not None:
        ax.axvline(est_tick, color="gray", linestyle="--", linewidth=1.4, alpha=0.9, label="Estimated current tick")

    ax.set_title("UniV4 liquidity distribution (coverage proxy across tick bins)")
    ax.set_xlabel("Tick (binned by tickSpacing)")
    ax.set_ylabel("Coverage: number of active ranges overlapping tick bin")
    ax.grid(True, alpha=0.25)
    ax.legend(frameon=False)

    fig.tight_layout()
    out_png = figures / "univ4_liquidity_distribution_coverage.png"
    fig.savefig(out_png, dpi=180)
    plt.close(fig)
    print(f"Wrote {out_png}")

    # Console summary
    print("\nPoolKey:")
    print(f"  currency0: {c0} ({sym0}, dec={dec0})")
    print(f"  currency1: {c1} ({sym1}, dec={dec1})")
    print(f"  fee_uint24: {pool_key.fee} (rate={float(fee_rate):.6f})")
    print(f"  tickSpacing: {pool_key.tick_spacing}")
    print(f"  hooks: {pool_key.hooks}")

    print("\nRanges:")
    for r in norm_ranges:
        print(f"  [{r['tickLower']}, {r['tickUpper']}] width={r['tickUpper'] - r['tickLower']}")

    if est_tick is not None:
        print(f"\nEstimated tick (raw): {est_tick}")
    if est_tick_snapped is not None:
        print(f"Estimated tick (snapped to tickSpacing): {est_tick_snapped}")


if __name__ == "__main__":
    main()
