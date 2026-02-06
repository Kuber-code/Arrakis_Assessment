import os
import json
import math
from dataclasses import dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from web3 import Web3

getcontext().prec = 60

ARRAKIS_VAULT = Web3.to_checksum_address("0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600")
UNIV4_QUOTER = Web3.to_checksum_address("0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203")
ADDRESS_ZERO = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

# Try common Arrakis vault module getters
VAULT_MODULE_GETTERS = [
    ("module", [{"name": "module", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("activeModule", [{"name": "activeModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("getModule", [{"name": "getModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("strategy", [{"name": "strategy", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
]

ERC20_ABI = [
    {"name": "decimals", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint8"}]},
    {"name": "symbol", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "string"}]},
]

UNIV4_MODULE_ABI = [
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
    {
        "name": "getRanges",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [
            {
                "name": "ranges",
                "type": "tuple[]",
                "components": [
                    {"name": "tickLower", "type": "int24"},
                    {"name": "tickUpper", "type": "int24"},
                ],
            }
        ],
    },
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


def fee_to_rate(fee_u24: int) -> Decimal:
    return Decimal(int(fee_u24)) / Decimal(1_000_000)


def detect_arrakis_module(w3: Web3, vault_addr: str) -> str:
    for name, abi in VAULT_MODULE_GETTERS:
        try:
            v = w3.eth.contract(address=vault_addr, abi=abi)
            mod = v.functions.__getattribute__(name)().call()
            mod = Web3.to_checksum_address(mod)
            if int(mod, 16) != 0:
                print(f"Detected vault module via {name}(): {mod}")
                return mod
        except Exception:
            continue
    raise RuntimeError("Could not detect Arrakis module address from vault.")


def currency_meta(w3: Web3, currency: str) -> tuple[str, int]:
    if currency.lower() == ADDRESS_ZERO.lower():
        return ("ETH", 18)
    t = w3.eth.contract(address=Web3.to_checksum_address(currency), abi=ERC20_ABI)
    sym = t.functions.symbol().call()
    dec = int(t.functions.decimals().call())
    return (sym, dec)


def to_raw(amount: Decimal, decimals: int) -> int:
    if amount <= 0:
        return 0
    q = amount * (Decimal(10) ** decimals)
    return int(q.to_integral_value(rounding="ROUND_FLOOR"))


def from_raw(amount_raw: int, decimals: int) -> Decimal:
    return Decimal(int(amount_raw)) / (Decimal(10) ** decimals)


def quote_exact_in_single(quoter, pool_key: PoolKey, token_in: str, amount_in_raw: int, block_number: int) -> tuple[int, int]:
    if amount_in_raw <= 0:
        return 0, 0
    amount_in_raw = int(min(int(amount_in_raw), 2**128 - 1))

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


def tick_from_price_token1_per_token0_raw(price1_per_0_raw: Decimal) -> Optional[int]:
    # Uniswap-style tick ~ log_{1.0001}(price)
    if price1_per_0_raw <= 0:
        return None
    x = float(price1_per_0_raw)
    if not np.isfinite(x) or x <= 0:
        return None
    return int(math.floor(math.log(x) / math.log(1.0001)))


def build_range_coverage(df_ranges: pd.DataFrame, tick_spacing: int) -> pd.DataFrame:
    if df_ranges.empty:
        return pd.DataFrame(columns=["tick_bin", "coverage_count"])

    tl_min = int(df_ranges["tickLower"].min())
    tu_max = int(df_ranges["tickUpper"].max())

    # Align bins to tickSpacing
    def align_down(t: int) -> int:
        return (t // tick_spacing) * tick_spacing

    def align_up(t: int) -> int:
        return ((t + tick_spacing - 1) // tick_spacing) * tick_spacing

    start = align_down(tl_min)
    end = align_up(tu_max)

    bins = list(range(start, end + 1, tick_spacing))
    coverage = np.zeros(len(bins), dtype=int)

    # For each range, increment bins that fall inside [tickLower, tickUpper)
    for r in df_ranges.itertuples(index=False):
        tl = int(r.tickLower)
        tu = int(r.tickUpper)
        if tu <= tl:
            continue
        # indices of bins within [tl, tu]
        i0 = int((align_up(tl) - start) // tick_spacing)
        i1 = int((align_down(tu - 1) - start) // tick_spacing)
        if i1 < i0:
            continue
        coverage[i0 : i1 + 1] += 1

    out = pd.DataFrame({"tick_bin": bins, "coverage_count": coverage})
    return out


def main():
    load_dotenv()
    rpc_url = os.environ.get("RPC_URL")
    if not rpc_url:
        raise RuntimeError("Missing RPC_URL in .env")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError("Cannot connect to RPC_URL")
    if w3.eth.chain_id != 1:
        raise RuntimeError(f"Not Ethereum mainnet: chain_id={w3.eth.chain_id}")

    root = Path(__file__).resolve().parents[1]
    processed = root / "data" / "processed"
    figures = root / "figures"
    processed.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)

    latest = int(w3.eth.block_number)

    # Detect Arrakis module and read poolKey + ranges
    module_addr = detect_arrakis_module(w3, ARRAKIS_VAULT)
    module = w3.eth.contract(address=module_addr, abi=UNIV4_MODULE_ABI)

    c0, c1, fee_u24, tick_spacing, hooks = module.functions.poolKey().call()
    c0 = Web3.to_checksum_address(c0)
    c1 = Web3.to_checksum_address(c1)
    hooks = Web3.to_checksum_address(hooks)

    pool_key = PoolKey(currency0=c0, currency1=c1, fee=int(fee_u24), tick_spacing=int(tick_spacing), hooks=hooks)
    fee_rate = fee_to_rate(pool_key.fee)

    sym0, dec0 = currency_meta(w3, c0)
    sym1, dec1 = currency_meta(w3, c1)

    ranges = module.functions.getRanges().call()
    rows = []
    for i, r in enumerate(ranges):
        tl = int(r[0])
        tu = int(r[1])
        rows.append(
            {
                "range_index": i,
                "tickLower": tl,
                "tickUpper": tu,
                "width_ticks": tu - tl,
                "mid_tick": (tl + tu) / 2.0,
            }
        )

    df_ranges = pd.DataFrame(rows).sort_values(["tickLower", "tickUpper"]).reset_index(drop=True)
    out_ranges_csv = processed / "univ4_active_ranges.csv"
    df_ranges.to_csv(out_ranges_csv, index=False)
    print(f"Wrote {out_ranges_csv}")

    # Build coverage distribution (proxy)
    ts = int(pool_key.tick_spacing)
    df_cov = build_range_coverage(df_ranges, tick_spacing=ts)
    out_cov_csv = processed / "univ4_range_coverage.csv"
    df_cov.to_csv(out_cov_csv, index=False)
    print(f"Wrote {out_cov_csv}")

    # Estimate current tick from micro-quote (spot price), in Uniswap-style raw units price(token1)/price(token0)
    quoter = w3.eth.contract(address=UNIV4_QUOTER, abi=V4_QUOTER_ABI)

    # token1 per token0 spot: if we microquote token0 -> token1, we get token1/token0
    tiny0 = Decimal(os.environ.get("UNIV4_SPOT_TINY_C0", "0.000001"))  # in currency0 units (ETH if address0)
    spot_1_per_0 = spot_out_per_in_from_microquote(
        quoter=quoter,
        pool_key=pool_key,
        token_in=pool_key.currency0,
        token_out_dec=dec1,
        token_in_dec=dec0,
        fee_rate=fee_rate,
        block_number=latest,
        tiny_amount_in=tiny0,
    )

    tick_est = None
    spot_1_per_0_raw = None
    if spot_1_per_0 is not None and spot_1_per_0 > 0:
        # convert human token1/token0 to raw units token1_raw/token0_raw
        spot_1_per_0_raw = spot_1_per_0 * (Decimal(10) ** dec1) / (Decimal(10) ** dec0)
        tick_est = tick_from_price_token1_per_token0_raw(spot_1_per_0_raw)

    snapshot = {
        "block_number": latest,
        "module": module_addr,
        "vault": ARRAKIS_VAULT,
        "quoter": UNIV4_QUOTER,
        "poolKey": {
            "currency0": pool_key.currency0,
            "currency1": pool_key.currency1,
            "symbol0": sym0,
            "symbol1": sym1,
            "decimals0": dec0,
            "decimals1": dec1,
            "fee_uint24": int(pool_key.fee),
            "fee_rate": float(fee_rate),
            "tickSpacing": int(pool_key.tick_spacing),
            "hooks": pool_key.hooks,
        },
        "spot_estimate": {
            "token1_per_token0": str(spot_1_per_0) if spot_1_per_0 is not None else None,
            "token1_per_token0_raw_units": str(spot_1_per_0_raw) if spot_1_per_0_raw is not None else None,
            "tick_estimate": int(tick_est) if tick_est is not None else None,
            "note": "Tick estimated from micro-quote spot price; used only as a visual marker.",
        },
        "note": (
            "Liquidity 'distribution' here is shown as range coverage count across ticks (proxy), "
            "because per-tick liquidity amounts are not exposed via the minimal module ABI used."
        ),
    }

    out_snap = processed / "univ4_liquidity_distribution_snapshot.json"
    out_snap.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    print(f"Wrote {out_snap}")

    # Plot: coverage vs tick + current tick marker + full-range baseline (constant 1)
    fig, ax = plt.subplots(figsize=(12, 5.5))
    if not df_cov.empty:
        ax.step(df_cov["tick_bin"], df_cov["coverage_count"], where="post", linewidth=1.6, label="Active range coverage (count)")

        # "Full-range" theoretical baseline: one continuous range would imply constant coverage=1 across the plotted domain
        ax.hlines(1, df_cov["tick_bin"].min(), df_cov["tick_bin"].max(), colors="gray", linestyles="--", linewidth=1.2, label="Full-range baseline (uniform)")

        if tick_est is not None:
            ax.axvline(tick_est, color="black", alpha=0.7, linewidth=1.0, label="Estimated current tick")

    ax.set_title("UniV4 liquidity distribution proxy (range coverage) vs full-range baseline")
    ax.set_xlabel("Tick (binned by tickSpacing)")
    ax.set_ylabel("Coverage count (# active ranges covering tick)")
    ax.grid(True, alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()

    out_png = figures / "univ4_liquidity_distribution_coverage.png"
    fig.savefig(out_png, dpi=180)
    plt.close(fig)
    print(f"Wrote {out_png}")

    # Print quick text summary for report
    if not df_ranges.empty:
        total_width = int(df_ranges["width_ticks"].sum())
        widest = df_ranges.sort_values("width_ticks", ascending=False).iloc[0].to_dict()
        narrowest = df_ranges.sort_values("width_ticks", ascending=True).iloc[0].to_dict()
        print("\nRanges summary:")
        print(f"- n_ranges: {len(df_ranges)}")
        print(f"- total_active_width_ticks (sum): {total_width}")
        print(f"- widest: [{int(widest['tickLower'])}, {int(widest['tickUpper'])}] width={int(widest['width_ticks'])}")
        print(f"- narrowest: [{int(narrowest['tickLower'])}, {int(narrowest['tickUpper'])}] width={int(narrowest['width_ticks'])}")
    else:
        print("\nNo ranges returned by module.getRanges().")


if __name__ == "__main__":
    main()
