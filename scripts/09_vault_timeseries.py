import os
import json
import math
from dataclasses import dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

getcontext().prec = 70

CHAIN_ID_MAINNET = 1

ARRAKIS_VAULT = Web3.to_checksum_address("0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600")
UNIV4_QUOTER = Web3.to_checksum_address("0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203")
ADDRESS_ZERO = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

# ETH/USD (valuation) via UniV3 WETH/USDC slot0
UNIV3_FACTORY = Web3.to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984")
WETH = Web3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
USDC = Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
UNIV3_FEE = 500  # 0.05%

# --- ABIs ---
VAULT_ABI = [
    {"name": "owner", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "module", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "activeModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "getModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "strategy", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {
        "name": "totalUnderlying",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"type": "uint256"}, {"type": "uint256"}],
    },
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
    }
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

FACTORY_ABI = [
    {
        "name": "getPool",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
            {"name": "fee", "type": "uint24"},
        ],
        "outputs": [{"name": "pool", "type": "address"}],
    }
]

POOL_V3_ABI = [
    {"name": "token0", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "token1", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {
        "name": "slot0",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "observationIndex", "type": "uint16"},
            {"name": "observationCardinality", "type": "uint16"},
            {"name": "observationCardinalityNext", "type": "uint16"},
            {"name": "feeProtocol", "type": "uint8"},
            {"name": "unlocked", "type": "bool"},
        ],
    },
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


def ensure_dirs(root: Path):
    (root / "data" / "raw").mkdir(parents=True, exist_ok=True)
    (root / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (root / "figures").mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def fee_to_rate(fee_u24: int) -> Decimal:
    return Decimal(int(fee_u24)) / Decimal(1_000_000)


def to_raw(amount: Decimal, decimals: int) -> int:
    if amount <= 0:
        return 0
    return int((amount * (Decimal(10) ** decimals)).to_integral_value(rounding="ROUND_FLOOR"))


def from_raw(amount_raw: int, decimals: int) -> Decimal:
    return Decimal(int(amount_raw)) / (Decimal(10) ** decimals)


def currency_meta(w3: Web3, currency: str) -> Tuple[str, int]:
    c = Web3.to_checksum_address(currency)
    if c.lower() == ADDRESS_ZERO.lower():
        return ("ETH", 18)
    t = w3.eth.contract(address=c, abi=ERC20_ABI)
    return (t.functions.symbol().call(), int(t.functions.decimals().call()))


def detect_arrakis_module(w3: Web3, vault_addr: str) -> str:
    v = w3.eth.contract(address=Web3.to_checksum_address(vault_addr), abi=VAULT_ABI)
    for fn in ("module", "activeModule", "getModule", "strategy"):
        try:
            mod = getattr(v.functions, fn)().call()
            mod = Web3.to_checksum_address(mod)
            if int(mod, 16) != 0:
                print(f"Detected vault module via {fn}(): {mod}")
                return mod
        except Exception:
            continue
    raise RuntimeError("Could not detect module address from vault (module/activeModule/getModule/strategy all failed).")


def init_univ3_eth_usd_pool(w3: Web3):
    factory = w3.eth.contract(address=UNIV3_FACTORY, abi=FACTORY_ABI)
    pool_addr = Web3.to_checksum_address(factory.functions.getPool(WETH, USDC, UNIV3_FEE).call())
    if int(pool_addr, 16) == 0:
        raise RuntimeError("UniV3 WETH/USDC pool not found via factory.getPool.")
    pool = w3.eth.contract(address=pool_addr, abi=POOL_V3_ABI)
    token0 = Web3.to_checksum_address(pool.functions.token0().call())
    token1 = Web3.to_checksum_address(pool.functions.token1().call())
    return pool, token0, token1


def eth_usd_from_univ3(pool_v3, token0: str, token1: str, block_number: int) -> Decimal:
    sqrtp = int(pool_v3.functions.slot0().call(block_identifier=int(block_number))[0])
    if sqrtp <= 0:
        return Decimal("NaN")
    p_raw = (Decimal(sqrtp) * Decimal(sqrtp)) / (Decimal(2) ** 192)  # token1/token0
    # Want USDC per WETH; adjust decimals (USDC 6, WETH 18 => factor 1e12)
    if token0.lower() == USDC.lower() and token1.lower() == WETH.lower():
        return (Decimal(1) / p_raw) * (Decimal(10) ** 12)
    if token0.lower() == WETH.lower() and token1.lower() == USDC.lower():
        return p_raw * (Decimal(10) ** 12)
    return Decimal("NaN")


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

    # “spot” as pre-fee by undoing fee impact for tiny trade
    return (out_amt / in_amt) / denom


def choose_underlying_mapping(u0: Decimal, u1: Decimal, spot_ixs_per_eth: Decimal) -> Tuple[str, Decimal, Decimal]:
    """
    Decide whether vault.totalUnderlying() returns (IXS, ETH) or (ETH, IXS)
    by comparing the ratio to spot IXS/ETH.

    Returns: (mode, amt_ixs, amt_eth)
    """
    if u0 <= 0 or u1 <= 0 or spot_ixs_per_eth <= 0:
        return ("assume_u0_ixs_u1_eth", u0, u1)

    def ldiff(a: Decimal, b: Decimal) -> float:
        a = float(a)
        b = float(b)
        if not np.isfinite(a) or not np.isfinite(b) or a <= 0 or b <= 0:
            return float("inf")
        return abs(math.log(a) - math.log(b))

    r_u0_u1 = u0 / u1
    r_u1_u0 = u1 / u0

    # If u0/u1 ~ IXS/ETH => u0=IXS, u1=ETH
    d0 = ldiff(r_u0_u1, spot_ixs_per_eth)
    # If u1/u0 ~ IXS/ETH => u1=IXS, u0=ETH
    d1 = ldiff(r_u1_u0, spot_ixs_per_eth)

    if d0 <= d1:
        return ("u0_ixs_u1_eth", u0, u1)
    return ("u0_eth_u1_ixs", u1, u0)


def main():
    load_dotenv()
    rpc_url = os.environ.get("RPC_URL")
    if not rpc_url:
        raise RuntimeError("Missing RPC_URL in environment or .env")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError("Cannot connect to RPC_URL")
    if int(w3.eth.chain_id) != CHAIN_ID_MAINNET:
        raise RuntimeError(f"Not Ethereum mainnet: chain_id={w3.eth.chain_id}")

    root = Path(__file__).resolve().parents[1]
    ensure_dirs(root)
    raw_dir = root / "data" / "raw"
    processed_dir = root / "data" / "processed"

    mig_json = raw_dir / "migration_block_final.json"
    meta_json = raw_dir / "univ2_pair_metadata.json"

    if not mig_json.exists():
        raise RuntimeError("Missing data/raw/migration_block_final.json")
    if not meta_json.exists():
        raise RuntimeError("Missing data/raw/univ2_pair_metadata.json")

    mig = load_json(mig_json)
    mig_block = int(mig["selected"]["migration_block_final"])

    meta = load_json(meta_json)
    # We use this only for sanity / reporting symbol
    ixs_addr = Web3.to_checksum_address(meta["token0"]["address"])
    ixs_sym = meta["token0"].get("symbol", "IXS")

    latest = int(w3.eth.block_number)
    block_stride = int(os.environ.get("VAULT_BLOCK_STRIDE", "600"))
    tiny_c0 = Decimal(os.environ.get("VAULT_SPOT_TINY_C0", "0.000001"))

    vault = w3.eth.contract(address=ARRAKIS_VAULT, abi=VAULT_ABI)
    owner = Web3.to_checksum_address(vault.functions.owner().call())
    module_addr = detect_arrakis_module(w3, ARRAKIS_VAULT)

    module = w3.eth.contract(address=module_addr, abi=MODULE_ABI)
    c0, c1, fee_u24, tick_spacing, hooks = module.functions.poolKey().call()
    c0 = Web3.to_checksum_address(c0)
    c1 = Web3.to_checksum_address(c1)
    hooks = Web3.to_checksum_address(hooks)

    pool_key = PoolKey(currency0=c0, currency1=c1, fee=int(fee_u24), tick_spacing=int(tick_spacing), hooks=hooks)
    fee_rate = fee_to_rate(pool_key.fee)

    sym0, dec0 = currency_meta(w3, c0)
    sym1, dec1 = currency_meta(w3, c1)

    # Validate that IXS is one of pool currencies (if metadata token0 isn't IXS, allow token1)
    if ixs_addr.lower() not in (c0.lower(), c1.lower()):
        ixs_addr = Web3.to_checksum_address(meta["token1"]["address"])
        ixs_sym = meta["token1"].get("symbol", ixs_sym)
    if ixs_addr.lower() not in (c0.lower(), c1.lower()):
        raise RuntimeError("IXS from metadata is not in UniV4 pool currencies (check token address).")

    # ETH/USD price source
    pool_v3, v3_t0, v3_t1 = init_univ3_eth_usd_pool(w3)

    quoter = w3.eth.contract(address=UNIV4_QUOTER, abi=V4_QUOTER_ABI)

    blocks = list(range(mig_block, latest + 1, block_stride))
    if not blocks or blocks[-1] != latest:
        blocks.append(latest)

    print(f"Migration block: {mig_block}")
    print(f"Latest block: {latest}")
    print(f"Vault: {ARRAKIS_VAULT}")
    print(f"Owner: {owner}")
    print(f"Module: {module_addr}")
    print(f"PoolKey currency0: {c0} ({sym0}, dec={dec0})")
    print(f"PoolKey currency1: {c1} ({sym1}, dec={dec1})")
    print(f"fee_uint24: {pool_key.fee} (rate={float(fee_rate):.6f}), tickSpacing={pool_key.tick_spacing}, hooks={pool_key.hooks}")

    rows = []
    for i, b in enumerate(blocks, start=1):
        try:
            blk = w3.eth.get_block(int(b))
        except Exception:
            continue

        ts = int(blk["timestamp"])
        dt = pd.to_datetime(ts, unit="s", utc=True)

        eth_usd = eth_usd_from_univ3(pool_v3, v3_t0, v3_t1, int(b))
        if not eth_usd.is_finite() or eth_usd <= 0:
            continue

        # Spot token1/token0 via micro-quote currency0 -> currency1
        spot_1_per_0 = spot_out_per_in_from_microquote(
            quoter=quoter,
            pool_key=pool_key,
            token_in=pool_key.currency0,
            token_out_dec=dec1,
            token_in_dec=dec0,
            fee_rate=fee_rate,
            block_number=int(b),
            tiny_amount_in=tiny_c0,
        )
        if spot_1_per_0 is None or spot_1_per_0 <= 0:
            continue

        # Convert to spot IXS per ETH + ETH per IXS for valuation
        # In your poolKey, currency0 is ETH (address(0)), currency1 is IXS.
        if c0.lower() == ADDRESS_ZERO.lower():
            spot_ixs_per_eth = spot_1_per_0
            spot_eth_per_ixs = (Decimal(1) / spot_ixs_per_eth) if spot_ixs_per_eth > 0 else Decimal("NaN")
        else:
            spot_eth_per_ixs = spot_1_per_0
            spot_ixs_per_eth = (Decimal(1) / spot_eth_per_ixs) if spot_eth_per_ixs > 0 else Decimal("NaN")

        if not spot_ixs_per_eth.is_finite() or spot_ixs_per_eth <= 0:
            continue
        if not spot_eth_per_ixs.is_finite() or spot_eth_per_ixs <= 0:
            continue

        # Vault reported underlying (unknown ordering)
        try:
            u0_raw, u1_raw = vault.functions.totalUnderlying().call(block_identifier=int(b))
        except Exception:
            continue

        u0_raw = int(u0_raw)
        u1_raw = int(u1_raw)

        # In your case both assets are 18 decimals; treat both as 18 for human conversion.
        u0 = from_raw(u0_raw, 18)
        u1 = from_raw(u1_raw, 18)

        mapping_mode, amt_ixs, amt_eth = choose_underlying_mapping(u0, u1, spot_ixs_per_eth)

        ixs_usd = spot_eth_per_ixs * eth_usd
        value_eth_usd = amt_eth * eth_usd
        value_ixs_usd = amt_ixs * ixs_usd
        value_total_usd = value_eth_usd + value_ixs_usd

        rows.append(
            {
                "block_number": int(b),
                "timestamp": ts,
                "datetime_utc": str(dt),
                "vault_owner": owner,
                "vault": ARRAKIS_VAULT,
                "module": module_addr,
                "poolKey_currency0": c0,
                "poolKey_currency1": c1,
                "poolKey_symbol0": sym0,
                "poolKey_symbol1": sym1,
                "fee_uint24": int(pool_key.fee),
                "fee_rate": float(fee_rate),
                "tick_spacing": int(pool_key.tick_spacing),
                "hooks": pool_key.hooks,
                "vault_underlying0_raw": u0_raw,
                "vault_underlying1_raw": u1_raw,
                "underlying_mapping_mode": mapping_mode,
                "amt_eth": float(amt_eth),
                "amt_ixs": float(amt_ixs),
                "eth_usd": float(eth_usd),
                "spot_ixs_per_eth": float(spot_ixs_per_eth),
                "spot_eth_per_ixs": float(spot_eth_per_ixs),
                "ixs_usd": float(ixs_usd),
                "value_eth_usd": float(value_eth_usd),
                "value_ixs_usd": float(value_ixs_usd),
                "value_total_usd": float(value_total_usd),
                "ixs_symbol": ixs_sym,
                "eth_symbol": "ETH",
            }
        )

        if i % 50 == 0:
            print(f"processed {i}/{len(blocks)} blocks")

    df = pd.DataFrame(rows).sort_values("block_number").reset_index(drop=True)
    if df.empty:
        raise RuntimeError("Produced 0 rows. Check RPC/stride/quoter and vault.totalUnderlying calls.")

    # --- Baselines ---
    amt_eth0 = float(df.loc[0, "amt_eth"])
    amt_ixs0 = float(df.loc[0, "amt_ixs"])

    df["hold_value_usd"] = amt_eth0 * df["eth_usd"].astype(float) + amt_ixs0 * df["ixs_usd"].astype(float)

    # Full-range LP baseline (simplified constant-product, no fees)
    p0 = float(df.loc[0, "spot_eth_per_ixs"])
    if (not np.isfinite(p0)) or p0 <= 0 or amt_eth0 <= 0 or amt_ixs0 <= 0:
        df["full_range_lp_amt_eth"] = np.nan
        df["full_range_lp_amt_ixs"] = np.nan
        df["full_range_lp_value_usd"] = np.nan
    else:
        k = amt_eth0 * amt_ixs0
        p = df["spot_eth_per_ixs"].astype(float).values
        amt_ixs_fr = np.sqrt(k / p)
        amt_eth_fr = np.sqrt(k * p)
        df["full_range_lp_amt_ixs"] = amt_ixs_fr
        df["full_range_lp_amt_eth"] = amt_eth_fr
        df["full_range_lp_value_usd"] = amt_eth_fr * df["eth_usd"].astype(float).values + amt_ixs_fr * df["ixs_usd"].astype(float).values

    # Indices
    v0 = float(df.loc[0, "value_total_usd"])
    df["vault_value_index"] = df["value_total_usd"].astype(float) / v0 if np.isfinite(v0) and v0 > 0 else np.nan

    h0 = float(df.loc[0, "hold_value_usd"])
    df["hold_value_index"] = df["hold_value_usd"].astype(float) / h0 if np.isfinite(h0) and h0 > 0 else np.nan

    fr0 = float(df.loc[0, "full_range_lp_value_usd"]) if "full_range_lp_value_usd" in df.columns else np.nan
    df["full_range_lp_value_index"] = (
        df["full_range_lp_value_usd"].astype(float) / fr0 if np.isfinite(fr0) and fr0 > 0 else np.nan
    )

    out_csv = processed_dir / "vault_timeseries.csv"
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")
    print(df.head(8).to_string(index=False))

    note = (
        "Note: amounts come from vault.totalUnderlying() and are mapped to (IXS, ETH) "
        "by matching the amount ratio to spot IXS/ETH inferred from a UniV4 micro-quote."
    )
    print("\n" + note)


if __name__ == "__main__":
    main()
