import os
import json
import math
from dataclasses import dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

getcontext().prec = 70

# ---------------- Addresses ----------------
ARRAKIS_VAULT = Web3.to_checksum_address("0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600")
UNIV4_QUOTER = Web3.to_checksum_address("0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203")
ADDRESS_ZERO = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

# ETH/USD via UniV3 USDC/WETH slot0 (valuation)
UNIV3_FACTORY = Web3.to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984")
WETH = Web3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
USDC = Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
UNIV3_FEE = 500  # 0.05%

# ---------------- Minimal ABIs ----------------
VAULT_MODULE_GETTERS = [
    ("module", [{"name": "module", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("activeModule", [{"name": "activeModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("getModule", [{"name": "getModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("strategy", [{"name": "strategy", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
]

# Vault: owner() + module() + totalUnderlying()
VAULT_ABI = [
    {"name": "owner", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "module", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "totalUnderlying", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint256"}, {"type": "uint256"}]},
]

ERC20_ABI = [
    {"name": "decimals", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint8"}]},
    {"name": "symbol", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "string"}]},
]

# Module: poolKey()
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
    }
]

# UniV4 Quoter
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

# UniV3 ETH/USD
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


def fee_to_rate(fee_u24: int) -> Decimal:
    return Decimal(int(fee_u24)) / Decimal(1_000_000)


def currency_meta(w3: Web3, currency: str) -> tuple[str, int]:
    if currency.lower() == ADDRESS_ZERO.lower():
        return ("ETH", 18)
    t = w3.eth.contract(address=Web3.to_checksum_address(currency), abi=ERC20_ABI)
    return (t.functions.symbol().call(), int(t.functions.decimals().call()))


def to_raw(amount: Decimal, decimals: int) -> int:
    if amount <= 0:
        return 0
    q = amount * (Decimal(10) ** decimals)
    return int(q.to_integral_value(rounding="ROUND_FLOOR"))


def from_raw(amount_raw: int, decimals: int) -> Decimal:
    return Decimal(int(amount_raw)) / (Decimal(10) ** decimals)


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


def quote_exact_in_single(quoter, pool_key: PoolKey, token_in: str, amount_in_raw: int, block_number: int) -> Tuple[int, int]:
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
    p_raw = (Decimal(sqrtp) * Decimal(sqrtp)) / (Decimal(2) ** 192)
    # Want USDC per WETH, adjust by decimals (USDC 6, WETH 18 => factor 1e12)
    if token0.lower() == USDC.lower() and token1.lower() == WETH.lower():
        return (Decimal(1) / p_raw) * (Decimal(10) ** 12)
    if token0.lower() == WETH.lower() and token1.lower() == USDC.lower():
        return p_raw * (Decimal(10) ** 12)
    return Decimal("NaN")


def choose_underlying_mapping(
    u0_human: Decimal,
    u1_human: Decimal,
    spot_ixs_per_eth: Decimal,
) -> Tuple[str, Decimal, Decimal]:
    """
    Decide whether vault.totalUnderlying() returns (IXS, ETH) or (ETH, IXS).
    We compare the ratio of amounts to the spot IXS/ETH.
    Returns (mode, amt_ixs, amt_eth).
    """
    if u0_human <= 0 or u1_human <= 0 or spot_ixs_per_eth <= 0:
        # fallback: assume (IXS, ETH)
        return ("assume_u0_ixs_u1_eth", u0_human, u1_human)

    # Compare in log-space to avoid scale issues
    r_u0_u1 = u0_human / u1_human
    r_u1_u0 = u1_human / u0_human

    def ldiff(a: Decimal, b: Decimal) -> float:
        a = float(a)
        b = float(b)
        if not np.isfinite(a) or not np.isfinite(b) or a <= 0 or b <= 0:
            return float("inf")
        return abs(math.log(a) - math.log(b))

    # If u0/u1 ~ IXS/ETH => u0=IXS, u1=ETH
    d0 = ldiff(r_u0_u1, spot_ixs_per_eth)
    # If u1/u0 ~ IXS/ETH => u1=IXS, u0=ETH
    d1 = ldiff(r_u1_u0, spot_ixs_per_eth)

    if d0 <= d1:
        return ("u0_ixs_u1_eth", u0_human, u1_human)
    return ("u0_eth_u1_ixs", u1_human, u0_human)


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
    raw = root / "data" / "raw"
    processed.mkdir(parents=True, exist_ok=True)

    mig_json = processed / "migration_block_final.json"
    meta_json = raw / "univ2_pair_metadata.json"
    if not mig_json.exists():
        raise RuntimeError("Missing data/processed/migration_block_final.json")
    if not meta_json.exists():
        raise RuntimeError("Missing data/raw/univ2_pair_metadata.json")

    mig = json.loads(mig_json.read_text(encoding="utf-8"))
    mig_block = int(mig["selected"]["migration_block_final"])

    meta = json.loads(meta_json.read_text(encoding="utf-8"))
    ixs_addr = Web3.to_checksum_address(meta["token0"]["address"])
    ixs_sym = meta["token0"]["symbol"]

    latest = int(w3.eth.block_number)
    block_stride = int(os.environ.get("VAULT_BLOCK_STRIDE", "600"))

    # Contracts
    vault = w3.eth.contract(address=ARRAKIS_VAULT, abi=VAULT_ABI)
    module_addr = Web3.to_checksum_address(vault.functions.module().call())
    module = w3.eth.contract(address=module_addr, abi=UNIV4_MODULE_ABI)
    quoter = w3.eth.contract(address=UNIV4_QUOTER, abi=V4_QUOTER_ABI)

    owner = Web3.to_checksum_address(vault.functions.owner().call())
    c0, c1, fee_u24, tick_spacing, hooks = module.functions.poolKey().call()
    c0 = Web3.to_checksum_address(c0)
    c1 = Web3.to_checksum_address(c1)
    hooks = Web3.to_checksum_address(hooks)

    pool_key = PoolKey(currency0=c0, currency1=c1, fee=int(fee_u24), tick_spacing=int(tick_spacing), hooks=hooks)
    fee_rate = fee_to_rate(pool_key.fee)

    sym0, dec0 = currency_meta(w3, c0)
    sym1, dec1 = currency_meta(w3, c1)

    # Spot IXS/ETH needs mapping
    if ixs_addr.lower() not in (c0.lower(), c1.lower()):
        raise RuntimeError("IXS from metadata is not in UniV4 pool currencies (check token address).")

    # UniV3 ETH/USD pool init
    pool_v3, v3_token0, v3_token1 = init_univ3_eth_usd_pool(w3)

    blocks = list(range(mig_block, latest + 1, block_stride))
    if blocks[-1] != latest:
        blocks.append(latest)

    tiny_c0 = Decimal(os.environ.get("VAULT_SPOT_TINY_C0", "0.000001"))

    rows = []
    for i, b in enumerate(blocks, start=1):
        try:
            blk = w3.eth.get_block(int(b))
        except Exception:
            continue

        ts = int(blk["timestamp"])
        dt = pd.to_datetime(ts, unit="s", utc=True)

        eth_usd = eth_usd_from_univ3(pool_v3, v3_token0, v3_token1, int(b))
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

        # Convert to spot IXS per ETH
        # If currency0 is ETH => spot_1_per_0 = IXS/ETH.
        # If currency0 is IXS => spot_1_per_0 = ETH/IXS, so invert.
        if c0.lower() == ADDRESS_ZERO.lower():  # ETH as currency0
            spot_ixs_per_eth = spot_1_per_0
            spot_eth_per_ixs = (Decimal(1) / spot_ixs_per_eth) if spot_ixs_per_eth > 0 else Decimal("NaN")
        else:
            # currency0 is not ETH (likely IXS)
            spot_eth_per_ixs = spot_1_per_0
            spot_ixs_per_eth = (Decimal(1) / spot_eth_per_ixs) if spot_eth_per_ixs > 0 else Decimal("NaN")

        if not spot_ixs_per_eth.is_finite() or spot_ixs_per_eth <= 0:
            continue
        if not spot_eth_per_ixs.is_finite() or spot_eth_per_ixs <= 0:
            continue

        # Vault-reported underlying (unknown ordering)
        try:
            u0_raw, u1_raw = vault.functions.totalUnderlying().call(block_identifier=int(b))
        except Exception:
            continue
        u0_raw = int(u0_raw)
        u1_raw = int(u1_raw)

        # We don't know which decimals apply to vault u0/u1; since poolKey currencies are both 18 in your case,
        # we can safely treat both as 18 for human conversion. (If that changes, we must add vault token getters.)
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
                "vault_underlying0": str(u0),
                "vault_underlying1": str(u1),
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

    # Baselines
    amt_eth0 = float(df.loc[0, "amt_eth"])
    amt_ixs0 = float(df.loc[0, "amt_ixs"])
    df["hold_value_usd"] = amt_eth0 * df["eth_usd"] + amt_ixs0 * df["ixs_usd"]

    p0 = float(df.loc[0, "spot_eth_per_ixs"])
    if not np.isfinite(p0) or p0 <= 0 or amt_eth0 <= 0 or amt_ixs0 <= 0:
        df["full_range_lp_value_usd"] = np.nan
        df["full_range_lp_amt_eth"] = np.nan
        df["full_range_lp_amt_ixs"] = np.nan
    else:
        k = amt_eth0 * amt_ixs0
        p = df["spot_eth_per_ixs"].astype(float).values
        amt_ixs_fr = np.sqrt(k / p)
        amt_eth_fr = np.sqrt(k * p)
        df["full_range_lp_amt_ixs"] = amt_ixs_fr
        df["full_range_lp_amt_eth"] = amt_eth_fr
        df["full_range_lp_value_usd"] = amt_eth_fr * df["eth_usd"].values + amt_ixs_fr * df["ixs_usd"].values

    v0 = df.loc[0, "value_total_usd"]
    df["vault_value_index"] = df["value_total_usd"] / v0
    h0 = df.loc[0, "hold_value_usd"]
    df["hold_value_index"] = df["hold_value_usd"] / h0 if h0 and np.isfinite(h0) and h0 > 0 else np.nan
    fr0 = df.loc[0, "full_range_lp_value_usd"]
    df["full_range_lp_value_index"] = df["full_range_lp_value_usd"] / fr0 if fr0 and np.isfinite(fr0) and fr0 > 0 else np.nan

    out_csv = processed / "vault_timeseries.csv"
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")
    print(df.head(8).to_string(index=False))

    print("\nNote: amounts are sourced from vault.totalUnderlying() and mapped to (IXS, ETH) by matching amount ratio to spot IXS/ETH.")


if __name__ == "__main__":
    main()
