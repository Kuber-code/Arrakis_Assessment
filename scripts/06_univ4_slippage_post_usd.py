import os
import json
from dataclasses import dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

getcontext().prec = 70

CHAIN_ID_MAINNET = 1

# Arrakis vault (used to discover the UniV4 poolKey)
ARRAKIS_VAULT = Web3.to_checksum_address("0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600")

# UniV4 Quoter (from challenge hint / your pipeline)
UNIV4_QUOTER = Web3.to_checksum_address("0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203")

ADDRESS_ZERO = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

# ETH/USD via UniV3 WETH/USDC slot0
UNIV3_FACTORY = Web3.to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984")
WETH = Web3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
USDC = Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
UNIV3_FEE = 500  # 0.05%

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
    # optional: some modules expose poolManager(), but we don't need it for quoting
    {"name": "poolManager", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
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


def ensure_dirs(root: Path) -> Tuple[Path, Path, Path]:
    raw = root / "data" / "raw"
    processed = root / "data" / "processed"
    figures = root / "figures"
    raw.mkdir(parents=True, exist_ok=True)
    processed.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)
    return raw, processed, figures


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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
    # Want USDC per WETH; adjust decimals: USDC 6, WETH 18 => factor 1e12
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

    # Adjust for fee so "spot" is pre-fee
    return (out_amt / in_amt) / denom


def compute_slippage_pct(spot: Decimal, exec_price: Decimal, fee_rate: Decimal) -> Tuple[Decimal, Decimal]:
    if spot <= 0 or exec_price <= 0:
        return Decimal("NaN"), Decimal("NaN")
    gross = (abs(spot - exec_price) / spot) * Decimal(100)
    excl = gross - (fee_rate * Decimal(100))
    return gross, excl


def main():
    load_dotenv()
    rpc_url = os.environ.get("RPC_URL")
    if not rpc_url:
        raise RuntimeError("Missing RPC_URL in environment or .env")

    stride = int(os.environ.get("SLIPPAGE_BLOCK_STRIDE", "600"))
    tiny_c0 = Decimal(os.environ.get("V4_SPOT_TINY_C0", "0.000001"))

    notionals = [
        Decimal("1000"),
        Decimal("5000"),
        Decimal("10000"),
        Decimal("50000"),
    ]

    root = Path(__file__).resolve().parents[1]
    raw, processed, _figures = ensure_dirs(root)

    meta_path = raw / "univ2_pair_metadata.json"
    mig_path = raw / "migration_block_final.json"
    if not meta_path.exists():
        raise RuntimeError(f"Missing {meta_path}. Run scripts/01_univ2_pair_metadata.py first.")
    if not mig_path.exists():
        raise RuntimeError(f"Missing {mig_path}. Run scripts/04_write_migration_block_final.py first.")

    meta = load_json(meta_path)
    mig = load_json(mig_path)
    migration_block = int(mig["selected"]["migration_block_final"])

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError("Cannot connect to RPC_URL")
    if int(w3.eth.chain_id) != CHAIN_ID_MAINNET:
        raise RuntimeError(f"Not Ethereum mainnet: chain_id={w3.eth.chain_id}")

    latest = int(w3.eth.block_number)

    # Discover poolKey via vault.module().poolKey()
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

    # sanity: token from metadata should be in poolKey
    ixs_addr = Web3.to_checksum_address(meta["token0"]["address"])
    if ixs_addr.lower() not in (c0.lower(), c1.lower()):
        # try token1, depending on how metadata was written
        ixs_addr = Web3.to_checksum_address(meta["token1"]["address"])
    if ixs_addr.lower() not in (c0.lower(), c1.lower()):
        raise RuntimeError("IXS from univ2_pair_metadata.json is not one of the UniV4 pool currencies.")

    # UniV3 ETH/USD init
    pool_v3, v3_t0, v3_t1 = init_univ3_eth_usd_pool(w3)

    blocks = list(range(int(migration_block), int(latest) + 1, int(stride)))
    if blocks[-1] != latest:
        blocks.append(latest)

    print(f"Migration block: {migration_block}")
    print(f"Latest block: {latest}")
    print(f"Arrakis vault: {ARRAKIS_VAULT}")
    print(f"UniV4 Quoter: {UNIV4_QUOTER}")
    print(f"Module: {module_addr}")
    try:
        pm = Web3.to_checksum_address(module.functions.poolManager().call())
        print(f"PoolManager (from module): {pm}")
    except Exception:
        pass
    print(f"PoolKey currency0: {c0}")
    print(f"PoolKey currency1: {c1}")
    print(f"PoolKey fee: {pool_key.fee} (rate={float(fee_rate):.6f})")
    print(f"PoolKey tickSpacing: {pool_key.tick_spacing}")
    print(f"PoolKey hooks: {pool_key.hooks}")

    quoter = w3.eth.contract(address=UNIV4_QUOTER, abi=V4_QUOTER_ABI)

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

        # spot token1 per token0 via micro-quote (currency0 -> currency1)
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

        # Convert to both spot formats
        if c0.lower() == ADDRESS_ZERO.lower():  # currency0 is ETH
            spot_ixs_per_eth = spot_1_per_0  # IXS per ETH
            spot_eth_per_ixs = (Decimal(1) / spot_ixs_per_eth) if spot_ixs_per_eth > 0 else Decimal("NaN")
        else:
            # currency0 is not ETH (likely IXS)
            spot_eth_per_ixs = spot_1_per_0  # ETH per IXS
            spot_ixs_per_eth = (Decimal(1) / spot_eth_per_ixs) if spot_eth_per_ixs > 0 else Decimal("NaN")

        if not spot_ixs_per_eth.is_finite() or spot_ixs_per_eth <= 0:
            continue
        if not spot_eth_per_ixs.is_finite() or spot_eth_per_ixs <= 0:
            continue

        # Derived IXS/USD
        ixs_usd = spot_eth_per_ixs * eth_usd
        if not ixs_usd.is_finite() or ixs_usd <= 0:
            continue

        for usd_notional in notionals:
            # ETH -> IXS
            amt_in_eth = usd_notional / eth_usd
            amt_in_eth_raw = to_raw(amt_in_eth, 18)
            try:
                out_ixs_raw, gas_est = quote_exact_in_single(
                    quoter, pool_key, token_in=c0, amount_in_raw=amt_in_eth_raw, block_number=int(b)
                )
            except Exception:
                continue
            amt_out_ixs = from_raw(out_ixs_raw, 18)

            if amt_in_eth > 0 and amt_out_ixs > 0:
                exec_ixs_per_eth = amt_out_ixs / amt_in_eth
                gross, excl = compute_slippage_pct(spot_ixs_per_eth, exec_ixs_per_eth, fee_rate)

                rows.append(
                    {
                        "period": "UniV4 post",
                        "block_number": int(b),
                        "timestamp": ts,
                        "datetime_utc": str(dt),
                        "direction": "ETH->IXS",
                        "usd_notional_in": float(usd_notional),
                        "eth_usd": float(eth_usd),
                        "amount_in": float(amt_in_eth),
                        "amount_in_unit": "ETH",
                        "amount_out": float(amt_out_ixs),
                        "amount_out_unit": "IXS",
                        "spot_price": float(spot_ixs_per_eth),
                        "spot_price_unit": "IXS_per_ETH",
                        "avg_exec_price": float(exec_ixs_per_eth),
                        "avg_exec_price_unit": "IXS_per_ETH",
                        "gross_slippage_pct": float(gross) if gross.is_finite() else float("nan"),
                        "slippage_excl_fees_pct_raw": float(excl) if excl.is_finite() else float("nan"),
                        "fee_rate": float(fee_rate),
                        "fee_uint24": int(pool_key.fee),
                        "tick_spacing": int(pool_key.tick_spacing),
                        "hooks": pool_key.hooks,
                        "gas_estimate": int(gas_est),
                    }
                )

            # IXS -> ETH
            amt_in_ixs = usd_notional / ixs_usd
            amt_in_ixs_raw = to_raw(amt_in_ixs, 18)
            try:
                out_eth_raw, gas_est2 = quote_exact_in_single(
                    quoter, pool_key, token_in=c1, amount_in_raw=amt_in_ixs_raw, block_number=int(b)
                )
            except Exception:
                continue
            amt_out_eth = from_raw(out_eth_raw, 18)

            if amt_in_ixs > 0 and amt_out_eth > 0:
                exec_eth_per_ixs = amt_out_eth / amt_in_ixs
                gross, excl = compute_slippage_pct(spot_eth_per_ixs, exec_eth_per_ixs, fee_rate)

                rows.append(
                    {
                        "period": "UniV4 post",
                        "block_number": int(b),
                        "timestamp": ts,
                        "datetime_utc": str(dt),
                        "direction": "IXS->ETH",
                        "usd_notional_in": float(usd_notional),
                        "eth_usd": float(eth_usd),
                        "amount_in": float(amt_in_ixs),
                        "amount_in_unit": "IXS",
                        "amount_out": float(amt_out_eth),
                        "amount_out_unit": "ETH",
                        "spot_price": float(spot_eth_per_ixs),
                        "spot_price_unit": "ETH_per_IXS",
                        "avg_exec_price": float(exec_eth_per_ixs),
                        "avg_exec_price_unit": "ETH_per_IXS",
                        "gross_slippage_pct": float(gross) if gross.is_finite() else float("nan"),
                        "slippage_excl_fees_pct_raw": float(excl) if excl.is_finite() else float("nan"),
                        "fee_rate": float(fee_rate),
                        "fee_uint24": int(pool_key.fee),
                        "tick_spacing": int(pool_key.tick_spacing),
                        "hooks": pool_key.hooks,
                        "gas_estimate": int(gas_est2),
                    }
                )

        if i % 25 == 0:
            print(f"processed {i}/{len(blocks)} blocks")

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Produced 0 rows. Check RPC, quoter calls, and block sampling.")

    df = df.sort_values(["block_number", "direction", "usd_notional_in"]).reset_index(drop=True)

    out_csv = processed / "univ4_slippage_post_usd.csv"
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")
    print(df.head(12).to_string(index=False))


if __name__ == "__main__":
    main()
