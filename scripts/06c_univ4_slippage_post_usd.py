import os
import json
from dataclasses import dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

getcontext().prec = 60

# -------- Addresses (mainnet) --------
ARRAKIS_VAULT = Web3.to_checksum_address("0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600")

UNIV4_QUOTER = Web3.to_checksum_address("0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203")

# ETH/USD via UniV3 USDC/WETH slot0 (you already validated this path)
UNIV3_FACTORY = Web3.to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984")
WETH = Web3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
USDC = Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
UNIV3_FEE = 500  # 0.05%

ADDRESS_ZERO = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

# -------- Minimal ABIs --------
ERC20_ABI = [
    {"name": "decimals", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint8"}]},
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

# IV4Quoter.quoteExactInputSingle params struct (PoolKey, zeroForOne, exactAmount, hookData) [page:1]
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
]  # [page:1]

# Arrakis UniV4 standard module interface: poolKey() + poolManager() [page:3]
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
        "name": "poolManager",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"type": "address"}],
    },
]  # [page:3]

# We don't know the exact Arrakis vault getter name for the module; try common ones.
VAULT_MODULE_GETTERS = [
    ("module", [{"name": "module", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("activeModule", [{"name": "activeModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("getModule", [{"name": "getModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("strategy", [{"name": "strategy", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
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


def fee_to_rate(fee_uint24: int) -> Decimal:
    # fee in 1e-6 units in v4 quoter interface docs [page:1]
    return Decimal(fee_uint24) / Decimal(1_000_000)


def slippage_excl_fees_pct(spot: Decimal, avg: Decimal, fee_rate: Decimal) -> Decimal:
    # Assessment definition: |spot-avg|/spot*100 - fee*100 [file:335]
    if spot <= 0 or avg <= 0:
        return Decimal("NaN")
    gross = (abs(spot - avg) / spot) * Decimal(100)
    return gross - fee_rate * Decimal(100)


def currency_decimals(w3: Web3, currency: str) -> int:
    if currency.lower() == ADDRESS_ZERO.lower():
        return 18
    c = w3.eth.contract(address=Web3.to_checksum_address(currency), abi=ERC20_ABI)
    return int(c.functions.decimals().call())


def to_raw(amount: Decimal, decimals: int) -> int:
    if amount <= 0:
        return 0
    q = amount * (Decimal(10) ** decimals)
    return int(q.to_integral_value(rounding="ROUND_FLOOR"))


def from_raw(amount_raw: int, decimals: int) -> Decimal:
    return Decimal(amount_raw) / (Decimal(10) ** decimals)


def quote_exact_in_single(quoter, pool_key: PoolKey, token_in: str, amount_in_raw: int, block_number: int) -> Tuple[int, int]:
    if amount_in_raw <= 0:
        return 0, 0
    if token_in.lower() == pool_key.currency0.lower():
        zero_for_one = True  # currency0 -> currency1 [page:1]
    elif token_in.lower() == pool_key.currency1.lower():
        zero_for_one = False
    else:
        raise ValueError("token_in not in pool currencies")

    params = (pool_key.as_tuple(), bool(zero_for_one), int(amount_in_raw), b"")
    amount_out, gas_est = quoter.functions.quoteExactInputSingle(params).call(block_identifier=int(block_number))
    return int(amount_out), int(gas_est)


def eth_usd_from_univ3(w3: Web3, block_number: int) -> Decimal:
    factory = w3.eth.contract(address=UNIV3_FACTORY, abi=FACTORY_ABI)
    pool_addr = Web3.to_checksum_address(factory.functions.getPool(WETH, USDC, UNIV3_FEE).call())
    pool = w3.eth.contract(address=pool_addr, abi=POOL_V3_ABI)

    token0 = Web3.to_checksum_address(pool.functions.token0().call())
    token1 = Web3.to_checksum_address(pool.functions.token1().call())
    sqrtp = int(pool.functions.slot0().call(block_identifier=int(block_number))[0])
    if sqrtp <= 0:
        return Decimal("NaN")

    p_raw = Decimal(sqrtp) * Decimal(sqrtp) / (Decimal(2) ** 192)

    # Want USDC per WETH
    if token0.lower() == USDC.lower() and token1.lower() == WETH.lower():
        return (Decimal(1) / p_raw) * (Decimal(10) ** 12)
    if token0.lower() == WETH.lower() and token1.lower() == USDC.lower():
        return p_raw * (Decimal(10) ** 12)
    return Decimal("NaN")


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
    raise RuntimeError("Could not detect Arrakis module address from vault (tried module/activeModule/getModule/strategy).")


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

    meta_json = raw / "univ2_pair_metadata.json"
    mig_final_json = processed / "migration_block_final.json"
    if not meta_json.exists() or not mig_final_json.exists():
        raise RuntimeError("Missing univ2_pair_metadata.json or migration_block_final.json")

    meta = json.loads(meta_json.read_text(encoding="utf-8"))
    mig = json.loads(mig_final_json.read_text(encoding="utf-8"))
    mig_block = int(mig["selected"]["migration_block_final"])

    ixs_addr = Web3.to_checksum_address(meta["token0"]["address"])
    sym_ixs = meta["token0"]["symbol"]
    sym_eth = "ETH"

    latest = int(w3.eth.block_number)
    print("Migration block:", mig_block)
    print("Latest block:", latest)
    print("Arrakis vault:", ARRAKIS_VAULT)
    print("UniV4 Quoter:", UNIV4_QUOTER)

    # 1) Read PoolKey from Arrakis module (view) [page:3]
    module_addr = detect_arrakis_module(w3, ARRAKIS_VAULT)
    module = w3.eth.contract(address=module_addr, abi=UNIV4_MODULE_ABI)

    (c0, c1, fee_u24, tick_spacing, hooks) = module.functions.poolKey().call()
    pool_manager_addr = Web3.to_checksum_address(module.functions.poolManager().call())

    c0 = Web3.to_checksum_address(c0)
    c1 = Web3.to_checksum_address(c1)
    hooks = Web3.to_checksum_address(hooks)

    pool_key = PoolKey(currency0=c0, currency1=c1, fee=int(fee_u24), tick_spacing=int(tick_spacing), hooks=hooks)
    fee_rate = fee_to_rate(pool_key.fee)

    print("Module:", module_addr)
    print("PoolManager (from module):", pool_manager_addr)
    print("PoolKey currency0:", pool_key.currency0)
    print("PoolKey currency1:", pool_key.currency1)
    print("PoolKey fee:", pool_key.fee, f"(rate={float(fee_rate):.6f})")
    print("PoolKey tickSpacing:", pool_key.tick_spacing)
    print("PoolKey hooks:", pool_key.hooks)

    if ixs_addr.lower() not in (pool_key.currency0.lower(), pool_key.currency1.lower()):
        raise RuntimeError("IXS from UniV2 metadata is not in the UniV4 PoolKey currencies. Verify token address/link.")

    eth_currency = pool_key.currency1 if pool_key.currency0.lower() == ixs_addr.lower() else pool_key.currency0

    ixs_dec = currency_decimals(w3, ixs_addr)
    eth_dec = currency_decimals(w3, eth_currency)

    quoter = w3.eth.contract(address=UNIV4_QUOTER, abi=V4_QUOTER_ABI)

    # 2) Sample post-migration blocks
    block_stride = int(os.environ.get("UNIV4_BLOCK_STRIDE", "300"))
    blocks = list(range(mig_block, latest + 1, block_stride))
    if blocks[-1] != latest:
        blocks.append(latest)

    usd_sizes = [1000, 5000, 10000, 50000]

    rows = []
    for i, b in enumerate(blocks, start=1):
        blk = w3.eth.get_block(int(b))
        ts = int(blk["timestamp"])
        dt = pd.to_datetime(ts, unit="s", utc=True)

        eth_usd = eth_usd_from_univ3(w3, int(b))
        if not eth_usd.is_finite() or eth_usd <= 0:
            continue

        # Spot ETH/IXS excluding fees using micro-quote and de-fee by (1-fee)
        tiny_ixs = Decimal(1) / Decimal(10_000)  # 0.0001 IXS
        tiny_ixs_raw = max(1, to_raw(tiny_ixs, ixs_dec))

        try:
            out_eth_raw, _ = quote_exact_in_single(quoter, pool_key, token_in=ixs_addr, amount_in_raw=tiny_ixs_raw, block_number=int(b))
        except Exception:
            continue

        out_eth = from_raw(out_eth_raw, eth_dec)
        in_ixs = from_raw(tiny_ixs_raw, ixs_dec)
        if out_eth <= 0 or in_ixs <= 0:
            continue

        spot_eth_per_ixs = out_eth / (in_ixs * (Decimal(1) - fee_rate))
        spot_ixs_usd = spot_eth_per_ixs * eth_usd
        if spot_eth_per_ixs <= 0 or spot_ixs_usd <= 0:
            continue

        for usd_notional in usd_sizes:
            usdN = Decimal(usd_notional)

            # IXS -> ETH
            amount_in_ixs = usdN / spot_ixs_usd
            amount_in_ixs_raw = to_raw(amount_in_ixs, ixs_dec)

            try:
                out_eth_raw, gas1 = quote_exact_in_single(quoter, pool_key, token_in=ixs_addr, amount_in_raw=amount_in_ixs_raw, block_number=int(b))
            except Exception:
                out_eth_raw, gas1 = 0, 0

            out_eth = from_raw(out_eth_raw, eth_dec)
            avg_exec_usd_per_eth = (usdN / out_eth) if out_eth > 0 else Decimal("NaN")
            slip1 = slippage_excl_fees_pct(eth_usd, avg_exec_usd_per_eth, fee_rate)

            rows.append(
                {
                    "block_number": int(b),
                    "timestamp": ts,
                    "datetime_utc": str(dt),
                    "direction": f"{sym_ixs}->{sym_eth}",
                    "usd_notional_in": float(usd_notional),
                    "amount_in": float(amount_in_ixs),
                    "amount_in_unit": sym_ixs,
                    "amount_out": float(out_eth) if out_eth.is_finite() else np.nan,
                    "amount_out_unit": sym_eth,
                    "spot_price": float(eth_usd),
                    "spot_price_unit": "USD_per_ETH",
                    "avg_exec_price": float(avg_exec_usd_per_eth) if avg_exec_usd_per_eth.is_finite() else np.nan,
                    "avg_exec_price_unit": "USD_per_ETH",
                    "slippage_pct": float(slip1) if slip1.is_finite() else np.nan,
                    "fee_rate": float(fee_rate),
                    "fee_uint24": int(pool_key.fee),
                    "tick_spacing": int(pool_key.tick_spacing),
                    "hooks": pool_key.hooks,
                    "gas_estimate": int(gas1),
                }
            )

            # ETH -> IXS
            amount_in_eth = usdN / eth_usd
            amount_in_eth_raw = to_raw(amount_in_eth, eth_dec)

            try:
                out_ixs_raw, gas2 = quote_exact_in_single(quoter, pool_key, token_in=eth_currency, amount_in_raw=amount_in_eth_raw, block_number=int(b))
            except Exception:
                out_ixs_raw, gas2 = 0, 0

            out_ixs = from_raw(out_ixs_raw, ixs_dec)
            avg_exec_usd_per_ixs = (usdN / out_ixs) if out_ixs > 0 else Decimal("NaN")
            slip2 = slippage_excl_fees_pct(spot_ixs_usd, avg_exec_usd_per_ixs, fee_rate)

            rows.append(
                {
                    "block_number": int(b),
                    "timestamp": ts,
                    "datetime_utc": str(dt),
                    "direction": f"{sym_eth}->{sym_ixs}",
                    "usd_notional_in": float(usd_notional),
                    "amount_in": float(amount_in_eth),
                    "amount_in_unit": sym_eth,
                    "amount_out": float(out_ixs) if out_ixs.is_finite() else np.nan,
                    "amount_out_unit": sym_ixs,
                    "spot_price": float(spot_ixs_usd),
                    "spot_price_unit": "USD_per_IXS",
                    "avg_exec_price": float(avg_exec_usd_per_ixs) if avg_exec_usd_per_ixs.is_finite() else np.nan,
                    "avg_exec_price_unit": "USD_per_IXS",
                    "slippage_pct": float(slip2) if slip2.is_finite() else np.nan,
                    "fee_rate": float(fee_rate),
                    "fee_uint24": int(pool_key.fee),
                    "tick_spacing": int(pool_key.tick_spacing),
                    "hooks": pool_key.hooks,
                    "gas_estimate": int(gas2),
                }
            )

        if i % 25 == 0:
            print(f"processed {i}/{len(blocks)} blocks")

    out = pd.DataFrame(rows).sort_values(["block_number", "direction", "usd_notional_in"]).reset_index(drop=True)
    out_csv = processed / "univ4_slippage_post_usd.csv"
    out.to_csv(out_csv, index=False)

    print(f"Wrote {out_csv}")
    print(out.head(12).to_string(index=False))


if __name__ == "__main__":
    main()
