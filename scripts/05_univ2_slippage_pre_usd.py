import os
import json
from pathlib import Path
from decimal import Decimal, getcontext
from typing import Dict, Any, Tuple, Optional

import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

getcontext().prec = 70

# --- Constants / addresses ---
CHAIN_ID_MAINNET = 1

UNIV2_PAIR = Web3.to_checksum_address("0xC09bf2B1Bc8725903C509e8CAeef9190857215A8")

# UniV2 fee (0.30%)
UNIV2_FEE_RATE = Decimal("0.003")
UNIV2_FEE_NUM = 997
UNIV2_FEE_DEN = 1000

# ETH/USD from UniV3 WETH/USDC slot0
UNIV3_FACTORY = Web3.to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984")
WETH = Web3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
USDC = Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
UNIV3_FEE = 500  # 0.05%

# --- Minimal ABIs ---
UNIV2_PAIR_ABI = [
    {
        "name": "getReserves",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [
            {"name": "_reserve0", "type": "uint112"},
            {"name": "_reserve1", "type": "uint112"},
            {"name": "_blockTimestampLast", "type": "uint32"},
        ],
    },
    {"name": "token0", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "token1", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
]

ERC20_ABI = [
    {"name": "decimals", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint8"}]},
    {"name": "symbol", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "string"}]},
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


def ensure_dirs(root: Path) -> Tuple[Path, Path, Path]:
    raw = root / "data" / "raw"
    processed = root / "data" / "processed"
    figures = root / "figures"
    raw.mkdir(parents=True, exist_ok=True)
    processed.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)
    return raw, processed, figures


def from_raw(x: int, decimals: int) -> Decimal:
    return Decimal(int(x)) / (Decimal(10) ** decimals)


def to_raw(x: Decimal, decimals: int) -> int:
    if x <= 0:
        return 0
    return int((x * (Decimal(10) ** decimals)).to_integral_value(rounding="ROUND_FLOOR"))


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def safe_checksum(addr: str) -> str:
    return Web3.to_checksum_address(addr)


def token_meta_from_chain(w3: Web3, token: str) -> Tuple[str, int]:
    c = w3.eth.contract(address=safe_checksum(token), abi=ERC20_ABI)
    sym = c.functions.symbol().call()
    dec = int(c.functions.decimals().call())
    return sym, dec


def init_univ3_eth_usd_pool(w3: Web3):
    factory = w3.eth.contract(address=UNIV3_FACTORY, abi=FACTORY_ABI)
    pool_addr = safe_checksum(factory.functions.getPool(WETH, USDC, UNIV3_FEE).call())
    if int(pool_addr, 16) == 0:
        raise RuntimeError("UniV3 WETH/USDC pool not found via factory.getPool.")
    pool = w3.eth.contract(address=pool_addr, abi=POOL_V3_ABI)
    token0 = safe_checksum(pool.functions.token0().call())
    token1 = safe_checksum(pool.functions.token1().call())
    return pool, token0, token1


def eth_usd_from_univ3(pool_v3, token0: str, token1: str, block_number: int) -> Decimal:
    sqrtp = int(pool_v3.functions.slot0().call(block_identifier=int(block_number))[0])
    if sqrtp <= 0:
        return Decimal("NaN")
    p_raw = (Decimal(sqrtp) * Decimal(sqrtp)) / (Decimal(2) ** 192)  # token1/token0
    # Want USDC per WETH; adjust by decimals: USDC 6, WETH 18 => factor 1e12
    if token0.lower() == USDC.lower() and token1.lower() == WETH.lower():
        return (Decimal(1) / p_raw) * (Decimal(10) ** 12)
    if token0.lower() == WETH.lower() and token1.lower() == USDC.lower():
        return p_raw * (Decimal(10) ** 12)
    return Decimal("NaN")


def univ2_amount_out(amount_in_raw: int, reserve_in_raw: int, reserve_out_raw: int) -> int:
    if amount_in_raw <= 0 or reserve_in_raw <= 0 or reserve_out_raw <= 0:
        return 0
    amount_in_with_fee = amount_in_raw * UNIV2_FEE_NUM
    numerator = amount_in_with_fee * reserve_out_raw
    denominator = reserve_in_raw * UNIV2_FEE_DEN + amount_in_with_fee
    return int(numerator // denominator)


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

    # Defaults chosen to roughly match ~800 points pre-migration with stride=600.
    stride = int(os.environ.get("SLIPPAGE_BLOCK_STRIDE", "600"))
    lookback_blocks = int(os.environ.get("PRE_LOOKBACK_BLOCKS", "500000"))

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

    pair = w3.eth.contract(address=UNIV2_PAIR, abi=UNIV2_PAIR_ABI)
    pair_token0 = safe_checksum(pair.functions.token0().call())
    pair_token1 = safe_checksum(pair.functions.token1().call())

    # Expect metadata to include token0/token1; if not, fall back to on-chain
    meta_t0 = safe_checksum(meta["token0"]["address"]) if "token0" in meta and "address" in meta["token0"] else pair_token0
    meta_t1 = safe_checksum(meta["token1"]["address"]) if "token1" in meta and "address" in meta["token1"] else pair_token1

    if {pair_token0.lower(), pair_token1.lower()} != {meta_t0.lower(), meta_t1.lower()}:
        raise RuntimeError("Token addresses from metadata do not match UniV2 pair.token0/token1 (check univ2_pair_metadata.json).")

    # Identify which side is WETH and which is IXS
    if pair_token0.lower() == WETH.lower():
        weth_is_token0 = True
        weth_addr = pair_token0
        ixs_addr = pair_token1
    elif pair_token1.lower() == WETH.lower():
        weth_is_token0 = False
        weth_addr = pair_token1
        ixs_addr = pair_token0
    else:
        raise RuntimeError("UniV2 pair does not contain WETH; expected IXS/WETH for ETH-denominated analysis.")

    # Decimals/symbols
    # Prefer metadata if it contains them; otherwise fetch on-chain
    ixs_symbol = meta.get("token0", {}).get("symbol") if meta.get("token0", {}).get("address", "").lower() == ixs_addr.lower() else meta.get("token1", {}).get("symbol")
    try:
        ixs_dec = int(meta.get("token0", {}).get("decimals")) if meta.get("token0", {}).get("address", "").lower() == ixs_addr.lower() else int(meta.get("token1", {}).get("decimals"))
    except Exception:
        ixs_dec = None

    if not ixs_symbol or ixs_dec is None:
        ixs_symbol, ixs_dec = token_meta_from_chain(w3, ixs_addr)

    weth_symbol, weth_dec = token_meta_from_chain(w3, weth_addr)  # WETH is standard

    # UniV3 pool for ETH/USD
    pool_v3, v3_token0, v3_token1 = init_univ3_eth_usd_pool(w3)

    # Define block range (pre-migration)
    end_block = max(0, migration_block - 1)
    start_block = max(0, end_block - lookback_blocks)

    blocks = list(range(start_block, end_block + 1, stride))
    if blocks and blocks[-1] != end_block:
        blocks.append(end_block)
    if not blocks:
        raise RuntimeError("Empty block range for pre-migration period.")

    print(f"UniV2 pair: {UNIV2_PAIR}")
    print(f"Migration block: {migration_block} (pre ends at {end_block})")
    print(f"Block range: {start_block}..{end_block} stride={stride} (n={len(blocks)})")
    print(f"IXS token: {ixs_addr} ({ixs_symbol}, dec={ixs_dec})")
    print(f"WETH token: {weth_addr} ({weth_symbol}, dec={weth_dec})")

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

        try:
            r0, r1, _ = pair.functions.getReserves().call(block_identifier=int(b))
        except Exception:
            continue

        r0 = int(r0)
        r1 = int(r1)

        # Map reserves to (weth, ixs)
        if weth_is_token0:
            reserve_weth_raw = r0
            reserve_ixs_raw = r1
        else:
            reserve_weth_raw = r1
            reserve_ixs_raw = r0

        reserve_weth = from_raw(reserve_weth_raw, weth_dec)
        reserve_ixs = from_raw(reserve_ixs_raw, ixs_dec)

        if reserve_weth <= 0 or reserve_ixs <= 0:
            continue

        # Spot prices from reserves (before any trade)
        spot_ixs_per_eth = reserve_ixs / reserve_weth
        spot_eth_per_ixs = reserve_weth / reserve_ixs

        # IXS USD derived from spot and ETH/USD
        ixs_usd = spot_eth_per_ixs * eth_usd
        if not ixs_usd.is_finite() or ixs_usd <= 0:
            continue

        for usd_notional in notionals:
            # Direction: ETH (WETH) -> IXS
            amt_in_weth = usd_notional / eth_usd
            amt_in_weth_raw = to_raw(amt_in_weth, weth_dec)
            amt_out_ixs_raw = univ2_amount_out(amt_in_weth_raw, reserve_weth_raw, reserve_ixs_raw)
            amt_out_ixs = from_raw(amt_out_ixs_raw, ixs_dec)

            if amt_in_weth > 0 and amt_out_ixs > 0:
                exec_ixs_per_eth = amt_out_ixs / amt_in_weth
                gross, excl = compute_slippage_pct(spot_ixs_per_eth, exec_ixs_per_eth, UNIV2_FEE_RATE)

                rows.append(
                    {
                        "period": "UniV2 pre",
                        "block_number": int(b),
                        "timestamp": ts,
                        "datetime_utc": str(dt),
                        "direction": "ETH->IXS",
                        "usd_notional_in": float(usd_notional),
                        "eth_usd": float(eth_usd),
                        "ixs_usd": float(ixs_usd),
                        "amount_in": float(amt_in_weth),
                        "amount_in_unit": "ETH",
                        "amount_out": float(amt_out_ixs),
                        "amount_out_unit": "IXS",
                        "spot_price": float(spot_ixs_per_eth),
                        "spot_price_unit": "IXS_per_ETH",
                        "avg_exec_price": float(exec_ixs_per_eth),
                        "avg_exec_price_unit": "IXS_per_ETH",
                        "gross_slippage_pct": float(gross) if gross.is_finite() else float("nan"),
                        "slippage_excl_fees_pct_raw": float(excl) if excl.is_finite() else float("nan"),
                        "fee_rate": float(UNIV2_FEE_RATE),
                    }
                )

            # Direction: IXS -> ETH (WETH)
            amt_in_ixs = usd_notional / ixs_usd
            amt_in_ixs_raw = to_raw(amt_in_ixs, ixs_dec)
            amt_out_weth_raw = univ2_amount_out(amt_in_ixs_raw, reserve_ixs_raw, reserve_weth_raw)
            amt_out_weth = from_raw(amt_out_weth_raw, weth_dec)

            if amt_in_ixs > 0 and amt_out_weth > 0:
                exec_eth_per_ixs = amt_out_weth / amt_in_ixs
                gross, excl = compute_slippage_pct(spot_eth_per_ixs, exec_eth_per_ixs, UNIV2_FEE_RATE)

                rows.append(
                    {
                        "period": "UniV2 pre",
                        "block_number": int(b),
                        "timestamp": ts,
                        "datetime_utc": str(dt),
                        "direction": "IXS->ETH",
                        "usd_notional_in": float(usd_notional),
                        "eth_usd": float(eth_usd),
                        "ixs_usd": float(ixs_usd),
                        "amount_in": float(amt_in_ixs),
                        "amount_in_unit": "IXS",
                        "amount_out": float(amt_out_weth),
                        "amount_out_unit": "ETH",
                        "spot_price": float(spot_eth_per_ixs),
                        "spot_price_unit": "ETH_per_IXS",
                        "avg_exec_price": float(exec_eth_per_ixs),
                        "avg_exec_price_unit": "ETH_per_IXS",
                        "gross_slippage_pct": float(gross) if gross.is_finite() else float("nan"),
                        "slippage_excl_fees_pct_raw": float(excl) if excl.is_finite() else float("nan"),
                        "fee_rate": float(UNIV2_FEE_RATE),
                    }
                )

        if i % 50 == 0:
            print(f"processed {i}/{len(blocks)} blocks")

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Produced 0 rows. Check RPC, block range, and reserve reads.")

    df = df.sort_values(["block_number", "direction", "usd_notional_in"]).reset_index(drop=True)

    out_csv = processed / "univ2_slippage_pre_usd.csv"
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")
    print(df.head(12).to_string(index=False))


if __name__ == "__main__":
    main()
