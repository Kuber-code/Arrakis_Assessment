import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

# Mainnet deployment addresses (Uniswap docs)
UNIV3_FACTORY = Web3.to_checksum_address("0x1F98431c8aD98523631AE4a59f267346ea31F984")  # [web:292]
WETH = Web3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
USDC = Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
FEE = 500  # 0.05%

# Minimal ABIs (robust decoding; no manual hex slicing)
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
]  # signature per Uniswap V3 factory interface [web:292]

POOL_ABI = [
    {
        "name": "token0",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "address"}],
    },
    {
        "name": "token1",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "address"}],
    },
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
]  # slot0 interface per IUniswapV3PoolState [web:303]


def eth_usd_from_sqrtprice_x96(sqrt_price_x96: int, token0: str, token1: str) -> float:
    """
    Return USDC per WETH (~USD/ETH) using Uniswap V3 slot0.sqrtPriceX96 (Q64.96).

    p_raw = token1_raw / token0_raw = (sqrtPriceX96^2) / 2^192  [web:277][web:278]
    """
    if not sqrt_price_x96 or sqrt_price_x96 <= 0:
        return float("nan")

    p_raw = (sqrt_price_x96 * sqrt_price_x96) / (2 ** 192)

    # Handle both token orderings, but for this pool we expect token0=USDC, token1=WETH.
    if token0.lower() == USDC.lower() and token1.lower() == WETH.lower():
        # p_raw = WETH_raw / USDC_raw
        # USDC/WETH = (1/p_raw) * 10^(decWETH-decUSDC) = (1/p_raw) * 1e12
        return float((1.0 / p_raw) * (10 ** 12)) if p_raw > 0 else float("nan")

    if token0.lower() == WETH.lower() and token1.lower() == USDC.lower():
        # p_raw = USDC_raw / WETH_raw
        # USDC/WETH = p_raw * 10^(decWETH-decUSDC) = p_raw * 1e12
        return float(p_raw * (10 ** 12))

    return float("nan")


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
    processed.mkdir(parents=True, exist_ok=True)

    binned_path = processed / "univ2_reserve1_binned_30m.csv"
    if not binned_path.exists():
        raise RuntimeError("Missing data/processed/univ2_reserve1_binned_30m.csv. Run the binning step first.")

    bins = pd.read_csv(binned_path)
    if "block_median" not in bins.columns:
        raise RuntimeError("Expected column 'block_median' in univ2_reserve1_binned_30m.csv")

    blocks = bins["block_median"].round().astype(int)
    blocks = sorted(set(int(b) for b in blocks.tolist() if int(b) > 0))
    if not blocks:
        raise RuntimeError("No valid block numbers in 'block_median'")

    factory = w3.eth.contract(address=UNIV3_FACTORY, abi=FACTORY_ABI)
    pool_addr = factory.functions.getPool(WETH, USDC, FEE).call()
    pool_addr = Web3.to_checksum_address(pool_addr)

    if int(pool_addr, 16) == 0:
        raise RuntimeError("Factory.getPool returned 0x0 (pool not found). Check fee/tokens.")

    pool = w3.eth.contract(address=pool_addr, abi=POOL_ABI)
    token0 = Web3.to_checksum_address(pool.functions.token0().call())
    token1 = Web3.to_checksum_address(pool.functions.token1().call())

    print("Uniswap V3 Factory:", UNIV3_FACTORY)
    print("Requested pool tokens:", WETH, USDC, "fee=", FEE)
    print("Resolved pool address:", pool_addr)
    print("Pool token0:", token0)
    print("Pool token1:", token1)
    print("Query points (30-min bins):", len(blocks))

    rows = []
    for i, b in enumerate(blocks, start=1):
        slot0 = pool.functions.slot0().call(block_identifier=int(b))
        sqrtp = int(slot0[0])

        eth_usd = eth_usd_from_sqrtprice_x96(sqrtp, token0, token1)
        rows.append(
            {"block_number": int(b), "sqrtPriceX96": int(sqrtp), "eth_usd": float(eth_usd)}
        )

        if i % 200 == 0:
            print(f"slot0 fetched for {i}/{len(blocks)} blocks")

    df = pd.DataFrame(rows).sort_values("block_number").reset_index(drop=True)
    out_csv = processed / "eth_usd_univ3_slot0.csv"
    df.to_csv(out_csv, index=False)

    print(f"Wrote {out_csv}")
    print(df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
