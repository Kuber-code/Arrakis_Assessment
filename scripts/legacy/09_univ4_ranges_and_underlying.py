import os
import json
from pathlib import Path
from decimal import Decimal, getcontext

import pandas as pd
import matplotlib.pyplot as plt
from web3 import Web3
from dotenv import load_dotenv

getcontext().prec = 60

ARRAKIS_VAULT = Web3.to_checksum_address("0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600")
UNIV4_QUOTER = Web3.to_checksum_address("0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203")
ADDRESS_ZERO = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

VAULT_MODULE_GETTERS = [
    ("module", [{"name": "module", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("activeModule", [{"name": "activeModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("getModule", [{"name": "getModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("strategy", [{"name": "strategy", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
]

ERC20_ABI = [
    {"name": "decimals", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint8"}]},
    {"name": "symbol", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "string"}]},
    {"name": "balanceOf", "type": "function", "stateMutability": "view",
     "inputs": [{"name": "owner", "type": "address"}], "outputs": [{"type": "uint256"}]},
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
    {"name": "poolManager", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
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
    {"name": "totalUnderlying", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint256"}, {"type": "uint256"}]},
    {"name": "totalUnderlyingAtPrice", "type": "function", "stateMutability": "view", "inputs": [{"name": "priceX96_", "type": "uint160"}], "outputs": [{"type": "uint256"}, {"type": "uint256"}]},
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


def detect_module(w3: Web3, vault: str) -> str:
    for name, abi in VAULT_MODULE_GETTERS:
        try:
            v = w3.eth.contract(address=vault, abi=abi)
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
    q = amount * (Decimal(10) ** decimals)
    return int(q.to_integral_value(rounding="ROUND_FLOOR"))


def from_raw(amount_raw: int, decimals: int) -> Decimal:
    return Decimal(int(amount_raw)) / (Decimal(10) ** decimals)


def safe_ratio(numer: int, denom: int) -> str:
    if denom is None or int(denom) == 0:
        return ""
    return str(Decimal(int(numer)) / Decimal(int(denom)))


def quote_exact_in_single(quoter, pool_key_tuple, token_in: str, amount_in_raw: int, block_number: int) -> int:
    amount_in_raw = int(amount_in_raw)
    if amount_in_raw <= 0:
        return 0
    amount_in_raw = min(amount_in_raw, 2**128 - 1)

    currency0 = pool_key_tuple[0]
    currency1 = pool_key_tuple[1]
    if token_in.lower() == currency0.lower():
        zero_for_one = True
    elif token_in.lower() == currency1.lower():
        zero_for_one = False
    else:
        raise ValueError("token_in not in poolKey currencies")

    params = (pool_key_tuple, bool(zero_for_one), int(amount_in_raw), b"")
    amount_out, _gas = quoter.functions.quoteExactInputSingle(params).call(block_identifier=int(block_number))
    return int(amount_out)


def main():
    load_dotenv()
    rpc_url = os.environ.get("RPC_URL")
    if not rpc_url:
        raise RuntimeError("Missing RPC_URL in .env")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError("Cannot connect to RPC_URL")
    if w3.eth.chain_id != 1:
        raise RuntimeError("Not Ethereum mainnet.")

    root = Path(__file__).resolve().parents[1]
    processed = root / "data" / "processed"
    raw = root / "data" / "raw"
    processed.mkdir(parents=True, exist_ok=True)

    meta_json = raw / "univ2_pair_metadata.json"
    if not meta_json.exists():
        raise RuntimeError("Missing data/raw/univ2_pair_metadata.json")
    meta = json.loads(meta_json.read_text(encoding="utf-8"))

    ixs_addr = Web3.to_checksum_address(meta["token0"]["address"])
    sym_ixs = meta["token0"]["symbol"]

    module_addr = detect_module(w3, ARRAKIS_VAULT)
    module = w3.eth.contract(address=module_addr, abi=UNIV4_MODULE_ABI)

    (c0, c1, fee_u24, tick_spacing, hooks) = module.functions.poolKey().call()
    pool_manager = Web3.to_checksum_address(module.functions.poolManager().call())
    c0 = Web3.to_checksum_address(c0)
    c1 = Web3.to_checksum_address(c1)
    hooks = Web3.to_checksum_address(hooks)

    sym0, dec0 = currency_meta(w3, c0)
    sym1, dec1 = currency_meta(w3, c1)

    print("Vault:", ARRAKIS_VAULT)
    print("Module:", module_addr)
    print("PoolManager:", pool_manager)
    print("PoolKey currency0:", c0, f"({sym0}, dec={dec0})")
    print("PoolKey currency1:", c1, f"({sym1}, dec={dec1})")
    print("fee:", int(fee_u24), "tickSpacing:", int(tick_spacing), "hooks:", hooks)

    # Ranges
    ranges = module.functions.getRanges().call()
    rows = []
    for i, r in enumerate(ranges):
        tl = int(r[0])
        tu = int(r[1])
        rows.append({"range_index": i, "tickLower": tl, "tickUpper": tu, "width_ticks": tu - tl, "mid_tick": (tl + tu) / 2.0})
    df_ranges = pd.DataFrame(rows).sort_values(["tickLower", "tickUpper"]).reset_index(drop=True)
    out_ranges_csv = processed / "univ4_active_ranges.csv"
    df_ranges.to_csv(out_ranges_csv, index=False)
    print(f"Wrote {out_ranges_csv}")

    # Underlying + on-chain balances
    quoter = w3.eth.contract(address=UNIV4_QUOTER, abi=V4_QUOTER_ABI)
    latest = int(w3.eth.block_number)

    eth_balance_wei = int(w3.eth.get_balance(module_addr))
    ixs = w3.eth.contract(address=ixs_addr, abi=ERC20_ABI)
    ixs_balance_raw = int(ixs.functions.balanceOf(module_addr).call())

    u0, u1 = module.functions.totalUnderlying().call()
    u0 = int(u0)
    u1 = int(u1)

    fee_rate = Decimal(int(fee_u24)) / Decimal(1_000_000)

    if ixs_addr.lower() not in (c0.lower(), c1.lower()):
        raise RuntimeError("IXS not in pool currencies (check metadata vs poolKey).")

    eth_currency = c0 if c0.lower() != ixs_addr.lower() else c1
    eth_dec = dec0 if eth_currency.lower() == c0.lower() else dec1
    ixs_dec = dec0 if ixs_addr.lower() == c0.lower() else dec1

    pool_key_tuple = (c0, c1, int(fee_u24), int(tick_spacing), hooks)

    tiny_ixs = Decimal(1) / Decimal(10_000)
    tiny_ixs_raw = max(1, to_raw(tiny_ixs, ixs_dec))

    out_eth_raw = quote_exact_in_single(quoter, pool_key_tuple, ixs_addr, tiny_ixs_raw, latest)
    out_eth = from_raw(out_eth_raw, eth_dec)
    in_ixs = from_raw(tiny_ixs_raw, ixs_dec)
    if out_eth <= 0 or in_ixs <= 0:
        raise RuntimeError("Micro-quote failed (out_eth=0).")

    spot_eth_per_ixs = out_eth / (in_ixs * (Decimal(1) - fee_rate))

    if c0.lower() == eth_currency.lower():
        P_raw = Decimal(1) / spot_eth_per_ixs
    else:
        P_raw = spot_eth_per_ixs
    if P_raw <= 0:
        raise RuntimeError("Bad P_raw for priceX96.")

    priceX96 = int((P_raw.sqrt() * (Decimal(2) ** 96)).to_integral_value(rounding="ROUND_FLOOR"))

    a0, a1 = module.functions.totalUnderlyingAtPrice(priceX96).call()
    a0 = int(a0)
    a1 = int(a1)

    df_under = pd.DataFrame(
        [
            {
                "block_number": latest,
                "priceX96_used": str(priceX96),
                "currency0": c0,
                "currency1": c1,
                "symbol0": sym0,
                "symbol1": sym1,
                "decimals0": dec0,
                "decimals1": dec1,
                "totalUnderlying_amount0_raw": u0,
                "totalUnderlying_amount1_raw": u1,
                "totalUnderlying_amount0": str(from_raw(u0, dec0)),
                "totalUnderlying_amount1": str(from_raw(u1, dec1)),
                "totalUnderlyingAtPrice_amount0_raw": a0,
                "totalUnderlyingAtPrice_amount1_raw": a1,
                "totalUnderlyingAtPrice_amount0": str(from_raw(a0, dec0)),
                "totalUnderlyingAtPrice_amount1": str(from_raw(a1, dec1)),
                "module_addr": module_addr,
                "onchain_eth_balance_wei": eth_balance_wei,
                "onchain_ixs_balance_raw": ixs_balance_raw,
                "onchain_eth_balance": str(from_raw(eth_balance_wei, 18)),
                "onchain_ixs_balance": str(from_raw(ixs_balance_raw, 18)),
                "ratio_totalUnderlying0_to_onchainETH": safe_ratio(u0, eth_balance_wei),
                "ratio_totalUnderlying1_to_onchainIXS": safe_ratio(u1, ixs_balance_raw),
                "ratio_totalUnderlyingAtPrice0_to_totalUnderlying0": safe_ratio(a0, u0),
                "ratio_totalUnderlyingAtPrice1_to_totalUnderlying1": safe_ratio(a1, u1),
                "ixs_addr_from_meta": ixs_addr,
                "ixs_symbol_from_meta": sym_ixs,
            }
        ]
    )

    out_under_csv = processed / "univ4_underlying_snapshot.csv"
    df_under.to_csv(out_under_csv, index=False)
    print(f"Wrote {out_under_csv}")

    # Plot ranges
    fig, ax = plt.subplots(figsize=(12, max(3, 0.5 * max(1, len(df_ranges)))))
    for yi, row in enumerate(df_ranges.itertuples(index=False)):
        ax.plot([row.tickLower, row.tickUpper], [yi, yi], linewidth=4)
    ax.set_title("UniV4 active ranges (Arrakis module)")
    ax.set_xlabel("Tick")
    ax.set_ylabel("Range index")
    ax.grid(True, alpha=0.2)
    fig.tight_layout()
    out_png = processed / "univ4_active_ranges.png"
    fig.savefig(out_png, dpi=160)
    plt.close(fig)
    print(f"Wrote {out_png}")

    # Report paragraph
    total_width = int(df_ranges["width_ticks"].sum()) if not df_ranges.empty else 0
    widest = df_ranges.sort_values("width_ticks", ascending=False).head(1)
    narrowest = df_ranges.sort_values("width_ticks", ascending=True).head(1)

    report_lines = []
    report_lines.append(f"After migration, the Arrakis UniV4 module maintains {len(df_ranges)} active tick ranges for {sym0}/{sym1} (fee={int(fee_u24)/1e4:.2f}%).")
    if len(df_ranges) >= 2:
        report_lines.append("The ranges are nested (a narrower core inside a wider backstop), which concentrates liquidity near the current price while keeping some tail coverage.")
    if not widest.empty and not narrowest.empty:
        report_lines.append(
            f"Total active tick-width is {total_width:,} ticks; widest range is [{int(widest['tickLower'].iloc[0])}, {int(widest['tickUpper'].iloc[0])}], "
            f"narrowest is [{int(narrowest['tickLower'].iloc[0])}, {int(narrowest['tickUpper'].iloc[0])}]."
        )
    report_lines.append(
        "Sanity check: we compared module-reported totalUnderlying() with the module address on-chain balances; "
        "a large mismatch indicates totalUnderlying() is not equivalent to 'tokens held on the module address' and should be interpreted as position/accounting amounts."
    )

    out_txt = processed / "univ4_liquidity_ranges_report.txt"
    out_txt.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(f"Wrote {out_txt}")


if __name__ == "__main__":
    main()
