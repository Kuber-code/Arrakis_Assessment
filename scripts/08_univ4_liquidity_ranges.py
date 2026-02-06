from pathlib import Path
import os

import pandas as pd
import matplotlib.pyplot as plt
from web3 import Web3
from dotenv import load_dotenv

ARRAKIS_VAULT = Web3.to_checksum_address("0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600")

VAULT_MODULE_GETTERS = [
    ("module", [{"name": "module", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("activeModule", [{"name": "activeModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("getModule", [{"name": "getModule", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
    ("strategy", [{"name": "strategy", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]}]),
]

# From Arrakis docs: getRanges() returns Range[] where Range={int24 tickLower, int24 tickUpper} [page:0]
UNIV4_MODULE_RANGES_ABI = [
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
]


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
    processed.mkdir(parents=True, exist_ok=True)

    module_addr = detect_arrakis_module(w3, ARRAKIS_VAULT)
    module = w3.eth.contract(address=module_addr, abi=UNIV4_MODULE_RANGES_ABI)

    (c0, c1, fee_u24, tick_spacing, hooks) = module.functions.poolKey().call()
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

    df = pd.DataFrame(rows).sort_values(["tickLower", "tickUpper"]).reset_index(drop=True)

    out_csv = processed / "univ4_active_ranges.csv"
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")
    print(df.to_string(index=False))

    # Simple visualization: horizontal segments for each active range on tick axis
    fig, ax = plt.subplots(figsize=(12, max(3, 0.35 * max(1, len(df)))))
    y = range(len(df))
    for yi, (_, row) in zip(y, df.iterrows()):
        ax.plot([row["tickLower"], row["tickUpper"]], [yi, yi], linewidth=3)

    ax.set_title("UniV4 active liquidity ranges (Arrakis module) vs full-range baseline")
    ax.set_xlabel("Tick")
    ax.set_ylabel("Active range index")

    # Full-range baseline (conceptual): just draw a faint line spanning min..max of active ranges
    if not df.empty:
        ax.plot([df["tickLower"].min(), df["tickUpper"].max()], [-1, -1], color="gray", alpha=0.4, linewidth=6)
        ax.text(df["tickLower"].min(), -1.3, "Full-range baseline (min..max active)", color="gray")

    fig.tight_layout()
    out_png = processed / "univ4_active_ranges.png"
    fig.savefig(out_png, dpi=160)
    plt.close(fig)
    print(f"Wrote {out_png}")

    print("\nPoolKey snapshot:")
    print(" currency0:", Web3.to_checksum_address(c0))
    print(" currency1:", Web3.to_checksum_address(c1))
    print(" fee:", int(fee_u24))
    print(" tickSpacing:", int(tick_spacing))
    print(" hooks:", Web3.to_checksum_address(hooks))


if __name__ == "__main__":
    main()
