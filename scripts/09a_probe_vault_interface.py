import os
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from web3 import Web3

VAULT = Web3.to_checksum_address("0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600")


def ensure_dirs(root: Path):
    (root / "data" / "raw").mkdir(parents=True, exist_ok=True)
    (root / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (root / "figures").mkdir(parents=True, exist_ok=True)


def sel(w3: Web3, signature: str) -> str:
    return "0x" + w3.keccak(text=signature)[:4].hex()


def call_raw(w3: Web3, to: str, data_hex: str, block_identifier: Optional[int] = None) -> Optional[bytes]:
    try:
        call_obj = {"to": Web3.to_checksum_address(to), "data": data_hex}
        if block_identifier is None:
            return w3.eth.call(call_obj)
        return w3.eth.call(call_obj, block_identifier=block_identifier)
    except Exception:
        return None


def decode_uint256(ret: Optional[bytes]) -> Optional[int]:
    if ret is None or len(ret) < 32:
        return None
    return int.from_bytes(ret[-32:], "big")


def decode_address(ret: Optional[bytes]) -> Optional[str]:
    if ret is None or len(ret) < 32:
        return None
    return Web3.to_checksum_address("0x" + ret[-20:].hex())


def probe(w3: Web3, addr: str, signatures: List[Tuple[str, str]]) -> List[Dict[str, Any]]:
    out = []
    print(f"\n=== Probe {addr} ===")
    for sig, kind in signatures:
        data = sel(w3, sig)
        ret = call_raw(w3, addr, data)
        if ret is None:
            print(f"{sig:<42} -> revert")
            out.append({"signature": sig, "kind": kind, "status": "revert"})
            continue

        if kind == "uint256":
            v = decode_uint256(ret)
            print(f"{sig:<42} -> uint256 {v}")
            out.append({"signature": sig, "kind": kind, "status": "ok", "value": v})
        elif kind == "address":
            a = decode_address(ret)
            print(f"{sig:<42} -> address {a}")
            out.append({"signature": sig, "kind": kind, "status": "ok", "value": a})
        else:
            # raw bytes
            print(f"{sig:<42} -> ok (len={len(ret)} bytes) {ret[:16].hex()}...")
            out.append({"signature": sig, "kind": kind, "status": "ok", "value_hex_prefix": ret[:64].hex()})
    return out


def main():
    load_dotenv()
    rpc_url = os.environ.get("RPC_URL")
    if not rpc_url:
        raise RuntimeError("Missing RPC_URL in environment or .env")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError("RPC not connected")

    root = Path(__file__).resolve().parents[1]
    ensure_dirs(root)
    raw_dir = root / "data" / "raw"

    latest = int(w3.eth.block_number)
    print("latest block:", latest)

    common = [
        ("name()", "raw"),
        ("symbol()", "raw"),
        ("decimals()", "uint256"),
        ("totalSupply()", "uint256"),
        ("owner()", "address"),
        ("asset()", "address"),  # ERC4626
        ("totalAssets()", "uint256"),  # ERC4626
        ("module()", "address"),  # Arrakis-style
        ("activeModule()", "address"),
        ("getModule()", "address"),
        ("strategy()", "address"),
        ("poolKey()", "raw"),
        ("poolManager()", "address"),
        ("totalUnderlying()", "raw"),  # expect (uint256,uint256)
        ("getRanges()", "raw"),
    ]

    vault_res = probe(w3, VAULT, common)

    out = {
        "block_number": latest,
        "vault": VAULT,
        "results": {
            "vault": vault_res,
        },
    }

    out_path = raw_dir / "vault_interface_probe.json"
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
