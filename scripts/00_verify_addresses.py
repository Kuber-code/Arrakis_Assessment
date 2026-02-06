import os
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
from web3 import Web3


ADDRESSES = {
    "univ2_pair": "0xC09bf2B1Bc8725903C509e8CAeef9190857215A8",
    "arrakis_vault": "0x90bde935ce7feb6636afd5a1a0340af45eeae600",
    "univ4_quoter": "0x52F0E24D1c21C8A0cB1e5a5dD6198556BD9E1203",
}


def redact_rpc_url(rpc_url: str) -> str:
    """
    Redact common API-key style RPC URLs (Alchemy/Infura/etc).
    Goal: keep provider host, remove secrets.
    """
    try:
        p = urlparse(rpc_url)
        base = f"{p.scheme}://{p.netloc}"
        path = p.path or ""
        # Common patterns: /v2/<key>, /v3/<key>
        parts = [x for x in path.split("/") if x]
        if len(parts) >= 2 and parts[0] in {"v2", "v3"}:
            return f"{base}/{parts[0]}/<redacted>"
        # Otherwise keep only the host
        return base
    except Exception:
        return "<redacted>"


def is_contract(w3: Web3, addr: str) -> tuple[str, bool, int]:
    checksum = Web3.to_checksum_address(addr)
    code = w3.eth.get_code(checksum)
    code_hex_len = len(code.hex())  # includes "0x"
    return checksum, (code_hex_len > 2), code_hex_len


def main() -> None:
    load_dotenv()
    rpc_url = os.environ.get("RPC_URL")
    if not rpc_url:
        raise RuntimeError("Missing RPC_URL in .env")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError("Cannot connect to RPC_URL")

    chain_id = w3.eth.chain_id
    latest_block = w3.eth.block_number

    report = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "rpc_url_redacted": redact_rpc_url(rpc_url),
        "chain_id": chain_id,
        "latest_block": latest_block,
        "contracts": {},
    }

    for name, addr in ADDRESSES.items():
        checksum, ok, code_len = is_contract(w3, addr)
        report["contracts"][name] = {
            "address": checksum,
            "is_contract": ok,
            "bytecode_hex_len": code_len,
        }

    root = Path(__file__).resolve().parents[1]
    out_path = root / "data" / "raw" / "address_verification.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))

    if chain_id != 1:
        raise RuntimeError(f"Not mainnet: chain_id={chain_id}")

    for name, meta in report["contracts"].items():
        if not meta["is_contract"]:
            raise RuntimeError(f"{name} is not a contract on this chain: {meta['address']}")


if __name__ == "__main__":
    main()
