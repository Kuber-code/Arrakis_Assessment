import os
from dotenv import load_dotenv
from web3 import Web3

VAULT = Web3.to_checksum_address("0x90bdE935Ce7FEB6636aFD5A1A0340af45EEAe600")
MODULE = Web3.to_checksum_address("0xC56d93dD1D48f93814901cF685C3cD0eAc0E849D")  # from your runs


def sel(w3: Web3, signature: str) -> str:
    return w3.keccak(text=signature)[:4].hex()


def call_raw(w3: Web3, to: str, data_hex: str):
    try:
        return w3.eth.call({"to": to, "data": data_hex})
    except Exception:
        return None


def as_uint256(ret: bytes):
    if ret is None or len(ret) < 32:
        return None
    return int.from_bytes(ret[-32:], "big")


def as_address(ret: bytes):
    if ret is None or len(ret) < 32:
        return None
    return Web3.to_checksum_address("0x" + ret[-20:].hex())


def probe(w3: Web3, addr: str, signatures):
    print(f"\n=== Probe {addr} ===")
    for sig, kind in signatures:
        data = "0x" + sel(w3, sig)
        ret = call_raw(w3, addr, data)
        if ret is None:
            print(f"{sig:<40} -> revert")
            continue

        if kind == "uint256":
            v = as_uint256(ret)
            print(f"{sig:<40} -> uint256 {v}")
        elif kind == "address":
            a = as_address(ret)
            print(f"{sig:<40} -> address {a}")
        else:
            print(f"{sig:<40} -> ok (len={len(ret)} bytes) {ret[:16].hex()}...")


def main():
    load_dotenv()
    w3 = Web3(Web3.HTTPProvider(os.environ["RPC_URL"]))
    if not w3.is_connected():
        raise RuntimeError("RPC not connected")
    print("latest block:", w3.eth.block_number)

    common = [
        ("name()", "raw"),
        ("symbol()", "raw"),
        ("decimals()", "uint256"),
        ("totalSupply()", "uint256"),
        ("owner()", "address"),
        ("asset()", "address"),                 # ERC4626
        ("totalAssets()", "uint256"),           # ERC4626
        ("module()", "address"),                # Arrakis-style
        ("activeModule()", "address"),
        ("getModule()", "address"),
        ("strategy()", "address"),
        ("poolKey()", "raw"),
        ("poolManager()", "address"),
        ("totalUnderlying()", "raw"),           # expect (uint256,uint256)
        ("totalUnderlyingAtPrice(uint160)", "raw"),
        ("getUnderlyingBalances()", "raw"),
        ("getUnderlyingBalancesAtPrice(uint160)", "raw"),
    ]

    probe(w3, VAULT, common)
    probe(w3, MODULE, common)


if __name__ == "__main__":
    main()
