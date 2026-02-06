
import os
from web3 import Web3
from dotenv import load_dotenv

load_dotenv()
w3 = Web3(Web3.HTTPProvider(os.environ["RPC_URL"]))

POOL_MANAGER = Web3.to_checksum_address("0x000000000004444c5dc75cB358380D2e3dE08A90")
topic0 = w3.keccak(text="Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)").hex()

latest = w3.eth.block_number
frm = 24_000_000

logs = w3.eth.get_logs({"fromBlock": frm, "toBlock": latest, "address": POOL_MANAGER, "topics": [topic0]})
print("logs:", len(logs), "range:", frm, latest)

def word(data_hex, i): return data_hex[2+i*64:2+(i+1)*64]
def int256(x):
    v = int(x, 16)
    return v - 2**256 if v >= 2**255 else v
def addr_from_topic(t):
    t = t.hex() if hasattr(t, "hex") else t
    return Web3.to_checksum_address("0x" + t[-40:])
def addr_from_word(w): return Web3.to_checksum_address("0x" + w[-40:])

for log in logs:
    topics = [t.hex() if hasattr(t,"hex") else t for t in log["topics"]]
    data = log["data"].hex() if hasattr(log["data"], "hex") else log["data"]

    poolId = topics[1]
    c0 = addr_from_topic(topics[2])
    c1 = addr_from_topic(topics[3])
    fee = int(word(data, 0), 16)
    tickSpacing = int256(word(data, 1))
    hooks = addr_from_word(word(data, 2))

    print("block:", log["blockNumber"])
    print("poolId(topic):", poolId)
    print("currency0:", c0)
    print("currency1:", c1)
    print("fee:", fee, "tickSpacing:", tickSpacing, "hooks:", hooks)

