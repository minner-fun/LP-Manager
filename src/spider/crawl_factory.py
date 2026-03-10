import json
import sys
from pathlib import Path
from web3 import Web3

# 兼容在项目根目录外直接执行脚本时的导入路径
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.Constracts import (
    MAINNET_RPC_URL,
    UNISWAP_V3_FACTORY_ADDRESS,
    UNISWAP_V3_FACTORY_ABI
)

w3 = Web3(Web3.HTTPProvider(MAINNET_RPC_URL))

print("开始检查 RPC 连通性...")
if not w3.is_connected():
    print("RPC 连接失败：无法连接到节点，请检查 MAINNET_RPC_URL 是否正确。")
    sys.exit(1)

try:
    chain_id = w3.eth.chain_id
    latest_block = w3.eth.block_number
    print(f"RPC 连接成功: chain_id={chain_id}, latest_block={latest_block}")
except Exception as e:
    print(f"RPC 连接失败：节点可达但读取链信息失败 -> {e}")
    sys.exit(1)

FROM_BLOCK = 24625333
TO_BLOCK =   24625454

# Alchemy 免费套餐 eth_getLogs 单次最多查 10 个区块
ALCHEMY_FREE_MAX_RANGE = 10

factory_address = w3.to_checksum_address(UNISWAP_V3_FACTORY_ADDRESS)
abi = json.loads(UNISWAP_V3_FACTORY_ABI)

factory = w3.eth.contract(address=factory_address, abi=abi)

# PoolCreated(address,address,uint24,int24,address) 的 keccak256 topic
POOL_CREATED_TOPIC = w3.keccak(
    text="PoolCreated(address,address,uint24,int24,address)"
).hex()

# 自动按最大区块范围分批拉取，避免触发 Alchemy 免费套餐限制
raw_logs = []
batch_start = FROM_BLOCK
while batch_start <= TO_BLOCK:
    batch_end = min(batch_start + ALCHEMY_FREE_MAX_RANGE - 1, TO_BLOCK)
    print(f"  拉取区块 {batch_start} ~ {batch_end} ...")
    try:
        batch_logs = w3.eth.get_logs({
            "address": factory_address,
            "fromBlock": hex(batch_start),
            "toBlock": hex(batch_end),
            "topics": [POOL_CREATED_TOPIC],
        })
        raw_logs.extend(batch_logs)
    except Exception as e:
        print(f"获取区块 {batch_start}~{batch_end} 失败: {e}")
        sys.exit(1)
    batch_start = batch_end + 1

# 用 ABI 解析 raw log 数据
events = [factory.events.PoolCreated().process_log(log) for log in raw_logs]

print(f"总共获取到 {len(events)} 个 PoolCreated 事件")

for event in events[:5]:
    print("-----")
    print("token0:", event["args"]["token0"])
    print("token1:", event["args"]["token1"])
    print("fee:", event["args"]["fee"])
    print("tickSpacing:", event["args"]["tickSpacing"])
    print("pool:", event["args"]["pool"])
    print("blockNumber:", event["blockNumber"])
    print("txHash:", event["transactionHash"].hex())