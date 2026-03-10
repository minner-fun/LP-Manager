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
    UNISWAP_V3_USDC_ETH_POOL_ADDRESS,
    POOLS_ABI
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

FROM_BLOCK = 24625757
TO_BLOCK =   24625772

# Alchemy 免费套餐 eth_getLogs 单次最多查 10 个区块
ALCHEMY_FREE_MAX_RANGE = 10

pools_address = w3.to_checksum_address(UNISWAP_V3_USDC_ETH_POOL_ADDRESS)
abi = json.loads(POOLS_ABI)

pools = w3.eth.contract(address=pools_address, abi=abi)

# 四个事件的 keccak256 topic（需与 ABI 中的签名严格一致）
POOL_MINT_TOPIC    = w3.keccak(text="Mint(address,address,int24,int24,uint128,uint256,uint256)").hex()
POOL_BURN_TOPIC    = w3.keccak(text="Burn(address,int24,int24,uint128,uint256,uint256)").hex()
POOL_COLLECT_TOPIC = w3.keccak(text="Collect(address,address,int24,int24,uint128,uint128)").hex()
POOL_SWAP_TOPIC    = w3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()

# topic -> 对应的 web3 事件类，用于后续按类型解析 raw log
TOPIC_TO_EVENT = {
    POOL_MINT_TOPIC:    pools.events.Mint,
    POOL_BURN_TOPIC:    pools.events.Burn,
    POOL_COLLECT_TOPIC: pools.events.Collect,
    POOL_SWAP_TOPIC:    pools.events.Swap,
}

# 自动按最大区块范围分批拉取，避免触发 Alchemy 免费套餐限制
# topics 用嵌套列表 [[t1, t2, t3, t4]] 表示 topic[0] 为 OR 条件
raw_logs = []
batch_start = FROM_BLOCK
while batch_start <= TO_BLOCK:
    batch_end = min(batch_start + ALCHEMY_FREE_MAX_RANGE - 1, TO_BLOCK)
    print(f"  拉取区块 {batch_start} ~ {batch_end} ...")
    try:
        batch_logs = w3.eth.get_logs({
            "address": pools_address,
            "fromBlock": hex(batch_start),
            "toBlock": hex(batch_end),
            "topics": [[POOL_MINT_TOPIC, POOL_BURN_TOPIC, POOL_COLLECT_TOPIC, POOL_SWAP_TOPIC]],
        })
        raw_logs.extend(batch_logs)
    except Exception as e:
        print(f"获取区块 {batch_start}~{batch_end} 失败: {e}")
        sys.exit(1)
    batch_start = batch_end + 1

# 每条 log 按自身的 topic[0] 匹配对应的事件类型来解析
events = []
for log in raw_logs:
    topic0 = log["topics"][0].hex()
    event_cls = TOPIC_TO_EVENT.get(topic0)
    if event_cls:
        events.append(event_cls().process_log(log))

print(f"总共获取到 {len(events)} 个事件 (Mint/Burn/Collect/Swap)")

for event in events[:5]:
    event_name = event["event"]
    args = event["args"]
    print("-----")
    print("event:", event_name)
    print("blockNumber:", event["blockNumber"])
    print("txHash:", event["transactionHash"].hex())
    if event_name == "Mint":
        print("  sender:", args["sender"])
        print("  owner:", args["owner"])
        print("  tickLower:", args["tickLower"])
        print("  tickUpper:", args["tickUpper"])
        print("  amount:", args["amount"])
        print("  amount0:", args["amount0"])
        print("  amount1:", args["amount1"])
    elif event_name == "Burn":
        print("  owner:", args["owner"])
        print("  tickLower:", args["tickLower"])
        print("  tickUpper:", args["tickUpper"])
        print("  amount:", args["amount"])
        print("  amount0:", args["amount0"])
        print("  amount1:", args["amount1"])
    elif event_name == "Collect":
        print("  owner:", args["owner"])
        print("  recipient:", args["recipient"])
        print("  tickLower:", args["tickLower"])
        print("  tickUpper:", args["tickUpper"])
        print("  amount0:", args["amount0"])
        print("  amount1:", args["amount1"])
    elif event_name == "Swap":
        print("  sender:", args["sender"])
        print("  recipient:", args["recipient"])
        print("  amount0:", args["amount0"])
        print("  amount1:", args["amount1"])
        print("  sqrtPriceX96:", args["sqrtPriceX96"])
        print("  liquidity:", args["liquidity"])
        print("  tick:", args["tick"])