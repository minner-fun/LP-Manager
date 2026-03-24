import json
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

from web3 import Web3

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.Constracts import (
    MAINNET_RPC_URL,
    POOLS_ABI,
    UNISWAP_V3_WBTC_USDC_POOL_ADDRESS,
)
from src.db import repository as repo
from src.db.database import get_session, init_db

# ── 监听配置 ──────────────────────────────────────────────────────────────────
CHAIN_ID         = 1
POLL_INTERVAL    = 12       # 每次轮询间隔（秒），接近以太坊出块时间
CONFIRM_BLOCKS   = 2        # 等待确认块数，避免因链重组导致数据回滚
MAX_FETCH_RANGE  = 10       # 单次 eth_getLogs 最大查询区块数（Alchemy 免费限制）
RETRY_MAX        = 5
RETRY_BASE_DELAY = 2.0

# ── RPC 连接 ──────────────────────────────────────────────────────────────────
w3 = Web3(Web3.HTTPProvider(MAINNET_RPC_URL))

print("检查 RPC 连通性...")
if not w3.is_connected():
    print("RPC 连接失败，请检查 MAINNET_RPC_URL。")
    sys.exit(1)

try:
    print(f"RPC 连接成功: chain_id={w3.eth.chain_id}, latest_block={w3.eth.block_number}")
except Exception as e:
    print(f"RPC 读取链信息失败 -> {e}")
    sys.exit(1)

# ── 合约对象 & 事件 topic ─────────────────────────────────────────────────────
pools_address  = w3.to_checksum_address(UNISWAP_V3_WBTC_USDC_POOL_ADDRESS)
pool_contract  = w3.eth.contract(address=pools_address, abi=json.loads(POOLS_ABI))

POOL_MINT_TOPIC    = w3.keccak(text="Mint(address,address,int24,int24,uint128,uint256,uint256)").hex()
POOL_BURN_TOPIC    = w3.keccak(text="Burn(address,int24,int24,uint128,uint256,uint256)").hex()
POOL_COLLECT_TOPIC = w3.keccak(text="Collect(address,address,int24,int24,uint128,uint128)").hex()
POOL_SWAP_TOPIC    = w3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()

TOPIC_TO_EVENT = {
    POOL_MINT_TOPIC:    pool_contract.events.Mint,
    POOL_BURN_TOPIC:    pool_contract.events.Burn,
    POOL_COLLECT_TOPIC: pool_contract.events.Collect,
    POOL_SWAP_TOPIC:    pool_contract.events.Swap,
}
ALL_TOPICS = [POOL_MINT_TOPIC, POOL_BURN_TOPIC, POOL_COLLECT_TOPIC, POOL_SWAP_TOPIC]

# ── 优雅退出标志 ──────────────────────────────────────────────────────────────
_shutdown = False

def _handle_signal(signum, frame):
    global _shutdown
    print("\n收到退出信号，完成当前轮询后退出...")
    _shutdown = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _get_logs_with_retry(params: dict) -> list:
    """带指数退避重试的 eth_getLogs，429 时等待后重试。"""
    delay = RETRY_BASE_DELAY
    for attempt in range(1, RETRY_MAX + 1):
        try:
            return w3.eth.get_logs(params)
        except Exception as e:
            if "429" in str(e):
                if attempt == RETRY_MAX:
                    print(f"    已重试 {RETRY_MAX} 次仍触发限流，放弃。")
                    raise
                print(f"    触发限流 (429)，{delay:.1f}s 后第 {attempt} 次重试...")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    return []


def _fetch_range_logs(from_block: int, to_block: int) -> list:
    """
    在 from_block ~ to_block 范围内，按 MAX_FETCH_RANGE 分批拉取四种事件的原始日志。
    """
    raw_logs    = []
    batch_start = from_block
    while batch_start <= to_block:
        batch_end = min(batch_start + MAX_FETCH_RANGE - 1, to_block)
        logs = _get_logs_with_retry({
            "address":   pools_address,
            "fromBlock": hex(batch_start),
            "toBlock":   hex(batch_end),
            "topics":    [ALL_TOPICS],
        })
        raw_logs.extend(logs)
        batch_start = batch_end + 1
        if batch_start <= to_block:
            time.sleep(0.05)
    return raw_logs


def _parse_logs(raw_logs: list) -> list:
    """按 topic0 路由，把 raw log 解析为结构化事件列表。"""
    events = []
    for log in raw_logs:
        topic0    = log["topics"][0].hex()
        event_cls = TOPIC_TO_EVENT.get(topic0)
        if event_cls:
            events.append(event_cls().process_log(log))
    return events


def _rpc_fetch_timestamp(block_number: int) -> datetime:
    """从链上获取单个区块时间戳。"""
    block = w3.eth.get_block(block_number)
    return datetime.utcfromtimestamp(block["timestamp"])


def _build_common_fields(event, ts_map: dict) -> dict:
    """提取四张事件表的公共字段。"""
    return {
        "chain_id":        CHAIN_ID,
        "pool_address":    pools_address,
        "block_number":    event["blockNumber"],
        "block_timestamp": ts_map[event["blockNumber"]],
        "tx_hash":         event["transactionHash"].hex(),
        "log_index":       event["logIndex"],
    }


def _save_events(session, events: list, ts_map: dict, last_block: int) -> dict:
    """
    在同一个 session / 事务里将解析好的事件写入对应表，并更新 sync_cursor。
    写入失败整体回滚，sync_cursor 不前进，下次轮询会重试这批区块。
    """
    counts = {"Mint": 0, "Burn": 0, "Collect": 0, "Swap": 0}

    for event in events:
        name   = event["event"]
        args   = event["args"]
        common = _build_common_fields(event, ts_map)

        if name == "Mint":
            ok = repo.insert_mint(session, {
                **common,
                "sender":           w3.to_checksum_address(args["sender"]),
                "owner":            w3.to_checksum_address(args["owner"]),
                "tick_lower":       args["tickLower"],
                "tick_upper":       args["tickUpper"],
                "amount_liquidity": args["amount"],
                "amount0_raw":      args["amount0"],
                "amount1_raw":      args["amount1"],
            })
        elif name == "Burn":
            ok = repo.insert_burn(session, {
                **common,
                "owner":            w3.to_checksum_address(args["owner"]),
                "tick_lower":       args["tickLower"],
                "tick_upper":       args["tickUpper"],
                "amount_liquidity": args["amount"],
                "amount0_raw":      args["amount0"],
                "amount1_raw":      args["amount1"],
            })
        elif name == "Collect":
            ok = repo.insert_collect(session, {
                **common,
                "owner":       w3.to_checksum_address(args["owner"]),
                "recipient":   w3.to_checksum_address(args["recipient"]),
                "tick_lower":  args["tickLower"],
                "tick_upper":  args["tickUpper"],
                "amount0_raw": args["amount0"],
                "amount1_raw": args["amount1"],
            })
        elif name == "Swap":
            ok = repo.insert_swap(session, {
                **common,
                "sender":         w3.to_checksum_address(args["sender"]),
                "recipient":      w3.to_checksum_address(args["recipient"]),
                "amount0_raw":    args["amount0"],
                "amount1_raw":    args["amount1"],
                "sqrt_price_x96": args["sqrtPriceX96"],
                "liquidity":      args["liquidity"],
                "tick":           args["tick"],
            })
        else:
            continue

        if ok:
            counts[name] += 1

    repo.update_sync_cursor(
        session,
        chain_id          = CHAIN_ID,
        target_type       = "pool_live",
        target_address    = pools_address,
        last_synced_block = last_block,
    )
    return counts


# ── 初始化数据库 & 确定起始区块 ───────────────────────────────────────────────
init_db()
print("数据库表已就绪")

with get_session() as _s:
    last_synced = repo.get_sync_cursor(_s, CHAIN_ID, "pool_live", pools_address)

current_latest = w3.eth.block_number

if last_synced is not None:
    # 从上次监听结束处继续，最多回溯 100 个区块防止遗漏
    next_block = last_synced + 1
    print(f"检测到历史进度：已同步至区块 {last_synced}，从 {next_block} 继续监听\n")
else:
    # 全新启动：从当前最新已确认块开始，不回溯历史
    next_block = max(current_latest - CONFIRM_BLOCKS, 0)
    print(f"全新启动，从当前区块 {next_block} 开始实时监听\n")

print(f"监听目标  : {pools_address}")
print(f"轮询间隔  : {POLL_INTERVAL}s")
print(f"确认块数  : {CONFIRM_BLOCKS} 个区块")
print(f"按 Ctrl+C 可优雅退出\n")
print("─" * 60)


# ── 主监听循环 ────────────────────────────────────────────────────────────────
session_counts = {"Mint": 0, "Burn": 0, "Collect": 0, "Swap": 0}

while not _shutdown:
    try:
        latest_block = w3.eth.block_number
        # 只处理已获得足够确认的区块
        confirmed_head = latest_block - CONFIRM_BLOCKS

        if confirmed_head < next_block:
            # 还没有新的已确认区块，等待下一个轮询周期
            time.sleep(POLL_INTERVAL)
            continue

        from_block = next_block
        to_block   = confirmed_head

        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"[{now_str}] 扫描区块 {from_block:,} ~ {to_block:,} "
              f"(+{to_block - from_block + 1} 块)", end="  ", flush=True)

        # ① 拉取原始日志
        raw_logs = _fetch_range_logs(from_block, to_block)
        events   = _parse_logs(raw_logs)
        print(f"事件 {len(events):3d} 条", end="  ", flush=True)

        if events:
            with get_session() as session:
                unique_blocks = {e["blockNumber"] for e in events}

                # ② 获取区块时间戳（DB 优先，缺失走 RPC）
                ts_map = repo.get_or_fetch_block_timestamps(
                    session,
                    chain_id      = CHAIN_ID,
                    block_numbers = unique_blocks,
                    rpc_fetcher   = _rpc_fetch_timestamp,
                )

                # ③ 写入事件 + 更新 sync_cursor（同一事务）
                counts = _save_events(session, events, ts_map, to_block)

            for k, v in counts.items():
                session_counts[k] += v

            print(f"Mint {counts['Mint']} Burn {counts['Burn']} "
                  f"Collect {counts['Collect']} Swap {counts['Swap']}")
        else:
            # 无事件也推进 sync_cursor，避免下次重扫空块
            with get_session() as session:
                repo.update_sync_cursor(
                    session,
                    chain_id          = CHAIN_ID,
                    target_type       = "pool_live",
                    target_address    = pools_address,
                    last_synced_block = to_block,
                )
            print("(无事件)")

        next_block = to_block + 1

    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"\n  轮询异常: {e}，{POLL_INTERVAL}s 后重试...")
        time.sleep(POLL_INTERVAL)
        continue

    if not _shutdown:
        time.sleep(POLL_INTERVAL)


# ── 退出汇总 ──────────────────────────────────────────────────────────────────
print("\n" + "─" * 60)
print("监听已停止，本次会话累计写入：")
for event_type, count in session_counts.items():
    print(f"  {event_type:8s}: {count:,} 条")
print(f"最终进度已记录至区块 {next_block - 1:,}")
