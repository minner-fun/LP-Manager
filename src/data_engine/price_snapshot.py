"""
价格快照构建器

从 swaps 原始表提取每个区块的价格状态，写入 pool_price_snapshots。

规则：每个区块取 log_index 最大的那笔 Swap 作为该块的「收盘快照」。
这样 hourly OHLC 的 close 价格就对应当小时最后一个区块的最后一笔 Swap。
"""

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db import repository as repo
from src.data_engine.utils import sqrt_price_x96_to_prices


def build_price_snapshots(
    session: Session,
    pool_address: str,
    decimals0: int,
    decimals1: int,
    from_block: int,
    to_block: int,
    chain_id: int = 1,
) -> int:
    """
    为指定 pool 在 [from_block, to_block] 范围内构建价格快照。

    处理逻辑：
      1. 用 DISTINCT ON 从 swaps 中每块取 log_index 最大的一行（SQL 层完成）
      2. 在 Python 中把 sqrtPriceX96 转换为人类可读价格
      3. 批量 upsert 到 pool_price_snapshots（幂等，可重跑）

    Returns:
        写入（新增 + 更新）的快照条数。
    """
    sql = text("""
        SELECT DISTINCT ON (block_number)
            block_number,
            block_timestamp,
            sqrt_price_x96,
            tick,
            liquidity
        FROM swaps
        WHERE pool_address  = :pool_address
          AND block_number >= :from_block
          AND block_number <= :to_block
        ORDER BY block_number, log_index DESC
    """)

    rows = session.execute(sql, {
        "pool_address": pool_address,
        "from_block":   from_block,
        "to_block":     to_block,
    }).fetchall()

    if not rows:
        return 0

    snapshots = []
    for row in rows:
        p0, p1 = sqrt_price_x96_to_prices(
            int(row.sqrt_price_x96), decimals0, decimals1
        )
        snapshots.append({
            "pool_address":    pool_address,
            "chain_id":        chain_id,
            "block_number":    row.block_number,
            "block_timestamp": row.block_timestamp,
            "sqrt_price_x96":  row.sqrt_price_x96,
            "tick":            row.tick,
            "liquidity":       row.liquidity,
            "price_token0":    p0,
            "price_token1":    p1,
        })

    return repo.bulk_upsert_price_snapshots(session, snapshots)
