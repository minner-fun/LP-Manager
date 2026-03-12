"""
小时指标聚合器

从原始事件表（swaps / mints / burns / collects）和 pool_price_snapshots
聚合生成 pool_metrics_hourly。

稳定币对（如 USDC/WETH）：自动计算 volume_usd / fee_usd。
非稳定币对：volume_usd / fee_usd 留 NULL。
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db import repository as repo
from src.data_engine.utils import get_stablecoin_side, raw_to_human


def build_hourly_metrics(
    session: Session,
    pool_address: str,
    fee: int,
    symbol0: Optional[str],
    symbol1: Optional[str],
    decimals0: int,
    decimals1: int,
    from_time: datetime,
    to_time: datetime,
    chain_id: int = 1,
) -> int:
    """
    聚合指定时间范围内的小时指标。

    Args:
        fee:       pool fee（pips），500=0.05%，3000=0.3%，10000=1%
        from_time: 包含，建议已对齐到小时整点
        to_time:   不包含

    Returns:
        upsert 的小时记录数。
    """
    fee_rate     = Decimal(fee) / Decimal(1_000_000)
    stable_side  = get_stablecoin_side(symbol0, symbol1)
    stable_decs  = decimals0 if stable_side == 0 else decimals1 if stable_side == 1 else None

    params = {"pool_address": pool_address, "from_time": from_time, "to_time": to_time}

    # ── ① Swap 聚合 ───────────────────────────────────────────────────────────
    swap_rows = {
        row.metric_hour: row
        for row in session.execute(text("""
            SELECT
                DATE_TRUNC('hour', block_timestamp)  AS metric_hour,
                SUM(ABS(amount0_raw))                AS volume_token0_raw,
                SUM(ABS(amount1_raw))                AS volume_token1_raw,
                COUNT(*)                             AS swap_count
            FROM swaps
            WHERE pool_address = :pool_address
              AND block_timestamp >= :from_time
              AND block_timestamp <  :to_time
            GROUP BY DATE_TRUNC('hour', block_timestamp)
        """), params).fetchall()
    }

    # ── ② Mint / Burn / Collect 计数 ─────────────────────────────────────────
    def _count_events(table: str, col: str) -> dict:
        return {
            row.metric_hour: getattr(row, col)
            for row in session.execute(text(f"""
                SELECT DATE_TRUNC('hour', block_timestamp) AS metric_hour,
                       COUNT(*) AS {col}
                FROM {table}
                WHERE pool_address = :pool_address
                  AND block_timestamp >= :from_time
                  AND block_timestamp <  :to_time
                GROUP BY DATE_TRUNC('hour', block_timestamp)
            """), params).fetchall()
        }

    mint_counts    = _count_events("mints",    "mint_count")
    burn_counts    = _count_events("burns",     "burn_count")
    collect_counts = _count_events("collects",  "collect_count")

    # ── ③ OHLC + 流动性（来自 price_snapshots，用 price_token1 作为分析价格）
    #        对于 USDC/WETH 池，price_token1 = ETH 价格（USDC 计），更直观。
    ohlc_rows = {
        row.metric_hour: row
        for row in session.execute(text("""
            WITH ranked AS (
                SELECT
                    DATE_TRUNC('hour', block_timestamp) AS metric_hour,
                    price_token1,
                    liquidity,
                    ROW_NUMBER() OVER (
                        PARTITION BY DATE_TRUNC('hour', block_timestamp)
                        ORDER BY block_number ASC
                    ) AS rn_asc,
                    ROW_NUMBER() OVER (
                        PARTITION BY DATE_TRUNC('hour', block_timestamp)
                        ORDER BY block_number DESC
                    ) AS rn_desc
                FROM pool_price_snapshots
                WHERE pool_address = :pool_address
                  AND block_timestamp >= :from_time
                  AND block_timestamp <  :to_time
            )
            SELECT
                metric_hour,
                MAX(CASE WHEN rn_asc  = 1 THEN price_token1 END) AS price_open,
                MAX(CASE WHEN rn_desc = 1 THEN price_token1 END) AS price_close,
                MAX(price_token1)                                 AS price_high,
                MIN(price_token1)                                 AS price_low,
                AVG(liquidity)                                    AS avg_liquidity,
                MAX(CASE WHEN rn_desc = 1 THEN liquidity END)    AS close_liquidity
            FROM ranked
            GROUP BY metric_hour
            ORDER BY metric_hour
        """), params).fetchall()
    }

    # ── ④ 合并所有小时并构建记录 ─────────────────────────────────────────────
    all_hours = set(swap_rows) | set(ohlc_rows) | set(mint_counts) | \
                set(burn_counts) | set(collect_counts)

    if not all_hours:
        return 0

    for hour in sorted(all_hours):
        sw = swap_rows.get(hour)
        oh = ohlc_rows.get(hour)

        vol_t0_raw = int(sw.volume_token0_raw) if sw else 0
        vol_t1_raw = int(sw.volume_token1_raw) if sw else 0

        # USD volume（仅稳定币对）
        volume_usd = fee_usd = fee_t0_raw = fee_t1_raw = None
        if stable_side is not None and stable_decs is not None:
            stable_raw = vol_t0_raw if stable_side == 0 else vol_t1_raw
            volume_usd = raw_to_human(stable_raw, stable_decs)
            if volume_usd is not None:
                fee_usd    = volume_usd * fee_rate
                fee_t0_raw = int(Decimal(vol_t0_raw) * fee_rate)
                fee_t1_raw = int(Decimal(vol_t1_raw) * fee_rate)

        repo.upsert_hourly_metrics(session, {
            "pool_address":     pool_address,
            "chain_id":         chain_id,
            "metric_hour":      hour,
            "price_open":       oh.price_open    if oh else None,
            "price_close":      oh.price_close   if oh else None,
            "price_high":       oh.price_high    if oh else None,
            "price_low":        oh.price_low     if oh else None,
            "volume_token0_raw": vol_t0_raw,
            "volume_token1_raw": vol_t1_raw,
            "volume_usd":       volume_usd,
            "swap_count":       int(sw.swap_count) if sw else 0,
            "mint_count":       int(mint_counts.get(hour, 0)),
            "burn_count":       int(burn_counts.get(hour, 0)),
            "collect_count":    int(collect_counts.get(hour, 0)),
            "fee_token0_raw":   fee_t0_raw,
            "fee_token1_raw":   fee_t1_raw,
            "fee_usd":          fee_usd,
            "avg_liquidity":    int(oh.avg_liquidity)   if oh and oh.avg_liquidity   else None,
            "close_liquidity":  int(oh.close_liquidity) if oh and oh.close_liquidity else None,
        })

    return len(all_hours)
