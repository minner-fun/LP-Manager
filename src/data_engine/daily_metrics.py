"""
日指标聚合器

从 pool_metrics_hourly 聚合生成 pool_metrics_daily，并额外计算：
  - volatility_1d：当日 24 根小时 K 线对数收益率标准差
  - tvl_estimate_usd：mint/burn 累计净值（稳定币侧，近似值）
  - volume_tvl_ratio：volume / TVL
  - fee_apr：日手续费 / TVL × 365
  - il_estimate_fullrange_1d：全范围 V2 假设的无常损失（昨收→今收）
"""

import math
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db import repository as repo
from src.data_engine.utils import (
    calc_il_fullrange,
    calc_log_return_volatility,
    get_stablecoin_side,
    raw_to_human,
)


def build_daily_metrics(
    session: Session,
    pool_address: str,
    fee: int,
    symbol0: Optional[str],
    symbol1: Optional[str],
    decimals0: int,
    decimals1: int,
    from_date: date,
    to_date: date,
    chain_id: int = 1,
) -> int:
    """
    聚合指定日期范围（含两端）的日指标。

    依赖：pool_metrics_hourly 在对应时间段已构建完毕。

    Returns:
        upsert 的日记录数。
    """
    stable_side = get_stablecoin_side(symbol0, symbol1)
    stable_decs = decimals0 if stable_side == 0 else decimals1 if stable_side == 1 else None

    # ── ① 从 hourly 聚合基础 OHLC + volume + counts ───────────────────────────
    agg_sql = text("""
        WITH ranked AS (
            SELECT
                DATE(metric_hour)  AS metric_date,
                price_open,
                price_close,
                price_high,
                price_low,
                volume_token0_raw,
                volume_token1_raw,
                volume_usd,
                fee_usd,
                swap_count,
                mint_count,
                burn_count,
                collect_count,
                close_liquidity,
                ROW_NUMBER() OVER (
                    PARTITION BY DATE(metric_hour) ORDER BY metric_hour ASC
                ) AS rn_asc,
                ROW_NUMBER() OVER (
                    PARTITION BY DATE(metric_hour) ORDER BY metric_hour DESC
                ) AS rn_desc
            FROM pool_metrics_hourly
            WHERE pool_address     = :pool_address
              AND metric_hour::DATE >= :from_date
              AND metric_hour::DATE <= :to_date
        )
        SELECT
            metric_date,
            MAX(CASE WHEN rn_asc  = 1 THEN price_open   END) AS price_open,
            MAX(CASE WHEN rn_desc = 1 THEN price_close  END) AS price_close,
            MAX(price_high)                                   AS price_high,
            MIN(price_low)                                    AS price_low,
            SUM(volume_token0_raw)                            AS volume_token0_raw,
            SUM(volume_token1_raw)                            AS volume_token1_raw,
            SUM(volume_usd)                                   AS volume_usd,
            SUM(fee_usd)                                      AS fee_usd,
            SUM(swap_count)                                   AS swap_count,
            SUM(mint_count)                                   AS mint_count,
            SUM(burn_count)                                   AS burn_count,
            SUM(collect_count)                                AS collect_count,
            MAX(CASE WHEN rn_desc = 1 THEN close_liquidity END) AS close_liquidity
        FROM ranked
        GROUP BY metric_date
        ORDER BY metric_date
    """)

    agg_rows = session.execute(agg_sql, {
        "pool_address": pool_address,
        "from_date":    from_date,
        "to_date":      to_date,
    }).fetchall()

    if not agg_rows:
        return 0

    # ── ② TVL 估算：从所有历史 mint/burn 累计（仅稳定币侧） ───────────────────
    # 策略：pre-fetch 每日增量，Python 累加 → 避免每天单独查一次 DB
    tvl_by_date: dict[date, Optional[Decimal]] = {}
    if stable_side is not None and stable_decs is not None:
        col = "amount0_raw" if stable_side == 0 else "amount1_raw"

        # 截至 from_date 前一天的累计基线（一次查询）
        baseline_sql = text(f"""
            SELECT
                COALESCE((SELECT SUM({col}) FROM mints
                          WHERE pool_address = :pa AND block_timestamp::DATE < :d), 0)
              - COALESCE((SELECT SUM({col}) FROM burns
                          WHERE pool_address = :pa AND block_timestamp::DATE < :d), 0)
                AS baseline
        """)
        baseline = int(session.execute(
            baseline_sql, {"pa": pool_address, "d": from_date}
        ).scalar())

        # 每日 mint/burn 增量
        delta_sql = text(f"""
            SELECT d, COALESCE(mint_{col}, 0) - COALESCE(burn_{col}, 0) AS delta
            FROM (
                SELECT block_timestamp::DATE AS d,
                       SUM({col}) AS mint_{col}
                FROM mints
                WHERE pool_address = :pa
                  AND block_timestamp::DATE >= :from_date
                  AND block_timestamp::DATE <= :to_date
                GROUP BY block_timestamp::DATE
            ) m
            FULL OUTER JOIN (
                SELECT block_timestamp::DATE AS d,
                       SUM({col}) AS burn_{col}
                FROM burns
                WHERE pool_address = :pa
                  AND block_timestamp::DATE >= :from_date
                  AND block_timestamp::DATE <= :to_date
                GROUP BY block_timestamp::DATE
            ) b USING (d)
            ORDER BY d
        """)
        delta_rows = {
            row.d: int(row.delta)
            for row in session.execute(
                delta_sql, {"pa": pool_address, "from_date": from_date, "to_date": to_date}
            ).fetchall()
        }

        cumulative = baseline
        cur = from_date
        while cur <= to_date:
            cumulative += delta_rows.get(cur, 0)
            tvl_by_date[cur] = (
                raw_to_human(cumulative, stable_decs) if cumulative > 0 else None
            )
            cur += timedelta(days=1)

    # ── ③ 小时收盘价序列（用于波动率和 IL） ──────────────────────────────────
    # 多取 from_date 前一天的收盘价作为 IL 计算的基准
    prices_sql = text("""
        SELECT metric_hour, price_close
        FROM pool_metrics_hourly
        WHERE pool_address = :pool_address
          AND metric_hour >= :start
          AND metric_hour <  :end
          AND price_close IS NOT NULL
        ORDER BY metric_hour ASC
    """)
    prices_start = datetime.combine(from_date - timedelta(days=1), datetime.min.time())
    prices_end   = datetime.combine(to_date   + timedelta(days=1), datetime.min.time())
    price_rows   = session.execute(prices_sql, {
        "pool_address": pool_address,
        "start": prices_start,
        "end":   prices_end,
    }).fetchall()

    # 按日分组存 hourly 收盘价
    hourly_prices_by_date: dict[date, list[float]] = {}
    for r in price_rows:
        d = r.metric_hour.date()
        hourly_prices_by_date.setdefault(d, []).append(float(r.price_close))

    # 前一日收盘（用于 IL 计算）
    prev_close_by_date: dict[date, Optional[float]] = {}
    prev_day = from_date - timedelta(days=1)
    prev_prices = hourly_prices_by_date.get(prev_day, [])
    prev_close_by_date[from_date] = prev_prices[-1] if prev_prices else None

    for i, row in enumerate(agg_rows):
        d = row.metric_date if isinstance(row.metric_date, date) else row.metric_date.date()
        if i > 0:
            prev_row = agg_rows[i - 1]
            prev_close_by_date[d] = (
                float(prev_row.price_close) if prev_row.price_close else None
            )

    # ── ④ 构建并 upsert 每日记录 ──────────────────────────────────────────────
    for row in agg_rows:
        d = row.metric_date if isinstance(row.metric_date, date) else row.metric_date.date()

        tvl_usd = tvl_by_date.get(d)

        # volume / tvl
        volume_tvl_ratio = None
        if row.volume_usd and tvl_usd and tvl_usd > 0:
            volume_tvl_ratio = float(row.volume_usd) / float(tvl_usd)

        # fee APR = daily_fee / tvl × 365
        fee_apr = None
        if row.fee_usd and tvl_usd and tvl_usd > 0:
            fee_apr = float(row.fee_usd) / float(tvl_usd) * 365

        # 波动率（当日 24 根小时 K 线）
        today_prices = hourly_prices_by_date.get(d, [])
        volatility_1d = calc_log_return_volatility(today_prices)

        # IL（全范围，昨收 → 今收）
        il_estimate = None
        today_close = float(row.price_close) if row.price_close else None
        prev_close  = prev_close_by_date.get(d)
        if today_close and prev_close and prev_close > 0:
            il_estimate = calc_il_fullrange(today_close / prev_close)

        repo.upsert_daily_metrics(session, {
            "pool_address":             pool_address,
            "chain_id":                 chain_id,
            "metric_date":              d,
            "price_open":               row.price_open,
            "price_close":              row.price_close,
            "price_high":               row.price_high,
            "price_low":                row.price_low,
            "volume_token0_raw":        int(row.volume_token0_raw or 0),
            "volume_token1_raw":        int(row.volume_token1_raw or 0),
            "volume_usd":               row.volume_usd,
            "fee_usd":                  row.fee_usd,
            "tvl_estimate_usd":         tvl_usd,
            "swap_count":               int(row.swap_count or 0),
            "mint_count":               int(row.mint_count or 0),
            "burn_count":               int(row.burn_count or 0),
            "collect_count":            int(row.collect_count or 0),
            "volatility_1d":            volatility_1d,
            "volume_tvl_ratio":         volume_tvl_ratio,
            "fee_apr":                  fee_apr,
            "il_estimate_fullrange_1d": il_estimate,
        })

    return len(agg_rows)
