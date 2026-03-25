"""
StrategyRunner
==============

将策略评估、链上执行、状态持久化串联成一个完整闭环。

run_once() 的执行顺序：
    1. 从 DB 构建 MarketContext（价格快照 + 近期日指标）
    2. 从 DB 读取当前 OPEN 仓位（ActivePosition | None）
    3. strategy.evaluate(ctx, position) → Decision
    4. 根据 Decision 调用 execution_engine 执行
    5. 更新 lp_positions / lp_position_actions（同一 session）
    6. 写入 strategy_signals（无论执行成功与否均记录）

run_loop(interval_secs) : 定时重复调用 run_once，适合生产部署。

用法示例::

    from src.execution_engine import build_position_manager
    from src.strategy_engine import StrategyRunner, PoolConfig
    from src.strategy_engine.strategies import VolumeRebalanceStrategy
    from src.Constracts import UNISWAP_V3_USDC_ETH_POOL_ADDRESS

    pm     = build_position_manager()
    runner = StrategyRunner(
        strategy         = VolumeRebalanceStrategy(),
        position_manager = pm,
        pool_config      = PoolConfig(pool_address=UNISWAP_V3_USDC_ETH_POOL_ADDRESS),
    )
    runner.run_once()
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from src.db import repository as repo
from src.db.database import get_session
from src.execution_engine import MintParams, PositionManager
from src.strategy_engine.base import BaseStrategy, Decision, StrategyDecision
from src.strategy_engine.context import (
    ActivePosition,
    MarketContext,
    build_context,
    get_active_position,
)

logger = logging.getLogger(__name__)


@dataclass
class PoolConfig:
    """Runner 绑定的 pool 配置。"""
    pool_address:          str
    chain_id:              int = 1
    metrics_lookback_days: int = 3   # volume/tvl 均值窗口


class StrategyRunner:
    """
    策略执行器。

    职责：上下文构建 → 策略评估 → 链上执行 → 状态持久化，
    自身不包含任何策略逻辑，只做调度。
    """

    def __init__(
        self,
        strategy:         BaseStrategy,
        position_manager: PositionManager,
        pool_config:      PoolConfig,
    ) -> None:
        self.strategy = strategy
        self.pm       = position_manager
        self.cfg      = pool_config

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------

    def run_once(self) -> Decision:
        """
        执行一次完整的评估 + 执行循环。

        Returns
        -------
        Decision : 本次策略决策（便于调用方日志记录或单测断言）
        """
        logger.info(
            "[runner] === run_once start  pool=%s ===", self.cfg.pool_address
        )

        # ── ① 构建上下文 ───────────────────────────────────────────────────────
        with get_session() as session:
            try:
                ctx = build_context(
                    session,
                    self.cfg.pool_address,
                    n_days=self.cfg.metrics_lookback_days,
                    chain_id=self.cfg.chain_id,
                )
                position = get_active_position(
                    session, self.cfg.pool_address, self.cfg.chain_id
                )
            except ValueError as exc:
                logger.error("[runner] 上下文构建失败：%s", exc)
                raise

        logger.info(
            "[runner] context  tick=%d  avg_vtv=%s  position=%s",
            ctx.current_tick,
            f"{ctx.avg_volume_tvl_ratio:.3f}" if ctx.avg_volume_tvl_ratio else "N/A",
            position.position_id if position else None,
        )

        # ── ② 策略评估 ─────────────────────────────────────────────────────────
        decision = self.strategy.evaluate(ctx, position)
        logger.info(
            "[runner] decision=%s  reason=%s",
            decision.action.value,
            decision.reason,
        )

        # ── ③ 执行 + 持久化（执行失败时仍记录信号）────────────────────────────
        exec_ok = True
        with get_session() as session:
            try:
                self._execute(session, ctx, position, decision)
            except Exception as exc:
                exec_ok = False
                logger.error("[runner] 链上执行失败：%s", exc, exc_info=True)
            finally:
                self._save_signal(session, ctx, decision, exec_ok=exec_ok)

        if not exec_ok:
            raise RuntimeError("链上执行失败，详见日志。")

        logger.info("[runner] === run_once done ===")
        return decision

    def run_loop(self, interval_secs: int = 3600) -> None:
        """
        定时循环运行策略，适合生产环境部署。

        Parameters
        ----------
        interval_secs : int
            每次 run_once 之间的等待秒数，默认 3600（1 小时）。
        """
        logger.info("[runner] loop start, interval=%ds", interval_secs)
        while True:
            try:
                self.run_once()
            except Exception as exc:
                logger.error("[runner] run_once 异常，下次循环继续：%s", exc)
            logger.info("[runner] sleep %ds...", interval_secs)
            time.sleep(interval_secs)

    # ------------------------------------------------------------------
    # 内部执行路由
    # ------------------------------------------------------------------

    def _execute(
        self,
        session,
        ctx: MarketContext,
        position: Optional[ActivePosition],
        decision: Decision,
    ) -> None:
        action = decision.action

        if action == StrategyDecision.OPEN:
            self._do_open(session, ctx, decision, action_type="OPEN")

        elif action == StrategyDecision.REBALANCE:
            if position is not None:
                self._do_close_position(session, position, action_type="REBALANCE_CLOSE")
            self._do_open(session, ctx, decision, action_type="REBALANCE_OPEN")

        elif action == StrategyDecision.CLOSE:
            if position is not None:
                self._do_close_position(session, position, action_type="CLOSE")

        # HOLD：仅记录信号，无链上操作

    def _do_open(
        self,
        session,
        ctx: MarketContext,
        decision: Decision,
        action_type: str = "OPEN",
    ) -> None:
        logger.info(
            "[runner] mint  tick=[%d, %d]  amount0=%d  amount1=%d",
            decision.tick_lower,
            decision.tick_upper,
            decision.amount0_desired,
            decision.amount1_desired,
        )

        result = self.pm.mint(
            MintParams(
                token0=ctx.token0,
                token1=ctx.token1,
                fee=ctx.fee,
                tick_lower=decision.tick_lower,
                tick_upper=decision.tick_upper,
                amount0_desired=decision.amount0_desired,
                amount1_desired=decision.amount1_desired,
                amount0_min=0,
                amount1_min=0,
            )
        )

        logger.info(
            "[runner] mint ok  token_id=%d  liquidity=%d  tx=%s",
            result.token_id,
            result.liquidity,
            result.tx_hash,
        )

        now = datetime.utcnow()
        repo.create_lp_position(
            session,
            {
                "position_id":   str(result.token_id),
                "pool_address":  self.cfg.pool_address,
                "owner_address": self.pm._address,
                "tick_lower":    decision.tick_lower,
                "tick_upper":    decision.tick_upper,
                "liquidity":     result.liquidity,
                "opened_at":     now,
                "status":        "OPEN",
            },
        )
        repo.create_lp_position_action(
            session,
            {
                "position_id": str(result.token_id),
                "action_type": action_type,
                "tx_hash":     result.tx_hash,
                "action_time": now,
                "metadata": {
                    "tick_lower":  decision.tick_lower,
                    "tick_upper":  decision.tick_upper,
                    "amount0":     result.amount0,
                    "amount1":     result.amount1,
                    "liquidity":   result.liquidity,
                    "reason":      decision.reason,
                },
            },
        )

    def _do_close_position(
        self,
        session,
        position: ActivePosition,
        action_type: str = "CLOSE",
    ) -> None:
        logger.info(
            "[runner] close_position  token_id=%d  action=%s",
            position.token_id,
            action_type,
        )

        close = self.pm.close_position(position.token_id)

        logger.info("[runner] close ok  burn_tx=%s", close["burn_tx"])

        now = datetime.utcnow()
        repo.close_lp_position(session, position.position_id, closed_at=now)
        repo.create_lp_position_action(
            session,
            {
                "position_id": position.position_id,
                "action_type": action_type,
                "tx_hash":     close["burn_tx"],
                "action_time": now,
                "metadata": {
                    "decrease_amount0": close["decrease"].amount0 if close["decrease"] else None,
                    "decrease_amount1": close["decrease"].amount1 if close["decrease"] else None,
                    "collect_amount0":  close["collect"].amount0,
                    "collect_amount1":  close["collect"].amount1,
                },
            },
        )

    # ------------------------------------------------------------------
    # 信号持久化
    # ------------------------------------------------------------------

    def _save_signal(
        self,
        session,
        ctx: MarketContext,
        decision: Decision,
        exec_ok: bool = True,
    ) -> None:
        """记录策略信号，无论链上执行是否成功都写入。"""
        lower_price = self._tick_to_human_price(decision.tick_lower, ctx)
        upper_price = self._tick_to_human_price(decision.tick_upper, ctx)

        reason_payload = {**decision.meta, "text": decision.reason, "exec_ok": exec_ok}

        repo.create_strategy_signal(
            session,
            {
                "pool_address":            self.cfg.pool_address,
                "chain_id":                self.cfg.chain_id,
                "signal_time":             datetime.utcnow(),
                "signal_type":             decision.action.value,
                "signal_score":            (
                    float(ctx.avg_volume_tvl_ratio)
                    if ctx.avg_volume_tvl_ratio is not None
                    else None
                ),
                "recommended_lower_price": lower_price,
                "recommended_upper_price": upper_price,
                "expected_fee_apr":        (
                    float(ctx.latest_fee_apr) if ctx.latest_fee_apr else None
                ),
                "reason":                  reason_payload,
            },
        )

    @staticmethod
    def _tick_to_human_price(
        tick: Optional[int], ctx: MarketContext
    ) -> Optional[Decimal]:
        """
        tick → 人类可读价格（1 token0 = X token1，已调整 decimals）。

        raw_price = 1.0001^tick  (token1_raw / token0_raw)
        human     = raw_price * 10^(decimals0) / 10^(decimals1)
                  = raw_price * 10^(decimals0 - decimals1)

        对于 USDC(6)/WETH(18)：
            human = 1.0001^tick * 10^(6-18) = 1.0001^tick * 10^-12
            ≈ 0.000333...（1 USDC ≈ 0.000333 WETH，即 ETH=3000 时）
        """
        if tick is None:
            return None
        raw   = Decimal("1.0001") ** tick
        adj   = Decimal(10 ** (ctx.decimals0 - ctx.decimals1))
        return raw * adj
