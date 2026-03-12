"""
Data Engine 公共工具函数

包含：
- Uniswap V3 价格计算（sqrtPriceX96 → 人类可读价格）
- 稳定币侧自动识别
- 对数收益率波动率
- 无常损失估算（全范围 V2 假设）
"""
import math
from decimal import Decimal, getcontext
from typing import Optional

getcontext().prec = 40

_Q96 = Decimal(2 ** 96)

# 常见稳定币符号集合（大写）
STABLECOINS: frozenset[str] = frozenset({
    "USDC", "USDT", "DAI", "BUSD", "FRAX", "LUSD", "USDE", "FDUSD", "TUSD", "USDP",
})


# ── 价格计算 ──────────────────────────────────────────────────────────────────

def sqrt_price_x96_to_prices(
    sqrt_price_x96: int,
    decimals0: int,
    decimals1: int,
) -> tuple[Optional[Decimal], Optional[Decimal]]:
    """
    从 sqrtPriceX96 计算双向人类可读价格。

    Uniswap V3 内部以最小单位存储：
        sqrt_price_x96 = sqrt(token1_raw / token0_raw) × 2^96

    调整 decimals 后得到人类可读价格：
        price_token0 = (sqrt/2^96)^2 × 10^decimals0 / 10^decimals1
                     = 1 个 token0 能换多少 token1（human units）
        price_token1 = 1 / price_token0

    示例（USDC/WETH 池，decimals0=6，decimals1=18）：
        price_token0 ≈ 0.0003   （1 USDC ≈ 0.0003 ETH）
        price_token1 ≈ 3000     （1 ETH ≈ 3000 USDC）

    Returns:
        (price_token0, price_token1)，无效输入时返回 (None, None)。
    """
    if sqrt_price_x96 <= 0:
        return None, None

    sq = Decimal(sqrt_price_x96) / _Q96
    raw_price = sq * sq                                           # token1_raw / token0_raw
    adjustment = Decimal(10 ** decimals0) / Decimal(10 ** decimals1)
    price_token0 = raw_price * adjustment

    if price_token0 == 0:
        return Decimal(0), None

    price_token1 = Decimal(1) / price_token0
    return price_token0, price_token1


# ── 稳定币侧识别 ──────────────────────────────────────────────────────────────

def get_stablecoin_side(
    symbol0: Optional[str],
    symbol1: Optional[str],
) -> Optional[int]:
    """
    识别哪一侧是稳定币。
    Returns:
        0  → token0 是稳定币
        1  → token1 是稳定币
        None → 双侧均非稳定币，无法做 USD 换算
    """
    if symbol0 and symbol0.upper() in STABLECOINS:
        return 0
    if symbol1 and symbol1.upper() in STABLECOINS:
        return 1
    return None


def raw_to_human(amount_raw: Optional[int], decimals: int) -> Optional[Decimal]:
    """将链上最小单位数量转换为人类可读数量（除以 10^decimals）。"""
    if amount_raw is None:
        return None
    return Decimal(amount_raw) / Decimal(10 ** decimals)


# ── 波动率 ────────────────────────────────────────────────────────────────────

def calc_log_return_volatility(prices: list) -> Optional[float]:
    """
    计算对数收益率标准差（样本标准差）。

    公式：
        r_t = ln(price_t / price_{t-1})
        volatility = std(r_t)   ← 非年化，调用方按需 × sqrt(N) 年化

    Args:
        prices: 按时间顺序排列的收盘价列表（float 或 Decimal）

    Returns:
        对数收益率标准差，数据不足时返回 None。
    """
    if len(prices) < 2:
        return None

    log_returns = []
    for i in range(1, len(prices)):
        p_prev = float(prices[i - 1])
        p_curr = float(prices[i])
        if p_prev > 0 and p_curr > 0:
            log_returns.append(math.log(p_curr / p_prev))

    n = len(log_returns)
    if n < 1:
        return None
    if n == 1:
        return 0.0

    mean = sum(log_returns) / n
    variance = sum((r - mean) ** 2 for r in log_returns) / (n - 1)   # 样本方差
    return math.sqrt(variance)


# ── 无常损失 ──────────────────────────────────────────────────────────────────

def calc_il_fullrange(price_ratio: Optional[float]) -> Optional[float]:
    """
    全范围（V2 式）无常损失估算。

    公式：
        IL = 2√r / (1+r) − 1     其中 r = price_end / price_start

    注意：这是 V2 全范围仓位的 IL，V3 窄范围仓位的真实 IL 更大。
    仅作为池级别的参考基准，字段名应包含 "fullrange" 以示区分。

    Args:
        price_ratio: price_end / price_start

    Returns:
        IL 值（负数表示损失），例如 -0.005 = -0.5%
        输入无效时返回 None。
    """
    if price_ratio is None or price_ratio <= 0:
        return None
    r = float(price_ratio)
    return 2 * math.sqrt(r) / (1 + r) - 1
