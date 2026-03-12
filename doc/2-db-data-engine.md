## 四、第三层：快照与聚合表

这一层是给 Data Engine 用的。
原则是：

原始事件表负责“真实记录”
聚合表负责“快速分析”


7. pool_price_snapshots

作用：

按时间记录池子价格和状态快照，用于：

波动率计算

价格曲线

tick 变化分析

TVL 估算
```
CREATE TABLE pool_price_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    pool_address        VARCHAR(42) NOT NULL,
    chain_id            INTEGER NOT NULL DEFAULT 1,
    block_number        BIGINT NOT NULL,
    block_timestamp     TIMESTAMP NOT NULL,

    sqrt_price_x96      NUMERIC(78, 0) NOT NULL,
    tick                INTEGER NOT NULL,
    liquidity           NUMERIC(78, 0) NOT NULL,

    price_token0        NUMERIC(38, 18),
    price_token1        NUMERIC(38, 18),

    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_snapshots_pool FOREIGN KEY (pool_address) REFERENCES pools(pool_address),
    CONSTRAINT uq_snapshots_pool_block UNIQUE (pool_address, block_number)
);
```
```
CREATE INDEX idx_snapshots_pool_time ON pool_price_snapshots(pool_address, block_timestamp);
CREATE INDEX idx_snapshots_block_time ON pool_price_snapshots(block_timestamp);
```

8. pool_metrics_hourly

作用：

按小时聚合池子指标。
这是 Data Engine 的核心中间表。

```
CREATE TABLE pool_metrics_hourly (
    id                      BIGSERIAL PRIMARY KEY,
    pool_address            VARCHAR(42) NOT NULL,
    chain_id                INTEGER NOT NULL DEFAULT 1,
    metric_hour             TIMESTAMP NOT NULL,

    price_open              NUMERIC(38, 18),
    price_close             NUMERIC(38, 18),
    price_high              NUMERIC(38, 18),
    price_low               NUMERIC(38, 18),

    volume_token0_raw       NUMERIC(78, 0) NOT NULL DEFAULT 0,
    volume_token1_raw       NUMERIC(78, 0) NOT NULL DEFAULT 0,
    volume_usd              NUMERIC(38, 18) NOT NULL DEFAULT 0,

    swap_count              INTEGER NOT NULL DEFAULT 0,
    mint_count              INTEGER NOT NULL DEFAULT 0,
    burn_count              INTEGER NOT NULL DEFAULT 0,
    collect_count           INTEGER NOT NULL DEFAULT 0,

    fee_usd                 NUMERIC(38, 18) NOT NULL DEFAULT 0,
    avg_liquidity           NUMERIC(78, 0),
    close_liquidity         NUMERIC(78, 0),

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_hourly_pool FOREIGN KEY (pool_address) REFERENCES pools(pool_address),
    CONSTRAINT uq_hourly_pool_hour UNIQUE (pool_address, metric_hour)
);
```
这张表能支持什么分析

24h volume

7d volume

hourly volatility

fee APR

活跃度排行


9. pool_metrics_daily

作用：

按天聚合，适合 Dashboard 和策略模块直接读取。
```
CREATE TABLE pool_metrics_daily (
    id                      BIGSERIAL PRIMARY KEY,
    pool_address            VARCHAR(42) NOT NULL,
    chain_id                INTEGER NOT NULL DEFAULT 1,
    metric_date             DATE NOT NULL,

    price_open              NUMERIC(38, 18),
    price_close             NUMERIC(38, 18),
    price_high              NUMERIC(38, 18),
    price_low               NUMERIC(38, 18),

    volume_usd              NUMERIC(38, 18) NOT NULL DEFAULT 0,
    fee_usd                 NUMERIC(38, 18) NOT NULL DEFAULT 0,
    tvl_usd                 NUMERIC(38, 18),

    swap_count              INTEGER NOT NULL DEFAULT 0,
    mint_count              INTEGER NOT NULL DEFAULT 0,
    burn_count              INTEGER NOT NULL DEFAULT 0,
    collect_count           INTEGER NOT NULL DEFAULT 0,

    volatility_1d           NUMERIC(20, 10),
    volume_tvl_ratio        NUMERIC(20, 10),
    fee_apr                 NUMERIC(20, 10),
    il_estimate_1d          NUMERIC(20, 10),

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_daily_pool FOREIGN KEY (pool_address) REFERENCES pools(pool_address),
    CONSTRAINT uq_daily_pool_date UNIQUE (pool_address, metric_date)
);
```
这是你后面最常查的一张表

比如：

最近 7 天哪个 pool 最值得做 LP

fee_apr 最高的是谁

volume/tvl 最高的是谁

波动率和 APR 组合最好的池子是谁


# Data Engine设计参考
指标概况
```
1 交易活跃度指标
2 收益指标
3 风险指标
4 LP 竞争指标
```

一、交易活跃度指标（Activity Metrics）
```

这些指标回答一个问题：

这个池子有没有交易？

因为 LP 的收益来自：

swap fee
1 24h Trading Volume

最基础指标。

数据来源
swaps 表
计算

如果 token0 是 stablecoin：

SELECT
    pool_address,
    SUM(ABS(amount0_raw)) AS volume_token0
FROM swaps
WHERE block_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY pool_address;

如果要统一 USD，需要 token price。

推荐输出
volume_1h
volume_24h
volume_7d
2 Swap Count

衡量交易频率。

SELECT
    pool_address,
    COUNT(*) AS swap_count_24h
FROM swaps
WHERE block_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY pool_address;

用途：

判断市场活跃程度
3 LP Activity

统计 LP 行为：

mint count
burn count
SELECT
    pool_address,
    COUNT(*) AS mint_count_24h
FROM mints
WHERE block_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY pool_address;
```

二、收益指标（Profit Metrics）
```
这些指标决定：

LP 能赚多少钱

4 Fee Revenue

手续费收入。

公式：

fee = volume × fee_rate

例如：

volume = $100M
fee tier = 0.05%

fee = 50k
SQL 示例
SELECT
    p.pool_address,
    SUM(ABS(amount0_raw)) * (p.fee / 1000000.0) AS fee_token0
FROM swaps s
JOIN pools p ON s.pool_address = p.pool_address
WHERE s.block_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY p.pool_address;
5 Fee APR

LP 年化手续费收益。

公式：

APR = daily_fee × 365 / TVL

例如：

daily_fee = 100k
TVL = 20M

APR = 182%
SQL

假设你有 tvl_usd

SELECT
    pool_address,
    fee_usd * 365 / tvl_usd AS fee_apr
FROM pool_metrics_daily;
```
三、风险指标（Risk Metrics）
```
LP 赚钱的前提：

fee > IL

所以必须计算风险。

6 Price Volatility

波动率。

计算方法：

log return std

公式：

r_t = ln(price_t / price_t-1)

volatility = std(r_t)
SQL 示例（简化版）
SELECT
    pool_address,
    STDDEV(price_close) AS volatility
FROM pool_metrics_hourly
WHERE metric_hour > NOW() - INTERVAL '24 hours'
GROUP BY pool_address;

更严格的要用 log return。

7 Impermanent Loss Estimate

IL 公式：

IL = 2√r / (1+r) − 1

r = price_ratio

例如：

price change = 1.2

IL = -0.45%
Python 实现
import math

def impermanent_loss(price_ratio):
    return 2 * math.sqrt(price_ratio) / (1 + price_ratio) - 1
```

四、LP 竞争指标（Liquidity Competition）
```
LP 不只是看 volume，还要看：

有多少人和你抢 fee
8 Volume / TVL

LP 最重要指标。

公式：

volume_tvl_ratio = volume / tvl

例如：

volume = 200M
TVL = 50M

ratio = 4

经验值：

ratio	LP情况
<0.5	差
1	OK
>2	很好
SQL
SELECT
    volume_usd / tvl_usd AS volume_tvl_ratio
FROM pool_metrics_daily;
9 Liquidity Density

V3 特有指标。

衡量：

当前 tick 附近有多少 liquidity

数据来源：

mint
burn
tick range

简单版本：

liquidity / TVL

复杂版本：

tick liquidity distribution

第一版可以不做复杂版。
```

## 最后建议
我建议先做这 6 个指标：
```
TVL
Volume_24h
Volume_TVL
Fee_APR
Volatility
LP_Score
```

推荐代码结构：
```
data_engine/
│
├ pool_metrics.py
├ volume_metrics.py
├ volatility_metrics.py
├ fee_metrics.py
├ il_estimator.py
└ scoring_model.py
```