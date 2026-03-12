## 五、第四层：系统状态与策略表


11. strategy_signals

作用：

存储策略模块输出结果
```
CREATE TABLE strategy_signals (
    id                      BIGSERIAL PRIMARY KEY,
    pool_address            VARCHAR(42) NOT NULL,
    chain_id                INTEGER NOT NULL DEFAULT 1,
    signal_time             TIMESTAMP NOT NULL,

    signal_type             VARCHAR(64) NOT NULL,
    signal_score            NUMERIC(20, 10),

    recommended_lower_price NUMERIC(38, 18),
    recommended_upper_price NUMERIC(38, 18),

    expected_fee_apr        NUMERIC(20, 10),
    expected_il             NUMERIC(20, 10),
    expected_net_apr        NUMERIC(20, 10),

    reason                  JSONB,
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_signal_pool FOREIGN KEY (pool_address) REFERENCES pools(pool_address)
);
```

六、如果你后面要做自己的 LP 仓位管理

这部分第一版可以不做，但我建议预留思路。
12. lp_positions
```
CREATE TABLE lp_positions (
    id                      BIGSERIAL PRIMARY KEY,
    position_id             VARCHAR(128) UNIQUE,
    pool_address            VARCHAR(42) NOT NULL,
    owner_address           VARCHAR(42) NOT NULL,

    tick_lower              INTEGER NOT NULL,
    tick_upper              INTEGER NOT NULL,
    liquidity               NUMERIC(78, 0) NOT NULL,

    opened_at               TIMESTAMP NOT NULL,
    closed_at               TIMESTAMP,

    status                  VARCHAR(32) NOT NULL DEFAULT 'OPEN',
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_lp_positions_pool FOREIGN KEY (pool_address) REFERENCES pools(pool_address)
);
```
13. lp_position_actions
```
CREATE TABLE lp_position_actions (
    id                  BIGSERIAL PRIMARY KEY,
    position_id         VARCHAR(128) NOT NULL,
    action_type         VARCHAR(32) NOT NULL,
    tx_hash             VARCHAR(66),
    block_number        BIGINT,
    action_time         TIMESTAMP NOT NULL,
    metadata            JSONB,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
```