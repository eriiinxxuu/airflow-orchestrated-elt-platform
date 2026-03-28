-- ──────────────────────────────────────────────────────────────
-- facts.fact_daily_prices
-- 策略：DELETE + INSERT（删掉今天的记录，重新插入）
-- 为什么不用 UPSERT（MERGE）？Redshift 没有原生 MERGE，
-- DELETE + INSERT 是标准做法。
-- ──────────────────────────────────────────────────────────────
BEGIN;

-- 删除今日已有数据（支持重跑幂等）
DELETE FROM facts.fact_daily_prices
WHERE date IN (SELECT DISTINCT date FROM staging.yf_ohlcv);

-- 插入新数据，同时计算日涨跌幅
INSERT INTO facts.fact_daily_prices (
    price_key,
    symbol_key,
    symbol,
    date,
    open, high, low, close, adj_close,
    volume,
    daily_return,
    currency,
    _loaded_at
)
SELECT
    MD5(s.symbol || s.date::VARCHAR)             AS price_key,
    COALESCE(d.symbol_key, 'UNKNOWN')            AS symbol_key,
    s.symbol,
    s.date,
    s.open, s.high, s.low, s.close, s.adj_close,
    s.volume,

    -- 日涨跌幅：(今日收盘 - 昨日收盘) / 昨日收盘
    -- LAG 窗口函数按 symbol 分组，按日期排序，取上一行
    CASE
        WHEN LAG(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date) IS NOT NULL
        THEN (s.close - LAG(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date))
             / NULLIF(LAG(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date), 0)
        ELSE NULL
    END                                           AS daily_return,

    s.currency,
    GETDATE()
FROM staging.yf_ohlcv s
LEFT JOIN dimensions.dim_symbols d ON s.symbol = d.symbol;

COMMIT;
