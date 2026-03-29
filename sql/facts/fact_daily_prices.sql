
BEGIN;


DELETE FROM facts.fact_daily_prices
WHERE date IN (SELECT DISTINCT date FROM staging.yf_ohlcv);

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
