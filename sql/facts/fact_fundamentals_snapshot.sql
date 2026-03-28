-- ──────────────────────────────────────────────────────────────
-- facts.fact_fundamentals_snapshot
-- 每天一条记录，保留历史快照（不覆盖过去的数据）
-- ──────────────────────────────────────────────────────────────
BEGIN;

DELETE FROM facts.fact_fundamentals_snapshot
WHERE snapshot_date IN (SELECT DISTINCT date FROM staging.yf_fundamentals);

INSERT INTO facts.fact_fundamentals_snapshot (
    snapshot_key,
    symbol_key,
    symbol,
    snapshot_date,
    market_cap,
    pe_ratio_ttm,
    forward_pe,
    ev_to_ebitda,
    profit_margin,
    return_on_equity,
    revenue_growth_yoy,
    earnings_growth_yoy,
    debt_to_equity,
    dividend_yield,
    beta,
    _loaded_at
)
SELECT
    MD5(f.symbol || f.date::VARCHAR)   AS snapshot_key,
    COALESCE(d.symbol_key, 'UNKNOWN')  AS symbol_key,
    f.symbol,
    f.date                             AS snapshot_date,
    f.market_cap,
    f.pe_ratio_ttm,
    f.forward_pe,
    f.ev_to_ebitda,
    f.profit_margin,
    f.return_on_equity,
    f.revenue_growth_yoy,
    f.earnings_growth_yoy,
    f.debt_to_equity,
    f.dividend_yield,
    f.beta,
    GETDATE()
FROM staging.yf_fundamentals f
LEFT JOIN dimensions.dim_symbols d ON f.symbol = d.symbol;

COMMIT;
