
BEGIN;

DELETE FROM facts.fact_earnings_surprises
WHERE (symbol, report_date) IN (
    SELECT symbol, CAST(report_date AS DATE)
    FROM staging.yf_earnings
    WHERE record_type = 'historical' AND report_date IS NOT NULL
);

INSERT INTO facts.fact_earnings_surprises (
    earnings_key,
    symbol_key,
    symbol,
    report_date,
    period_type,
    eps_actual,
    eps_estimate,
    eps_surprise,
    eps_surprise_pct,
    beat_estimate,
    _loaded_at
)
SELECT
    MD5(e.symbol || COALESCE(e.report_date, '') || e.period_type)  AS earnings_key,
    COALESCE(d.symbol_key, 'UNKNOWN')                              AS symbol_key,
    e.symbol,
    CAST(e.report_date AS DATE)                                    AS report_date,
    e.period_type,
    e.eps_actual,
    e.eps_estimate,
    e.eps_difference                                               AS eps_surprise,
    e.surprise_pct                                                 AS eps_surprise_pct,
    CASE WHEN e.eps_actual > e.eps_estimate THEN TRUE ELSE FALSE END AS beat_estimate,
    GETDATE()
FROM staging.yf_earnings e
LEFT JOIN dimensions.dim_symbols d ON e.symbol = d.symbol
WHERE e.record_type = 'historical'
  AND e.report_date IS NOT NULL;

COMMIT;

VACUUM facts.fact_earnings_surprises;
ANALYZE facts.fact_earnings_surprises;
