

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dimensions;
CREATE SCHEMA IF NOT EXISTS facts;




DROP TABLE IF EXISTS staging.yf_ohlcv;
CREATE TABLE staging.yf_ohlcv (
    symbol           VARCHAR(20)   NOT NULL,
    timestamp        VARCHAR(35),           -- ISO-8601 UTC
    date             DATE,
    open             DECIMAL(18,6),
    high             DECIMAL(18,6),
    low              DECIMAL(18,6),
    close            DECIMAL(18,6),
    adj_close        DECIMAL(18,6),         
    volume           BIGINT,
    dividend         DECIMAL(10,6),
    split            VARCHAR(20),           -- e.g. "4:1"
    currency         CHAR(3),
    exchange         VARCHAR(20),
    interval         VARCHAR(5),            -- "1d", "1h", etc.
    _extracted_at    TIMESTAMP,
    _execution_date  VARCHAR(35)
)
DISTSTYLE KEY DISTKEY (symbol)
SORTKEY (date, symbol);



DROP TABLE IF EXISTS staging.yf_fundamentals;
CREATE TABLE staging.yf_fundamentals (
    symbol                   VARCHAR(20)  NOT NULL,
    date                     DATE,
    company_name             VARCHAR(500),
    sector                   VARCHAR(200),
    industry                 VARCHAR(200),
    exchange                 VARCHAR(20),
    currency                 CHAR(3),
    country                  VARCHAR(100),
    full_time_employees      INT,
    regular_market_price     DECIMAL(18,6),
    market_cap               DECIMAL(22,2),
    regular_market_volume    BIGINT,
    pe_ratio_ttm             DECIMAL(12,4),
    forward_pe               DECIMAL(12,4),
    price_to_book            DECIMAL(12,4),
    price_to_sales_ttm       DECIMAL(12,4),
    enterprise_value         DECIMAL(22,2),
    ev_to_ebitda             DECIMAL(12,4),
    peg_ratio                DECIMAL(12,4),
    profit_margin            DECIMAL(10,6),
    gross_margin             DECIMAL(10,6),
    operating_margin         DECIMAL(10,6),
    return_on_equity         DECIMAL(10,6),
    return_on_assets         DECIMAL(10,6),
    revenue_ttm              DECIMAL(22,2),
    ebitda                   DECIMAL(22,2),
    free_cashflow            DECIMAL(22,2),
    eps_ttm                  DECIMAL(12,4),
    eps_forward              DECIMAL(12,4),
    revenue_growth_yoy       DECIMAL(10,6),
    earnings_growth_yoy      DECIMAL(10,6),
    total_cash               DECIMAL(22,2),
    total_debt               DECIMAL(22,2),
    debt_to_equity           DECIMAL(12,4),
    current_ratio            DECIMAL(10,4),
    dividend_yield           DECIMAL(10,6),
    dividend_rate            DECIMAL(10,6),
    payout_ratio             DECIMAL(10,6),
    fifty_two_week_high      DECIMAL(18,6),
    fifty_two_week_low       DECIMAL(18,6),
    beta                     DECIMAL(10,6),
    shares_outstanding       DECIMAL(22,0),
    short_ratio              DECIMAL(10,4),
    _extracted_at            TIMESTAMP,
    _execution_date          VARCHAR(35)
)
DISTSTYLE KEY DISTKEY (symbol)
SORTKEY (date, symbol);


DROP TABLE IF EXISTS staging.yf_earnings;
CREATE TABLE staging.yf_earnings (
    symbol                VARCHAR(20),
    record_type           VARCHAR(20),    -- 'historical' | 'estimate'
    period                VARCHAR(10),    -- '-4q', '0q', '+1q', '0y', '+1y'
    report_date           VARCHAR(35),
    period_type           VARCHAR(10),    -- 'quarterly' | 'annual'
    eps_actual            DECIMAL(12,4),
    eps_estimate          DECIMAL(12,4),
    eps_difference        DECIMAL(12,4),
    surprise_pct          DECIMAL(10,6),  -- (eps_actual - eps_estimate) / NULLIF(eps_estimate, 0)
    revenue_estimate_avg  DECIMAL(22,2),
    eps_estimate_avg      DECIMAL(12,4),
    num_analysts_eps      INT,
    next_earnings_date    VARCHAR(35),
    _extracted_at         TIMESTAMP,
    _execution_date       VARCHAR(35)
)
DISTSTYLE KEY DISTKEY (symbol)
SORTKEY (report_date, symbol);


-- ──────────────────────────────────────────────────────────────
-- DIMENSION TABLES
-- ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dimensions.dim_symbols (
    symbol_key        VARCHAR(32)  NOT NULL PRIMARY KEY,  -- MD5
    symbol            VARCHAR(20)  NOT NULL,
    company_name      VARCHAR(500),
    sector            VARCHAR(200),
    industry          VARCHAR(200),
    exchange          VARCHAR(20),
    currency          CHAR(3),
    country           VARCHAR(100),
    is_active         BOOLEAN      DEFAULT TRUE,
    first_seen_date   DATE,
    last_seen_date    DATE,
    _updated_at       TIMESTAMP    DEFAULT GETDATE()
)
DISTSTYLE ALL     
SORTKEY (symbol);


-- ──────────────────────────────────────────────────────────────
-- FACT TABLES
-- ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS facts.fact_daily_prices (
    price_key        VARCHAR(32)  NOT NULL PRIMARY KEY,
    symbol_key       VARCHAR(32),
    symbol           VARCHAR(20)  NOT NULL,
    date             DATE         NOT NULL,
    open             DECIMAL(18,6),
    high             DECIMAL(18,6),
    low              DECIMAL(18,6),
    close            DECIMAL(18,6),
    adj_close        DECIMAL(18,6),
    volume           BIGINT,
    daily_return     DECIMAL(12,8),   -- (close - prev_close) / prev_close
    currency         CHAR(3),
    _loaded_at       TIMESTAMP    DEFAULT GETDATE()
)
DISTSTYLE KEY DISTKEY (symbol)
SORTKEY (date, symbol);


CREATE TABLE IF NOT EXISTS facts.fact_fundamentals_snapshot (
    snapshot_key         VARCHAR(32) NOT NULL PRIMARY KEY,
    symbol_key           VARCHAR(32),
    symbol               VARCHAR(20) NOT NULL,
    snapshot_date        DATE        NOT NULL,
    market_cap           DECIMAL(22,2),
    pe_ratio_ttm         DECIMAL(12,4),
    forward_pe           DECIMAL(12,4),
    ev_to_ebitda         DECIMAL(12,4),
    profit_margin        DECIMAL(10,6),
    return_on_equity     DECIMAL(10,6),
    revenue_growth_yoy   DECIMAL(10,6),
    earnings_growth_yoy  DECIMAL(10,6),
    debt_to_equity       DECIMAL(12,4),
    dividend_yield       DECIMAL(10,6),
    beta                 DECIMAL(10,6),
    _loaded_at           TIMESTAMP   DEFAULT GETDATE()
)
DISTSTYLE KEY DISTKEY (symbol)
SORTKEY (snapshot_date, symbol);


CREATE TABLE IF NOT EXISTS facts.fact_earnings_surprises (
    earnings_key      VARCHAR(32) NOT NULL PRIMARY KEY,
    symbol_key        VARCHAR(32),
    symbol            VARCHAR(20) NOT NULL,
    report_date       DATE,
    period_type       VARCHAR(10),
    eps_actual        DECIMAL(12,4),
    eps_estimate      DECIMAL(12,4),
    eps_surprise      DECIMAL(12,4),
    eps_surprise_pct  DECIMAL(10,6),
    beat_estimate     BOOLEAN,
    _loaded_at        TIMESTAMP   DEFAULT GETDATE()
)
DISTSTYLE KEY DISTKEY (symbol)
SORTKEY (report_date, symbol);
