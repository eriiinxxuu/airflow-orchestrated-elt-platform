#!/usr/bin/env python3
"""
yahoo_extractor.py
──────────────────
Entry point for the ECS Fargate container.
Calls the Yahoo Finance unofficial REST API and writes data to S3 as gzip NDJSON.

Supported modes (controlled by the EXTRACT_MODE environment variable):
  ohlcv        -> GET /v8/finance/chart/{symbol}
  fundamentals -> GET /v10/finance/quoteSummary/{symbol}?modules=...
  earnings     -> GET /v10/finance/quoteSummary/{symbol}?modules=earnings,...
  news         -> GET /v1/finance/search?q={symbol}

All parameters are passed in as container environment variables
by the Airflow ECSOperator.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("yahoo-extractor")


# ─────────────────────────────────────────────────────────────
# Config  (read from environment variables injected by Airflow ECSOperator)
# ─────────────────────────────────────────────────────────────
class Config:
    EXTRACT_MODE: str = os.environ["EXTRACT_MODE"]
    SYMBOLS: list = [
        s.strip().upper() for s in os.environ["SYMBOLS"].split(",") if s.strip()
    ]
    S3_BUCKET: str = os.environ["S3_BUCKET"]
    S3_KEY: str = os.environ["S3_KEY"]
    EXECUTION_DATE: str = os.getenv("EXECUTION_DATE", datetime.utcnow().isoformat())
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "dev")
    OHLCV_INTERVAL: str = os.getenv("OHLCV_INTERVAL", "1d")
    OHLCV_RANGE: str = os.getenv("OHLCV_RANGE", "1d")
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))
    RETRY_ATTEMPTS: int = int(os.getenv("RETRY_ATTEMPTS", "5"))
    RETRY_BACKOFF: float = float(os.getenv("RETRY_BACKOFF", "1.5"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))


# ─────────────────────────────────────────────────────────────
# Yahoo Finance API Endpoints
# ─────────────────────────────────────────────────────────────
BASE_CHART = "https://query1.finance.yahoo.com/v8/finance/chart"
BASE_SUMMARY = "https://query2.finance.yahoo.com/v10/finance/quoteSummary"
BASE_SEARCH = "https://query2.finance.yahoo.com/v1/finance/search"
CRUMB_URL = "https://query2.finance.yahoo.com/v1/test/getcrumb"
CONSENT_URL = "https://finance.yahoo.com"

# Yahoo Finance blocks requests without a browser User-Agent
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


# ─────────────────────────────────────────────────────────────
# HTTP Session
# ─────────────────────────────────────────────────────────────
def build_session() -> tuple[requests.Session, str]:
    """
    Establish a Yahoo Finance HTTP session and obtain a crumb token.

    Why is a crumb needed?
    Yahoo Finance uses a crumb as an anti-CSRF token on the v10 quoteSummary API.
    You must first visit the homepage to receive a session cookie, then call
    /v1/test/getcrumb to exchange the cookie for a crumb. Every subsequent API
    request must include this crumb as a query parameter.

    Why configure Retry?
    Yahoo Finance returns 429 Too Many Requests under high request frequency.
    HTTPAdapter + Retry handles waiting and retrying automatically.
    respect_retry_after_header=True means we honour Yahoo's Retry-After header.
    """
    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))

    session.headers.update(
        {
            "User-Agent": BROWSER_UA,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://finance.yahoo.com/",
            "Origin": "https://finance.yahoo.com",
            "DNT": "1",
        }
    )

    # Step 1: visit homepage so Yahoo sets the session cookie
    log.info("Establishing Yahoo Finance session ...")
    session.get(CONSENT_URL, timeout=15)

    # Step 2: exchange the cookie for a crumb token
    resp = session.get(CRUMB_URL, timeout=15)
    resp.raise_for_status()
    crumb = resp.text.strip()

    if not crumb:
        raise RuntimeError("Yahoo Finance returned an empty crumb - cannot proceed")

    log.info("Session established. crumb=%s", crumb)
    return session, crumb


# ─────────────────────────────────────────────────────────────
# Generic HTTP GET wrapper
# ─────────────────────────────────────────────────────────────
def _get(session: requests.Session, url: str, params: dict, label: str = "") -> dict:
    """
    Execute a GET request and handle two failure modes:
    1. HTTP-level errors (4xx/5xx) -> raise_for_status()
    2. Yahoo returns HTTP 200 but includes an error field in the body -> manual check

    Yahoo Finance quirk: even failed requests may return status 200;
    the real error is buried inside the JSON body under an 'error' key.
    """
    log.debug("GET %s params=%s [%s]", url, params, label)
    resp = session.get(url, params=params, timeout=30)

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        raise RuntimeError(f"Rate limited - retry after {retry_after}s")

    resp.raise_for_status()
    body = resp.json()

    # Check for Yahoo business-layer errors
    for key in ("chart", "quoteSummary", "optionChain", "finance"):
        err = body.get(key, {}).get("error")
        if err:
            raise RuntimeError(f"Yahoo API error [{label}]: {err}")

    return body


# ─────────────────────────────────────────────────────────────
# Mode 1: OHLCV  ->  /v8/finance/chart
# ─────────────────────────────────────────────────────────────
def extract_ohlcv(
    symbol: str, config: Config, session: requests.Session, crumb: str
) -> list[dict]:
    """
    Fetch OHLCV data from /v8/finance/chart.

    API response structure (key parts):
    {
      "chart": {
        "result": [{
          "meta":       { "currency": "USD", "exchangeName": "NASDAQ" },
          "timestamp":  [1705330200, 1705416600, ...],   <- Unix timestamp array
          "events": {
            "dividends": { "<ts>": { "amount": 0.25 } },
            "splits":    { "<ts>": { "numerator": 4, "denominator": 1 } }
          },
          "indicators": {
            "quote":    [{ "open": [...], "high": [...], "low": [...],
                           "close": [...], "volume": [...] }],
            "adjclose": [{ "adjclose": [...] }]           <- adjusted close
          }
        }]
      }
    }

    All price fields are aligned arrays: the i-th timestamp corresponds to
    the i-th open/high/low/close/volume. Index alignment is required.
    """
    log.info(
        "[%s] Fetching OHLCV interval=%s range=%s",
        symbol,
        config.OHLCV_INTERVAL,
        config.OHLCV_RANGE,
    )

    body = _get(
        session,
        f"{BASE_CHART}/{symbol}",
        params={
            "interval": config.OHLCV_INTERVAL,
            "range": config.OHLCV_RANGE,
            "includePrePost": "false",  # exclude pre/post market data
            "events": "div,splits",
            "crumb": crumb,
        },
        label=f"ohlcv:{symbol}",
    )

    result = body["chart"]["result"]
    if not result:
        log.warning("[%s] chart result empty", symbol)
        return []

    result = result[0]
    meta = result.get("meta", {})
    timestamps = result.get("timestamp", [])
    quote = result["indicators"]["quote"][0]
    adjclose = result["indicators"].get("adjclose", [{}])[0].get("adjclose", [])

    # Convert events to {timestamp_str: value} for O(1) lookups
    dividends = {
        str(v["date"]): v["amount"]
        for v in result.get("events", {}).get("dividends", {}).values()
    }
    splits = {
        str(v["date"]): f"{v['numerator']}:{v['denominator']}"
        for v in result.get("events", {}).get("splits", {}).values()
    }

    records = []
    for i, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        records.append(
            {
                "symbol": symbol,
                "timestamp": dt.isoformat(),
                "date": dt.date().isoformat(),
                "open": _f(quote["open"][i]),
                "high": _f(quote["high"][i]),
                "low": _f(quote["low"][i]),
                "close": _f(quote["close"][i]),
                "adj_close": _f(adjclose[i]) if i < len(adjclose) else None,
                "volume": _i(quote["volume"][i]),
                "dividend": dividends.get(str(ts)),
                "split": splits.get(str(ts)),
                "currency": meta.get("currency"),
                "exchange": meta.get("exchangeName"),
                "interval": config.OHLCV_INTERVAL,
                "_extracted_at": datetime.utcnow().isoformat(),
                "_execution_date": config.EXECUTION_DATE,
            }
        )

    log.info("[%s] OHLCV: %d rows", symbol, len(records))
    return records


# ─────────────────────────────────────────────────────────────
# Mode 2: Fundamentals  ->  /v10/finance/quoteSummary
# ─────────────────────────────────────────────────────────────

# Fetch multiple modules in a single request to avoid multiple round-trips
FUNDAMENTAL_MODULES = ",".join(
    [
        "assetProfile",  # Company overview, sector, employee count
        "summaryDetail",  # PE ratio, dividends, 52-week range
        "financialData",  # Profit margins, ROE, cash flow
        "defaultKeyStatistics",  # EPS, price-to-book, short interest
        "price",  # Live price, market cap
        "earningsTrend",  # Forward earnings estimates
    ]
)


def extract_fundamentals(
    symbol: str, config: Config, session: requests.Session, crumb: str
) -> list[dict]:
    """
    Fetch fundamentals from /v10/finance/quoteSummary.

    Key parameter: formatted=false
    Without this, Yahoo returns formatted objects:
      { "raw": 29.5, "fmt": "29.50" }
    With it, plain numbers are returned:
      29.5

    Response structure:
    {
      "quoteSummary": {
        "result": [{
          "assetProfile":         { "sector": "Technology", ... },
          "summaryDetail":        { "trailingPE": 29.5, ... },
          "financialData":        { "profitMargins": 0.253, ... },
          "defaultKeyStatistics": { "trailingEps": 6.13, ... },
          "price":                { "marketCap": 2950000000000, ... },
          "earningsTrend":        { "trend": [...] }
        }]
      }
    }
    Each module is a separate key; we extract and flatten them into one record.
    """
    log.info("[%s] Fetching fundamentals", symbol)

    body = _get(
        session,
        f"{BASE_SUMMARY}/{symbol}",
        params={
            "modules": FUNDAMENTAL_MODULES,
            "crumb": crumb,
            "formatted": "false",
            "lang": "en-US",
        },
        label=f"fundamentals:{symbol}",
    )

    res = body["quoteSummary"]["result"][0]
    prof = res.get("assetProfile", {})
    summ = res.get("summaryDetail", {})
    fins = res.get("financialData", {})
    stat = res.get("defaultKeyStatistics", {})
    pric = res.get("price", {})

    record = {
        "symbol": symbol,
        "date": config.EXECUTION_DATE[:10],
        # ── Company info ───────────────────────────
        "company_name": pric.get("longName") or pric.get("shortName"),
        "sector": prof.get("sector"),
        "industry": prof.get("industry"),
        "exchange": pric.get("exchangeName"),
        "currency": pric.get("currency"),
        "country": prof.get("country"),
        "full_time_employees": _i(prof.get("fullTimeEmployees")),
        # ── Live price ─────────────────────────────
        "regular_market_price": _f(pric.get("regularMarketPrice")),
        "market_cap": _f(pric.get("marketCap")),
        "regular_market_volume": _i(pric.get("regularMarketVolume")),
        # ── Valuation ──────────────────────────────
        "pe_ratio_ttm": _f(summ.get("trailingPE")),
        "forward_pe": _f(summ.get("forwardPE")),
        "price_to_book": _f(stat.get("priceToBook")),
        "price_to_sales_ttm": _f(summ.get("priceToSalesTrailing12Months")),
        "enterprise_value": _f(stat.get("enterpriseValue")),
        "ev_to_ebitda": _f(stat.get("enterpriseToEbitda")),
        "peg_ratio": _f(stat.get("pegRatio")),
        # ── Profitability ───────────────────────────
        "profit_margin": _f(fins.get("profitMargins")),
        "gross_margin": _f(fins.get("grossMargins")),
        "operating_margin": _f(fins.get("operatingMargins")),
        "return_on_equity": _f(fins.get("returnOnEquity")),
        "return_on_assets": _f(fins.get("returnOnAssets")),
        "revenue_ttm": _f(fins.get("totalRevenue")),
        "ebitda": _f(fins.get("ebitda")),
        "free_cashflow": _f(fins.get("freeCashflow")),
        "eps_ttm": _f(stat.get("trailingEps")),
        "eps_forward": _f(stat.get("forwardEps")),
        # ── Growth ─────────────────────────────────
        "revenue_growth_yoy": _f(fins.get("revenueGrowth")),
        "earnings_growth_yoy": _f(fins.get("earningsGrowth")),
        # ── Balance sheet ───────────────────────────
        "total_cash": _f(fins.get("totalCash")),
        "total_debt": _f(fins.get("totalDebt")),
        "debt_to_equity": _f(fins.get("debtToEquity")),
        "current_ratio": _f(fins.get("currentRatio")),
        # ── Dividends ──────────────────────────────
        "dividend_yield": _f(summ.get("dividendYield")),
        "dividend_rate": _f(summ.get("dividendRate")),
        "payout_ratio": _f(summ.get("payoutRatio")),
        # ── 52-week range ───────────────────────────
        "fifty_two_week_high": _f(summ.get("fiftyTwoWeekHigh")),
        "fifty_two_week_low": _f(summ.get("fiftyTwoWeekLow")),
        "beta": _f(summ.get("beta")),
        # ── Shares ─────────────────────────────────
        "shares_outstanding": _f(stat.get("sharesOutstanding")),
        "short_ratio": _f(stat.get("shortRatio")),
        "_extracted_at": datetime.utcnow().isoformat(),
        "_execution_date": config.EXECUTION_DATE,
    }

    log.info("[%s] Fundamentals extracted", symbol)
    return [record]


# ─────────────────────────────────────────────────────────────
# Mode 3: Earnings  ->  /v10/finance/quoteSummary (earnings modules)
# ─────────────────────────────────────────────────────────────

EARNINGS_MODULES = ",".join(
    [
        "earnings",  # Quarterly/annual historical EPS (actual vs estimate)
        "earningsTrend",  # Analyst forward estimates for upcoming quarters/years
        "earningsHistory",  # EPS surprise % for the last 4 quarters
        "calendarEvents",  # Next earnings date
    ]
)


def extract_earnings(
    symbol: str, config: Config, session: requests.Session, crumb: str
) -> list[dict]:
    """
    Fetch earnings data including historical EPS surprise and forward estimates.

    earningsHistory structure:
    [
      { "period": "-4q", "epsActual": 1.29, "epsEstimate": 1.27,
        "epsDifference": 0.02, "surprisePercent": 0.016 },
      ...
    ]

    earningsTrend structure:
    [
      { "period": "+1q", "endDate": "2024-03-31",
        "earningsEstimate": { "avg": 1.50, "low": 1.42, "high": 1.58,
                              "numberOfAnalysts": 28 },
        "revenueEstimate":  { "avg": 94500000000, ... } },
      ...
    ]

    calendarEvents structure:
    { "earnings": { "earningsDate": [1708560000, ...] } }
    """
    log.info("[%s] Fetching earnings", symbol)

    body = _get(
        session,
        f"{BASE_SUMMARY}/{symbol}",
        params={
            "modules": EARNINGS_MODULES,
            "crumb": crumb,
            "formatted": "false",
        },
        label=f"earnings:{symbol}",
    )

    res = body["quoteSummary"]["result"][0]
    hist = res.get("earningsHistory", {}).get("history", [])
    trend = res.get("earningsTrend", {}).get("trend", [])
    cal = res.get("calendarEvents", {}).get("earnings", {})

    # Next earnings date
    next_date = None
    earn_dates = cal.get("earningsDate", [])
    if earn_dates:
        next_date = _ts(
            earn_dates[0]
            if not isinstance(earn_dates[0], dict)
            else earn_dates[0].get("raw")
        )

    records = []

    # Historical quarterly results (last 4 quarters)
    for item in hist:
        records.append(
            {
                "symbol": symbol,
                "record_type": "historical",
                "period": item.get("period"),
                "report_date": _ts(item.get("quarter")),
                "period_type": "quarterly",
                "eps_actual": _f(item.get("epsActual")),
                "eps_estimate": _f(item.get("epsEstimate")),
                "eps_difference": _f(item.get("epsDifference")),
                "surprise_pct": _f(item.get("surprisePercent")),
                "revenue_estimate_avg": None,
                "eps_estimate_avg": None,
                "num_analysts_eps": None,
                "next_earnings_date": next_date,
                "_extracted_at": datetime.utcnow().isoformat(),
                "_execution_date": config.EXECUTION_DATE,
            }
        )

    # Forward estimates (0q, +1q, 0y, +1y)
    for t in trend:
        period = t.get("period", "")
        earn_est = t.get("earningsEstimate", {})
        rev_est = t.get("revenueEstimate", {})
        records.append(
            {
                "symbol": symbol,
                "record_type": "estimate",
                "period": period,
                "report_date": _ts(t.get("endDate")),
                "period_type": "annual" if "y" in period else "quarterly",
                "eps_actual": None,
                "eps_estimate": _f(earn_est.get("avg")),
                "eps_difference": None,
                "surprise_pct": None,
                "revenue_estimate_avg": _f(rev_est.get("avg")),
                "eps_estimate_avg": _f(earn_est.get("avg")),
                "num_analysts_eps": _i(earn_est.get("numberOfAnalysts")),
                "next_earnings_date": next_date,
                "_extracted_at": datetime.utcnow().isoformat(),
                "_execution_date": config.EXECUTION_DATE,
            }
        )

    log.info("[%s] Earnings: %d records", symbol, len(records))
    return records


# ─────────────────────────────────────────────────────────────
# Mode 4: News  ->  /v1/finance/search
# ─────────────────────────────────────────────────────────────
def extract_news(
    symbol: str, config: Config, session: requests.Session, crumb: str
) -> list[dict]:
    """
    Fetch recent news from /v1/finance/search.

    Response structure:
    {
      "news": [
        { "uuid": "...", "title": "...", "publisher": "Reuters",
          "link": "...", "providerPublishTime": 1713484200,
          "thumbnail": { "resolutions": [{ "url": "...", "width": 360 }] },
          "relatedTickers": ["AAPL"] }
      ]
    }
    """
    log.info("[%s] Fetching news", symbol)

    body = _get(
        session,
        BASE_SEARCH,
        params={
            "q": symbol,
            "newsCount": 20,
            "enableFuzzyQuery": "false",
            "crumb": crumb,
        },
        label=f"news:{symbol}",
    )

    records = []
    for item in body.get("news", []):
        # Select the largest available thumbnail
        resolutions = (item.get("thumbnail") or {}).get("resolutions", [])
        thumb = (
            max(resolutions, key=lambda r: r.get("width", 0)).get("url")
            if resolutions
            else None
        )

        records.append(
            {
                "symbol": symbol,
                "uuid": item.get("uuid"),
                "title": item.get("title"),
                "publisher": item.get("publisher"),
                "link": item.get("link"),
                "published_at": _ts(item.get("providerPublishTime")),
                "type": item.get("type"),
                "thumbnail_url": thumb,
                "related_tickers": json.dumps(item.get("relatedTickers", [])),
                "_extracted_at": datetime.utcnow().isoformat(),
                "_execution_date": config.EXECUTION_DATE,
            }
        )

    log.info("[%s] News: %d articles", symbol, len(records))
    return records


# ─────────────────────────────────────────────────────────────
# Dispatch table  (adding a new mode only requires one line here)
# ─────────────────────────────────────────────────────────────
EXTRACTORS = {
    "ohlcv": extract_ohlcv,
    "fundamentals": extract_fundamentals,
    "earnings": extract_earnings,
    "news": extract_news,
}


# ─────────────────────────────────────────────────────────────
# Type-safe scalar converters
# ─────────────────────────────────────────────────────────────
def _f(v: Any) -> float | None:
    """Safely convert to float, filtering out NaN and None."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if f != f else f  # NaN != NaN
    except (TypeError, ValueError):
        return None


def _i(v: Any) -> int | None:
    """Safely convert to int."""
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _ts(v: Any) -> str | None:
    """Convert a Unix timestamp to an ISO-8601 UTC string."""
    try:
        if v is None:
            return None
        return datetime.fromtimestamp(int(v), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


# ─────────────────────────────────────────────────────────────
# Per-symbol extraction with retry
# ─────────────────────────────────────────────────────────────
def fetch_with_retry(
    symbol: str, config: Config, session: requests.Session, crumb: str
) -> list[dict]:
    """
    Extract data for a single symbol with exponential backoff retry.
    Wait time per attempt: 1.5^attempt seconds (1.5s, 2.25s, 3.4s, ...)
    """
    extract_fn = EXTRACTORS[config.EXTRACT_MODE]
    last_exc: Exception | None = None

    for attempt in range(1, config.RETRY_ATTEMPTS + 1):
        try:
            return extract_fn(symbol, config, session, crumb)
        except Exception as exc:
            last_exc = exc
            wait = config.RETRY_BACKOFF**attempt
            log.warning(
                "[%s] attempt %d/%d failed: %s  (retry in %.1fs)",
                symbol,
                attempt,
                config.RETRY_ATTEMPTS,
                exc,
                wait,
            )
            time.sleep(wait)

    log.error(
        "[%s] all %d attempts failed: %s", symbol, config.RETRY_ATTEMPTS, last_exc
    )
    return []


# ─────────────────────────────────────────────────────────────
# S3 writer
# ─────────────────────────────────────────────────────────────
def write_to_s3(records: list[dict], config: Config) -> None:
    """
    Serialise all records as NDJSON (one JSON object per line),
    gzip compress, and write to S3.

    Why NDJSON instead of a plain JSON array?
    Redshift COPY natively supports NDJSON — each line is parsed independently
    without loading the entire file into memory, making it suitable for large files.

    Why gzip?
    Lower S3 storage cost, faster transfer, and Redshift COPY reads gzip directly.
    """
    s3_key = config.S3_KEY
    if not s3_key.endswith(".gz"):
        s3_key += ".gz"

    ndjson = "\n".join(json.dumps(r, default=str) for r in records)
    compressed = gzip.compress(ndjson.encode("utf-8"))

    boto3.client("s3").put_object(
        Bucket=config.S3_BUCKET,
        Key=s3_key,
        Body=compressed,
        ContentEncoding="gzip",
        ContentType="application/x-ndjson",
        Metadata={
            "record-count": str(len(records)),
            "extract-mode": config.EXTRACT_MODE,
            "symbols": ",".join(config.SYMBOLS),
            "extracted-at": datetime.utcnow().isoformat(),
        },
    )
    log.info(
        "Wrote %d records (%.1f KB) -> s3://%s/%s",
        len(records),
        len(compressed) / 1024,
        config.S3_BUCKET,
        s3_key,
    )


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main() -> None:
    config = Config()
    log.info(
        "Starting: mode=%s symbols=%s env=%s",
        config.EXTRACT_MODE,
        config.SYMBOLS,
        config.ENVIRONMENT,
    )

    if config.EXTRACT_MODE not in EXTRACTORS:
        raise ValueError(
            f"Unknown EXTRACT_MODE '{config.EXTRACT_MODE}'. "
            f"Valid: {list(EXTRACTORS)}"
        )

    # All symbols share one session and crumb to minimise handshake overhead
    session, crumb = build_session()
    all_records: list[dict] = []

    if len(config.SYMBOLS) == 1 or config.MAX_WORKERS == 1:
        # Single-threaded: easier to debug
        for symbol in config.SYMBOLS:
            all_records.extend(fetch_with_retry(symbol, config, session, crumb))
            time.sleep(0.3)  # Polite throttle to avoid triggering rate limits
    else:
        # Multi-threaded: significantly faster for large watchlists.
        # Note: Yahoo Finance enforces per-IP concurrency limits;
        # keep MAX_WORKERS at 5 or below.
        with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as pool:
            futures = {
                pool.submit(fetch_with_retry, sym, config, session, crumb): sym
                for sym in config.SYMBOLS
            }
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    all_records.extend(future.result())
                except Exception as exc:
                    log.error("[%s] unhandled: %s", sym, exc)

    log.info("Done: %d records from %d symbols", len(all_records), len(config.SYMBOLS))
    write_to_s3(all_records, config)


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as exc:
        log.exception("Fatal: %s", exc)
        sys.exit(1)
