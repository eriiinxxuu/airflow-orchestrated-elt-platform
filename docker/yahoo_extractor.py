#!/usr/bin/env python3
"""
yahoo_extractor.py
──────────────────
ECS Fargate 容器的入口程序。
直接调用 Yahoo Finance 非官方 REST API，把数据以 gzip NDJSON 格式写入 S3。

支持四种模式（由 EXTRACT_MODE 环境变量控制）：
  ohlcv        → GET /v8/finance/chart/{symbol}
  fundamentals → GET /v10/finance/quoteSummary/{symbol}?modules=...
  earnings     → GET /v10/finance/quoteSummary/{symbol}?modules=earnings,...
  news         → GET /v1/finance/search?q={symbol}

Airflow 的 ECSOperator 通过容器环境变量传入所有参数。
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
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("yahoo-extractor")


# ─────────────────────────────────────────────────────────────
# Config  （从环境变量读，Airflow ECSOperator 注入）
# ─────────────────────────────────────────────────────────────
class Config:
    EXTRACT_MODE:    str   = os.environ["EXTRACT_MODE"]
    SYMBOLS:         list  = [s.strip().upper()
                               for s in os.environ["SYMBOLS"].split(",") if s.strip()]
    S3_BUCKET:       str   = os.environ["S3_BUCKET"]
    S3_KEY:          str   = os.environ["S3_KEY"]
    EXECUTION_DATE:  str   = os.getenv("EXECUTION_DATE", datetime.utcnow().isoformat())
    ENVIRONMENT:     str   = os.getenv("ENVIRONMENT", "dev")
    OHLCV_INTERVAL:  str   = os.getenv("OHLCV_INTERVAL", "1d")
    OHLCV_RANGE:     str   = os.getenv("OHLCV_RANGE",    "1d")
    MAX_WORKERS:     int   = int(os.getenv("MAX_WORKERS",    "4"))
    RETRY_ATTEMPTS:  int   = int(os.getenv("RETRY_ATTEMPTS", "5"))
    RETRY_BACKOFF:   float = float(os.getenv("RETRY_BACKOFF", "1.5"))
    REQUEST_TIMEOUT: int   = int(os.getenv("REQUEST_TIMEOUT", "30"))


# ─────────────────────────────────────────────────────────────
# Yahoo Finance API Endpoints
# ─────────────────────────────────────────────────────────────
BASE_CHART   = "https://query1.finance.yahoo.com/v8/finance/chart"
BASE_SUMMARY = "https://query2.finance.yahoo.com/v10/finance/quoteSummary"
BASE_SEARCH  = "https://query2.finance.yahoo.com/v1/finance/search"
CRUMB_URL    = "https://query2.finance.yahoo.com/v1/test/getcrumb"
CONSENT_URL  = "https://finance.yahoo.com"

# Yahoo Finance 会拦截没有浏览器 UA 的请求，必须伪装
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


# ─────────────────────────────────────────────────────────────
# HTTP Session 管理
# ─────────────────────────────────────────────────────────────
def build_session() -> tuple[requests.Session, str]:
    """
    建立 Yahoo Finance 的 HTTP session，获取 crumb token。

    为什么需要 crumb？
    Yahoo Finance 的 v10 quoteSummary 接口使用 crumb 作为防 CSRF token。
    必须先访问主页（拿 cookie），再请求 /v1/test/getcrumb（拿 crumb），
    之后每次 API 请求都要附带这个 crumb 参数。

    为什么配置 Retry？
    Yahoo Finance 会在高频请求时返回 429 Too Many Requests。
    HTTPAdapter + Retry 会自动等待并重试，无需手写循环。
    respect_retry_after_header=True 表示遵守 Yahoo 返回的 Retry-After 时间。
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

    session.headers.update({
        "User-Agent":      BROWSER_UA,
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         "https://finance.yahoo.com/",
        "Origin":          "https://finance.yahoo.com",
        "DNT":             "1",
    })

    # Step 1: 访问主页，让 Yahoo 设置 session cookie
    log.info("Establishing Yahoo Finance session …")
    session.get(CONSENT_URL, timeout=15)

    # Step 2: 用已有 cookie 获取 crumb
    resp = session.get(CRUMB_URL, timeout=15)
    resp.raise_for_status()
    crumb = resp.text.strip()

    if not crumb:
        raise RuntimeError("Yahoo Finance returned an empty crumb – cannot proceed")

    log.info("Session established. crumb=%s", crumb)
    return session, crumb


# ─────────────────────────────────────────────────────────────
# 通用 HTTP GET 封装
# ─────────────────────────────────────────────────────────────
def _get(session: requests.Session, url: str, params: dict, label: str = "") -> dict:
    """
    发起 GET 请求，处理两种错误情况：
    1. HTTP 层面的错误（4xx/5xx）→ raise_for_status()
    2. Yahoo 返回 HTTP 200 但 body 里包含 error 字段 → 手动检查

    Yahoo Finance 的特殊行为：即使请求失败，状态码也可能是 200，
    真正的错误信息藏在 JSON body 的 error 字段里。
    """
    log.debug("GET %s params=%s [%s]", url, params, label)
    resp = session.get(url, params=params, timeout=30)

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        raise RuntimeError(f"Rate limited – retry after {retry_after}s")

    resp.raise_for_status()
    body = resp.json()

    # 检查 Yahoo 的业务层错误
    for key in ("chart", "quoteSummary", "optionChain", "finance"):
        err = body.get(key, {}).get("error")
        if err:
            raise RuntimeError(f"Yahoo API error [{label}]: {err}")

    return body


# ─────────────────────────────────────────────────────────────
# 模式一：OHLCV  →  /v8/finance/chart
# ─────────────────────────────────────────────────────────────
def extract_ohlcv(symbol: str, config: Config,
                  session: requests.Session, crumb: str) -> list[dict]:
    """
    调用 /v8/finance/chart 获取 OHLCV 数据。

    API 返回结构（关键部分）：
    {
      "chart": {
        "result": [{
          "meta":       { "currency": "USD", "exchangeName": "NASDAQ" },
          "timestamp":  [1705330200, 1705416600, ...],   ← Unix 时间戳数组
          "events": {
            "dividends": { "<ts>": { "amount": 0.25 } },
            "splits":    { "<ts>": { "numerator": 4, "denominator": 1 } }
          },
          "indicators": {
            "quote":    [{ "open": [...], "high": [...], "low": [...],
                           "close": [...], "volume": [...] }],
            "adjclose": [{ "adjclose": [...] }]           ← 复权价
          }
        }]
      }
    }

    注意：所有价格字段都是"对齐的数组"，第 i 个 timestamp 对应
    第 i 个 open/high/low/close/volume，必须用 zip 或 index 对齐。
    """
    log.info("[%s] Fetching OHLCV interval=%s range=%s",
             symbol, config.OHLCV_INTERVAL, config.OHLCV_RANGE)

    body = _get(session, f"{BASE_CHART}/{symbol}", params={
        "interval":       config.OHLCV_INTERVAL,
        "range":          config.OHLCV_RANGE,
        "includePrePost": "false",   # 不要盘前盘后数据
        "events":         "div,splits",
        "crumb":          crumb,
    }, label=f"ohlcv:{symbol}")

    result = body["chart"]["result"]
    if not result:
        log.warning("[%s] chart result empty", symbol)
        return []

    result     = result[0]
    meta       = result.get("meta", {})
    timestamps = result.get("timestamp", [])
    quote      = result["indicators"]["quote"][0]
    adjclose   = result["indicators"].get("adjclose", [{}])[0].get("adjclose", [])

    # 把 events 转成 {timestamp_str: value} 便于 O(1) 查找
    dividends = {str(v["date"]): v["amount"]
                 for v in result.get("events", {}).get("dividends", {}).values()}
    splits    = {str(v["date"]): f"{v['numerator']}:{v['denominator']}"
                 for v in result.get("events", {}).get("splits", {}).values()}

    records = []
    for i, ts in enumerate(timestamps):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        records.append({
            "symbol":           symbol,
            "timestamp":        dt.isoformat(),
            "date":             dt.date().isoformat(),
            "open":             _f(quote["open"][i]),
            "high":             _f(quote["high"][i]),
            "low":              _f(quote["low"][i]),
            "close":            _f(quote["close"][i]),
            "adj_close":        _f(adjclose[i]) if i < len(adjclose) else None,
            "volume":           _i(quote["volume"][i]),
            "dividend":         dividends.get(str(ts)),
            "split":            splits.get(str(ts)),
            "currency":         meta.get("currency"),
            "exchange":         meta.get("exchangeName"),
            "interval":         config.OHLCV_INTERVAL,
            "_extracted_at":    datetime.utcnow().isoformat(),
            "_execution_date":  config.EXECUTION_DATE,
        })

    log.info("[%s] OHLCV: %d rows", symbol, len(records))
    return records


# ─────────────────────────────────────────────────────────────
# 模式二：基本面  →  /v10/finance/quoteSummary
# ─────────────────────────────────────────────────────────────

# 一次请求拉取多个 module，避免多次 round-trip
FUNDAMENTAL_MODULES = ",".join([
    "assetProfile",          # 公司简介、行业、员工数
    "summaryDetail",         # PE、股息、52周高低
    "financialData",         # 利润率、ROE、现金流
    "defaultKeyStatistics",  # EPS、市净率、空头数据
    "price",                 # 实时价格、市值
    "earningsTrend",         # 前瞻盈利预估
])


def extract_fundamentals(symbol: str, config: Config,
                          session: requests.Session, crumb: str) -> list[dict]:
    """
    调用 /v10/finance/quoteSummary 获取基本面数据。

    关键参数：formatted=false
    不加这个参数，Yahoo 会返回带格式的对象：
      { "raw": 29.5, "fmt": "29.50" }
    加了之后直接返回数字：
      29.5
    处理起来简单很多。

    返回的数据结构：
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
    每个 module 是独立的 key，我们分别提取再合并成一条记录。
    """
    log.info("[%s] Fetching fundamentals", symbol)

    body = _get(session, f"{BASE_SUMMARY}/{symbol}", params={
        "modules":   FUNDAMENTAL_MODULES,
        "crumb":     crumb,
        "formatted": "false",
        "lang":      "en-US",
    }, label=f"fundamentals:{symbol}")

    res  = body["quoteSummary"]["result"][0]
    prof = res.get("assetProfile",          {})
    summ = res.get("summaryDetail",         {})
    fins = res.get("financialData",         {})
    stat = res.get("defaultKeyStatistics",  {})
    pric = res.get("price",                 {})

    record = {
        "symbol":                    symbol,
        "date":                      config.EXECUTION_DATE[:10],
        # ── 公司基本信息 ──────────────────────────
        "company_name":              pric.get("longName") or pric.get("shortName"),
        "sector":                    prof.get("sector"),
        "industry":                  prof.get("industry"),
        "exchange":                  pric.get("exchangeName"),
        "currency":                  pric.get("currency"),
        "country":                   prof.get("country"),
        "full_time_employees":       _i(prof.get("fullTimeEmployees")),
        # ── 实时价格 ──────────────────────────────
        "regular_market_price":      _f(pric.get("regularMarketPrice")),
        "market_cap":                _f(pric.get("marketCap")),
        "regular_market_volume":     _i(pric.get("regularMarketVolume")),
        # ── 估值指标 ──────────────────────────────
        "pe_ratio_ttm":              _f(summ.get("trailingPE")),
        "forward_pe":                _f(summ.get("forwardPE")),
        "price_to_book":             _f(stat.get("priceToBook")),
        "price_to_sales_ttm":        _f(summ.get("priceToSalesTrailing12Months")),
        "enterprise_value":          _f(stat.get("enterpriseValue")),
        "ev_to_ebitda":              _f(stat.get("enterpriseToEbitda")),
        "peg_ratio":                 _f(stat.get("pegRatio")),
        # ── 盈利能力 ──────────────────────────────
        "profit_margin":             _f(fins.get("profitMargins")),
        "gross_margin":              _f(fins.get("grossMargins")),
        "operating_margin":          _f(fins.get("operatingMargins")),
        "return_on_equity":          _f(fins.get("returnOnEquity")),
        "return_on_assets":          _f(fins.get("returnOnAssets")),
        "revenue_ttm":               _f(fins.get("totalRevenue")),
        "ebitda":                    _f(fins.get("ebitda")),
        "free_cashflow":             _f(fins.get("freeCashflow")),
        "eps_ttm":                   _f(stat.get("trailingEps")),
        "eps_forward":               _f(stat.get("forwardEps")),
        # ── 增长率 ────────────────────────────────
        "revenue_growth_yoy":        _f(fins.get("revenueGrowth")),
        "earnings_growth_yoy":       _f(fins.get("earningsGrowth")),
        # ── 资产负债 ──────────────────────────────
        "total_cash":                _f(fins.get("totalCash")),
        "total_debt":                _f(fins.get("totalDebt")),
        "debt_to_equity":            _f(fins.get("debtToEquity")),
        "current_ratio":             _f(fins.get("currentRatio")),
        # ── 股息 ──────────────────────────────────
        "dividend_yield":            _f(summ.get("dividendYield")),
        "dividend_rate":             _f(summ.get("dividendRate")),
        "payout_ratio":              _f(summ.get("payoutRatio")),
        # ── 52周数据 ──────────────────────────────
        "fifty_two_week_high":       _f(summ.get("fiftyTwoWeekHigh")),
        "fifty_two_week_low":        _f(summ.get("fiftyTwoWeekLow")),
        "beta":                      _f(summ.get("beta")),
        # ── 股份 ──────────────────────────────────
        "shares_outstanding":        _f(stat.get("sharesOutstanding")),
        "short_ratio":               _f(stat.get("shortRatio")),
        "_extracted_at":             datetime.utcnow().isoformat(),
        "_execution_date":           config.EXECUTION_DATE,
    }

    log.info("[%s] Fundamentals extracted", symbol)
    return [record]


# ─────────────────────────────────────────────────────────────
# 模式三：财报  →  /v10/finance/quoteSummary (earnings modules)
# ─────────────────────────────────────────────────────────────

EARNINGS_MODULES = ",".join([
    "earnings",          # 季度/年度历史 EPS（实际 vs 预估）
    "earningsTrend",     # 未来季度/年度的分析师预估
    "earningsHistory",   # 最近4季的 surprise%
    "calendarEvents",    # 下次财报日期
])


def extract_earnings(symbol: str, config: Config,
                     session: requests.Session, crumb: str) -> list[dict]:
    """
    获取财报数据，包含历史 EPS surprise 和未来预估。

    earningsHistory 结构：
    [
      { "period": "-4q", "epsActual": 1.29, "epsEstimate": 1.27,
        "epsDifference": 0.02, "surprisePercent": 0.016 },
      ...
    ]

    earningsTrend 结构：
    [
      { "period": "+1q", "endDate": "2024-03-31",
        "earningsEstimate": { "avg": 1.50, "low": 1.42, "high": 1.58,
                              "numberOfAnalysts": 28 },
        "revenueEstimate":  { "avg": 94500000000, ... } },
      ...
    ]

    calendarEvents 结构：
    { "earnings": { "earningsDate": [1708560000, ...] } }
    """
    log.info("[%s] Fetching earnings", symbol)

    body = _get(session, f"{BASE_SUMMARY}/{symbol}", params={
        "modules":   EARNINGS_MODULES,
        "crumb":     crumb,
        "formatted": "false",
    }, label=f"earnings:{symbol}")

    res  = body["quoteSummary"]["result"][0]
    hist = res.get("earningsHistory", {}).get("history",  [])
    trend = res.get("earningsTrend",  {}).get("trend",    [])
    cal   = res.get("calendarEvents", {}).get("earnings", {})

    # 下次财报日期
    next_date = None
    earn_dates = cal.get("earningsDate", [])
    if earn_dates:
        next_date = _ts(earn_dates[0] if not isinstance(earn_dates[0], dict)
                        else earn_dates[0].get("raw"))

    records = []

    # 历史季报（最近4季）
    for item in hist:
        records.append({
            "symbol":               symbol,
            "record_type":          "historical",
            "period":               item.get("period"),
            "report_date":          _ts(item.get("quarter")),
            "period_type":          "quarterly",
            "eps_actual":           _f(item.get("epsActual")),
            "eps_estimate":         _f(item.get("epsEstimate")),
            "eps_difference":       _f(item.get("epsDifference")),
            "surprise_pct":         _f(item.get("surprisePercent")),
            "revenue_estimate_avg": None,
            "eps_estimate_avg":     None,
            "num_analysts_eps":     None,
            "next_earnings_date":   next_date,
            "_extracted_at":        datetime.utcnow().isoformat(),
            "_execution_date":      config.EXECUTION_DATE,
        })

    # 前瞻预估（0q, +1q, 0y, +1y）
    for t in trend:
        period   = t.get("period", "")
        earn_est = t.get("earningsEstimate",  {})
        rev_est  = t.get("revenueEstimate",   {})
        records.append({
            "symbol":               symbol,
            "record_type":          "estimate",
            "period":               period,
            "report_date":          _ts(t.get("endDate")),
            "period_type":          "annual" if "y" in period else "quarterly",
            "eps_actual":           None,
            "eps_estimate":         _f(earn_est.get("avg")),
            "eps_difference":       None,
            "surprise_pct":         None,
            "revenue_estimate_avg": _f(rev_est.get("avg")),
            "eps_estimate_avg":     _f(earn_est.get("avg")),
            "num_analysts_eps":     _i(earn_est.get("numberOfAnalysts")),
            "next_earnings_date":   next_date,
            "_extracted_at":        datetime.utcnow().isoformat(),
            "_execution_date":      config.EXECUTION_DATE,
        })

    log.info("[%s] Earnings: %d records", symbol, len(records))
    return records


# ─────────────────────────────────────────────────────────────
# 模式四：新闻  →  /v1/finance/search
# ─────────────────────────────────────────────────────────────
def extract_news(symbol: str, config: Config,
                 session: requests.Session, crumb: str) -> list[dict]:
    """
    调用 /v1/finance/search 获取最新新闻。

    返回结构：
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

    body = _get(session, BASE_SEARCH, params={
        "q":                symbol,
        "newsCount":        20,
        "enableFuzzyQuery": "false",
        "crumb":            crumb,
    }, label=f"news:{symbol}")

    records = []
    for item in body.get("news", []):
        # 选最大的缩略图
        resolutions = (item.get("thumbnail") or {}).get("resolutions", [])
        thumb = max(resolutions, key=lambda r: r.get("width", 0)).get("url") \
                if resolutions else None

        records.append({
            "symbol":           symbol,
            "uuid":             item.get("uuid"),
            "title":            item.get("title"),
            "publisher":        item.get("publisher"),
            "link":             item.get("link"),
            "published_at":     _ts(item.get("providerPublishTime")),
            "type":             item.get("type"),
            "thumbnail_url":    thumb,
            "related_tickers":  json.dumps(item.get("relatedTickers", [])),
            "_extracted_at":    datetime.utcnow().isoformat(),
            "_execution_date":  config.EXECUTION_DATE,
        })

    log.info("[%s] News: %d articles", symbol, len(records))
    return records


# ─────────────────────────────────────────────────────────────
# Dispatch table  （扩展新模式只需在这里加一行）
# ─────────────────────────────────────────────────────────────
EXTRACTORS = {
    "ohlcv":        extract_ohlcv,
    "fundamentals": extract_fundamentals,
    "earnings":     extract_earnings,
    "news":         extract_news,
}


# ─────────────────────────────────────────────────────────────
# 类型安全的标量转换
# ─────────────────────────────────────────────────────────────
def _f(v: Any) -> float | None:
    """安全转 float，过滤 NaN 和 None"""
    if v is None:
        return None
    try:
        f = float(v)
        return None if f != f else f   # NaN != NaN
    except (TypeError, ValueError):
        return None


def _i(v: Any) -> int | None:
    """安全转 int"""
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _ts(v: Any) -> str | None:
    """Unix 时间戳 → ISO-8601 UTC 字符串"""
    try:
        if v is None:
            return None
        return datetime.fromtimestamp(int(v), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


# ─────────────────────────────────────────────────────────────
# 单个 symbol 的提取（含重试）
# ─────────────────────────────────────────────────────────────
def fetch_with_retry(symbol: str, config: Config,
                     session: requests.Session, crumb: str) -> list[dict]:
    """
    对单个 symbol 执行提取，失败时指数退避重试。
    每次等待时间：1.5^attempt 秒（1.5s → 2.25s → 3.4s → …）
    """
    extract_fn = EXTRACTORS[config.EXTRACT_MODE]
    last_exc: Exception | None = None

    for attempt in range(1, config.RETRY_ATTEMPTS + 1):
        try:
            return extract_fn(symbol, config, session, crumb)
        except Exception as exc:
            last_exc = exc
            wait = config.RETRY_BACKOFF ** attempt
            log.warning("[%s] attempt %d/%d failed: %s  (retry in %.1fs)",
                        symbol, attempt, config.RETRY_ATTEMPTS, exc, wait)
            time.sleep(wait)

    log.error("[%s] all %d attempts failed: %s", symbol, config.RETRY_ATTEMPTS, last_exc)
    return []


# ─────────────────────────────────────────────────────────────
# S3 写入
# ─────────────────────────────────────────────────────────────
def write_to_s3(records: list[dict], config: Config) -> None:
    """
    将所有记录序列化为 NDJSON（每行一个 JSON 对象），
    gzip 压缩后写入 S3。

    为什么用 NDJSON 而不是普通 JSON 数组？
    Redshift COPY 命令原生支持 NDJSON 格式，每行独立解析，
    不需要把整个文件读入内存，适合大文件。

    为什么 gzip？
    S3 存储成本低、传输快，Redshift COPY 支持直接读取 gzip 文件。
    """
    s3_key = config.S3_KEY
    if not s3_key.endswith(".gz"):
        s3_key += ".gz"

    ndjson     = "\n".join(json.dumps(r, default=str) for r in records)
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
            "symbols":      ",".join(config.SYMBOLS),
            "extracted-at": datetime.utcnow().isoformat(),
        },
    )
    log.info("Wrote %d records (%.1f KB) → s3://%s/%s",
             len(records), len(compressed) / 1024, config.S3_BUCKET, s3_key)


# ─────────────────────────────────────────────────────────────
# 主函数
# ─────────────────────────────────────────────────────────────
def main() -> None:
    config = Config()
    log.info("Starting: mode=%s symbols=%s env=%s",
             config.EXTRACT_MODE, config.SYMBOLS, config.ENVIRONMENT)

    if config.EXTRACT_MODE not in EXTRACTORS:
        raise ValueError(f"Unknown EXTRACT_MODE '{config.EXTRACT_MODE}'. "
                         f"Valid: {list(EXTRACTORS)}")

    # 所有 symbol 共用一个 session 和 crumb（节省握手次数）
    session, crumb = build_session()
    all_records: list[dict] = []

    if len(config.SYMBOLS) == 1 or config.MAX_WORKERS == 1:
        # 单线程：调试友好
        for symbol in config.SYMBOLS:
            all_records.extend(fetch_with_retry(symbol, config, session, crumb))
            time.sleep(0.3)   # 礼貌性 throttle，避免触发 rate limit
    else:
        # 多线程：大 watchlist 时提速明显
        # 注意：Yahoo Finance 对 IP 有并发限制，MAX_WORKERS 不要超过 5
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
