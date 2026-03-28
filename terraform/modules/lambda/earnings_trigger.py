"""
earnings_trigger.py
────────────────────
Lambda function triggered daily by EventBridge at 06:00 UTC.

Responsibilities:
  1. Fetch calendarEvents from Yahoo Finance for each symbol in the watchlist
  2. Check whether any symbol has earnings today or tomorrow
  3. Found     -> call the Airflow REST API to trigger yf_event_earnings DAG
  4. Not found -> exit silently, DAG never starts

Why Lambda instead of a ShortCircuitOperator inside the DAG?
  - A DAG run has scheduling overhead (MWAA worker must spin up every time)
  - Lambda is far lighter for a simple check that runs in seconds
  - Separation of concerns: event detection vs data extraction are different jobs
  - Setting schedule_interval=None on the DAG keeps the architecture clean

Why urllib instead of requests?
  - Smaller deployment package means faster cold starts
  - urllib is part of the standard library, no extra dependencies to package
  - The logic here is simple enough that Session/retry abstractions are not needed

Authentication: MWAA IAM token (CreateWebLoginToken)
  - No credentials stored anywhere
  - Lambda IAM role is granted airflow:CreateWebLoginToken
  - boto3 exchanges the role for a short-lived web login token (~60s)
  - Token is passed as a Bearer header on the DAG trigger request

Environment variables (injected by Terraform):
  AIRFLOW_BASE_URL    MWAA webserver URL
  AIRFLOW_DAG_ID      Target DAG ID (default: yf_event_earnings)
  MWAA_ENV_NAME       MWAA environment name used to generate the token
  WATCHLIST           JSON array string e.g. '["AAPL","MSFT"]'
  DAYS_AHEAD          How many days ahead to look for earnings (default: 1)
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config ────────────────────────────────────────────────────
AIRFLOW_BASE_URL = os.environ["AIRFLOW_BASE_URL"].rstrip("/")
AIRFLOW_DAG_ID = os.environ.get("AIRFLOW_DAG_ID", "yf_event_earnings")
MWAA_ENV_NAME = os.environ["MWAA_ENV_NAME"]
WATCHLIST = json.loads(os.environ["WATCHLIST"])
DAYS_AHEAD = int(os.environ.get("DAYS_AHEAD", "1"))

YAHOO_SUMMARY = "https://query2.finance.yahoo.com/v10/finance/quoteSummary"
YAHOO_CRUMB = "https://query2.finance.yahoo.com/v1/test/getcrumb"
YAHOO_CONSENT = "https://finance.yahoo.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com/",
}


# ── MWAA IAM token ────────────────────────────────────────────
def _get_mwaa_token() -> str:
    """
    Generate a short-lived MWAA web login token using the Lambda execution role.

    boto3 calls mwaa.create_web_login_token() which exchanges the IAM role
    for a temporary token valid for approximately 60 seconds.
    The Lambda IAM role must have airflow:CreateWebLoginToken on the
    MWAA environment ARN. No username or password is stored anywhere.
    """
    resp = boto3.client("mwaa").create_web_login_token(Name=MWAA_ENV_NAME)
    logger.info("MWAA token generated successfully")
    return resp["WebToken"]


# ── Yahoo Finance session ─────────────────────────────────────
def _build_opener():
    """
    Build a urllib opener with a persistent cookie jar.

    Yahoo Finance requires a two-step handshake before any API call:
      Step 1: GET finance.yahoo.com   -> sets the session cookie
      Step 2: GET /v1/test/getcrumb   -> exchanges cookie for a crumb token

    The crumb must be appended as a query parameter on every subsequent
    API request, otherwise Yahoo returns an error or an empty result.
    """
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
    opener.open(urllib.request.Request(YAHOO_CONSENT, headers=HEADERS))

    crumb_req = urllib.request.Request(YAHOO_CRUMB, headers=HEADERS)
    crumb = opener.open(crumb_req).read().decode().strip()

    if not crumb:
        raise RuntimeError("Received empty crumb – Yahoo Finance session failed")

    logger.info("Yahoo Finance session established. crumb=%s", crumb)
    return opener, crumb


# ── Earnings date check ───────────────────────────────────────
def _check_earnings_date(
    symbol: str, opener, crumb: str, target_dates: set,
) -> str | None:
    url = (
        f"{YAHOO_SUMMARY}/{symbol}"
        f"?modules=calendarEvents&crumb={crumb}&formatted=false"
    )
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        body = json.loads(opener.open(req, timeout=15).read())
        result = (body.get("quoteSummary", {}).get("result") or [{}])[0]
        dates = (
            result.get("calendarEvents", {}).get("earnings", {}).get("earningsDate", [])
        )
    except Exception as exc:
        logger.warning("[%s] Failed to fetch calendar events: %s", symbol, exc)
        return None

    for ts in dates:
        if isinstance(ts, dict):
            ts = ts.get("raw")
        try:
            report_date = datetime.fromtimestamp(int(ts), tz=SYDNEY_TZ).date()
            if report_date in target_dates:
                return report_date.isoformat()
        except (TypeError, ValueError, OSError):
            continue

    return None


# ── Airflow REST API ──────────────────────────────────────────
def _trigger_dag(
    symbols: list[str], earnings_dates: dict[str, str], token: str,
) -> None:

    run_id = f"earnings_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
    payload = json.dumps(
        {
            "dag_run_id": run_id,
            "conf": {
                "symbols": symbols,
                "earnings_dates": earnings_dates,
                "triggered_by": "earnings_trigger_lambda",
            },
        }
    ).encode()

    req = urllib.request.Request(
        f"{AIRFLOW_BASE_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        body = json.loads(resp.read())
        logger.info(
            "DAG triggered successfully. run_id=%s state=%s",
            body.get("dag_run_id"),
            body.get("state"),
        )
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"Airflow API returned HTTP {exc.code}: {exc.read().decode()}"
        )


# ── Lambda handler ────────────────────────────────────────────
def handler(event: dict, context: Any) -> dict:
    logger.info("Earnings trigger started. watchlist=%s", WATCHLIST)

    opener, crumb = _build_opener()

    now = datetime.now(tz=SYDNEY_TZ)
    target_dates = {now.date() + timedelta(days=i) for i in range(DAYS_AHEAD + 1)}
    logger.info("Checking dates: %s", sorted(str(d) for d in target_dates))

    upcoming: dict[str, str] = {}
    for symbol in WATCHLIST:
        result = _check_earnings_date(symbol, opener, crumb, target_dates)
        if result:
            upcoming[symbol] = result
            logger.info("[%s] Earnings on %s", symbol, result)

    if not upcoming:
        logger.info("No earnings found in the next %d day(s). Exiting.", DAYS_AHEAD)
        return {"triggered": False, "symbols": []}

    symbols = list(upcoming.keys())
    logger.info("Triggering DAG for %d symbol(s): %s", len(symbols), symbols)

    # Generate the token as late as possible since it expires in ~60 seconds
    token = _get_mwaa_token()
    _trigger_dag(symbols, upcoming, token)

    return {
        "triggered": True,
        "symbols": symbols,
        "earnings_dates": upcoming,
    }
