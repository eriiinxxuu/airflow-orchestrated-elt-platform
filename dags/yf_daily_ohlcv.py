"""
yf_daily_ohlcv.py
──────────────────
Daily OHLCV price data pipeline.

Schedule: Monday to Friday at 21:00 UTC
  - US markets close at 20:00 UTC; waiting 1 hour allows Yahoo Finance
    data to stabilise before extraction

SLA: Must complete by 23:00 UTC (2-hour window)

Task chain:
  extract_ohlcv -> load_staging -> quality_checks -> transform
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from dag_utils import default_args, sla_miss_callback
from operators.yahoo_finance_ecs_operator import YahooFinanceOHLCVOperator
from operators.data_quality_operator import (
    DataQualityOperator,
    row_count_check,
    null_check,
    duplicate_check,
    freshness_check,
)
from yf_config import get_watchlist, get_s3_bucket, get_ecs_config, S3_PARTITION

with DAG(
    dag_id="yf_daily_ohlcv",
    description="Yahoo Finance /v8/finance/chart -> S3 -> Redshift (daily OHLCV)",
    schedule_interval="0 21 * * 1-5",
    start_date=datetime(2026, 3, 28),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(retries=2),
    sla_miss_callback=sla_miss_callback,
    tags=["yahoo-finance", "ohlcv", "daily"],
) as dag:

    S3_BUCKET = get_s3_bucket()
    S3_PREFIX = "raw/ohlcv/"

    # ── Task 1: Launch ECS task to extract OHLCV ─────────────
    # YahooFinanceOHLCVOperator internally:
    #   1. Builds the S3 key with date partitioning
    #   2. Packs parameters into ECS environment variables
    #   3. Calls RunTask and polls until STOPPED
    #   4. Writes the s3:// path to XCom
    extract = YahooFinanceOHLCVOperator(
        task_id="extract_ohlcv",
        symbols=get_watchlist(),
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        interval="1d",
        range_="1d",
        sla=timedelta(minutes=15),
        **get_ecs_config(),
    )

    # ── Task 2: COPY S3 file into Redshift staging ────────────
    # S3ToRedshiftOperator executes:
    #   COPY staging.yf_ohlcv FROM 's3://bucket/raw/ohlcv/year=.../...'
    #   FORMAT AS JSON 'auto'   <- automatically maps JSON keys to columns
    #   GZIP                    <- reads compressed files
    #   TIMEFORMAT 'auto'       <- automatically parses timestamp formats
    load = S3ToRedshiftOperator(
        task_id="load_staging",
        schema="staging",
        table="yf_ohlcv",
        s3_bucket=S3_BUCKET,
        s3_key=f"{S3_PREFIX}{S3_PARTITION}",  # Jinja template, rendered at runtime
        copy_options=["FORMAT AS JSON 'auto'", "GZIP", "TIMEFORMAT 'auto'"],
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_default",
    )

    # ── Task 3: Data quality checks ───────────────────────────
    quality = DataQualityOperator(
        task_id="quality_checks",
        redshift_conn_id="redshift_default",
        checks=[
            row_count_check("staging.yf_ohlcv", min_rows=1),
            null_check("staging.yf_ohlcv", "symbol"),
            null_check("staging.yf_ohlcv", "close"),
            duplicate_check("staging.yf_ohlcv", ["symbol", "date"]),
            freshness_check("staging.yf_ohlcv", "_extracted_at", max_age_hours=6),
            {
                "description": "High >= Low (basic candlestick sanity check)",
                "sql":         "SELECT COUNT(*) FROM staging.yf_ohlcv WHERE high < low",
                "expected":    0,
            },
            {
                "description": "No negative close price",
                "sql":         "SELECT COUNT(*) FROM staging.yf_ohlcv WHERE close < 0",
                "expected":    0,
            },
        ],
    )

    # ── Task 4: SQL transformation -> fact table ──────────────
    transform = RedshiftSQLOperator(
        task_id="transform_fact_daily_prices",
        sql="sql/facts/fact_daily_prices.sql",
        redshift_conn_id="redshift_default",
    )

    # Linear dependency chain
    extract >> load >> quality >> transform