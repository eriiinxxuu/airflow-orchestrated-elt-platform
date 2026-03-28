"""
yf_daily_fundamentals.py
─────────────────────────
Daily fundamentals snapshot pipeline.

Schedule: Monday to Friday at 22:00 UTC
  - Runs 1 hour after the OHLCV DAG to avoid simultaneous extraction
    hammering Yahoo Finance rate limits
  - Independent of the OHLCV DAG — can fail and be retried separately

Task chain:
  extract_fundamentals -> load_staging -> quality_checks -> transform
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from dag_utils import default_args, sla_miss_callback
from operators.yahoo_finance_ecs_operator import YahooFinanceFundamentalsOperator
from operators.data_quality_operator import (
    DataQualityOperator,
    row_count_check,
    null_check,
    duplicate_check,
)
from yf_config import get_watchlist, get_s3_bucket, get_ecs_config, S3_PARTITION

with DAG(
    dag_id="yf_daily_fundamentals",
    description="Yahoo Finance /v10/quoteSummary -> S3 -> Redshift (daily fundamentals)",
    schedule_interval="0 22 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(retries=2),
    sla_miss_callback=sla_miss_callback,
    tags=["yahoo-finance", "fundamentals", "daily"],
) as dag:

    S3_BUCKET = get_s3_bucket()
    S3_PREFIX = "raw/fundamentals/"

    extract = YahooFinanceFundamentalsOperator(
        task_id="extract_fundamentals",
        symbols=get_watchlist(),
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        sla=timedelta(minutes=20),
        **get_ecs_config(),
    )

    load = S3ToRedshiftOperator(
        task_id="load_staging",
        schema="staging",
        table="yf_fundamentals",
        s3_bucket=S3_BUCKET,
        s3_key=f"{S3_PREFIX}{S3_PARTITION}",
        copy_options=["FORMAT AS JSON 'auto'", "GZIP", "TIMEFORMAT 'auto'"],
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_default",
    )

    quality = DataQualityOperator(
        task_id="quality_checks",
        redshift_conn_id="redshift_default",
        checks=[
            row_count_check("staging.yf_fundamentals", min_rows=1),
            null_check("staging.yf_fundamentals", "symbol"),
            null_check("staging.yf_fundamentals", "market_cap"),
            duplicate_check("staging.yf_fundamentals", ["symbol", "date"]),
            {
                "description": "PE ratio positive where not null",
                "sql": """
                    SELECT COUNT(*) FROM staging.yf_fundamentals
                    WHERE pe_ratio_ttm IS NOT NULL AND pe_ratio_ttm < 0
                """,
                "expected": 0,
            },
        ],
    )

    transform = SQLExecuteQueryOperator(
        task_id="transform_fact_fundamentals",
        sql="sql/facts/fact_fundamentals_snapshot.sql",
        conn_id="redshift_default",
    )

    extract >> load >> quality >> transform
