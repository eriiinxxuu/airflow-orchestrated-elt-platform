"""
yf_event_earnings.py
──────────────────────
Event-driven earnings data pipeline.

schedule_interval=None
  - This DAG is never scheduled automatically
  - It is only triggered when the earnings_trigger Lambda calls the Airflow REST API

Trigger payload (sent by Lambda):
  POST /api/v1/dags/yf_event_earnings/dagRuns
  {
    "dag_run_id": "earnings_20240125T060012",
    "conf": {
      "symbols":        ["AAPL", "MSFT"],
      "earnings_dates": { "AAPL": "2024-01-25", "MSFT": "2024-01-24" },
      "triggered_by":   "earnings_trigger_lambda"
    }
  }

The DAG reads the conf above via context["dag_run"].conf and only processes
symbols that actually have earnings — not the entire watchlist.

Task chain:
  parse_conf -> extract_earnings -> load_staging
             -> quality_checks  -> transform -> notify_sns
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from dag_utils import default_args, sla_miss_callback, send_sns_message
from operators.yahoo_finance_ecs_operator import YahooFinanceEarningsOperator
from operators.data_quality_operator import (
    DataQualityOperator,
    row_count_check,
    null_check,
)
from yf_config import get_s3_bucket, get_ecs_config, S3_PARTITION


# ── Task function: parse Lambda conf from dag_run ─────────────
def _parse_conf(**context) -> None:
    """
    Read the conf injected by Lambda, validate it, and push values to XCom.

    Why use XCom instead of reading conf directly in downstream tasks?
    Passing data through XCom is the Airflow standard pattern — it makes
    data flow visible in the UI and easier to test.
    """
    conf = context["dag_run"].conf or {}

    symbols = conf.get("symbols")
    if not symbols:
        raise ValueError(
            "dag_run.conf['symbols'] is required. "
            "This DAG must be triggered by the earnings_trigger Lambda."
        )

    # Normalise: symbols must be a list
    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(",")]

    earnings_dates = conf.get("earnings_dates", {})
    triggered_by = conf.get("triggered_by", "unknown")

    context["ti"].xcom_push(key="symbols", value=symbols)
    context["ti"].xcom_push(key="earnings_dates", value=earnings_dates)

    import logging

    logging.getLogger(__name__).info(
        "Triggered by: %s | symbols: %s | dates: %s",
        triggered_by,
        symbols,
        earnings_dates,
    )


# ── Task function: SNS completion notification ────────────────
def _notify_sns(**context) -> None:
    symbols = context["ti"].xcom_pull(key="symbols", task_ids="parse_conf")
    earn_dates = context["ti"].xcom_pull(key="earnings_dates", task_ids="parse_conf")

    lines = [f"- {s}: {earn_dates.get(s, 'today')}" for s in symbols]
    send_sns_message(
        subject=f"[Airflow] Earnings data loaded - {len(symbols)} symbol(s)",
        message=(
            "Earnings pipeline completed successfully.\n\n"
            + "\n".join(lines)
            + f"\n\nRun ID: {context['run_id']}"
        ),
    )


# ── DAG definition ────────────────────────────────────────────
with DAG(
    dag_id="yf_event_earnings",
    description="Yahoo Finance earnings (Lambda-triggered, event-driven)",
    schedule_interval=None,  # Never scheduled — triggered by Lambda only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=3,  # Allow multiple concurrent earnings events
    default_args=default_args(retries=3),
    sla_miss_callback=sla_miss_callback,
    render_template_as_native_obj=True,  # Preserve native Python types after Jinja render
    tags=["yahoo-finance", "earnings", "event-driven"],
) as dag:

    S3_BUCKET = get_s3_bucket()
    S3_PREFIX = "raw/earnings/"

    # ── Task 1: Parse Lambda conf ─────────────────────────────
    parse_conf = PythonOperator(
        task_id="parse_conf",
        python_callable=_parse_conf,
    )

    # ── Task 2: ECS extraction ────────────────────────────────
    # symbols is pulled from XCom via Jinja — only symbols with earnings are processed
    extract = YahooFinanceEarningsOperator(
        task_id="extract_earnings",
        symbols="{{ ti.xcom_pull(key='symbols', task_ids='parse_conf') }}",
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        sla=timedelta(minutes=20),
        **get_ecs_config(),
    )

    # ── Task 3: COPY into staging ─────────────────────────────
    load = S3ToRedshiftOperator(
        task_id="load_staging",
        schema="staging",
        table="yf_earnings",
        s3_bucket=S3_BUCKET,
        s3_key=f"{S3_PREFIX}{S3_PARTITION}",
        copy_options=["FORMAT AS JSON 'auto'", "GZIP", "TIMEFORMAT 'auto'"],
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_default",
    )

    # ── Task 4: Quality checks ────────────────────────────────
    quality = DataQualityOperator(
        task_id="quality_checks",
        redshift_conn_id="redshift_default",
        checks=[
            row_count_check("staging.yf_earnings", min_rows=1),
            null_check("staging.yf_earnings", "symbol"),
            null_check("staging.yf_earnings", "report_date"),
            {
                "description": "Historical records have EPS actual",
                "sql": """
                    SELECT COUNT(*) FROM staging.yf_earnings
                    WHERE record_type = 'historical' AND eps_actual IS NULL
                """,
                "expected": 0,
            },
        ],
    )

    # ── Task 5: Transform into fact table ─────────────────────
    transform = RedshiftSQLOperator(
        task_id="transform_fact_earnings",
        sql="sql/facts/fact_earnings_surprises.sql",
        redshift_conn_id="redshift_default",
    )

    # ── Task 6: SNS completion notification ───────────────────
    notify = PythonOperator(
        task_id="notify_sns",
        python_callable=_notify_sns,
    )

    parse_conf >> extract >> load >> quality >> transform >> notify
