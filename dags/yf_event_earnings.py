"""
yf_event_earnings.py
──────────────────────
事件驱动的财报数据管道。

schedule_interval=None
  → 这个 DAG 永远不会自动调度
  → 只有 earnings_trigger Lambda 调用 Airflow REST API 才会触发

触发方式（Lambda 发送）：
  POST /api/v1/dags/yf_event_earnings/dagRuns
  {
    "dag_run_id": "earnings_20240125T060012",
    "conf": {
      "symbols": ["AAPL", "MSFT"],
      "earnings_dates": { "AAPL": "2024-01-25", "MSFT": "2024-01-24" },
      "triggered_by": "earnings_trigger_lambda"
    }
  }

DAG 通过 context["dag_run"].conf 读取上面的 conf，
只处理有财报的 symbol，而不是整个 watchlist。

任务链：
  parse_conf → extract_earnings → load_staging
             → quality_checks  → transform → notify_slack
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from dag_utils import default_args, sla_miss_callback, send_slack_message
from operators.yahoo_finance_ecs_operator import YahooFinanceEarningsOperator
from operators.data_quality_operator import (
    DataQualityOperator,
    row_count_check,
    null_check,
)
from yf_config import get_s3_bucket, get_ecs_config, S3_PARTITION


# ── Task 函数：从 dag_run.conf 解析 Lambda 传入的参数 ──────────
def _parse_conf(**context) -> None:
    """
    读取 Lambda 注入的 conf，做基本校验后存入 XCom。

    为什么用 XCom 而不是直接在后续任务里读 conf？
    让后续任务通过 XCom 拿数据是 Airflow 的标准做法，
    便于在 UI 里追踪数据流向，也便于测试。
    """
    conf = context["dag_run"].conf or {}

    symbols = conf.get("symbols")
    if not symbols:
        raise ValueError(
            "dag_run.conf['symbols'] is required. "
            "This DAG must be triggered by the earnings_trigger Lambda."
        )

    # 校验：symbols 必须是列表
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


# ── Task 函数：Slack 通知 ─────────────────────────────────────
def _notify_slack(**context) -> None:
    symbols = context["ti"].xcom_pull(key="symbols", task_ids="parse_conf")
    earn_dates = context["ti"].xcom_pull(key="earnings_dates", task_ids="parse_conf")

    lines = [f"• `{s}` – {earn_dates.get(s, 'today')}" for s in symbols]
    send_slack_message(
        ":bell: *Earnings data loaded*\n"
        + "\n".join(lines)
        + f"\nRun: `{context['run_id']}`",
        color="#36a64f",
    )


# ── DAG 定义 ─────────────────────────────────────────────────
with DAG(
    dag_id="yf_event_earnings",
    description="Yahoo Finance earnings (Lambda-triggered, event-driven)",
    schedule_interval=None,  # ← 关键：不自动调度
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=3,  # 允许同一时间处理多次财报事件
    default_args=default_args(retries=3),
    sla_miss_callback=sla_miss_callback,
    render_template_as_native_obj=True,  # 让 Jinja 渲染后保持原生 Python 类型
    tags=["yahoo-finance", "earnings", "event-driven"],
) as dag:

    S3_BUCKET = get_s3_bucket()
    S3_PREFIX = "raw/earnings/"

    # ── Task 1: 解析 Lambda conf ─────────────────────────────
    parse_conf = PythonOperator(task_id="parse_conf", python_callable=_parse_conf,)

    # ── Task 2: ECS 提取财报数据 ─────────────────────────────
    # symbols 用 Jinja 从 XCom 拉取，只处理有财报的 symbol
    extract = YahooFinanceEarningsOperator(
        task_id="extract_earnings",
        symbols="{{ ti.xcom_pull(key='symbols', task_ids='parse_conf') }}",
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        sla=timedelta(minutes=20),
        **get_ecs_config(),
    )

    # ── Task 3: COPY 到 staging ──────────────────────────────
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

    # ── Task 4: 质检 ─────────────────────────────────────────
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

    # ── Task 5: 转换到 fact 表 ──────────────────────────────
    transform = RedshiftSQLOperator(
        task_id="transform_fact_earnings",
        sql="sql/facts/fact_earnings_surprises.sql",
        redshift_conn_id="redshift_default",
    )

    # ── Task 6: Slack 汇报 ──────────────────────────────────
    notify = PythonOperator(task_id="notify_slack", python_callable=_notify_slack,)

    parse_conf >> extract >> load >> quality >> transform >> notify
