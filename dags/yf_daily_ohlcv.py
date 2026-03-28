"""
yf_daily_ohlcv.py
──────────────────
每日 OHLCV 价格数据管道。

调度时间：周一到周五 21:00 UTC
  → 美股 20:00 UTC 收盘，等 1 小时让 Yahoo Finance 数据稳定

SLA：23:00 UTC 前必须完成（2小时窗口）

任务链：
  extract_ohlcv → load_staging → quality_checks → transform
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
    description="Yahoo Finance /v8/finance/chart → S3 → Redshift (daily OHLCV)",
    schedule_interval="0 21 * * 1-5",  # 周一到周五 21:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(retries=2),
    sla_miss_callback=sla_miss_callback,
    tags=["yahoo-finance", "ohlcv", "daily"],
) as dag:

    S3_BUCKET = get_s3_bucket()
    S3_PREFIX = "raw/ohlcv/"

    # ── Task 1: 启动 ECS 任务抓取 OHLCV ─────────────────────
    # YahooFinanceOHLCVOperator 内部会：
    #   1. 构建 S3 key（含日期分区）
    #   2. 把参数打包成 ECS 环境变量
    #   3. 调用 RunTask + 轮询
    #   4. 把 s3:// 路径写入 XCom
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

    # ── Task 2: 把 S3 文件 COPY 到 Redshift staging ──────────
    # S3ToRedshiftOperator 内部执行：
    #   COPY staging.yf_ohlcv FROM 's3://bucket/raw/ohlcv/year=.../...'
    #   FORMAT AS JSON 'auto'   ← 自动映射 JSON key 到列名
    #   GZIP                    ← 读取压缩文件
    #   TIMEFORMAT 'auto'       ← 自动解析时间戳格式
    load = S3ToRedshiftOperator(
        task_id="load_staging",
        schema="staging",
        table="yf_ohlcv",
        s3_bucket=S3_BUCKET,
        s3_key=f"{S3_PREFIX}{S3_PARTITION}",  # Jinja 模板，运行时渲染
        copy_options=["FORMAT AS JSON 'auto'", "GZIP", "TIMEFORMAT 'auto'"],
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_default",
    )

    # ── Task 3: 数据质检 ─────────────────────────────────────
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
                "description": "High >= Low（K线基本逻辑）",
                "sql": "SELECT COUNT(*) FROM staging.yf_ohlcv WHERE high < low",
                "expected": 0,
            },
            {
                "description": "No negative close price",
                "sql": "SELECT COUNT(*) FROM staging.yf_ohlcv WHERE close < 0",
                "expected": 0,
            },
        ],
    )

    # ── Task 4: SQL 转换 → fact 表 ───────────────────────────
    transform = RedshiftSQLOperator(
        task_id="transform_fact_daily_prices",
        sql="sql/facts/fact_daily_prices.sql",
        redshift_conn_id="redshift_default",
    )

    # 线性依赖链
    extract >> load >> quality >> transform
