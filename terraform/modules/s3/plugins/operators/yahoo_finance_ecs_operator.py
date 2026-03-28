"""
yahoo_finance_ecs_operator.py
──────────────────────────────
Custom Airflow Operator that encapsulates the logic for launching ECS Fargate tasks.

Why a custom operator instead of EcsRunTaskOperator directly?
  The built-in EcsRunTaskOperator is feature-complete, but:
  1. We need to dynamically build the S3 key with execution date partitioning
  2. We need to inject Airflow context (execution_date, run_id) as ECS env vars
  3. We need to pass the S3 path downstream via XCom
  A custom operator centralises all of this logic and keeps DAG files clean.

Inheritance:
  BaseOperator
    └── YahooFinanceECSOperator  (base class — launches ECS task and polls)
          ├── YahooFinanceOHLCVOperator
          ├── YahooFinanceFundamentalsOperator
          └── YahooFinanceEarningsOperator
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import boto3
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class YahooFinanceECSOperator(BaseOperator):
    """
    Launches an ECS Fargate task to extract Yahoo Finance data,
    polls until the task completes, and returns the S3 path via XCom.

    template_fields tells Airflow to Jinja-render these fields before execution,
    so symbols can be set to "{{ ti.xcom_pull(...) }}" in a DAG.
    """

    template_fields = ("symbols", "s3_prefix")
    ui_color = "#f5a623"  # Task color in the Airflow UI

    @apply_defaults
    def __init__(
        self,
        *,
        extract_mode: str,          # "ohlcv" | "fundamentals" | "earnings"
        symbols: list[str],         # List of ticker symbols
        s3_bucket: str,
        s3_prefix: str,             # e.g. "raw/ohlcv/"
        cluster: str,               # ECS cluster ARN or name
        task_definition: str,       # ECS task definition ARN or name
        container_name: str,        # Container name (must match task definition)
        subnets: list[str],         # Private subnet IDs (required for awsvpc mode)
        security_groups: list[str],
        extra_env: dict | None = None,  # Additional environment variable overrides
        poll_interval: int = 15,        # Seconds between status checks
        max_attempts: int = 80,         # Max polls: 80 × 15s = 20 minutes
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.extract_mode    = extract_mode
        self.symbols         = symbols
        self.s3_bucket       = s3_bucket
        self.s3_prefix       = s3_prefix
        self.cluster         = cluster
        self.task_definition = task_definition
        self.container_name  = container_name
        self.subnets         = subnets
        self.security_groups = security_groups
        self.extra_env       = extra_env or {}
        self.poll_interval   = poll_interval
        self.max_attempts    = max_attempts

    def execute(self, context: dict) -> str:
        """
        Called by Airflow to run this task.

        Steps:
          1. Build the S3 partition key from execution_date
          2. Pack all parameters into ECS environment variables
          3. Call ECS RunTask API
          4. Poll until the task reaches STOPPED status
          5. Check exit code — raise AirflowException if non-zero
          6. Return the S3 path (automatically stored in XCom)
        """
        exec_date: datetime = context["execution_date"]

        # Partition S3 key by execution date for efficient Redshift COPY and data management
        s3_key = (
            f"{self.s3_prefix.rstrip('/')}/"
            f"year={exec_date.year}/"
            f"month={exec_date.month:02d}/"
            f"day={exec_date.day:02d}/"
            f"{self.extract_mode}_{context['run_id']}.ndjson"
        )

        # Build ECS container environment variables
        env = [
            {"name": "EXTRACT_MODE",    "value": self.extract_mode},
            {
                "name": "SYMBOLS",
                "value": ",".join(
                    self.symbols
                    if isinstance(self.symbols, list)
                    else [self.symbols]  # Handle string pulled from XCom
                ),
            },
            {"name": "S3_BUCKET",       "value": self.s3_bucket},
            {"name": "S3_KEY",          "value": s3_key},
            {"name": "EXECUTION_DATE",  "value": exec_date.isoformat()},
        ]
        for k, v in self.extra_env.items():
            env.append({"name": k, "value": str(v)})

        task_arn = self._run_ecs_task(env)
        self.log.info("ECS task launched: %s", task_arn)

        exit_code = self._poll_task(task_arn)
        if exit_code != 0:
            raise AirflowException(f"ECS task failed (exit={exit_code}): {task_arn}")

        s3_uri = f"s3://{self.s3_bucket}/{s3_key}.gz"
        self.log.info("Success -> %s", s3_uri)
        return s3_uri  # Stored in XCom automatically; downstream tasks can xcom_pull

    def _run_ecs_task(self, env: list[dict]) -> str:
        """Call ECS RunTask and return the task ARN."""
        ecs = boto3.client("ecs")
        resp = ecs.run_task(
            cluster=self.cluster,
            taskDefinition=self.task_definition,
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets":         self.subnets,
                    "securityGroups":  self.security_groups,
                    "assignPublicIp":  "DISABLED",  # Outbound via NAT Gateway
                }
            },
            overrides={
                "containerOverrides": [
                    {
                        "name":        self.container_name,
                        "environment": env,
                    }
                ]
            },
            tags=[
                {"key": "ManagedBy",    "value": "airflow"},
                {"key": "ExtractMode",  "value": self.extract_mode},
            ],
        )
        failures = resp.get("failures", [])
        if failures:
            raise AirflowException(f"ECS RunTask failures: {failures}")
        return resp["tasks"][0]["taskArn"]

    def _poll_task(self, task_arn: str) -> int:
        """
        Poll ECS task status every poll_interval seconds until STOPPED.

        ECS task status lifecycle:
          PROVISIONING -> PENDING -> RUNNING -> DEPROVISIONING -> STOPPED

        Exit code 0 = success, anything else = failure (container exited non-zero).
        """
        ecs = boto3.client("ecs")
        for attempt in range(self.max_attempts):
            time.sleep(self.poll_interval)
            desc = ecs.describe_tasks(cluster=self.cluster, tasks=[task_arn])
            task = desc["tasks"][0]
            status = task["lastStatus"]
            self.log.info(
                "[%d/%d] ECS status: %s", attempt + 1, self.max_attempts, status
            )

            if status == "STOPPED":
                exit_code = (task.get("containers") or [{}])[0].get("exitCode", -1)
                self.log.info(
                    "Stopped. exit=%s reason=%s",
                    exit_code,
                    task.get("stoppedReason", ""),
                )
                return exit_code

        raise AirflowException(
            f"ECS task did not finish within "
            f"{self.max_attempts * self.poll_interval}s: {task_arn}"
        )


# ── Convenience subclasses — one per extraction mode ─────────────────────────


class YahooFinanceOHLCVOperator(YahooFinanceECSOperator):
    """Extract daily or intraday OHLCV price data."""

    def __init__(self, *, interval: str = "1d", range_: str = "1d", **kwargs):
        super().__init__(
            extract_mode="ohlcv",
            extra_env={"OHLCV_INTERVAL": interval, "OHLCV_RANGE": range_},
            **kwargs,
        )


class YahooFinanceFundamentalsOperator(YahooFinanceECSOperator):
    """Extract fundamentals snapshot (PE ratio, profit margins, market cap, etc.)."""

    def __init__(self, **kwargs):
        super().__init__(extract_mode="fundamentals", **kwargs)


class YahooFinanceEarningsOperator(YahooFinanceECSOperator):
    """Extract earnings data (historical EPS surprise and forward estimates)."""

    def __init__(self, **kwargs):
        super().__init__(extract_mode="earnings", **kwargs)