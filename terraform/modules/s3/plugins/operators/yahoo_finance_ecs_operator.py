"""
yahoo_finance_ecs_operator.py
──────────────────────────────
自定义 Airflow Operator，封装启动 ECS Fargate 任务的逻辑。

为什么要自定义而不直接用 EcsRunTaskOperator？
  Airflow 内置的 EcsRunTaskOperator 功能完整，但：
  1. 我们需要动态构建 S3 key（含执行日期分区）
  2. 需要把 Airflow 的 execution_date 等上下文传入 ECS 环境变量
  3. 需要把 S3 路径通过 XCom 传给下游任务
  自定义 Operator 让这些逻辑集中在一处，DAG 文件保持简洁。

继承关系：
  BaseOperator
    └── YahooFinanceECSOperator  （通用基类，负责启动 ECS + 轮询）
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
    启动一个 ECS Fargate 任务来提取 Yahoo Finance 数据，
    轮询直到任务完成，通过 XCom 返回 S3 路径。

    template_fields 让 Airflow 在执行前对这些字段做 Jinja 渲染，
    symbols 列表可以用 "{{ ti.xcom_pull(...) }}" 这样的模板。
    """

    template_fields = ("symbols", "s3_prefix")
    ui_color = "#f5a623"  # 在 Airflow UI 中的任务颜色

    @apply_defaults
    def __init__(
        self,
        *,
        extract_mode: str,  # "ohlcv" | "fundamentals" | "earnings"
        symbols: list[str],  # 股票代码列表
        s3_bucket: str,
        s3_prefix: str,  # e.g. "raw/ohlcv/"
        cluster: str,  # ECS cluster ARN 或名称
        task_definition: str,  # ECS task definition ARN 或名称
        container_name: str,  # 容器名（与 task definition 里一致）
        subnets: list[str],  # private subnet IDs（awsvpc 模式必须）
        security_groups: list[str],
        extra_env: dict | None = None,  # 额外的环境变量覆盖
        poll_interval: int = 15,  # 每隔多少秒查询一次任务状态
        max_attempts: int = 80,  # 最多等 80×15=20分钟
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.extract_mode = extract_mode
        self.symbols = symbols
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.cluster = cluster
        self.task_definition = task_definition
        self.container_name = container_name
        self.subnets = subnets
        self.security_groups = security_groups
        self.extra_env = extra_env or {}
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts

    def execute(self, context: dict) -> str:
        """
        Airflow 调用 execute() 来运行这个任务。

        流程：
          1. 根据 execution_date 构建 S3 分区 key
          2. 把所有参数打包成 ECS 环境变量
          3. 调用 ECS RunTask API
          4. 轮询直到 STOPPED
          5. 检查 exit code，非 0 则抛异常
          6. 返回 S3 路径（存入 XCom）
        """
        exec_date: datetime = context["execution_date"]

        # S3 key 按执行日期分区，便于 Redshift COPY 和数据管理
        s3_key = (
            f"{self.s3_prefix.rstrip('/')}/"
            f"year={exec_date.year}/"
            f"month={exec_date.month:02d}/"
            f"day={exec_date.day:02d}/"
            f"{self.extract_mode}_{context['run_id']}.ndjson"
        )

        # 组装 ECS 容器环境变量
        env = [
            {"name": "EXTRACT_MODE", "value": self.extract_mode},
            {
                "name": "SYMBOLS",
                "value": ",".join(
                    self.symbols
                    if isinstance(self.symbols, list)
                    else [self.symbols]  # 兼容 XCom 拉取的字符串
                ),
            },
            {"name": "S3_BUCKET", "value": self.s3_bucket},
            {"name": "S3_KEY", "value": s3_key},
            {"name": "EXECUTION_DATE", "value": exec_date.isoformat()},
        ]
        for k, v in self.extra_env.items():
            env.append({"name": k, "value": str(v)})

        task_arn = self._run_ecs_task(env)
        self.log.info("ECS task launched: %s", task_arn)

        exit_code = self._poll_task(task_arn)
        if exit_code != 0:
            raise AirflowException(f"ECS task failed (exit={exit_code}): {task_arn}")

        s3_uri = f"s3://{self.s3_bucket}/{s3_key}.gz"
        self.log.info("Success → %s", s3_uri)
        return s3_uri  # 自动存入 XCom，下游可用 xcom_pull 获取

    def _run_ecs_task(self, env: list[dict]) -> str:
        """调用 ECS RunTask，返回 task ARN。"""
        ecs = boto3.client("ecs")
        resp = ecs.run_task(
            cluster=self.cluster,
            taskDefinition=self.task_definition,
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": self.subnets,
                    "securityGroups": self.security_groups,
                    "assignPublicIp": "DISABLED",  # 用 NAT Gateway 出网
                }
            },
            overrides={
                "containerOverrides": [
                    {
                        "name": self.container_name,
                        "environment": env,
                    }
                ]
            },
            tags=[
                {"key": "ManagedBy", "value": "airflow"},
                {"key": "ExtractMode", "value": self.extract_mode},
            ],
        )
        failures = resp.get("failures", [])
        if failures:
            raise AirflowException(f"ECS RunTask failures: {failures}")
        return resp["tasks"][0]["taskArn"]

    def _poll_task(self, task_arn: str) -> int:
        """
        每隔 poll_interval 秒查询一次 ECS 任务状态，直到 STOPPED。

        ECS 任务状态流转：
          PROVISIONING → PENDING → RUNNING → DEPROVISIONING → STOPPED

        exit code 0 = 成功，其他 = 失败（容器以非 0 退出）
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


# ── 便捷子类，每种模式一个 ─────────────────────────────────────


class YahooFinanceOHLCVOperator(YahooFinanceECSOperator):
    """提取日线/分钟线 OHLCV 数据。"""

    def __init__(self, *, interval: str = "1d", range_: str = "1d", **kwargs):
        super().__init__(
            extract_mode="ohlcv",
            extra_env={"OHLCV_INTERVAL": interval, "OHLCV_RANGE": range_},
            **kwargs,
        )


class YahooFinanceFundamentalsOperator(YahooFinanceECSOperator):
    """提取基本面快照（PE、利润率、市值等）。"""

    def __init__(self, **kwargs):
        super().__init__(extract_mode="fundamentals", **kwargs)


class YahooFinanceEarningsOperator(YahooFinanceECSOperator):
    """提取财报数据（历史 EPS surprise + 前瞻预估）。"""

    def __init__(self, **kwargs):
        super().__init__(extract_mode="earnings", **kwargs)
