"""
data_quality_operator.py
─────────────────────────
对 Redshift staging 表执行 SQL 质检，任何一条失败则抛出异常。

设计思路：
  - checks 是一个 list of dict，每条定义一个检查
  - 每条检查包含 sql（返回单个标量）和 expected（期望值或 callable）
  - 用 callable 支持范围检查（如 lambda n: n > 0）
  - 工厂函数（row_count_check 等）生成常用检查，减少 DAG 里的重复代码
"""

from __future__ import annotations

import logging
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class DataQualityOperator(BaseOperator):

    template_fields = ("checks",)
    ui_color = "#fde8d8"

    @apply_defaults
    def __init__(
        self,
        *,
        checks: list[dict],
        redshift_conn_id: str = "redshift_default",
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.checks = checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context: dict) -> None:
        hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)
        failures = []

        for check in self.checks:
            desc = check.get("description", check["sql"][:60])
            sql = check["sql"]
            expected = check["expected"]

            log.info("Check: %s", desc)
            rows = hook.get_records(sql)

            if not rows or not rows[0] or rows[0][0] is None:
                failures.append(f"FAIL [{desc}] – no result returned")
                continue

            actual = rows[0][0]
            passed = expected(actual) if callable(expected) else actual == expected

            if passed:
                log.info("PASS [%s] actual=%s", desc, actual)
            else:
                msg = f"FAIL [{desc}] expected={expected}, actual={actual}"
                log.error(msg)
                failures.append(msg)

        if failures:
            raise AirflowException(
                f"{len(failures)} quality check(s) failed:\n" + "\n".join(failures)
            )
        log.info("All %d checks passed ✓", len(self.checks))


# ── 工厂函数：生成常用检查 dict ───────────────────────────────


def row_count_check(table: str, min_rows: int = 1) -> dict:
    return {
        "description": f"{table} has >= {min_rows} rows",
        "sql": f"SELECT COUNT(*) FROM {table}",
        "expected": lambda n: n >= min_rows,
    }


def null_check(table: str, column: str) -> dict:
    return {
        "description": f"{table}.{column} has no NULLs",
        "sql": f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL",
        "expected": 0,
    }


def duplicate_check(table: str, keys: list[str]) -> dict:
    key_expr = ", ".join(keys)
    return {
        "description": f"{table} no duplicate ({key_expr})",
        "sql": f"""
            SELECT COUNT(*) FROM (
                SELECT {key_expr} FROM {table}
                GROUP BY {key_expr} HAVING COUNT(*) > 1
            )
        """,
        "expected": 0,
    }


def freshness_check(table: str, ts_col: str, max_age_hours: int = 25) -> dict:
    return {
        "description": f"{table}.{ts_col} within {max_age_hours}h",
        "sql": f"""
            SELECT CASE WHEN MAX({ts_col}) >= DATEADD(hour,-{max_age_hours},GETDATE())
                        THEN 1 ELSE 0 END
            FROM {table}
        """,
        "expected": 1,
    }
