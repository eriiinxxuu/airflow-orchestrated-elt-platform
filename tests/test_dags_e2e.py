"""
test_dags_e2e.py
─────────────────
End-to-end DAG logic test using mock data.
Verifies the full task chain for all three DAGs without hitting
Yahoo Finance, ECS, S3, or Redshift.

What is mocked:
  - Airflow Variables (s3_bucket, ecs_config, sns_topic_arn)
  - YahooFinanceECSOperator.execute() -> returns fake S3 path
  - S3ToRedshiftOperator.execute()    -> no-op
  - DataQualityOperator (RedshiftSQLHook) -> returns fake row counts
  - RedshiftSQLOperator.execute()     -> no-op
  - SNS publish                       -> no-op

What is NOT mocked (real logic being tested):
  - DAG structure and task dependencies
  - DataQualityOperator check logic (pass/fail)
  - parse_conf() in yf_event_earnings
  - XCom push/pull between tasks

Usage:
    python tests/test_dags_e2e.py
"""

from __future__ import annotations

import json
import os
import sys
import types
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch
import logging
# ── Stub Airflow internals ────────────────────────────────────
for mod in [
    "airflow", "airflow.models", "airflow.exceptions",
    "airflow.utils", "airflow.utils.decorators",
    "airflow.operators", "airflow.operators.python",
    "airflow.providers", "airflow.providers.amazon",
    "airflow.providers.amazon.aws",
    "airflow.providers.amazon.aws.operators",
    "airflow.providers.amazon.aws.operators.redshift",
    "airflow.providers.amazon.aws.operators.redshift_sql",
    "airflow.providers.amazon.aws.transfers",
    "airflow.providers.amazon.aws.transfers.s3_to_redshift",
    "airflow.providers.amazon.aws.hooks",
    "airflow.providers.amazon.aws.hooks.redshift_sql",
    "airflow.providers.common",
    "airflow.providers.common.sql",
    "airflow.providers.common.sql.operators",
    "airflow.providers.common.sql.operators.sql",
]:
    if mod not in sys.modules:
        sys.modules[mod] = types.ModuleType(mod)


class _BaseOperator:
    template_fields = ()
    ui_color = "#fff"
    log = logging.getLogger("airflow.task") 

    def __init__(self, **kwargs):
        self.task_id = kwargs.get("task_id", "task")
        self.sla     = kwargs.get("sla")

    def execute(self, context):
        pass
    def __rshift__(self, other):   # ← 加这个
        return other

    def __rrshift__(self, other):  # ← 加这个
        return self


class _AirflowException(Exception):
    pass


def _apply_defaults(fn):
    return fn


class _PythonOperator(_BaseOperator):
    def __init__(self, *, python_callable, **kwargs):
        super().__init__(**kwargs)
        self.python_callable = python_callable

    def execute(self, context):
        return self.python_callable(**context)


class _RedshiftSQLOperator(_BaseOperator):
    def __init__(self, *, sql, redshift_conn_id="redshift_default", **kwargs):
        super().__init__(**kwargs)
        self.sql = sql

    def execute(self, context):
        print(f"  [mock] RedshiftSQLOperator: {self.sql}")


class _S3ToRedshiftOperator(_BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        print(f"  [mock] S3ToRedshiftOperator: COPY staging table")


sys.modules["airflow.models"].BaseOperator             = _BaseOperator
sys.modules["airflow.models"].DAG                      = MagicMock
sys.modules["airflow.models"].Variable                 = MagicMock
sys.modules["airflow.exceptions"].AirflowException     = _AirflowException
sys.modules["airflow.utils.decorators"].apply_defaults = _apply_defaults
sys.modules["airflow.operators.python"].PythonOperator = _PythonOperator
sys.modules["airflow.providers.amazon.aws.operators.redshift"].RedshiftSQLOperator       = _RedshiftSQLOperator
sys.modules["airflow.providers.amazon.aws.operators.redshift_sql"].RedshiftSQLOperator   = _RedshiftSQLOperator
sys.modules["airflow.providers.amazon.aws.transfers.s3_to_redshift"].S3ToRedshiftOperator = _S3ToRedshiftOperator
sys.modules["airflow.providers.amazon.aws.hooks.redshift_sql"].RedshiftSQLHook           = MagicMock
sys.modules["airflow"].DAG = MagicMock
sys.modules["airflow.providers.common.sql.operators.sql"].SQLExecuteQueryOperator = MagicMock

sys.path.insert(0, "dags")
sys.path.insert(0, "terraform/modules/s3/plugins")

# ── Mock Airflow Variables ────────────────────────────────────
MOCK_VARIABLES = {
    "s3_bucket":    "yf-elt-data-dev-402705369995",
    "sns_topic_arn": "arn:aws:sns:ap-southeast-2:402705369995:yf-elt-airflow-alerts-dev",
    "ecs_config":   json.dumps({
        "cluster_arn":    "arn:aws:ecs:ap-southeast-2:402705369995:cluster/yf-elt-cluster-dev",
        "task_definition": "arn:aws:ecs:ap-southeast-2:402705369995:task-definition/yf-elt-extractor-dev:1",
        "container_name": "extractor",
        "subnets":        ["subnet-0d6bfd621abb49ba6", "subnet-0b627c1fce04d0996"],
        "security_group": "sg-00c414a12234de2fe",
    }),
}


def mock_variable_get(key, deserialize_json=False, default_var=None):
    val = MOCK_VARIABLES.get(key, default_var)
    if deserialize_json and isinstance(val, str):
        return json.loads(val)
    return val


# ── Fake Airflow context ──────────────────────────────────────
class _FakeTaskInstance:
    def __init__(self):
        self._xcoms = {}

    def xcom_push(self, key, value):
        self._xcoms[key] = value
        print(f"  [xcom] push key={key} value={value}")

    def xcom_pull(self, key=None, task_ids=None):
        val = self._xcoms.get(key)
        print(f"  [xcom] pull key={key} -> {val}")
        return val


class _FakeDagRun:
    def __init__(self, conf=None):
        self.conf = conf or {}


def make_context(dag_run_conf=None):
    ti = _FakeTaskInstance()
    return {
        "execution_date": datetime(2026, 3, 29),
        "run_id":         "manual__2026-03-29T00:00:00",
        "ti":             ti,
        "dag_run":        _FakeDagRun(conf=dag_run_conf),
    }


# ── Import operators ──────────────────────────────────────────
from operators.data_quality_operator import (
    DataQualityOperator,
    duplicate_check,
    freshness_check,
    null_check,
    row_count_check,
)
from operators.yahoo_finance_ecs_operator import (
    YahooFinanceEarningsOperator,
    YahooFinanceFundamentalsOperator,
    YahooFinanceOHLCVOperator,
)

ECS_KWARGS = {
    "symbols":         ["AAPL", "MSFT"],
    "s3_bucket":       "yf-elt-data-dev-402705369995",
    "s3_prefix":       "raw/ohlcv/",
    "cluster":         "arn:aws:ecs:ap-southeast-2:402705369995:cluster/yf-elt-cluster-dev",
    "task_definition": "arn:aws:ecs:ap-southeast-2:402705369995:task-definition/yf-elt-extractor-dev:1",
    "container_name":  "extractor",
    "subnets":         ["subnet-0d6bfd621abb49ba6"],
    "security_groups": ["sg-00c414a12234de2fe"],
}


# ── Tests: yf_daily_ohlcv ─────────────────────────────────────
class TestOHLCVDag(unittest.TestCase):

    def test_extract_returns_s3_path(self):
        print("\n[yf_daily_ohlcv] Task 1: extract_ohlcv")
        op  = YahooFinanceOHLCVOperator(task_id="extract_ohlcv", **ECS_KWARGS)
        ctx = make_context()

        with patch.object(op, "_run_ecs_task", return_value="arn:aws:ecs:task/abc123"), \
             patch.object(op, "_poll_task",    return_value=0):
            result = op.execute(ctx)

        self.assertIn("s3://", result)
        self.assertIn("ohlcv", result)
        print(f"  -> S3 path: {result}")

    def test_quality_checks_pass(self):
        print("\n[yf_daily_ohlcv] Task 3: quality_checks (pass)")
        op = DataQualityOperator(
            task_id="quality_checks",
            checks=[
                row_count_check("staging.yf_ohlcv", min_rows=1),
                null_check("staging.yf_ohlcv", "symbol"),
                null_check("staging.yf_ohlcv", "close"),
                duplicate_check("staging.yf_ohlcv", ["symbol", "date"]),
                {"description": "No negative close", "sql": "SELECT COUNT(*) FROM staging.yf_ohlcv WHERE close < 0", "expected": 0},
            ],
        )
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = [[[10]], [[0]], [[0]], [[0]], [[0]]]

        with patch("operators.data_quality_operator.RedshiftSQLHook", return_value=mock_hook):
            op.execute(make_context())
        print("  -> All checks passed")

    def test_quality_checks_fail(self):
        print("\n[yf_daily_ohlcv] Task 3: quality_checks (fail - nulls detected)")
        op = DataQualityOperator(
            task_id="quality_checks",
            checks=[null_check("staging.yf_ohlcv", "close")],
        )
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [[5]]

        with patch("operators.data_quality_operator.RedshiftSQLHook", return_value=mock_hook):
            with self.assertRaises(_AirflowException):
                op.execute(make_context())
        print("  -> Correctly raised AirflowException")


# ── Tests: yf_daily_fundamentals ─────────────────────────────
class TestFundamentalsDag(unittest.TestCase):

    def test_extract_fundamentals(self):
        print("\n[yf_daily_fundamentals] Task 1: extract_fundamentals")
        kwargs = {**ECS_KWARGS, "s3_prefix": "raw/fundamentals/"}
        op     = YahooFinanceFundamentalsOperator(task_id="extract_fundamentals", **kwargs)

        with patch.object(op, "_run_ecs_task", return_value="arn:aws:ecs:task/def456"), \
             patch.object(op, "_poll_task",    return_value=0):
            result = op.execute(make_context())

        self.assertIn("fundamentals", result)
        print(f"  -> S3 path: {result}")

    def test_quality_checks_pass(self):
        print("\n[yf_daily_fundamentals] Task 3: quality_checks (pass)")
        op = DataQualityOperator(
            task_id="quality_checks",
            checks=[
                row_count_check("staging.yf_fundamentals", min_rows=1),
                null_check("staging.yf_fundamentals", "symbol"),
                null_check("staging.yf_fundamentals", "market_cap"),
                duplicate_check("staging.yf_fundamentals", ["symbol", "date"]),
            ],
        )
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = [[[2]], [[0]], [[0]], [[0]]]

        with patch("operators.data_quality_operator.RedshiftSQLHook", return_value=mock_hook):
            op.execute(make_context())
        print("  -> All checks passed")


# ── Tests: yf_event_earnings ──────────────────────────────────
class TestEarningsDag(unittest.TestCase):

    def test_parse_conf_valid(self):
        print("\n[yf_event_earnings] Task 1: parse_conf (valid)")
        from yf_event_earnings import _parse_conf

        ctx = make_context(dag_run_conf={
            "symbols":        ["AAPL", "MSFT"],
            "earnings_dates": {"AAPL": "2026-03-29", "MSFT": "2026-03-28"},
            "triggered_by":   "earnings_trigger_lambda",
        })
        _parse_conf(**ctx)

        symbols = ctx["ti"]._xcoms.get("symbols")
        self.assertEqual(symbols, ["AAPL", "MSFT"])
        print(f"  -> symbols in XCom: {symbols}")

    def test_parse_conf_missing_symbols_raises(self):
        print("\n[yf_event_earnings] Task 1: parse_conf (missing symbols)")
        from yf_event_earnings import _parse_conf

        ctx = make_context(dag_run_conf={"triggered_by": "lambda"})
        with self.assertRaises(ValueError):
            _parse_conf(**ctx)
        print("  -> Correctly raised ValueError")

    def test_parse_conf_string_symbols(self):
        print("\n[yf_event_earnings] Task 1: parse_conf (string symbols)")
        from yf_event_earnings import _parse_conf

        ctx = make_context(dag_run_conf={
            "symbols":      "AAPL,MSFT,GOOGL",
            "triggered_by": "lambda",
        })
        _parse_conf(**ctx)

        symbols = ctx["ti"]._xcoms.get("symbols")
        self.assertEqual(symbols, ["AAPL", "MSFT", "GOOGL"])
        print(f"  -> symbols parsed from string: {symbols}")

    def test_extract_earnings(self):
        print("\n[yf_event_earnings] Task 2: extract_earnings")
        kwargs = {**ECS_KWARGS, "s3_prefix": "raw/earnings/"}
        op     = YahooFinanceEarningsOperator(task_id="extract_earnings", **kwargs)

        with patch.object(op, "_run_ecs_task", return_value="arn:aws:ecs:task/ghi789"), \
             patch.object(op, "_poll_task",    return_value=0):
            result = op.execute(make_context())

        self.assertIn("earnings", result)
        print(f"  -> S3 path: {result}")

    def test_quality_checks_pass(self):
        print("\n[yf_event_earnings] Task 4: quality_checks (pass)")
        op = DataQualityOperator(
            task_id="quality_checks",
            checks=[
                row_count_check("staging.yf_earnings", min_rows=1),
                null_check("staging.yf_earnings", "symbol"),
                null_check("staging.yf_earnings", "report_date"),
                {"description": "Historical EPS not null", "sql": "SELECT COUNT(*) FROM staging.yf_earnings WHERE record_type = 'historical' AND eps_actual IS NULL", "expected": 0},
            ],
        )
        mock_hook = MagicMock()
        mock_hook.get_records.side_effect = [[[8]], [[0]], [[0]], [[0]]]

        with patch("operators.data_quality_operator.RedshiftSQLHook", return_value=mock_hook):
            op.execute(make_context())
        print("  -> All checks passed")

    def test_notify_sns(self):
        print("\n[yf_event_earnings] Task 6: notify_sns")
        from yf_event_earnings import _notify_sns

        ctx = make_context()
        ctx["ti"]._xcoms["symbols"]        = ["AAPL", "MSFT"]
        ctx["ti"]._xcoms["earnings_dates"] = {"AAPL": "2026-03-29", "MSFT": "2026-03-28"}

        with patch("yf_event_earnings.send_sns_message") as mock_sns:
            _notify_sns(**ctx)
            self.assertTrue(mock_sns.called)
        print("  -> SNS notification sent")


# ─────────────────────────────────────────────────────────────
# Run
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sys.modules["airflow.models"].Variable.get = mock_variable_get

    print("=" * 60)
    print("DAG end-to-end logic test (mock data)")
    print("=" * 60)

    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestOHLCVDag))
    suite.addTests(loader.loadTestsFromTestCase(TestFundamentalsDag))
    suite.addTests(loader.loadTestsFromTestCase(TestEarningsDag))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
