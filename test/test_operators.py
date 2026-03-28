"""
test_operators.py
──────────────────
Unit tests for terraform/modules/s3/plugins/operators/

Stubs out Airflow internals so tests run without a live Airflow database.
"""
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import pytest

# ── Stub Airflow modules ──────────────────────────────────────
# Allows importing operators without a running Airflow installation

for mod in [
    "airflow", "airflow.models", "airflow.exceptions",
    "airflow.utils", "airflow.utils.decorators",
    "airflow.providers", "airflow.providers.amazon",
    "airflow.providers.amazon.aws",
    "airflow.providers.amazon.aws.hooks",
    "airflow.providers.amazon.aws.hooks.redshift_sql",
]:
    if mod not in sys.modules:
        sys.modules[mod] = types.ModuleType(mod)


class _BaseOperator:
    def __init__(self, **kwargs):
        pass


class _AirflowException(Exception):
    pass


def _apply_defaults(fn):
    return fn


sys.modules["airflow.models"].BaseOperator       = _BaseOperator
sys.modules["airflow.exceptions"].AirflowException = _AirflowException
sys.modules["airflow.utils.decorators"].apply_defaults = _apply_defaults

sys.path.insert(0, "terraform/modules/s3/plugins")
from operators.data_quality_operator import (
    DataQualityOperator,
    row_count_check,
    null_check,
    duplicate_check,
    freshness_check,
)


# ── DataQualityOperator tests ─────────────────────────────────

class TestDataQualityOperator(unittest.TestCase):

    def _make_operator(self, checks) -> DataQualityOperator:
        op = DataQualityOperator.__new__(DataQualityOperator)
        op.checks           = checks
        op.redshift_conn_id = "redshift_default"
        return op

    def _mock_hook(self, return_values: list):
        hook = MagicMock()
        hook.get_records.side_effect = return_values
        return hook

    def test_passes_when_all_checks_pass(self):
        op   = self._make_operator([
            row_count_check("staging.yf_ohlcv", min_rows=1),
            null_check("staging.yf_ohlcv", "symbol"),
        ])
        hook = self._mock_hook([[[100]], [[0]]])

        with patch("operators.data_quality_operator.RedshiftSQLHook", return_value=hook):
            op.execute({})  # should not raise

    def test_raises_when_row_count_is_zero(self):
        op   = self._make_operator([row_count_check("staging.yf_ohlcv", min_rows=1)])
        hook = self._mock_hook([[[0]]])

        with patch("operators.data_quality_operator.RedshiftSQLHook", return_value=hook):
            with pytest.raises(_AirflowException, match="quality check"):
                op.execute({})

    def test_raises_when_nulls_found(self):
        op   = self._make_operator([null_check("staging.yf_ohlcv", "symbol")])
        hook = self._mock_hook([[[5]]])  # 5 nulls found

        with patch("operators.data_quality_operator.RedshiftSQLHook", return_value=hook):
            with pytest.raises(_AirflowException):
                op.execute({})

    def test_raises_when_query_returns_no_rows(self):
        op   = self._make_operator([null_check("staging.yf_ohlcv", "symbol")])
        hook = self._mock_hook([[[]]])  # empty result

        with patch("operators.data_quality_operator.RedshiftSQLHook", return_value=hook):
            with pytest.raises(_AirflowException):
                op.execute({})

    def test_callable_expected_passes(self):
        op   = self._make_operator([{
            "description": "at least 10 symbols",
            "sql":         "SELECT COUNT(DISTINCT symbol) FROM staging.yf_ohlcv",
            "expected":    lambda n: n >= 10,
        }])
        hook = self._mock_hook([[[15]]])

        with patch("operators.data_quality_operator.RedshiftSQLHook", return_value=hook):
            op.execute({})  # should not raise

    def test_callable_expected_fails(self):
        op   = self._make_operator([{
            "description": "at least 10 symbols",
            "sql":         "SELECT COUNT(DISTINCT symbol) FROM staging.yf_ohlcv",
            "expected":    lambda n: n >= 10,
        }])
        hook = self._mock_hook([[[5]]])  # only 5 symbols

        with patch("operators.data_quality_operator.RedshiftSQLHook", return_value=hook):
            with pytest.raises(_AirflowException):
                op.execute({})

    def test_multiple_failures_reported_together(self):
        op   = self._make_operator([
            null_check("staging.yf_ohlcv", "symbol"),
            null_check("staging.yf_ohlcv", "close"),
        ])
        hook = self._mock_hook([[[3]], [[7]]])  # both fail

        with patch("operators.data_quality_operator.RedshiftSQLHook", return_value=hook):
            with pytest.raises(_AirflowException) as exc_info:
                op.execute({})
        assert "2 quality check" in str(exc_info.value)


# ── Check factory tests ───────────────────────────────────────

class TestCheckFactories(unittest.TestCase):

    def test_row_count_check_passes_above_min(self):
        check = row_count_check("staging.yf_ohlcv", min_rows=1)
        assert check["expected"](100) is True
        assert check["expected"](1)   is True
        assert check["expected"](0)   is False

    def test_null_check_structure(self):
        check = null_check("staging.yf_ohlcv", "close")
        assert check["expected"]         == 0
        assert "staging.yf_ohlcv" in check["sql"]
        assert "close"            in check["sql"]
        assert "IS NULL"          in check["sql"]

    def test_duplicate_check_structure(self):
        check = duplicate_check("staging.yf_ohlcv", ["symbol", "date"])
        assert check["expected"]         == 0
        assert "symbol"           in check["sql"]
        assert "date"             in check["sql"]
        assert "HAVING COUNT(*)"  in check["sql"]

    def test_freshness_check_structure(self):
        check = freshness_check("staging.yf_ohlcv", "_extracted_at", max_age_hours=6)
        assert check["expected"]          == 1
        assert "_extracted_at"    in check["sql"]
        assert "-6"               in check["sql"]


if __name__ == "__main__":
    unittest.main()
