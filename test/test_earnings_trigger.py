"""
test_earnings_trigger.py
─────────────────────────
Unit tests for terraform/modules/lambda/earnings_trigger.py

All external calls (Yahoo Finance, MWAA, boto3) are mocked.
"""
import json
import os
import sys
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

os.environ.update({
    "AIRFLOW_BASE_URL": "https://airflow.example.com",
    "AIRFLOW_DAG_ID":   "yf_event_earnings",
    "MWAA_ENV_NAME":    "yf-elt-airflow-dev",
    "WATCHLIST":        '["AAPL","MSFT"]',
    "DAYS_AHEAD":       "1",
})

sys.path.insert(0, "terraform/modules/lambda")
import earnings_trigger as et


# ── _check_earnings_date tests ────────────────────────────────

class TestCheckEarningsDate(unittest.TestCase):

    def _make_opener(self, earnings_timestamp: int):
        mock_opener = MagicMock()
        mock_opener.open.return_value.read.return_value = json.dumps({
            "quoteSummary": {"result": [{
                "calendarEvents": {
                    "earnings": {"earningsDate": [earnings_timestamp]}
                }
            }]}
        }).encode()
        return mock_opener

    def test_match_returns_iso_date_string(self):
        opener = self._make_opener(1705276800)  # 2024-01-15 UTC
        result = et._check_earnings_date(
            "AAPL", opener, "crumb",
            target_dates={date(2024, 1, 15)}
        )
        assert result == "2024-01-15"

    def test_no_match_returns_none(self):
        opener = self._make_opener(1705276800)  # 2024-01-15
        result = et._check_earnings_date(
            "AAPL", opener, "crumb",
            target_dates={date(2024, 1, 20)}    # different date
        )
        assert result is None

    def test_network_error_returns_none(self):
        mock_opener = MagicMock()
        mock_opener.open.side_effect = Exception("Connection refused")
        result = et._check_earnings_date(
            "AAPL", mock_opener, "crumb",
            target_dates={date(2024, 1, 15)}
        )
        assert result is None

    def test_empty_earnings_date_returns_none(self):
        mock_opener = MagicMock()
        mock_opener.open.return_value.read.return_value = json.dumps({
            "quoteSummary": {"result": [{
                "calendarEvents": {"earnings": {"earningsDate": []}}
            }]}
        }).encode()
        result = et._check_earnings_date(
            "AAPL", mock_opener, "crumb",
            target_dates={date(2024, 1, 15)}
        )
        assert result is None

    def test_handles_dict_format_timestamp(self):
        """Yahoo sometimes returns {"raw": <ts>} instead of plain int."""
        mock_opener = MagicMock()
        mock_opener.open.return_value.read.return_value = json.dumps({
            "quoteSummary": {"result": [{
                "calendarEvents": {
                    "earnings": {"earningsDate": [{"raw": 1705276800, "fmt": "2024-01-15"}]}
                }
            }]}
        }).encode()
        result = et._check_earnings_date(
            "AAPL", mock_opener, "crumb",
            target_dates={date(2024, 1, 15)}
        )
        assert result == "2024-01-15"


# ── _get_mwaa_token tests ─────────────────────────────────────

class TestGetMWAAToken(unittest.TestCase):

    def test_returns_web_token(self):
        mock_mwaa = MagicMock()
        mock_mwaa.create_web_login_token.return_value = {"WebToken": "test-token-123"}
        with patch("boto3.client", return_value=mock_mwaa):
            token = et._get_mwaa_token()
        assert token == "test-token-123"
        mock_mwaa.create_web_login_token.assert_called_once_with(Name="yf-elt-airflow-dev")


# ── _trigger_dag tests ────────────────────────────────────────

class TestTriggerDag(unittest.TestCase):

    def test_posts_correct_payload(self):
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.read.return_value = json.dumps({
                "dag_run_id": "earnings_20240115T060000",
                "state":      "queued",
            }).encode()

            et._trigger_dag(
                symbols=["AAPL"],
                earnings_dates={"AAPL": "2024-01-15"},
                token="test-token",
            )

        req = mock_urlopen.call_args[0][0]
        assert "yf_event_earnings" in req.full_url
        assert "dagRuns" in req.full_url
        assert "Bearer test-token" in req.get_header("Authorization")

        body = json.loads(req.data)
        assert body["conf"]["symbols"]                == ["AAPL"]
        assert body["conf"]["earnings_dates"]["AAPL"] == "2024-01-15"
        assert body["conf"]["triggered_by"]           == "earnings_trigger_lambda"

    def test_raises_on_http_error(self):
        import urllib.error
        with patch("urllib.request.urlopen",
                   side_effect=urllib.error.HTTPError(None, 403, "Forbidden", {}, None)):
            with pytest.raises(RuntimeError, match="403"):
                et._trigger_dag(["AAPL"], {"AAPL": "2024-01-15"}, "bad-token")


# ── handler tests ─────────────────────────────────────────────

class TestHandler(unittest.TestCase):

    def test_no_earnings_does_not_trigger_dag(self):
        with patch.object(et, "_build_opener", return_value=(MagicMock(), "crumb")):
            with patch.object(et, "_check_earnings_date", return_value=None):
                result = et.handler({}, MagicMock())
        assert result["triggered"] is False
        assert result["symbols"]   == []

    def test_found_earnings_triggers_dag(self):
        with patch.object(et, "_build_opener", return_value=(MagicMock(), "crumb")):
            with patch.object(et, "_check_earnings_date",
                              side_effect=["2024-01-15", None]):
                with patch.object(et, "_get_mwaa_token", return_value="token"):
                    with patch.object(et, "_trigger_dag") as mock_trigger:
                        result = et.handler({}, MagicMock())

        assert result["triggered"]       is True
        assert "AAPL" in result["symbols"]
        assert "MSFT" not in result["symbols"]
        mock_trigger.assert_called_once()

    def test_all_symbols_have_earnings(self):
        with patch.object(et, "_build_opener", return_value=(MagicMock(), "crumb")):
            with patch.object(et, "_check_earnings_date",
                              side_effect=["2024-01-15", "2024-01-15"]):
                with patch.object(et, "_get_mwaa_token", return_value="token"):
                    with patch.object(et, "_trigger_dag") as mock_trigger:
                        result = et.handler({}, MagicMock())

        assert len(result["symbols"]) == 2
        mock_trigger.assert_called_once()


if __name__ == "__main__":
    unittest.main()
