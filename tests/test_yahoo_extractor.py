"""
test_yahoo_extractor.py
────────────────────────
Unit tests for docker/yahoo_extractor.py

All external calls (Yahoo Finance API, S3) are mocked —
no real network or AWS credentials needed.
"""
import gzip
import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import pytest

# Set required env vars before importing the module
os.environ.update({
    "EXTRACT_MODE":   "ohlcv",
    "SYMBOLS":        "AAPL,MSFT",
    "S3_BUCKET":      "test-bucket",
    "S3_KEY":         "raw/ohlcv/test.ndjson",
    "EXECUTION_DATE": "2024-01-15T21:00:00+00:00",
    "OHLCV_INTERVAL": "1d",
    "OHLCV_RANGE":    "1d",
    "LOG_LEVEL":      "WARNING",
})

sys.path.insert(0, "docker")
import yahoo_extractor as ext


# ── Fixtures ──────────────────────────────────────────────────

MOCK_OHLCV = {
    "chart": {
        "result": [{
            "meta": {"currency": "USD", "exchangeName": "NASDAQ"},
            "timestamp": [1705330200, 1705416600],
            "events": {},
            "indicators": {
                "quote": [{
                    "open":   [182.15, 183.92],
                    "high":   [184.26, 185.55],
                    "low":    [180.63, 182.11],
                    "close":  [183.92, 185.04],
                    "volume": [65432100, 71234500],
                }],
                "adjclose": [{"adjclose": [183.92, 185.04]}],
            },
        }],
        "error": None,
    }
}

MOCK_FUNDAMENTALS = {
    "quoteSummary": {
        "result": [{
            "assetProfile":         {"sector": "Technology", "industry": "Consumer Electronics",
                                     "country": "United States", "fullTimeEmployees": 164000},
            "summaryDetail":        {"trailingPE": 29.5, "forwardPE": 27.1,
                                     "dividendYield": 0.0054, "beta": 1.28,
                                     "fiftyTwoWeekHigh": 199.62, "fiftyTwoWeekLow": 124.17},
            "financialData":        {"profitMargins": 0.2531, "returnOnEquity": 1.4696,
                                     "totalRevenue": 394328006656, "revenueGrowth": 0.071},
            "defaultKeyStatistics": {"trailingEps": 6.13, "forwardEps": 7.00,
                                     "enterpriseValue": 2850000000000, "priceToBook": 46.5,
                                     "sharesOutstanding": 15552799744},
            "price":                {"longName": "Apple Inc.", "exchangeName": "NASDAQ",
                                     "currency": "USD", "regularMarketPrice": 189.84,
                                     "regularMarketVolume": 65432100,
                                     "marketCap": 2950000000000},
            "earningsTrend":        {"trend": []},
        }],
        "error": None,
    }
}


# ── OHLCV tests ───────────────────────────────────────────────

class TestExtractOHLCV(unittest.TestCase):

    def _cfg(self):
        cfg = ext.Config.__new__(ext.Config)
        cfg.OHLCV_INTERVAL = "1d"
        cfg.OHLCV_RANGE    = "1d"
        cfg.EXECUTION_DATE = "2024-01-15T21:00:00+00:00"
        return cfg

    def test_parses_two_rows(self):
        with patch.object(ext, "_get", return_value=MOCK_OHLCV):
            records = ext.extract_ohlcv("AAPL", self._cfg(), MagicMock(), "crumb")
        assert len(records) == 2

    def test_correct_fields(self):
        with patch.object(ext, "_get", return_value=MOCK_OHLCV):
            r = ext.extract_ohlcv("AAPL", self._cfg(), MagicMock(), "crumb")[0]
        assert r["symbol"]   == "AAPL"
        assert r["currency"] == "USD"
        assert r["exchange"] == "NASDAQ"
        assert r["interval"] == "1d"
        assert isinstance(r["open"],   float)
        assert isinstance(r["volume"], int)

    def test_adj_close_populated(self):
        with patch.object(ext, "_get", return_value=MOCK_OHLCV):
            records = ext.extract_ohlcv("AAPL", self._cfg(), MagicMock(), "crumb")
        assert all(r["adj_close"] is not None for r in records)

    def test_empty_result_returns_empty_list(self):
        empty = {"chart": {"result": [], "error": None}}
        with patch.object(ext, "_get", return_value=empty):
            records = ext.extract_ohlcv("AAPL", self._cfg(), MagicMock(), "crumb")
        assert records == []

    def test_high_is_highest_value(self):
        with patch.object(ext, "_get", return_value=MOCK_OHLCV):
            records = ext.extract_ohlcv("AAPL", self._cfg(), MagicMock(), "crumb")
        for r in records:
            assert r["high"] >= r["low"]
            assert r["high"] >= r["open"]
            assert r["high"] >= r["close"]


# ── Fundamentals tests ────────────────────────────────────────

class TestExtractFundamentals(unittest.TestCase):

    def _cfg(self):
        cfg = ext.Config.__new__(ext.Config)
        cfg.EXECUTION_DATE = "2024-01-15T21:00:00+00:00"
        return cfg

    def test_returns_one_record(self):
        with patch.object(ext, "_get", return_value=MOCK_FUNDAMENTALS):
            records = ext.extract_fundamentals("AAPL", self._cfg(), MagicMock(), "crumb")
        assert len(records) == 1

    def test_key_fields(self):
        with patch.object(ext, "_get", return_value=MOCK_FUNDAMENTALS):
            r = ext.extract_fundamentals("AAPL", self._cfg(), MagicMock(), "crumb")[0]
        assert r["company_name"] == "Apple Inc."
        assert r["sector"]       == "Technology"
        assert r["pe_ratio_ttm"] == pytest.approx(29.5)
        assert r["market_cap"]   == pytest.approx(2950000000000)
        assert r["date"]         == "2024-01-15"

    def test_missing_modules_returns_none_fields(self):
        """Any missing module should produce None fields, not raise an exception."""
        sparse = {"quoteSummary": {"result": [{
            "assetProfile": {}, "summaryDetail": {}, "financialData": {},
            "defaultKeyStatistics": {}, "price": {}, "earningsTrend": {"trend": []},
        }], "error": None}}
        with patch.object(ext, "_get", return_value=sparse):
            records = ext.extract_fundamentals("TEST", self._cfg(), MagicMock(), "crumb")
        assert len(records) == 1
        assert records[0]["pe_ratio_ttm"] is None
        assert records[0]["market_cap"]   is None


# ── Helper function tests ─────────────────────────────────────

class TestHelpers(unittest.TestCase):

    def test_f_handles_nan(self):
        assert ext._f(float("nan")) is None
        assert ext._f(None)         is None
        assert ext._f("1.23")       == pytest.approx(1.23)
        assert ext._f(0)            == 0.0

    def test_f_handles_non_numeric(self):
        assert ext._f("hello") is None
        assert ext._f([])      is None

    def test_i_coercion(self):
        assert ext._i("42")  == 42
        assert ext._i(None)  is None
        assert ext._i("abc") is None
        assert ext._i(3.9)   == 3

    def test_ts_converts_unix_to_utc(self):
        result = ext._ts(1705330200)
        assert "2024" in result
        assert "+00:00" in result

    def test_ts_handles_none(self):
        assert ext._ts(None)    is None
        assert ext._ts("hello") is None


# ── S3 write tests ────────────────────────────────────────────

class TestWriteToS3(unittest.TestCase):

    def _cfg(self):
        cfg = ext.Config.__new__(ext.Config)
        cfg.S3_BUCKET    = "test-bucket"
        cfg.S3_KEY       = "raw/ohlcv/run.ndjson"
        cfg.EXTRACT_MODE = "ohlcv"
        cfg.SYMBOLS      = ["AAPL", "MSFT"]
        cfg.ENVIRONMENT  = "test"
        return cfg

    def test_writes_valid_gzip_ndjson(self):
        records = [{"symbol": "AAPL", "close": 189.84},
                   {"symbol": "MSFT", "close": 420.55}]
        mock_s3 = MagicMock()
        with patch("boto3.client", return_value=mock_s3):
            ext.write_to_s3(records, self._cfg())

        kw = mock_s3.put_object.call_args.kwargs
        assert kw["Bucket"]          == "test-bucket"
        assert kw["ContentEncoding"] == "gzip"
        assert kw["Metadata"]["record-count"] == "2"

        lines = gzip.decompress(kw["Body"]).decode().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["symbol"] == "AAPL"

    def test_appends_gz_suffix(self):
        mock_s3 = MagicMock()
        with patch("boto3.client", return_value=mock_s3):
            ext.write_to_s3([], self._cfg())
        assert mock_s3.put_object.call_args.kwargs["Key"].endswith(".gz")

    def test_empty_records_still_writes(self):
        mock_s3 = MagicMock()
        with patch("boto3.client", return_value=mock_s3):
            ext.write_to_s3([], self._cfg())
        mock_s3.put_object.assert_called_once()


if __name__ == "__main__":
    unittest.main()
