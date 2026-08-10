"""Phase 3 tests for Frankfurter extraction and coverage handling."""

from datetime import date, datetime

import pytest
import requests

from src.config import load_config
from src.extract import ExtractionError, ExtractionResult, FrankfurterClient, extract_to_dataframes, parse_range_payload


class FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, outcomes):
        self.outcomes = iter(outcomes)
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append((url, params, timeout))
        outcome = next(self.outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _config():
    return load_config()


def _payload():
    return {
        "base": "USD",
        "start_date": "2024-01-01",
        "end_date": "2024-01-03",
        "rates": {
            "2024-01-01": {"MXN": 17.1, "EUR": 0.91, "BRL": 4.8, "COP": 3900.0},
            "2024-01-02": {"MXN": 17.0, "EUR": 0.92},
        },
    }


def test_range_payload_normalizes_one_row_per_currency_and_records_gaps():
    config = _config()
    result = parse_range_payload(_payload(), config, datetime(2024, 1, 4, 12, 0, 0))

    assert isinstance(result, ExtractionResult)
    assert len(result.rows) == 6
    assert result.rows[0]["base_currency"] == "USD"
    assert result.rows[0]["source_name"] == "frankfurter"
    assert result.rows[0]["ingestion_timestamp"] == datetime(2024, 1, 4, 12, 0, 0)
    statuses = {row["coverage_status"] for row in result.coverage_rows if row["rate_date"] == date(2024, 1, 1)}
    assert statuses == {"observed" if currency in {"MXN", "EUR", "BRL", "COP"} else "missing_business_day" for currency in config.target_currencies}


def test_no_data_response_creates_explicit_weekday_and_weekend_gaps():
    config = _config()
    result = parse_range_payload({"base": "USD", "rates": {}}, config)

    assert result.rows == ()
    assert any(row["coverage_status"] == "weekend_gap" for row in result.coverage_rows)
    assert any(row["coverage_status"] == "missing_business_day" for row in result.coverage_rows)


def test_http_request_uses_range_params_and_explicit_connect_read_timeout():
    config = _config()
    session = FakeSession([FakeResponse(_payload())])
    result = FrankfurterClient(config, session=session).fetch_range()

    assert len(result.rows) == 6
    url, params, timeout = session.calls[0]
    assert url.endswith("/v1/2024-01-01..2024-06-30")
    assert params == {"base": "USD", "symbols": "MXN,EUR,BRL,COP"}
    assert timeout == (5.0, 15.0)


def test_timeout_is_retried_with_exponential_backoff_then_succeeds():
    config = _config()
    session = FakeSession([requests.Timeout("connect"), requests.Timeout("read"), FakeResponse(_payload())])
    delays = []
    result = FrankfurterClient(config, session=session, sleep=delays.append).fetch_range()

    assert len(result.rows) == 6
    assert delays == [1.0, 2.0]
    assert len(session.calls) == 3


def test_rate_limit_is_retried_and_http_failure_exhaustion_is_actionable():
    config = _config()
    session = FakeSession([FakeResponse({}, 429), FakeResponse({}, 429), FakeResponse({}, 429), FakeResponse({}, 429)])
    with pytest.raises(ExtractionError, match=r"failed after 4 attempt\(s\)"):
        FrankfurterClient(config, session=session, sleep=lambda _: None).fetch_range()
    assert len(session.calls) == 4


def test_malformed_payload_fails_without_retrying():
    config = _config()
    session = FakeSession([FakeResponse({"base": "USD", "rates": []})])
    with pytest.raises(ExtractionError, match="malformed"):
        FrankfurterClient(config, session=session).fetch_range()
    assert len(session.calls) == 1


def test_typed_spark_dataframes_include_empty_schema(spark):
    config = _config()
    client = FrankfurterClient(config, session=FakeSession([FakeResponse({"base": "USD", "rates": {}})]))
    rates, coverage = extract_to_dataframes(spark, config, client)

    assert rates.schema.fieldNames() == [
        "rate_date", "base_currency", "quote_currency", "exchange_rate", "source_name", "ingestion_timestamp"
    ]
    assert rates.count() == 0
    expected_dates = (config.end_date - config.start_date).days + 1
    assert coverage.count() == expected_dates * len(config.target_currencies)
    assert coverage.filter("coverage_status = 'weekend_gap'").count() > 0
