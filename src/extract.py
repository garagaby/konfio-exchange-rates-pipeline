"""Frankfurter API extraction and typed Spark conversion."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import PipelineConfig

LOGGER = logging.getLogger(__name__)

RATE_SCHEMA = StructType(
    [
        StructField("rate_date", DateType(), nullable=False),
        StructField("base_currency", StringType(), nullable=False),
        StructField("quote_currency", StringType(), nullable=False),
        StructField("exchange_rate", DoubleType(), nullable=False),
        StructField("source_name", StringType(), nullable=False),
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
    ]
)

COVERAGE_SCHEMA = StructType(
    [
        StructField("rate_date", DateType(), nullable=False),
        StructField("quote_currency", StringType(), nullable=False),
        StructField("is_weekend", BooleanType(), nullable=False),
        StructField("expected_business_day", BooleanType(), nullable=False),
        StructField("observed_rate_count", IntegerType(), nullable=False),
        StructField("missing_data", BooleanType(), nullable=False),
        StructField("coverage_status", StringType(), nullable=False),
    ]
)


class ExtractionError(RuntimeError):
    """Raised when the API cannot produce a valid extraction response."""


@dataclass(frozen=True)
class ExtractionResult:
    """Normalized rates plus explicit date/currency coverage outcomes."""

    rows: tuple[dict[str, Any], ...]
    coverage_rows: tuple[dict[str, Any], ...]


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _request_timeout(config: PipelineConfig) -> tuple[float, float]:
    connect = getattr(config, "api_connect_timeout_seconds", config.api_timeout_seconds)
    read = getattr(config, "api_read_timeout_seconds", config.api_timeout_seconds)
    return float(connect), float(read)


def parse_range_payload(
    payload: Mapping[str, Any],
    config: PipelineConfig,
    ingestion_timestamp: datetime | None = None,
) -> ExtractionResult:
    """Validate and normalize Frankfurter's nested range response."""
    if not isinstance(payload, Mapping) or not isinstance(payload.get("rates"), Mapping):
        raise ExtractionError("Frankfurter response is malformed: expected an object with a rates object")

    base = str(payload.get("base", config.base_currency)).upper()
    if base != config.base_currency:
        raise ExtractionError(f"Frankfurter response base {base!r} does not match configured base {config.base_currency!r}")

    timestamp = ingestion_timestamp or datetime.now(timezone.utc).replace(tzinfo=None)
    target_set = set(config.target_currencies)
    rows: list[dict[str, Any]] = []
    observed: dict[tuple[date, str], int] = {}

    for raw_date, raw_rates in payload["rates"].items():
        try:
            rate_date = date.fromisoformat(str(raw_date))
        except ValueError as exc:
            raise ExtractionError(f"Frankfurter response contains invalid rate date {raw_date!r}") from exc
        if not isinstance(raw_rates, Mapping):
            raise ExtractionError(f"Frankfurter rates for {raw_date!r} must be an object")
        for currency in target_set:
            if currency not in raw_rates:
                continue
            try:
                value = float(raw_rates[currency])
            except (TypeError, ValueError) as exc:
                raise ExtractionError(f"Frankfurter rate for {raw_date} {currency} is not numeric") from exc
            if value != value or value in (float("inf"), float("-inf")):
                raise ExtractionError(f"Frankfurter rate for {raw_date} {currency} is not finite")
            rows.append(
                {
                    "rate_date": rate_date,
                    "base_currency": base,
                    "quote_currency": currency,
                    "exchange_rate": value,
                    "source_name": "frankfurter",
                    "ingestion_timestamp": timestamp,
                }
            )
            observed[(rate_date, currency)] = observed.get((rate_date, currency), 0) + 1

    coverage: list[dict[str, Any]] = []
    for current in _date_range(config.start_date, config.end_date):
        weekend = current.weekday() >= 5
        for currency in config.target_currencies:
            count = observed.get((current, currency), 0)
            missing = count == 0
            coverage.append(
                {
                    "rate_date": current,
                    "quote_currency": currency,
                    "is_weekend": weekend,
                    "expected_business_day": not weekend,
                    "observed_rate_count": count,
                    "missing_data": missing,
                    "coverage_status": "weekend_gap" if missing and weekend else "missing_business_day" if missing else "observed",
                }
            )
    return ExtractionResult(tuple(rows), tuple(coverage))


class FrankfurterClient:
    """Small retrying client for the Frankfurter historical range endpoint."""

    def __init__(
        self,
        config: PipelineConfig,
        session: requests.Session | Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.sleep = sleep

    def fetch_range(self) -> ExtractionResult:
        endpoint = self.config.api_range_endpoint.format(
            start_date=self.config.start_date.isoformat(), end_date=self.config.end_date.isoformat()
        )
        url = f"{self.config.api_base_url}{endpoint}"
        params = {"base": self.config.base_currency, "symbols": ",".join(self.config.target_currencies)}
        max_attempts = self.config.api_max_retries + 1
        timeout = _request_timeout(self.config)
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                LOGGER.info("Frankfurter request attempt=%s/%s endpoint=%s params=%s", attempt, max_attempts, endpoint, params)
                response = self.session.get(url, params=params, timeout=timeout)
                status = getattr(response, "status_code", None)
                if status in self.config.api_retry_status_codes:
                    raise requests.HTTPError(f"retryable HTTP status {status}", response=response)
                response.raise_for_status()
                return parse_range_payload(response.json(), self.config)
            except (requests.RequestException, ValueError, ExtractionError) as exc:
                last_error = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                retryable = isinstance(exc, ExtractionError) is False and (
                    isinstance(exc, (requests.Timeout, requests.ConnectionError))
                    or status in self.config.api_retry_status_codes
                )
                LOGGER.warning(
                    "Frankfurter request failed attempt=%s/%s status=%s error=%s retryable=%s",
                    attempt, max_attempts, status, type(exc).__name__, retryable,
                )
                if not retryable or attempt == max_attempts:
                    break
                delay = self.config.api_retry_backoff_seconds * (2 ** (attempt - 1))
                self.sleep(delay)

        raise ExtractionError(
            f"Frankfurter extraction failed after {max_attempts} attempt(s) for {endpoint}; "
            f"last_error={type(last_error).__name__}: {last_error}"
        ) from last_error


def extract_to_dataframes(spark: SparkSession, config: PipelineConfig, client: FrankfurterClient | None = None) -> tuple[DataFrame, DataFrame]:
    """Fetch rates and return typed raw-rate and coverage DataFrames."""
    result = (client or FrankfurterClient(config)).fetch_range()
    rates = spark.createDataFrame(list(result.rows), schema=RATE_SCHEMA)
    coverage = spark.createDataFrame(list(result.coverage_rows), schema=COVERAGE_SCHEMA)
    LOGGER.info("Frankfurter extraction completed rates=%s coverage_rows=%s", len(result.rows), len(result.coverage_rows))
    return rates, coverage
