"""Phase 6/7 tests for CDC semantics and JSON event output."""

import json
from datetime import date, datetime

from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType, TimestampType

from src.cdc import build_cdc
from src.events import emit_events


CDC_SCHEMA = StructType([
    StructField("rate_date", DateType(), False),
    StructField("base_currency", StringType(), False),
    StructField("quote_currency", StringType(), False),
    StructField("exchange_rate", DoubleType(), False),
    StructField("row_hash", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),
])


def _fact(spark, rows):
    return spark.createDataFrame(rows, CDC_SCHEMA)


def test_cdc_reports_insert_and_update_only_for_changes(spark):
    existing = _fact(spark, [(date(2024, 1, 2), "USD", "MXN", 10.0, "old", datetime(2024, 1, 1), datetime(2024, 1, 1))])
    incoming = _fact(spark, [
        (date(2024, 1, 2), "USD", "MXN", 11.0, "new", datetime(2024, 1, 2), datetime(2024, 1, 2)),
        (date(2024, 1, 3), "USD", "MXN", 12.0, "inserted", datetime(2024, 1, 2), datetime(2024, 1, 2)),
    ])

    rows = {row["entity_id"]: row["operation_type"] for row in build_cdc(incoming, existing).collect()}
    assert rows == {"USD|MXN|2024-01-02": "UPDATE", "USD|MXN|2024-01-03": "INSERT"}


def test_events_are_valid_deterministic_and_one_per_change(spark, tmp_path):
    existing = _fact(spark, [(date(2024, 1, 2), "USD", "MXN", 10.0, "old", datetime(2024, 1, 1), datetime(2024, 1, 1))])
    incoming = _fact(spark, [(date(2024, 1, 2), "USD", "MXN", 11.0, "new", datetime(2024, 1, 2), datetime(2024, 1, 2))])
    cdc = build_cdc(incoming, existing)

    assert emit_events(cdc, tmp_path) == 1
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
    event = json.loads(files[0].read_text())
    assert event["event_type"] == "UPDATE"
    assert event["entity_id"] == "USD|MXN|2024-01-02"
    assert event["payload"]["current_exchange_rate"] == 11.0
    assert emit_events(cdc, tmp_path) == 1
    assert len(list(tmp_path.glob("*.json"))) == 1
    assert emit_events(build_cdc(incoming, incoming), tmp_path) == 0
