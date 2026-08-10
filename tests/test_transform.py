"""Phase 4 tests for cleaning, enrichment, aggregation, anomalies, and quality."""

from datetime import date, datetime, timedelta

from pyspark.sql.types import StructField, StructType

from src.config import load_config
from src.extract import RATE_SCHEMA, COVERAGE_SCHEMA, parse_range_payload
from src.transform import build_anomalies, build_monthly_metrics, clean_rates, enrich_rates, build_quality_report


def _config():
    return load_config()


def _raw(spark, rows):
    return spark.createDataFrame(rows, schema=RATE_SCHEMA)


def _raw_nullable(spark, rows):
    schema = StructType([StructField(field.name, field.dataType, nullable=True) for field in RATE_SCHEMA.fields])
    return spark.createDataFrame(rows, schema=schema)


def _row(rate_date, currency="MXN", rate=10.0, ingestion=1):
    return {
        "rate_date": rate_date,
        "base_currency": "USD",
        "quote_currency": currency,
        "exchange_rate": rate,
        "source_name": "test",
        "ingestion_timestamp": datetime(2024, 1, 1, 0, 0, ingestion),
    }


def test_clean_rates_rejects_invalid_rows_and_deduplicates_deterministically(spark):
    config = _config()
    rows = [
        _row(date(2024, 1, 2), rate=10.0, ingestion=1),
        _row(date(2024, 1, 2), rate=11.0, ingestion=2),
        _row(date(2024, 1, 3), rate=0.0),
        _row(date(2024, 1, 4), rate=2_000_000.0),
        {**_row(date(2024, 1, 5)), "exchange_rate": None},
    ]

    result = clean_rates(_raw_nullable(spark, rows), config)

    assert result.cleaned.count() == 1
    assert result.cleaned.first()["exchange_rate"] == 11.0
    reasons = {row["rejection_reason"] for row in result.rejected.collect()}
    assert reasons == {"duplicate_business_key", "rate_out_of_range", "null_required_field"}


def test_enrichment_uses_calendar_windows_and_nulls_first_history(spark):
    config = _config()
    rows = [_row(date(2024, 1, 1), rate=10.0), _row(date(2024, 1, 2), rate=11.0), _row(date(2024, 1, 8), rate=12.0)]
    enriched = enrich_rates(clean_rates(_raw(spark, rows), config).cleaned).orderBy("rate_date").collect()

    assert enriched[0]["daily_variation_pct"] is None
    assert round(enriched[1]["daily_variation_pct"], 6) == 10.0
    assert round(enriched[2]["moving_avg_7d"], 6) == 11.5
    assert enriched[2]["volatility_30d"] is not None


def test_monthly_metrics_include_required_summary_fields(spark):
    config = _config()
    rows = [_row(date(2024, 1, 1), rate=10.0), _row(date(2024, 1, 2), rate=12.0), _row(date(2024, 2, 1), rate=14.0)]
    monthly = build_monthly_metrics(enrich_rates(clean_rates(_raw(spark, rows), config).cleaned)).orderBy("calendar_month").collect()

    assert len(monthly) == 2
    assert monthly[0]["average_rate"] == 11.0
    assert monthly[0]["minimum_rate"] == 10.0
    assert monthly[0]["maximum_rate"] == 12.0
    assert monthly[0]["observation_count"] == 2
    assert monthly[0]["monthly_volatility"] is not None


def test_anomaly_requires_sufficient_history_and_flags_large_variation(spark):
    config = _config()
    rows = [_row(date(2024, 1, 1) + timedelta(days=index), rate=100.0 + index * 0.01) for index in range(35)]
    rows.append(_row(date(2024, 2, 5), rate=200.0))
    enriched = enrich_rates(clean_rates(_raw(spark, rows), config).cleaned)
    anomalies = build_anomalies(enriched)

    assert anomalies.count() >= 1
    assert anomalies.filter("rate_date = '2024-02-05'").count() == 1
    assert enriched.filter("rate_date = '2024-01-02'").first()["is_anomaly"] is False


def test_quality_report_contains_counts_gaps_and_dates_without_coverage(spark):
    config = _config()
    payload = {"base": "USD", "rates": {"2024-01-01": {"MXN": 10.0, "EUR": 0.9, "BRL": 4.8, "COP": 3900.0}}}
    result = parse_range_payload(payload, config, datetime(2024, 1, 7))
    raw = _raw(spark, list(result.rows))
    coverage = spark.createDataFrame(list(result.coverage_rows), schema=COVERAGE_SCHEMA)
    clean = clean_rates(raw, config)
    report = build_quality_report(raw, clean.cleaned, clean.rejected, coverage, config, datetime(2024, 1, 7))

    assert report.filter("category = 'dataset_statistics' AND metric_name = 'input_row_count'").first()["metric_value"] == 4.0
    assert report.filter("category = 'coverage_gap' AND metric_name = 'weekend_gap'").count() > 0
    assert report.filter("category = 'dates_without_coverage'").count() > 0
