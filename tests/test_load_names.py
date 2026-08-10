"""Tests for the canonical English Iceberg table names."""

from src.load import (
    ANOMALIES_TABLE,
    FACT_TABLE,
    MONTHLY_METRICS_TABLE,
    QUALITY_REPORT_TABLE,
    table_name,
)


def test_canonical_table_names_are_english_and_stable():
    names = {
        FACT_TABLE,
        MONTHLY_METRICS_TABLE,
        ANOMALIES_TABLE,
        QUALITY_REPORT_TABLE,
    }
    assert names == {
        "fact_exchange_rates",
        "monthly_exchange_rate_metrics",
        "exchange_rate_anomalies",
        "data_quality_report",
    }
    assert not any(any(token in name for token in ("tipos", "cambio", "metricas", "anomalias", "reporte")) for name in names)

