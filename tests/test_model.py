"""Tests for fact/dimension modeling and CSV enrichment."""

from datetime import date, datetime

import pytest

from src.config import load_config
from src.extract import RATE_SCHEMA
from src.model import build_model
from src.transform import clean_rates, enrich_rates


def _row(rate_date, currency="MXN", rate=10.0):
    return {
        "rate_date": rate_date,
        "base_currency": "USD",
        "quote_currency": currency,
        "exchange_rate": rate,
        "source_name": "test",
        "ingestion_timestamp": datetime(2024, 1, 1),
    }


def _enriched(spark, rows):
    config = load_config()
    raw = spark.createDataFrame(rows, schema=RATE_SCHEMA)
    return enrich_rates(clean_rates(raw, config).cleaned), config


def test_model_builds_required_fact_and_dimension_with_keys_and_hash(spark):
    enriched, config = _enriched(spark, [_row(date(2024, 1, 2), rate=10.0), _row(date(2024, 1, 3), rate=11.0)])
    model = build_model(enriched, config, datetime(2024, 1, 4))

    fact = model.fact_exchange_rates.collect()
    dimension = model.dim_currency.collect()
    currencies = {row["currency_code"] for row in dimension}
    assert len(fact) == 2
    assert all(row["row_hash"] for row in fact)
    assert len({row["row_hash"] for row in fact}) == 2
    assert all(row["year"] == 2024 and row["month"] == 1 for row in fact)
    assert currencies == {"USD", "MXN", "EUR", "BRL", "COP"}
    assert {row["currency_name"] for row in dimension} == {
        "US Dollar",
        "Mexican Peso",
        "Euro",
        "Brazilian Real",
        "Colombian Peso",
    }
    assert all(row["source_name"] == "simulated_csv" for row in dimension)


def test_model_rejects_duplicate_fact_grain(spark):
    enriched, config = _enriched(spark, [_row(date(2024, 1, 2), rate=10.0)])
    duplicated = enriched.unionByName(enriched)

    with pytest.raises(ValueError, match="duplicate"):
        build_model(duplicated, config, datetime(2024, 1, 4))
