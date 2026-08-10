"""Tests for the optional simulated payments/cards model."""

from datetime import datetime

from src.model import build_optional_payment_model


def test_optional_payment_model_has_documented_grains_and_relationships(spark):
    result = build_optional_payment_model(spark, datetime(2024, 1, 4))

    assert result.fact_transactions.count() == 4
    assert result.fact_transactions.select("transaction_id").distinct().count() == 4
    assert result.dim_customer.count() == 2
    assert result.dim_card.count() == 3
    assert result.fact_transactions.select("customer_id").distinct().join(
        result.dim_customer.select("customer_id"), "customer_id", "left_anti"
    ).count() == 0
    assert result.fact_transactions.select("card_id").distinct().join(
        result.dim_card.select("card_id"), "card_id", "left_anti"
    ).count() == 0
