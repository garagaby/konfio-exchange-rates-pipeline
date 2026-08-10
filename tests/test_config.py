"""Configuration tests for Phase 1."""

from src.config import load_config


def test_default_configuration_is_valid():
    config = load_config()
    assert config.base_currency == "USD"
    assert {"MXN", "EUR"}.issubset(config.target_currencies)
    assert len(config.target_currencies) >= 4

