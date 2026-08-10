"""Configuration loading and validation for the pipeline bootstrap."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "settings.yaml"


@dataclass(frozen=True)
class PipelineConfig:
    project_name: str
    environment: str
    api_base_url: str
    api_range_endpoint: str
    api_timeout_seconds: float
    api_connect_timeout_seconds: float
    api_read_timeout_seconds: float
    api_max_retries: int
    api_retry_backoff_seconds: float
    api_retry_status_codes: tuple[int, ...]
    start_date: date
    end_date: date
    base_currency: str
    target_currencies: tuple[str, ...]
    min_exchange_rate: float
    max_exchange_rate: float
    iceberg_catalog_name: str
    iceberg_database_name: str
    iceberg_warehouse_path: str
    iceberg_storage_type: str
    iceberg_s3_endpoint: str
    iceberg_s3_access_key_id: str
    iceberg_s3_secret_access_key: str
    iceberg_s3_path_style_access: bool
    events_path: Path

    @property
    def all_currencies(self) -> tuple[str, ...]:
        return (self.base_currency,) + tuple(c for c in self.target_currencies if c != self.base_currency)


def _coerce(raw: str, existing: Any) -> Any:
    if isinstance(existing, bool):
        return raw.lower() in {"1", "true", "yes", "on"}
    if isinstance(existing, int) and not isinstance(existing, bool):
        return int(raw)
    if isinstance(existing, float):
        return float(raw)
    if isinstance(existing, list):
        return [item.strip() for item in raw.split(",") if item.strip()]
    return raw


def _apply_environment_overrides(settings: dict[str, Any]) -> dict[str, Any]:
    result = {section: dict(values) if isinstance(values, dict) else values for section, values in settings.items()}
    for name, raw in os.environ.items():
        if not name.startswith("KONFIO_"):
            continue
        override = name[len("KONFIO_") :].lower()
        matches = [
            (section, key)
            for section, values in result.items()
            if isinstance(values, dict)
            for key in values
            if override == f"{section}_{key}"
        ]
        if len(matches) != 1:
            raise ValueError(f"Unknown configuration override: {name}")
        section, key = matches[0]
        existing = result[section][key]
        if existing is None:
            raise ValueError(f"Unknown configuration override: {name}")
        result[section][key] = _coerce(raw, existing)
    return result


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> PipelineConfig:
    """Load YAML settings, apply environment overrides, and validate them."""
    with Path(path).open("r", encoding="utf-8") as stream:
        settings = _apply_environment_overrides(yaml.safe_load(stream) or {})
    project, api, source = settings["project"], settings["api"], settings["source"]
    validation, iceberg, output = settings["validation"], settings["iceberg"], settings["output"]
    start_date, end_date = date.fromisoformat(source["start_date"]), date.fromisoformat(source["end_date"])
    base = str(source["base_currency"]).upper()
    targets = tuple(str(currency).upper() for currency in source["target_currencies"])
    _require(start_date <= end_date, "source.start_date must be on or before source.end_date")
    _require(len(targets) >= 4, "source.target_currencies must contain at least four currencies")
    _require(len(set(targets)) == len(targets), "target currencies must be unique")
    _require("MXN" in targets and "EUR" in targets, "MXN and EUR are required targets")
    _require(base not in targets, "base currency must not be a target currency")
    _require(float(validation["min_exchange_rate"]) >= 0, "minimum exchange rate cannot be negative")
    _require(float(validation["max_exchange_rate"]) > float(validation["min_exchange_rate"]), "maximum rate must exceed minimum rate")
    _require(int(api["max_retries"]) >= 0, "api.max_retries cannot be negative")
    _require(float(api["timeout_seconds"]) > 0, "api.timeout_seconds must be positive")
    _require(float(api.get("connect_timeout_seconds", api["timeout_seconds"])) > 0, "api.connect_timeout_seconds must be positive")
    _require(float(api.get("read_timeout_seconds", api["timeout_seconds"])) > 0, "api.read_timeout_seconds must be positive")
    return PipelineConfig(
        project_name=str(project["name"]), environment=str(project["environment"]),
        api_base_url=str(api["base_url"]).rstrip("/"), api_range_endpoint=str(api["range_endpoint"]),
        api_timeout_seconds=float(api["timeout_seconds"]),
        api_connect_timeout_seconds=float(api.get("connect_timeout_seconds", api["timeout_seconds"])),
        api_read_timeout_seconds=float(api.get("read_timeout_seconds", api["timeout_seconds"])),
        api_max_retries=int(api["max_retries"]),
        api_retry_backoff_seconds=float(api["retry_backoff_seconds"]),
        api_retry_status_codes=tuple(int(code) for code in api["retry_status_codes"]),
        start_date=start_date, end_date=end_date, base_currency=base, target_currencies=targets,
        min_exchange_rate=float(validation["min_exchange_rate"]), max_exchange_rate=float(validation["max_exchange_rate"]),
        iceberg_catalog_name=str(iceberg["catalog_name"]), iceberg_database_name=str(iceberg["database_name"]),
        iceberg_warehouse_path=str(iceberg["warehouse_path"]),
        iceberg_storage_type=str(iceberg.get("storage_type", "hadoop")),
        iceberg_s3_endpoint=str(iceberg.get("s3_endpoint", "")),
        iceberg_s3_access_key_id=str(iceberg.get("s3_access_key_id", "")),
        iceberg_s3_secret_access_key=str(iceberg.get("s3_secret_access_key", "")),
        iceberg_s3_path_style_access=bool(iceberg.get("s3_path_style_access", False)),
        events_path=Path(str(output["events_path"])),
    )
