"""JSON event emission from the persisted CDC result."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pyspark.sql import DataFrame


def emit_events(cdc: DataFrame, output_path: str | Path) -> int:
    """Write exactly one deterministic JSON file per CDC row."""
    directory = Path(output_path)
    directory.mkdir(parents=True, exist_ok=True)
    count = 0
    for row in cdc.orderBy("entity_id", "operation_type").collect():
        event_id = hashlib.sha256(
            f"{row.entity_id}|{row.operation_type}|{row.current_row_hash}".encode("utf-8")
        ).hexdigest()
        event = {
            "schema_version": "1.0",
            "event_id": event_id,
            "event_type": row.operation_type,
            "event_timestamp": row.updated_at.isoformat() if row.updated_at else row.ingestion_timestamp.isoformat(),
            "entity_type": "exchange_rate",
            "entity_id": row.entity_id,
            "payload": {
                "rate_date": row.rate_date.isoformat(),
                "base_currency": row.base_currency,
                "quote_currency": row.quote_currency,
                "previous_exchange_rate": row.previous_exchange_rate,
                "current_exchange_rate": row.current_exchange_rate,
                "previous_row_hash": row.previous_row_hash,
                "current_row_hash": row.current_row_hash,
            },
        }
        (directory / f"{event_id}.json").write_text(json.dumps(event, sort_keys=True) + "\n", encoding="utf-8")
        count += 1
    return count
