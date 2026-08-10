"""CDC comparison for the exchange-rate Iceberg fact."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CDC_COLUMNS = [
    "rate_date", "base_currency", "quote_currency", "operation_type", "entity_id",
    "previous_row_hash", "current_row_hash", "previous_exchange_rate", "current_exchange_rate",
    "ingestion_timestamp", "updated_at",
]


def build_cdc(incoming: DataFrame, existing: DataFrame) -> DataFrame:
    """Compare incoming facts to the latest persisted state before MERGE INTO."""
    keys = ["rate_date", "base_currency", "quote_currency"]
    joined = incoming.alias("i").join(existing.alias("e"), keys, "left")
    is_insert = F.col("e.rate_date").isNull()
    is_update = (~is_insert) & (
        F.col("i.row_hash") != F.col("e.row_hash")
    )
    return joined.filter(is_insert | is_update).select(
        F.col("i.rate_date"), F.col("i.base_currency"), F.col("i.quote_currency"),
        F.when(is_insert, F.lit("INSERT")).otherwise(F.lit("UPDATE")).alias("operation_type"),
        F.concat_ws("|", F.col("i.base_currency"), F.col("i.quote_currency"), F.date_format("i.rate_date", "yyyy-MM-dd")).alias("entity_id"),
        F.col("e.row_hash").alias("previous_row_hash"), F.col("i.row_hash").alias("current_row_hash"),
        F.col("e.exchange_rate").alias("previous_exchange_rate"), F.col("i.exchange_rate").alias("current_exchange_rate"),
        F.col("i.ingestion_timestamp"), F.col("i.updated_at"),
    )


def verify_cdc_matches_fact(cdc: DataFrame, fact: DataFrame) -> None:
    """Ensure every emitted CDC row has its current hash in the persisted fact."""
    mismatches = (
        cdc.alias("c").join(
            fact.alias("f"),
            (F.col("c.rate_date") == F.col("f.rate_date"))
            & (F.col("c.base_currency") == F.col("f.base_currency"))
            & (F.col("c.quote_currency") == F.col("f.quote_currency")),
            "left",
        )
        .filter(F.col("f.row_hash").isNull() | (F.col("c.current_row_hash") != F.col("f.row_hash")))
        .limit(1)
        .count()
    )
    if mismatches:
        raise ValueError("CDC and persisted fact diverged after Iceberg merge")
