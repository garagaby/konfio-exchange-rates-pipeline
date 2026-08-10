"""Apache Iceberg catalog, table, merge, and time-travel operations."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .cdc import build_cdc, verify_cdc_matches_fact
from .config import PipelineConfig
from .model import ModelResult
from .transform import TransformationResult


FACT_TABLE = "fact_exchange_rates"
MONTHLY_METRICS_TABLE = "monthly_exchange_rate_metrics"
ANOMALIES_TABLE = "exchange_rate_anomalies"
QUALITY_REPORT_TABLE = "data_quality_report"


@dataclass(frozen=True)
class LoadResult:
    cdc: DataFrame
    fact_snapshot_before: int | None
    fact_snapshot_after: int | None
    time_travel_row_count: int | None


def table_name(config: PipelineConfig, name: str) -> str:
    return f"{config.iceberg_catalog_name}.{config.iceberg_database_name}.{name}"


def ensure_namespace(spark: SparkSession, config: PipelineConfig) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.iceberg_catalog_name}.{config.iceberg_database_name}")


def table_exists(spark: SparkSession, identifier: str) -> bool:
    return spark.catalog.tableExists(identifier)


def _create_table(df: DataFrame, identifier: str, partitions: list[str] | None = None) -> None:
    writer = df.limit(0).writeTo(identifier).using("iceberg")
    if partitions:
        writer = writer.partitionedBy(*partitions)
    writer.create()


def _replace_table(df: DataFrame, identifier: str, partitions: list[str] | None = None) -> None:
    writer = df.writeTo(identifier).using("iceberg")
    if partitions:
        writer = writer.partitionedBy(*partitions)
    writer.createOrReplace()


def _snapshot_id(spark: SparkSession, identifier: str) -> int | None:
    if not table_exists(spark, identifier):
        return None
    rows = spark.sql(f"SELECT snapshot_id FROM {identifier}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    return int(rows[0][0]) if rows else None


def _merge_fact(spark: SparkSession, incoming: DataFrame, identifier: str) -> tuple[int | None, int | None, int | None]:
    before = _snapshot_id(spark, identifier)
    if not table_exists(spark, identifier):
        _create_table(incoming, identifier, ["year", "month"])
        incoming.writeTo(identifier).append()
    else:
        incoming.createOrReplaceTempView("incoming_exchange_rate_fact")
        spark.sql(f"""
            MERGE INTO {identifier} AS target
            USING incoming_exchange_rate_fact AS source
            ON target.rate_date = source.rate_date
               AND target.base_currency = source.base_currency
               AND target.quote_currency = source.quote_currency
            WHEN MATCHED AND target.row_hash <> source.row_hash THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    after = _snapshot_id(spark, identifier)
    previous_count = None
    if before is not None:
        previous_count = spark.sql(f"SELECT count(*) FROM {identifier} VERSION AS OF {before}").first()[0]
    return before, after, previous_count


def load_pipeline(spark: SparkSession, model: ModelResult, transformed: TransformationResult, config: PipelineConfig) -> LoadResult:
    """Persist all Phase 6 outputs and return the pre-merge CDC/time-travel result."""
    ensure_namespace(spark, config)
    fact_identifier = table_name(config, FACT_TABLE)
    existing = spark.table(fact_identifier) if table_exists(spark, fact_identifier) else spark.createDataFrame([], model.fact_exchange_rates.schema)
    cdc = build_cdc(model.fact_exchange_rates, existing)
    before, after, previous_count = _merge_fact(spark, model.fact_exchange_rates, fact_identifier)
    verify_cdc_matches_fact(cdc, spark.table(fact_identifier))

    _replace_table(transformed.monthly_metrics, table_name(config, MONTHLY_METRICS_TABLE), ["calendar_year", "calendar_month"])
    anomalies = transformed.anomalies.withColumn("year", F.year("rate_date")).withColumn("month", F.month("rate_date"))
    _replace_table(anomalies, table_name(config, ANOMALIES_TABLE), ["year", "month"])
    _replace_table(transformed.quality_report, table_name(config, QUALITY_REPORT_TABLE))
    _replace_table(model.dim_currency, table_name(config, "dim_currency"))
    _replace_table(model.fact_transactions, table_name(config, "fact_transactions"))
    _replace_table(model.dim_customer, table_name(config, "dim_customer"))
    _replace_table(model.dim_card, table_name(config, "dim_card"))

    time_travel_count = previous_count
    return LoadResult(cdc, before, after, time_travel_count)
