"""PySpark cleaning, enrichment, aggregation, anomaly, and quality logic."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import PipelineConfig

LOGGER = logging.getLogger(__name__)

REQUIRED_RATE_COLUMNS = {
    "rate_date",
    "base_currency",
    "quote_currency",
    "exchange_rate",
    "source_name",
    "ingestion_timestamp",
}

REJECTED_SCHEMA = StructType(
    [
        StructField("rate_date", DateType(), nullable=True),
        StructField("base_currency", StringType(), nullable=True),
        StructField("quote_currency", StringType(), nullable=True),
        StructField("exchange_rate", DoubleType(), nullable=True),
        StructField("source_name", StringType(), nullable=True),
        StructField("ingestion_timestamp", TimestampType(), nullable=True),
        StructField("rejection_reason", StringType(), nullable=False),
    ]
)

QUALITY_SCHEMA = StructType(
    [
        StructField("report_timestamp", TimestampType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("metric_name", StringType(), nullable=False),
        StructField("metric_value", DoubleType(), nullable=True),
        StructField("rate_date", DateType(), nullable=True),
        StructField("quote_currency", StringType(), nullable=True),
        StructField("is_weekend", BooleanType(), nullable=True),
        StructField("details", StringType(), nullable=True),
    ]
)


@dataclass(frozen=True)
class CleanResult:
    cleaned: DataFrame
    rejected: DataFrame


@dataclass(frozen=True)
class TransformationResult:
    cleaned: DataFrame
    rejected: DataFrame
    enriched: DataFrame
    monthly_metrics: DataFrame
    anomalies: DataFrame
    quality_report: DataFrame


def _normalized_rates(rates: DataFrame) -> DataFrame:
    missing = REQUIRED_RATE_COLUMNS.difference(rates.columns)
    if missing:
        raise ValueError(f"Raw rate DataFrame is missing required columns: {sorted(missing)}")
    return rates.select(
        F.to_date("rate_date").alias("rate_date"),
        F.upper(F.col("base_currency").cast("string")).alias("base_currency"),
        F.upper(F.col("quote_currency").cast("string")).alias("quote_currency"),
        F.col("exchange_rate").cast("double").alias("exchange_rate"),
        F.col("source_name").cast("string").alias("source_name"),
        F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
    )


def clean_rates(rates: DataFrame, config: PipelineConfig) -> CleanResult:
    """Normalize types, reject invalid rows, and deterministically deduplicate keys."""
    normalized = _normalized_rates(rates)
    required_null = (
        F.col("rate_date").isNull()
        | F.col("base_currency").isNull()
        | F.col("quote_currency").isNull()
        | F.col("exchange_rate").isNull()
    )
    invalid_base = F.col("base_currency") != F.lit(config.base_currency)
    invalid_currency = ~F.col("quote_currency").isin(list(config.target_currencies))
    invalid_rate = (
        F.isnan(F.col("exchange_rate"))
        | F.col("exchange_rate").isNull()
        | (F.col("exchange_rate") <= F.lit(config.min_exchange_rate))
        | (F.col("exchange_rate") > F.lit(config.max_exchange_rate))
    )
    reason = (
        F.when(required_null, F.lit("null_required_field"))
        .when(invalid_base, F.lit("unexpected_base_currency"))
        .when(invalid_currency, F.lit("unexpected_quote_currency"))
        .when(invalid_rate, F.lit("rate_out_of_range"))
    )
    marked = normalized.withColumn("initial_rejection_reason", reason)
    key_window = Window.partitionBy("rate_date", "base_currency", "quote_currency").orderBy(
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col("source_name").asc_nulls_last(),
        F.col("exchange_rate").desc_nulls_last(),
    )
    marked = marked.withColumn("key_row_number", F.row_number().over(key_window))
    marked = marked.withColumn(
        "rejection_reason",
        F.when(F.col("initial_rejection_reason").isNotNull(), F.col("initial_rejection_reason"))
        .when(F.col("key_row_number") > 1, F.lit("duplicate_business_key")),
    )
    rejected = marked.filter(F.col("rejection_reason").isNotNull()).select(
        "rate_date", "base_currency", "quote_currency", "exchange_rate", "source_name", "ingestion_timestamp", "rejection_reason"
    )
    cleaned = marked.filter(F.col("rejection_reason").isNull()).select(
        "rate_date", "base_currency", "quote_currency", "exchange_rate", "source_name", "ingestion_timestamp"
    )
    return CleanResult(cleaned=cleaned, rejected=rejected)


def enrich_rates(cleaned: DataFrame) -> DataFrame:
    """Add observed-rate and daily-variation rolling metrics with Spark windows."""
    partition = Window.partitionBy("base_currency", "quote_currency").orderBy("rate_date")
    day_index = F.datediff(F.col("rate_date"), F.lit("1970-01-01"))
    calendar_window = Window.partitionBy("base_currency", "quote_currency").orderBy(day_index)
    seven_days = calendar_window.rangeBetween(-6, 0)
    thirty_days = calendar_window.rangeBetween(-29, 0)
    previous_rate = F.lag("exchange_rate").over(partition)
    enriched = (
        cleaned.withColumn("previous_exchange_rate", previous_rate)
        .withColumn(
            "daily_variation_pct",
            F.when(
                F.col("previous_exchange_rate").isNull() | (F.col("previous_exchange_rate") == 0),
                F.lit(None).cast("double"),
            ).otherwise((F.col("exchange_rate") - F.col("previous_exchange_rate")) / F.col("previous_exchange_rate") * 100.0),
        )
        .withColumn("moving_avg_7d", F.avg("exchange_rate").over(seven_days))
        .withColumn("moving_avg_30d", F.avg("exchange_rate").over(thirty_days))
        .withColumn("volatility_30d", F.stddev_samp("exchange_rate").over(thirty_days))
        .withColumn("variation_mean_30d", F.avg("daily_variation_pct").over(thirty_days))
        .withColumn("variation_stddev_30d", F.stddev_samp("daily_variation_pct").over(thirty_days))
    )
    variation_count = F.count("daily_variation_pct").over(thirty_days)
    return enriched.withColumn(
        "is_anomaly",
        F.when(
            (variation_count >= 30)
            & F.col("daily_variation_pct").isNotNull()
            & F.col("variation_mean_30d").isNotNull()
            & F.col("variation_stddev_30d").isNotNull()
            & (
                F.abs(F.col("daily_variation_pct") - F.col("variation_mean_30d"))
                > 2.0 * F.col("variation_stddev_30d")
            ),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )


def build_monthly_metrics(enriched: DataFrame) -> DataFrame:
    """Aggregate one row per base/quote currency and calendar month."""
    return enriched.withColumn("calendar_year", F.year("rate_date")).withColumn("calendar_month", F.month("rate_date")).groupBy(
        "base_currency", "quote_currency", "calendar_year", "calendar_month"
    ).agg(
        F.avg("exchange_rate").alias("average_rate"),
        F.min("exchange_rate").alias("minimum_rate"),
        F.max("exchange_rate").alias("maximum_rate"),
        F.stddev_samp("exchange_rate").alias("monthly_volatility"),
        F.count(F.lit(1)).cast("long").alias("observation_count"),
    )


def build_anomalies(enriched: DataFrame) -> DataFrame:
    """Return only sufficiently-historied rows classified as anomalies."""
    return enriched.filter(F.col("is_anomaly")).withColumn("anomaly_reason", F.lit("daily_variation_over_2_rolling_stddev"))


def _metric_rows(spark, timestamp: datetime, metrics: list[tuple[str, float, str]]) -> DataFrame:
    return spark.createDataFrame(
        [(timestamp, "dataset_statistics", name, value, None, None, None, details) for name, value, details in metrics],
        schema=QUALITY_SCHEMA,
    )


def build_quality_report(
    raw_rates: DataFrame,
    cleaned: DataFrame,
    rejected: DataFrame,
    coverage: DataFrame,
    config: PipelineConfig,
    run_timestamp: datetime | None = None,
) -> DataFrame:
    """Build structured row-level coverage and run-level quality outcomes."""
    timestamp = run_timestamp or datetime.now(timezone.utc).replace(tzinfo=None)
    spark = raw_rates.sparkSession
    input_count = float(raw_rates.count())
    valid_count = float(cleaned.count())
    rejected_count = float(rejected.count())
    duplicate_count = float(rejected.filter(F.col("rejection_reason") == "duplicate_business_key").count())
    summary = _metric_rows(
        spark,
        timestamp,
        [
            ("input_row_count", input_count, f"interval={config.start_date}..{config.end_date}"),
            ("valid_row_count", valid_count, None),
            ("duplicate_row_count", duplicate_count, None),
            ("rejected_row_count", rejected_count, None),
            ("output_row_count", valid_count, None),
        ],
    )
    coverage_rows = coverage.select(
        F.lit(timestamp).cast("timestamp").alias("report_timestamp"),
        F.when(F.col("missing_data"), F.lit("coverage_gap")).otherwise(F.lit("coverage")).alias("category"),
        F.col("coverage_status").alias("metric_name"),
        F.lit(1.0).alias("metric_value"),
        "rate_date",
        "quote_currency",
        "is_weekend",
        F.lit("one row per date and configured quote currency").alias("details"),
    )
    date_coverage = coverage.groupBy("rate_date").agg(F.sum(F.col("observed_rate_count")).alias("observed_count")).filter(F.col("observed_count") == 0).select(
        F.lit(timestamp).cast("timestamp").alias("report_timestamp"),
        F.lit("dates_without_coverage").alias("category"),
        F.lit("date_without_any_observation").alias("metric_name"),
        F.lit(1.0).alias("metric_value"),
        "rate_date",
        F.lit(None).cast("string").alias("quote_currency"),
        F.lit(None).cast("boolean").alias("is_weekend"),
        F.lit("all configured currencies missing for date").alias("details"),
    )
    return summary.unionByName(coverage_rows).unionByName(date_coverage)


def run_transformations(raw_rates: DataFrame, coverage: DataFrame, config: PipelineConfig) -> TransformationResult:
    """Execute all Phase 4 layers and return traceable DataFrames."""
    cleaned_result = clean_rates(raw_rates, config)
    enriched = enrich_rates(cleaned_result.cleaned)
    monthly = build_monthly_metrics(enriched)
    anomalies = build_anomalies(enriched)
    quality = build_quality_report(raw_rates, cleaned_result.cleaned, cleaned_result.rejected, coverage, config)
    LOGGER.info(
        "Transformations completed cleaned=%s rejected=%s monthly=%s anomalies=%s",
        cleaned_result.cleaned.count(), cleaned_result.rejected.count(), monthly.count(), anomalies.count(),
    )
    return TransformationResult(cleaned_result.cleaned, cleaned_result.rejected, enriched, monthly, anomalies, quality)
