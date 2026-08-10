"""Analytical facts and dimensions for the exchange-rate domain."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DoubleType, StringType, StructField, StructType, TimestampType

from .config import PipelineConfig

CURRENCY_PROFILE_SCHEMA = StructType(
    [
        StructField("currency_code", StringType(), nullable=False),
        StructField("currency_name", StringType(), nullable=False),
        StructField("market_region", StringType(), nullable=False),
        StructField("market_tier", StringType(), nullable=False),
        StructField("typical_spread_bps", DoubleType(), nullable=False),
        StructField("settlement_lag_days", DoubleType(), nullable=False),
        StructField("source_name", StringType(), nullable=False),
    ]
)


@dataclass(frozen=True)
class ModelResult:
    """Analytical model outputs before Iceberg persistence."""

    fact_exchange_rates: DataFrame
    dim_currency: DataFrame
    fact_transactions: DataFrame
    dim_customer: DataFrame
    dim_card: DataFrame


@dataclass(frozen=True)
class PaymentModelResult:
    """Deterministic synthetic payments/cards model; not sourced from Frankfurter."""

    fact_transactions: DataFrame
    dim_customer: DataFrame
    dim_card: DataFrame


def validate_fact_uniqueness(fact_exchange_rates: DataFrame) -> None:
    """Fail before persistence if the documented fact grain is violated."""
    duplicate_keys = (
        fact_exchange_rates.groupBy("rate_date", "base_currency", "quote_currency")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_keys:
        raise ValueError("fact_exchange_rates contains duplicate rate_date + base_currency + quote_currency keys")


def load_currency_profile(spark: SparkSession) -> DataFrame:
    """Load the simulated CSV source with Spark."""
    path = Path(__file__).resolve().parents[1] / "data" / "simulated_currency_profile.csv"
    if not path.exists():
        raise FileNotFoundError(f"Simulated currency profile CSV not found: {path}")
    return spark.read.option("header", True).schema(CURRENCY_PROFILE_SCHEMA).csv(str(path))


def build_dim_currency(enriched: DataFrame, config: PipelineConfig, run_timestamp: datetime) -> DataFrame:
    """Build one dimension row per configured or observed ISO currency code."""
    spark = enriched.sparkSession
    configured = spark.createDataFrame([(code,) for code in config.all_currencies], ["currency_code"])
    observed = enriched.select(F.col("base_currency").alias("currency_code")).unionByName(
        enriched.select(F.col("quote_currency").alias("currency_code"))
    )
    codes = configured.unionByName(observed).distinct()
    profile = load_currency_profile(spark).dropDuplicates(["currency_code"])
    return (
        codes.join(profile, on="currency_code", how="left")
        .select(
            F.col("currency_code").cast("string"),
            F.coalesce(F.col("currency_name"), F.col("currency_code")).alias("currency_name"),
            F.col("market_region").alias("market_region"),
            F.col("market_tier").alias("market_tier"),
            F.col("typical_spread_bps").cast("double").alias("typical_spread_bps"),
            F.col("settlement_lag_days").cast("double").alias("settlement_lag_days"),
            F.lit(True).alias("is_active"),
            F.coalesce(F.col("source_name"), F.lit("configuration_and_observed_rates")).alias("source_name"),
            F.lit(run_timestamp).cast("timestamp").alias("ingestion_timestamp"),
            F.lit(run_timestamp).cast("timestamp").alias("updated_at"),
        )
    )


def build_fact_exchange_rates(enriched: DataFrame, run_timestamp: datetime) -> DataFrame:
    """Build the exchange-rate fact at one row per business key."""
    fact = (
        enriched.withColumn("year", F.year("rate_date"))
        .withColumn("month", F.month("rate_date"))
        .withColumn(
            "row_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.date_format("rate_date", "yyyy-MM-dd"),
                    F.col("base_currency"),
                    F.col("quote_currency"),
                    F.col("exchange_rate").cast("string"),
                ),
                256,
            ),
        )
        .withColumn("updated_at", F.lit(run_timestamp).cast("timestamp"))
    )
    validate_fact_uniqueness(fact)
    return fact


def build_optional_payment_model(spark, run_timestamp: datetime) -> PaymentModelResult:
    """Build a small deterministic simulated payments/cards domain."""
    customer_schema = StructType([
        StructField("customer_id", StringType(), False), StructField("customer_segment", StringType(), False),
        StructField("country_code", StringType(), False), StructField("is_active", BooleanType(), False),
        StructField("ingestion_timestamp", TimestampType(), False), StructField("updated_at", TimestampType(), False),
    ])
    customers = spark.createDataFrame([
        ("CUST-001", "small_business", "MX", True, run_timestamp, run_timestamp),
        ("CUST-002", "enterprise", "MX", True, run_timestamp, run_timestamp),
    ], customer_schema)
    card_schema = StructType([
        StructField("card_id", StringType(), False), StructField("customer_id", StringType(), False),
        StructField("card_type", StringType(), False), StructField("card_network", StringType(), False),
        StructField("last_four", StringType(), False), StructField("is_active", BooleanType(), False),
        StructField("ingestion_timestamp", TimestampType(), False), StructField("updated_at", TimestampType(), False),
    ])
    cards = spark.createDataFrame([
        ("CARD-001", "CUST-001", "credit", "visa", "1001", True, run_timestamp, run_timestamp),
        ("CARD-002", "CUST-001", "debit", "mastercard", "1002", True, run_timestamp, run_timestamp),
        ("CARD-003", "CUST-002", "credit", "visa", "1003", True, run_timestamp, run_timestamp),
    ], card_schema)
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), False), StructField("customer_id", StringType(), False),
        StructField("card_id", StringType(), False), StructField("transaction_date", DateType(), False),
        StructField("amount", DoubleType(), False), StructField("currency_code", StringType(), False),
        StructField("merchant_category", StringType(), False), StructField("status", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False), StructField("updated_at", TimestampType(), False),
    ])
    transactions = spark.createDataFrame([
        ("TX-0001", "CUST-001", "CARD-001", datetime(2024, 1, 2).date(), 1250.50, "MXN", "software", "approved", run_timestamp, run_timestamp),
        ("TX-0002", "CUST-001", "CARD-002", datetime(2024, 1, 3).date(), 89.90, "USD", "travel", "approved", run_timestamp, run_timestamp),
        ("TX-0003", "CUST-002", "CARD-003", datetime(2024, 1, 4).date(), 4200.00, "MXN", "inventory", "approved", run_timestamp, run_timestamp),
        ("TX-0004", "CUST-002", "CARD-003", datetime(2024, 1, 5).date(), 300.00, "USD", "office", "declined", run_timestamp, run_timestamp),
    ], transaction_schema)
    return PaymentModelResult(transactions, customers, cards)


def build_model(enriched: DataFrame, config: PipelineConfig, run_timestamp: datetime | None = None) -> ModelResult:
    """Build and validate the required fact/dimension analytical model."""
    timestamp = run_timestamp or datetime.now(timezone.utc).replace(tzinfo=None)
    fact = build_fact_exchange_rates(enriched, timestamp)
    dimension = build_dim_currency(enriched, config, timestamp)
    payments = build_optional_payment_model(enriched.sparkSession, timestamp)
    return ModelResult(
        fact_exchange_rates=fact,
        dim_currency=dimension,
        fact_transactions=payments.fact_transactions,
        dim_customer=payments.dim_customer,
        dim_card=payments.dim_card,
    )
