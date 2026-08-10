"""Spark and Apache Iceberg runtime initialization."""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import SparkSession

from .config import PipelineConfig

LOGGER = logging.getLogger(__name__)
ICEBERG_RUNTIME_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
ICEBERG_AWS_PACKAGE = "org.apache.iceberg:iceberg-aws-bundle:1.6.1"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


def build_spark_session(config: PipelineConfig) -> SparkSession:
    """Build a Spark session configured with an Iceberg Hadoop catalog on MinIO."""
    warehouse = config.iceberg_warehouse_path
    if not warehouse.startswith(("s3://", "s3a://", "http://", "https://")):
        Path(warehouse).mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.appName(config.project_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{config.iceberg_catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{config.iceberg_catalog_name}.type", "hadoop")
        .config(f"spark.sql.catalog.{config.iceberg_catalog_name}.warehouse", warehouse)
        .config("spark.jars.packages", f"{ICEBERG_RUNTIME_PACKAGE},{ICEBERG_AWS_PACKAGE},{HADOOP_AWS_PACKAGE}")
    )
    if config.iceberg_storage_type == "minio":
        prefix = f"spark.sql.catalog.{config.iceberg_catalog_name}"
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", config.iceberg_s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", config.iceberg_s3_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", config.iceberg_s3_secret_access_key)
            .config("spark.hadoop.fs.s3a.path.style.access", str(config.iceberg_s3_path_style_access).lower())
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )
    spark = builder.getOrCreate()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.iceberg_catalog_name}.{config.iceberg_database_name}")
    LOGGER.info("Spark version=%s", spark.version)
    LOGGER.info("Iceberg packages=%s,%s,%s", ICEBERG_RUNTIME_PACKAGE, ICEBERG_AWS_PACKAGE, HADOOP_AWS_PACKAGE)
    LOGGER.info("Iceberg catalog=%s type=hadoop storage=%s warehouse=%s", config.iceberg_catalog_name, config.iceberg_storage_type, warehouse)
    LOGGER.info("Iceberg namespace=%s.%s", config.iceberg_catalog_name, config.iceberg_database_name)
    return spark
