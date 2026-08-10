"""Shared Spark fixture for extraction integration tests."""

import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("konfio-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
