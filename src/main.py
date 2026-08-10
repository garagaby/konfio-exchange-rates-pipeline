"""DAG entry point for the exchange-rate pipeline."""

from __future__ import annotations

import logging
from typing import Any

from src.config import PipelineConfig, load_config
from src.dag import PipelineDAG
from src.events import emit_events
from src.extract import extract_to_dataframes
from src.load import LoadResult, load_pipeline
from src.model import ModelResult, build_model
from src.spark_session import build_spark_session
from src.transform import TransformationResult, run_transformations


LOGGER = logging.getLogger(__name__)


def build_pipeline_dag() -> PipelineDAG:
    """Declare the complete pipeline dependency graph."""
    dag = PipelineDAG()
    dag.add_task("configuration", lambda _: load_config())
    dag.add_task("spark", lambda context: build_spark_session(context["configuration"]), ("configuration",))

    def extract(context: dict[str, Any]) -> dict[str, Any]:
        rates, coverage = extract_to_dataframes(context["spark"], context["configuration"])
        LOGGER.info("Extraction summary raw_rate_rows=%s coverage_rows=%s", rates.count(), coverage.count())
        return {"rates": rates, "coverage": coverage}

    dag.add_task("extract", extract, ("spark",))

    def transform(context: dict[str, Any]) -> TransformationResult:
        extracted = context["extract"]
        result = run_transformations(extracted["rates"], extracted["coverage"], context["configuration"])
        LOGGER.info(
            "Transformation summary cleaned=%s rejected=%s monthly_metrics=%s anomalies=%s quality_rows=%s",
            result.cleaned.count(), result.rejected.count(), result.monthly_metrics.count(),
            result.anomalies.count(), result.quality_report.count(),
        )
        return result

    dag.add_task("transform", transform, ("extract",))

    def model(context: dict[str, Any]) -> ModelResult:
        result = build_model(context["transform"].enriched, context["configuration"])
        LOGGER.info("Model summary fact_exchange_rates=%s dim_currency=%s", result.fact_exchange_rates.count(), result.dim_currency.count())
        LOGGER.info(
            "Optional payments model summary fact_transactions=%s dim_customer=%s dim_card=%s",
            result.fact_transactions.count(), result.dim_customer.count(), result.dim_card.count(),
        )
        return result

    dag.add_task("model", model, ("transform",))

    def load(context: dict[str, Any]) -> LoadResult:
        result = load_pipeline(
            context["spark"], context["model"], context["transform"], context["configuration"]
        )
        LOGGER.info(
            "Load summary cdc_rows=%s snapshot_before=%s snapshot_after=%s time_travel_rows=%s",
            result.cdc.count(), result.fact_snapshot_before, result.fact_snapshot_after, result.time_travel_row_count,
        )
        return result

    dag.add_task("load", load, ("spark", "model", "transform"))

    def events(context: dict[str, Any]) -> int:
        emitted = emit_events(context["load"].cdc, context["configuration"].events_path)
        LOGGER.info("Event summary cdc_rows=%s emitted_events=%s", context["load"].cdc.count(), emitted)
        return emitted

    dag.add_task("events", events, ("load",))

    def summary(context: dict[str, Any]) -> None:
        LOGGER.info(
            "Pipeline summary stages=%s cdc_rows=%s events=%s",
            ",".join(context.keys()), context["load"].cdc.count(), context["events"],
        )

    dag.add_task("summary", summary, ("events",))
    return dag


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    LOGGER.info("Pipeline startup")
    spark = None
    try:
        dag = build_pipeline_dag()
        context = dag.run()
        spark = context.get("spark")
        LOGGER.info("Pipeline completed cleanly")
        return 0
    except Exception:
        LOGGER.exception("Pipeline failure")
        return 1
    finally:
        if spark is not None:
            spark.stop()
            LOGGER.info("Spark session stopped")


if __name__ == "__main__":
    raise SystemExit(main())
