# Exchange Rates Lakehouse Pipeline

Dockerized PySpark pipeline for historical FX data ingestion, transformation, CDC comparison, Iceberg persistence, and JSON event emission.

## Overview

The pipeline consumes historical USD exchange rates from the Frankfurter API, cleans and enriches the data, builds a simple analytical model, persists curated tables in Apache Iceberg, and emits one JSON event per detected change.

## Implementation checklist

- ✅ Extracción histórica desde Frankfurter API con base `USD` y monedas `MXN`, `EUR`, `BRL`, `COP`.
- ✅ Handles weekends, holidays, missing coverage, HTTP errors, rate limiting, timeouts, and exponential-backoff retries.
- ✅ DataFrame PySpark con schema tipado.
- ✅ Limpieza: nulos, rangos válidos, duplicados y tipos de datos.
- ✅ Enriquecimiento: variación diaria, promedios móviles de 7/30 días y volatilidad móvil.
- ✅ Agregaciones mensuales por moneda: promedio, mínimo, máximo, volatilidad y observaciones.
- ✅ Detección de anomalías con comportamiento móvil de 30 días y umbral de 2 desviaciones estándar.
- ✅ Quality report: daily coverage, missing dates, weekend/holiday gaps, and general statistics.
- ✅ Incremental CDC keyed by `rate_date + base_currency + quote_currency`.
- ✅ Estrategia de comparación por hash de fila (`row_hash`).
- ✅ CDC `INSERT` y `UPDATE`, con auditoría (`ingestion_timestamp`, `updated_at`).
- ✅ Analytical model with documented facts, dimensions, grains, keys, and relationships.
- ✅ Optional payments model: `fact_transactions`, `dim_customer`, `dim_card`.
- ✅ Second source: simulated CSV joined to `dim_currency` for currency enrichment.
- ✅ Eventos JSON derivados del CDC, un evento por cambio, con tipo, timestamp, entidad y payload.
- ✅ Apache Iceberg con runtime Spark, catálogo Hadoop y tablas particionadas por año/mes.
- ✅ `MERGE INTO` para la carga incremental.
- ✅ Time travel para consultar el snapshot anterior.
- ✅ MinIO como almacenamiento S3-compatible ejecutado con Docker.
- ✅ Docker Compose ejecuta el pipeline de principio a fin sin pasos manuales.
- ✅ Logs de startup, configuración, Spark, DAG, extracción, transformación, carga, eventos y finalización/fallo.
- ✅ DAG con dependencias explícitas entre etapas.
- ✅ Tests unitarios y validación de transformaciones, CDC, eventos, modelo y configuración.
- ✅ Notebook Jupyter con análisis exploratorio de los datos generados.

## Repository layout

```text
README.md
Dockerfile
docker-compose.yml
requirements.txt
config/settings.yaml          Runtime configuration
src/main.py                   DAG entry point
src/dag.py                    Dependency-aware stage runner
src/extract.py                Frankfurter extraction
src/transform.py              Cleaning, enrichment, aggregates, quality
src/model.py                  Facts and dimensions
src/load.py                   Iceberg writes, MERGE, time travel
src/cdc.py, src/events.py     CDC comparison and JSON events
data/simulated_currency_profile.csv  Simulated CSV enrichment source
notebooks/exploratory_analysis.ipynb  Exploratory analysis of generated tables
tests/                        Unit and integration-oriented tests
events/                       Generated events (ignored by Git)
```

## Data source

- Base currency: `USD`
- Target currencies: `MXN`, `EUR`, `BRL`, `COP`
- Base URL: `https://api.frankfurter.dev`
- Supported endpoints include latest rates, date-specific rates, date ranges, and currency metadata
- Second source: `data/simulated_currency_profile.csv`, joined into the currency dimension for enrichment

The pipeline handles:

- Missing dates, including weekends and holidays
- HTTP errors
- Timeouts and retries

## Configuration

Defaults are defined in `config/settings.yaml`:

- Historical interval: `2024-01-01` to `2024-06-30`
- Iceberg catalog/database: `konfio.db`
- Warehouse location: `s3a://konfio-warehouse/` on MinIO
- Event output directory: `events/`
- HTTP timeout: 5 seconds to connect, 15 seconds to read
- Retries: up to three retries with exponential backoff

Environment overrides use `KONFIO_<SECTION>_<KEY>`, for example:

```bash
KONFIO_SOURCE_START_DATE=2024-02-01 \
KONFIO_SOURCE_END_DATE=2024-02-29 \
KONFIO_API_TIMEOUT_SECONDS=20 \
python -m src.main
```

Target currencies can be overridden with a comma-separated list such as:

```bash
KONFIO_SOURCE_TARGET_CURRENCIES=MXN,EUR,BRL,COP
```

## Analytical model

The project uses a simple exchange-rate domain model and also simulates a small payments/cards domain.

| Table | Grain | Key / relationship |
|---|---|---|
| `fact_exchange_rates` | One row per `rate_date + base_currency + quote_currency` | Business key; `base_currency` and `quote_currency` relate to `dim_currency.currency_code` |
| `dim_currency` | One row per ISO currency code | Primary key: `currency_code` |
| `monthly_exchange_rate_metrics` | One row per `base_currency + quote_currency + calendar_year + calendar_month` | Monthly summary derived from fact rows |
| `exchange_rate_anomalies` | One row per flagged exchange-rate fact record | References the fact business key and anomaly reason |
| `data_quality_report` | One or more rows per pipeline run and quality category | Report timestamp plus category/metric grain |
| `fact_transactions` | One row per simulated `transaction_id` | Foreign keys: `customer_id` -> `dim_customer`, `card_id` -> `dim_card` |
| `dim_customer` | One row per simulated `customer_id` | Primary key: `customer_id` |
| `dim_card` | One row per simulated `card_id` | Primary key: `card_id` |

The exchange-rate fact includes the rate, prior rate, daily variation, 7-day and 30-day rolling averages, volatility, anomaly flag, `row_hash`, timestamps, and `year`/`month` fields. The payments fact contains transaction amount, currency, merchant category, status, and audit fields.

## Transformations

The transformation layer performs:

- Null handling
- Rate-range validation
- Duplicate removal
- Type enforcement
- Daily variation calculation
- 7-day and 30-day moving averages
- Rolling volatility
- Monthly aggregates
- Anomaly detection using 30-day rolling behavior
- Data-quality reporting for missing coverage and summary statistics

## Storage

Curated data is written to Apache Iceberg using a Hadoop catalog backed by MinIO.

Partitioning:

- Exchange-rate facts (`fact_exchange_rates`): `year`, `month`
- Monthly metrics (`monthly_exchange_rate_metrics`): `calendar_year`, `calendar_month`
- Anomalies (`exchange_rate_anomalies`): `year`, `month`
- Quality reports (`data_quality_report`): run-based reporting fields

The fact table is updated with `MERGE INTO`. The pipeline also captures the previous snapshot when available and can query it through Iceberg time travel.

## Events

The pipeline emits one JSON file per detected CDC change in `events/`.

Each event includes:

- `event_type` (`INSERT`, `UPDATE`, `DELETE`)
- `event_timestamp`
- `entity_id`
- relevant payload fields

Kafka is not required for local execution. JSON event files are the default event output.

## DAG and execution flow

The pipeline runs as a dependency-aware DAG with explicit stage ordering:

```text
configuration -> spark -> extract -> transform -> model
                                      \-> load -> events -> summary
```

Every stage logs its start, dependencies, completion time, and failure context. Any stage failure stops the run and returns a non-zero exit code.

## Run locally

```bash
python -m src.main
pytest -q
```

## Run with Docker and MinIO

Build and run the full stack from the repository root:

```bash
docker compose up --build
```

This starts MinIO, creates the warehouse bucket, runs the Spark pipeline, writes Iceberg tables to MinIO, and emits JSON events under `events/`.

## Open the exploratory notebook

For a complete interview demonstration script covering architecture, live logs, data modeling, CDC/idempotency, tests, and the notebook, see [PRESENTATION_GUIDE.md](PRESENTATION_GUIDE.md).

The visual slide deck is available as [presentation.html](presentation.html). It uses Cytoscape.js for the architecture, data-model, and DAG diagrams. Open it in a browser with internet access and use the arrow keys, Page Up/Page Down, Home, and End to navigate.

The Jupyter notebook runs as an optional Docker Compose profile after the pipeline completes. Start it with:

```bash
docker compose --profile notebook up --build
```

Open [http://localhost:8888](http://localhost:8888) without a token. Select `exploratory_analysis.ipynb` and run the cells from top to bottom. The notebook reads the generated Iceberg tables from MinIO; it does not require manual shell steps inside the container.

Stop the notebook and its services with:

```bash
docker compose --profile notebook down
```

To run the test suite inside Docker:

```bash
docker compose run --rm --no-deps -v "$PWD/tests:/app/tests:ro" pipeline pytest -q /app/tests
```
