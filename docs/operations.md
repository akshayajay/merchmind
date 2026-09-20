# Local operations

## Start, inspect, stop

```bash
make compose-check
make retail-demo
make retail-status
make retail-logs        # Airflow startup/login information; keep credentials local.
make retail-stop       # Preserve all named volumes.
```

The full stack combines `docker-compose.yml`, `docker-compose.airflow.yml`, and
`docker-compose.inventory.yml`. Use all three files when inspecting or restarting it:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml \
  -f docker-compose.inventory.yml logs --tail 80 spark inventory-spark refresh stock-refresh
```

Airflow standalone is a local demonstration deployment. The DAG begins paused; unpause
it in the UI to enable midnight UTC closes. A manual historical run can supply
`{"business_date":"2025-12-30"}`. Use a distinct Compose project for isolated experiments,
and avoid port conflicts with another local stack.

## Services and durable state

| Services | Purpose / state |
|---|---|
| pipeline | One-shot synthetic Bronze/Silver/Gold generator |
| kafka, kafka-init | Redpanda and transaction topic initialization; `kafka-data` volume |
| replay, spark | Transaction producer and Spark raw/window consumers |
| inventory-init, inventory-seed, inventory-replay | Inventory topic, synthetic opening stock, producer |
| inventory-spark | Inventory envelope ingestion only |
| refresh, stock-refresh | Transaction and stock snapshot publishers |
| airflow-data-init | Set ownership of generated retail volume for the shared application UID |
| airflow | Standalone scheduler, workers, UI, metadata/logs in `airflow-home` |
| api, dashboard | Read the latest published artifacts |

`merchmind-data` contains baseline files, `live/`, `inventory/`, and `closes/`. Application
containers mount it at `/app/data`; Airflow mounts it at `/opt/airflow/retail-data`.
Serving revisions identify file roles/names rather than mount prefixes.

`stream-data` contains transaction `output/raw`, `output/aggregates`, and `checkpoints`,
plus `inventory/raw` and `inventory-checkpoints`. Spark sees `/app/stream-data`; Airflow
reads `/opt/airflow/stream-data`. The `spark-ivy` volume caches connector dependencies,
with separate `transactions` and `inventory` subdirectories for concurrent consumers.

The application and Airflow source is mounted read-only from this checkout. Restart
long-running Python services after changing imported modules; rebuild images after
changing dependencies. The base PostgreSQL warehouse is an optional batch destination
and is not started by the full-demo target.

The Python dev container installs application/testing/warehouse dependencies and opens
the batch Streamlit demo. It does not start Kafka, Spark, or Airflow; use the documented
Docker Compose stack on a machine with Docker available for those services.

## Recovery rules

- Keep the broker, output, and checkpoint volumes together. Do not delete them merely
  to restart a process. `down --volumes` destroys the retained demo state.
- Never share one checkpoint between active writers. A missing Kafka offset is an error,
  not permission to skip data. Preserve retention long enough to cover downtime.
- Read only files listed in Spark's commit metadata; a driver crash may leave orphan
  Parquet files. `merchmind.live.committed_files` implements this for flat file sinks.
- Airflow retries twice after the initial attempt. Inspect quality/reconciliation JSON
  in the failed attempt, correct the cause, and rerun. Publication requires passing gates.
- Rerun the affected historical date after late arrivals. The system does not yet
  automatically schedule all affected dates when a late event arrives.
- Keep baseline products/customers and source files fixed while streaming. Stop the
  stack before generating a new baseline; use isolated volumes for a different dataset.

Forecasts created by a close are stored with that attempt; they do not replace the live
dashboard's forecast. Inventory and transaction pointers publish independently, so stock
may briefly lag the latest sales snapshot. Inspect each report's source revision.

## Useful API checks

- `/health`: application availability and analytical-data readiness.
- `/v1/live-status`: active transaction revision and accepted/quarantined event counts.
- `/v1/stock`: product stock and low-stock/oversold status; 404 until the first projection.
- `/v1/daily-close/YYYY-MM-DD`: last successful close for that day; 404 if none exists.
- `/docs`: all analytics endpoints and their response contracts.

## Evidence and retention

`make airflow-test` runs actual Airflow tasks against isolated synthetic file-commit
fixtures. `make streaming-test` uses real broker/Spark output and unique test topics.
The tests preserve reports/logs in `data/airflow-evidence/` and `data/streaming-evidence/`.
CI uploads those artifacts; the repository contains only compact, dated summaries.

Raw events, snapshots, close attempts, and failed pending snapshots are retained without
automatic cleanup. Archive evidence and design a retention policy before extended use.
Reconciliation and stock rebuilding scan history on one machine; this is not a production
availability or capacity guarantee. No cloud services are required for local verification.
