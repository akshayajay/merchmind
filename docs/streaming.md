# Kafka and Spark execution

This local development path uses Redpanda's Kafka API, Spark 4.0.1, Scala 2.13 and the matching `spark-sql-kafka-0-10_2.13:4.0.1` connector. Spark runs with two local worker threads by default. It demonstrates streaming execution and checkpoint recovery, not distributed-cluster scale or AWS deployment.

## Complete retail platform

`make retail-demo` starts transaction and inventory topics/consumers, live publishers,
the app, and Airflow. Transaction events produce raw envelopes and provisional windows;
inventory uses the same Spark job's `--raw-only` mode with its own output/checkpoint roots.
The two consumers use separate Ivy cache directories to avoid concurrent-download races.

The [platform guide](retail-platform.md) explains daily reconciliation and stock; the
[operations guide](operations.md) documents service names, volumes, and recovery. The
September 20 integration now finishes by replaying a duplicate transaction at a new Kafka
offset and ingesting three inventory events, including a duplicate opening balance.
It verifies the daily close, idempotent rerun, stock projection, and FastAPI responses.
[Current integration evidence](verification/retail-streaming.json).

## Verified run: September 19, 2026

The real-broker integration test passed both on the host and in the Docker image (Python 3.11, Java 17, Spark 4.0.1). A separate full-demo run confirmed delivery and consumption of **49,400 events across three Kafka partitions**. All **44,687 finalized channel-window aggregates**, covering 49,398 transactions, reconciled with pandas batch calculations. Two transactions remained in windows newer than the watermark, as expected. The full-demo run used the existing host Spark installation; it is not a cluster throughput benchmark.

The [machine-readable verification snapshot](streaming-verification.json) records observed counts and environment details. To repeat the full reconciliation after installing the host dependencies and generating the batch demo:

```bash
python scripts/verify_streaming_demo.py
```

This command creates a unique three-partition test topic, replays the Silver input, runs Spark, checks every finalized window against the batch calculation, saves evidence, and deletes only its temporary topic. The integration test also uses isolated temporary topics; normal `retail-transactions` data is retained.

## Reproduce the integration test

```bash
make streaming-test
```

Docker Compose starts a healthy broker, builds the Spark/Python image and runs the integration test. No paid service, cloud credential, or external dataset is required. Docker registry, Python package index and Maven access are needed on first setup. Allow at least 4 GB memory for Docker.

The test publishes seven initial messages before the consumer starts: four valid transactions and three malformed/invalid messages. It verifies all seven raw envelopes, exact offset coverage, and two finalized channel aggregates. One valid transaction remains in an open window. A second run uses the same checkpoints after publishing a too-late transaction and a newer transaction; it verifies only two new inputs, preservation of all nine raw envelopes, rejection of the late event from aggregates, and a newly finalized window. A third run without new messages verifies zero new inputs and no duplicated outputs.

Assertions include returned-revenue reversal, exact units/transaction counts, Spark query identity across restarts, and the watermark's dropped-row metric. Each run writes Spark logs and progress JSON, followed by `summary.json` only after all assertions pass. Files are saved in `data/streaming-evidence/<unique-test-topic>/` and uploaded by the streaming CI job. A normal `pytest` run skips this integration test unless `RUN_KAFKA_INTEGRATION=1` is set.

After the initial/restart stages, a SIGKILL recovery stage and final marker bring the
transaction raw count to 11. A duplicate replay raises it to 12 without changing the
eight unique served transactions. Three inventory messages produce 1,092 on-hand units
for the fixture product after sales and returns. These deliberately small fixtures test
correctness, not throughput.

## Inspect a demo run

```bash
docker compose --profile streaming run -T --build --rm replay
docker compose --profile streaming run -T --build --rm spark \
  python3 scripts/run_stream.py --bootstrap-servers kafka:29092 \
  --output /app/stream-data/output --checkpoint /app/stream-data/checkpoints \
  --available-now --progress-report /app/stream-data/progress.json

# Broker offsets and topic metadata
docker compose exec kafka rpk topic describe retail-transactions -X brokers=localhost:29092

# Inspect the Parquet output in the persistent stream-data volume
docker compose --profile streaming run -T --rm --no-deps spark python3 -c \
  'import pandas as pd; print(pd.read_parquet("/app/stream-data/output/raw").shape); print(pd.read_parquet("/app/stream-data/output/aggregates").head())'
```

Read Parquet with Spark when inspecting a file sink after failures: Spark honors its `_spark_metadata` commit log; readers that glob every Parquet file could include uncommitted files left by an interrupted write. The automated test also kills the running driver with SIGKILL after a raw-file commit, then verifies recovery using the same checkpoints. The live publisher honors the commit log too.

## Recovery and limits

- Preserve the broker, output and checkpoint volumes. `docker compose stop kafka spark` stops the demo without deleting them. Do not use `down --volumes` if you want to resume this history.
- Each query has its own checkpoint directory. Keep the output/topic configuration stable across restarts and do not run two writers against the same checkpoint.
- The raw and aggregate queries commit independently. Their offsets may briefly differ; compare both query reports. There is no atomic transaction spanning the two outputs.
- Missing retained offsets fail the consumer (`failOnDataLoss=true`) instead of silently skipping data. Retention must cover downtime.
- Final windows need later event timestamps to advance the watermark. Waiting in wall-clock time does not finalize them. The test uses real later transactions, without injecting fake revenue into normal demo data.
- Malformed events and fields rejected by the Spark filter remain raw but do not reach aggregates. Discount and reference checks belong to the full Silver/live validator; provisional aggregates do not apply all of those contracts.
- Replaying a Parquet file twice intentionally creates two sets of Kafka events and affects the five-minute aggregates twice. The live serving publisher deduplicates business IDs (first valid value wins; baseline takes precedence). Production authentication and AWS/MSK integration remain unverified. A separate local lab tests replicated broker failure; see [validation](validation.md).
- The live publisher connects committed transaction events to the dashboard. Product/customer dimensions and company/macro data remain baseline inputs; streaming does not fetch public APIs or synchronize PostgreSQL.

## Technical references

- [Spark 4.0.1 Kafka connector and deployment](https://spark.apache.org/docs/4.0.1/streaming/structured-streaming-kafka-integration.html)
- [Spark event-time watermarks and available-now triggers](https://spark.apache.org/docs/4.0.1/streaming/apis-on-dataframes-and-datasets.html)
- [Redpanda local broker setup](https://docs.redpanda.com/streaming/current/get-started/quick-start/)

## Live serving

Run `docker compose --profile streaming up --build spark refresh api dashboard` after replay. The publisher polls committed raw files every five seconds. All six Gold outputs are built before one atomic pointer switch; failed builds preserve the previous snapshot. The dashboard polls the active snapshot every five seconds. See [publication semantics, scope and limitations](validation.md).
