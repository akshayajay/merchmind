"""Kafka -> raw Parquet audit log and event-time merchandising aggregates.

Run through scripts/run_stream.py to include the matching Kafka connector.
Raw events include malformed/late messages; aggregates apply basic field checks.
The pandas Silver/Gold pipeline remains a separate batch demo.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("transaction_ts", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()),
        StructField("discount_pct", DoubleType()),
        StructField("sales_channel", StringType()),
        StructField("returned", BooleanType()),
    ]
)


def aggregate_events(raw: DataFrame) -> DataFrame:
    events = (
        raw.select(F.from_json("value", TRANSACTION_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("event_time", F.try_to_timestamp("transaction_ts"))
    )
    # Silver replay has passed reference/duplicate checks. Raw retains every offset,
    # including malformed external messages excluded from these aggregates.
    valid = (
        F.col("event_time").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") >= 0)
        & (F.col("unit_price") < F.lit(float("inf")))
        & ~F.isnan("unit_price")
        & F.col("returned").isNotNull()
    )
    for name in ("transaction_id", "customer_id", "product_id", "sales_channel"):
        valid = valid & F.col(name).isNotNull() & (F.length(F.trim(F.col(name))) > 0)
    events = (
        events.filter(valid)
        .withColumn(
            "net_revenue",
            F.col("quantity") * F.col("unit_price") * F.when(F.col("returned"), -1).otherwise(1),
        )
        .withWatermark("event_time", "10 minutes")
    )
    return events.groupBy(F.window("event_time", "5 minutes"), "sales_channel").agg(
        F.count("*").alias("transactions"),
        F.sum("quantity").alias("units"),
        F.sum("net_revenue").alias("net_revenue"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", default="retail-transactions")
    parser.add_argument("--output", required=True, help="Root for raw/ and aggregates/")
    parser.add_argument("--checkpoint", required=True, help="Dedicated durable checkpoint root")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--available-now", action="store_true", help="Drain backlog and stop")
    parser.add_argument("--progress-report", type=Path, help="Write local query evidence on exit")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("merchmind-transaction-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    queries = []
    try:
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "true")
            .load()
            .selectExpr(
                "topic",
                "partition",
                "offset",
                "timestamp AS kafka_timestamp",
                "CAST(key AS STRING) AS key",
                "CAST(value AS STRING) AS value",
            )
        )
        for name, frame in (("raw", raw), ("aggregates", aggregate_events(raw))):
            writer = (
                frame.writeStream.format("parquet")
                .queryName(f"merchmind-{name}")
                .outputMode("append")
                .option("path", f"{args.output.rstrip('/')}/{name}")
                .option("checkpointLocation", f"{args.checkpoint.rstrip('/')}/{name}")
            )
            writer = (
                writer.trigger(availableNow=True)
                if args.available_now
                else writer.trigger(processingTime="5 seconds")
            )
            queries.append(writer.start())
        if args.available_now:
            for query in queries:
                query.awaitTermination()
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        if args.progress_report:
            args.progress_report.parent.mkdir(parents=True, exist_ok=True)
            report = {
                "spark_version": spark.version,
                "topic": args.topic,
                "queries": {
                    query.name: {
                        "id": str(query.id),
                        "progress": [json.loads(batch.json) for batch in query.recentProgress],
                    }
                    for query in queries
                },
            }
            args.progress_report.write_text(json.dumps(report, indent=2) + "\n")
        spark.stop()


if __name__ == "__main__":
    main()
