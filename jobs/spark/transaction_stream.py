"""Spark Structured Streaming job for live merchandising aggregates.

Submit locally with spark-submit or package for EMR Serverless. This job is not
required by the pandas-backed demo pipeline; it is the distributed production path.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.functions import sum as spark_sum
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
        StructField("transaction_id", StringType(), False),
        StructField("transaction_ts", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount_pct", DoubleType(), False),
        StructField("sales_channel", StringType(), False),
        StructField("returned", BooleanType(), False),
    ]
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", default="retail-transactions")
    parser.add_argument("--output", required=True)
    parser.add_argument("--checkpoint", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("merchmind-transaction-stream").getOrCreate()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )
    events = (
        raw.select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("event_time", to_timestamp("transaction_ts"))
        .withColumn("net_revenue", col("quantity") * col("unit_price"))
        .withWatermark("event_time", "10 minutes")
    )
    aggregates = events.groupBy(window("event_time", "5 minutes"), "sales_channel").agg(
        spark_sum("quantity").alias("units"),
        spark_sum("net_revenue").alias("net_revenue"),
    )
    query = (
        aggregates.writeStream.format("parquet")
        .outputMode("append")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
