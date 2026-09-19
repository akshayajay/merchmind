"""Launch the pinned Spark runtime with its matching Kafka connector."""

from __future__ import annotations

import os
import sys
from pathlib import Path

SPARK_VERSION = "4.0.1"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}"


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    command = [
        os.getenv("SPARK_SUBMIT", "spark-submit"),
        "--master",
        os.getenv("SPARK_MASTER", "local[2]"),
        "--packages",
        KAFKA_PACKAGE,
        "--conf",
        f"spark.jars.ivy={os.getenv('SPARK_IVY_DIR', '/tmp/merchmind-ivy')}",
        "--conf",
        "spark.sql.shuffle.partitions=2",
        "--conf",
        "spark.ui.enabled=false",
        *(
            [
                "--conf",
                f"spark.driver.host={os.environ['SPARK_DRIVER_HOST']}",
                "--conf",
                "spark.driver.bindAddress=0.0.0.0",
                "--conf",
                "spark.cores.max=2",
                "--conf",
                "spark.executor.cores=1",
                "--conf",
                "spark.executor.memory=512m",
            ]
            if os.getenv("SPARK_DRIVER_HOST")
            else []
        ),
        str(root / "jobs/spark/transaction_stream.py"),
        *sys.argv[1:],
    ]
    # Preserve signals and the Spark process exit status (including in Compose/CI).
    os.execvp(command[0], command)


if __name__ == "__main__":
    main()
