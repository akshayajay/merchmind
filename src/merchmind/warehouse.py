from __future__ import annotations

import argparse
import io
import os
from pathlib import Path

import pandas as pd

from merchmind.config import DataPaths, default_data_dir

PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "department",
    "color",
    "aesthetic",
    "season",
    "list_price",
    "unit_cost",
    "launch_date",
]
CUSTOMER_COLUMNS = [
    "customer_id",
    "age_band",
    "region",
    "acquisition_channel",
    "joined_date",
]
TRANSACTION_COLUMNS = [
    "transaction_id",
    "transaction_ts",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_pct",
    "sales_channel",
    "returned",
]


def _copy_frame(cursor: object, table: str, columns: list[str], frame: pd.DataFrame) -> None:
    buffer = io.StringIO()
    frame.loc[:, columns].to_csv(buffer, index=False, na_rep="")
    buffer.seek(0)
    statement = f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
    with cursor.copy(statement) as copy:
        while chunk := buffer.read(1024 * 1024):
            copy.write(chunk)


def load_postgres(data_dir: Path, dsn: str, sql_dir: Path) -> dict[str, int]:
    """Load conformed Parquet into normalized PostgreSQL tables and build Gold views."""
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "Install warehouse dependencies with `pip install -e '.[warehouse]'`"
        ) from exc

    paths = DataPaths(data_dir)
    required = {
        "products": paths.silver / "dim_products.parquet",
        "customers": paths.silver / "dim_customers.parquet",
        "transactions": paths.silver / "fact_transactions.parquet",
    }
    missing = [str(path) for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Silver inputs not found: {', '.join(missing)}")

    schema_sql = (sql_dir / "schema.sql").read_text(encoding="utf-8")
    views_sql = (sql_dir / "merchandising_views.sql").read_text(encoding="utf-8")
    products = pd.read_parquet(required["products"])
    customers = pd.read_parquet(required["customers"])
    transactions = pd.read_parquet(required["transactions"])

    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(schema_sql)
        cursor.execute("TRUNCATE fact_transactions, dim_products, dim_customers")
        _copy_frame(cursor, "dim_products", PRODUCT_COLUMNS, products)
        _copy_frame(cursor, "dim_customers", CUSTOMER_COLUMNS, customers)
        _copy_frame(cursor, "fact_transactions", TRANSACTION_COLUMNS, transactions)
        cursor.execute(views_sql)
        cursor.execute(
            """
                SELECT
                  (SELECT count(*) FROM dim_products),
                  (SELECT count(*) FROM dim_customers),
                  (SELECT count(*) FROM fact_transactions),
                  (SELECT count(*) FROM gold_product_velocity),
                  (SELECT count(*) FROM gold_customer_rfm)
                """
        )
        counts = cursor.fetchone()
    return dict(
        zip(
            ["products", "customers", "transactions", "product_velocity_rows", "rfm_rows"],
            counts,
            strict=True,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Load MERCHMIND Silver data into PostgreSQL")
    parser.add_argument("--data-dir", type=Path, default=default_data_dir())
    parser.add_argument("--sql-dir", type=Path, default=Path("sql/postgres"))
    parser.add_argument("--dsn", default=os.getenv("MERCHMIND_POSTGRES_DSN"))
    args = parser.parse_args()
    if not args.dsn:
        parser.error("Set --dsn or MERCHMIND_POSTGRES_DSN")
    counts = load_postgres(args.data_dir, args.dsn, args.sql_dir)
    print("PostgreSQL warehouse loaded:", ", ".join(f"{k}={v:,}" for k, v in counts.items()))


if __name__ == "__main__":
    main()
