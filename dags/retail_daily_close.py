"""Nightly UTC close; each task writes evidence before downstream publication."""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
    dag_id="retail_daily_close",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    tags=["merchmind", "reconciliation", "retail"],
)
def retail_daily_close():
    @task
    def prepare():
        import os
        from pathlib import Path

        from merchmind.close import prepare_close

        context = get_current_context()
        # Scheduled/backfill runs close their UTC data interval. Manual runs may
        # specify a business_date, useful for late data and the synthetic demo.
        business_date = (context["dag_run"].conf or {}).get("business_date")
        if business_date is None:
            business_date = context["data_interval_start"].date().isoformat()
        return prepare_close(
            Path(os.environ.get("MERCHMIND_DATA_DIR", "/opt/airflow/retail-data")),
            Path(os.environ.get("MERCHMIND_RAW_DIR", "/opt/airflow/stream-data/output/raw")),
            business_date,
        )

    @task
    def quality(attempt):
        import os

        from merchmind.close import check_quality

        check_quality(attempt, float(os.environ.get("MERCHMIND_MAX_INVALID_RATE", "0.05")))
        return attempt

    @task
    def reconcile(attempt):
        from merchmind.close import reconcile_close

        reconcile_close(attempt)
        return attempt

    @task
    def forecast(attempt):
        from merchmind.close import refresh_forecast

        refresh_forecast(attempt)
        return attempt

    @task
    def publish(attempt):
        from merchmind.close import publish_close

        return publish_close(attempt)

    publish(forecast(reconcile(quality(prepare()))))


retail_daily_close()
