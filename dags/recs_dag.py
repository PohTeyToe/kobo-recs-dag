"""
recs_dag.py — Airflow DAG that orchestrates the Better-than-Quiz recommendations
pipeline end-to-end.

Pattern lifted from SearchFlow's transformation_dag.py, re-targeted at the
Open Library + Tolino-BI use case described in the README.

Schedule: hourly. catchup=False. SLA 15 min.
Stages:
    ingest_open_library  -> KubernetesPodOperator (Python ingestion service)
    generate_events      -> PythonOperator (synthetic event generator — DEMO ONLY)
    dbt_deps             -> BashOperator
    dbt_run_staging      -> BashOperator   (stg_books, stg_events)
    dbt_run_intermediate -> BashOperator   (int_user_genre_affinity)
    dbt_run_marts        -> BashOperator   (mart_recs)
    dbt_test             -> BashOperator   (schema + funnel tests)
    publish_to_redis     -> KubernetesPodOperator (reverse-ETL, top-10 per user)
    refresh_bi_dashboard -> SimpleHttpOperator (Domo dataset refresh)

NOTE: This file is documentation-as-code. The demo at kobo-recs-dag.vercel.app
materializes the same pipeline to static JSON (see scripts/build-data.ts) so
that the dashboard can run on Vercel without an Airflow control plane. The
DAG exists to show the production shape.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.providers.http.operators.http import SimpleHttpOperator

DBT_DIR = "/dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt"

default_args = {
    "owner": "tolino-bi",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["tolino-bi-alerts@kobo.example"],
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "sla": timedelta(minutes=15),
}


def generate_synthetic_events(**ctx):
    """SYNTHETIC ONLY — see scripts/build-data.ts for the real generator.

    In production this stage would be replaced with a Kafka/Spark consumer
    of real anonymized reader telemetry. The demo uses synthetic data to
    avoid any privacy or licensing exposure.
    """
    from kobo_recs_dag.event_generator import build_event_batch  # noqa: F401

    build_event_batch(
        n_users=50, events_per_user=100, output_table="raw.reader_events"
    )


with DAG(
    dag_id="kobo_recs_pipeline",
    default_args=default_args,
    description="Better-than-Quiz: Open Library ingest → dbt models → Redis cache → Domo",
    schedule_interval="0 * * * *",  # hourly
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["recs", "tolino-bi", "open-library", "dbt"],
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_open_library = KubernetesPodOperator(
        task_id="ingest_open_library",
        name="ol-ingest",
        namespace="data-pipelines",
        image="ghcr.io/pohteytoe/kobo-recs-dag/ingest:latest",
        cmds=["python", "-m", "kobo_recs_dag.ingest"],
        arguments=[
            "--subjects",
            "fiction,mystery,science_fiction,romance,fantasy,young_adult",
            "--limit-per-subject",
            "100",
            "--target-table",
            "raw.openlibrary_search",
        ],
        get_logs=True,
    )

    generate_events = PythonOperator(
        task_id="generate_events",
        python_callable=generate_synthetic_events,
        doc_md=(
            "**SYNTHETIC DATA STAGE** — replace with real Kafka consumer in prod."
        ),
    )

    dbt_deps = BashOperator(task_id="dbt_deps", bash_command=f"{DBT_CMD} deps")
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging", bash_command=f"{DBT_CMD} run --select staging"
    )
    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"{DBT_CMD} run --select intermediate",
    )
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts", bash_command=f"{DBT_CMD} run --select marts"
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test",
        doc_md="Schema + funnel-integrity tests. Pipeline halts on failure.",
    )

    publish_to_redis = KubernetesPodOperator(
        task_id="publish_to_redis",
        name="reverse-etl",
        namespace="data-pipelines",
        image="ghcr.io/pohteytoe/kobo-recs-dag/reverse-etl:latest",
        cmds=["python", "-m", "kobo_recs_dag.reverse_etl"],
        arguments=["--source-table", "mart_recs", "--target", "redis://recs-cache:6379/0"],
        get_logs=True,
    )

    refresh_bi_dashboard = SimpleHttpOperator(
        task_id="refresh_bi_dashboard",
        http_conn_id="domo_api",
        endpoint="/v1/datasets/{{ var.value.domo_recs_dataset_id }}/refresh",
        method="POST",
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> [ingest_open_library, generate_events]
        >> dbt_deps
        >> dbt_run_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> dbt_test
        >> [publish_to_redis, refresh_bi_dashboard]
        >> end
    )
