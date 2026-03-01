from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="fx_daily_pipeline",
    start_date=datetime(2026, 2, 1),
    schedule="@daily"
    catchup=False,
    max_active_runs=1,
    tags=["capstone", "fx"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_fx",
        bash_command=(
            "cd /opt/airflow && "
            "PYTHONPATH=/opt/airflow "
            "python -m src.ingestion.fx_frankfurter "
            "--from '{{ macros.ds_add(ds, -14) }}' "
            "--to '{{ ds }}' "
            "--base EUR"
        ),
    )

    load = BashOperator(
        task_id="load_to_postgres",
        bash_command=(
            "cd /opt/airflow && "
            "PYTHONPATH=/opt/airflow "
            "python -m src.storage.postgres_loader"
        ),
    )

    transform = BashOperator(
        task_id="build_mart",
        bash_command=(
            "cd /opt/airflow && "
            "PYTHONPATH=/opt/airflow "
            "python -m src.transformations.run_sql_file "
            "--path src/transformations/sql/020_mart_fx_daily.sql"
        ),
    )

    quality = BashOperator(
        task_id="data_quality_tests",
        bash_command="cd /opt/airflow && pytest -q",
    )

    ingest >> load >> transform >> quality
