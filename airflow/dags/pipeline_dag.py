"""Boxoffice pipeline - jeden DAG, 4 taski.

raw_data_ingestion (PythonOp) -> omdb_api_enrichment (PythonOp) -> dbt_build (BashOp) -> refresh_pbi (BashOp)

Manual trigger only (schedule=None) - webapp wciska "Run" przez Airflow REST API.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from include.scripts.omdb_fetch import main as omdb_fetch_main
from include.scripts.raw_data_ingestion import pickup_one_file


def _raw_data_ingestion_callable() -> dict:
    result = pickup_one_file()
    if result is None:
        raise AirflowSkipException("inbox empty - nothing to load this run")
    return result


with DAG(
    dag_id="pipeline_dag",
    description="Boxoffice EL + T + PBI refresh; manual trigger via webapp",
    schedule=None,


    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["boxoffice"],
    doc_md=__doc__,


    params={
        "omdb_limit": Param(
            25,
            type="integer",
            minimum=0,
            maximum=1000,
            title="OMDb API fetch limit",
            description="Max calli OMDb w tym runie. Default 25 (safety dla webapp). Full run: 950 (free tier 1000/day - 50 buffer).",
        ),
    },


    render_template_as_native_obj=True,
):

    raw_data_ingestion = PythonOperator(
        task_id="raw_data_ingestion",
        python_callable=_raw_data_ingestion_callable,
    )


    omdb_api_enrichment = PythonOperator(
        task_id="omdb_api_enrichment",
        python_callable=omdb_fetch_main,
        op_kwargs={"limit": "{{ params.omdb_limit }}"},
        trigger_rule="none_failed",
    )


    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            'printf "%b" "$SNOWFLAKE_DBT_PRIVATE_KEY_CONTENT" > /tmp/dbt_key.p8 && '
            'chmod 600 /tmp/dbt_key.p8 && '
            'SNOWFLAKE_DBT_PRIVATE_KEY_PATH=/tmp/dbt_key.p8 '
            'SNOWFLAKE_DBT_PRIVATE_KEY_CONTENT= '
            '/usr/local/airflow/dbt-venv/bin/dbt build '
            '--project-dir /usr/local/airflow/include/dbt '
            '--profiles-dir /usr/local/airflow/include/dbt '
            '--log-path /tmp/dbt-logs '
            '--target-path /tmp/dbt-target'
        ),
    )


    refresh_pbi = BashOperator(
        task_id="refresh_pbi",
        bash_command="bash /usr/local/airflow/include/pbi/refresh.sh --wait",
    )

    raw_data_ingestion >> omdb_api_enrichment >> dbt_build >> refresh_pbi
