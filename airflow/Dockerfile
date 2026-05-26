FROM quay.io/astronomer/astro-runtime:13.7.0

RUN python -m venv /usr/local/airflow/dbt-venv && \
    /usr/local/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-snowflake==1.11.5
