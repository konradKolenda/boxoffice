"""DAG integrity test - sprawdza ze wszystkie DAGi parsuja sie bez bledow i maja tagi.

Uruchamiac przez:
  cd airflow && astro dev pytest

       wiec NIE sprawdzamy `retries >= 2` jak w default Astronomer template.
       'omdb_fetch' ma retry wbudowany na poziomie HTTP (urllib3.Retry w skrypcie); duplikowanie na poziomie taska to czysty waste.
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager

import pytest
from airflow.models import DagBag


@contextmanager
def _suppress_logging(namespace: str):
    logger = logging.getLogger(namespace)
    old = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old


def _import_errors():
    with _suppress_logging("airflow"):
        bag = DagBag(include_examples=False)
        prefix = os.environ.get("AIRFLOW_HOME", "")

        return [(None, None)] + [
            (os.path.relpath(k, prefix), v.strip()) for k, v in bag.import_errors.items()
        ]


def _dags():
    with _suppress_logging("airflow"):
        bag = DagBag(include_examples=False)
        prefix = os.environ.get("AIRFLOW_HOME", "")
        return [(dag_id, dag, os.path.relpath(dag.fileloc, prefix)) for dag_id, dag in bag.dags.items()]


@pytest.mark.parametrize("rel_path,err", _import_errors(), ids=[x[0] for x in _import_errors()])
def test_no_import_errors(rel_path, err):
    if rel_path and err:
        raise AssertionError(f"{rel_path} failed to import:\n{err}")


@pytest.mark.parametrize("dag_id,dag,fileloc", _dags(), ids=[x[2] for x in _dags()])
def test_dag_has_tags(dag_id, dag, fileloc):
    assert dag.tags, f"{dag_id} ({fileloc}) ma brak tagow - dorzuc tags=[...] przy DAG()"
