#!/usr/bin/env bash
# Sync shared assets z repo-root do airflow/include/ - source-of-truth zostaje w repo root,
# airflow/include/ to deployment artifact dla obrazu Astronomer (build context = airflow/).
#
# Odpalac przed:
#   astro dev start    (lokalna iteracja)
#   astro deploy       (push do Astronomer Cloud)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AIRFLOW_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$AIRFLOW_DIR")"

echo "=== sync-include.sh  repo_root=$REPO_ROOT  → airflow/include/ ==="

echo "[1/3] scripts/{omdb_fetch,raw_data_ingestion}.py → include/scripts/"
mkdir -p "$AIRFLOW_DIR/include/scripts"
cp -f "$REPO_ROOT/scripts/omdb_fetch.py"          "$AIRFLOW_DIR/include/scripts/omdb_fetch.py"
cp -f "$REPO_ROOT/scripts/raw_data_ingestion.py"  "$AIRFLOW_DIR/include/scripts/raw_data_ingestion.py"

echo "[2/3] dbt/ → include/dbt/"
rsync -a --delete \
  --exclude='.venv-dbt' \
  --exclude='target/' \
  --exclude='logs/' \
  --exclude='.user.yml' \
  --exclude='__pycache__/' \
  --exclude='*.pyc' \
  "$REPO_ROOT/dbt/" "$AIRFLOW_DIR/include/dbt/"

echo "[3/3] pbi/refresh.sh → include/pbi/refresh.sh"
mkdir -p "$AIRFLOW_DIR/include/pbi"
cp -f "$REPO_ROOT/pbi/refresh.sh" "$AIRFLOW_DIR/include/pbi/refresh.sh"

find "$AIRFLOW_DIR/include" -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
find "$AIRFLOW_DIR/include" -type f -name '*.pyc' -delete 2>/dev/null || true

echo
echo "=== done. include/ tree (first 3 levels): ==="
find "$AIRFLOW_DIR/include" -maxdepth 3 -not -path '*/\.*' | sort
