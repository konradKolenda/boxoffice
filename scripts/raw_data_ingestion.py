#!/usr/bin/env python3
"""Raw data ingestion: one box-office CSV from S3 inbox → BOXOFFICE.RAW.REVENUES → archive.

Returns a dict on success, None when inbox is empty. The DAG wrapper turns None into AirflowSkipException.

Standalone test (from repo root):
  source .venv/bin/activate
  python scripts/raw_data_ingestion.py
"""
from __future__ import annotations

import os
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import boto3
import snowflake.connector


def load_env() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ[key.strip()] = val.strip()


def open_snowflake_connection(connection_name: str = "boxoffice_airflow"):
    config_path = Path.home() / ".config" / "snowflake" / "config.toml"
    if config_path.exists():
        with config_path.open("rb") as f:
            cfg = tomllib.load(f)
        if connection_name not in cfg.get("connections", {}):
            raise RuntimeError(
                f"snow CLI config.toml is missing [connections.{connection_name}] — add an entry "
                f"with user=BOXOFFICE_AIRFLOW_SVC + role=BOXOFFICE_AIRFLOW + private_key_path to the PEM file"
            )
        conn_cfg = dict(cfg["connections"][connection_name])

        if "private_key_path" in conn_cfg:
            conn_cfg["private_key_file"] = conn_cfg.pop("private_key_path")
        return snowflake.connector.connect(**conn_cfg)

    common = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_AIRFLOW_USER"],
        role=os.environ["SNOWFLAKE_AIRFLOW_ROLE"],
        warehouse=os.environ["SNOWFLAKE_AIRFLOW_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )

    pem_content = os.environ.get("SNOWFLAKE_AIRFLOW_PRIVATE_KEY_CONTENT")
    if pem_content:


        from cryptography.hazmat.primitives import serialization


        pem_content = pem_content.replace("\\n", "\n")
        passphrase = os.environ.get("SNOWFLAKE_AIRFLOW_PRIVATE_KEY_PASSPHRASE") or None
        p_key = serialization.load_pem_private_key(
            pem_content.encode("utf-8"),
            password=passphrase.encode("utf-8") if passphrase else None,
        )
        key_bytes = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return snowflake.connector.connect(**common, private_key=key_bytes)

    pem_path = os.environ.get("SNOWFLAKE_AIRFLOW_PRIVATE_KEY_PATH")
    if pem_path:
        return snowflake.connector.connect(**common, private_key_file=pem_path)

    raise RuntimeError(
        "Snowflake auth missing: set ~/.config/snowflake/config.toml or "
        "SNOWFLAKE_AIRFLOW_PRIVATE_KEY_CONTENT (multiline PEM) or "
        "SNOWFLAKE_AIRFLOW_PRIVATE_KEY_PATH (file path)"
    )


def find_oldest_inbox_file(s3, bucket: str, prefix: str) -> str | None:
    paginator = s3.get_paginator("list_objects_v2")
    candidates = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv.gz"):
                candidates.append(key)
    if not candidates:
        return None
    return min(candidates)


def s3_move(s3, bucket: str, src_key: str, dst_key: str) -> None:
    s3.copy_object(Bucket=bucket, Key=dst_key, CopySource={"Bucket": bucket, "Key": src_key})
    s3.delete_object(Bucket=bucket, Key=src_key)


def copy_into_revenues(conn, partitioned_path: str) -> int:
    cur = conn.cursor()
    cur.execute("CALL BOXOFFICE.RAW.SP_COPY_REVENUES_FROM_RAW(%s)", (partitioned_path,))
    rows_loaded = cur.fetchone()[0]
    return rows_loaded


def pickup_one_file() -> dict | None:
    """One tick: inbox → raw → COPY → archive.

    Returns dict on success, None when inbox is empty. The DAG wrapper raises AirflowSkipException on None.
    """
    bucket = os.environ.get("S3_BUCKET", "kk-demo-pipeline")
    inbox_prefix = "inbox/box_office"
    raw_prefix = "raw/box_office"
    archive_prefix = "archive/box_office"

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    s3 = boto3.client("s3")

    src_key = find_oldest_inbox_file(s3, bucket, f"{inbox_prefix}/")
    if src_key is None:
        print(f"[pickup] inbox {inbox_prefix}/ empty, nothing to load")
        return None


    partitioned_path = src_key.removeprefix(f"{inbox_prefix}/")
    raw_key = f"{raw_prefix}/{partitioned_path}"
    archive_key = f"{archive_prefix}/{run_date}/{partitioned_path}"

    print(f"[pickup] picked: {src_key}")

    print(f"[pickup] s3 mv inbox → raw: {raw_key}")
    s3_move(s3, bucket, src_key, raw_key)

    print(f"[pickup] CALL SP_COPY_REVENUES_FROM_RAW('{partitioned_path}')")
    conn = open_snowflake_connection()
    try:
        rows_loaded = copy_into_revenues(conn, partitioned_path)
    finally:
        conn.close()
    print(f"[pickup] rows_loaded={rows_loaded}")

    print(f"[pickup] s3 mv raw → archive: {archive_key}")
    s3_move(s3, bucket, raw_key, archive_key)

    return {"file": src_key, "rows_loaded": rows_loaded, "archive_key": archive_key}


if __name__ == "__main__":
    load_env()
    result = pickup_one_file()
    if result is None:
        print("[pickup] skipped (empty inbox)")
        sys.exit(0)
    print(f"[pickup] OK: {result}")
    sys.exit(0)
