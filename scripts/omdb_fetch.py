#!/usr/bin/env python3
"""Enrich films from BOXOFFICE.RAW.REVENUES with OMDb API data.

Reads queue from the BOXOFFICE.RAW.OMDB_FETCH_QUEUE view (already-anti-joined),
calls OMDb, wraps response with metadata, writes one JSON file to S3 per call,
INSERTs one row per call to RAW.OMDB_FETCH_LOG, and REFRESHes the external table.

Usage:
  source .venv/bin/activate
  python scripts/omdb_fetch.py --limit 10            # fetch up to 10
  python scripts/omdb_fetch.py --limit 10 --dry-run  # show queue + quota, no API calls
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import snowflake.connector


DAILY_QUOTA = 1000
QUOTA_BUFFER = 50
LOG_BATCH_SIZE = 50
OMDB_URL = "https://www.omdbapi.com/"
HTTP_TIMEOUT = 10
RETRY_TOTAL = 2
RETRY_BACKOFF = 2


def load_env() -> None:
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ[key.strip()] = val.strip()


def make_http_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def get_remaining_quota(conn) -> int:
    """Pozostalo do limitu OMDb dzisiaj (po odjeciu buffera)."""


    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT {DAILY_QUOTA} - COUNT(*) - {QUOTA_BUFFER} AS remaining
        FROM BOXOFFICE.RAW.OMDB_FETCH_LOG
        WHERE call_at::DATE = CURRENT_DATE()
        """
    )
    remaining = cur.fetchone()[0]
    return max(0, remaining)


def get_queue(conn, limit: int) -> list[tuple[str, int]]:
    """Top N (title, release_year) wedlug lifetime_rev DESC z view'a."""


    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT title, release_year
        FROM BOXOFFICE.RAW.OMDB_FETCH_QUEUE
        ORDER BY lifetime_rev DESC NULLS LAST
        LIMIT {int(limit)}
        """
    )
    return cur.fetchall()


def call_omdb(session: requests.Session, title: str, year: int) -> tuple[str, dict | None, int | None, str | None]:
    """Jeden call do OMDb. Returns (outcome, response_body, http_status, error_msg).

    outcome in {'found', 'not_found', 'error_5xx', 'error_429', 'error_timeout', 'error_other'}
    """
    try:

        r = session.get(
            OMDB_URL,
            params={"t": title, "y": year, "apikey": os.environ["OMDB_API_KEY"]},
            timeout=HTTP_TIMEOUT,
        )
    except requests.Timeout:
        return ("error_timeout", None, None, "request timed out")
    except requests.RequestException as e:
        return ("error_other", None, None, str(e)[:200])


    if r.status_code == 429:
        return ("error_429", None, 429, "rate limit")
    if r.status_code >= 500:
        return ("error_5xx", None, r.status_code, r.reason)
    if r.status_code != 200:
        return ("error_other", None, r.status_code, r.reason)


    body = r.json()
    if body.get("Response") == "True":
        return ("found", body, 200, None)
    if body.get("Response") == "False":
        return ("not_found", body, 200, body.get("Error"))
    return ("error_other", None, r.status_code, "unexpected response shape")


def wrap_response(body: dict, title: str, year: int, status: str) -> dict:
    """Dodaj namespace-d metadata do OMDb response (_lookup, _status, _fetched_at)."""

    return {
        "_lookup": {"title": title, "year": year},
        "_status": status,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        **body,
    }


def s3_key_for(wrapped: dict, title: str, year: int) -> str:
    """S3 key: raw/omdb/yyyy=YYYY/mm=MM/dd=DD/<imdbID-or-hash>.json"""
    now = datetime.now(timezone.utc)
    prefix = f"raw/omdb/yyyy={now.year}/mm={now.month:02d}/dd={now.day:02d}"
    imdb_id = wrapped.get("imdbID")
    if imdb_id:
        return f"{prefix}/{imdb_id}.json"

    h = hashlib.sha1(f"{title}|{year}".encode("utf-8")).hexdigest()[:12]
    return f"{prefix}/notfound_{h}.json"


def write_s3(s3, bucket: str, key: str, wrapped: dict) -> None:

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(wrapped, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def flush_log_buffer(conn, buffer: list[tuple]) -> None:
    if not buffer:
        return


    batch_json = json.dumps([
        {
            "lookup_title": title,
            "lookup_year": year,
            "outcome": outcome,
            "http_status": http_status,
            "error_message": err_msg,
        }
        for (title, year, outcome, http_status, err_msg) in buffer
    ])
    cur = conn.cursor()
    cur.execute(
        "CALL BOXOFFICE.RAW.SP_LOG_OMDB_CALL_BATCH(PARSE_JSON(%s))",
        (batch_json,),
    )


def refresh_external_table(conn) -> None:
    """Snowflake REFRESH wrapped w SP_REFRESH_OMDB_CACHE.

    """
    cur = conn.cursor()
    cur.execute("CALL BOXOFFICE.RAW.SP_REFRESH_OMDB_CACHE()")


def print_profile_summary(profile: dict[str, list[float]]) -> None:
    import statistics


    denom = sum(sum(profile.get(s, [])) for s in ("api", "s3", "log"))

    print()
    print("=== Per-step timing profile ===")
    print(f"{'step':<10} {'count':>6} {'mean_ms':>10} {'p50_ms':>10} {'p95_ms':>10} {'max_ms':>10} {'total_s':>10} {'%_total':>8}")
    for step in ("api", "s3", "log", "total"):
        samples = profile.get(step, [])
        if not samples:
            continue
        sorted_s = sorted(samples)
        n = len(sorted_s)
        mean_ms = statistics.mean(sorted_s) * 1000
        p50_ms = sorted_s[n // 2] * 1000
        p95_ms = sorted_s[min(int(n * 0.95), n - 1)] * 1000
        max_ms = sorted_s[-1] * 1000
        total_s = sum(samples)


        if step == "total":
            pct = 100.0
        else:
            pct = (total_s / denom * 100) if denom > 0 else 0.0
        print(f"{step:<10} {n:>6} {mean_ms:>10.1f} {p50_ms:>10.1f} {p95_ms:>10.1f} {max_ms:>10.1f} {total_s:>10.2f} {pct:>7.1f}%")


def open_snowflake_connection(connection_name: str = "boxoffice_airflow"):
    config_path = Path.home() / ".config" / "snowflake" / "config.toml"
    if config_path.exists():
        with config_path.open("rb") as f:
            cfg = tomllib.load(f)
        if connection_name not in cfg.get("connections", {}):
            raise RuntimeError(
                f"snow CLI config.toml nie ma [connections.{connection_name}] - dorzuc entry "
                f"z user=BOXOFFICE_AIRFLOW_SVC + role=BOXOFFICE_AIRFLOW + private_key_path do PEM-a "
                f"(patrz airflow/QUICKSTART.md)"
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
        "Snowflake auth missing: ustaw ~/.config/snowflake/config.toml lub "
        "SNOWFLAKE_AIRFLOW_PRIVATE_KEY_CONTENT (multiline PEM) lub "
        "SNOWFLAKE_AIRFLOW_PRIVATE_KEY_PATH (file path)"
    )


def main(limit: int = 10, dry_run: bool = False, profile: bool = False, quiet: bool = False) -> int:
    load_env()
    bucket = os.environ.get("S3_BUCKET", "kk-demo-pipeline")

    if "OMDB_API_KEY" not in os.environ:
        print("ERROR: OMDB_API_KEY not set (check .env)", file=sys.stderr)
        return 2

    conn = open_snowflake_connection()

    try:
        remaining = get_remaining_quota(conn)
        effective_limit = min(limit, remaining)

        print(f"=== omdb_fetch  quota_remaining={remaining}  requested={limit}  effective={effective_limit} ===")

        if effective_limit <= 0:
            print("No quota left today (or already over with buffer). Exiting cleanly.")
            return 0

        queue = get_queue(conn, effective_limit)
        print(f"queue: {len(queue)} films, top 5:")
        for t, y in queue[:5]:
            print(f"  {t} ({y})")
        if len(queue) > 5:
            print(f"  ... and {len(queue) - 5} more")

        if dry_run:
            print("\n--dry-run mode: not calling API. Exiting.")
            return 0

        session = make_http_session()
        s3 = boto3.client("s3")

        counts: dict[str, int] = {}


        prof: dict[str, list[float]] = {"api": [], "s3": [], "log": [], "total": []}


        log_buffer: list[tuple] = []

        try:
            print()
            for i, (title, year) in enumerate(queue, 1):
                t_iter = time.monotonic()

                t0 = time.monotonic()
                outcome, body, http_status, err_msg = call_omdb(session, title, year)
                prof["api"].append(time.monotonic() - t0)
                where = ""


                if outcome in ("found", "not_found") and body is not None:
                    wrapped = wrap_response(body, title, year, outcome)
                    key = s3_key_for(wrapped, title, year)
                    t0 = time.monotonic()
                    try:
                        write_s3(s3, bucket, key, wrapped)
                        where = f"s3://{bucket}/{key}"
                    except Exception as s3_err:
                        original = outcome
                        outcome = "error_other"
                        err_msg = f"s3 write failed (orig outcome={original}): {s3_err}"[:500]
                        where = "(s3 write failed)"
                    prof["s3"].append(time.monotonic() - t0)
                else:
                    where = f"(no S3 write: {err_msg})"

                counts[outcome] = counts.get(outcome, 0) + 1
                log_buffer.append((title, year, outcome, http_status, err_msg))

                if len(log_buffer) >= LOG_BATCH_SIZE:
                    t0 = time.monotonic()
                    flush_log_buffer(conn, log_buffer)
                    prof["log"].append(time.monotonic() - t0)
                    log_buffer = []

                if not quiet:
                    print(f"[{i}/{len(queue)}] {outcome:11} {title[:50]:50} ({year}) → {where}")

                prof["total"].append(time.monotonic() - t_iter)
        finally:


            if log_buffer:
                t0 = time.monotonic()
                flush_log_buffer(conn, log_buffer)
                prof["log"].append(time.monotonic() - t0)

        print()
        print("refreshing RAW.OMDB external table...")
        refresh_external_table(conn)

        print()
        print(f"=== summary: {counts} ===")

        if profile:
            print_profile_summary(prof)

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=10, help="Max API calls this run (capped by remaining quota)")
    parser.add_argument("--dry-run", action="store_true", help="Show queue and quota, do not call API or write")
    parser.add_argument("--profile", action="store_true", help="Print per-step timing summary at the end (api/s3/log/total)")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-call progress line. Header, summary, and --profile output still show.")
    args = parser.parse_args()
    sys.exit(main(limit=args.limit, dry_run=args.dry_run, profile=args.profile, quiet=args.quiet))
