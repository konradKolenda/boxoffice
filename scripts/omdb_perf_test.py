#!/usr/bin/env python3
"""OMDb API performance benchmark — sequential vs parallel.

Usage:
  .venv/bin/python scripts/omdb_perf_test.py            # default 10 titles per mode
  .venv/bin/python scripts/omdb_perf_test.py --n 20     # bigger sample
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import snowflake.connector


OMDB_URL = "https://www.omdbapi.com/"
HTTP_TIMEOUT = 10
RETRY_TOTAL = 2
RETRY_BACKOFF = 2
SEQUENTIAL_SLEEP_SEC = 0.15
PARALLEL_WORKERS = 5


DAILY_QUOTA_USABLE = 950
FULL_DATASET_TITLES = 6545


def load_env() -> None:
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ[key.strip()] = val.strip()


def open_snowflake_connection(connection_name: str, role: str | None = None):
    config_path = Path.home() / ".config" / "snowflake" / "config.toml"
    with config_path.open("rb") as f:
        cfg = tomllib.load(f)
    conn_cfg = dict(cfg["connections"][connection_name])
    if "private_key_path" in conn_cfg:
        conn_cfg["private_key_file"] = conn_cfg.pop("private_key_path")
    if role is not None:
        conn_cfg["role"] = role
    return snowflake.connector.connect(**conn_cfg)


def make_http_session() -> requests.Session:
    """Same retry config as production omdb_fetch.py — fair comparison."""
    s = requests.Session()
    retry = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def get_titles(conn, n: int) -> list[tuple[str, int]]:
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT title, release_year
        FROM BOXOFFICE.RAW.OMDB_FETCH_QUEUE
        ORDER BY lifetime_rev DESC NULLS LAST
        LIMIT {int(n)}
        """
    )
    return cur.fetchall()


def call_omdb_raw(session: requests.Session, title: str, year: int) -> int:
    """One API call, returns HTTP status. No parsing, no logging — pure timing."""
    r = session.get(
        OMDB_URL,
        params={"t": title, "y": year, "apikey": os.environ["OMDB_API_KEY"]},
        timeout=HTTP_TIMEOUT,
    )
    return r.status_code


def time_sequential(session: requests.Session, titles: list[tuple[str, int]]) -> float:


    start = time.monotonic()
    for i, (title, year) in enumerate(titles):
        call_omdb_raw(session, title, year)
        if i < len(titles) - 1:
            time.sleep(SEQUENTIAL_SLEEP_SEC)
    return time.monotonic() - start


def time_parallel(session: requests.Session, titles: list[tuple[str, int]], workers: int) -> float:


    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(lambda ty: call_omdb_raw(session, ty[0], ty[1]), titles))
    return time.monotonic() - start


def fmt_seconds(s: float) -> str:
    if s < 1:
        return f"{s*1000:.0f}ms"
    if s < 60:
        return f"{s:.2f}s"
    minutes = s / 60
    return f"{minutes:.1f}min"


def print_table(rows: list[list[str]], headers: list[str]) -> None:

    widths = [max(len(headers[i]), *(len(r[i]) for r in rows)) for i in range(len(headers))]
    def line(items: list[str]) -> str:
        return "│ " + " │ ".join(f"{items[i]:<{widths[i]}}" for i in range(len(items))) + " │"
    sep = "├─" + "─┼─".join("─" * w for w in widths) + "─┤"
    top = "┌─" + "─┬─".join("─" * w for w in widths) + "─┐"
    bot = "└─" + "─┴─".join("─" * w for w in widths) + "─┘"
    print(top)
    print(line(headers))
    print(sep)
    for r in rows:
        print(line(r))
    print(bot)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=10, help="Titles to test per mode (default 10)")
    args = parser.parse_args()

    load_env()

    conn = open_snowflake_connection("default", role="BOXOFFICE_AIRFLOW")
    try:
        titles = get_titles(conn, args.n)
    finally:
        conn.close()

    if len(titles) < args.n:
        print(f"WARNING: queue has only {len(titles)} candidates (requested {args.n}); test will use available.", file=sys.stderr)
    n = len(titles)

    print(f"OMDb API performance benchmark — {n} calls per mode, {n*2} API calls total\n")

    session = make_http_session()


    print("warming up (1 call, excluded from timing)...")
    call_omdb_raw(session, titles[0][0], titles[0][1])

    print(f"→ sequential ({SEQUENTIAL_SLEEP_SEC*1000:.0f}ms sleep between calls)...")
    seq_total = time_sequential(session, titles)
    seq_per_call = seq_total / n

    print(f"→ parallel ({PARALLEL_WORKERS} workers, no sleep)...")
    par_total = time_parallel(session, titles, PARALLEL_WORKERS)
    par_effective = par_total / n
    speedup = seq_total / par_total

    print()
    print_table(
        rows=[
            ["Sequential",                       fmt_seconds(seq_total),  f"~{seq_per_call*1000:.0f}ms", "1x"],
            [f"Parallel ({PARALLEL_WORKERS} concurrent)",  fmt_seconds(par_total),  "—",                         f"~{speedup:.1f}x"],
        ],
        headers=["Mode", f"{n} calls", "Per call avg", "Speedup"],
    )


    print()
    print("Extrapolations:")
    print_table(
        rows=[
            [f"Daily run ({DAILY_QUOTA_USABLE} titles)",
             fmt_seconds(seq_per_call * DAILY_QUOTA_USABLE),
             fmt_seconds(par_total / n * DAILY_QUOTA_USABLE)],
            [f"Full dataset ({FULL_DATASET_TITLES} titles, theoretical)",
             fmt_seconds(seq_per_call * FULL_DATASET_TITLES),
             fmt_seconds(par_total / n * FULL_DATASET_TITLES)],
        ],
        headers=["Workload", "Sequential", "Parallel (5x)"],
    )

    print()
    print(f"Note: full dataset needs ≥{(FULL_DATASET_TITLES + DAILY_QUOTA_USABLE - 1) // DAILY_QUOTA_USABLE} days of runs (1000 daily quota cap from OMDb).")
    print(f"      Sequential is well within reasonable runtime for daily quota; parallelization is optional, not required.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
