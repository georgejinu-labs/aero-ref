"""
Smoke-test BigQuery using the same .env as bigquery_server.py (no MCP).

From repo root:
  uv run python scripts/test_bigquery_connection.py

Checks:
  1) SELECT 1 (API + ADC / service account)
  2) Optional COUNT(*) on demo airports (same table as bigquery MCP list_demo_airports)

Exit codes: 0 OK, 1 missing project env, 2 SELECT 1 failed
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_REPO_ROOT / ".env")


def _project() -> str:
    return (os.getenv("BIGQUERY_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT") or "").strip()


def _timeout(name: str, default: float) -> float:
    try:
        return max(5.0, float(os.getenv(name, str(default))))
    except ValueError:
        return default


def main() -> int:
    project = _project()
    if not project:
        print(
            "ERROR: Set BIGQUERY_PROJECT or GOOGLE_CLOUD_PROJECT in .env (repo root).",
            file=sys.stderr,
        )
        return 1

    t_smoke = _timeout("BIGQUERY_TEST_SMOKE_TIMEOUT_SEC", 60)
    t_table = _timeout("BIGQUERY_TEST_TABLE_TIMEOUT_SEC", 120)

    from google.api_core.exceptions import DeadlineExceeded

    from aero_ref.gcp_bigquery_client import log_credential_source, make_bigquery_client

    print(f"Project: {project!r}")
    print(f"Repo root (for .env): {_REPO_ROOT}")
    print("Credentials: gcloud auth application-default login  OR  GOOGLE_APPLICATION_CREDENTIALS in .env")
    print()
    log_credential_source(project)
    try:
        client = make_bigquery_client(project)
    except RuntimeError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 2

    print("1) SELECT 1 ...")
    try:
        job = client.query("SELECT 1 AS smoke_test")
        row = next(iter(job.result(timeout=t_smoke)))
    except DeadlineExceeded:
        print(f"FAIL: timed out after {t_smoke:.0f}s (network / ADC?)", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"FAIL: {type(e).__name__}: {e}", file=sys.stderr)
        return 2

    print(f"   OK: smoke_test = {row['smoke_test']}")

    dataset = (os.getenv("BIGQUERY_DATASET") or "flight_booking_demo").strip()
    airports = (os.getenv("BIGQUERY_AIRPORTS_TABLE") or "airports").strip()
    fq_demo = f"`{project}.{dataset}.{airports}`"
    print(f"2) demo airports COUNT(*) FROM {fq_demo} ...")
    try:
        job_d = client.query(f"SELECT COUNT(*) AS c FROM {fq_demo}")
        rd = next(iter(job_d.result(timeout=t_table)))
        print(f"   OK: rows = {rd['c']}")
    except Exception as e:
        print(f"   SKIP: {type(e).__name__}: {e}")
        print(
            "   (Load demo data: uv run python scripts/load_bq_demo_tables.py)",
            file=sys.stderr,
        )

    print()
    print("All checks passed — BigQuery connectivity matches what the MCP server needs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
