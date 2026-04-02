"""
BigQuery MCP server — read optional travel rows and/or demo airport catalog (pairs with flight MCP for live AeroAPI).

Run via: uv run src/aero_ref/bigquery_server.py
  Or in MCP config: .venv/Scripts/python.exe -m aero_ref.bigquery_server (Windows) for slightly faster cold start than uv run.
Configure: BIGQUERY_PROJECT (or GOOGLE_CLOUD_PROJECT), BIGQUERY_DATASET.

Demo airports table (see scripts/bq_demo_ddl.sql, scripts/load_bq_demo_tables.py):
  BIGQUERY_AIRPORTS_TABLE (default airports). Live airport boards: use flight MCP AeroAPI.

Optional: BIGQUERY_SKIP_WARMUP=1 — skip pre-warming the client on MCP startup.
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.server.lifespan import lifespan

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_REPO_ROOT / ".env")
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from aero_ref.gcp_bigquery_client import get_cached_bigquery_client

_SEGMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")
_PROJECT = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")


def _bq_project() -> str:
    return (os.getenv("BIGQUERY_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT") or "").strip()


def _segment(name: str, default: str) -> str:
    raw = (os.getenv(name) or default).strip()
    if not _SEGMENT.match(raw):
        raise ValueError(f"Invalid {name}: must be a valid BigQuery dataset/table segment")
    return raw


def _qualified_table(table_env_var: str, default_table: str) -> str:
    project = _bq_project()
    if not project or not _PROJECT.match(project):
        raise ValueError(
            "BIGQUERY_PROJECT or GOOGLE_CLOUD_PROJECT must be a valid GCP project id "
            "(lowercase, digits, hyphens; 6–30 chars)."
        )
    dataset = _segment("BIGQUERY_DATASET", "flight_booking_demo")
    table = _segment(table_env_var, default_table)
    return f"`{project}.{dataset}.{table}`"


@lifespan
async def _warmup_bigquery_lifespan(_server: Any) -> Any:
    """
    After MCP initialize, build the cached BigQuery client so the first tools/call
    does not pay cold-start + ADC refresh alone.
    """
    if os.getenv("BIGQUERY_SKIP_WARMUP", "").strip().lower() in ("1", "true", "yes"):
        yield {}
        return

    proj = _bq_project()
    if not proj or not _PROJECT.match(proj):
        yield {}
        return

    try:
        _qualified_table("BIGQUERY_AIRPORTS_TABLE", "airports")
    except ValueError:
        yield {}
        return

    print("[bigquery-mcp] lifespan: warming BigQuery client...", file=sys.stderr, flush=True)
    t0 = time.monotonic()

    def _warm() -> None:
        get_cached_bigquery_client(proj)

    try:
        await asyncio.to_thread(_warm)
        print(
            f"[bigquery-mcp] lifespan: warmup finished in {time.monotonic() - t0:.2f}s",
            file=sys.stderr,
            flush=True,
        )
    except Exception as e:
        print(
            f"[bigquery-mcp] lifespan: warmup failed (tools will retry on first call): {e}",
            file=sys.stderr,
            flush=True,
        )
    yield {}


mcp = FastMCP(
    name="aero-ref-bigquery",
    version="1.0.0",
    instructions="Read-only BigQuery: demo airports catalog — list_demo_airports, get_demo_airport. "
    "Live flight boards: flight MCP get_airport_flights, get_airport_arrivals, "
    "get_airport_departures, get_airport_flight_counts (AeroAPI).",
    lifespan=_warmup_bigquery_lifespan,
)


def _row_to_jsonable(row: bigquery.table.Row) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in row.keys():
        v = row[key]
        if v is None:
            out[key] = None
        elif hasattr(v, "isoformat"):
            out[key] = v.isoformat()
        else:
            out[key] = v
    return out


def _query_timeout_sec() -> float:
    try:
        return max(5.0, float(os.getenv("BIGQUERY_QUERY_TIMEOUT_SEC", "120")))
    except ValueError:
        return 120.0


async def _query_rows(sql: str, params: list[bigquery.ScalarQueryParameter]) -> list[dict[str, Any]]:
    def _run() -> list[dict[str, Any]]:
        project = _bq_project()
        t0 = time.monotonic()
        print(
            f"[bigquery-mcp] query start project={project!r} timeout_sec={_query_timeout_sec():.0f}",
            file=sys.stderr,
            flush=True,
        )
        client = get_cached_bigquery_client(project)
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        job = client.query(sql, job_config=job_config)
        rows_iter = job.result(timeout=_query_timeout_sec())
        rows = [_row_to_jsonable(r) for r in rows_iter]
        elapsed = time.monotonic() - t0
        print(
            f"[bigquery-mcp] query done in {elapsed:.2f}s, rows={len(rows)}",
            file=sys.stderr,
            flush=True,
        )
        return rows

    return await asyncio.to_thread(_run)


@mcp.tool()
async def list_demo_airports(limit: int = 50) -> dict[str, Any]:
    """List rows from the demo airports table (AeroAPI-style catalog)."""
    try:
        lim = max(1, min(int(limit), 500))
    except (TypeError, ValueError):
        return {"error": "limit must be an integer", "rows": []}

    if not _bq_project():
        return {
            "error": "Set BIGQUERY_PROJECT or GOOGLE_CLOUD_PROJECT (and Application Default Credentials).",
            "rows": [],
        }

    try:
        table = _qualified_table("BIGQUERY_AIRPORTS_TABLE", "airports")
    except ValueError as e:
        return {"error": str(e), "rows": []}

    sql = f"""
    SELECT
      airport_code,
      alternate_ident,
      code_icao,
      code_iata,
      code_lid,
      name,
      type,
      elevation,
      city,
      state,
      longitude,
      latitude,
      timezone,
      country_code,
      wiki_url,
      airport_flights_url,
      airport_info_url
    FROM {table}
    ORDER BY airport_code
    LIMIT @lim
    """
    try:
        rows = await _query_rows(
            sql,
            [bigquery.ScalarQueryParameter("lim", "INT64", lim)],
        )
    except DeadlineExceeded:
        return {
            "error": f"BigQuery query timed out after {_query_timeout_sec():.0f}s.",
            "rows": [],
        }
    except GoogleCloudError as e:
        return {"error": str(e), "rows": []}
    except Exception as e:
        return {"error": str(e), "rows": []}

    return {"table": table.strip("`"), "count": len(rows), "rows": rows}


@mcp.tool()
async def get_demo_airport(airport_code: str) -> dict[str, Any]:
    """Look up one airport by code (matches airport_code, code_iata, or code_icao, case-insensitive)."""
    code = (airport_code or "").strip()
    if not code or len(code) > 32:
        return {"error": "airport_code is required (max 32 chars)", "rows": []}

    if not _bq_project():
        return {
            "error": "Set BIGQUERY_PROJECT or GOOGLE_CLOUD_PROJECT (and Application Default Credentials).",
            "rows": [],
        }

    try:
        table = _qualified_table("BIGQUERY_AIRPORTS_TABLE", "airports")
    except ValueError as e:
        return {"error": str(e), "rows": []}

    sql = f"""
    SELECT
      airport_code,
      alternate_ident,
      code_icao,
      code_iata,
      code_lid,
      name,
      type,
      elevation,
      city,
      state,
      longitude,
      latitude,
      timezone,
      country_code,
      wiki_url,
      airport_flights_url,
      airport_info_url
    FROM {table}
    WHERE UPPER(airport_code) = UPPER(@code)
       OR UPPER(IFNULL(code_iata, '')) = UPPER(@code)
       OR UPPER(IFNULL(code_icao, '')) = UPPER(@code)
    LIMIT 20
    """
    try:
        rows = await _query_rows(
            sql,
            [bigquery.ScalarQueryParameter("code", "STRING", code)],
        )
    except DeadlineExceeded:
        return {
            "error": f"BigQuery query timed out after {_query_timeout_sec():.0f}s.",
            "rows": [],
        }
    except GoogleCloudError as e:
        return {"error": str(e), "rows": []}
    except Exception as e:
        return {"error": str(e), "rows": []}

    return {"table": table.strip("`"), "airport_code": code, "count": len(rows), "rows": rows}


if __name__ == "__main__":
    mcp.run()
