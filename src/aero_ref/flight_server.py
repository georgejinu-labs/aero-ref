"""
flight_server.py — FastMCP server #1
Exposes live airport flight boards via FlightAware AeroAPI.

Run via: uv run src/aero_ref/flight_server.py
Registered in mcp_config.json as "flight"
"""

import os
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import httpx
from dotenv import load_dotenv
from fastmcp import FastMCP

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_REPO_ROOT / ".env")

# ── Server init ──────────────────────────────────────────────
mcp = FastMCP(
    name="aero-ref-flight",
    version="1.0.0",
    instructions="FlightAware AeroAPI for one airport (prefer ICAO, e.g. KIAH): "
    "get_airport_flights (full board), get_airport_arrivals, get_airport_departures, "
    "get_airport_flight_counts. Pair with BigQuery airports catalog for stored metadata.",
)

AEROAPI_BASE = "https://aeroapi.flightaware.com/aeroapi"


def _api_key() -> str:
    return (os.getenv("FLIGHTAWARE_API_KEY") or os.getenv("AEROAPI_KEY") or "").strip()


def _normalize_airport_id(airport_id: str) -> tuple[Optional[str], Optional[str]]:
    aid = (airport_id or "").strip()
    if not aid:
        return None, "airport_id is required (ICAO preferred, e.g. KIAH)"
    if len(aid) > 16:
        return None, "airport_id too long (max 16 chars)"
    return aid, None


def _flight_query_params(
    airline: Optional[str],
    category: Optional[str],
    start: Optional[str],
    end: Optional[str],
    max_pages: int,
    cursor: Optional[str],
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    al = (airline or "").strip() or None
    cat = (category or "").strip() or None
    if al and cat:
        return None, "AeroAPI allows only one of airline or category (type); omit one."
    if cat and cat not in ("Airline", "General_Aviation"):
        return None, "category must be Airline or General_Aviation (or omit)."
    pages = max(1, min(max_pages, 50))
    params: dict[str, Any] = {"max_pages": pages}
    if al:
        params["airline"] = al.upper()
    if cat:
        params["type"] = cat
    if (start or "").strip():
        params["start"] = start.strip()
    if (end or "").strip():
        params["end"] = end.strip()
    if cursor:
        params["cursor"] = cursor
    return params, None


async def _aeroapi_airport_flights_request(
    airport_id: str,
    path_suffix: str,
    *,
    airline: Optional[str] = None,
    category: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    max_pages: int = 1,
    cursor: Optional[str] = None,
    with_query_params: bool = True,
) -> dict[str, Any]:
    """
    GET /airports/{id}/flights[/{path_suffix}]
    path_suffix: "" | "arrivals" | "departures" | "counts"
    """
    key = _api_key()
    if not key:
        return {"error": "Set FLIGHTAWARE_API_KEY (or AEROAPI_KEY) for AeroAPI."}

    aid, err = _normalize_airport_id(airport_id)
    if err:
        return {"error": err}

    params: dict[str, Any] = {}
    if with_query_params:
        p, perr = _flight_query_params(airline, category, start, end, max_pages, cursor)
        if perr:
            return {"error": perr, "airport_id": aid}
        params = p or {}

    headers = {"x-apikey": key}
    path_id = quote(aid, safe="")
    base_path = f"{AEROAPI_BASE.rstrip('/')}/airports/{path_id}/flights"
    url = f"{base_path}/{path_suffix}" if path_suffix else base_path

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(url, params=params, headers=headers)
    except httpx.HTTPError as e:
        return {"error": str(e), "airport_id": aid}

    if resp.status_code >= 400:
        return {
            "error": resp.text[:2000],
            "status_code": resp.status_code,
            "airport_id": aid,
        }

    try:
        data = resp.json()
    except ValueError:
        return {"error": "Response was not JSON", "raw": resp.text[:500], "airport_id": aid}

    if not isinstance(data, dict):
        return {"error": "Unexpected response shape", "airport_id": aid}

    out: dict[str, Any] = {"airport_id": aid, **data}
    if with_query_params and "max_pages" in params:
        out["max_pages_requested"] = params["max_pages"]
    return out


@mcp.tool()
async def get_airport_flights(
    airport_id: str,
    airline: Optional[str] = None,
    category: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    max_pages: int = 1,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    """
    All recent and upcoming flights at an airport: GET /airports/{id}/flights.
    Returns arrivals, departures, scheduled_arrivals, scheduled_departures, links, num_pages.

    Optional: airline (e.g. UAL) XOR category (Airline | General_Aviation); start/end ISO8601;
    max_pages 1–50; cursor for pagination. Prefer ICAO for airport_id.
    """
    result = await _aeroapi_airport_flights_request(
        airport_id,
        "",
        airline=airline,
        category=category,
        start=start,
        end=end,
        max_pages=max_pages,
        cursor=cursor,
        with_query_params=True,
    )
    if "error" in result:
        return result
    data = {k: v for k, v in result.items() if k != "airport_id"}
    keys = ("arrivals", "departures", "scheduled_arrivals", "scheduled_departures")
    counts = {k: len(data.get(k) or []) if isinstance(data.get(k), list) else 0 for k in keys}
    return {
        "airport_id": result["airport_id"],
        "max_pages_requested": result.get("max_pages_requested"),
        "num_pages": data.get("num_pages"),
        "links": data.get("links") or {},
        "counts": counts,
        **{k: data.get(k) or [] for k in keys},
    }


@mcp.tool()
async def get_airport_arrivals(
    airport_id: str,
    airline: Optional[str] = None,
    category: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    max_pages: int = 1,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    """
    Flights that have recently arrived: GET /airports/{id}/flights/arrivals
    (ordered by actual_on descending; default window last 24h per AeroAPI).

    Same optional filters as the combined board endpoint.
    """
    return await _aeroapi_airport_flights_request(
        airport_id,
        "arrivals",
        airline=airline,
        category=category,
        start=start,
        end=end,
        max_pages=max_pages,
        cursor=cursor,
        with_query_params=True,
    )


@mcp.tool()
async def get_airport_departures(
    airport_id: str,
    airline: Optional[str] = None,
    category: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    max_pages: int = 1,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    """
    Flights that have recently departed (not diverted): GET /airports/{id}/flights/departures
    (ordered by actual_off descending; optional start/end compare to actual_off).

    Same optional filters as the combined board endpoint.
    """
    return await _aeroapi_airport_flights_request(
        airport_id,
        "departures",
        airline=airline,
        category=category,
        start=start,
        end=end,
        max_pages=max_pages,
        cursor=cursor,
        with_query_params=True,
    )


@mcp.tool()
async def get_airport_flight_counts(airport_id: str) -> dict[str, Any]:
    """
    Summary counts by status: GET /airports/{id}/flights/counts
    (departed, enroute, scheduled_arrivals, scheduled_departures — see AeroAPI docs for semantics).

    No query parameters; path id only. Prefer ICAO for airport_id.
    """
    return await _aeroapi_airport_flights_request(
        airport_id,
        "counts",
        with_query_params=False,
    )


if __name__ == "__main__":
    mcp.run()
