# aero-ref

Local **agent + MCP** demo: **FlightAware AeroAPI** (live airport boards) and **Google BigQuery** (demo airports catalog), orchestrated by **mcp-use**, **LangGraph**, and **Ollama** (`langchain-ollama`).

The Python package name is `aero-ref` (distribution); import path is `aero_ref`.

---

## Long-form visual reference (`docs/index.html`)

[`docs/index.html`](docs/index.html) is a **styled HTML walkthrough** of how an MCP stdio client, **mcp-use**’s `MCPClient` / `MCPAgent`, and **LangGraph** fit together: phased startup, `initialize` / `tools/list`, the model ↔ tools loop, memory, and context growth. It uses an older **weather MCP** example in the prose and snippets; the **mechanics are the same** here—substitute two servers (`flight`, `bigquery`) and the six tools below.

- **Offline:** open `docs/index.html` in a browser from a local clone.
- **Publishing:** point GitHub Pages (or any static host) at `/docs` if you want the HTML live; update the `<link rel="canonical">` and titles inside the file when the public URL is final.

---

## What a run looks like (`execution.log`)

[`execution.log`](execution.log) is a captured trace from:

```text
uv run .\main.py
```

with trace logging enabled (see [Run](#run)). It shows the same pipeline you would see on the console:

1. **Two stdio MCP sessions** — `mcp_use` spawns `uv run --directory <repo> src/aero_ref/flight_server.py` and `.../bigquery_server.py`.
2. **Cold `initialize` latency** — in this log, flight ~17s and BigQuery ~21s (first import + deps + optional BigQuery warmup; your machine will differ).
3. **Tool discovery** — flight MCP exposes four tools; BigQuery MCP exposes two (**six LangChain tools** total):

   | Server    | Tools |
   | --------- | ----- |
   | `flight`  | `get_airport_flights`, `get_airport_arrivals`, `get_airport_departures`, `get_airport_flight_counts` |
   | `bigquery`| `list_demo_airports`, `get_demo_airport` |

4. **LangGraph** — `ModelCallLimitMiddleware` then **model** (ChatOllama → Ollama, e.g. `qwen2.5:3b`), then **tools** node issuing MCP `tools/call` JSON-RPC over stdin/stdout.
5. **Example dialogue** — first turn: catalog lookup for **KIAH** plus `get_airport_flight_counts`; second turn: compare airports (limited by which ICAO codes exist in your BigQuery `airports` table—the log illustrates a missing catalog row and model recovery).
6. **Shutdown** — sessions closed cleanly after `agent.run` completes.

Use that file when you want **line-level** correlation with LangChain `[chain/*]`, `[llm/*]`, and mcp-use DEBUG lines.

---

## Architecture (short)

| Piece | Role |
| ----- | ---- |
| [`main.py`](main.py) | Loads [`mcp_config.json`](mcp_config.json), builds `MCPClient` + `ChatOllama` + `MCPAgent`, runs scripted queries (env-configurable). |
| [`src/aero_ref/flight_server.py`](src/aero_ref/flight_server.py) | FastMCP server: AeroAPI HTTP tools (API key from env). |
| [`src/aero_ref/bigquery_server.py`](src/aero_ref/bigquery_server.py) | FastMCP server: read-only BigQuery queries for the demo `airports` table. |
| **mcp-use** | Spawns servers, MCP protocol over stdio, adapts tools for LangChain. |
| **LangGraph** | Agent loop inside `MCPAgent` with step limits (`AGENT_MAX_STEPS`, middleware). |

---

## Prerequisites

- [uv](https://docs.astral.sh/uv/)
- [Ollama](https://ollama.com/) running with your model pulled (default: `qwen2.5:3b`)
- **FlightAware AeroAPI** key — `FLIGHTAWARE_API_KEY` or `AEROAPI_KEY` in `.env`
- **Google BigQuery** — `BIGQUERY_PROJECT` or `GOOGLE_CLOUD_PROJECT`, dataset/table env vars, and Application Default Credentials (e.g. `gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`)

---

## Setup

```powershell
cd C:\path\to\aero-ref
uv sync
```

Copy and edit `.env` at the repo root (see comments in your template). Load demo airport data when needed: `scripts/load_bq_demo_tables.py` and related SQL under `scripts/`.

**BigQuery smoke test (no MCP):**

```powershell
uv run python scripts/test_bigquery_connection.py
```

---

## Run

```powershell
uv run python main.py
```

Optional queries (defaults are in `main.py`):

- `AGENT_QUERY` — first user message
- `SECOND_AGENT_QUERY` — second turn (conversation memory on)

**Verbose trace** (similar style to `execution.log`):

```powershell
$env:FLIGHT_BOOKING_AGENT_TRACE = "1"
uv run python main.py
```

(`WEATHER_AGENT_TRACE=1` is also accepted for the same behavior.)

For mcp-use INFO-style logs without full LangChain verbosity: `MCP_USE_DEBUG=2`.

**Telemetry:** mcp-use may log anonymized telemetry; set `MCP_USE_ANONYMIZED_TELEMETRY=false` to disable.

**Python 3.14:** you may see a LangChain / Pydantic v1 compatibility warning; it does not stop the demo from running in typical setups.

---

## Tests

```powershell
uv run pytest
```

---

## MCP config for other hosts

[`mcp_config.json`](mcp_config.json) is the source of truth for Cursor or other MCP clients. `main.py` rewrites `--directory` to the repo root at runtime so paths stay correct when you move the project.
