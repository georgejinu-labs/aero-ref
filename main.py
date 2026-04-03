"""
Demo agent: BigQuery MCP (airports catalog) + Flight MCP (live airport boards only).

Run from repo root:
  uv run python main.py

Trace pipeline (MCP stdio, tool calls, LLM):
  PowerShell:  $env:FLIGHT_BOOKING_AGENT_TRACE=1; uv run python main.py
  (WEATHER_AGENT_TRACE=1 is also accepted if you reuse the same habit.)
  Or set MCP_USE_DEBUG=2 for mcp-use INFO-style logs without LangChain internals.

When FLIGHT_BOOKING_AGENT_TRACE=1 (or WEATHER_AGENT_TRACE=1):
  - mcp_use: session init, tools discovered, DEBUG-level client logs
  - LangChain: set_debug(True) + MCPAgent(verbose=True) → create_agent(debug=True)
  - StdOutCallbackHandler: LLM / chain and tool events on stdout
  - After each agent.run: system prompt + conversation history on stderr (truncated)

Optional env:
  AGENT_QUERY, SECOND_AGENT_QUERY (natural user-style questions when MCP prompts are off),
  AGENT_USE_MCP_PROMPTS=1 — first/second user messages from bigquery MCP prompts (see README),
  AGENT_PROMPT_FIRST, AGENT_PROMPT_ICAO, SECOND_AGENT_PROMPT, SECOND_PROMPT_ICAO_A, SECOND_PROMPT_ICAO_B,
  OLLAMA_MODEL, OLLAMA_BASE_URL, AGENT_MAX_STEPS
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def _trace_enabled() -> bool:
    for key in ("FLIGHT_BOOKING_AGENT_TRACE", "WEATHER_AGENT_TRACE"):
        if os.environ.get(key, "").strip().lower() in ("1", "true", "yes"):
            return True
    return False


def _configure_trace_logging() -> bool:
    """Return True if trace mode is on. Must run before `from mcp_use import MCPAgent`."""
    if not _trace_enabled():
        return False
    from mcp_use.logging import Logger

    Logger.set_debug(2)
    Logger.configure(level=logging.DEBUG)
    logging.getLogger("httpx").setLevel(logging.DEBUG)
    logging.getLogger("httpcore").setLevel(logging.INFO)
    logging.getLogger("mcp").setLevel(logging.DEBUG)
    from langchain_core.globals import set_debug as langchain_set_debug

    langchain_set_debug(True)
    return True


_TRACE = _configure_trace_logging()

from langchain_ollama import ChatOllama
from mcp.types import GetPromptResult, TextContent
from mcp_use import MCPAgent, MCPClient

_SCENARIO_INSTRUCTIONS = """
You can call tools on two MCP servers:

1) bigquery — Airports catalog: list_demo_airports(limit), get_demo_airport(airport_code) for
   ICAO / IATA / LID.

2) flight (live AeroAPI) — get_airport_flights (full board), get_airport_arrivals,
   get_airport_departures, get_airport_flight_counts (prefer ICAO, e.g. KIAH).

Typical flow: airport metadata from BigQuery when useful, live boards from flight tools. Use small
limits unless the user asks for more. Say whether facts came from BigQuery vs live API.

User messages are normal questions (e.g. delays at an airport, comparing two hubs). You choose tools —
do not ask the user to name MCP tools.

When the user message was produced from an MCP prompt (server-side template), follow that workflow
exactly—especially any rule about missing catalog rows and not substituting other airports.
"""


def _prompt_result_to_user_text(result: GetPromptResult) -> str:
    parts: list[str] = []
    for pm in result.messages:
        c = pm.content
        if isinstance(c, TextContent):
            parts.append(c.text)
        else:
            parts.append(str(c))
    text = "\n\n".join(parts).strip()
    return text if text else "(empty MCP prompt)"


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes")


def _print_injected_context(agent: MCPAgent, title: str) -> None:
    """Show system prompt and memory the next LLM call will build from."""
    sys_msg = agent.get_system_message()
    print(f"\n--- {title} ---", file=sys.stderr)
    if sys_msg:
        preview = sys_msg.content
        if isinstance(preview, str) and len(preview) > 2000:
            preview = preview[:2000] + "\n... [truncated]"
        print(f"System message:\n{preview}", file=sys.stderr)
    hist = agent.get_conversation_history()
    print(f"Conversation history: {len(hist)} message(s)", file=sys.stderr)
    for i, msg in enumerate(hist):
        t = type(msg).__name__
        c = getattr(msg, "content", "")
        if isinstance(c, str) and len(c) > 500:
            c = c[:500] + "... [truncated]"
        print(f"  [{i}] {t}: {c!r}", file=sys.stderr)


def _load_mcp_config_for_repo(config_path: Path, repo_root: Path) -> dict[str, Any]:
    """Ensure `uv run` uses this repo as the project directory (fixes wrong cwd hangs)."""
    data = json.loads(config_path.read_text(encoding="utf-8"))
    root_s = str(repo_root.resolve())
    for srv in data.get("mcpServers", {}).values():
        if not isinstance(srv, dict):
            continue
        if srv.get("command") != "uv":
            continue
        args = list(srv.get("args") or [])
        if len(args) < 2 or args[0] != "run":
            continue
        if args[1] == "--directory":
            if len(args) >= 4:
                args[2] = root_s
                srv["args"] = args
            continue
        srv["args"] = ["run", "--directory", root_s, *args[1:]]
    return data


async def _run() -> None:
    root = Path(__file__).resolve().parent
    cfg = root / "mcp_config.json"
    if not cfg.is_file():
        raise SystemExit(f"Missing MCP config: {cfg}")

    client = MCPClient(config=_load_mcp_config_for_repo(cfg, root))
    llm = ChatOllama(
        model=os.getenv("OLLAMA_MODEL", "qwen2.5:3b"),
        base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
        temperature=0,
    )

    trace_callbacks = None
    if _TRACE:
        from langchain_core.callbacks import StdOutCallbackHandler

        trace_callbacks = [StdOutCallbackHandler()]

    agent = MCPAgent(
        llm=llm,
        client=client,
        max_steps=int(os.getenv("AGENT_MAX_STEPS", "24")),
        additional_instructions=_SCENARIO_INSTRUCTIONS.strip(),
        memory_enabled=True,
        pretty_print=True,
        verbose=_TRACE,
        callbacks=trace_callbacks,
    )

    if _TRACE:
        print(
            "\n(FLIGHT_BOOKING_AGENT_TRACE=1: debug on stdout/stderr; "
            "context dumps on stderr after each reply)\n",
            file=sys.stderr,
        )

    query = os.getenv(
        "AGENT_QUERY",
        "At Houston Bush (KIAH), use our airport reference for the official name and city. "
        "Then summarize live activity: flight counts (departed, enroute, scheduled arrivals and "
        "departures), plus whether recent arrivals or departures look unusually delayed.",
    )

    await agent.initialize()
    try:
        use_prompts = _env_flag("AGENT_USE_MCP_PROMPTS", True)
        bq = client.get_session("bigquery")

        if use_prompts:
            p1 = os.getenv("AGENT_PROMPT_FIRST", "airport-summary").strip() or "airport-summary"
            icao = os.getenv("AGENT_PROMPT_ICAO", "KIAH").strip() or "KIAH"
            pr = await bq.get_prompt(p1, {"icao_code": icao})
            query = _prompt_result_to_user_text(pr)
            if _TRACE:
                print(f"\n[AGENT_USE_MCP_PROMPTS] first prompt={p1!r} -> user message len={len(query)}\n", file=sys.stderr)

        out = await agent.run(query)
        print(out)
        if _TRACE:
            _print_injected_context(agent, "Context after first turn")

        second = ""
        if use_prompts:
            p2 = os.getenv("SECOND_AGENT_PROMPT", "compare-airports").strip() or "compare-airports"
            a = os.getenv("SECOND_PROMPT_ICAO_A", "KHOU").strip() or "KHOU"
            b = os.getenv("SECOND_PROMPT_ICAO_B", "KIAH").strip() or "KIAH"
            pr2 = await bq.get_prompt(p2, {"icao_a": a, "icao_b": b})
            second = _prompt_result_to_user_text(pr2)
            if _TRACE:
                print(
                    f"\n[AGENT_USE_MCP_PROMPTS] second prompt={p2!r} -> user message len={len(second)}\n",
                    file=sys.stderr,
                )
        else:
            second = os.getenv(
                "SECOND_AGENT_QUERY",
                "Compare Houston Hobby (KHOU) and Bush (KIAH): official names and cities from our catalog, "
                "then contrast live flight counts and recent arrival and departure activity — which airport "
                "looks busier or more disrupted? If either code is missing from the catalog, say so clearly "
                "and do not substitute another airport.",
            ).strip()

        if second:
            out2 = await agent.run(second)
            print(out2)
            if _TRACE:
                _print_injected_context(agent, "Context after second turn")
    finally:
        await client.close_all_sessions()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
