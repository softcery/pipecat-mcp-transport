#!/usr/bin/env python3
"""Latency bench of the MCP transport. Seconds from one chat call to its reply.
It reads one OpenAI-shaped server, so a row holds model seconds and transport seconds."""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import socket
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

import uvicorn
from mcp import Client
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.services.llm_service import FunctionCallParams
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.workers.runner import WorkerRunner

from pipecat_mcp_transport import McpBotServer, McpRunnerArguments

PROMPT = "You answer in one short sentence."
LINE = "when do you open"
TOOL_LINE = "when does the west branch open"
HOURS = "The west branch opens at nine, Monday to Friday."
TOOL = "hours"
TURNS = 5
RUNS = 5
HOST = "127.0.0.1"
BACKLOG = 16
ENCODING = "utf-8"


def main() -> int:
    """Measures each shape, then appends one row."""
    parsed = _arguments()
    row = {
        "sha": parsed.sha,
        "run": datetime.now(UTC).isoformat(timespec="seconds"),
        "python": platform.python_version(),
        "machine": platform.machine(),
        "runs": parsed.runs,
        "url": parsed.url,
        "model": parsed.model,
        "seconds": asyncio.run(_shapes(parsed)),
    }
    with parsed.out.open("a", encoding=ENCODING) as rows:
        rows.write(json.dumps(row) + "\n")
    print(json.dumps(row))
    return 0


async def _shapes(parsed: argparse.Namespace) -> dict[str, Any]:
    """Gives the spread of each shape, one fresh session per run."""
    async with _serving(parsed) as client:
        return {
            "turn": _spread([await _turns(client, [LINE]) for _ in range(parsed.runs)]),
            f"turns{TURNS}": _spread(
                [await _turns(client, [LINE] * TURNS) for _ in range(parsed.runs)]
            ),
            "tool": _spread([await _turns(client, [TOOL_LINE]) for _ in range(parsed.runs)]),
        }


async def _turns(client: Client, lines: list[str]) -> float:
    """Gives seconds of one session of these lines. An empty reply fails this run."""
    handle = _text(await client.call_tool("start"))
    started = time.perf_counter()
    for line in lines:
        reply = _text(await client.call_tool("chat", {"handle": handle, "line": line}))
        if not reply:
            raise RuntimeError(f"chat: expected reply text of {line!r}, got an empty block")
    return time.perf_counter() - started


def _spread(taken: list[float]) -> dict[str, float]:
    """Gives median, min and max seconds."""
    return {
        "median": round(median(taken), 4),
        "min": round(min(taken), 4),
        "max": round(max(taken), 4),
    }


def _text(result: Any) -> str:
    """Gives text of content blocks of one tool result."""
    return "".join(getattr(block, "text", "") for block in result.content).strip()


@asynccontextmanager
async def _serving(parsed: argparse.Namespace) -> AsyncIterator[Client]:
    """Serves bench bot on one free port. Gives one stock client of it."""
    held = socket.socket()
    held.bind((HOST, 0))
    held.listen(BACKLOG)
    with held:
        server = McpBotServer(_bot_of(parsed), port=held.getsockname()[1])
        served = uvicorn.Server(uvicorn.Config(server.app(), log_level="warning"))
        run = asyncio.create_task(served.serve(sockets=[held]))
        try:
            async with Client(server.url) as client:
                yield client
        finally:
            served.should_exit = True
            await run


def _bot_of(parsed: argparse.Namespace):
    """Gives one llm-only bot on the MCP transport, with one tool."""

    async def bot(runner_args: McpRunnerArguments) -> None:
        transport = runner_args.transport
        llm = OpenAILLMService(
            api_key=parsed.key,
            base_url=parsed.url,
            settings=OpenAILLMService.Settings(model=parsed.model),
        )
        context = LLMContext(
            [{"role": "system", "content": PROMPT}],
            tools=ToolsSchema(standard_tools=[_hours()]),
        )
        aggregators = LLMContextAggregatorPair(context)
        pipeline = Pipeline(
            [
                transport.input(),
                aggregators.user(),
                llm,
                transport.output(),
                aggregators.assistant(),
            ]
        )
        worker = PipelineWorker(pipeline, idle_timeout_secs=runner_args.pipeline_idle_timeout_secs)
        runner = WorkerRunner(handle_sigint=False)
        await runner.add_workers(worker)
        await runner.run()

    return bot


def _hours() -> FunctionSchema:
    """Gives one tool that model calls on tool shape."""

    async def opened(params: FunctionCallParams) -> None:
        await params.result_callback(HOURS)

    return FunctionSchema(
        name=TOOL,
        description="opening hours of one branch",
        properties={},
        required=[],
        handler=opened,
    )


def _arguments() -> argparse.Namespace:
    """Reads model server, output path, run count and commit."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:7861/v1", help="OpenAI-shaped url")
    parser.add_argument("--model", default="stub", help="model name the server serves")
    parser.add_argument("--key", default="stub", help="api key of that server")
    parser.add_argument("--out", type=Path, required=True, help="path of the jsonl rows")
    parser.add_argument("--runs", type=int, default=RUNS, help="runs of one shape")
    parser.add_argument("--sha", default="", help="commit this run measures")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
