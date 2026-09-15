"""One scripted bot on the MCP transport, and 2 ways one test runs it."""

import asyncio
import socket
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager

import uvicorn
from mcp import Client
from pipecat.frames.frames import (
    Frame,
    FunctionCallsStartedFrame,
    LLMContextFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext, LLMContextMessage
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.services.llm_service import LLMService
from pipecat.services.tts_service import TTSService
from pipecat.workers.runner import WorkerRunner

from pipecat_mcp_transport import McpBotServer, McpRunnerArguments, McpTransport

PROMPT = "you answer questions"
REPLY = "we open at nine"
HOST = "127.0.0.1"
READY_SECONDS = 5.0
BACKLOG = 16
TURN_SECONDS = 10.0

Answer = Callable[[Sequence[LLMContextMessage]], str]
Build = Callable[[int], McpBotServer]


class Echo(LLMService):
    """Stands in for one llm service. It answers each context frame with one scripted reply.
    tools counts responses that start function calls, each one with no text of its own."""

    def __init__(self, answer: Answer | None = None, *, delay: float = 0.0, tools: int = 0):
        super().__init__()
        self._answer = answer or (lambda messages: REPLY)
        self._delay = delay
        self._tools = tools

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)
        if isinstance(frame, LLMContextFrame):
            await self._respond(frame.context)
        else:
            await self.push_frame(frame, direction)

    async def _respond(self, context: LLMContext) -> None:
        await self.push_frame(LLMFullResponseStartFrame())
        if self._tools:
            self._tools -= 1
            await self.push_frame(FunctionCallsStartedFrame(function_calls=[]))
            await asyncio.sleep(self._delay)
            await self.push_frame(LLMFullResponseEndFrame())
            await self._respond(context)
            return
        for word in self._answer(context.get_messages()).split(" "):
            await asyncio.sleep(self._delay)
            await self.push_frame(LLMTextFrame(f"{word} "))
        await self.push_frame(LLMFullResponseEndFrame())


class Mute(TTSService):
    """Stands in for one tts service. calls counts each tts request of one session."""

    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    async def run_tts(self, text: str, context_id: str) -> AsyncGenerator[Frame | None, None]:
        self.calls += 1
        yield None


def bot_of(llm_of: Callable[[], FrameProcessor]) -> Callable:
    """Gives one bot function that runs scripted llm on MCP transport."""

    async def bot(runner_args: McpRunnerArguments) -> None:
        runner = await runner_of(
            runner_args.transport, llm_of(), runner_args.pipeline_idle_timeout_secs
        )
        await runner.run()

    return bot


async def runner_of(
    transport: McpTransport,
    llm: FrameProcessor,
    seconds: float | None = None,
    *,
    watch: FrameProcessor | None = None,
    text: FrameProcessor | None = None,
    tts: FrameProcessor | None = None,
) -> WorkerRunner:
    """Gives one runner over one worker on this transport.
    watch sits under input transport, where it reads each frame one chat call pushes."""
    context = LLMContext([{"role": "system", "content": PROMPT}])
    aggregators = LLMContextAggregatorPair(context)
    pipeline = Pipeline(
        [
            transport.input(),
            *([watch] if watch else []),
            aggregators.user(),
            llm,
            *([text] if text else []),
            *([tts] if tts else []),
            transport.output(),
            aggregators.assistant(),
        ]
    )
    worker = PipelineWorker(pipeline, idle_timeout_secs=seconds)
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)
    return runner


@asynccontextmanager
async def running(
    llm: FrameProcessor,
    watch: FrameProcessor | None = None,
    text: FrameProcessor | None = None,
    tts: FrameProcessor | None = None,
) -> AsyncIterator[McpTransport]:
    """Runs one worker on one transport, with no server. Exit cancels that worker."""
    transport = McpTransport()
    runner = await runner_of(transport, llm, watch=watch, text=text, tts=tts)
    ready = asyncio.Event()

    @runner.event_handler("on_ready")
    async def _on_ready(runner: WorkerRunner) -> None:
        ready.set()

    run = asyncio.create_task(runner.run())
    try:
        await asyncio.wait_for(ready.wait(), timeout=READY_SECONDS)
        yield transport
    finally:
        await runner.cancel()
        await asyncio.wait_for(run, timeout=READY_SECONDS)


@asynccontextmanager
async def serving(build: Build) -> AsyncIterator[tuple[Client, McpBotServer]]:
    """Serves one bot over streamable HTTP. Gives one stock client and that server."""
    # port listens before uvicorn takes it, so no client waits for a start
    with listening() as held:
        built = build(held.getsockname()[1])
        served = uvicorn.Server(uvicorn.Config(built.app()))
        run = asyncio.create_task(served.serve(sockets=[held]))
        try:
            async with Client(built.url) as client:
                yield client, built
        finally:
            served.should_exit = True
            await asyncio.wait_for(run, timeout=READY_SECONDS)


def listening() -> socket.socket:
    """Gives one listening socket on one free port of this host."""
    held = socket.socket()
    held.bind((HOST, 0))
    held.listen(BACKLOG)
    return held


async def text(client: Client, tool: str, **arguments: str) -> str:
    """Calls one tool and gives text of its content blocks."""
    result = await client.call_tool(tool, dict(arguments))
    return "".join(getattr(block, "text", "") for block in result.content)
