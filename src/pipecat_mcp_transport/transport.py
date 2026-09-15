"""MCP transport. One transport is one session, one chat call is one turn."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from loguru import logger
from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    Frame,
    FunctionCallsStartedFrame,
    InterruptionFrame,
    LLMConfigureOutputFrame,
    LLMFullResponseEndFrame,
    LLMMessagesAppendFrame,
    StartFrame,
    TextFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.runner.types import RunnerArguments
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.transports.base_transport import BaseTransport, TransportParams

from pipecat_mcp_transport.turn import Notify, Turn


class SessionEnded(Exception):
    """Worker of this transport ended."""


class McpTransport(BaseTransport):
    """One pipecat session that chat calls drive."""

    def __init__(self, params: TransportParams | None = None, *, name: str | None = None) -> None:
        super().__init__(name=name)
        self._params = params or TransportParams()
        self._input: McpInputTransport | None = None
        self._output: McpOutputTransport | None = None
        self._turn: Turn | None = None
        self._ready = asyncio.Event()
        self._ended = False

    async def chat(self, line: str, notify: Notify) -> str:
        """Runs one turn and gives its reply. Empty line reads turn in flight."""
        await self._ready.wait()
        if self._ended:
            raise SessionEnded
        if line:
            await self._start_turn(line, notify)
        turn = self._turn
        if turn is None:
            return ""
        turn.notify = notify
        try:
            await turn.done.wait()
        finally:
            # caller that leaves notifies no more. Cancelled call keeps its turn to read
            turn.notify = None
        if turn is self._turn:
            self._turn = None
        if self._ended and not turn.text:
            raise SessionEnded
        return turn.text

    def input(self) -> McpInputTransport:
        if not self._input:
            self._input = McpInputTransport(self, self._params)
        return self._input

    def output(self) -> McpOutputTransport:
        if not self._output:
            self._output = McpOutputTransport(self, self._params)
        return self._output

    @property
    def ended(self) -> bool:
        return self._ended

    def open(self) -> None:
        """Opens this transport to chat calls. Input transport calls it on start."""
        self._ready.set()

    def close(self) -> None:
        """Ends this transport. Each waiter reads its reply so far."""
        self._ended = True
        self._ready.set()
        self.end_turn()

    def end_turn(self) -> None:
        """Ends turn in flight. Its waiter reads reply so far."""
        if self._turn:
            self._turn.end()

    def end_response(self) -> None:
        """Reports end of one model response."""
        if self._turn:
            self._turn.end_response()

    def start_calls(self) -> None:
        """Reports one round of function calls."""
        if self._turn:
            self._turn.start_calls()

    async def collect(self, text: str, *, spaced: bool) -> None:
        """Adds one reply piece to turn in flight."""
        if self._turn is None:
            logger.debug(f"{self}: no chat call waits for {text!r}")
            return
        await self._turn.add(text, spaced=spaced)

    async def _start_turn(self, line: str, notify: Notify) -> None:
        if self._turn and not self._turn.done.is_set():
            await self.input().interrupt()
            # no turn is replaced in flight, so no waiter of one waits on this line
            self.end_turn()
        self._turn = Turn(notify)
        await self.input().say(line)
        if self._ended:
            self.end_turn()


@dataclass
class McpRunnerArguments(RunnerArguments):
    """Session arguments MCP server gives to bot."""

    transport: McpTransport


class McpInputTransport(BaseInputTransport):
    """Turns one chat call into one user message on context of bot."""

    def __init__(self, transport: McpTransport, params: TransportParams, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self._transport = transport

    async def say(self, line: str) -> None:
        """Appends one user line to context and runs model."""
        # send-text path of RTVI. It bypasses VAD, so a voice bot takes this turn
        await self.push_frame(
            LLMMessagesAppendFrame(messages=[{"role": "user", "content": line}], run_llm=True)
        )

    async def interrupt(self) -> None:
        """Interrupts bot, then drains pipeline before next user line."""
        # drain lands commit of reply so far before next user line
        await self.broadcast_interruption()
        await self.pipeline_worker.flush_pipeline()

    async def start(self, frame: StartFrame) -> None:
        await super().start(frame)
        await self.set_transport_ready(frame)
        # llm service keeps skip_tts, so one push covers each turn of this session
        await self.push_frame(LLMConfigureOutputFrame(skip_tts=True))
        self._transport.open()


class McpOutputTransport(BaseOutputTransport):
    """Reads reply of turn in flight, the text tts skips, and 2 frames that end it."""

    def __init__(self, transport: McpTransport, params: TransportParams, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self._transport = transport

    async def write_transport_frame(self, frame: Frame) -> None:
        if isinstance(frame, TextFrame) and frame.skip_tts:
            await self._transport.collect(frame.text, spaced=frame.includes_inter_frame_spaces)
        elif isinstance(frame, LLMFullResponseEndFrame):
            self._transport.end_response()

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        """Reads 2 system frames that media sender never queues."""
        await super().process_frame(frame, direction)
        if isinstance(frame, FunctionCallsStartedFrame):
            self._transport.start_calls()
        elif isinstance(frame, InterruptionFrame):
            # interruption drops queued response end, so this turn ends here
            self._transport.end_turn()

    async def start(self, frame: StartFrame) -> None:
        await super().start(frame)
        await self.set_transport_ready(frame)

    async def stop(self, frame: EndFrame) -> None:
        await super().stop(frame)
        self._transport.close()

    async def cancel(self, frame: CancelFrame) -> None:
        await super().cancel(frame)
        self._transport.close()
