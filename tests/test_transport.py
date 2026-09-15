"""Turns on one transport, with no server: reply, history, tts skip, rounds and cancel."""

import asyncio

import pytest
from pipecat.frames.frames import Frame, LLMConfigureOutputFrame, LLMMessagesAppendFrame
from pipecat.processors.aggregators.llm_text_processor import LLMTextProcessor
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from scripted import REPLY, TURN_SECONDS, Echo, Mute, running

from pipecat_mcp_transport import McpTransport, SessionEndedError

LINE = "when do you open"
SECOND = "and on sunday"
SENTENCES = "We open at nine. We close at five."
WORD_DELAY = 0.05


async def quiet(count: int, text: str) -> None:
    """Takes one reply piece and keeps none."""


async def test_one_chat_call_gives_the_whole_reply_of_the_bot():
    async with running(Echo()) as transport:
        reply = await turn(transport, LINE)

    assert reply == REPLY


async def test_a_second_call_on_one_transport_reads_the_first_line_back():
    async with running(Echo(first_user_line)) as transport:
        await turn(transport, LINE)
        reply = await turn(transport, SECOND)

    assert reply == LINE


async def test_one_session_configures_the_model_output_once_to_skip_the_tts():
    spy = Spy()

    async with running(Echo(), watch=spy) as transport:
        await turn(transport, LINE)
        await turn(transport, SECOND)

    configured = [frame for frame in spy.seen if isinstance(frame, LLMConfigureOutputFrame)]
    appended = [frame for frame in spy.seen if isinstance(frame, LLMMessagesAppendFrame)]
    assert [frame.skip_tts for frame in configured] == [True]
    assert [frame.run_llm for frame in appended] == [True, True]


async def test_the_turns_of_one_session_pay_no_tts_request():
    mute = Mute()

    async with running(Echo(), tts=mute) as transport:
        reply = await turn(transport, LINE)
        await turn(transport, SECOND)

    assert reply == REPLY
    assert mute.calls == 0


async def test_a_text_processor_before_the_tts_gives_the_whole_reply():
    mute = Mute()

    async with running(
        Echo(lambda messages: SENTENCES), text=LLMTextProcessor(), tts=mute
    ) as transport:
        reply = await turn(transport, LINE)

    assert reply == SENTENCES
    assert mute.calls == 0


async def test_a_turn_with_one_function_call_ends_on_the_second_response():
    async with running(Echo(tools=1)) as transport:
        reply = await turn(transport, LINE)

    assert reply == REPLY


async def test_a_turn_with_2_function_call_rounds_ends_on_the_third_response():
    async with running(Echo(tools=2)) as transport:
        reply = await turn(transport, LINE)

    assert reply == REPLY


async def test_a_cancelled_call_leaves_the_whole_reply_for_the_next_call():
    async with running(Echo(delay=WORD_DELAY)) as transport:
        call = asyncio.create_task(turn(transport, LINE))
        await asyncio.sleep(WORD_DELAY)
        call.cancel()
        await asyncio.gather(call, return_exceptions=True)

        reply = await turn(transport, "")

    assert reply == REPLY


async def test_an_empty_line_after_a_read_turn_gives_an_empty_reply():
    async with running(Echo()) as transport:
        read = await turn(transport, LINE)
        again = await turn(transport, "")

    assert read == REPLY
    assert again == ""


async def test_a_line_that_interrupts_a_tool_call_leaves_the_next_turn_whole():
    async with running(Echo(tools=1, delay=WORD_DELAY)) as transport:
        call = asyncio.create_task(turn(transport, LINE))
        await asyncio.sleep(WORD_DELAY / 2)
        call.cancel()
        await asyncio.gather(call, return_exceptions=True)

        reply = await turn(transport, SECOND)

    assert reply == REPLY


async def test_a_call_on_a_transport_whose_worker_ended_raises():
    async with running(Echo()) as transport:
        await turn(transport, LINE)

    with pytest.raises(SessionEndedError):
        await turn(transport, SECOND)


async def turn(transport: McpTransport, line: str) -> str:
    """Runs one turn. Turn that hangs fails this test."""
    return await asyncio.wait_for(transport.chat(line, quiet), timeout=TURN_SECONDS)


def first_user_line(messages) -> str:
    """Gives first user line of one context."""
    return next(message["content"] for message in messages if message["role"] == "user")


class Spy(FrameProcessor):
    """Keeps each frame one chat call pushes, and passes it on."""

    def __init__(self) -> None:
        super().__init__()
        self.seen: list[Frame] = []

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)
        if isinstance(frame, LLMConfigureOutputFrame | LLMMessagesAppendFrame):
            self.seen.append(frame)
        await self.push_frame(frame, direction)
