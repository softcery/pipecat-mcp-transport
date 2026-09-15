"""Server over streamable HTTP: 2 tools, one lock, one sweep, the slot and the header check."""

import asyncio
from time import monotonic
from typing import Any
from uuid import uuid4

import httpx
from mcp.server.transport_security import TransportSecuritySettings
from scripted import REPLY, TURN_SECONDS, Build, Echo, bot_of, hosting, serving, text

from pipecat_mcp_transport import McpBotServer, McpRunnerArguments
from pipecat_mcp_transport.server import HANDLE_LENGTH, HANDLE_SECONDS

LINE = "when do you open"
ALPHA = "alpha"
BRAVO = "bravo"
WORD_DELAY = 0.05
SWEEP_SECONDS = 0.05
GONE_SECONDS = 1.0
UNKNOWN = "not-a-handle"
OPEN = "0.0.0.0"  # noqa: S104
NAMED = "bot.example.com"
FOREIGN = "http://evil.test"
ACCEPT = "application/json, text/event-stream"
INITIALIZE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-11-25",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0"},
    },
}


async def test_the_stock_client_starts_a_session_and_reads_one_reply():
    async with serving(server()) as client:
        handle = await text(client, "start")
        reply = await text(client, "chat", handle=handle, line=LINE)

    assert reply == REPLY


async def test_the_call_sends_one_progress_notification_per_reply_piece():
    seen: list[str] = []

    async def notify(progress: float, total: float | None, message: str | None) -> None:
        seen.append(message or "")

    async with serving(server()) as client:
        handle = await text(client, "start")
        await client.call_tool("chat", {"handle": handle, "line": LINE}, progress_callback=notify)

    assert "".join(seen).strip() == REPLY


async def test_two_calls_on_one_handle_run_in_order_and_keep_their_replies_apart():
    async with serving(server(last_user_line, delay=WORD_DELAY)) as client:
        handle = await text(client, "start")
        replies = await asyncio.wait_for(
            asyncio.gather(
                text(client, "chat", handle=handle, line=ALPHA),
                text(client, "chat", handle=handle, line=BRAVO),
            ),
            timeout=TURN_SECONDS,
        )

    assert sorted(replies) == [ALPHA, BRAVO]


async def test_a_client_cancel_ends_the_handler_and_the_next_call_gives_the_whole_reply():
    async with serving(server(delay=WORD_DELAY)) as client:
        handle = await text(client, "start")
        call = asyncio.create_task(text(client, "chat", handle=handle, line=LINE))
        await asyncio.sleep(WORD_DELAY)
        started = monotonic()
        call.cancel()
        await asyncio.gather(call, return_exceptions=True)
        ended = monotonic() - started
        reply = await text(client, "chat", handle=handle, line="")

    assert ended < GONE_SECONDS
    assert reply == REPLY


async def test_a_handle_of_another_length_gives_an_error_that_names_the_field():
    async with serving(server()) as client:
        result = await client.call_tool("chat", {"handle": UNKNOWN, "line": LINE})

    assert result.is_error
    assert "handle" in text_of(result)
    assert f"{HANDLE_LENGTH} characters" in text_of(result)
    assert f"{len(UNKNOWN)}" in text_of(result)


async def test_a_handle_with_no_session_gives_an_error_that_names_the_lifetime():
    async with serving(server()) as client:
        result = await client.call_tool("chat", {"handle": str(uuid4()), "line": LINE})

    assert result.is_error
    assert "handle" in text_of(result)
    assert f"{HANDLE_SECONDS:.0f} seconds" in text_of(result)


async def test_the_sweep_drops_a_handle_no_call_returns_to():
    async with serving(server(handle_seconds=SWEEP_SECONDS)) as client:
        handle = await text(client, "start")
        await asyncio.sleep(GONE_SECONDS)
        result = await client.call_tool("chat", {"handle": handle, "line": LINE})

    assert result.is_error
    assert "no live session" in text_of(result)


async def test_a_start_call_over_the_session_cap_gives_an_error_that_names_the_cap():
    async with serving(server(sessions=1)) as client:
        await text(client, "start")
        result = await client.call_tool("start", {})

    assert result.is_error
    assert "1 open sessions" in text_of(result)


async def test_a_bot_that_raises_frees_its_slot_and_its_handle_names_the_next_call():
    async with serving(lambda port: McpBotServer(broken, port=port, sessions=1)) as client:
        first = await text(client, "start")
        second = await client.call_tool("start", {})
        result = await client.call_tool("chat", {"handle": first, "line": LINE})

    assert not second.is_error
    assert result.is_error
    assert "handle" in text_of(result)
    assert f"{HANDLE_SECONDS:.0f} seconds" in text_of(result)
    assert "Call start" in text_of(result)


async def test_a_request_with_a_foreign_origin_gets_403():
    async with hosting(server()) as url:
        refused = await status(url, origin=FOREIGN)

    assert refused == 403


async def test_an_open_bind_with_no_setting_answers_loopback_and_gives_421_on_another_host():
    async with hosting(server(host=OPEN)) as url:
        loopback = await status(url)
        named = await status(url, host=NAMED)

    assert loopback == 200
    assert named == 421


async def test_a_setting_that_names_one_host_answers_it_and_gives_403_to_a_foreign_origin():
    security = TransportSecuritySettings(
        allowed_hosts=[NAMED], allowed_origins=[f"https://{NAMED}"]
    )

    async with hosting(server(transport_security=security)) as url:
        named = await status(url, host=NAMED)
        foreign = await status(url, host=NAMED, origin=FOREIGN)

    assert named == 200
    assert foreign == 403


def server(answer=None, *, delay: float = 0.0, **options: Any) -> Build:
    """Gives one builder of server of scripted bot. options go to server as they are."""

    def build(port: int) -> McpBotServer:
        return McpBotServer(
            bot_of(lambda: Echo(answer, delay=delay)),
            port=port,
            sweep_seconds=SWEEP_SECONDS,
            **options,
        )

    return build


async def broken(runner_args: McpRunnerArguments) -> None:
    """Stands in for one bot that raises on start."""
    raise RuntimeError("scripted bot failure")


async def status(url: str, **headers: str) -> int:
    """Posts one initialize request with these headers. Gives its status code."""
    async with httpx.AsyncClient() as caller:
        answer = await caller.post(url, json=INITIALIZE, headers={"accept": ACCEPT, **headers})
    return answer.status_code


def last_user_line(messages) -> str:
    """Gives last user line of one context."""
    return next(message["content"] for message in reversed(messages) if message["role"] == "user")


def text_of(result) -> str:
    """Gives text of content blocks of one tool result."""
    return "".join(getattr(block, "text", "") for block in result.content)
