"""MCP server of one pipecat bot: 2 tools, session table, sweep."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from time import monotonic
from typing import Any
from uuid import uuid4

import uvicorn
from loguru import logger
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from pipecat.transports.base_transport import TransportParams
from starlette.applications import Starlette
from starlette.routing import Mount

from pipecat_mcp_transport.transport import McpRunnerArguments, McpTransport, SessionEndedError
from pipecat_mcp_transport.turn import Notify

Bot = Callable[[McpRunnerArguments], Coroutine[Any, Any, None]]
Params = Callable[[], TransportParams]

HANDLE_SECONDS = 300.0
SWEEP_SECONDS = 30.0
HANDLE_LENGTH = 36
SESSIONS = 32

START_DESCRIPTION = """Open a conversation with the bot and return its handle.

Call this once per conversation, then pass the handle to every chat call. Treat the
handle as a secret: do not print it and do not send it anywhere else. A conversation
that takes no chat call for {seconds:.0f} seconds ends and its handle stops working."""

CHAT_DESCRIPTION = """Send one line to the bot and return its whole reply.

The bot keeps the history of the handle, so send the new line only. The calls of one
handle run in order, so a second call waits for the reply of the first. If you stop
waiting for a reply, send an empty line to read it, or send a new line to interrupt it.
A conversation that takes no chat call for {seconds:.0f} seconds ends."""


class McpBotServer:
    """Serves one pipecat bot to MCP clients over streamable HTTP.
    It owns session table, one handle and one lock per session, and sweep."""

    def __init__(
        self,
        bot: Bot,
        *,
        name: str = "pipecat",
        host: str = "127.0.0.1",
        port: int = 7870,
        path: str = "/mcp",
        params: Params = TransportParams,
        transport_security: TransportSecuritySettings | None = None,
        sessions: int = SESSIONS,
        handle_seconds: float = HANDLE_SECONDS,
        sweep_seconds: float = SWEEP_SECONDS,
    ) -> None:
        self._bot = bot
        self._host = host
        self._port = port
        self._path = path
        self._params = params
        self._security = _loopback() if transport_security is None else transport_security
        self._cap = sessions
        self._handle_seconds = handle_seconds
        self._sweep_seconds = sweep_seconds
        self._sessions: dict[str, Session] = {}
        self._app: Starlette | None = None
        self._mcp = MCPServer(name=name)
        self._register()

    def run(self) -> None:
        """Serves until this process ends."""
        logger.info(f"MCP transport: serving {self.url}")
        uvicorn.run(self.app(), host=self._host, port=self._port)

    def app(self) -> Starlette:
        """Gives ASGI app of this server. Its lifespan runs session manager and sweep."""
        if self._app is None:
            self._app = self._host_app()
        return self._app

    @property
    def url(self) -> str:
        """Gives streamable HTTP url of this server."""
        return f"http://{self._host}:{self._port}{self._path}"

    def _register(self) -> None:
        """Adds 2 tools of this server."""

        @self._mcp.tool(
            name="start",
            description=START_DESCRIPTION.format(seconds=self._handle_seconds),
            structured_output=False,
        )
        async def start() -> str:
            return self._open()

        @self._mcp.tool(
            name="chat",
            description=CHAT_DESCRIPTION.format(seconds=self._handle_seconds),
            structured_output=False,
        )
        async def chat(handle: str, context: Context, line: str = "") -> str:
            return await self._turn(handle, line, context)

    def _open(self) -> str:
        """Starts one bot on one transport. Gives handle of that session."""
        if len(self._sessions) >= self._cap:
            raise ToolError(self._full())
        handle = str(uuid4())
        transport = McpTransport(self._params())
        arguments = McpRunnerArguments(transport=transport)
        # sweep owns lifetime of one session, so worker takes no idle timeout
        arguments.pipeline_idle_timeout_secs = None
        task = asyncio.create_task(self._bot(arguments))
        task.add_done_callback(lambda done: self._stopped(handle, done))
        self._sessions[handle] = Session(transport=transport, task=task)
        logger.debug(f"MCP transport: one session opened, {len(self._sessions)} open")
        return handle

    async def _turn(self, handle: str, line: str, context: Context) -> str:
        """Runs one turn on one handle. One lock keeps turns of one handle in order."""
        session = self._session(handle)
        async with session.lock:
            session.used = monotonic()
            try:
                reply = await session.transport.chat(line, _progress(context))
            except SessionEndedError:
                self._drop(handle)
                raise ToolError(self._gone(handle)) from None
            session.used = monotonic()
            return reply

    def _session(self, handle: str) -> Session:
        """Gives session of one handle, or raises tool error."""
        session = self._sessions.get(handle)
        if session is None:
            raise ToolError(self._gone(handle))
        return session

    def _gone(self, handle: str) -> str:
        """Gives error text of one handle with no live session."""
        if len(handle) != HANDLE_LENGTH:
            return (
                f"handle: no live session. Expected the handle one start call gives, "
                f"{HANDLE_LENGTH} characters; received {len(handle)}."
            )
        return (
            f"handle: no live session. A session ends after {self._handle_seconds:.0f} seconds "
            f"with no chat call, or when its bot stops. Call start for a new handle."
        )

    def _full(self) -> str:
        """Gives error text of one server at its cap of sessions."""
        return (
            f"sessions: no free session. This server holds {self._cap} open sessions, its cap. "
            f"A session with no chat call for {self._handle_seconds:.0f} seconds ends."
        )

    def _stopped(self, handle: str, done: asyncio.Task) -> None:
        """Drops handle of bot that stopped, which frees its slot. Reports bot that raised."""
        self._drop(handle)
        if not done.cancelled() and done.exception():
            logger.error(f"MCP transport: the bot of one session raised {done.exception()!r}")

    def _drop(self, handle: str) -> None:
        """Drops one handle and ends its session."""
        session = self._sessions.pop(handle, None)
        if session is None:
            return
        session.transport.close()
        session.task.cancel()

    def _over(self, session: Session) -> bool:
        """Gives whether no call holds one session and none returned inside its lifetime."""
        return not session.lock.locked() and monotonic() - session.used > self._handle_seconds

    async def _sweep(self) -> None:
        """Drops each handle no call returns to, for as long as this server serves."""
        while True:
            await asyncio.sleep(self._sweep_seconds)
            try:
                for handle in [h for h, s in self._sessions.items() if self._over(s)]:
                    logger.debug("MCP transport: the sweep drops one session")
                    self._drop(handle)
            except Exception as error:  # noqa: BLE001
                logger.error(f"MCP transport: one sweep round raised {error!r}")

    async def _shut(self) -> None:
        """Drops each handle, then waits for each bot task to end."""
        tasks = [session.task for session in self._sessions.values()]
        for handle in list(self._sessions):
            self._drop(handle)
        await asyncio.gather(*tasks, return_exceptions=True)

    def _host_app(self) -> Starlette:
        """Builds host app around streamable HTTP app of MCP server."""
        served = self._mcp.streamable_http_app(
            streamable_http_path=self._path, transport_security=self._security
        )

        @asynccontextmanager
        async def serving(_: Starlette) -> AsyncIterator[None]:
            async with served.router.lifespan_context(served):
                sweep = asyncio.create_task(self._sweep())
                try:
                    yield
                finally:
                    sweep.cancel()
                    await asyncio.gather(sweep, return_exceptions=True)
                    await self._shut()

        return Starlette(routes=[Mount("", app=served)], lifespan=serving)


@dataclass
class Session:
    """One handle: bot of one conversation, its transport, its task and its lock."""

    transport: McpTransport
    task: asyncio.Task
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    used: float = field(default_factory=monotonic)


def _progress(context: Context) -> Notify:
    """Gives notifier of one call. One progress notification per reply piece."""

    async def notify(count: int, text: str) -> None:
        try:
            await context.report_progress(count, message=text)
        except Exception as error:  # noqa: BLE001
            logger.debug(f"MCP transport: one progress notification failed, {error!r}")

    return notify


def _loopback() -> TransportSecuritySettings:
    """Gives loopback setting of MCP SDK: 3 loopback hosts on any port, their http origins."""
    hosts = ("127.0.0.1", "localhost", "[::1]")
    return TransportSecuritySettings(
        allowed_hosts=[f"{host}:*" for host in hosts],
        allowed_origins=[f"http://{host}:*" for host in hosts],
    )
