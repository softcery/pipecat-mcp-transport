<div align="center">
  <a href="https://github.com/pipecat-ai/pipecat">
    <img alt="Pipecat" width="220px" height="auto" src="https://raw.githubusercontent.com/pipecat-ai/pipecat/main/pipecat.png">
  </a>
  <p><em>A <a href="https://docs.pipecat.ai/api-reference/server/services/transport/mcp">community integration</a> for <a href="https://github.com/pipecat-ai/pipecat">Pipecat</a></em></p>
</div>

# pipecat-mcp-transport

[![PyPI version](https://img.shields.io/pypi/v/pipecat-mcp-transport?cacheSeconds=3600)](https://pypi.org/project/pipecat-mcp-transport) [![Python versions](https://img.shields.io/pypi/pyversions/pipecat-mcp-transport?cacheSeconds=3600)](https://pypi.org/project/pipecat-mcp-transport) [![Check workflow](https://img.shields.io/github/actions/workflow/status/softcery/pipecat-mcp-transport/check.yml?branch=main&label=check)](https://github.com/softcery/pipecat-mcp-transport/actions/workflows/check.yml) [![License BSD 2-Clause](https://img.shields.io/github/license/softcery/pipecat-mcp-transport)](https://github.com/softcery/pipecat-mcp-transport/blob/main/LICENSE)

An MCP transport for [pipecat](https://github.com/pipecat-ai/pipecat). It serves your pipecat bot as an MCP server, so any agent with an MCP client, for example Claude Code, talks to the bot. The bot keeps its pipeline, its tools and its history. The server exposes 2 tools, `start` and `chat`, over streamable HTTP.

Tested with pipecat 1.10.0 and mcp 2.2.0 on Python 3.12, 3.13 and 3.14.

## Install

```
pip install pipecat-mcp-transport
```

## Use

`McpBotServer` serves your bot function. The bot reads its transport from the runner arguments, and that branch is the only edit an existing bot needs.

```python
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import create_transport
from pipecat.transports.base_transport import TransportParams

from pipecat_mcp_transport import McpBotServer, McpRunnerArguments

voice_params = TransportParams(audio_in_enabled=True, audio_out_enabled=True)


async def bot(runner_args: RunnerArguments) -> None:
    if isinstance(runner_args, McpRunnerArguments):
        transport = runner_args.transport
    else:
        transport = await create_transport(runner_args, {"webrtc": voice_params})
    ...


if __name__ == "__main__":
    McpBotServer(bot).run()
```

`examples/bot.py` runs one stt llm tts bot on `http://127.0.0.1:7870/mcp`, and the same file runs on the stock webrtc transport. It reads `OPENAI_API_KEY`, and `OPENAI_BASE_URL` for an OpenAI-shaped server of your own. The example needs 4 pipecat extras and a clone of this repository, since the wheel has no `examples/`.

```
uv run --with "pipecat-ai[runner,webrtc,openai,silero]" examples/bot.py
```

Add the server to an MCP client as a streamable HTTP server on that url. For Claude Code:

```
claude mcp add --transport http pipecat http://127.0.0.1:7870/mcp
```

## Tools

| tool | arguments | returns |
| --- | --- | --- |
| `start` | none | the handle of one new conversation |
| `chat` | `handle`, `line` | the whole reply of the bot to that line |

`start` runs your bot function in a task and returns a UUIDv4 handle. One handle is one session: one pipeline, one context, one history. The handle is a bearer secret, so the server binds localhost by default.

`chat` appends `line` to the context of the handle and runs the model. It returns the reply as one text block when the turn ends. An empty `line` starts no turn and returns the reply of the turn in flight. A turn that one call read is gone, so the next empty `line` returns an empty block.

## Turn end

The reply is each text frame with `skip_tts` set that reaches the output transport: the `LLMTextFrame` pieces of the model, or the `AggregatedTextFrame` sentences of an `LLMTextProcessor` in front of the tts. The output joins them as the pipecat aggregators do, and answers the call on `LLMFullResponseEndFrame`.

A response that starts a round of function calls carries no reply of its own. The turn counts each `FunctionCallsStartedFrame` and ends on the response that starts no round, so a turn of 1 round and a turn of 2 rounds both give the whole reply. That frame is a system frame, so it reaches the output before the end frame of its response. `FunctionCallInProgressFrame` comes from the task of each call and reaches the output after that end frame, so the turn reads no round from it.

The input transport pushes `LLMConfigureOutputFrame(skip_tts=True)` once, when the pipeline starts. The llm service keeps that setting, so no turn of the session costs a tts request and no `TTSTextFrame` reaches the caller. A text bot and a voice bot end their turn on the same frame.

## Interruption

A chat call on a handle with a turn in flight interrupts the bot and drains the pipeline before it appends the new line. One lock per handle keeps the calls of that handle in order, so 2 concurrent calls run one after the other and neither reply carries the text of the other.

## Progress and cancel

The call sends one progress notification per reply piece, so a caller with an idle window resets it as the reply arrives.

On streamable HTTP a caller cancels by closing the response stream. The tool handler ends, the session stays and the turn runs on. A later `chat` call with an empty line returns the whole reply.

## Session lifetime

A sweep on the server drops a handle that takes no chat call for 300 seconds, ends its transport and cancels its bot task. `McpBotServer(bot, handle_seconds=600)` changes that number.

When a bot returns or raises, the server drops its handle, so its slot is free for the next `start` call. A call on a dropped handle returns one tool error for both causes. The error names the handle, the lifetime and the next call, `start`.

The cap on open sessions is 32, and `McpBotServer(bot, sessions=64)` changes it. A `start` call over the cap returns a tool error that names it.

The server gives the bot `pipeline_idle_timeout_secs=None`, so the idle timeout of the worker runs on no session of this transport and your bot needs no idle frame set.

## Security

The server binds `127.0.0.1` by default, and on any bind it checks the `Host` and `Origin` headers of each request. A request with a foreign host gets 421, and a request with a foreign origin gets 403.

With no `transport_security`, the server takes the loopback setting of the MCP SDK: the hosts `127.0.0.1`, `localhost` and `[::1]` on any port, and their `http` origins. A server on `host="0.0.0.0"` keeps that setting, so a caller that dials another name gets 421.

If a caller dials another name, pass a `TransportSecuritySettings` of the MCP SDK that names it:

```python
from mcp.server.transport_security import TransportSecuritySettings

security = TransportSecuritySettings(
    allowed_hosts=["bot.example.com"],
    allowed_origins=["https://app.example.com"],
)
McpBotServer(bot, host="0.0.0.0", transport_security=security).run()
```

A host entry that ends in `:*` takes any port. `enable_dns_rebinding_protection=False` turns both checks off. The handle is a bearer secret, so put an authenticating proxy in front of an open bind.

## Cost

Seconds from one chat call to its reply, median of 5 runs on one x86-64 workstation, Python 3.14.7. The model is a local stub that streams one word every 0.03 seconds and answers 7 words, so 0.21 seconds of each row is model pace.

| shape | median | min | max |
| --- | --- | --- | --- |
| 1 turn | 0.292 s | 0.289 s | 0.792 s |
| 5 turns on one handle | 1.446 s | 1.442 s | 1.448 s |
| 1 turn with 1 tool call | 0.385 s | 0.383 s | 0.385 s |

The first run of a shape waits for its pipeline to start, which the max of the 1 turn row shows.

`python examples/bench.py --url <openai url> --out rows.jsonl --sha <commit>` writes one row.

## Limits

- The reply arrives as one text block after the turn, with no token streaming.
- Markup tags the bot writes reach the caller in the text block unless the bot strips them.
- An interrupted turn returns its text so far with no marker, so a caller cannot tell it from a whole reply.
- The greeting a bot speaks on client ready reaches no caller, so the first turn starts on the first chat call.
- A chat call carries no audio block and no image block, so the bot runs no stt.
- A function handler that does not run the model again leaves the call waiting. The caller cancels, and the next call with an empty line returns the reply so far.
- A response that calls only the cancel tool of an async tool starts no round, because pipecat sends no `FunctionCallsStartedFrame` for that tool. The turn ends on that response, and the reply after the cancel reaches no caller.
- A new line replaces a done turn that no call read. The reply of that turn reaches no caller.
- The server awaits each progress notification, so a slow caller slows its own session.
- A session lives in one process. A second replica needs sticky routing on the handle.
- Claude Code caps a tool result at 25 000 tokens and ends a call after 5 minutes with no output. A bot that writes long replies needs a system prompt that keeps them under the cap.
- A client-side tool has no client on this transport, and pipecat marks no function call as client-side, so register none.

## Develop

- The package ships a `py.typed` marker.
- `make lint` checks the lock, the format, the lint rules, and the types with pyright.
- `make test` runs the tests. `make test-lowest` runs them on the lowest allowed pipecat, mcp, starlette and uvicorn. CI runs both.
- `make audit` checks `uv.lock` for known vulnerabilities.
- `make build` builds the wheel and the sdist into `dist/`. CI runs it on each push to `main`.
- A `v` tag that matches the project version publishes the package to PyPI.

## License

BSD 2-Clause. [Softcery](https://softcery.com) builds and maintains the package.
