# pipecat-mcp-transport

[![PyPI version](https://img.shields.io/pypi/v/pipecat-mcp-transport?cacheSeconds=3600)](https://pypi.org/project/pipecat-mcp-transport)
[![Python versions](https://img.shields.io/pypi/pyversions/pipecat-mcp-transport?cacheSeconds=3600)](https://pypi.org/project/pipecat-mcp-transport)
[![Check workflow](https://img.shields.io/github/actions/workflow/status/softcery/pipecat-mcp-transport/check.yml?branch=main&label=check)](https://github.com/softcery/pipecat-mcp-transport/actions/workflows/check.yml)
[![License BSD 2-Clause](https://img.shields.io/github/license/softcery/pipecat-mcp-transport)](LICENSE)

An MCP transport for [pipecat](https://github.com/pipecat-ai/pipecat). One MCP client, for
example Claude Code, holds one text conversation with one pipecat bot. The server gives 2 tools,
`start` and `chat`, over streamable HTTP.

Tested with pipecat-ai 1.10.0 and mcp 2.2.0 on Python 3.12, 3.13 and 3.14.

## Install

```
pip install pipecat-mcp-transport
```

## Use

`McpBotServer` takes your bot function and serves it. Your bot takes its transport from the
runner arguments. That branch is the only edit an existing bot needs.

```python
from pipecat_mcp_transport import McpBotServer, McpRunnerArguments


async def bot(runner_args) -> None:
    if isinstance(runner_args, McpRunnerArguments):
        transport = runner_args.transport
    else:
        transport = await create_transport(runner_args, {"webrtc": voice_params})
    ...


if __name__ == "__main__":
    McpBotServer(bot).run()
```

`examples/bot.py` runs one stt llm tts bot on `http://127.0.0.1:7870/mcp`. It runs on the stock
webrtc transport too, from the same file. It reads `OPENAI_API_KEY`, and `OPENAI_BASE_URL` for
an OpenAI-shaped server of your own.

Add the server to an MCP client as a streamable HTTP server on that url. For Claude Code:

```
claude mcp add --transport http pipecat http://127.0.0.1:7870/mcp
```

## Tools

| tool | arguments | gives |
| --- | --- | --- |
| `start` | none | the handle of one new conversation |
| `chat` | `handle`, `line` | the whole reply of the bot to that line |

`start` runs your bot function in a task and gives a UUIDv4 handle. One handle is one session:
one pipeline, one context, one history. The handle is a bearer secret, so the server binds
localhost by default.

`chat` appends `line` to the context of the handle and runs the model. It gives the reply as one
text block when the turn ends. An empty `line` starts no turn and gives the reply of the turn in
flight. A turn that one call read is gone, so the next empty `line` gives an empty block.

## Turn end

The reply is each text frame with `skip_tts` set that reaches the output transport. That is the
`LLMTextFrame` pieces of the model, or the `AggregatedTextFrame` sentences of an
`LLMTextProcessor` in front of the tts. The output joins them as the pipecat aggregators do,
and answers the call on `LLMFullResponseEndFrame`. A response that starts a round of function calls carries
no reply of its own. The turn counts each `FunctionCallsStartedFrame` and ends on the response
that starts no round, so a turn of 1 round and a turn of 3 rounds both give the whole reply.

The input transport pushes `LLMConfigureOutputFrame(skip_tts=True)` once, when the pipeline
starts. The llm service keeps that setting, so every turn of the session pays 0 tts requests and
no `TTSTextFrame` reaches the caller. A text bot and a voice bot end their turn on the same
frame.

## Interruption

A chat call on a handle with a turn in flight interrupts the bot and drains the pipeline before
it appends the new line. One lock per handle keeps the calls of that handle in order, so 2
concurrent calls run one after the other and neither reply carries the text of the other.

## Progress and cancel

The call sends one progress notification per reply piece. A caller with an idle window resets it
on each notification.

On streamable HTTP a caller cancels by closing the response stream. The tool handler ends, the
session stays and the turn runs on. A later `chat` call with an empty line gives the whole reply.

## Session lifetime

The sweep of the server owns the lifetime. It drops a handle that takes no chat call for 300
seconds, ends its transport and cancels its bot task. A call on a dropped handle gives a tool
error that names the field and the lifetime. `McpBotServer(bot, handle_seconds=600)` moves that
number.

The server gives the bot `pipeline_idle_timeout_secs=None`, so the idle timeout of the worker
runs on no session of this transport and your bot needs no idle frame set.

## Security

The server binds `127.0.0.1` and validates the `Origin` header. A request with a foreign origin
gets 403 and a request with a foreign host gets 421. `McpBotServer(bot, host="0.0.0.0")` drops
that protection and logs a warning, and the handle is then a bearer secret on an open port.
`McpBotServer(bot, host="0.0.0.0", origins=["https://app.example.com"])` takes the check back,
with the host header bound to the host and port of the server. Put an authenticating proxy in
front of an open bind.

`McpBotServer(bot, sessions=64)` moves the cap on open sessions. The default is 32, and a
`start` call over the cap gives a tool error that names it.

## Cost

Seconds from one chat call to its reply, median of 5 runs on one x86-64 workstation, Python
3.14.7. The model is a local stub that streams one word every 0.03 seconds and answers 7 words,
so 0.21 seconds of each row is model pace.

| shape | median | min | max |
| --- | --- | --- | --- |
| 1 turn | 0.292 s | 0.289 s | 0.792 s |
| 5 turns on one handle | 1.446 s | 1.442 s | 1.448 s |
| 1 turn with 1 tool call | 0.385 s | 0.383 s | 0.385 s |

The first run of a shape waits for its pipeline to start, which the max of the 1 turn row holds.

`python examples/bench.py --url <openai url> --out rows.jsonl --sha <commit>` writes one row.

## Limits

- The reply lands as one text block after the turn. No token streams to the caller.
- Markup tags the bot writes reach the caller in the text block unless the bot strips them.
- An interrupted turn gives its text so far with no marker. A caller reads it as whole.
- The greeting a bot speaks on client ready reaches no caller. The first turn starts on the
  first chat call.
- A chat call carries no audio block and no image block. The bot pays stt on nothing.
- A function handler that does not run the model again leaves the call waiting. The caller
  cancels, and the next call with an empty line gives the reply so far.
- The collector awaits each progress notification, so a slow caller slows its own session.
- A session lives in one process. A second replica needs sticky routing on the handle.
- Claude Code caps a tool result at 25 000 tokens and ends a call after 5 minutes with no
  output. A long reply needs a shorter system prompt.
- The client tool of a bot has no caller on MCP, and pipecat marks no function call as one.
  Register no client tool.

## Develop

- The package ships a `py.typed` marker.
- `make lint` checks the lock, the format, the lint rules, and the types with pyright.
- `make test` runs the tests. `make test-lowest` runs them on the lowest allowed pipecat-ai,
  mcp, starlette and uvicorn. CI runs both.
- `make audit` checks `uv.lock` for known vulnerabilities.
- A `v` tag publishes the package to PyPI.

## License

BSD 2-Clause. [Softcery](https://softcery.com) builds and maintains the package.
