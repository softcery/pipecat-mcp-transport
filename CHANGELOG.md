# Changelog

## 0.1.0, 2026-09-15

- `McpTransport` on `BaseTransport`, with `McpInputTransport` and `McpOutputTransport` on the
  stock input and output bases. One transport is one session, and each chat call waits for the
  pipeline to start.
- The input transport pushes `LLMConfigureOutputFrame(skip_tts=True)` once, on start, so every
  turn of the session pays 0 tts requests. A call on a turn in flight interrupts the bot and
  drains the pipeline before the new line.
- The reply is each text frame with `skip_tts` set, so a pipeline with an `LLMTextProcessor` in
  front of the tts gives its sentences, joined with their spaces.
- The turn counts one round per `FunctionCallsStartedFrame` and ends on the
  `LLMFullResponseEndFrame` of the response that starts no round. An interruption ends the turn
  in flight.
- A turn that one call read is gone. A later call with an empty line gives an empty block, and a
  cancelled call keeps its turn to read.
- `McpTransport.chat` takes an optional notifier and sets it once, before the wait.
- `McpBotServer` with the `start` and `chat` tools on streamable HTTP, the session table, one
  lock per handle and the sweep. The sweep owns the lifetime of a session.
- The server drops the handle of a bot that stops, which frees its slot. A dropped handle gives
  one tool error that names the lifetime and the call `start`.
- The server binds localhost and checks the `Host` and `Origin` headers on each bind. It takes a
  `TransportSecuritySettings` of the MCP SDK as `transport_security`, and the loopback setting of
  the SDK with none. It caps the open sessions at 32.
- `McpRunnerArguments` on `RunnerArguments`, with `pipeline_idle_timeout_secs=None`, so a bot
  takes one branch and no idle frame set.
- One progress notification per reply piece. A client cancel ends the handler and keeps the
  session.
- A `py.typed` marker, a single-file bot and a latency bench under `examples/`, and a dev group
  with the tests.
