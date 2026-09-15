"""One stt llm tts bot one MCP client talks to. Run it, then point the client at its url.
It runs on the stock webrtc transport too. MCP adds one branch and no other line."""

import os

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import create_transport
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.services.openai.stt import OpenAISTTService
from pipecat.services.openai.tts import OpenAITTSService
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.workers.runner import WorkerRunner

from pipecat_mcp_transport import McpBotServer, McpRunnerArguments

PROMPT = "You are a helpful assistant. Answer in one short sentence."


async def bot(runner_args: RunnerArguments) -> None:
    """Runs one session. One chat call is one turn."""
    transport, turn = await transport_of(runner_args)
    key = os.environ["OPENAI_API_KEY"]
    url = os.environ.get("OPENAI_BASE_URL")
    context = LLMContext([{"role": "system", "content": PROMPT}])
    aggregators = LLMContextAggregatorPair(context, user_params=turn)
    pipeline = Pipeline(
        [
            transport.input(),
            OpenAISTTService(api_key=key, base_url=url),
            aggregators.user(),
            OpenAILLMService(api_key=key, base_url=url),
            OpenAITTSService(api_key=key, base_url=url),
            transport.output(),
            aggregators.assistant(),
        ]
    )
    worker = PipelineWorker(pipeline, idle_timeout_secs=runner_args.pipeline_idle_timeout_secs)
    runner = WorkerRunner(handle_sigint=runner_args.handle_sigint)
    await runner.add_workers(worker)
    await runner.run()


async def transport_of(
    runner_args: RunnerArguments,
) -> tuple[BaseTransport, LLMUserAggregatorParams]:
    """Gives the transport of one session and its turn params. Only audio needs the VAD."""
    if isinstance(runner_args, McpRunnerArguments):
        return runner_args.transport, LLMUserAggregatorParams()
    transport = await create_transport(runner_args, {"webrtc": voice_params})
    return transport, LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer())


def voice_params() -> TransportParams:
    """Gives the transport params of one voice client."""
    return TransportParams(audio_in_enabled=True, audio_out_enabled=True)


if __name__ == "__main__":
    McpBotServer(bot).run()
