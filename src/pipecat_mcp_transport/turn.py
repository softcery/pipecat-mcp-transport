"""One turn of one chat call: reply pieces, rounds and end of turn."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from pipecat.utils.string import TextPartForConcatenation, concatenate_aggregated_text

Notify = Callable[[int, str], Awaitable[None]]


class Turn:
    """One user line and reply of bot to it. It ends at zero rounds."""

    def __init__(self) -> None:
        self.done = asyncio.Event()
        self.notify: Notify | None = None
        self._pieces: list[TextPartForConcatenation] = []
        self._rounds = 0

    @property
    def text(self) -> str:
        return concatenate_aggregated_text(self._pieces).strip()

    async def add(self, text: str, *, spaced: bool) -> None:
        self._pieces.append(TextPartForConcatenation(text, includes_inter_part_spaces=spaced))
        if self.notify:
            await self.notify(len(self._pieces), text)

    def start_calls(self) -> None:
        self._rounds += 1

    def end_response(self) -> None:
        # response that started one round carries no reply
        if self._rounds:
            self._rounds -= 1
        else:
            self.end()

    def end(self) -> None:
        self._rounds = 0
        self.done.set()
