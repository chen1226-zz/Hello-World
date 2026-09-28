"""Upstream event model and deterministic batch generation.

The replay harness feeds the *same* file to the consumer more than once,
including duplicated and out-of-order rows, to simulate at-least-once
delivery.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Event:
    """One upstream business event.

    ``event_id`` is assigned upstream, stable across every redelivery and
    therefore the only correct idempotency key. ``emitted_at`` is upstream
    metadata and MUST NOT participate in the key.
    """

    event_id: str
    account: str
    amount: int  # signed integer cents; positive charges the account
    emitted_at: str

    def to_json(self) -> str:
        return json.dumps(self.__dict__, sort_keys=True)

    @classmethod
    def from_json(cls, line: str) -> "Event":
        data = json.loads(line)
        return cls(
            event_id=data["event_id"],
            account=data["account"],
            amount=int(data["amount"]),
            emitted_at=data["emitted_at"],
        )


def gen_batch() -> list[Event]:
    """Deterministic base batch (20 unique events across 4 accounts)."""
    events: list[Event] = []
    for i in range(20):
        events.append(
            Event(
                event_id=f"evt-{i:04d}",
                account=f"acct-{i % 4}",
                amount=(i % 7 + 1) * 100 * (1 if i % 3 else -1),
                emitted_at=f"2026-09-28T10:{i:02d}:00Z",
            )
        )
    return events


def replay_stream(base: Iterable[Event]) -> list[Event]:
    """Build the redelivery stream: shuffled base + exact redelivery.

    The second pass arrives out of order and contains intra-batch
    duplicates, mirroring real at-least-once upstream behaviour.
    """
    ordered = list(base)
    redelivery = list(reversed(ordered))
    redelivery.append(ordered[7])
    redelivery.append(ordered[3])
    return ordered + redelivery


def write_jsonl(path: str, events: Iterable[Event]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for event in events:
            fh.write(event.to_json() + "\n")


def read_jsonl(path: str) -> list[Event]:
    out: list[Event] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(Event.from_json(line))
    return out
