"""Replay harness: deliver the same batch of events twice (out of order,
with duplicates) and verify the result equals a single-pass summary."""

from __future__ import annotations

import random
import shutil
import sys
import tempfile

from consumer import connect, run

GIVE_SEED = 20260928
N_EVENTS = 60
ACCOUNTS = ("checking", "savings", "credit")


def make_events(n: int, seed: int) -> list[dict]:
    """Generate a deterministic batch of business events."""
    rng = random.Random(seed)
    return [
        {
            "event_id": f"G-{i:04d}",
            "account": rng.choice(ACCOUNTS),
            "amount": rng.randint(1, 10_000),
        }
        for i in range(n)
    ]


def make_delivery(batch: list[dict], seed: int) -> list[dict]:
    """Simulate a flaky >1x delivery: the whole batch is delivered twice,
    some events a third time, all in shuffled order."""
    rng = random.Random(seed)
    delivery = list(batch) + list(batch)
    delivery += rng.sample(batch, k=len(batch) // 3)  # extra duplicates
    rng.shuffle(delivery)
    return delivery


def summary_of(db_path: str) -> dict:
    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT account, SUM(amount) FROM ledger GROUP BY account ORDER BY account"
        ).fetchall()
        return dict(rows)
    finally:
        conn.close()


def expected_summary(batch: list[dict]) -> dict:
    """Summary of applying each unique event exactly once."""
    seen: dict[str, dict] = {}
    for event in batch:
        seen[event["event_id"]] = event
    out: dict[str, int] = {}
    for event in seen.values():
        out[event["account"]] = out.get(event["account"], 0) + event["amount"]
    return dict(sorted(out.items()))


def main() -> int:
    workdir = tempfile.mkdtemp(prefix="replay-")
    try:
        batch = make_events(N_EVENTS, GIVE_SEED)

        # 1. Deliver the same batch twice, with duplicates and out of order.
        delivery = make_delivery(batch, seed=GIVE_SEED + 1)
        db = f"{workdir}/replayed.db"
        applied = run(db, delivery)

        # 2. Expected: each event applied exactly once.
        expected = expected_summary(batch)
        actual = summary_of(db)

        print(f"delivered={len(delivery)} applied={applied} unique={len(batch)}")
        print(f"expected: {expected}")
        print(f"actual:   {actual}")

        if actual != expected:
            print("FAIL: replayed summary does not match single-pass summary")
            return 1
        print("PASS: replaying the same batch twice yields exactly-once results")
        return 0
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
