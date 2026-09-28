#!/usr/bin/env python3
"""Reproduction harness: deliver one batch twice (out of order + duplicates).

Compares the summary after one pass with the summary after the full
redelivery replay for both the legacy (broken) and fixed consumer.

Exit code is 0 only when the fixed consumer's replay summary is identical
to the single-pass summary.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from idempotent_consumer.consumer import consume_file
from idempotent_consumer.db import connect, grand_total, init_db, totals_by_account
from idempotent_consumer.events import gen_batch, replay_stream, write_jsonl
from idempotent_consumer.legacy import legacy_consume


def _summary(db_path: str) -> tuple[dict[str, int], int]:
    conn = connect(db_path)
    try:
        return totals_by_account(conn), grand_total(conn)
    finally:
        conn.close()


def _format(name: str, per_account: dict[str, int], total: int) -> str:
    detail = " ".join(f"{acct}={value}" for acct, value in sorted(per_account.items()))
    return f"{name:28} total={total:>6}  [{detail}]"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep-dir", help="optional directory to keep generated DBs")
    args = parser.parse_args()

    base = gen_batch()
    stream = replay_stream(base)
    unique_ids = {event.event_id for event in stream}
    print(
        f"stream: {len(base)} unique events delivered twice "
        f"({len(stream)} rows, {len(stream) - len(unique_ids)} duplicate rows)"
    )

    with tempfile.TemporaryDirectory() as tmp:
        one_pass = Path(tmp) / "one.jsonl"
        replay_path = Path(tmp) / "replay.jsonl"
        write_jsonl(str(one_pass), base)
        write_jsonl(str(replay_path), stream)

        if args.keep_dir:
            Path(args.keep_dir).mkdir(parents=True, exist_ok=True)
            write_jsonl(str(Path(args.keep_dir) / "one_pass.jsonl"), base)
            write_jsonl(str(Path(args.keep_dir) / "replay.jsonl"), stream)

        # ground truth: fixed consumer, exactly one delivery
        db_once = str(Path(tmp) / "once.db")
        init_db(db_once)
        consume_file(db_once, str(one_pass))
        once_accounts, once_total = _summary(db_once)

        # legacy consumer over the redelivery stream
        db_legacy = str(Path(tmp) / "legacy.db")
        legacy_consume(db_legacy, str(replay_path))
        legacy_accounts, legacy_total = _summary(db_legacy)

        # fixed consumer over the redelivery stream
        db_fixed = str(Path(tmp) / "fixed.db")
        init_db(db_fixed)
        applied, duplicates = consume_file(db_fixed, str(replay_path))
        fixed_accounts, fixed_total = _summary(db_fixed)

        if args.keep_dir:
            import shutil

            for name in ("once.db", "legacy.db", "fixed.db"):
                src = Path(tmp) / name
                for suffix in ("", "-wal", "-shm"):
                    candidate = Path(str(src) + suffix)
                    if candidate.exists():
                        shutil.copy2(candidate, Path(args.keep_dir) / (name + suffix))

    print(_format("single pass (ground truth)", once_accounts, once_total))
    print(_format("legacy after replay", legacy_accounts, legacy_total))
    print(
        _format("fixed after replay", fixed_accounts, fixed_total)
        + f"  applied={applied} duplicates={duplicates}"
    )

    ok = fixed_accounts == once_accounts and fixed_total == once_total
    legacy_bad = not (legacy_accounts == once_accounts and legacy_total == once_total)
    print()
    print(f"legacy double-counts: {'YES (bug reproduced)' if legacy_bad else 'no'}")
    print(f"fixed == single pass:  {'YES' if ok else 'NO'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
