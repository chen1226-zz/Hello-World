"""Reproduce the stale-field leak and prove the pool stays clean and cheap.

Run:  python3 leak_demo.py

Two very differently sized requests alternate:

* /echo round: large body, secret username and large amount;
* /ping round: empty body, empty username, zero amount.

If pooled objects were recycled without a full reset, the small response
would keep showing the previous round's username/amount/headers/body.  After
1000 rounds we also report how many Request/Response objects were ever
allocated and the resident delta measured by tracemalloc.
"""

import tracemalloc

from handler import Handler

ROUNDS = 1000
BIG = b"X" * 64 * 1024
SECRET = "alice-the-giant"


def run():
    handler = Handler()

    # Warm the pool up so steady-state rounds allocate nothing at all.
    handler.handle("", 0, {}, b"", path="/ping")
    handler.handle(SECRET, 1, {"X-Big": "yes"}, BIG, path="/echo")
    warm_allocs = handler.pool.allocations

    leaked = 0
    tracemalloc.start()

    def one_round(round_no):
        nonlocal leaked
        if round_no % 2 == 0:
            result = handler.handle(
                SECRET, 10_000 + round_no, {"X-Big": "yes"}, BIG, path="/echo"
            )
            if result.username != SECRET or result.amount != 10_000 + round_no:
                leaked += 1
            if result.body != BIG or result.headers.get("X-Echo") != "1":
                leaked += 1
        else:
            result = handler.handle("", 0, {}, b"", path="/ping")
            # These are the classic carry-over failures:
            if result.username != "" or result.amount != 0:
                leaked += 1
            if "username" in result.fields or "amount" in result.fields:
                leaked += 1
            if "X-Big" in result.headers or "X-Echo" in result.headers:
                leaked += 1
            if result.body != b"" or result.fields.get("echo_len"):
                leaked += 1

    WARM_ROUNDS = 100
    for round_no in range(ROUNDS):
        one_round(round_no)
        if round_no == WARM_ROUNDS - 1:
            steady = tracemalloc.take_snapshot()

    end = tracemalloc.take_snapshot()
    delta = sum(stat.size_diff for stat in end.compare_to(steady, "filename"))
    tracemalloc.stop()

    new_allocs = handler.pool.allocations - warm_allocs

    assert leaked == 0, f"{leaked} leaked fields detected"
    # Sequential reuse: no new pair may be built after warm-up.
    assert new_allocs == 0, f"pool allocated {new_allocs} extra pairs"
    # Once steady state is reached, the remaining 900 rounds must not grow
    # resident memory per round (allocator free-list noise stays bounded).
    assert abs(delta) < 4 * 1024, f"grew {delta} bytes over rounds {WARM_ROUNDS}-{ROUNDS}"

    print(f"OK: {ROUNDS} rounds, 0 leaked fields")
    print(f"proof: pool allocations={handler.pool.allocations} "
          f"(+{new_allocs} after warm-up), resident delta rounds "
          f"{WARM_ROUNDS}-{ROUNDS}={delta} bytes")


if __name__ == "__main__":
    run()
