#!/usr/bin/env python3
"""Crash harness: child writes -> SIGKILL at a random point -> recover & verify.

Every record the child acknowledges ("ACK i") was fsync'd before the ack, so
after recovery every acked key must be present with its exact full value.
"""
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile

ROUNDS = 500
WRITES_PER_ROUND = 120
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from walstore import WalStore


def expected_value(round_no, i):
    return ("val-%d-%d-" % (round_no, i)) + "x" * (i % 89)


def child_main(dir_path, round_no, n):
    store = WalStore(dir_path, flush_every=50)
    for i in range(n):
        store.put(b"k%06d" % i, expected_value(round_no, i).encode())
        sys.stdout.write("ACK %d\n" % i)
        sys.stdout.flush()
    store.close()


def run_round(round_no, workdir):
    dir_path = os.path.join(workdir, "store")
    shutil.rmtree(dir_path, ignore_errors=True)
    proc = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "--child",
         dir_path, str(round_no), str(WRITES_PER_ROUND)],
        stdout=subprocess.PIPE, text=True, cwd=HERE)
    kill_after = random.randint(5, WRITES_PER_ROUND - 1)
    acked = []
    for line in proc.stdout:
        if line.endswith("\n") and line.startswith("ACK "):
            acked.append(int(line.split()[1]))
            if len(acked) >= kill_after and proc.poll() is None:
                proc.send_signal(signal.SIGKILL)
    proc.wait()
    lost = truncated = 0
    store = WalStore(dir_path, flush_every=50)  # "restart": recovery runs here
    for i in acked:
        got = store.get(b"k%06d" % i)
        if got is None:
            lost += 1
        elif got != expected_value(round_no, i).encode():
            truncated += 1
    store.put(b"post-crash", b"ok")  # store must stay writable after recovery
    if store.get(b"post-crash") != b"ok":
        lost += 1
    store.close()
    return lost, truncated


def main():
    random.seed(20260929)
    workdir = tempfile.mkdtemp(prefix="walcrash-", dir=HERE)
    total_lost = total_truncated = 0
    try:
        for round_no in range(ROUNDS):
            lost, truncated = run_round(round_no, workdir)
            total_lost += lost
            total_truncated += truncated
            if lost or truncated:
                print("round %d: lost=%d truncated=%d" % (round_no, lost, truncated))
        status = "OK" if total_lost == 0 and total_truncated == 0 else "FAIL"
        print("%s: %d rounds, %d lost, %d truncated"
              % (status, ROUNDS, total_lost, total_truncated))
        return 0 if status == "OK" else 1
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        child_main(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
    else:
        sys.exit(main())
