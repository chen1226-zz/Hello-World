"""Acceptance: parse -> store -> read back -> re-serialize must be identical."""

import sqlite3

import ordersvc

SAMPLES = "samples/samples.jsonl"


def main():
    with open(SAMPLES, encoding="utf-8") as fh:
        lines = [ln.rstrip("\n") for ln in fh if ln.strip()]
    conn = sqlite3.connect(":memory:")
    ordersvc.ensure_schema(conn)
    for line in lines:
        ordersvc.ingest(conn, line)
    exported = ordersvc.export(conn).splitlines()
    ok = sum(1 for original, out in zip(lines, exported) if original == out)
    if ok == len(lines) == len(exported):
        print("OK: %d/%d samples identical" % (ok, len(lines)))
        return
    print("FAIL: %d/%d samples identical" % (ok, len(lines)))
    for index, (original, out) in enumerate(zip(lines, exported)):
        if original != out:
            print("  line %d:\n    in : %s\n    out: %s"
                  % (index + 1, original, out))
    raise SystemExit(1)


if __name__ == "__main__":
    main()
