"""Generate samples/samples.jsonl: 50 orders stressing numeric precision.

Covers 18-19+ digit order ids (beyond float53 and beyond int64), trailing
zeros, negatives, and very large/small amounts. Deterministic output.
"""

import json
import os
import random

TEMPLATE = '{"order_id": %d, "customer": %s, "amount": %s, "currency": %s}'

CUSTOMERS = ["Alice", "Bob", "Carol", "Dave", "Eve", "张伟", "Mallory"]
CURRENCIES = ["CNY", "USD", "EUR", "JPY"]

FIXED_IDS = [
    9007199254740993,           # 2**53 + 1: first int float64 cannot represent
    9223372036854775807,        # int64 max, 19 digits
    123456789012345678901234567890,  # 30 digits, beyond int64
]

FIXED_AMOUNTS = [
    "1234.560",     # trailing zero must survive
    "0.01",
    "-0.01",
    "99999999999999999.99",   # huge: minor units overflow int64
    "0.0000001",    # tiny
    "-9999.9950",   # negative with trailing zero
    "100",          # integer amount
    "7.50",
]


def main():
    rng = random.Random(10996)
    ids = list(FIXED_IDS)
    seen = set(ids)
    while len(ids) < 50:
        candidate = rng.randrange(10**17, 10**19)  # 18-19 digits
        if candidate not in seen:
            seen.add(candidate)
            ids.append(candidate)

    amounts = list(FIXED_AMOUNTS)
    while len(amounts) < 50:
        whole = rng.randrange(0, 10**12)
        frac = rng.randrange(0, 10**6)
        if rng.random() < 0.4:
            frac = (frac // 10) * 10  # force a trailing zero
        text = "%d.%06d" % (whole, frac)
        if rng.random() < 0.2:
            text = "-" + text
        amounts.append(text)

    lines = [
        TEMPLATE % (
            ids[i],
            json.dumps(CUSTOMERS[i % len(CUSTOMERS)]),
            amounts[i],
            json.dumps(CURRENCIES[i % len(CURRENCIES)]),
        )
        for i in range(50)
    ]
    path = os.path.join(os.path.dirname(__file__), "samples.jsonl")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print("wrote %d samples to %s" % (len(lines), path))


if __name__ == "__main__":
    main()
