"""Order service: parse upstream JSON orders, persist to sqlite, export as JSON.

Precision strategy (no floats anywhere):
- order ids are parsed as arbitrary-precision ints and stored as their
  canonical decimal text, so 18-19+ digit ids survive exactly;
- amounts are parsed as Decimal and stored as integer minor units plus a
  scale, so values like 1234.560 round-trip with trailing zeros intact.
"""

import json
from decimal import Decimal

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL UNIQUE,
    customer TEXT NOT NULL,
    amount_minor TEXT NOT NULL,
    amount_scale INTEGER NOT NULL,
    currency TEXT NOT NULL
)
"""

FIELDS = ("order_id", "customer", "amount", "currency")


def ensure_schema(conn):
    """Create the orders table if it does not exist."""
    conn.execute(SCHEMA)
    conn.commit()


def _parse_order(line):
    data = json.loads(line, parse_int=int, parse_float=Decimal)
    if not isinstance(data, dict):
        raise ValueError("order line must be a JSON object")
    missing = [key for key in FIELDS if key not in data]
    if missing:
        raise ValueError("missing fields: " + ", ".join(missing))
    order_id = data["order_id"]
    if isinstance(order_id, bool) or not isinstance(order_id, int):
        raise ValueError("order_id must be an integer")
    amount = data["amount"]
    if isinstance(amount, bool) or not isinstance(amount, (int, Decimal)):
        raise ValueError("amount must be a JSON number")
    for key in ("customer", "currency"):
        if not isinstance(data[key], str):
            raise ValueError(key + " must be a string")
    return {
        "order_id": order_id,
        "customer": data["customer"],
        "amount": Decimal(amount),
        "currency": data["currency"],
    }


def _split_amount(amount):
    """Exact Decimal -> (minor units int, scale int), preserving trailing zeros."""
    exponent = amount.as_tuple().exponent
    scale = -exponent if exponent < 0 else 0
    return int(amount.scaleb(scale)), scale


def _format_amount(minor, scale):
    """Inverse of _split_amount; 'f' format never uses exponent notation."""
    return format(Decimal(int(minor)).scaleb(-scale), "f")


def _render(order_id, customer, amount_minor, amount_scale, currency):
    return '{"order_id": %d, "customer": %s, "amount": %s, "currency": %s}' % (
        int(order_id),
        json.dumps(customer),
        _format_amount(amount_minor, amount_scale),
        json.dumps(currency),
    )


def ingest(conn, line):
    """Parse one JSON order line and store it. Idempotent per order_id.

    Returns True if the order was inserted, False if it already existed.
    Raises ValueError for malformed lines or invalid fields.
    """
    order = _parse_order(line)
    minor, scale = _split_amount(order["amount"])
    cursor = conn.execute(
        "INSERT OR IGNORE INTO orders"
        " (order_id, customer, amount_minor, amount_scale, currency)"
        " VALUES (?, ?, ?, ?, ?)",
        (str(order["order_id"]), order["customer"], str(minor), scale,
         order["currency"]),
    )
    conn.commit()
    return cursor.rowcount > 0


def export(conn):
    """Read all orders back and re-serialize as JSONL text, in ingest order."""
    rows = conn.execute(
        "SELECT order_id, customer, amount_minor, amount_scale, currency"
        " FROM orders ORDER BY seq"
    ).fetchall()
    return "\n".join(_render(*row) for row in rows)


def log_line(order):
    """Human-readable one-line summary of a parsed order dict."""
    return "order {} customer={} amount={} currency={}".format(
        order["order_id"], order["customer"],
        format(order["amount"], "f"), order["currency"])
