import json
import sqlite3
import unittest

import ordersvc


def make_conn():
    conn = sqlite3.connect(":memory:")
    ordersvc.ensure_schema(conn)
    return conn


def order_line(order_id, amount, customer="Alice", currency="CNY"):
    return '{"order_id": %s, "customer": %s, "amount": %s, "currency": %s}' % (
        order_id, json.dumps(customer), amount, json.dumps(currency))


class RoundTripTests(unittest.TestCase):
    def roundtrip(self, line):
        conn = make_conn()
        self.assertTrue(ordersvc.ingest(conn, line))
        return ordersvc.export(conn)

    def test_19_digit_order_id_exact(self):
        line = order_line(9223372036854775807, "1.00")
        self.assertEqual(self.roundtrip(line), line)

    def test_order_id_beyond_int64(self):
        line = order_line(123456789012345678901234567890, "2.50")
        self.assertEqual(self.roundtrip(line), line)

    def test_order_id_beyond_float53(self):
        line = order_line(9007199254740993, "0.01")  # 2**53 + 1
        self.assertEqual(self.roundtrip(line), line)

    def test_negative_amount(self):
        line = order_line(1001, "-0.01")
        self.assertEqual(self.roundtrip(line), line)

    def test_trailing_zeros_preserved(self):
        line = order_line(1002, "1234.560")
        self.assertEqual(self.roundtrip(line), line)

    def test_huge_amount(self):
        line = order_line(1003, "99999999999999999.99")
        self.assertEqual(self.roundtrip(line), line)

    def test_tiny_amount(self):
        line = order_line(1004, "0.0000001")
        self.assertEqual(self.roundtrip(line), line)

    def test_integer_amount(self):
        line = order_line(1005, "100")
        self.assertEqual(self.roundtrip(line), line)

    def test_duplicate_ingest_is_idempotent(self):
        conn = make_conn()
        line = order_line(2001, "9.99")
        self.assertTrue(ordersvc.ingest(conn, line))
        self.assertFalse(ordersvc.ingest(conn, line))
        self.assertEqual(ordersvc.export(conn), line)

    def test_export_preserves_ingest_order(self):
        conn = make_conn()
        lines = [order_line(3000 + i, "%d.10" % i) for i in range(5)]
        for line in lines:
            ordersvc.ingest(conn, line)
        self.assertEqual(ordersvc.export(conn), "\n".join(lines))


class ValidationTests(unittest.TestCase):
    def assert_rejected(self, line):
        conn = make_conn()
        with self.assertRaises(ValueError):
            ordersvc.ingest(conn, line)
        self.assertEqual(ordersvc.export(conn), "")

    def test_missing_field_rejected(self):
        self.assert_rejected('{"order_id": 1, "customer": "Alice",'
                             ' "currency": "CNY"}')

    def test_invalid_json_rejected(self):
        self.assert_rejected("not json at all")

    def test_non_object_rejected(self):
        self.assert_rejected("[1, 2, 3]")

    def test_float_order_id_rejected(self):
        self.assert_rejected(order_line(1.5, "1.00"))

    def test_string_order_id_rejected(self):
        self.assert_rejected(order_line('"123"', "1.00"))

    def test_string_amount_rejected(self):
        self.assert_rejected(order_line(1, '"1.00"'))

    def test_bool_amount_rejected(self):
        self.assert_rejected(order_line(1, "true"))

    def test_non_string_customer_rejected(self):
        self.assert_rejected(order_line(1, "1.00", customer=42))


class LogLineTests(unittest.TestCase):
    def test_log_line_format(self):
        order = ordersvc._parse_order(order_line(42, "1234.560", "张伟"))
        self.assertEqual(
            ordersvc.log_line(order),
            "order 42 customer=张伟 amount=1234.560 currency=CNY")


if __name__ == "__main__":
    unittest.main()
