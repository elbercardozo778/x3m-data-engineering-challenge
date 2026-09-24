import datetime as dt
import json
from pathlib import Path

from pipeline.load import build_rows, replace_snapshot, save_quality_report
from pipeline.quality import evaluate, load_rules

FIXTURES = Path(__file__).parent / "fixtures"


def test_build_rows_keeps_full_payload_keyed_by_snapshot_date_and_id():
    carts = json.loads((FIXTURES / "carts.json").read_text())
    snapshot_date = dt.date(2026, 9, 23)

    rows = build_rows(snapshot_date, carts, run_id="manual__test")

    assert [(r[0], r[1], r[3]) for r in rows] == [(snapshot_date, c["id"], "manual__test") for c in carts]
    assert json.loads(rows[1][2]) == carts[1]


def test_payload_preserves_repeated_products_within_a_cart():
    carts = json.loads((FIXTURES / "carts.json").read_text())
    cart_7 = next(c for c in carts if c["id"] == 7)

    payload = json.loads(build_rows(dt.date(2026, 9, 23), [cart_7], run_id="r")[0][2])

    product_ids = [line["id"] for line in payload["products"]]
    assert product_ids.count(56) == 2


class _FakeCursor:
    rowcount = 0

    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params):
        self.log.append((" ".join(sql.split()), params))

    def executemany(self, sql, rows):
        self.log.append((" ".join(sql.split()), list(rows)))


class _FakeConnection:
    def __init__(self):
        self.log = []
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *exc):
        self.committed = exc_type is None
        return False

    def cursor(self):
        return _FakeCursor(self.log)


def test_replace_snapshot_deletes_partition_before_inserting_in_one_transaction():
    products = json.loads((FIXTURES / "products.json").read_text())
    conn = _FakeConnection()
    snapshot_date = dt.date(2026, 9, 23)

    loaded = replace_snapshot(conn, "products", snapshot_date, products, run_id="r")

    statements = [sql for sql, _ in conn.log]
    assert statements[0] == "DELETE FROM raw.products_snapshot WHERE snapshot_date = %s"
    assert statements[1].startswith("INSERT INTO raw.products_snapshot")
    assert statements[2].startswith("INSERT INTO raw.load_audit")
    assert len(conn.log[1][1]) == loaded == len(products)
    assert conn.committed


def test_replace_snapshot_logs_the_partition_it_replaces(caplog):
    products = json.loads((FIXTURES / "products.json").read_text())

    with caplog.at_level("INFO", logger="pipeline"):
        replace_snapshot(_FakeConnection(), "products", dt.date(2026, 9, 23), products, run_id="r")

    messages = "\n".join(caplog.messages)
    assert "[raw_partition_cleared] Partición 2026-09-23 de raw.products_snapshot vaciada" in messages
    assert "inserted_rows=3" in messages


def test_save_quality_report_writes_one_row_per_rule_plus_file_accuracy():
    products = json.loads((FIXTURES / "products.json").read_text())
    report = evaluate("products", products, len(products), load_rules()["products"])
    conn = _FakeConnection()

    save_quality_report(conn, report, dt.date(2026, 9, 23), run_id="r")

    sql, rows = conn.log[0]
    assert sql.startswith("INSERT INTO raw.dq_results")
    assert len(rows) == len(report.results) + 1
    assert rows[-1][2:9] == ("file", "*", len(report.results), 0, 1.0, 1.0, True)
    assert conn.committed
