from __future__ import annotations

import datetime as dt
import json
from typing import TYPE_CHECKING, Any

from pipeline.observability import log_event

if TYPE_CHECKING:
    from pipeline.quality import QualityReport

RAW_TABLES = {
    "products": "raw.products_snapshot",
    "carts": "raw.carts_snapshot",
}


def build_rows(
    snapshot_date: dt.date, items: list[dict[str, Any]], run_id: str
) -> list[tuple[dt.date, int, str, str]]:
    return [(snapshot_date, item["id"], json.dumps(item), run_id) for item in items]


def replace_snapshot(
    conn: Any,
    resource: str,
    snapshot_date: dt.date,
    items: list[dict[str, Any]],
    run_id: str,
) -> int:
    """Reemplaza idempotentemente el snapshot de `resource` para `snapshot_date` en una transacción."""
    table = RAW_TABLES[resource]
    rows = build_rows(snapshot_date, items, run_id)
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table} WHERE snapshot_date = %s", (snapshot_date,))
            log_event(
                "raw_partition_cleared",
                f"Partición {snapshot_date} de {table} vaciada antes de insertar (re-ejecución idempotente)",
                table=table,
                snapshot_date=snapshot_date,
                previous_rows=cur.rowcount,
            )
            cur.executemany(
                f"INSERT INTO {table} (snapshot_date, id, payload, run_id) VALUES (%s, %s, %s::jsonb, %s)",
                rows,
            )
            cur.execute(
                """
                INSERT INTO raw.load_audit (snapshot_date, resource, row_count, run_id)
                VALUES (%s, %s, %s, %s)
                """,
                (snapshot_date, resource, len(rows), run_id),
            )
    log_event(
        "raw_partition_loaded",
        f"Partición {snapshot_date} de {table} cargada y commiteada",
        table=table,
        snapshot_date=snapshot_date,
        inserted_rows=len(rows),
        run_id=run_id,
    )
    return len(rows)


def save_quality_report(conn: Any, report: QualityReport, snapshot_date: dt.date, run_id: str) -> None:
    """Guarda una fila por regla (rule_accuracy) más una fila `file` con el accuracy del archivo."""
    rows = [
        (
            snapshot_date,
            report.resource,
            r.rule,
            r.field,
            r.checked,
            r.failed,
            r.expected_accuracy,
            r.measured_accuracy,
            r.passed,
            json.dumps(r.sample_failed_ids),
            run_id,
        )
        for r in report.results
    ]
    rows.append(
        (
            snapshot_date,
            report.resource,
            "file",
            "*",
            len(report.results),
            len(report.failed_rules),
            report.expected_accuracy,
            report.measured_accuracy,
            report.passed,
            "[]",
            run_id,
        )
    )
    with conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw.dq_results (
                    snapshot_date, resource, rule, field, checked, failed,
                    expected_accuracy, measured_accuracy, passed, sample_failed_ids, run_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                """,
                rows,
            )
