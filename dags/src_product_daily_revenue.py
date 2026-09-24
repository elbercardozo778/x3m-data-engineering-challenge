from __future__ import annotations

import datetime as dt
import os
import time
from pathlib import Path
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import TaskGroup, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import InvocationMode, LoadMode, TestBehavior

from pipeline.client import BASE_URL, build_session, fetch_all
from pipeline.load import RAW_TABLES, replace_snapshot, save_quality_report
from pipeline.observability import log_event, log_group, on_task_failure
from pipeline.quality import DEFAULT_RULES_PATH, enforce, evaluate, load_rules
from pipeline.snapshot import StaleSnapshotError, check_snapshot_date

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE", "/opt/dbt-venv/bin/dbt")
WAREHOUSE_CONN_ID = "warehouse"
RESOURCES = ("products", "carts")

# En Airflow 3 un run manual puede no tener logical_date (y entonces no existe en el contexto del
# template); se usa la fecha en que se pidió el run. Ambos caminos leen dag_run para coincidir siempre.
SNAPSHOT_DATE = "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}"


def _snapshot_date() -> tuple[dt.date, str]:
    dag_run = get_current_context()["dag_run"]
    if dag_run.logical_date:
        return dag_run.logical_date.date(), "logical_date"
    return dag_run.run_after.date(), "run_after (run manual sin logical_date)"


def _warehouse_conn():
    return PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()


@task
def extract_resource(resource: str) -> dict[str, Any]:
    snapshot_date, date_source = _snapshot_date()
    try:
        check_snapshot_date(snapshot_date, today=dt.datetime.now(dt.UTC).date())
    except StaleSnapshotError as error:
        raise AirflowFailException(str(error)) from error
    log_event(
        "extract_started",
        f"Extrayendo {resource} para la partición snapshot_date={snapshot_date}",
        resource=resource,
        snapshot_date=snapshot_date,
        date_source=date_source,
        source=f"{BASE_URL}/{resource}",
    )
    with log_group(f"Páginas de {BASE_URL}/{resource}"):
        items, api_total = fetch_all(build_session(), resource)
    log_event("extract_finished", f"{resource}: {len(items)} registros extraídos", api_total=api_total)
    return {"items": items, "api_total": api_total}


# Sin reintentos: una regla que falla no se arregla volviendo a evaluar los mismos datos.
@task(retries=0)
def validate_resource(resource: str, extracted: dict[str, Any]) -> dict[str, Any]:
    snapshot_date, _ = _snapshot_date()
    report = evaluate(resource, extracted["items"], extracted["api_total"], load_rules()[resource])

    with log_group(f"Reglas de {DEFAULT_RULES_PATH.name} para {resource} (partición {snapshot_date})"):
        for r in report.results:
            log_event(
                "dq_rule",
                f"{'CUMPLE' if r.passed else 'NO CUMPLE'} {r.rule} en {r.field}: "
                f"rule_accuracy {r.measured_accuracy:.2%} (esperado {r.expected_accuracy:.0%})",
                checked=r.checked,
                failed=r.failed,
                sample_failed_ids=r.sample_failed_ids,
            )

    conn = _warehouse_conn()
    try:
        save_quality_report(conn, report, snapshot_date, get_current_context()["run_id"])
    finally:
        conn.close()

    log_event(
        "dq_evaluated",
        f"{'CUMPLE' if report.passed else 'NO CUMPLE'} {resource}: file_accuracy "
        f"{report.measured_accuracy:.2%} (esperado {report.expected_accuracy:.0%})",
        resource=resource,
        snapshot_date=snapshot_date,
        rules_passed=f"{len(report.results) - len(report.failed_rules)}/{len(report.results)}",
    )
    enforce(report)
    return {"file_accuracy": report.measured_accuracy, "failed_rules": len(report.failed_rules)}


@task
def load_resource(resource: str, extracted: dict[str, Any]) -> int:
    snapshot_date, _ = _snapshot_date()
    target_table = RAW_TABLES[resource]
    started = time.monotonic()
    with log_group(f"Carga en {target_table} (partición {snapshot_date})"):
        conn = _warehouse_conn()
        try:
            rows = replace_snapshot(
                conn, resource, snapshot_date, extracted["items"], get_current_context()["run_id"]
            )
        finally:
            conn.close()
    log_event(
        "raw_snapshot_loaded",
        f"Snapshot de {resource} para {snapshot_date} listo en {target_table}",
        resource=resource,
        snapshot_date=snapshot_date,
        rows=rows,
        duration_s=round(time.monotonic() - started, 2),
    )
    return rows


@dag(
    dag_id="src_product_daily_revenue",
    description="ELT diario de DummyJSON: extract, control de calidad, load en raw y transform con dbt.",
    # La fuente se actualiza a medianoche UTC; el margen de 15 min evita leerla a mitad de la actualización.
    schedule="15 0 * * *",
    start_date=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
    # La API no guarda historia: un backfill guardaría los datos de hoy bajo una fecha pasada.
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": dt.timedelta(minutes=1),
        "execution_timeout": dt.timedelta(minutes=15),
        "on_failure_callback": on_task_failure,
    },
    tags=["dummyjson", "dbt", "revenue"],
)
def src_product_daily_revenue():
    # Sin prefijo de grupo: cada tarea ya nombra su acción y su recurso, y el id se lee solo en la
    # CLI y en las rutas de log.
    with TaskGroup("extract", prefix_group_id=False):
        extracted = {r: extract_resource.override(task_id=f"extract_{r}")(r) for r in RESOURCES}

    with TaskGroup("control", prefix_group_id=False):
        controlled = {
            r: validate_resource.override(task_id=f"validate_{r}")(r, extracted[r]) for r in RESOURCES
        }

    with TaskGroup("load", prefix_group_id=False) as load:
        for r in RESOURCES:
            controlled[r] >> load_resource.override(task_id=f"load_{r}")(r, extracted[r])

    transform = DbtTaskGroup(
        group_id="transform",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
            manifest_path=DBT_PROJECT_DIR / "target" / "manifest.json",
        ),
        profile_config=ProfileConfig(
            profile_name="warehouse",
            target_name="docker",
            profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_EACH,
            should_detach_multiple_parents_tests=True,
        ),
        operator_args={
            "vars": {"snapshot_date": SNAPSHOT_DATE},
            "install_deps": False,
        },
        default_args={"retries": 0},
    )

    load >> transform


src_product_daily_revenue()
