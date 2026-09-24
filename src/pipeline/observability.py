from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from typing import Any

logger = logging.getLogger("pipeline")


def log_event(event: str, message: str, **fields: Any) -> None:
    """Loguea un mensaje legible seguido de los campos en formato key=value (logfmt)."""
    detail = " ".join(f"{key}={value}" for key, value in sorted(fields.items()))
    logger.info("[%s] %s | %s", event, message, detail)


@contextlib.contextmanager
def log_group(title: str) -> Iterator[None]:
    # La UI de Airflow muestra las líneas entre ::group:: y ::endgroup:: como una sección plegable.
    logger.info("::group::%s", title)
    try:
        yield
    finally:
        logger.info("::endgroup::")


def on_task_failure(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    log_event(
        "task_failed",
        "La tarea falló",
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        try_number=ti.try_number,
        error=repr(context.get("exception")),
    )
