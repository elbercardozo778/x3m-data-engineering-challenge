from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

DEFAULT_RULES_PATH = Path(__file__).resolve().parents[2] / "data_quality" / "rules.json"
DEFAULT_ACCURACY = 1.0
TYPES: dict[str, tuple[type, ...]] = {"int": (int,), "number": (int, float), "str": (str,), "list": (list,)}
MAX_SAMPLE_IDS = 5


class DataQualityError(Exception):
    pass


@dataclasses.dataclass(frozen=True)
class RuleResult:
    rule: str
    field: str
    expected_accuracy: float
    checked: int
    failed: int
    sample_failed_ids: list[Any]

    @property
    def measured_accuracy(self) -> float:
        """Registros que cumplen la regla / registros chequeados."""
        return round(1 - self.failed / self.checked, 4) if self.checked else 1.0

    @property
    def passed(self) -> bool:
        return self.measured_accuracy >= self.expected_accuracy


@dataclasses.dataclass(frozen=True)
class QualityReport:
    resource: str
    results: list[RuleResult]
    expected_accuracy: float

    @property
    def failed_rules(self) -> list[RuleResult]:
        return [r for r in self.results if not r.passed]

    @property
    def measured_accuracy(self) -> float:
        """Reglas que cumplen / reglas totales del archivo."""
        if not self.results:
            return 1.0
        return round(1 - len(self.failed_rules) / len(self.results), 4)

    @property
    def passed(self) -> bool:
        return self.measured_accuracy >= self.expected_accuracy


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _in_range(value: Any, spec: dict[str, Any]) -> bool:
    return _is_number(value) and spec.get("min", float("-inf")) <= value <= spec.get("max", float("inf"))


def _has_type(value: Any, spec: dict[str, Any]) -> bool:
    return isinstance(value, TYPES[spec["type"]]) and not isinstance(value, bool)


RECORD_CHECKS: dict[str, Callable[[Any, dict[str, Any]], bool]] = {
    "not_null": lambda value, spec: value is not None and value != "",
    "type": _has_type,
    "range": _in_range,
    "accepted_values": lambda value, spec: value in spec["values"],
    "not_empty_list": lambda value, spec: isinstance(value, list) and len(value) > 0,
}
COLLECTION_RULES = ("not_empty", "count_matches_total", "unique")
REQUIRED_PARAMS = {"type": ("type",), "accepted_values": ("values",), "unique": ("field",)}


def load_rules(path: Path = DEFAULT_RULES_PATH) -> dict[str, dict[str, Any]]:
    config = json.loads(path.read_text())
    for resource, file_config in config.items():
        if not isinstance(file_config.get("rules"), list):
            raise ValueError(f"{resource}: falta la lista `rules`")
        _validate_accuracy(file_config, "file_accuracy", resource)
        for spec in file_config["rules"]:
            _validate_spec(resource, spec)
    return config


def _validate_accuracy(spec: dict[str, Any], key: str, where: str) -> None:
    value = spec.get(key, DEFAULT_ACCURACY)
    if not _is_number(value) or not 0 <= value <= 1:
        raise ValueError(f"`{key}` tiene que ser un número entre 0 y 1 en {where}")


def _validate_spec(resource: str, spec: dict[str, Any]) -> None:
    where = f"{resource}: {spec}"
    known = COLLECTION_RULES + tuple(RECORD_CHECKS)
    if spec.get("rule") not in known:
        raise ValueError(f"Regla desconocida en {where}. Válidas: {', '.join(known)}")
    _validate_accuracy(spec, "rule_accuracy", where)
    missing = [p for p in REQUIRED_PARAMS.get(spec["rule"], ()) if p not in spec]
    if spec["rule"] in RECORD_CHECKS and "field" not in spec:
        missing.append("field")
    if spec["rule"] == "range" and "min" not in spec and "max" not in spec:
        missing.append("min/max")
    if missing:
        raise ValueError(f"Faltan parámetros {missing} en {where}")
    if spec["rule"] == "type" and spec["type"] not in TYPES:
        raise ValueError(f"type inválido en {where}. Válidos: {', '.join(TYPES)}")


def _values(record: dict[str, Any], path: str) -> list[Any]:
    if "[]." in path:
        head, tail = path.split("[].", 1)
        return [value for item in (record.get(head) or []) for value in _values(item, tail)]
    return [record.get(path)]


def _evaluate_rule(spec: dict[str, Any], items: list[dict[str, Any]], api_total: int) -> RuleResult:
    rule = spec["rule"]
    field = spec.get("field", "*")
    expected = spec.get("rule_accuracy", DEFAULT_ACCURACY)
    if rule == "not_empty":
        return RuleResult(rule, field, expected, 1, int(not items), [])
    if rule == "count_matches_total":
        return RuleResult(rule, field, expected, max(api_total, 1), abs(api_total - len(items)), [])
    if rule == "unique":
        seen: set[Any] = set()
        duplicated = []
        for value in (item.get(field) for item in items):
            if value in seen:
                duplicated.append(value)
            seen.add(value)
        return RuleResult(rule, field, expected, len(items), len(duplicated), duplicated[:MAX_SAMPLE_IDS])

    check = RECORD_CHECKS[rule]
    failed_ids = [
        item.get("id") for item in items if not all(check(value, spec) for value in _values(item, field))
    ]
    return RuleResult(rule, field, expected, len(items), len(failed_ids), failed_ids[:MAX_SAMPLE_IDS])


def evaluate(
    resource: str, items: list[dict[str, Any]], api_total: int, file_config: dict[str, Any]
) -> QualityReport:
    results = [_evaluate_rule(spec, items, api_total) for spec in file_config["rules"]]
    return QualityReport(resource, results, file_config.get("file_accuracy", DEFAULT_ACCURACY))


def enforce(report: QualityReport) -> None:
    if not report.passed:
        detail = "; ".join(
            f"{r.rule}({r.field}) {r.measured_accuracy} < {r.expected_accuracy} ids={r.sample_failed_ids}"
            for r in report.failed_rules
        )
        raise DataQualityError(
            f"{report.resource}: file_accuracy {report.measured_accuracy} < {report.expected_accuracy}. "
            f"Reglas que no cumplen: {detail}"
        )
