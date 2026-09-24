import copy
import json
from pathlib import Path

import pytest

from pipeline.quality import DataQualityError, enforce, evaluate, load_rules

FIXTURES = Path(__file__).parent / "fixtures"
RULES = load_rules()


def _fixture(resource):
    return json.loads((FIXTURES / f"{resource}.json").read_text())


def _result(report, rule, field):
    return next(r for r in report.results if r.rule == rule and r.field == field)


@pytest.mark.parametrize("resource", ["products", "carts"])
def test_real_api_records_reach_the_configured_file_accuracy(resource):
    items = _fixture(resource)

    report = evaluate(resource, items, len(items), RULES[resource])

    assert report.failed_rules == []
    assert (report.measured_accuracy, report.passed) == (1.0, True)
    enforce(report)


def test_file_accuracy_is_rules_passed_over_total_rules():
    items = _fixture("products")

    report = evaluate("products", items, len(items) + 5, RULES["products"])

    total = len(RULES["products"]["rules"])
    assert not _result(report, "count_matches_total", "*").passed
    assert report.measured_accuracy == round((total - 1) / total, 4)
    with pytest.raises(DataQualityError, match="count_matches_total"):
        enforce(report)


def test_file_accuracy_below_one_tolerates_failing_rules():
    config = {
        "file_accuracy": 0.5,
        "rules": [{"rule": "count_matches_total"}, {"rule": "not_null", "field": "title"}],
    }
    items = _fixture("products")

    report = evaluate("products", items, len(items) + 5, config)

    assert (report.measured_accuracy, report.passed) == (0.5, True)
    enforce(report)


def test_duplicated_ids_are_measured_and_reported():
    items = _fixture("products")
    items.append(copy.deepcopy(items[0]))

    report = evaluate("products", items, len(items), RULES["products"])

    unique = _result(report, "unique", "id")
    assert (unique.measured_accuracy, unique.passed) == (0.75, False)
    assert unique.sample_failed_ids == [items[0]["id"]]


def test_nested_rule_flags_the_cart_with_an_invalid_line():
    carts = _fixture("carts")
    carts[1]["products"][0]["quantity"] = -1

    report = evaluate("carts", carts, len(carts), RULES["carts"])

    quantity = _result(report, "range", "products[].quantity")
    assert (quantity.failed, quantity.sample_failed_ids) == (1, [carts[1]["id"]])
    with pytest.raises(DataQualityError):
        enforce(report)


@pytest.mark.parametrize(("rule_accuracy", "passed"), [(0.6, True), (0.7, False)])
def test_rule_accuracy_is_records_passed_over_records_checked(rule_accuracy, passed):
    config = {"rules": [{"rule": "not_null", "field": "brand", "rule_accuracy": rule_accuracy}]}
    products = _fixture("products")
    del products[0]["brand"]

    report = evaluate("products", products, len(products), config)

    brand = _result(report, "not_null", "brand")
    assert (brand.measured_accuracy, brand.passed) == (round(2 / 3, 4), passed)


def test_missing_accuracies_default_to_one():
    config = {"rules": [{"rule": "not_null", "field": "brand"}]}
    products = _fixture("products")
    del products[0]["brand"]

    report = evaluate("products", products, len(products), config)

    assert (report.expected_accuracy, report.results[0].expected_accuracy) == (1.0, 1.0)
    with pytest.raises(DataQualityError, match=r"file_accuracy 0.0 < 1.0"):
        enforce(report)


@pytest.mark.parametrize(
    ("config", "message"),
    [
        ({"rules": [{"rule": "is_positive", "field": "price"}]}, "Regla desconocida"),
        ({"rules": [{"rule": "not_null", "field": "price", "rule_accuracy": 95}]}, "rule_accuracy"),
        ({"file_accuracy": 2, "rules": []}, "file_accuracy"),
        ({"rules": [{"rule": "range", "field": "price"}]}, "min/max"),
        ({"file_accuracy": 1.0}, "rules"),
    ],
)
def test_invalid_config_is_rejected_with_a_clear_message(tmp_path, config, message):
    path = tmp_path / "rules.json"
    path.write_text(json.dumps({"products": config}))

    with pytest.raises(ValueError, match=message):
        load_rules(path)
