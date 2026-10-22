"""Tests du DataQualityOperator avec une base mockée."""
from unittest import mock

import pytest
from airflow.exceptions import AirflowException

from marketplace.operators.data_quality_operator import DataQualityOperator


def make_operator(**kwargs):
    params = dict(
        task_id="dq",
        table="staging.orders",
        partition_filter="dt = '2026-04-07'",
        not_null_columns=["order_id", "seller_id"],
        no_future_date_column="order_ts",
    )
    params.update(kwargs)
    return DataQualityOperator(**params)


def run(operator, total=100, nulls=0, future=0):
    def get_first(query):
        if "IS NULL" in query:
            return (nulls,)
        if "now()" in query:
            return (future,)
        return (total,)

    with mock.patch("marketplace.operators.data_quality_operator.PostgresHook") as hook_cls:
        hook_cls.return_value.get_first.side_effect = get_first
        return operator.execute(context={})


def test_all_checks_pass():
    result = run(make_operator())
    assert result == {"passed": True, "row_count": 100, "failures": []}


def test_empty_partition_fails():
    result = run(make_operator(), total=0)
    assert not result["passed"]
    assert any(f.startswith("not_empty") for f in result["failures"])


def test_null_values_fail():
    result = run(make_operator(), nulls=3)
    assert not result["passed"]
    assert len([f for f in result["failures"] if f.startswith("not_null")]) == 2  # 2 colonnes


def test_future_dates_fail():
    result = run(make_operator(), future=1)
    assert result["failures"] == ["no_future_date:order_ts (1 ligne(s) dans le futur)"]


def test_fail_on_error_raises():
    with pytest.raises(AirflowException):
        run(make_operator(fail_on_error=True), total=0)


def test_partition_filter_is_templated():
    assert "partition_filter" in DataQualityOperator.template_fields
