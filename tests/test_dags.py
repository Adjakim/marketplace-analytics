"""Tests de structure des DAGs (sans base de données)."""
import re

import pytest

from marketplace import sql

EXPECTED_DAGS = {
    "marketplace_dims_refresh_daily",
    "marketplace_orders_ingest_daily",
    "marketplace_analytics_aggregate_daily",
    "marketplace_anomaly_detect_daily",
}
INGEST = "marketplace_orders_ingest_daily"


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"Erreurs d'import : {dagbag.import_errors}"


def test_expected_dags_present(dagbag):
    assert EXPECTED_DAGS <= set(dagbag.dag_ids)


def test_all_dags_catchup_false(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.catchup is False, f"{dag_id} doit avoir catchup=False"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_dags_have_retries_and_tags(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag.tags, f"{dag_id} sans tags"
    for task in dag.tasks:
        assert task.retries >= 1, f"{dag_id}.{task.task_id} sans retry"


def test_marketplace_ingest_has_idempotent_transform(dagbag):
    dag = dagbag.get_dag(INGEST)
    assert "transform_fact_orders" in dag.task_ids

    statements = [" ".join(s.split()).upper() for s in sql.FACT_ORDERS_TRANSFORM]
    delete_idx = next(i for i, s in enumerate(statements) if s.startswith("DELETE FROM DWH.FACT_ORDERS"))
    insert_idx = next(i for i, s in enumerate(statements) if s.startswith("INSERT INTO DWH.FACT_ORDERS"))
    assert delete_idx < insert_idx, "le DELETE de la partition doit précéder l'INSERT"
    # La partition supprimée et la partition insérée sont la même : dt = date du run.
    assert "WHERE DT = %(DT)S" in statements[delete_idx]
    assert "WHERE O.DT = %(DT)S" in statements[insert_idx]


def test_no_hardcoded_dates_in_sql():
    sql_text = "\n".join(v for v in vars(sql).values() if isinstance(v, str))
    assert not re.search(r"'20\d\d-\d\d-\d\d'", sql_text), "date en dur dans le SQL"


def test_ingest_pipeline_order(dagbag):
    dag = dagbag.get_dag(INGEST)

    def downstream(task_id):
        return dag.get_task(task_id).downstream_task_ids

    assert downstream("extract_orders_to_raw") == {"load_staging_orders"}
    assert downstream("load_staging_orders") == {"dq_check_staging_orders"}
    assert "branch_on_dq" in downstream("dq_check_staging_orders")
    assert downstream("branch_on_dq") == {"transform_fact_orders", "alert_dq_failure"}
    assert downstream("transform_fact_orders") == {"purge_old_staging"}


def test_ingest_publishes_fact_asset(dagbag):
    task = dagbag.get_dag(INGEST).get_task("transform_fact_orders")
    assert any(getattr(o, "name", None) == "dwh_fact_orders" for o in task.outlets)


def test_analytics_is_asset_scheduled(dagbag):
    dag = dagbag.get_dag("marketplace_analytics_aggregate_daily")
    assert "dwh_fact_orders" in repr(dag.timetable.asset_condition)
