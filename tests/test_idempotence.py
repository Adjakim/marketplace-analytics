"""Idempotence de la transformation staging -> dwh.fact_orders.

1. Tests unitaires avec une connexion mockée : ordre DELETE/INSERT, une seule
   transaction, ROLLBACK en cas d'erreur.
2. Test d'intégration sur le vrai postgres-dwh (ignoré s'il est inaccessible) :
   on joue 2 fois la transformation sur une date de test et on compare.
"""
from decimal import Decimal
from unittest import mock

import pytest

from marketplace import sql
from marketplace.db import replace_rows, run_in_transaction

TEST_DT = "2023-01-02"  # date hors période réelle, nettoyée après le test


@pytest.fixture
def fake_conn():
    with mock.patch("marketplace.db.PostgresHook") as hook_cls:
        conn = hook_cls.return_value.get_conn.return_value
        yield conn


def executed(conn):
    cursor = conn.cursor.return_value.__enter__.return_value
    return [" ".join(c.args[0].split()) for c in cursor.execute.call_args_list]


def test_transform_runs_delete_then_insert_in_one_transaction(fake_conn):
    run_in_transaction(sql.FACT_ORDERS_TRANSFORM, {"dt": "2026-04-07", "rate": Decimal("0.12")})
    queries = executed(fake_conn)
    delete = next(i for i, q in enumerate(queries) if q.startswith("DELETE FROM dwh.fact_orders"))
    insert = next(i for i, q in enumerate(queries) if q.startswith("INSERT INTO dwh.fact_orders"))
    assert delete < insert
    fake_conn.commit.assert_called_once()
    fake_conn.rollback.assert_not_called()


def test_transform_rolls_back_on_error(fake_conn):
    cursor = fake_conn.cursor.return_value.__enter__.return_value
    cursor.execute.side_effect = [None, None, None, None, RuntimeError("INSERT KO")]
    with pytest.raises(RuntimeError):
        run_in_transaction(sql.FACT_ORDERS_TRANSFORM, {"dt": "2026-04-07", "rate": Decimal("0.12")})
    fake_conn.rollback.assert_called_once()
    fake_conn.commit.assert_not_called()


# ----------------------------------------------------------------- intégration
@pytest.fixture
def dwh():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    try:
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        hook.get_first("SELECT 1")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres-dwh inaccessible : {exc}")
    yield hook
    hook.run([
        "DELETE FROM dwh.fact_orders WHERE dt = %(dt)s",
        "DELETE FROM staging.orders WHERE dt = %(dt)s",
        "DELETE FROM dwh.dim_customer WHERE customer_id LIKE 'TEST\\_%%'",
        "DELETE FROM dwh.dim_product WHERE product_id LIKE 'TEST\\_%%'",
        "DELETE FROM dwh.dim_seller WHERE seller_id LIKE 'TEST\\_%%'",
    ], parameters={"dt": TEST_DT})


@pytest.mark.integration
def test_replaying_transform_gives_same_result(dwh):
    rows = [
        (f"TEST_ORD_{i}", TEST_DT, f"{TEST_DT}T10:00:0{i}", "TEST_C1" if i else None,
         f"TEST_S{i % 2}", f"TEST_P{i}", 1 + i, Decimal("10.00"), Decimal(10 * (1 + i)),
         "EUR", "cancelled" if i == 2 else "delivered", "card")
        for i in range(3)
    ]
    replace_rows(sql.STAGING_ORDERS_DELETE, sql.STAGING_ORDERS_INSERT, rows, {"dt": TEST_DT})
    params = {"dt": TEST_DT, "rate": Decimal("0.12")}
    snapshot = ("SELECT order_id, seller_id, gross_amount, commission_amount, is_revenue "
                "FROM dwh.fact_orders WHERE dt = %(dt)s ORDER BY order_id")

    run_in_transaction(sql.FACT_ORDERS_TRANSFORM, params)
    first = dwh.get_records(snapshot, parameters={"dt": TEST_DT})
    run_in_transaction(sql.FACT_ORDERS_TRANSFORM, params)
    run_in_transaction(sql.FACT_ORDERS_TRANSFORM, params)
    second = dwh.get_records(snapshot, parameters={"dt": TEST_DT})

    assert len(first) == 3
    assert first == second
    # commission = 12 % des commandes valides, 0 pour la commande annulée
    commissions = {r[0]: r[3] for r in first}
    assert commissions == {"TEST_ORD_0": Decimal("1.20"), "TEST_ORD_1": Decimal("2.40"),
                           "TEST_ORD_2": Decimal("0.00")}
