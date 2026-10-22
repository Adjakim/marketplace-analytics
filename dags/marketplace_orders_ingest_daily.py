"""
### Pipeline principal : commandes du jour

`API /orders?date={{ ds }}` → **MinIO** `raw/orders/dt={{ ds }}/data.json` → `staging.orders`
→ contrôle qualité → `dwh.fact_orders` (DELETE + INSERT par partition `dt`).

* Idempotent : rejouer une date donne exactement le même contenu en base.
* Le brut est relu depuis MinIO (et non depuis l'XCom) : on peut rejouer
  le chargement sans rappeler l'API.
* Si le contrôle qualité échoue, la tâche `alert_dq_failure` remplace le load DWH.
* En succès, l'Asset `dwh_fact_orders` est publié → déclenche le DAG analytics.
"""
from __future__ import annotations

import json
from decimal import Decimal

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

from marketplace import sql
from marketplace.config import (
    COMMISSION_RATE,
    DATA_LAKE_BUCKET,
    DEFAULT_ARGS,
    DWH_CONN_ID,
    FACT_ORDERS_ASSET,
    MARKETPLACE_CONN_ID,
    S3_CONN_ID,
    STAGING_RETENTION_DAYS,
    raw_key,
)
from marketplace.db import replace_rows, run_in_transaction
from marketplace.hooks import MarketplaceAPIHook
from marketplace.operators import DataQualityOperator

ORDER_FIELDS = ["order_id", "order_ts", "customer_id", "seller_id", "product_id", "quantity",
                "unit_price", "total_amount", "currency", "status", "payment_method"]


@dag(
    dag_id="marketplace_orders_ingest_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=3,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "ingestion", "dwh"],
    doc_md=__doc__,
)
def marketplace_orders_ingest_daily():

    @task
    def extract_orders_to_raw(ds: str | None = None) -> str:
        """Appelle l'API et archive la réponse brute dans le data lake."""
        orders = MarketplaceAPIHook(MARKETPLACE_CONN_ID).get_orders(ds)
        key = raw_key("orders", ds)
        payload = {"date": ds, "count": len(orders), "orders": orders}
        S3Hook(aws_conn_id=S3_CONN_ID).load_string(
            json.dumps(payload, ensure_ascii=False),
            key=key,
            bucket_name=DATA_LAKE_BUCKET,
            replace=True,
        )
        print(f"{len(orders)} commandes archivées dans s3://{DATA_LAKE_BUCKET}/{key}")
        return key

    @task
    def load_staging_orders(key: str, ds: str | None = None) -> int:
        """Relit le JSON brut depuis MinIO et remplace la partition dt de staging.orders."""
        body = S3Hook(aws_conn_id=S3_CONN_ID).read_key(key=key, bucket_name=DATA_LAKE_BUCKET)
        orders = json.loads(body)["orders"]
        rows = [(o["order_id"], ds, *(o.get(f) for f in ORDER_FIELDS[1:])) for o in orders]
        return replace_rows(sql.STAGING_ORDERS_DELETE, sql.STAGING_ORDERS_INSERT, rows, {"dt": ds})

    dq_check = DataQualityOperator(
        task_id="dq_check_staging_orders",
        postgres_conn_id=DWH_CONN_ID,
        table="staging.orders",
        partition_filter="dt = '{{ ds }}'",
        check_not_empty=True,
        not_null_columns=["order_id", "order_ts", "seller_id", "product_id",
                          "quantity", "unit_price", "total_amount", "status"],
        no_future_date_column="order_ts",
        results_table="analytics.dq_check_results",
    )

    @task.branch
    def branch_on_dq(dq_result: dict) -> str:
        return "transform_fact_orders" if dq_result["passed"] else "alert_dq_failure"

    @task(outlets=[FACT_ORDERS_ASSET])
    def transform_fact_orders(ds: str | None = None, outlet_events=None) -> int:
        """staging -> dwh : membres inférés + DELETE/INSERT de la partition, en 1 transaction."""
        counts = run_in_transaction(
            sql.FACT_ORDERS_TRANSFORM,
            {"dt": ds, "rate": Decimal(str(COMMISSION_RATE))},
        )
        inserted = counts[-1]
        outlet_events[FACT_ORDERS_ASSET].extra = {"dt": ds, "rows": inserted}
        return inserted

    @task
    def alert_dq_failure(dq_result: dict, ds: str | None = None) -> None:
        # Point d'extension : envoyer un e-mail / Slack ici (on_failure_callback).
        raise AirflowFailException(
            f"Qualité KO pour staging.orders dt={ds} : {dq_result['failures']} - "
            "dwh.fact_orders n'a PAS été mis à jour."
        )

    @task
    def purge_old_staging(ds: str | None = None) -> None:
        """Évite que staging.orders grossisse indéfiniment (le brut reste dans MinIO)."""
        run_in_transaction([sql.STAGING_ORDERS_PURGE], {"dt": ds, "retention_days": STAGING_RETENTION_DAYS})

    key = extract_orders_to_raw()
    loaded = load_staging_orders(key)
    loaded >> dq_check
    branch = branch_on_dq(dq_check.output)
    fact = transform_fact_orders()
    branch >> [fact, alert_dq_failure(dq_check.output)]
    fact >> purge_old_staging()


marketplace_orders_ingest_daily()
