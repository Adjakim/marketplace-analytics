"""
### Agrégats analytics (Asset-scheduled)

Déclenché à chaque mise à jour de l'Asset `dwh_fact_orders`. Reconstruit, pour
chaque date reçue dans les événements, `analytics.daily_summary`,
`analytics.seller_daily` et `analytics.category_daily` (DELETE + INSERT par `dt`).

Déclenchement manuel (sans événement) : reconstruction complète de toutes les
dates présentes dans `dwh.fact_orders`.
"""
from __future__ import annotations

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from marketplace import sql
from marketplace.config import DAILY_SUMMARY_ASSET, DEFAULT_ARGS, DWH_CONN_ID, FACT_ORDERS_ASSET
from marketplace.db import run_in_transaction
from marketplace.events import dates_from_events


@dag(
    dag_id="marketplace_analytics_aggregate_daily",
    schedule=[FACT_ORDERS_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,  # piloté par l'Asset : actif dès le déploiement
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "analytics"],
    doc_md=__doc__,
)
def marketplace_analytics_aggregate_daily():

    @task
    def resolve_dates(triggering_asset_events=None) -> list[str]:
        dates = dates_from_events(triggering_asset_events, "dt")
        if not dates:
            records = PostgresHook(postgres_conn_id=DWH_CONN_ID).get_records(sql.ANALYTICS_LOADED_DATES)
            dates = [r[0] for r in records]
            print(f"Aucun événement d'Asset : reconstruction complète ({len(dates)} dates)")
        print(f"Dates à agréger : {dates}")
        return dates

    @task(outlets=[DAILY_SUMMARY_ASSET])
    def build_analytics(dates: list[str], outlet_events=None) -> int:
        for dt in dates:
            run_in_transaction(sql.ANALYTICS_ALL, {"dt": dt})
        outlet_events[DAILY_SUMMARY_ASSET].extra = {"dates": dates}
        return len(dates)

    build_analytics(resolve_dates())


marketplace_analytics_aggregate_daily()
