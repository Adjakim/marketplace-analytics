"""
### Détection d'anomalies commerciales (bonus)

Déclenché après chaque mise à jour de `analytics.daily_summary` (Asset).
Pour chaque date reçue :

* **revenue_drop** (US-05) : CA du jour < 70 % de la moyenne des 7 jours précédents ;
* **price_outlier** : commande vendue à plus de 5x ou moins de 0,2x le prix catalogue.

Les anomalies sont écrites dans `analytics.anomalies` (DELETE + INSERT par `dt`)
puis signalées par la tâche `notify_anomalies`.
"""
from __future__ import annotations

from decimal import Decimal

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from marketplace import sql
from marketplace.config import DAILY_SUMMARY_ASSET, DEFAULT_ARGS, DWH_CONN_ID, REVENUE_DROP_THRESHOLD
from marketplace.db import run_in_transaction
from marketplace.events import dates_from_events


@dag(
    dag_id="marketplace_anomaly_detect_daily",
    schedule=[DAILY_SUMMARY_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,  # piloté par l'Asset : actif dès le déploiement
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "analytics", "anomalies"],
    doc_md=__doc__,
)
def marketplace_anomaly_detect_daily():

    @task
    def detect_anomalies(triggering_asset_events=None) -> list[dict]:
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        received = dates_from_events(triggering_asset_events, "dates")
        # Une nouvelle date modifie aussi la moyenne 7j des 7 jours suivants
        # (cas d'un backfill où les runs se terminent dans le désordre) : on les recalcule.
        # Sans événement (déclenchement manuel) : toutes les dates.
        records = hook.get_records(
            "SELECT dt::text FROM analytics.daily_summary "
            "WHERE %(first)s IS NULL OR dt BETWEEN %(first)s::date AND %(last)s::date + 7 ORDER BY dt",
            parameters={"first": received[0] if received else None, "last": received[-1] if received else None},
        )
        dates = [r[0] for r in records]

        found = []
        for dt in dates:
            run_in_transaction(sql.ANOMALIES_ALL, {"dt": dt, "threshold": Decimal(str(REVENUE_DROP_THRESHOLD))})
            for anomaly_type, entity_id, details in hook.get_records(sql.ANOMALIES_FOR_DATE, parameters={"dt": dt}):
                found.append({"dt": dt, "type": anomaly_type, "entity": entity_id, "details": details})
        print(f"{len(found)} anomalie(s) sur {len(dates)} date(s)")
        return found

    @task.branch
    def any_anomaly(anomalies: list[dict]) -> str:
        return "notify_anomalies" if anomalies else "no_anomaly"

    @task
    def notify_anomalies(anomalies: list[dict]) -> None:
        # Point d'extension : remplacer les logs par un envoi Slack / e-mail.
        for a in anomalies:
            print(f"ALERTE [{a['dt']}] {a['type']} {a['entity']} : {a['details']}")

    @task
    def no_anomaly() -> None:
        print("Aucune anomalie détectée.")

    anomalies = detect_anomalies()
    any_anomaly(anomalies) >> [notify_anomalies(anomalies), no_anomaly()]


marketplace_anomaly_detect_daily()
