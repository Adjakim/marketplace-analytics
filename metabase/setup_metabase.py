"""Configure Metabase automatiquement via son API REST (rejouable sans doublon).

1. crée le compte admin au premier démarrage (sinon se connecte) ;
2. déclare la source PostgreSQL du DWH (host = postgres-dwh, port interne 5432) ;
3. crée les questions SQL et les dashboards définis dans DASHBOARDS.

Usage (depuis la machine hôte, Metabase démarré) :
    python metabase/setup_metabase.py
Bibliothèque standard uniquement : aucune installation nécessaire.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

MB_URL = os.environ.get("MB_URL", "http://localhost:3000")
ADMIN_EMAIL = os.environ.get("MB_ADMIN_EMAIL", "admin@maelys.local")
ADMIN_PASSWORD = os.environ.get("MB_ADMIN_PASSWORD", "Admin2026!")
DB_NAME = "DWH Marketplace"
DB_DETAILS = {
    "host": "postgres-dwh",     # nom du service Docker, pas localhost
    "port": 5432,               # port interne du conteneur, pas 5433
    "dbname": os.environ.get("DWH_DB", "dwh"),
    "user": os.environ.get("DWH_USER", "dwh_user"),
    "password": os.environ.get("DWH_PASSWORD", "dwh_password_2026"),
    "ssl": False,
}

# "Aujourd'hui" = dernier jour chargé (le pipeline traite J-1), ce qui rend les
# dashboards lisibles aussi sur des données historiques rejouées.
LAST_DAY = "(SELECT MAX(dt) FROM analytics.daily_summary)"


def line(x, *y, series=None):
    dims = [x] + ([series] if series else [])
    return {"display": "line", "visualization_settings": {"graph.dimensions": dims, "graph.metrics": list(y)}}


def bar(x, *y, series=None, stacked=False):
    dims = [x] + ([series] if series else [])
    settings = {"graph.dimensions": dims, "graph.metrics": list(y)}
    if stacked:
        settings["stackable.stack_type"] = "stacked"
    return {"display": "bar", "visualization_settings": settings}


def row_chart(x, y):
    return {"display": "row", "visualization_settings": {"graph.dimensions": [x], "graph.metrics": [y]}}


SCALAR = {"display": "scalar", "visualization_settings": {}}
TABLE = {"display": "table", "visualization_settings": {}}

# Chaque carte : (titre, SQL, visualisation, (col, row, largeur, hauteur)) - grille de 24 colonnes.
DASHBOARDS = [
    {
        "name": "1 - Executive Summary",
        "description": "KPIs clés pour le CEO (US-01, US-02). Dernier jour chargé = J-1.",
        "cards": [
            ("CA du dernier jour (EUR)",
             f"SELECT total_revenue AS \"CA\" FROM analytics.daily_summary WHERE dt = {LAST_DAY}",
             SCALAR, (0, 0, 6, 4)),
            ("Commandes valides du dernier jour",
             f"SELECT valid_orders AS \"Commandes\" FROM analytics.daily_summary WHERE dt = {LAST_DAY}",
             SCALAR, (6, 0, 6, 4)),
            ("Panier moyen du dernier jour (EUR)",
             f"SELECT avg_basket AS \"Panier moyen\" FROM analytics.daily_summary WHERE dt = {LAST_DAY}",
             SCALAR, (12, 0, 6, 4)),
            ("Commission Maelys du dernier jour (EUR)",
             f"SELECT total_commission AS \"Commission\" FROM analytics.daily_summary WHERE dt = {LAST_DAY}",
             SCALAR, (18, 0, 6, 4)),
            ("CA des 30 derniers jours",
             f"""SELECT dt, total_revenue AS revenue
                 FROM analytics.daily_summary
                 WHERE dt > {LAST_DAY} - 30
                 ORDER BY dt""",
             line("dt", "revenue"), (0, 4, 16, 8)),
            ("Top 5 vendeurs du dernier jour",
             f"""SELECT seller_name, revenue
                 FROM analytics.seller_daily
                 WHERE dt = {LAST_DAY}
                 ORDER BY revenue DESC LIMIT 5""",
             row_chart("seller_name", "revenue"), (16, 4, 8, 8)),
        ],
    },
    {
        "name": "2 - Top Sellers",
        "description": "Performance des vendeurs (US-02).",
        "cards": [
            ("Top 10 vendeurs du mois",
             f"""SELECT seller_name, SUM(revenue) AS revenue
                 FROM analytics.seller_daily
                 WHERE dt >= date_trunc('month', {LAST_DAY})::date
                 GROUP BY seller_name
                 ORDER BY revenue DESC LIMIT 10""",
             bar("seller_name", "revenue"), (0, 0, 12, 8)),
            ("Évolution du CA des top 3 vendeurs (30 jours)",
             f"""WITH top3 AS (
                     SELECT seller_id FROM analytics.seller_daily
                     WHERE dt > {LAST_DAY} - 30
                     GROUP BY seller_id ORDER BY SUM(revenue) DESC LIMIT 3)
                 SELECT s.dt, s.seller_name, s.revenue
                 FROM analytics.seller_daily s JOIN top3 USING (seller_id)
                 WHERE s.dt > {LAST_DAY} - 30
                 ORDER BY s.dt""",
             line("dt", "revenue", series="seller_name"), (12, 0, 12, 8)),
            ("Vendeurs inactifs (plus de 7 jours sans vente)",
             """SELECT seller_id, seller_name, seller_type, country,
                       last_sale_date, days_since_last_sale, lifetime_orders, lifetime_revenue
                FROM analytics.v_seller_activity
                WHERE last_sale_date IS NOT NULL AND days_since_last_sale > 7
                ORDER BY lifetime_revenue DESC""",
             TABLE, (0, 8, 24, 8)),
        ],
    },
    {
        "name": "3 - Commissions (Finance)",
        "description": "Récapitulatif des commissions pour la clôture mensuelle (US-03).",
        "cards": [
            ("Commission totale du mois en cours (EUR)",
             f"""SELECT SUM(total_commission) AS "Commission"
                 FROM analytics.daily_summary
                 WHERE dt >= date_trunc('month', {LAST_DAY})::date""",
             SCALAR, (0, 0, 8, 4)),
            ("Commandes annulées ou remboursées du mois",
             f"""SELECT SUM(cancelled_orders + refunded_orders) AS "Annulées / remboursées"
                 FROM analytics.daily_summary
                 WHERE dt >= date_trunc('month', {LAST_DAY})::date""",
             SCALAR, (8, 0, 8, 4)),
            ("Taux de commission effectif (%)",
             f"""SELECT ROUND(100 * SUM(total_commission) / NULLIF(SUM(total_revenue), 0), 2) AS "Taux"
                 FROM analytics.daily_summary
                 WHERE dt >= date_trunc('month', {LAST_DAY})::date""",
             SCALAR, (16, 0, 8, 4)),
            ("Commission par jour (60 jours)",
             f"""SELECT dt, total_commission AS commission
                 FROM analytics.daily_summary WHERE dt > {LAST_DAY} - 60 ORDER BY dt""",
             bar("dt", "commission"), (0, 4, 24, 7)),
            ("Récapitulatif journalier des commissions",
             """SELECT dt AS "Date", valid_orders AS "Commandes valides",
                       cancelled_orders AS "Annulées", refunded_orders AS "Remboursées",
                       total_revenue AS "CA (EUR)", total_commission AS "Commission (EUR)",
                       total_revenue - total_commission AS "Reversé vendeurs (EUR)"
                FROM analytics.daily_summary ORDER BY dt DESC""",
             TABLE, (0, 11, 24, 8)),
        ],
    },
    {
        "name": "4 - Catégories",
        "description": "Répartition du CA par catégorie (US-04).",
        "cards": [
            ("CA par catégorie (30 jours)",
             f"""SELECT category, SUM(revenue) AS revenue
                 FROM analytics.category_daily WHERE dt > {LAST_DAY} - 30
                 GROUP BY category ORDER BY revenue DESC""",
             {"display": "pie", "visualization_settings": {"pie.dimension": "category", "pie.metric": "revenue"}},
             (0, 0, 10, 8)),
            ("CA journalier par catégorie (30 jours)",
             f"""SELECT dt, category, revenue FROM analytics.category_daily
                 WHERE dt > {LAST_DAY} - 30 ORDER BY dt""",
             bar("dt", "revenue", series="category", stacked=True), (10, 0, 14, 8)),
            ("Panier moyen et volume par catégorie (30 jours)",
             f"""SELECT category AS "Catégorie", SUM(orders) AS "Commandes",
                       SUM(items_sold) AS "Articles", SUM(revenue) AS "CA (EUR)",
                       ROUND(SUM(revenue) / NULLIF(SUM(orders), 0), 2) AS "Panier moyen (EUR)",
                       SUM(commission) AS "Commission (EUR)"
                 FROM analytics.category_daily WHERE dt > {LAST_DAY} - 30
                 GROUP BY category ORDER BY 4 DESC""",
             TABLE, (0, 8, 24, 7)),
        ],
    },
    {
        "name": "5 - Anomalies & qualité des données",
        "description": "Alertes commerciales (US-05) et résultats des contrôles qualité.",
        "cards": [
            ("Anomalies détectées",
             """SELECT dt AS "Date", anomaly_type AS "Type", entity_id AS "Entité",
                       observed_value AS "Observé", expected_value AS "Attendu", ratio AS "Ratio",
                       details AS "Détails"
                FROM analytics.anomalies ORDER BY dt DESC, anomaly_type""",
             TABLE, (0, 0, 24, 8)),
            ("CA vs moyenne mobile 7 jours",
             """SELECT dt, total_revenue AS revenue,
                       ROUND(AVG(total_revenue) OVER (ORDER BY dt ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING), 2)
                           AS moyenne_7j
                FROM analytics.daily_summary ORDER BY dt""",
             line("dt", "revenue", "moyenne_7j"), (0, 8, 14, 8)),
            ("Derniers contrôles qualité",
             """SELECT checked_at AS "Contrôlé le", partition AS "Partition", check_name AS "Règle",
                       CASE WHEN passed THEN 'OK' ELSE 'KO' END AS "Statut", details AS "Détails"
                FROM analytics.dq_check_results ORDER BY checked_at DESC LIMIT 100""",
             TABLE, (14, 8, 10, 8)),
        ],
    },
]


# --------------------------------------------------------------------- API client
class Metabase:
    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.session_id: str | None = None

    def call(self, method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(f"{self.url}/api{path}", data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.session_id:
            req.add_header("X-Metabase-Session", self.session_id)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"{method} {path} -> {exc.code} : {exc.read().decode()[:500]}") from exc
        return json.loads(raw) if raw else None

    def wait_until_ready(self, timeout: int = 300):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if self.call("GET", "/health").get("status") == "ok":
                    return
            except Exception:  # noqa: BLE001
                pass
            print("  Metabase démarre...")
            time.sleep(5)
        sys.exit("Metabase ne répond pas sur " + self.url)

    def login(self):
        token = self.call("GET", "/session/properties").get("setup-token")
        has_user = self.call("GET", "/session/properties").get("has-user-setup")
        if token and not has_user:
            print("Premier démarrage : création du compte admin", ADMIN_EMAIL)
            resp = self.call("POST", "/setup", {
                "token": token,
                "user": {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD,
                         "first_name": "Admin", "last_name": "Maelys", "site_name": "Maelys Marketplace"},
                "prefs": {"site_name": "Maelys Marketplace", "site_locale": "fr", "allow_tracking": False},
            })
            self.session_id = resp["id"]
        else:
            resp = self.call("POST", "/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
            self.session_id = resp["id"]

    def database_id(self) -> int:
        dbs = self.call("GET", "/database")
        dbs = dbs.get("data", dbs) if isinstance(dbs, dict) else dbs
        for db in dbs:
            if db["name"] == DB_NAME:
                return db["id"]
        print("Ajout de la source de données", DB_NAME)
        db = self.call("POST", "/database", {"engine": "postgres", "name": DB_NAME, "details": DB_DETAILS})
        return db["id"]

    def existing_dashboards(self) -> set[str]:
        return {d["name"] for d in self.call("GET", "/dashboard?f=all") if not d.get("archived")}

    def create_dashboard(self, db_id: int, spec: dict):
        dashboard = self.call("POST", "/dashboard", {"name": spec["name"], "description": spec["description"]})
        dashcards = []
        for i, (title, query, viz, (col, row, width, height)) in enumerate(spec["cards"], start=1):
            card = self.call("POST", "/card", {
                "name": title,
                "dataset_query": {"type": "native", "native": {"query": query}, "database": db_id},
                "display": viz["display"],
                "visualization_settings": viz["visualization_settings"],
            })
            dashcards.append({"id": -i, "card_id": card["id"], "col": col, "row": row,
                              "size_x": width, "size_y": height})
        self.call("PUT", f"/dashboard/{dashboard['id']}", {"dashcards": dashcards})
        print(f"  dashboard créé : {spec['name']} ({len(dashcards)} cartes) -> "
              f"{self.url}/dashboard/{dashboard['id']}")


def main():
    mb = Metabase(MB_URL)
    print("Connexion à", MB_URL)
    mb.wait_until_ready()
    mb.login()
    db_id = mb.database_id()
    existing = mb.existing_dashboards()
    for spec in DASHBOARDS:
        if spec["name"] in existing:
            print(f"  déjà présent, ignoré : {spec['name']}")
            continue
        mb.create_dashboard(db_id, spec)
    print(f"Terminé. Connexion : {MB_URL}  ({ADMIN_EMAIL} / {ADMIN_PASSWORD})")


if __name__ == "__main__":
    main()
