"""Requêtes SQL du pipeline.

Toutes les transformations sont partitionnées par ``dt`` et suivent le pattern
idempotent DELETE + INSERT exécuté dans UNE transaction : rejouer un run sur la
même date produit exactement le même état en base.
Paramètres psycopg2 : %(dt)s, %(rate)s, %(threshold)s.
"""

# --------------------------------------------------------------------- staging
STAGING_ORDERS_DELETE = "DELETE FROM staging.orders WHERE dt = %(dt)s;"

STAGING_ORDERS_INSERT = """
INSERT INTO staging.orders (order_id, dt, order_ts, customer_id, seller_id, product_id,
                            quantity, unit_price, total_amount, currency, status, payment_method)
VALUES %s
"""

STAGING_ORDERS_PURGE = "DELETE FROM staging.orders WHERE dt < %(dt)s::date - %(retention_days)s;"

# ------------------------------------------------------------------- dimensions
# Upsert SCD1 depuis staging ; is_inferred repasse à FALSE quand le vrai membre arrive.
DIM_UPSERT = {
    "sellers": """
        INSERT INTO dwh.dim_seller (seller_id, seller_name, seller_type, country, city, joined_date,
                                    is_inferred, updated_at)
        SELECT seller_id, name, seller_type, country, city, joined_date, FALSE, now()
        FROM staging.sellers
        ON CONFLICT (seller_id) DO UPDATE SET
            seller_name = EXCLUDED.seller_name, seller_type = EXCLUDED.seller_type,
            country = EXCLUDED.country, city = EXCLUDED.city, joined_date = EXCLUDED.joined_date,
            is_inferred = FALSE, updated_at = now();
    """,
    "products": """
        INSERT INTO dwh.dim_product (product_id, product_name, category, seller_id, list_price, created_at,
                                     is_inferred, updated_at)
        SELECT product_id, name, category, seller_id, unit_price, created_at, FALSE, now()
        FROM staging.products
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name, category = EXCLUDED.category,
            seller_id = EXCLUDED.seller_id, list_price = EXCLUDED.list_price,
            created_at = EXCLUDED.created_at, is_inferred = FALSE, updated_at = now();
    """,
    "customers": """
        INSERT INTO dwh.dim_customer (customer_id, country, city, segment, signup_date, is_inferred, updated_at)
        SELECT customer_id, country, city, segment, signup_date, FALSE, now()
        FROM staging.customers
        ON CONFLICT (customer_id) DO UPDATE SET
            country = EXCLUDED.country, city = EXCLUDED.city, segment = EXCLUDED.segment,
            signup_date = EXCLUDED.signup_date, is_inferred = FALSE, updated_at = now();
    """,
}

# ------------------------------------------------------------------ fact_orders
# 1) Membres "inférés" : si une commande référence un vendeur/produit/client pas
#    encore présent dans la dimension, on crée une ligne minimale pour respecter
#    les FOREIGN KEY. Le DAG dims_refresh complètera ses attributs.
FACT_ORDERS_INFERRED_MEMBERS = [
    """INSERT INTO dwh.dim_seller (seller_id, is_inferred)
       SELECT DISTINCT seller_id, TRUE FROM staging.orders WHERE dt = %(dt)s
       ON CONFLICT (seller_id) DO NOTHING;""",
    """INSERT INTO dwh.dim_product (product_id, seller_id, is_inferred)
       SELECT DISTINCT ON (product_id) product_id, seller_id, TRUE FROM staging.orders WHERE dt = %(dt)s
       ON CONFLICT (product_id) DO NOTHING;""",
    """INSERT INTO dwh.dim_customer (customer_id, is_inferred)
       SELECT DISTINCT customer_id, TRUE FROM staging.orders
       WHERE dt = %(dt)s AND customer_id IS NOT NULL
       ON CONFLICT (customer_id) DO NOTHING;""",
]

# 2) Pattern d'idempotence obligatoire : DELETE de la partition puis INSERT.
FACT_ORDERS_DELETE = "DELETE FROM dwh.fact_orders WHERE dt = %(dt)s;"

FACT_ORDERS_INSERT = """
INSERT INTO dwh.fact_orders (order_id, dt, order_ts, seller_id, product_id, customer_id,
                             quantity, unit_price, gross_amount, commission_rate,
                             commission_amount, seller_net_amount, status, is_revenue, payment_method)
SELECT o.order_id,
       o.dt,
       o.order_ts,
       o.seller_id,
       o.product_id,
       o.customer_id,
       o.quantity,
       o.unit_price,
       o.total_amount,
       %(rate)s,
       CASE WHEN o.status IN ('cancelled', 'refunded') THEN 0
            ELSE ROUND(o.total_amount * %(rate)s, 2) END,
       CASE WHEN o.status IN ('cancelled', 'refunded') THEN 0
            ELSE o.total_amount - ROUND(o.total_amount * %(rate)s, 2) END,
       o.status,
       o.status NOT IN ('cancelled', 'refunded'),
       o.payment_method
FROM staging.orders o
WHERE o.dt = %(dt)s;
"""

FACT_ORDERS_TRANSFORM = FACT_ORDERS_INFERRED_MEMBERS + [FACT_ORDERS_DELETE, FACT_ORDERS_INSERT]

# -------------------------------------------------------------------- analytics
ANALYTICS_DAILY_SUMMARY = [
    "DELETE FROM analytics.daily_summary WHERE dt = %(dt)s;",
    """
    INSERT INTO analytics.daily_summary (dt, total_orders, valid_orders, cancelled_orders, refunded_orders,
                                         total_revenue, total_commission, avg_basket, unique_customers,
                                         active_sellers, top_seller_id)
    SELECT %(dt)s::date,
           COUNT(*),
           COUNT(*) FILTER (WHERE f.is_revenue),
           COUNT(*) FILTER (WHERE f.status = 'cancelled'),
           COUNT(*) FILTER (WHERE f.status = 'refunded'),
           COALESCE(SUM(f.gross_amount) FILTER (WHERE f.is_revenue), 0),
           COALESCE(SUM(f.commission_amount), 0),
           ROUND(AVG(f.gross_amount) FILTER (WHERE f.is_revenue), 2),
           COUNT(DISTINCT f.customer_id),
           COUNT(DISTINCT f.seller_id) FILTER (WHERE f.is_revenue),
           (SELECT t.seller_id FROM dwh.fact_orders t
            WHERE t.dt = %(dt)s AND t.is_revenue
            GROUP BY t.seller_id ORDER BY SUM(t.gross_amount) DESC, t.seller_id LIMIT 1)
    FROM dwh.fact_orders f
    WHERE f.dt = %(dt)s
    HAVING COUNT(*) > 0;
    """,
]

ANALYTICS_SELLER_DAILY = [
    "DELETE FROM analytics.seller_daily WHERE dt = %(dt)s;",
    """
    INSERT INTO analytics.seller_daily (dt, seller_id, seller_name, orders, items_sold, revenue, commission)
    SELECT f.dt, f.seller_id, COALESCE(s.seller_name, f.seller_id),
           COUNT(*), SUM(f.quantity), SUM(f.gross_amount), SUM(f.commission_amount)
    FROM dwh.fact_orders f
    JOIN dwh.dim_seller s ON s.seller_id = f.seller_id
    WHERE f.dt = %(dt)s AND f.is_revenue
    GROUP BY f.dt, f.seller_id, s.seller_name;
    """,
]

ANALYTICS_CATEGORY_DAILY = [
    "DELETE FROM analytics.category_daily WHERE dt = %(dt)s;",
    """
    INSERT INTO analytics.category_daily (dt, category, orders, items_sold, revenue, commission, avg_basket)
    SELECT f.dt, COALESCE(p.category, 'Inconnue'),
           COUNT(*), SUM(f.quantity), SUM(f.gross_amount), SUM(f.commission_amount),
           ROUND(AVG(f.gross_amount), 2)
    FROM dwh.fact_orders f
    JOIN dwh.dim_product p ON p.product_id = f.product_id
    WHERE f.dt = %(dt)s AND f.is_revenue
    GROUP BY f.dt, COALESCE(p.category, 'Inconnue');
    """,
]

ANALYTICS_ALL = ANALYTICS_DAILY_SUMMARY + ANALYTICS_SELLER_DAILY + ANALYTICS_CATEGORY_DAILY

ANALYTICS_LOADED_DATES = "SELECT DISTINCT dt::text FROM dwh.fact_orders ORDER BY 1;"

# -------------------------------------------------------------------- anomalies
ANOMALIES_DELETE = "DELETE FROM analytics.anomalies WHERE dt = %(dt)s;"

# US-05 : CA du jour < seuil x moyenne des 7 jours précédents (au moins 3 jours d'historique).
ANOMALY_REVENUE_DROP = """
INSERT INTO analytics.anomalies (dt, anomaly_type, entity_id, observed_value, expected_value, ratio, details)
SELECT d.dt, 'revenue_drop', 'GLOBAL', d.total_revenue, ROUND(b.avg_7d, 2),
       ROUND(d.total_revenue / b.avg_7d, 4),
       'CA du jour = ' || ROUND(100 * d.total_revenue / b.avg_7d) || ' pct de la moyenne 7j ('
           || b.n_days || ' jours de reference)'
FROM analytics.daily_summary d
CROSS JOIN LATERAL (
    SELECT AVG(p.total_revenue) AS avg_7d, COUNT(*) AS n_days
    FROM analytics.daily_summary p
    WHERE p.dt BETWEEN d.dt - 7 AND d.dt - 1
) b
WHERE d.dt = %(dt)s
  AND b.n_days >= 3
  AND b.avg_7d > 0
  AND d.total_revenue < %(threshold)s * b.avg_7d;
"""

# Prix suspect : prix de vente > 5x ou < 0,2x le prix catalogue du produit.
ANOMALY_PRICE_OUTLIER = """
INSERT INTO analytics.anomalies (dt, anomaly_type, entity_id, observed_value, expected_value, ratio, details)
SELECT f.dt, 'price_outlier', f.order_id, f.unit_price, p.list_price,
       ROUND(f.unit_price / p.list_price, 4),
       'Produit ' || p.product_id || ' (vendeur ' || f.seller_id || ') vendu ' || f.unit_price
           || ' EUR pour un prix catalogue de ' || p.list_price || ' EUR'
FROM dwh.fact_orders f
JOIN dwh.dim_product p ON p.product_id = f.product_id
WHERE f.dt = %(dt)s
  AND p.list_price > 0
  AND (f.unit_price > 5 * p.list_price OR f.unit_price < 0.2 * p.list_price);
"""

ANOMALIES_ALL = [ANOMALIES_DELETE, ANOMALY_REVENUE_DROP, ANOMALY_PRICE_OUTLIER]

ANOMALIES_FOR_DATE = """
SELECT anomaly_type, entity_id, details FROM analytics.anomalies
WHERE dt = %(dt)s ORDER BY anomaly_type, entity_id;
"""
