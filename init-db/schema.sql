-- =============================================================================
-- MarketPlace Analytics - Data Warehouse (PostgreSQL 18)
-- Exécuté automatiquement au 1er démarrage du conteneur postgres-dwh
-- (/docker-entrypoint-initdb.d). Rejouable à la main : tout est IF NOT EXISTS.
--
--   staging   : copie typée des réponses API (1 table par endpoint)
--   dwh       : modèle en étoile (Kimball) - 4 dimensions + 1 fait
--   analytics : agrégats pré-calculés pour Metabase + journaux (DQ, anomalies)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;

-- -----------------------------------------------------------------------------
-- STAGING
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id        TEXT          NOT NULL,
    dt              DATE          NOT NULL,   -- partition logique = date du run ({{ ds }})
    order_ts        TIMESTAMP,
    customer_id     TEXT,                     -- NULL = commande invité
    seller_id       TEXT,
    product_id      TEXT,
    quantity        INTEGER,
    unit_price      NUMERIC(12, 2),
    total_amount    NUMERIC(12, 2),
    currency        TEXT,
    status          TEXT,
    payment_method  TEXT,
    loaded_at       TIMESTAMP     NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, order_id)
);

CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id    TEXT PRIMARY KEY,
    name         TEXT,
    seller_type  TEXT,
    country      TEXT,
    city         TEXT,
    joined_date  DATE,
    loaded_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id  TEXT PRIMARY KEY,
    seller_id   TEXT,
    name        TEXT,
    category    TEXT,
    unit_price  NUMERIC(12, 2),
    created_at  DATE,
    loaded_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id  TEXT PRIMARY KEY,
    country      TEXT,
    city         TEXT,
    segment      TEXT,
    signup_date  DATE,
    loaded_at    TIMESTAMP NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- DWH : dimensions (SCD type 1 : on écrase les attributs, pas d'historique)
-- Clés naturelles de l'API utilisées comme PK : pas de SCD2 => pas besoin de
-- clé de substitution, et les jointures restent lisibles dans Metabase.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    dt            DATE PRIMARY KEY,
    year          SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    month_name    TEXT     NOT NULL,
    iso_week      SMALLINT NOT NULL,
    day_of_month  SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,   -- 1 = lundi ... 7 = dimanche
    day_name      TEXT     NOT NULL,
    is_weekend    BOOLEAN  NOT NULL
);

INSERT INTO dwh.dim_date
SELECT d::date,
       EXTRACT(YEAR FROM d),
       EXTRACT(QUARTER FROM d),
       EXTRACT(MONTH FROM d),
       (ARRAY['janvier','février','mars','avril','mai','juin','juillet',
              'août','septembre','octobre','novembre','décembre'])[EXTRACT(MONTH FROM d)],
       EXTRACT(WEEK FROM d),
       EXTRACT(DAY FROM d),
       EXTRACT(ISODOW FROM d),
       (ARRAY['lundi','mardi','mercredi','jeudi','vendredi','samedi','dimanche'])[EXTRACT(ISODOW FROM d)],
       EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2023-01-01'::date, '2030-12-31'::date, interval '1 day') AS d
ON CONFLICT (dt) DO NOTHING;

-- is_inferred = TRUE : membre créé à la volée par le chargement des faits parce
-- que la dimension n'avait pas encore été rafraîchie ("late arriving dimension").
-- Le DAG de refresh des dimensions complète ensuite ses attributs.
CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_id    TEXT PRIMARY KEY,
    seller_name  TEXT,
    seller_type  TEXT,
    country      TEXT,
    city         TEXT,
    joined_date  DATE,
    is_inferred  BOOLEAN   NOT NULL DEFAULT FALSE,
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id    TEXT PRIMARY KEY,
    product_name  TEXT,
    category      TEXT,
    seller_id     TEXT,             -- attribut descriptif (pas de FK : pas de flocon)
    list_price    NUMERIC(12, 2),
    created_at    DATE,
    is_inferred   BOOLEAN   NOT NULL DEFAULT FALSE,
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_id  TEXT PRIMARY KEY,
    country      TEXT,
    city         TEXT,
    segment      TEXT,
    signup_date  DATE,
    is_inferred  BOOLEAN   NOT NULL DEFAULT FALSE,
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- DWH : table de faits - grain = 1 ligne par commande
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id           TEXT           PRIMARY KEY,
    dt                 DATE           NOT NULL REFERENCES dwh.dim_date (dt),
    order_ts           TIMESTAMP      NOT NULL,
    seller_id          TEXT           NOT NULL REFERENCES dwh.dim_seller (seller_id),
    product_id         TEXT           NOT NULL REFERENCES dwh.dim_product (product_id),
    customer_id        TEXT                    REFERENCES dwh.dim_customer (customer_id),
    quantity           INTEGER        NOT NULL,
    unit_price         NUMERIC(12, 2) NOT NULL,
    gross_amount       NUMERIC(12, 2) NOT NULL,  -- montant payé par le client
    commission_rate    NUMERIC(5, 4)  NOT NULL,
    commission_amount  NUMERIC(12, 2) NOT NULL,  -- revenu Maelys (0 si annulée/remboursée)
    seller_net_amount  NUMERIC(12, 2) NOT NULL,
    status             TEXT           NOT NULL,
    is_revenue         BOOLEAN        NOT NULL,  -- FALSE si cancelled / refunded
    payment_method     TEXT,
    loaded_at          TIMESTAMP      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fact_orders_dt        ON dwh.fact_orders (dt);
CREATE INDEX IF NOT EXISTS ix_fact_orders_seller_dt ON dwh.fact_orders (seller_id, dt);
CREATE INDEX IF NOT EXISTS ix_fact_orders_product   ON dwh.fact_orders (product_id);

-- -----------------------------------------------------------------------------
-- ANALYTICS : agrégats (reconstruits par dt, DELETE + INSERT)
-- "revenue" = CA brut (GMV) des commandes non annulées/remboursées.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.daily_summary (
    dt                DATE PRIMARY KEY REFERENCES dwh.dim_date (dt),
    total_orders      INTEGER        NOT NULL,
    valid_orders      INTEGER        NOT NULL,
    cancelled_orders  INTEGER        NOT NULL,
    refunded_orders   INTEGER        NOT NULL,
    total_revenue     NUMERIC(14, 2) NOT NULL,
    total_commission  NUMERIC(14, 2) NOT NULL,
    avg_basket        NUMERIC(12, 2),
    unique_customers  INTEGER        NOT NULL,
    active_sellers    INTEGER        NOT NULL,
    top_seller_id     TEXT,
    refreshed_at      TIMESTAMP      NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.seller_daily (
    dt            DATE           NOT NULL,
    seller_id     TEXT           NOT NULL,
    seller_name   TEXT,
    orders        INTEGER        NOT NULL,
    items_sold    INTEGER        NOT NULL,
    revenue       NUMERIC(14, 2) NOT NULL,
    commission    NUMERIC(14, 2) NOT NULL,
    refreshed_at  TIMESTAMP      NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, seller_id)
);

CREATE TABLE IF NOT EXISTS analytics.category_daily (
    dt            DATE           NOT NULL,
    category      TEXT           NOT NULL,
    orders        INTEGER        NOT NULL,
    items_sold    INTEGER        NOT NULL,
    revenue       NUMERIC(14, 2) NOT NULL,
    commission    NUMERIC(14, 2) NOT NULL,
    avg_basket    NUMERIC(12, 2),
    refreshed_at  TIMESTAMP      NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, category)
);

-- Anomalies commerciales détectées (DAG marketplace_anomaly_detect_daily)
CREATE TABLE IF NOT EXISTS analytics.anomalies (
    dt              DATE      NOT NULL,
    anomaly_type    TEXT      NOT NULL,   -- revenue_drop | price_outlier
    entity_id       TEXT      NOT NULL,   -- 'GLOBAL' ou order_id
    observed_value  NUMERIC(14, 2),
    expected_value  NUMERIC(14, 2),
    ratio           NUMERIC(10, 4),
    details         TEXT,
    detected_at     TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, anomaly_type, entity_id)
);

-- Résultats des contrôles qualité (DataQualityOperator)
CREATE TABLE IF NOT EXISTS analytics.dq_check_results (
    id           BIGSERIAL PRIMARY KEY,
    dag_id       TEXT,
    run_id       TEXT,
    table_name   TEXT      NOT NULL,
    partition    TEXT,
    check_name   TEXT      NOT NULL,
    passed       BOOLEAN   NOT NULL,
    details      TEXT,
    checked_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- Vue : dernière vente par vendeur (dashboard "vendeurs inactifs")
CREATE OR REPLACE VIEW analytics.v_seller_activity AS
SELECT s.seller_id,
       s.seller_name,
       s.seller_type,
       s.country,
       MAX(f.dt)                                        AS last_sale_date,
       (SELECT MAX(dt) FROM dwh.fact_orders) - MAX(f.dt) AS days_since_last_sale,
       COUNT(f.order_id)                                AS lifetime_orders,
       COALESCE(SUM(f.gross_amount) FILTER (WHERE f.is_revenue), 0) AS lifetime_revenue
FROM dwh.dim_seller s
LEFT JOIN dwh.fact_orders f ON f.seller_id = s.seller_id
GROUP BY s.seller_id, s.seller_name, s.seller_type, s.country;
