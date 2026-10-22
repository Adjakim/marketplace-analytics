"""API Marketplace simulée (backoffice Maelys).

Les données sont générées de façon DÉTERMINISTE :
  - le catalogue (vendeurs, produits, clients) dépend d'une graine fixe ;
  - les commandes d'un jour dépendent de seed = md5(date).
Même date => même réponse, ce qui permet de tester l'idempotence du pipeline.

Échelle réduite par rapport aux chiffres du sujet pour rester léger en local :
2 400 vendeurs, 18 000 produits (1/10 du catalogue réel), 20 000 clients,
~8 500 commandes/jour (variable selon le jour de la semaine).
"""
import hashlib
import os
import random
from datetime import date, datetime, timedelta
from functools import lru_cache

from flask import Flask, jsonify, request

API_VERSION = "1.0.0"
API_TOKEN = os.environ.get("API_TOKEN", "formation-token-2026")
CATALOG_SEED = os.environ.get("CATALOG_SEED", "maelys-catalog-v1")
ORDERS_PER_DAY = int(os.environ.get("ORDERS_PER_DAY", "8500"))
# Probabilité de renvoyer une 503 (pour tester le retry du Hook). 0 par défaut.
FAILURE_RATE = float(os.environ.get("API_FAILURE_RATE", "0"))

N_SELLERS = 2400
N_PRODUCTS = 18000
N_CUSTOMERS = 20000

CATEGORIES = {
    "Mode": (15, 120),
    "Maison & Déco": (10, 250),
    "Beauté": (8, 60),
    "Bijoux": (20, 300),
    "Épicerie fine": (5, 45),
    "Enfants": (8, 80),
    "Sport & Loisirs": (12, 200),
    "High-Tech": (20, 400),
    "Papeterie": (3, 35),
    "Animaux": (5, 70),
}
CATEGORY_WEIGHTS = [18, 16, 12, 9, 10, 8, 8, 6, 7, 6]
SELLER_TYPES = ["artisan", "dnvb", "revendeur"]
COUNTRIES = [("FR", 80), ("BE", 7), ("ES", 4), ("IT", 4), ("DE", 3), ("PT", 2)]
CITIES = {
    "FR": ["Paris", "Lyon", "Marseille", "Lille", "Bordeaux", "Nantes", "Toulouse", "Rennes", "Nice", "Strasbourg"],
    "BE": ["Bruxelles", "Liège", "Gand"],
    "ES": ["Madrid", "Barcelone", "Valence"],
    "IT": ["Milan", "Rome", "Turin"],
    "DE": ["Berlin", "Munich", "Hambourg"],
    "PT": ["Lisbonne", "Porto"],
}
NAME_PREFIX = ["Atelier", "Maison", "Studio", "Les Ateliers", "Comptoir", "La Fabrique", "Boutique", "Collectif"]
NAME_SUFFIX = ["Lumière", "Sauvage", "du Sud", "Nomade", "Bohème", "Azur", "Céleste", "des Halles", "Ivoire",
               "Végétal", "Ardoise", "Sésame", "Opaline", "Riviera", "Garance", "Indigo"]
PRODUCT_ADJ = ["Classique", "Premium", "Bio", "Artisanal", "Édition limitée", "Vintage", "Éco", "Mini", "XL", "Essentiel"]
PAYMENT_METHODS = [("card", 70), ("paypal", 18), ("apple_pay", 8), ("bank_transfer", 4)]
STATUSES = [("delivered", 82), ("shipped", 9), ("cancelled", 6), ("refunded", 3)]

CATALOG_START = date(2023, 1, 1)
CATALOG_END = date(2026, 3, 31)

app = Flask(__name__)
app.json.ensure_ascii = False
app.json.sort_keys = False


def _rng(seed_text: str) -> random.Random:
    """Générateur reproductible entre processus (hash() de Python ne l'est pas)."""
    return random.Random(int(hashlib.md5(seed_text.encode("utf-8")).hexdigest(), 16))


def _weighted(rng: random.Random, pairs):
    values, weights = zip(*pairs)
    return rng.choices(values, weights=weights, k=1)[0]


def _random_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, (end - start).days))


# --------------------------------------------------------------------------- catalogue

@lru_cache(maxsize=1)
def _sellers():
    rng = _rng(CATALOG_SEED + ":sellers")
    sellers = []
    for i in range(1, N_SELLERS + 1):
        country = _weighted(rng, COUNTRIES)
        joined = _random_date(rng, CATALOG_START, CATALOG_END)
        # ~6% des vendeurs arrêtent de vendre à une date donnée (churn).
        churn_date = None
        if rng.random() < 0.06:
            churn_date = _random_date(rng, max(joined, date(2026, 1, 1)), date(2026, 12, 31))
        sellers.append({
            "seller_id": f"S{i:05d}",
            "name": f"{rng.choice(NAME_PREFIX)} {rng.choice(NAME_SUFFIX)} {i}",
            "seller_type": rng.choices(SELLER_TYPES, weights=[45, 25, 30])[0],
            "country": country,
            "city": rng.choice(CITIES[country]),
            "joined_date": joined.isoformat(),
            "_churn_date": churn_date,
            # Popularité très inégale (loi de Pareto) : quelques gros vendeurs, une longue traîne.
            "_weight": rng.paretovariate(1.2),
        })
    return sellers


@lru_cache(maxsize=1)
def _products():
    rng = _rng(CATALOG_SEED + ":products")
    sellers = _sellers()
    categories = list(CATEGORIES)
    # Chaque vendeur a une catégorie principale.
    main_cat = {s["seller_id"]: rng.choices(categories, weights=CATEGORY_WEIGHTS)[0] for s in sellers}
    products = []
    for i in range(1, N_PRODUCTS + 1):
        seller = sellers[(i - 1) % N_SELLERS] if i <= N_SELLERS else rng.choice(sellers)
        category = main_cat[seller["seller_id"]] if rng.random() < 0.85 else rng.choice(categories)
        low, high = CATEGORIES[category]
        price = round(rng.uniform(low, high) * rng.choice([1, 1, 1, 0.9, 1.1]), 2)
        created = _random_date(rng, date.fromisoformat(seller["joined_date"]), CATALOG_END)
        products.append({
            "product_id": f"P{i:06d}",
            "seller_id": seller["seller_id"],
            "name": f"{category.split(' ')[0]} {rng.choice(PRODUCT_ADJ)} #{i}",
            "category": category,
            "unit_price": price,
            "created_at": created.isoformat(),
            "_weight": rng.paretovariate(1.5),
        })
    return products


@lru_cache(maxsize=1)
def _customers():
    rng = _rng(CATALOG_SEED + ":customers")
    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        country = _weighted(rng, COUNTRIES)
        customers.append({
            "customer_id": f"C{i:06d}",
            "country": country,
            "city": rng.choice(CITIES[country]),
            "segment": rng.choices(["new", "regular", "vip"], weights=[50, 42, 8])[0],
            "signup_date": _random_date(rng, CATALOG_START, CATALOG_END).isoformat(),
        })
    return customers


def _public(rows):
    """Retire les champs internes (préfixés par _)."""
    return [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]


# --------------------------------------------------------------------------- commandes

def _generate_orders(day: date):
    rng = _rng(day.isoformat())  # seed = md5(date)

    # Volume : saisonnalité hebdo (+ le week-end), légère croissance, bruit.
    weekday_factor = [0.92, 0.95, 0.97, 1.0, 1.05, 1.15, 1.10][day.weekday()]
    growth = 1 + 0.0004 * (day - date(2026, 1, 1)).days
    volume = ORDERS_PER_DAY * weekday_factor * growth * rng.uniform(0.93, 1.07)
    # ~4% des jours : incident (panne paiement, bug front...) => chute des ventes.
    incident = rng.random() < 0.04
    if incident:
        volume *= rng.uniform(0.35, 0.6)
    n_orders = max(0, int(volume))

    # Produits vendables ce jour-là : vendeur déjà inscrit et pas encore parti.
    sellers = {s["seller_id"]: s for s in _sellers()}
    eligible = []
    for p in _products():
        s = sellers[p["seller_id"]]
        if date.fromisoformat(s["joined_date"]) > day:
            continue
        if s["_churn_date"] and s["_churn_date"] <= day:
            continue
        eligible.append(p)
    if not eligible:
        return []
    weights = [p["_weight"] * sellers[p["seller_id"]]["_weight"] for p in eligible]

    customers = _customers()
    picked = rng.choices(eligible, weights=weights, k=n_orders)
    orders = []
    for i, product in enumerate(picked, start=1):
        quantity = rng.choices([1, 2, 3, 4, 5], weights=[70, 18, 7, 3, 2])[0]
        unit_price = product["unit_price"]
        # ~0,1% de prix aberrants (erreur de saisie vendeur) : à détecter côté analytics.
        if rng.random() < 0.001:
            unit_price = round(unit_price * rng.choice([10, 100, 0.01]), 2)
        ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86399))
        # ~2% de commandes en mode invité (pas de compte client).
        customer_id = None if rng.random() < 0.02 else rng.choice(customers)["customer_id"]
        orders.append({
            "order_id": f"ORD-{day:%Y%m%d}-{i:06d}",
            "order_ts": ts.isoformat(),
            "customer_id": customer_id,
            "seller_id": product["seller_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(unit_price * quantity, 2),
            "currency": "EUR",
            "status": _weighted(rng, STATUSES),
            "payment_method": _weighted(rng, PAYMENT_METHODS),
        })
    orders.sort(key=lambda o: o["order_ts"])
    return orders


# --------------------------------------------------------------------------- HTTP

@app.before_request
def _auth_and_chaos():
    if request.path == "/health":
        return None
    if FAILURE_RATE and random.random() < FAILURE_RATE:
        return jsonify(error="Service temporarily unavailable"), 503
    if request.headers.get("Authorization", "") != f"Bearer {API_TOKEN}":
        return jsonify(error="Unauthorized"), 401
    return None


def _limit():
    raw = request.args.get("limit")
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError:
        return -1
    return value if value > 0 else -1


@app.get("/health")
def health():
    return jsonify(status="ok", version=API_VERSION)


@app.get("/orders")
def orders():
    raw = request.args.get("date")
    if not raw:
        return jsonify(error="Missing required parameter 'date' (YYYY-MM-DD)"), 400
    try:
        day = date.fromisoformat(raw)
    except ValueError:
        return jsonify(error=f"Invalid date '{raw}', expected YYYY-MM-DD"), 400
    data = [] if day > date.today() else _generate_orders(day)
    return jsonify(date=day.isoformat(), count=len(data), orders=data)


@app.get("/sellers")
def sellers():
    limit = _limit()
    if limit == -1:
        return jsonify(error="'limit' must be a positive integer"), 400
    data = _public(_sellers())[:limit]
    return jsonify(count=len(data), sellers=data)


@app.get("/products")
def products():
    data = _public(_products())
    return jsonify(count=len(data), products=data)


@app.get("/customers")
def customers():
    limit = _limit()
    if limit == -1:
        return jsonify(error="'limit' must be a positive integer"), 400
    data = _public(_customers())[:limit]
    return jsonify(count=len(data), customers=data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
