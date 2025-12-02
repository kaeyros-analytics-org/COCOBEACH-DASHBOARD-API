#routers/analytics.py

from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import datetime, date
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["Analytics1"])

# ---------------------------------------
# Fonction de calcul du total de réservation
# ---------------------------------------
def compute_total_reservations(
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
):
    conn = get_connection()
    cur = conn.cursor()

    # 🔹 Déterminer date_start / date_end si non fournis
    if not date_start or not date_end:
        cur.execute('SELECT MIN("date_of_booking")::date, MAX("date_of_booking")::date FROM public."Booking";')
        row = cur.fetchone()
        date_start = date_start or row[0]
        date_end = date_end or row[1]

    # 🔹 Construire les filtres dynamiques
    filters = ['"date_of_booking"::date BETWEEN %s AND %s', '"deletedAt" IS NULL']
    params = [date_start, date_end]

    if events:
        filters.append('"product_id" IN (SELECT id FROM public."Products" WHERE "event_id" = ANY(%s))')
        params.append(events)
    if companies:
        filters.append('"company_id" = ANY(%s)')
        params.append(companies)
    if products:
        filters.append('"product_id" = ANY(%s)')
        params.append(products)
    if payment_methods:
        filters.append('"payment_method" = ANY(%s)')
        params.append(payment_methods)
    if locations:
        filters.append('"id" IN (SELECT "booking_id" FROM public."Events" WHERE "location" = ANY(%s))')
        params.append(locations)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    query = f'SELECT COUNT(*) FROM public."Booking" {where_clause};'
    cur.execute(query, tuple(params))
    total = cur.fetchone()[0]

    cur.close()
    conn.close()

    return {"total_reservations": total}

# ---------------------------------------
# Endpoint FastAPI
# ---------------------------------------
@router.get("/anlytics")
def total_reservations(
    date_start: Optional[str] = Query(None, description="Date de début (YYYY-MM-DD)"),
    date_end: Optional[str] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des méthodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    # 🔹 Générer clé cache
    cache_key = make_cache_key(
        "total_reservations",
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )

    # 🔹 Vérifier cache
    cached = get_cache(cache_key)
    if cached:
        print("⚡ HIT CACHE")
        return json.loads(cached)

    # 🔹 Calculer le total
    result = compute_total_reservations(
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )

    # 🔹 Stocker dans le cache
    set_cache(cache_key, json.dumps(result))

    return result
