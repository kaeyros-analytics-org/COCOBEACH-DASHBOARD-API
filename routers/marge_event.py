from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import datetime, date, timedelta
from typing import List, Optional
import json



router = APIRouter(prefix="/analytics", tags=["Analytics"])

# Fonction de calcul du revenue net par evenement
def compute_net_revenue_by_event(
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

    if not date_start or not date_end:
        cur.execute('SELECT MIN("date_of_booking")::date, MAX("date_of_booking")::date FROM public."Booking";')
        row = cur.fetchone()
        date_start = date_start or row[0]
        date_end = date_end or row[1]

    # filtres pour Booking
    filters = ['b."deletedAt" IS NULL', 'b."date_of_booking"::date BETWEEN %s AND %s']
    params = [date_start, date_end]

    if events:
        filters.append('pr."event_id" = ANY(%s)')
        params.append(events)
    if companies:
        filters.append('b."company_id" = ANY(%s)')
        params.append(companies)
    if products:
        filters.append('b."product_id" = ANY(%s)')
        params.append(products)
    if payment_methods:
        filters.append('p."payment_method" = ANY(%s)')
        params.append(payment_methods)
    if locations:
        filters.append('e."location" = ANY(%s)')
        params.append(locations)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    # Query SQL
    query = f"""
    SELECT
        e.name AS event_name,
        COALESCE(SUM(p.amount),0) AS revenue,
        COALESCE(SUM(ex.amount),0) AS expenses,
        COALESCE(SUM(p.amount),0) - COALESCE(SUM(ex.amount),0) AS net_margin
    FROM public."Events" e
    LEFT JOIN public."Products" pr ON pr.event_id = e.id
    LEFT JOIN public."Booking" b ON b.product_id = pr.id
    LEFT JOIN public."Payments" p ON p.booking_id = b.id AND p.status='success'
    LEFT JOIN public."Expenses" ex ON ex.booking_id = b.id
    {where_clause}
    GROUP BY e.id, e.name
    ORDER BY net_margin DESC;
    """

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    
    if not rows:
        result = [{"event_name": None, "revenue": 0, "expenses": 0, "marge": 0}]
    else:
        result = [
            {"event_name": row[0], "revenue": float(row[1]), "expenses": float(row[2]), "marge": float(row[3])}
            for row in rows
        ]

    cur.close()
    conn.close()
    return result


@router.get("/net_revenue_by_event",summary="Revenue net / marge simple par evenement")
def net_revenue_by_event(
    date_start: Optional[date] = Query(None, description="Date de debut (YYYY-MM-DD)"),
    date_end: Optional[date] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des methodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    # genere cle
    cache_key = make_cache_key(
        "net_revenue_by_event",
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )

    # verifie dans le cache
    cached = get_cache(cache_key)
    if cached:
        print("HIT CACHE")
        return json.loads(cached)

    # revenu net
    result = compute_net_revenue_by_event(
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )

    # stock en cache
    set_cache(cache_key, json.dumps(result))

    return result


@router.get("/net_revenue_by_event",summary="Revenue net / marge simple par événement")
def net_revenue_by_event(
    date_start: Optional[date] = Query(None, description="Date de début (YYYY-MM-DD)"),
    date_end: Optional[date] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des méthodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    """revenue net (marge simple) par événement."""
    # genere cle
    cache_key = make_cache_key(
        "net_revenue_by_event",
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )

    # verifie dans le cache
    cached = get_cache(cache_key)
    if cached:
        print("⚡ HIT CACHE")
        return json.loads(cached)

    # revenu net
    result = compute_net_revenue_by_event(
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )

    # stock en cache
    set_cache(cache_key, json.dumps(result))

    return result
