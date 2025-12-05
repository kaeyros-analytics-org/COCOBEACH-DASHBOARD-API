from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import date
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["Monitoring"])

def get_total_canceled_bookings(
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
):
    """Nombre total de réservations annulées"""
    
    conn = get_connection()
    cur = conn.cursor()

    
    if not date_start or not date_end:
        cur.execute('SELECT MIN("date_of_booking")::date, MAX("date_of_booking")::date FROM public."Booking";')
        row = cur.fetchone()
        date_start = date_start or row[0]
        date_end = date_end or row[1]

    filters = ['b."deletedAt" IS NOT NULL', 'b."date_of_booking"::date BETWEEN %s AND %s']
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

    where_clause = f"WHERE {' AND '.join(filters)}"

    query = f"""
        SELECT COUNT(b.id) AS canceled_bookings
        FROM public."Booking" b
        LEFT JOIN public."Products" pr ON pr.id = b.product_id
        LEFT JOIN public."Events" e ON e.id = pr.event_id
        LEFT JOIN public."Payments" p ON p.booking_id = b.id
        {where_clause};
    """

    cur.execute(query, params)
    row = cur.fetchone()
    canceled_bookings = row[0] if row and row[0] is not None else 0

    cur.close()
    conn.close()

    return {"canceled_bookings": canceled_bookings}


@router.get("/canceled_bookings", summary="Nombre total de réservations annulées")
def canceled_bookings(
    date_start: Optional[date] = Query(None),
    date_end: Optional[date] = Query(None),
    events: Optional[List[str]] = Query(None),
    companies: Optional[List[str]] = Query(None),
    products: Optional[List[str]] = Query(None),
    payment_methods: Optional[List[str]] = Query(None),
    locations: Optional[List[str]] = Query(None),
):
    """Endpoint pour récupérer le nombre total de réservations annulées"""
    
    cache_key = make_cache_key(
        "canceled_bookings",
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations,
    )

    cached = get_cache(cache_key)
    if cached:
        return json.loads(cached)

    result = get_total_canceled_bookings(
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations,
    )

    set_cache(cache_key, json.dumps(result))
    return result
