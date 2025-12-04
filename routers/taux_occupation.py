from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import date
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["Monitoring"])


def get_event_capacity_usage(
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
):
    """Taux d'occupation de la capacité des événements"""
    
    conn = get_connection()
    cur = conn.cursor()

    if not date_start or not date_end:
        cur.execute('SELECT MIN("createdAt")::date, MAX("createdAt")::date FROM public."Tickets";')
        row = cur.fetchone()
        date_start = date_start or row[0]
        date_end = date_end or row[1]

   
    query = f"""
        SELECT
            e.id AS event_id,
            e.name AS event_name,
            COUNT(T.id) AS tickets_sold,
            COALESCE(e.capacity, 0) AS capacity,
            COUNT(p.id) AS payments_count
        FROM public."Events" e
        LEFT JOIN public."Products" pr 
            ON pr.event_id = e.id
            AND (%(products)s IS NULL OR pr.id = ANY(%(products)s))
        LEFT JOIN public."Booking" b 
            ON b.product_id = pr.id
            AND (%(companies)s IS NULL OR b.company_id = ANY(%(companies)s))
        LEFT JOIN public."Tickets" T 
            ON T.booking_id = b.id
            AND T."deletedAt" IS NULL
            AND T."createdAt"::date BETWEEN %(date_start)s AND %(date_end)s
        LEFT JOIN public."Payments" p 
            ON p.booking_id = b.id
            AND (%(payment_methods)s IS NULL OR p.payment_method = ANY(%(payment_methods)s))
        WHERE (%(events)s IS NULL OR e.id = ANY(%(events)s))
          AND (%(locations)s IS NULL OR e.location = ANY(%(locations)s))
        GROUP BY e.id, e.name, e.capacity
        ORDER BY tickets_sold DESC;
    """

    params = {
        "date_start": date_start,
        "date_end": date_end,
        "events": events,
        "companies": companies,
        "products": products,
        "payment_methods": payment_methods,
        "locations": locations,
    }

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []

    
    filters_non_matching = False
    if rows:
        if (companies and all(r[2] == 0 for r in rows)) or \
           (products and all(r[2] == 0 for r in rows)) or \
           (locations and all(r[2] == 0 for r in rows)) or \
           (payment_methods and all(r[4] == 0 for r in rows)):  
            filters_non_matching = True

    if filters_non_matching or not rows:
       
        result.append({
            "event_id": None,
            "event_name": None,
            "tickets_sold": 0,
            "capacity": 0,
            "capacity_usage_percentage": 0
        })
    else:
        for r in rows:
            event_id = r[0]
            event_name = r[1] if r[1] is not None else None
            tickets_sold = r[2] or 0
            capacity = r[3] or 0

            usage_rate = (tickets_sold / capacity * 100) if capacity > 0 else 0

            result.append({
                "event_id": event_id,
                "event_name": event_name,
                "tickets_sold": tickets_sold,
                "capacity": capacity,
                "capacity_usage_percentage": round(usage_rate, 2)
            })

    return result


@router.get("/event_capacity_usage", summary="Tickets vendus / capacité par événement")
def event_capacity_usage(
    date_start: Optional[date] = Query(None),
    date_end: Optional[date] = Query(None),
    events: Optional[List[str]] = Query(None),
    companies: Optional[List[str]] = Query(None),
    products: Optional[List[str]] = Query(None),
    payment_methods: Optional[List[str]] = Query(None),
    locations: Optional[List[str]] = Query(None),
):
    """Taux d'occupation de la capacité des événements"""
    
    cache_key = make_cache_key(
        "event_capacity_usage",
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

    result = get_event_capacity_usage(
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
