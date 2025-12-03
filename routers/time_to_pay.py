from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import datetime, date, timedelta
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["Analytics"])


def time_to_pay(
        
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
):
    """time to pay (le temps moyen entre la creation de la reservation et le paiement)"""
    conn = get_connection()
    cur = conn.cursor()

    if not date_start or not date_end:
        cur.execute('SELECT MIN("date_of_booking")::date, MAX("date_of_booking")::date FROM public."Booking";')
        row = cur.fetchone()
        date_start = date_start or row[0]
        date_end = date_end or row[1]

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

    where_clause = f"WHERE {' AND '.join(filters)}"

    query = f"""
        SELECT 
            AVG(EXTRACT(EPOCH FROM (p."createdAt" - b."date_of_booking"))) AS avg_time_to_pay_seconds
        FROM public."Booking" b
        LEFT JOIN public."Products" pr ON pr.id = b.product_id
        LEFT JOIN public."Events" e ON e.id = pr.event_id
        LEFT JOIN public."Payments" p ON p.booking_id = b.id AND p.status = 'success'
        {where_clause}
        HAVING COUNT(p.id) > 0
        """
    cur.execute(query, params)
    row = cur.fetchone()
    avg_time_to_pay_seconds = float(row[0]) if row and row[0] is not None else 0 
    return {
        "avg_time_to_pay_seconds": round(avg_time_to_pay_seconds), 
        "avg_time_to_pay_days": round(avg_time_to_pay_seconds / 86400)  
    }    

    
@router.get("/time_to_pay",summary="AVG_TIME_TOPAY")
def get_time_topay(
    date_start: Optional[date] = Query(None, description="Date de debut (YYYY-MM-DD)"),
    date_end: Optional[date] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des methodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    """time to pay (le temps moyen entre la creation de la reservation et le paiement)"""
    # Cache key
    cache_key = make_cache_key(
        "get_time_topay",
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )
    
   


    # Check cache
    cached = get_cache(cache_key)
    if cached:
        print("⚡ HIT CACHE")
        return json.loads(cached)
    

    # Compute ARPU per day
    result = time_to_pay(
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )

    # Save cache
    set_cache(cache_key, json.dumps(result))

    return result
