
from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import date
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["Analytics"])

from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import date
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["Analytics"])

def compute_top_products(
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
):
    """Calcule le top 10 des produits generant le plus de revenue."""
    conn = get_connection()
    cur = conn.cursor()

    if not date_start or not date_end:
        cur.execute('SELECT MIN("date_of_booking")::date, MAX("date_of_booking")::date FROM public."Booking";')
        row = cur.fetchone()
        date_start = date_start or row[0]
        date_end = date_end or row[1]

    filters = ['b."date_of_booking"::date BETWEEN %s AND %s', 'b."deletedAt" IS NULL']
    params = [date_start, date_end]

    if events:
        filters.append('b."product_id" IN (SELECT id FROM public."Products" WHERE "event_id" = ANY(%s))')
        params.append(events)
    if companies:
        filters.append('b."company_id" = ANY(%s)')
        params.append(companies)
    if products:
        filters.append('b."product_id" = ANY(%s)')
        params.append(products)
    if payment_methods:
        norm_methods = [m.lower().strip() for m in payment_methods]
        filters.append('LOWER(TRIM(p."payment_method")) = ANY(%s)')
        params.append(norm_methods)
    if locations:
        filters.append('b."id" IN (SELECT "booking_id" FROM public."Events" WHERE "location" = ANY(%s))')
        params.append(locations)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    query = f'''
        SELECT pr.id, pr.name, COALESCE(SUM(p.amount), 0) as revenue
        FROM public."Products" pr
        LEFT JOIN public."Booking" b ON b.product_id = pr.id
        LEFT JOIN public."Payments" p ON p.booking_id = b.id AND p.status = 'success'
        {where_clause}
        GROUP BY pr.id, pr.name
        ORDER BY revenue DESC
        LIMIT 10;
    '''
    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    
    if not rows:
        result = [{"product_id": None, "product_name": None, "revenue": 0}]
    else:
        result = [
            {"product_id": r[0], "product_name": r[1], "revenue": float(r[2])} for r in rows
        ]

    cur.close()
    conn.close()
    return result

@router.get("/top_products", summary="Top Products")
def top_products(
    date_start: Optional[date] = Query(None, description="Date debut (YYYY-MM-DD)"),
    date_end: Optional[date] = Query(None, description="Date fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des methodes paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    cache_key = make_cache_key(
        "top_products",
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

    result = compute_top_products(
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


@router.get("/top_products",summary="Top Products ")
def top_products(
    date_start: Optional[date] = Query(None, description="Date debut (YYYY-MM-DD)"),
    date_end: Optional[date] = Query(None, description="Date fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des methodes paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
    
):
    """Calcule le top 10 des produits generant le plus de revenue"""
    cache_key = make_cache_key(
        "top_products",
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
        print("⚡ HIT CACHE")
        return json.loads(cached)

    result = compute_top_products(
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
