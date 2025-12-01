from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import date
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["Tableau"])

def get_bookings(
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None,
    page: Optional[int] = 1
):
    conn = get_connection()
    cur = conn.cursor()

    # 
    if not date_start or not date_end:
        cur.execute('SELECT MIN("date_of_booking")::date, MAX("date_of_booking")::date FROM public."Booking";')
        row = cur.fetchone()
        date_start = date_start or row[0]
        date_end = date_end or row[1]

    # Filtres
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

    offset = (page - 1) * 20

    # Query principale
    query = f"""
        SELECT 
            b.id AS booking_id,
            b.date_of_booking,
            c.id AS client_id,
            c.name AS client_name,
            pr.name AS product,
            e.name AS event,
            co.name AS company,
            e.location,
            COALESCE(p.amount,0) AS amount_paid,
            p.payment_method,
            p.status AS payment_status
        FROM public."Booking" b
        LEFT JOIN public."Client" c ON c.id = b.client_id
        LEFT JOIN public."Products" pr ON pr.id = b.product_id
        LEFT JOIN public."Events" e ON e.id = pr.event_id
        LEFT JOIN public."Company" co ON co.id = b.company_id
        LEFT JOIN public."Payments" p ON p.booking_id = b.id
        {where_clause}
        ORDER BY b.date_of_booking DESC
        LIMIT %s OFFSET %s;

    """
    
    cur.execute(query, tuple(params + [20, offset]))
    rows = cur.fetchall()


    
    result = []
    for r in rows:
        result.append({
            "booking_id": r[0] if r[0] is not None else None,
            "date_of_booking": str(r[1]) if r[1] is not None else None,
            "client_id": r[2] if r[2] is not None else None,
            "client_name": r[3] if r[3] is not None else None,
            "product": r[4] if r[4] is not None else None,
            "event": r[5] if r[5] is not None else None,
            "company": r[6] if r[6] is not None else None,
            "location": r[7] if r[7] is not None else None,
            "amount_paid": float(r[8]) if r[8] is not None else 0,
            "payment_methods": r[9] if r[9] is not None else None,
            "payment_status": r[10] if r[10] is not None else None
        })

    cur.close()
    conn.close()
    return result


@router.get("/bookings", summary="Tableau des bookings clients")
def bookings(
    date_start: Optional[date] = Query(None, description="Date debut (YYYY-MM-DD)"),
    date_end: Optional[date] = Query(None, description="Date fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des methodes paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations"),
    page: Optional[int] = Query(1, description="Numéro de page")
):
    #page_size = 20
    cache_key = make_cache_key(
        "bookings",
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations,
        page=page
        #page_size=page_size
    )

    cached = get_cache(cache_key)
    if cached:
        return json.loads(cached)
   

    result = get_bookings(
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations,
        page=page
        #page_size=page_size
    )

    set_cache(cache_key, json.dumps(result))
    return result
