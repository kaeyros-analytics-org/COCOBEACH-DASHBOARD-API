from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import datetime, date, timedelta
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["Analytics"])


def compute_arpu_daily(
        
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
):
    """Calcule l'ARPU (le revenue moyen fait sur chaque user par jour)"""
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
            b."date_of_booking"::date AS day,
            COALESCE(SUM(p.amount),0) AS total_revenue,
            COUNT(DISTINCT b.client_id) AS unique_users
        FROM public."Booking" b
        LEFT JOIN public."Products" pr ON pr.id = b.product_id
        LEFT JOIN public."Events" e ON e.id = pr.event_id
        LEFT JOIN public."Payments" p ON p.booking_id = b.id AND p.status = 'success'
        {where_clause}
        GROUP BY day
        ORDER BY day ASC;
    """

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    
    total_days = (date_end - date_start).days + 1
    result_dict = {str(date_start + timedelta(days=i)): {"arpu": 0} for i in range(total_days)}

    
    for row in rows:
        day = str(row[0])
        total_revenue = float(row[1])
        unique_users = int(row[2])
        arpu = total_revenue / unique_users if unique_users > 0 else 0
        result_dict[day] = {"arpu": arpu}

    cur.close()
    conn.close()

    # Transformer en liste pour le JSON
    result = [{"date": day, "arpu": data["arpu"]} for day, data in sorted(result_dict.items())]
    return result



@router.get("/arpu_daily",summary="ARPU")
def arpu_daily(
    date_start: Optional[date] = Query(None, description="Date de debut (YYYY-MM-DD)"),
    date_end: Optional[date] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des methodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    """Calcule l'ARPU (le revenue moyen fait sur chaque user par jour)"""
    # Cache key
    cache_key = make_cache_key(
        "arpu_daily",
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
    result = compute_arpu_daily(
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
