from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import date
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["Monitoring"])

def get_redemption_rate(
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
):
    """Calcul du taux de tickets validés"""
    conn = get_connection()
    cur = conn.cursor()

   
    if not date_start or not date_end:
        cur.execute('SELECT MIN("createdAt")::date, MAX("createdAt")::date FROM public."Tickets";')
        row = cur.fetchone()
        date_start = date_start or row[0]
        date_end = date_end or row[1]


    filters = ['T."deletedAt" IS NULL', 'T."createdAt"::date BETWEEN %s AND %s']
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
            COUNT(T.id) FILTER (WHERE T."is_used") AS redeemed_tickets,
            COUNT(T.id) AS total_tickets      
        FROM public."Tickets" T
        LEFT JOIN public."Booking" b ON b.id = T.booking_id
        LEFT JOIN public."Products" pr ON pr.id = b.product_id
        LEFT JOIN public."Events" e ON e.id = pr.event_id
        LEFT JOIN public."Payments" p ON p.booking_id = b.id AND p.status = 'success'
        {where_clause}
    """
    cur.execute(query, params)
    row = cur.fetchone()

    
    redeemed_tickets = row[0] or 0
    total_tickets = row[1] or 0

    # Calcul du taux
    redemption_rate = (redeemed_tickets / total_tickets * 100) if total_tickets > 0 else 0

    cur.close()
    conn.close()

    return {
        "tickets_valided": redeemed_tickets,
        "total_tickets": total_tickets,
        "redemption_rate_percentage": round(redemption_rate, 2)
    }

@router.get("/redemption_rate", summary="Taux de validation des tickets")
def redemption_rate(
    date_start: Optional[date] = Query(None, description="Date de début (YYYY-MM-DD)"),
    date_end: Optional[date] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des méthodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    """Taux de validation des tickets"""
    cache_key = make_cache_key(
        "redemption_rate",
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )

    # Vérification du cache
    cached = get_cache(cache_key)
    if cached:
        print("⚡ HIT CACHE")
        return json.loads(cached)

    # Calcul du taux
    result = get_redemption_rate(
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )

    # Sauvegarde dans le cache
    set_cache(cache_key, json.dumps(result))

    return result
