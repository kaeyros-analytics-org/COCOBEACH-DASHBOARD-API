from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import date
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["Analytics"])

def compute_cohort_retention(
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

    # filtres
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

    
    cur.execute(f"""
        SELECT b.client_id, DATE_TRUNC('month', MIN(b.date_of_booking)) AS cohort_month
        FROM public."Booking" b
        LEFT JOIN public."Products" pr ON pr.id = b.product_id
        LEFT JOIN public."Events" e ON e.id = pr.event_id
        LEFT JOIN public."Payments" p ON p.booking_id = b.id AND p.status='success'
        {where_clause}
        GROUP BY b.client_id
    """, tuple(params))
    cohort_data = cur.fetchall()

    # dictionnaire des cohortes
    cohort_sizes = {}
    for client_id, cohort_month in cohort_data:
        key = cohort_month.strftime("%Y-%m")
        cohort_sizes[key] = cohort_sizes.get(key, 0) + 1

    
    cur.execute(f"""
        WITH first_booking AS (
            SELECT b.client_id, DATE_TRUNC('month', MIN(b.date_of_booking)) AS cohort_month
            FROM public."Booking" b
            LEFT JOIN public."Products" pr ON pr.id = b.product_id
            LEFT JOIN public."Events" e ON e.id = pr.event_id
            LEFT JOIN public."Payments" p ON p.booking_id = b.id AND p.status='success'
            {where_clause}
            GROUP BY b.client_id
        )
        SELECT
            fb.cohort_month,
            DATE_PART('month', DATE_TRUNC('month', b.date_of_booking) - fb.cohort_month) AS month_number,
            COUNT(DISTINCT b.client_id) AS active_users
        FROM public."Booking" b
        INNER JOIN first_booking fb ON fb.client_id = b.client_id
        LEFT JOIN public."Products" pr ON pr.id = b.product_id
        LEFT JOIN public."Events" e ON e.id = pr.event_id
        LEFT JOIN public."Payments" p ON p.booking_id = b.id AND p.status='success'
        {where_clause}
        GROUP BY fb.cohort_month, month_number
        ORDER BY fb.cohort_month, month_number;
    """, tuple(params*2))  
    rows = cur.fetchall()

    # Construire dictionnaire brut
    cohort_dict = {}
    max_month = 0
    for cohort_month, month_number, active_users in rows:
        key = cohort_month.strftime("%Y-%m")
        if key not in cohort_dict:
            cohort_dict[key] = {}
        cohort_dict[key][int(month_number)] = active_users
        if month_number > max_month:
            max_month = int(month_number)

    
    cohorts_sorted = sorted(cohort_sizes.keys())
    months_since = list(range(max_month + 1))
    values_count = []
    values_percentage = []

    for cohort in cohorts_sorted:
        row_count = []
        row_percent = []
        size = cohort_sizes.get(cohort, 0)
        for m in months_since:
            count = cohort_dict.get(cohort, {}).get(m, 0)
            row_count.append(count)
            pct = (count / size * 100) if size > 0 else 0
            row_percent.append(round(pct, 2))
        values_count.append(row_count)
        values_percentage.append(row_percent)

    cur.close()
    conn.close()

    return {
        "cohorts": cohorts_sorted,
        "months_since": months_since,
        "values_count": values_count,
        "values_percentage": values_percentage,
        #"custormer_count": [cohort_sizes.get(c, 0) for c in cohorts_sorted]
    }

# FastAPI endpoint
@router.get("/cohort_retention", summary="Cohort Retention")
def cohort_retention(
    date_start: Optional[date] = Query(None, description="Date debut (YYYY-MM-DD)"),
    date_end: Optional[date] = Query(None, description="Date fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des methodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    cache_key = make_cache_key(
        "cohort_retention",
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )

    cached = get_cache(cache_key)
    if cached:
        print("⚡ HIT CACHE")
        return json.loads(cached)

    result = compute_cohort_retention(
        date_start=date_start,
        date_end=date_end,
        events=events,
        companies=companies,
        products=products,
        payment_methods=payment_methods,
        locations=locations
    )

    set_cache(cache_key, json.dumps(result))
    return result
