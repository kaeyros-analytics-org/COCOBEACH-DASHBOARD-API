from fastapi import APIRouter, Query
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import datetime, date, timedelta
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["filters_section"])

def get_filters_metadata():
    """Récupère les donnees pour les filtres disponibles."""
    conn = get_connection()
    cur = conn.cursor()

    metadata = {}

    
    cur.execute('SELECT id, name FROM public."Events" ORDER BY name;')
    events = [{"id": row[0], "name": row[1]} for row in cur.fetchall()]
    metadata["events"] = events

    
    cur.execute('SELECT id, name FROM public."Company" ORDER BY name;')
    companies = [{"id": row[0], "name": row[1]} for row in cur.fetchall()]
    metadata["companies"] = companies

    
    cur.execute('SELECT id, name FROM public."Products" ORDER BY name;')
    products = [{"id": row[0], "name": row[1]} for row in cur.fetchall()]
    metadata["products"] = products

    
    cur.execute('SELECT DISTINCT LOWER(TRIM("payment_method")) FROM public."Payments" ORDER BY LOWER(TRIM("payment_method"));')
    payment_methods = [row[0] for row in cur.fetchall()]
    metadata["payment_methods"] = payment_methods

    
    cur.execute('SELECT DISTINCT location FROM public."Events" ORDER BY location;')
    locations = [row[0] for row in cur.fetchall()]
    metadata["locations"] = locations

    cur.close()
    conn.close()

    return metadata
@router.get("/filters", summary="Filters")
def filters_metadata():
    # generate cache key 
    cache_key = make_cache_key("filters_metadata")

    # check cache
    cached = get_cache(cache_key)
    if cached:
        print("⚡ HIT CACHE")
        return json.loads(cached)

    # get metadata
    result = get_filters_metadata()

    # write into cache
    set_cache(cache_key, json.dumps(result))

    return result

