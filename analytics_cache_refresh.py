from routers.analytics import compute_total_reservations
from routers.ARPU import compute_arpu_daily
from routers.marge_event import compute_net_revenue_by_event
from routers.cohorte import compute_cohort_retention
from routers.total_revenue_produit import compute_top_products
from routers.filters_section import get_filters_metadata
from routers.tableau import get_bookings
from utils import set_cache, make_cache_key
import json

async def refresh_all_cache():
    print("🔄 Refreshing cache...")

    # Refresh total_reservations sans filtres
    key_total = make_cache_key("total_reservations")
    total_data = compute_total_reservations()
    set_cache(key_total, json.dumps(total_data))

    key_total_arpu = make_cache_key("arpu_daily")
    total_arpu= compute_arpu_daily()
    set_cache(key_total_arpu, json.dumps(total_arpu))

    key_total_marge = make_cache_key("net_revenue_by_event")
    total_marge= compute_net_revenue_by_event()
    set_cache(key_total_marge, json.dumps(total_marge))

    key_total_top_products = make_cache_key("top_products")
    total_top_products= compute_top_products()
    set_cache(key_total_top_products, json.dumps(total_top_products))

    key_total_cohort = make_cache_key("cohort_retention")
    total_cohort= compute_cohort_retention()
    set_cache(key_total_cohort, json.dumps(total_cohort))

    key_filters = make_cache_key("filters_metadata")
    filters_metadata = get_filters_metadata()
    set_cache(key_filters, json.dumps(filters_metadata))

    key_tableau = make_cache_key("bookings")
    bookings_data = get_bookings()
    set_cache(key_tableau, json.dumps(bookings_data))

    print("✅ Cache refreshed successfully !")
