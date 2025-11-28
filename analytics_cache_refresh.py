from routers.analytics import compute_total_reservations
from routers.total_reservations import compute_total_reservations
from routers.aov import compute_detailed_aov
from routers.daily_evolution import compute_daily_evolution
from routers.daily_reservations_payments import compute_daily_reservations_payments
from routers.daily_revenue import compute_daily_revenue
from routers.quantity_sold import compute_quantity_sold
from routers.reservation_paye import compute_percent_paid_reservations
from routers.revenue import compute_total_revenue
from routers.top_events import compute_top_events_by_revenue

from utils import set_cache, make_cache_key
import json

async def refresh_all_cache():
    print("🔄 Refreshing cache...")

    # Refresh total_reservations sans filtres
    key_total = make_cache_key("total_reservations")
    total_data = compute_total_reservations()
    set_cache(key_total, json.dumps(total_data))

    key_total1 = make_cache_key("total_reservations")
    total_data1 = compute_total_reservations()
    set_cache(key_total1, json.dumps(total_data1))

    key_aov = make_cache_key("aov")
    aov_data = compute_detailed_aov()
    set_cache(key_aov, json.dumps(aov_data))

    key_dailyev = make_cache_key("daily_evolution")
    dailyev_data = compute_daily_evolution()
    set_cache(key_dailyev, json.dumps(dailyev_data))

    key_dailyres = make_cache_key("daily_reservations_payments")
    dailyres_data = compute_daily_reservations_payments()
    set_cache(key_dailyres, json.dumps(dailyres_data))

    key_dailyrev = make_cache_key("daily_revenue")
    dailyrev_data = compute_daily_revenue()
    set_cache(key_dailyrev, json.dumps(dailyrev_data))

    key_qty = make_cache_key("quantity_sold")
    qty_data = compute_quantity_sold()
    set_cache(key_qty, json.dumps(qty_data))

    key_respaid = make_cache_key("reservation_paye")
    respaid_data = compute_percent_paid_reservations()
    set_cache(key_respaid, json.dumps(respaid_data))

    key_rev = make_cache_key("revenue")
    rev_data = compute_total_revenue()
    set_cache(key_rev, json.dumps(rev_data))

    key_topev = make_cache_key("top_events")
    topev_data = compute_top_events_by_revenue()
    set_cache(key_topev, json.dumps(topev_data))



    print("✅ Cache refreshed successfully !")
