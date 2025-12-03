from routers.analytics import compute_total_reservations
from routers.ARPU import compute_arpu_daily 
from routers.marge_event import compute_net_revenue_by_event
from routers.cohorte import compute_cohort_retention
from routers.total_revenue_produit import compute_top_products
from routers.filters_section import get_filters_metadata
from routers.time_to_pay import time_to_pay
from routers.aov import compute_detailed_aov
from routers.daily_evolution import compute_daily_evolution
from routers.daily_reservations_payments import compute_daily_reservations_payments
from routers.daily_revenue import compute_daily_revenue
from routers.quantity_sold import compute_quantity_sold
from routers.reservation_paye import compute_percent_paid_reservations
from routers.revenue import compute_total_revenue
from routers.top_events import compute_top_events_by_revenue
from routers.redemption_rate import get_redemption_rate
from routers.taux_echec import get_taux_echec
from routers.taux_occupation import get_event_capacity_usage
from routers.tableau import get_bookings

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

    key_aov = make_cache_key("aov_detailed")
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

    key_time_to_pay = make_cache_key("get_time_topay")
    time_to_pay_data = time_to_pay()
    set_cache(key_time_to_pay, json.dumps(time_to_pay_data))

    key_redemption_rate = make_cache_key("redemption_rate")
    redemption_rate_data = get_redemption_rate()
    set_cache(key_redemption_rate, json.dumps(redemption_rate_data))

    key_taux_echec = make_cache_key("taux_echec")
    taux_echec_data = get_taux_echec()
    set_cache(key_taux_echec, json.dumps(taux_echec_data))

    key_event_capacity_usage = make_cache_key("event_capacity_usage")
    event_capacity_usage_data = get_event_capacity_usage()
    set_cache(key_event_capacity_usage, json.dumps(event_capacity_usage_data))

    key_tableau_bookings = make_cache_key("bookings")
    tableau_bookings_data = get_bookings()
    set_cache(key_tableau_bookings, json.dumps(tableau_bookings_data))

    print("✅ Cache refreshed successfully !")

