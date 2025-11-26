from routers.analytics import compute_total_reservations
from utils import set_cache, make_cache_key
import json

async def refresh_all_cache():
    print("🔄 Refreshing cache...")

    # Refresh total_reservations sans filtres
    key_total = make_cache_key("total_reservations")
    total_data = compute_total_reservations()
    set_cache(key_total, json.dumps(total_data))

    print("✅ Cache refreshed successfully !")
