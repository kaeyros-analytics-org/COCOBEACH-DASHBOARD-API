# main.py

from fastapi import FastAPI
from routers import analytics, reservation_paye, revenue, total_reservations, aov, aov_debug, quantity_sold, daily_evolution, daily_revenue, top_events, daily_reservations_payments
from utils import set_cache, make_cache_key
from analytics_cache_refresh import refresh_all_cache
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Analytics API")
from routers import marge_event
from routers import ARPU
from routers import cohorte
from routers import total_revenue_produit
from routers import filters_section
from routers import time_to_pay
from routers import taux_echec  
from routers import redemption_rate
from routers import taux_occupation


# Charger router analytics
#app.include_router(analytics.router)
app.include_router(marge_event.router)
app.include_router(ARPU.router)
app.include_router(cohorte.router)
app.include_router(total_revenue_produit.router)
app.include_router(filters_section.router)
# Charger router analytics
#app.include_router(analytics.router)
app.include_router(total_reservations.router) 
app.include_router(reservation_paye.router)
app.include_router(revenue.router)
app.include_router(aov.router)
app.include_router(aov_debug.router)
app.include_router(quantity_sold.router)
app.include_router(daily_evolution.router)
app.include_router(daily_revenue.router)
app.include_router(top_events.router)
app.include_router(daily_reservations_payments.router)
app.include_router(time_to_pay.router)
app.include_router(redemption_rate.router)
app.include_router(taux_echec.router)
app.include_router(taux_occupation.router)
# Intervalle de refresh (en minutes)
REFRESH_MINUTES = int(os.getenv("CACHE_REFRESH_INTERVAL", 10))

@app.on_event("startup")
async def startup_event():
    """Démarre la tâche de rafraîchissement automatique du cache."""
    print(f"🚀 Cache auto-refresh activé : toutes les {REFRESH_MINUTES} minutes")

    async def repeated_refresh():
        while True:
            await refresh_all_cache()
            await asyncio.sleep(REFRESH_MINUTES * 60)

    asyncio.create_task(repeated_refresh())

# Home
@app.get("/")
def home():
    return {"message": "API OK 🚀"}
