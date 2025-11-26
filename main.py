from fastapi import FastAPI
from routers import analytics
from utils import set_cache, make_cache_key
from analytics_cache_refresh import refresh_all_cache
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Analytics API")

# Charger router analytics
app.include_router(analytics.router)

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
