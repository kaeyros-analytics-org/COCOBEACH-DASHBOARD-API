from fastapi import APIRouter, Query, HTTPException
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
import json

router = APIRouter(prefix="/chart", tags=["Chart"])

# ---------------------------------------
# Fonction pour convertir les dates en string
# ---------------------------------------
def serialize_dates(obj):
    """Convertit les objets date en string pour la sérialisation JSON"""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

# ---------------------------------------
# Fonction de calcul de l'évolution journalière du revenu
# ---------------------------------------
def compute_daily_revenue(
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Calcule l'évolution journalière du revenu avec filtres optionnels
    
    Returns:
        Liste de dictionnaires avec format: [{"date": "YYYY-MM-DD", "revenue": X}]
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 🔹 Déterminer date_start / date_end si non fournis (derniers 30 jours par défaut)
        if not date_start or not date_end:
            cur.execute('''
                SELECT 
                    COALESCE(MIN("createdAt")::date, CURRENT_DATE - INTERVAL '30 days'),
                    COALESCE(MAX("createdAt")::date, CURRENT_DATE)
                FROM public."Payments" 
                WHERE status = 'success';
            ''')
            row = cur.fetchone()
            date_start = date_start or row[0].isoformat() if row[0] else (date.today() - timedelta(days=30)).isoformat()
            date_end = date_end or row[1].isoformat() if row[1] else date.today().isoformat()

        # 🔹 S'assurer que les dates sont en string
        if isinstance(date_start, date):
            date_start = date_start.isoformat()
        if isinstance(date_end, date):
            date_end = date_end.isoformat()

        # 🔹 Construire les filtres dynamiques
        filters = [
            'p.status = %s',  # Seulement les paiements réussis
            'p."createdAt"::date BETWEEN %s AND %s',
            'b."deletedAt" IS NULL',
            'p."deletedAt" IS NULL'
        ]
        params = ['success', date_start, date_end]

        # 🔹 Filtre par événements (via Products -> Events)
        if events:
            filters.append('pr.event_id = ANY(%s)')
            params.append(events)

        # 🔹 Filtre par entreprises (via Booking)
        if companies:
            filters.append('b.company_id = ANY(%s)')
            params.append(companies)

        # 🔹 Filtre par produits (direct sur Booking)
        if products:
            filters.append('b.product_id = ANY(%s)')
            params.append(products)

        # 🔹 Filtre par méthodes de paiement (via Payments)
        if payment_methods:
            filters.append('p.payment_method = ANY(%s)')
            params.append(payment_methods)

        # 🔹 Filtre par locations (via Products -> Events)
        if locations:
            filters.append('pr.event_id IN (SELECT id FROM public."Events" WHERE location = ANY(%s))')
            params.append(locations)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        # 🔹 Requête simplifiée pour l'évolution journalière du revenu
        query = f'''
            SELECT 
                DATE(p."createdAt") as sale_date,
                SUM(p.amount) as daily_revenue
            FROM public."Payments" p
            INNER JOIN public."Booking" b ON p.booking_id = b.id
            INNER JOIN public."Products" pr ON b.product_id = pr.id
            {where_clause}
            GROUP BY DATE(p."createdAt")
            ORDER BY sale_date ASC
        '''

        print(f" REQUÊTE REVENU JOURNALIER SIMPLIFIÉE: {query}")
        print(f" PARAMÈTRES: {params}")

        cur.execute(query, tuple(params))
        daily_data = cur.fetchall()

        # 🔹 Formater les données en format simplifié
        result = [
            {
                "date": row[0].isoformat() if isinstance(row[0], date) else row[0],
                "revenue": round(float(row[1]), 2) if row[1] else 0.0
            }
            for row in daily_data
        ]

        print(f"💰 RÉSULTAT SIMPLIFIÉ: {len(result)} jours de données de revenu")

        return result

    except Exception as e:
        print(f"❌ Erreur dans compute_daily_revenue: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du calcul du revenu journalier: {str(e)}")
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# ---------------------------------------
# Endpoint FastAPI - Revenu journalier (format simplifié)
# ---------------------------------------
@router.get("/daily_revenue")
def daily_revenue(
    date_start: Optional[str] = Query(None, description="Date de début (YYYY-MM-DD)"),
    date_end: Optional[str] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des méthodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    """
    Retourne l'évolution journalière du revenu 

    """
    try:
        # 🔹 Générer clé cache unique
        cache_key = make_cache_key(
            "daily_revenue",
            date_start=date_start,
            date_end=date_end,
            events=events,
            companies=companies,
            products=products,
            payment_methods=payment_methods,
            locations=locations
        )

        # 🔹 Vérifier cache
        cached = get_cache(cache_key)
        if cached:
            print("⚡ HIT CACHE - daily_revenue")
            return json.loads(cached)

        # 🔹 Calculer si cache miss
        print("🔄 CALCUL DIRECT - daily_revenue")
        result = compute_daily_revenue(
            date_start=date_start,
            date_end=date_end,
            events=events,
            companies=companies,
            products=products,
            payment_methods=payment_methods,
            locations=locations
        )

        # 🔹 Stocker dans le cache
        set_cache(cache_key, json.dumps(result, default=serialize_dates))

        return result

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Erreur inattendue dans daily_revenue: {str(e)}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")