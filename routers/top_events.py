from fastapi import APIRouter, Query, HTTPException
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import datetime, date
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
# Fonction de calcul du top 10 des événements par revenu (format simplifié)
# ---------------------------------------
def compute_top_events_by_revenue(
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    companies: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Calcule le top 10 des événements par revenu généré (format simplifié)
    
    Returns:
        Liste de dictionnaires avec format: [{"event_name": "X", "total_revenue": Y}]
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 🔹 Déterminer date_start / date_end si non fournis
        if not date_start or not date_end:
            cur.execute('''
                SELECT 
                    COALESCE(MIN("createdAt")::date, CURRENT_DATE),
                    COALESCE(MAX("createdAt")::date, CURRENT_DATE)
                FROM public."Payments" 
                WHERE status = 'success';
            ''')
            row = cur.fetchone()
            date_start = date_start or row[0].isoformat() if row[0] else None
            date_end = date_end or row[1].isoformat() if row[1] else None

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
            'p."deletedAt" IS NULL',
            'e."deletedAt" IS NULL'
        ]
        params = ['success', date_start, date_end]

        # 🔹 Filtre par entreprises (via Events)
        if companies:
            filters.append('e.company_id = ANY(%s)')
            params.append(companies)

        # 🔹 Filtre par méthodes de paiement (via Payments)
        if payment_methods:
            filters.append('p.payment_method = ANY(%s)')
            params.append(payment_methods)

        # 🔹 Filtre par locations (direct sur Events)
        if locations:
            filters.append('e.location = ANY(%s)')
            params.append(locations)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        # 🔹 Requête simplifiée pour le top 10 des événements par revenu
        query = f'''
            SELECT 
                e.name as event_name,
                SUM(p.amount) as total_revenue
            FROM public."Events" e
            INNER JOIN public."Products" pr ON e.id = pr.event_id
            INNER JOIN public."Booking" b ON pr.id = b.product_id
            INNER JOIN public."Payments" p ON b.id = p.booking_id
            {where_clause}
            GROUP BY e.name
            ORDER BY total_revenue DESC
            LIMIT 10
        '''

        print(f"🏆 REQUÊTE TOP ÉVÉNEMENTS SIMPLIFIÉE: {query}")
        print(f"🏆 PARAMÈTRES: {params}")

        cur.execute(query, tuple(params))
        events_data = cur.fetchall()

        # 🔹 Formater les données en format simplifié
        result = [
            {
                "event_name": row[0],
                "total_revenue": round(float(row[1]), 2) if row[1] else 0.0
            }
            for row in events_data
        ]

        print(f"🏆 RÉSULTAT SIMPLIFIÉ: {len(result)} événements trouvés")

        return result

    except Exception as e:
        print(f"❌ Erreur dans compute_top_events_by_revenue: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du calcul du top événements: {str(e)}")
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# ---------------------------------------
# Endpoint FastAPI - Top 10 des événements par revenu (format simplifié)
# ---------------------------------------
@router.get("/top_events_by_revenue")
def top_events_by_revenue(
    date_start: Optional[str] = Query(None, description="Date de début (YYYY-MM-DD)"),
    date_end: Optional[str] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des méthodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    """
     Retourne le top 10 des événements par revenu généré 
    
    """
    try:
        # 🔹 Générer clé cache unique
        cache_key = make_cache_key(
            "top_events_by_revenue",
            date_start=date_start,
            date_end=date_end,
            companies=companies,
            payment_methods=payment_methods,
            locations=locations
        )

        # 🔹 Vérifier cache
        cached = get_cache(cache_key)
        if cached:
            print("⚡ HIT CACHE - top_events_by_revenue")
            return json.loads(cached)

        # 🔹 Calculer si cache miss
        print("🔄 CALCUL DIRECT - top_events_by_revenue")
        result = compute_top_events_by_revenue(
            date_start=date_start,
            date_end=date_end,
            companies=companies,
            payment_methods=payment_methods,
            locations=locations
        )

        # 🔹 Stocker dans le cache
        set_cache(cache_key, json.dumps(result, default=serialize_dates))

        return result

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Erreur inattendue dans top_events_by_revenue: {str(e)}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")