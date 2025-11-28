from fastapi import APIRouter, Query, HTTPException
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import datetime, date
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["Analytics"])

# ---------------------------------------
# Fonction pour convertir les dates en string
# ---------------------------------------
def serialize_dates(obj):
    """Convertit les objets date en string pour la sérialisation JSON"""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

# ---------------------------------------
# Fonction de calcul du revenu total
# ---------------------------------------
def compute_total_revenue(
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
):
    """
    Calcule le revenu total des paiements réussis avec filtres optionnels
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 🔹 Déterminer date_start / date_end si non fournis (avec gestion des NULL)
        if not date_start or not date_end:
            cur.execute('''
                SELECT 
                    COALESCE(MIN("createdAt")::date, CURRENT_DATE) as min_date,
                    COALESCE(MAX("createdAt")::date, CURRENT_DATE) as max_date 
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
            'p."deletedAt" IS NULL'
        ]
        params = ['success', date_start, date_end]

        # 🔹 Filtre par événements
        if events:
            filters.append('b."product_id" IN (SELECT id FROM public."Products" WHERE "event_id" = ANY(%s))')
            params.append(events)

        # 🔹 Filtre par entreprises
        if companies:
            filters.append('b."company_id" = ANY(%s)')
            params.append(companies)

        # 🔹 Filtre par produits
        if products:
            filters.append('b."product_id" = ANY(%s)')
            params.append(products)

        # 🔹 Filtre par méthodes de paiement
        if payment_methods:
            filters.append('p."payment_method" = ANY(%s)')
            params.append(payment_methods)

        # 🔹 Filtre par locations
        if locations:
            filters.append('b."product_id" IN (SELECT id FROM public."Products" WHERE "event_id" IN (SELECT id FROM public."Events" WHERE "location" = ANY(%s)))')
            params.append(locations)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        # 🔹 Requête finale avec gestion des erreurs
        query = f'''
            SELECT COALESCE(SUM(p.amount), 0) as total_revenue
            FROM public."Payments" p
            INNER JOIN public."Booking" b ON p.booking_id = b.id
            {where_clause};
        '''
        
        print(f" Query: {query}")
        print(f" Params: {params}")
        
        cur.execute(query, tuple(params))
        result = cur.fetchone()
        total_revenue = result[0] if result else 0

        # 🔹 Retourner seulement la valeur numérique
        return float(total_revenue) if total_revenue else 0.0

    except Exception as e:
        print(f" Erreur dans compute_total_revenue: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du calcul du revenu: {str(e)}")
    
    finally:
        # 🔹 Fermer les connexions
        if cur:
            cur.close()
        if conn:
            conn.close()

# ---------------------------------------
# Endpoint FastAPI - Revenu total
# ---------------------------------------
@router.get("/total_revenue")
def total_revenue(
    date_start: Optional[str] = Query(None, description="Date de début (YYYY-MM-DD)"),
    date_end: Optional[str] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des méthodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    """
    Retourne le revenu total des paiements réussis avec filtres optionnels
    
    """
    try:
        # 🔹 Générer clé cache unique
        cache_key = make_cache_key(
            "total_revenue",
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
            print("HIT CACHE - total_revenue")
            return {"total_revenue": json.loads(cached)}

        # 🔹 Calculer le revenu si cache miss
        print("CALCUL DIRECT - total_revenue")
        result_value = compute_total_revenue(
            date_start=date_start,
            date_end=date_end,
            events=events,
            companies=companies,
            products=products,
            payment_methods=payment_methods,
            locations=locations
        )

        # 🔹 Stocker dans le cache (seulement la valeur)
        set_cache(cache_key, json.dumps(result_value))

        # 🔹 Retourner seulement le format simple
        return {"total_revenue": result_value}

    except HTTPException:
        # Re-lancer les HTTPException
        raise
    except Exception as e:
        print(f" Erreur inattendue dans l'endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")