from fastapi import APIRouter, Query, HTTPException
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import datetime, date
from typing import List, Optional, Dict, Any
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
# Fonction de calcul détaillé de l'AOV par méthode de paiement
# ---------------------------------------
def compute_detailed_aov(
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
     Calcule l'AOV détaillé par méthode de paiement avec statistiques complètes
    
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 🔹 Construire les filtres de base
        filters = ['p.status = %s']
        params = ['success']

        # 🔹 Filtre par dates
        if date_start and date_end:
            filters.append('p."createdAt"::date BETWEEN %s AND %s')
            params.extend([date_start, date_end])
        elif date_start:
            filters.append('p."createdAt"::date >= %s')
            params.append(date_start)
        elif date_end:
            filters.append('p."createdAt"::date <= %s')
            params.append(date_end)

        # 🔹 Filtre par méthodes de paiement (si spécifié)
        if payment_methods:
            filters.append('p.payment_method = ANY(%s)')
            params.append(payment_methods)

        # 🔹 Autres filtres via jointures
        join_conditions = []
        if events or locations:
            join_conditions.append('INNER JOIN public."Booking" b ON p.booking_id = b.id')
            join_conditions.append('INNER JOIN public."Products" pr ON b.product_id = pr.id')
            join_conditions.append('INNER JOIN public."Events" e ON pr.event_id = e.id')
            
            if events:
                filters.append('e.id = ANY(%s)')
                params.append(events)
            
            if locations:
                filters.append('e.location = ANY(%s)')
                params.append(locations)
        
        if companies or products:
            if not join_conditions:  # Si pas déjà joint
                join_conditions.append('INNER JOIN public."Booking" b ON p.booking_id = b.id')
                join_conditions.append('INNER JOIN public."Products" pr ON b.product_id = pr.id')
            
            if companies:
                filters.append('b.company_id = ANY(%s)')
                params.append(companies)
            
            if products:
                filters.append('b.product_id = ANY(%s)')
                params.append(products)

        # 🔹 Construire la requête de jointure
        join_clause = ' '.join(join_conditions) if join_conditions else ''

        # 🔹 Requête pour les statistiques par méthode de paiement
        query = f'''
            SELECT 
                payment_method,
                COUNT(DISTINCT booking_id) as reservation_count,
                SUM(amount) as total_amount,
                AVG(amount) as aov
            FROM public."Payments" p
            {join_clause}
            WHERE {' AND '.join(filters)}
            GROUP BY payment_method
            ORDER BY total_amount DESC
        '''

        print(f" REQUÊTE DÉTAILLÉE AOV: {query}")
        print(f" PARAMÈTRES: {params}")

        cur.execute(query, tuple(params))
        payment_methods_data = cur.fetchall()

        # 🔹 Calculer les totaux globaux
        total_reservations = sum(row[1] for row in payment_methods_data)
        total_revenue = sum(row[2] for row in payment_methods_data)
        global_aov = total_revenue / total_reservations if total_reservations > 0 else 0

        # 🔹 Formater les données de retour
        result = {
            "aov_global": round(float(global_aov), 2),
            "payment_methods": [
                {
                    "method": row[0],
                    "reservation_count": row[1],
                    "total_amount": round(float(row[2]), 2) if row[2] else 0,
                    "aov": round(float(row[3]), 2) if row[3] else 0
                }
                for row in payment_methods_data
            ],
            "summary": {
                "total_reservations": total_reservations,
                "total_revenue": round(float(total_revenue), 2),
                "global_aov_formula": f"{total_revenue} / {total_reservations} = {round(global_aov, 2)}"
            },
            "filters_applied": {
                "date_range": f"{date_start} to {date_end}" if date_start or date_end else "all dates",
                "payment_methods_filter": payment_methods if payment_methods else "all methods",
                "events_filter": bool(events),
                "companies_filter": bool(companies),
                "products_filter": bool(products),
                "locations_filter": bool(locations)
            }
        }

        print(f" RÉSULTAT DÉTAILLÉ AOV: {result}")

        return result

    except Exception as e:
        print(f" Erreur dans compute_detailed_aov: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du calcul de l'AOV détaillé: {str(e)}")
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# ---------------------------------------
# Endpoint FastAPI - AOV Détaillé
# ---------------------------------------
@router.get("/aov")
def aov_detailed(
    date_start: Optional[str] = Query(None, description="Date de début (YYYY-MM-DD)"),
    date_end: Optional[str] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des méthodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    """
    Retourne l'AOV (Average Order Value) détaillé par méthode de paiement.
    Cet endpoint calcule la valeur moyenne par réservation (AOV) avec une analyse détaillée
    par méthode de paiement. Il retourne :
    - L'AOV global (moyenne de toutes les réservations)
    - Les statistiques détaillées pour chaque méthode de paiement
    - Un résumé des calculs
    
    """
    try:
        # 🔹 Générer clé cache unique basée sur tous les paramètres
        cache_key = make_cache_key(
            "aov_detailed",
            date_start=date_start,
            date_end=date_end,
            events=events,
            companies=companies,
            products=products,
            payment_methods=payment_methods,
            locations=locations
        )

        #  Vérifier le cache
        cached = get_cache(cache_key)
        if cached:
            print("⚡ HIT CACHE - aov_detailed")
            return json.loads(cached)

        #  Calculer les données détaillées si cache miss
        print(" CALCUL DIRECT - aov_detailed")
        result = compute_detailed_aov(
            date_start=date_start,
            date_end=date_end,
            events=events,
            companies=companies,
            products=products,
            payment_methods=payment_methods,
            locations=locations
        )

        #  Stocker dans le cache avec sérialisation custom
        set_cache(cache_key, json.dumps(result, default=serialize_dates))

        return result

    except HTTPException:
        # Re-lancer les HTTPException
        raise
    except Exception as e:
        print(f"❌ Erreur inattendue dans l'endpoint AOV détaillé: {str(e)}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")