from fastapi import APIRouter, Query, HTTPException
from utils import get_connection, get_cache, set_cache, make_cache_key
from typing import List, Optional
from datetime import date
import json

router = APIRouter(prefix="/analytics", tags=["Analytics"])

def compute_percent_paid_reservations(
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
):
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 🔹 CORRECTION : Utiliser toute l'étendue des données si pas de dates spécifiées
        if not date_start or not date_end:
            cur.execute('SELECT MIN("date_of_booking")::date, MAX("date_of_booking")::date FROM public."Booking" WHERE "deletedAt" IS NULL;')
            row = cur.fetchone()
            date_start = date_start or row[0].isoformat() if row[0] else None
            date_end = date_end or row[1].isoformat() if row[1] else None

        print(f" PÉRIODE ANALYSÉE: {date_start} à {date_end}")

        # 🔹 Filtres SQL dynamiques
        filters = ['b."date_of_booking"::date BETWEEN %s AND %s', 'b."deletedAt" IS NULL']
        params = [date_start, date_end]

        # 🔹 AJOUT : Filtre par événements (via Products -> Events)
        if events:
            filters.append('b.product_id IN (SELECT id FROM public."Products" WHERE event_id = ANY(%s))')
            params.append(events)

        # 🔹 Filtre par entreprises
        if companies:
            filters.append('b.company_id = ANY(%s)')
            params.append(companies)

        # 🔹 Filtre par produits
        if products:
            filters.append('b.product_id = ANY(%s)')
            params.append(products)

        # 🔹 AJOUT : Filtre par méthodes de paiement (via table Payments)
        if payment_methods:
            filters.append('b.id IN (SELECT booking_id FROM public."Payments" WHERE payment_method = ANY(%s) AND status = %s)')
            params.extend([payment_methods, 'success'])

        # 🔹 Filtre par locations (via Products -> Events)
        if locations:
            filters.append('b.product_id IN (SELECT id FROM public."Products" WHERE event_id IN (SELECT id FROM public."Events" WHERE location = ANY(%s)))')
            params.append(locations)

        where_clause = " AND ".join(filters)

        # 🔹 REQUÊTE AMÉLIORÉE : Calcul du pourcentage basé sur les paiements réussis
        query = f'''
            SELECT
                COUNT(DISTINCT b.id) as total_reservations,
                COUNT(DISTINCT p.booking_id) as paid_reservations,
                CASE 
                    WHEN COUNT(DISTINCT b.id) > 0 THEN 
                        ROUND((COUNT(DISTINCT p.booking_id)::decimal / COUNT(DISTINCT b.id)) * 100, 2)
                    ELSE 0 
                END as payment_percentage
            FROM public."Booking" b
            LEFT JOIN public."Payments" p ON (
                b.id = p.booking_id 
                AND p.status = 'success' 
                AND p."deletedAt" IS NULL
            )
            WHERE {where_clause};
        '''

        print(f"REQUÊTE RÉSERVATIONS PAYÉES: {query}")
        print(f" PARAMÈTRES: {params}")

        cur.execute(query, tuple(params))
        result = cur.fetchone()
        
        total_reservations = result[0] if result else 0
        paid_reservations = result[1] if result else 0
        payment_percentage = result[2] if result else 0.0

        print(f" RÉSULTAT: {total_reservations} réservations totales, {paid_reservations} payées, {payment_percentage}%")

        return {
            "percent_paid_reservations": f"{payment_percentage}%",  # Format en pourcentage avec symbole
            "total_reservations": total_reservations,
            "paid_reservations": paid_reservations
            
        }

    except Exception as e:
        print(f" Erreur dans compute_percent_paid_reservations: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du calcul du pourcentage de réservations payées: {str(e)}")
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# ---------------------------------------
# Endpoint FastAPI - Amélioré
# ---------------------------------------
@router.get("/percent")
def percent_paid_reservations(
    date_start: Optional[str] = Query(None, description="Date de début (YYYY-MM-DD)"),
    date_end: Optional[str] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des méthodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    """
    Retourne le pourcentage de réservations payées avec tous les filtres
    ## Logique de Calcul
    - **Réservations payées** : Réservations avec au moins un paiement réussi (status = 'success')
    - **Pourcentage** : (Réservations payées / Réservations totales) × 100
    """
    try:
        cache_key = make_cache_key(
            "percent_paid_reservations",
            date_start=date_start,
            date_end=date_end,
            events=events,
            companies=companies,
            products=products,
            payment_methods=payment_methods,
            locations=locations
        )

        cached = get_cache(cache_key)
        if cached:
            print(" HIT CACHE /reservation_paye/percent")
            return json.loads(cached)

        print(" CALCUL DIRECT /reservation_paye/percent")
        result = compute_percent_paid_reservations(
            date_start=date_start,
            date_end=date_end,
            events=events,
            companies=companies,
            products=products,
            payment_methods=payment_methods,
            locations=locations
        )

        set_cache(cache_key, json.dumps(result))
        return result

    except HTTPException:
        raise
    except Exception as e:
        print(f" Erreur inattendue dans percent_paid_reservations: {str(e)}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")