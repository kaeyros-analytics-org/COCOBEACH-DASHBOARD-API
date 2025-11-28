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
# Fonction de calcul des stats journalières réservations/paiements - CORRIGÉE
# ---------------------------------------
def compute_daily_reservations_payments(
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
     Calcule les statistiques journalières : réservations, paiements et ratio
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 🔹 Utiliser toute l'étendue des données si pas de dates spécifiées
        if not date_start or not date_end:
            cur.execute('''
                SELECT 
                    COALESCE(MIN("date_of_booking")::date, CURRENT_DATE),
                    COALESCE(MAX("date_of_booking")::date, CURRENT_DATE)
                FROM public."Booking" WHERE "deletedAt" IS NULL;
            ''')
            row = cur.fetchone()
            date_start = date_start or row[0].isoformat() if row[0] else None
            date_end = date_end or row[1].isoformat() if row[1] else None

        # 🔹 S'assurer que les dates sont en string
        if isinstance(date_start, date):
            date_start = date_start.isoformat()
        if isinstance(date_end, date):
            date_end = date_end.isoformat()

        print(f"PÉRIODE ANALYSÉE: {date_start} à {date_end}")

        # 🔹 Construire les filtres communs
        base_filters = ['b."deletedAt" IS NULL']
        params = []

        # 🔹 Filtre par dates
        base_filters.append('b.date_of_booking::date BETWEEN %s AND %s')
        params.extend([date_start, date_end])

        # 🔹 Filtre par événements
        if events:
            base_filters.append('pr.event_id = ANY(%s)')
            params.append(events)

        # 🔹 Filtre par entreprises
        if companies:
            base_filters.append('b.company_id = ANY(%s)')
            params.append(companies)

        # 🔹 Filtre par produits
        if products:
            base_filters.append('b.product_id = ANY(%s)')
            params.append(products)

        # 🔹 Filtre par locations
        if locations:
            base_filters.append('e.location = ANY(%s)')
            params.append(locations)

        # 🔹 CORRECTION : Gérer le filtre payment_methods séparément
        payment_condition = 'p_success.status = %s AND p_success."deletedAt" IS NULL'
        payment_params = ['success']

        if payment_methods:
            payment_condition += ' AND p_success.payment_method = ANY(%s)'
            payment_params.append(payment_methods)

        # 🔹 Construire la clause WHERE commune
        base_where = " AND ".join(base_filters)

        # 🔹 REQUÊTE CORRIGÉE - Gestion correcte des paramètres
        query = f'''
            SELECT 
                DATE(b.date_of_booking) as reservation_date,
                COUNT(DISTINCT b.id) as reservations_count,
                COUNT(DISTINCT p_success.booking_id) as payments_count,
                CASE 
                    WHEN COUNT(DISTINCT b.id) > 0 THEN 
                        ROUND((COUNT(DISTINCT p_success.booking_id)::decimal / COUNT(DISTINCT b.id)) * 100, 2)
                    ELSE 0 
                END as payment_ratio_percent
            FROM public."Booking" b
            LEFT JOIN public."Products" pr ON b.product_id = pr.id
            LEFT JOIN public."Events" e ON pr.event_id = e.id
            LEFT JOIN public."Payments" p_success ON (
                b.id = p_success.booking_id 
                AND {payment_condition}
            )
            WHERE {base_where}
            GROUP BY DATE(b.date_of_booking)
            ORDER BY reservation_date ASC
        '''

        # 🔹 Combiner tous les paramètres dans le bon ordre
        all_params = payment_params + params

        print(f" REQUÊTE CORRIGÉE: {query}")
        print(f" PARAMÈTRES: {all_params}")

        # 🔹 Exécuter la requête
        cur.execute(query, tuple(all_params))
        daily_data = cur.fetchall()

        # 🔹 DEBUG: Vérifier les données brutes
        print(f" DONNÉES BRUTES RÉCUPÉRÉES:")
        for row in daily_data:
            print(f"   {row[0]}: {row[1]} réservations, {row[2]} paiements, {row[3]}% ratio")

        # 🔹 Formater les résultats
        result = [
            {
                "date": row[0].isoformat() if isinstance(row[0], date) else row[0],
                "reservations_count": row[1],
                "payments_count": row[2],
                "payment_ratio": f"{row[3]}%"
            }
            for row in daily_data
        ]

        print(f" RÉSULTAT FINAL: {len(result)} jours analysés")

        return result

    except Exception as e:
        print(f" Erreur dans compute_daily_reservations_payments: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du calcul des stats journalières: {str(e)}")
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# ---------------------------------------
# Endpoint FastAPI - Stats journalières réservations/paiements
# ---------------------------------------
@router.get("/daily_reservations_payments")
def daily_reservations_payments(
    date_start: Optional[str] = Query(None, description="Date de début (YYYY-MM-DD)"),
    date_end: Optional[str] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des méthodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    """
    📊 Retourne les statistiques journalières des réservations et paiements avec ratio
    """
    try:
        # 🔹 Générer clé cache unique
        cache_key = make_cache_key(
            "daily_reservations_payments",
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
            print("⚡ HIT CACHE - daily_reservations_payments")
            return json.loads(cached)

        # 🔹 Calculer si cache miss
        print("🔄 CALCUL DIRECT - daily_reservations_payments")
        result = compute_daily_reservations_payments(
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
        print(f"❌ Erreur inattendue dans daily_reservations_payments: {str(e)}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")