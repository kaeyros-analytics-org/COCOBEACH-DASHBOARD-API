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
# Fonction de calcul des quantités vendues
# ---------------------------------------
def compute_quantity_sold(
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Calcule la quantité totale de produits vendus 
    
    Returns:
        Dict contenant:
        - total_quantity: Quantité totale vendue
        - products: Détails par produit
        - summary: Résumé des calculs
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

        # 🔹 Requête pour les quantités vendues par produit
        query = f'''
            SELECT 
                b.product_id,
                pr.name as product_name,
                pr.price as unit_price,
                SUM(b.quantity) as total_quantity_sold,
                SUM(b.total_amount) as total_revenue,
                COUNT(DISTINCT b.id) as reservation_count
            FROM public."Booking" b
            INNER JOIN public."Products" pr ON b.product_id = pr.id
            INNER JOIN public."Payments" p ON b.id = p.booking_id
            {where_clause}
            GROUP BY b.product_id, pr.name, pr.price
            ORDER BY total_quantity_sold DESC
        '''

        print(f"📦 REQUÊTE QUANTITÉ VENDUE: {query}")
        print(f"📦 PARAMÈTRES: {params}")

        cur.execute(query, tuple(params))
        products_data = cur.fetchall()

        # 🔹 Calculer les totaux globaux
        total_quantity = sum(row[3] for row in products_data)
        total_revenue = sum(row[4] for row in products_data)
        total_reservations = sum(row[5] for row in products_data)

        # 🔹 Formater les données de retour
        result = {
            "total_quantity_sold": total_quantity,
            "total_revenue": round(float(total_revenue), 2),
            "total_reservations": total_reservations
            
        }

        print(f"📦 RÉSULTAT QUANTITÉ VENDUE: {total_quantity} unités across {len(products_data)} produits")

        return result

    except Exception as e:
        print(f"❌ Erreur dans compute_quantity_sold: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du calcul des quantités vendues: {str(e)}")
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# ---------------------------------------
# Endpoint FastAPI - Quantités vendues
# ---------------------------------------
@router.get("/quantity_sold")
def quantity_sold(
    date_start: Optional[str] = Query(None, description="Date de début (YYYY-MM-DD)"),
    date_end: Optional[str] = Query(None, description="Date de fin (YYYY-MM-DD)"),
    events: Optional[List[str]] = Query(None, description="Liste des Event IDs"),
    companies: Optional[List[str]] = Query(None, description="Liste des Company IDs"),
    products: Optional[List[str]] = Query(None, description="Liste des Product IDs"),
    payment_methods: Optional[List[str]] = Query(None, description="Liste des méthodes de paiement"),
    locations: Optional[List[str]] = Query(None, description="Liste des locations")
):
    """
    Retourne la quantité totale de produits vendus avec analyse détaillée
    
    """
    try:
        # 🔹 Générer clé cache unique
        cache_key = make_cache_key(
            "quantity_sold",
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
            print("⚡ HIT CACHE - quantity_sold")
            return json.loads(cached)

        # 🔹 Calculer si cache miss
        print("🔄 CALCUL DIRECT - quantity_sold")
        result = compute_quantity_sold(
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
        print(f"❌ Erreur inattendue dans quantity_sold: {str(e)}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")