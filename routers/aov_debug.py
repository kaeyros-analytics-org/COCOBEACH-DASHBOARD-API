
from fastapi import APIRouter, Query, HTTPException
from utils import get_connection, get_cache, set_cache, make_cache_key
from datetime import datetime, date
from typing import List, Optional
import json

router = APIRouter(prefix="/analytics", tags=["AOV"])

def compute_aov_simple(
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    events: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    products: Optional[List[str]] = None,
    payment_methods: Optional[List[str]] = None,
    locations: Optional[List[str]] = None
):
    """
    📊 APPROCHE SIMPLIFIÉE : Calcul AOV basé sur les réservations plutôt que les paiements
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 🔹 APPROCHE ALTERNATIVE : Calcul basé sur le total_amount des réservations
        # plutôt que sur la somme des paiements
        
        base_query = '''
            SELECT 
                COALESCE(SUM(b.total_amount), 0) as total_revenue,
                COUNT(DISTINCT b.id) as unique_reservations
            FROM public."Booking" b
            INNER JOIN public."Products" pr ON b.product_id = pr.id
            INNER JOIN public."Events" e ON pr.event_id = e.id
            WHERE b."deletedAt" IS NULL
            AND EXISTS (
                SELECT 1 FROM public."Payments" p 
                WHERE p.booking_id = b.id 
                AND p.status = 'success'
                AND p."deletedAt" IS NULL
        '''
        
        params = []
        filters = []

        # 🔹 Filtre de date sur les paiements réussis
        if date_start and date_end:
            base_query += ' AND p."createdAt"::date BETWEEN %s AND %s'
            params.extend([date_start, date_end])
        elif date_start:
            base_query += ' AND p."createdAt"::date >= %s'
            params.append(date_start)
        elif date_end:
            base_query += ' AND p."createdAt"::date <= %s'
            params.append(date_end)
        
        base_query += ')'

        # 🔹 Appliquer les autres filtres
        if events:
            filters.append('e.id = ANY(%s)')
            params.append(events)

        if companies:
            filters.append('b.company_id = ANY(%s)')
            params.append(companies)

        if products:
            filters.append('b.product_id = ANY(%s)')
            params.append(products)

        if locations:
            filters.append('e.location = ANY(%s)')
            params.append(locations)

        # 🔹 Filtre spécial pour les méthodes de paiement
        if payment_methods:
            base_query += ' AND EXISTS (SELECT 1 FROM public."Payments" p2 WHERE p2.booking_id = b.id AND p2.status = %s AND p2.payment_method = ANY(%s)'
            params.extend(['success', payment_methods])
            if date_start and date_end:
                base_query += ' AND p2."createdAt"::date BETWEEN %s AND %s'
                params.extend([date_start, date_end])
            base_query += ')'

        if filters:
            base_query += " AND " + " AND ".join(filters)

        print(f"📊 REQUÊTE AOV SIMPLIFIÉE: {base_query}")
        print(f"📊 PARAMÈTRES: {params}")

        cur.execute(base_query, tuple(params))
        result = cur.fetchone()
        
        total_revenue = result[0] if result else 0
        unique_reservations = result[1] if result else 0

        aov = total_revenue / unique_reservations if unique_reservations > 0 else 0

        print(f"📊 RÉSULTAT AOV SIMPLIFIÉ: {total_revenue} / {unique_reservations} = {aov}")

        return float(aov)

    except Exception as e:
        print(f"❌ Erreur dans compute_aov_simple: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du calcul de l'AOV: {str(e)}")
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@router.get("/aov_debug")
def aov_debug():
    """
    🔍 Endpoint de diagnostic pour comprendre le problème des données
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 1. Analyse des méthodes de paiement
        cur.execute('''
            SELECT 
                payment_method,
                COUNT(DISTINCT booking_id) as reservation_count,
                AVG(amount) as avg_amount,
                SUM(amount) as total_amount,
                MAX(amount) as max_amount,
                MIN(amount) as min_amount
            FROM public."Payments" 
            WHERE status = 'success'
            GROUP BY payment_method
            ORDER BY total_amount DESC
        ''')
        
        payment_analysis = cur.fetchall()
        
        # 2. Réservations avec multiples paiements
        cur.execute('''
            SELECT 
                booking_id,
                COUNT(*) as payment_count,
                SUM(amount) as total_paid,
                AVG(amount) as avg_payment
            FROM public."Payments" 
            WHERE status = 'success'
            GROUP BY booking_id
            HAVING COUNT(*) > 1
            ORDER BY total_paid DESC
            LIMIT 10
        ''')
        
        multi_payment_bookings = cur.fetchall()

        # 3. Comparaison Booking.total_amount vs Payments.sum(amount)
        cur.execute('''
            SELECT 
                b.id as booking_id,
                b.total_amount as booking_total,
                SUM(p.amount) as payments_total,
                b.total_amount - SUM(p.amount) as difference
            FROM public."Booking" b
            INNER JOIN public."Payments" p ON b.id = p.booking_id
            WHERE p.status = 'success'
            GROUP BY b.id, b.total_amount
            HAVING ABS(b.total_amount - SUM(p.amount)) > 1
            LIMIT 10
        ''')
        
        amount_discrepancies = cur.fetchall()

        return {
            "payment_method_analysis": [
                {
                    "method": row[0],
                    "reservation_count": row[1],
                    "avg_amount": float(row[2]) if row[2] else 0,
                    "total_amount": float(row[3]) if row[3] else 0,
                    "max_amount": float(row[4]) if row[4] else 0,
                    "min_amount": float(row[5]) if row[5] else 0
                }
                for row in payment_analysis
            ],
            "multi_payment_bookings": [
                {
                    "booking_id": row[0],
                    "payment_count": row[1],
                    "total_paid": float(row[2]) if row[2] else 0,
                    "avg_payment": float(row[3]) if row[3] else 0
                }
                for row in multi_payment_bookings
            ],
            "amount_discrepancies": [
                {
                    "booking_id": row[0],
                    "booking_total": float(row[1]) if row[1] else 0,
                    "payments_total": float(row[2]) if row[2] else 0,
                    "difference": float(row[3]) if row[3] else 0
                }
                for row in amount_discrepancies
            ]
        }

    except Exception as e:
        return {"error": str(e)}
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()