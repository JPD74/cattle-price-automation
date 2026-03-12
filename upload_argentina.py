#!/usr/bin/env python3
"""
Upload Argentine Cattle Prices to Railway PostgreSQL Database
Source: Mercado de Liniers (Buenos Aires) - Primary cattle auction market
"""
import os
import psycopg
from datetime import datetime
from fx_rates import get_usd_rates, to_usd

# Argentine cattle price data - live weight prices
# Source: Mercado de Liniers - collected via agent
argentina_prices = [
    {"date": "2025-04-15", "country": "AR", "region": "Buenos Aires",
     "livestock_class": "Novillito", "weight_category": "300-400kg",
     "price_per_kg_local": 3044.70, "local_currency": "ARS", "data_source": "Mercado de Liniers"},
    {"date": "2025-04-15", "country": "AR", "region": "Buenos Aires",
     "livestock_class": "Novillo", "weight_category": "400-500kg",
     "price_per_kg_local": 2788.13, "local_currency": "ARS", "data_source": "Mercado de Liniers"},
    {"date": "2025-04-15", "country": "AR", "region": "Buenos Aires",
     "livestock_class": "Vaquillona", "weight_category": "300-400kg",
     "price_per_kg_local": 2913.27, "local_currency": "ARS", "data_source": "Mercado de Liniers"},
    {"date": "2025-04-15", "country": "AR", "region": "Buenos Aires",
     "livestock_class": "Vaca", "weight_category": "350-450kg",
     "price_per_kg_local": 1481.76, "local_currency": "ARS", "data_source": "Mercado de Liniers"},
    {"date": "2025-04-15", "country": "AR", "region": "Buenos Aires",
     "livestock_class": "Ternero", "weight_category": "150-250kg",
     "price_per_kg_local": 3200.00, "local_currency": "ARS", "data_source": "Mercado de Liniers"},
    {"date": "2025-04-15", "country": "AR", "region": "Cordoba",
     "livestock_class": "Novillo", "weight_category": "400-500kg",
     "price_per_kg_local": 2750.00, "local_currency": "ARS", "data_source": "Mercado de Liniers"},
    {"date": "2025-04-15", "country": "AR", "region": "Santa Fe",
     "livestock_class": "Novillito", "weight_category": "300-400kg",
     "price_per_kg_local": 3010.00, "local_currency": "ARS", "data_source": "Mercado de Liniers"},
    {"date": "2025-04-15", "country": "AR", "region": "Entre Rios",
     "livestock_class": "Vaquillona", "weight_category": "300-400kg",
     "price_per_kg_local": 2880.00, "local_currency": "ARS", "data_source": "Mercado de Liniers"},
    {"date": "2025-04-15", "country": "AR", "region": "La Pampa",
     "livestock_class": "Vaca", "weight_category": "350-450kg",
     "price_per_kg_local": 1450.00, "local_currency": "ARS", "data_source": "Mercado de Liniers"},
]

def upload_to_database():
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not found")
        return

    rates = get_usd_rates()

    try:
        conn = psycopg.connect(DATABASE_URL, sslmode='disable')
        cur = conn.cursor()
        print(f"AR: Uploading {len(argentina_prices)} records...")
        uploaded = 0
        skipped = 0

        for r in argentina_prices:
            cur.execute("""
                SELECT id FROM cattle_prices
                WHERE country = %s AND timestamp::date = %s::date
                AND livestock_class = %s AND region = %s
            """, (r['country'], r['date'], r['livestock_class'], r['region']))

            if cur.fetchone():
                skipped += 1
                continue

            usd_price = to_usd(r['price_per_kg_local'], r['local_currency'], rates)

            cur.execute("""
                INSERT INTO cattle_prices
                (country, region, livestock_class, weight_category,
                 price_per_kg_local, price_per_kg_usd, local_currency,
                 data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                r['country'], r['region'], r['livestock_class'],
                r['weight_category'], r['price_per_kg_local'],
                usd_price, r['local_currency'],
                r['data_source'], r['date']
            ))
            uploaded += 1
            print(f"  {r['livestock_class']} ({r['region']}) | "
                  f"ARS{r['price_per_kg_local']:.2f} = US${usd_price:.2f}/kg")

        conn.commit()
        print(f"AR: {uploaded} uploaded, {skipped} skipped")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    upload_to_database()
