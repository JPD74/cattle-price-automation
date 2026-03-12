#!/usr/bin/env python3
"""
Upload Paraguay Cattle Prices to Railway PostgreSQL Database
Source: SENACSA (Servicio Nacional de Calidad y Salud Animal)
"""
import os
import psycopg
from datetime import datetime

paraguay_prices = [
    {
        "date": "2026-03-11",
        "country": "PY",
        "region": "San Pedro",
        "livestock_class": "Novillo (Young Steer)",
        "weight_category": "400-500kg",
        "price_per_kg_local": 18500,
        "price_per_kg_usd": 2.55,
        "local_currency": "PYG",
        "data_source": "SENACSA/Market Reports"
    },
    {
        "date": "2026-03-11",
        "country": "PY",
        "region": "Chaco",
        "livestock_class": "Vaca Gorda (Fat Cow)",
        "weight_category": "350-450kg",
        "price_per_kg_local": 16800,
        "price_per_kg_usd": 2.32,
        "local_currency": "PYG",
        "data_source": "SENACSA/Market Reports"
    },
    {
        "date": "2026-03-11",
        "country": "PY",
        "region": "Alto Parana",
        "livestock_class": "Toro (Bull)",
        "weight_category": ">500kg",
        "price_per_kg_local": 19200,
        "price_per_kg_usd": 2.65,
        "local_currency": "PYG",
        "data_source": "SENACSA/Market Reports"
    },
    {
        "date": "2026-03-11",
        "country": "PY",
        "region": "Concepcion",
        "livestock_class": "Ternero (Calf)",
        "weight_category": "200-300kg",
        "price_per_kg_local": 20500,
        "price_per_kg_usd": 2.83,
        "local_currency": "PYG",
        "data_source": "SENACSA/Market Reports"
    }
]

def upload_to_database():
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not found")
        return

    try:
        conn = psycopg.connect(DATABASE_URL, sslmode='disable')
        cur = conn.cursor()
        print("Connected to Railway PostgreSQL database")
        print(f"PY Uploading {len(paraguay_prices)} Paraguay cattle price records...\n")

        uploaded = 0
        skipped = 0

        for r in paraguay_prices:
            cur.execute("""
                SELECT id FROM cattle_prices
                WHERE country = %s AND timestamp::date = %s::date
                AND livestock_class = %s AND region = %s
            """, (r['country'], r['date'], r['livestock_class'], r['region']))

            if cur.fetchone():
                skipped += 1
                print(f"SKIP (exists): {r['date']} | {r['livestock_class']} | {r['region']}")
                continue

            cur.execute("""
                INSERT INTO cattle_prices
                (country, region, livestock_class, weight_category,
                 price_per_kg_local, price_per_kg_usd, local_currency,
                 data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                r['country'], r['region'], r['livestock_class'],
                r['weight_category'], r['price_per_kg_local'],
                r['price_per_kg_usd'], r['local_currency'],
                r['data_source'], r['date']
            ))
            uploaded += 1
            print(f"Uploaded: {r['date']} | {r['livestock_class']} ({r['weight_category']}) | "
                  f"PYG {r['price_per_kg_local']:,.0f}/kg | ${r['price_per_kg_usd']:.2f}/kg")

        conn.commit()
        print(f"\nSUCCESS! Uploaded {uploaded} Paraguay price records to database")
        print(f"Source: SENACSA - Collected on March 12, 2026")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    upload_to_database()
