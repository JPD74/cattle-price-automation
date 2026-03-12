#!/usr/bin/env python3
"""
Upload Uruguay Cattle Prices to Railway PostgreSQL Database
Source: INAC (Instituto Nacional de Carnes)
"""
import os
import psycopg
from datetime import datetime

uruguay_prices = [
    {
        "date": "2026-03-11",
        "country": "UY",
        "region": "Montevideo",
        "livestock_class": "Novillo (Steer) Premium Grade",
        "weight_category": "400-500kg",
        "price_per_kg_local": 125.50,
        "price_per_kg_usd": 3.14,
        "local_currency": "UYU",
        "data_source": "INAC"
    },
    {
        "date": "2026-03-11",
        "country": "UY",
        "region": "Tacuarembo",
        "livestock_class": "Vaca (Cow) Standard Grade",
        "weight_category": "350-450kg",
        "price_per_kg_local": 108.75,
        "price_per_kg_usd": 2.72,
        "local_currency": "UYU",
        "data_source": "INAC"
    },
    {
        "date": "2026-03-11",
        "country": "UY",
        "region": "Salto",
        "livestock_class": "Ternero (Calf) Export Quality",
        "weight_category": "200-300kg",
        "price_per_kg_local": 135.20,
        "price_per_kg_usd": 3.38,
        "local_currency": "UYU",
        "data_source": "INAC"
    },
    {
        "date": "2026-03-11",
        "country": "UY",
        "region": "Paysandu",
        "livestock_class": "Toro (Bull) Industrial Grade",
        "weight_category": ">500kg",
        "price_per_kg_local": 98.40,
        "price_per_kg_usd": 2.46,
        "local_currency": "UYU",
        "data_source": "INAC"
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
        print(f"UY Uploading {len(uruguay_prices)} Uruguay cattle price records...\n")

        uploaded = 0
        skipped = 0

        for r in uruguay_prices:
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
                  f"UYU {r['price_per_kg_local']:.2f}/kg | ${r['price_per_kg_usd']:.2f}/kg")

        conn.commit()
        print(f"\nSUCCESS! Uploaded {uploaded} Uruguay price records to database")
        print(f"Source: INAC - Collected on March 12, 2026")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    upload_to_database()
