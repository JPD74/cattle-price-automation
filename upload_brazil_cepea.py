#!/usr/bin/env python3
"""
Upload Brazil CEPEA Cattle Prices to Railway PostgreSQL Database
Source: https://www.cepea.org.br/br/indicador/boi-gordo.aspx
"""
import os
import psycopg
from datetime import datetime

# Brazil CEPEA cattle price data (collected via agent on 2026-03-12)
brazil_prices = [
    {
        "date": "2026-03-11",
        "country": "BR",
        "region": "Sao Paulo",
        "livestock_class": "Boi Gordo (Fed Cattle)",
        "weight_category": ">450kg",
        "price_per_kg_local": 23.15,
        "price_per_kg_usd": 4.48,
        "local_currency": "BRL",
        "data_source": "CEPEA/ESALQ"
    },
    {
        "date": "2026-03-10",
        "country": "BR",
        "region": "Sao Paulo",
        "livestock_class": "Boi Gordo (Fed Cattle)",
        "weight_category": ">450kg",
        "price_per_kg_local": 23.12,
        "price_per_kg_usd": 4.48,
        "local_currency": "BRL",
        "data_source": "CEPEA/ESALQ"
    },
    {
        "date": "2026-03-09",
        "country": "BR",
        "region": "Sao Paulo",
        "livestock_class": "Boi Gordo (Fed Cattle)",
        "weight_category": ">450kg",
        "price_per_kg_local": 23.16,
        "price_per_kg_usd": 4.48,
        "local_currency": "BRL",
        "data_source": "CEPEA/ESALQ"
    },
    {
        "date": "2026-03-06",
        "country": "BR",
        "region": "Sao Paulo",
        "livestock_class": "Boi Gordo (Fed Cattle)",
        "weight_category": ">450kg",
        "price_per_kg_local": 23.07,
        "price_per_kg_usd": 4.39,
        "local_currency": "BRL",
        "data_source": "CEPEA/ESALQ"
    },
    {
        "date": "2026-03-05",
        "country": "BR",
        "region": "Sao Paulo",
        "livestock_class": "Boi Gordo (Fed Cattle)",
        "weight_category": ">450kg",
        "price_per_kg_local": 23.15,
        "price_per_kg_usd": 4.38,
        "local_currency": "BRL",
        "data_source": "CEPEA/ESALQ"
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
        print(f"BR Uploading {len(brazil_prices)} Brazil CEPEA cattle price records...\n")

        uploaded = 0
        skipped = 0

        for r in brazil_prices:
            cur.execute("""
                SELECT id FROM cattle_prices
                WHERE country = %s AND timestamp::date = %s::date
                AND livestock_class = %s AND region = %s
            """, (r['country'], r['date'], r['livestock_class'], r['region']))

            if cur.fetchone():
                skipped += 1
                print(f"SKIP (exists): {r['date']} | {r['livestock_class']}")
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
            print(f"Uploaded: {r['date']} | {r['livestock_class']} | "
                  f"R$ {r['price_per_kg_local']:.2f}/kg | ${r['price_per_kg_usd']:.2f}/kg")

        conn.commit()
        print(f"\nSUCCESS! Uploaded {uploaded} Brazil CEPEA price records to database")
        print(f"Source: CEPEA/ESALQ - Collected on March 12, 2026")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    upload_to_database()
