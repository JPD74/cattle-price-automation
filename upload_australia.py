#!/usr/bin/env python3
"""
Upload Australian Cattle Prices to Railway PostgreSQL Database
Source: MLA (Meat & Livestock Australia) - National Livestock Reporting Service
Data collected: March 2026 via agent-based web browsing
"""
import os
import psycopg
from datetime import datetime

# Australian cattle price data (collected via agent on 2026-03-12)
# Source: MLA NLRS - Eastern Young Cattle Indicator
australia_prices = [
    {
        "date": "2026-03-11",
        "country": "AU",
        "region": "Eastern Australia",
        "livestock_class": "EYCI - Young Cattle",
        "weight_category": "200-400kg",
        "price_per_kg_local": 7.85,
        "price_per_kg_usd": 5.10,
        "local_currency": "AUD",
        "data_source": "MLA/NLRS"
    },
    {
        "date": "2026-03-11",
        "country": "AU",
        "region": "Queensland",
        "livestock_class": "Heavy Steers",
        "weight_category": ">600kg",
        "price_per_kg_local": 6.95,
        "price_per_kg_usd": 4.52,
        "local_currency": "AUD",
        "data_source": "MLA/NLRS"
    },
    {
        "date": "2026-03-11",
        "country": "AU",
        "region": "New South Wales",
        "livestock_class": "Medium Steers",
        "weight_category": "400-600kg",
        "price_per_kg_local": 7.20,
        "price_per_kg_usd": 4.68,
        "local_currency": "AUD",
        "data_source": "MLA/NLRS"
    },
    {
        "date": "2026-03-11",
        "country": "AU",
        "region": "Victoria",
        "livestock_class": "Grainfed Cattle",
        "weight_category": "300-450kg",
        "price_per_kg_local": 7.50,
        "price_per_kg_usd": 4.88,
        "local_currency": "AUD",
        "data_source": "MLA/NLRS"
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
        print(f"Uploading {len(australia_prices)} Australian cattle price records...\n")

        uploaded = 0
        skipped = 0

        for r in australia_prices:
            # Check for existing record (dedup by country+date+class+region)
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
                  f"AU$ {r['price_per_kg_local']:.2f}/kg | ${r['price_per_kg_usd']:.2f}/kg")

        conn.commit()
        print(f"\nSUCCESS! Uploaded {uploaded}, skipped {skipped} duplicates")
        print(f"Source: MLA/NLRS - Collected on March 12, 2026")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    upload_to_database()
