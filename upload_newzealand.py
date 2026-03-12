#!/usr/bin/env python3
"""
Upload New Zealand Cattle Prices to Railway PostgreSQL Database
Source: https://www.interest.co.nz/rural/beef/steer-p2
"""
import os
import psycopg
from datetime import datetime

newzealand_prices = [
    {
        "date": "2026-03-09",
        "country": "NZ",
        "region": "South Island",
        "livestock_class": "Steer Prime P2",
        "weight_category": "270-295kg",
        "price_per_kg_local": 8.65,
        "price_per_kg_usd": 5.19,
        "local_currency": "NZD",
        "data_source": "interest.co.nz/Silver Fern Farms"
    },
    {
        "date": "2026-03-09",
        "country": "NZ",
        "region": "South Island",
        "livestock_class": "Steer Prime P2",
        "weight_category": "220-245kg",
        "price_per_kg_local": 8.65,
        "price_per_kg_usd": 5.19,
        "local_currency": "NZD",
        "data_source": "interest.co.nz/Silver Fern Farms"
    },
    {
        "date": "2026-03-09",
        "country": "NZ",
        "region": "South Island",
        "livestock_class": "Steer Prime P2",
        "weight_category": "195-220kg",
        "price_per_kg_local": 8.35,
        "price_per_kg_usd": 5.01,
        "local_currency": "NZD",
        "data_source": "interest.co.nz/Silver Fern Farms"
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
        print(f"NZ Uploading {len(newzealand_prices)} New Zealand cattle price records...\n")

        uploaded = 0
        skipped = 0

        for r in newzealand_prices:
            cur.execute("""
                SELECT id FROM cattle_prices
                WHERE country = %s AND timestamp::date = %s::date
                AND livestock_class = %s AND weight_category = %s
            """, (r['country'], r['date'], r['livestock_class'], r['weight_category']))

            if cur.fetchone():
                skipped += 1
                print(f"SKIP (exists): {r['date']} | {r['livestock_class']} {r['weight_category']}")
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
                  f"NZ$ {r['price_per_kg_local']:.2f}/kg | ${r['price_per_kg_usd']:.2f}/kg")

        conn.commit()
        print(f"\nSUCCESS! Uploaded {uploaded} New Zealand price records to database")
        print(f"Source: interest.co.nz - Collected on March 12, 2026")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    upload_to_database()
