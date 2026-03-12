#!/usr/bin/env python3
"""
Upload New Zealand Cattle Prices to Railway PostgreSQL Database
Source: interest.co.nz / Silver Fern Farms / NZX
"""
import os
import psycopg
from datetime import datetime
from fx_rates import get_usd_rates, to_usd

newzealand_prices = [
    {"date": "2026-03-09", "country": "NZ", "region": "South Island",
     "livestock_class": "Steer Prime P2", "weight_category": "270-295kg",
     "price_per_kg_local": 8.65, "local_currency": "NZD", "data_source": "Silver Fern Farms"},
    {"date": "2026-03-09", "country": "NZ", "region": "South Island",
     "livestock_class": "Steer Prime P2", "weight_category": "220-245kg",
     "price_per_kg_local": 8.65, "local_currency": "NZD", "data_source": "Silver Fern Farms"},
    {"date": "2026-03-09", "country": "NZ", "region": "South Island",
     "livestock_class": "Steer Prime P2", "weight_category": "195-220kg",
     "price_per_kg_local": 8.35, "local_currency": "NZD", "data_source": "Silver Fern Farms"},
    {"date": "2026-03-09", "country": "NZ", "region": "North Island",
     "livestock_class": "Steer M Grade", "weight_category": "270-295kg",
     "price_per_kg_local": 8.15, "local_currency": "NZD", "data_source": "AFFCO"},
    {"date": "2026-03-09", "country": "NZ", "region": "North Island",
     "livestock_class": "Bull", "weight_category": ">300kg",
     "price_per_kg_local": 6.95, "local_currency": "NZD", "data_source": "AFFCO"},
    {"date": "2026-03-09", "country": "NZ", "region": "North Island",
     "livestock_class": "Cow Prime P2", "weight_category": "195-220kg",
     "price_per_kg_local": 6.50, "local_currency": "NZD", "data_source": "AFFCO"},
    {"date": "2026-03-09", "country": "NZ", "region": "South Island",
     "livestock_class": "Heifer Prime P2", "weight_category": "195-245kg",
     "price_per_kg_local": 8.25, "local_currency": "NZD", "data_source": "Silver Fern Farms"},
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
        print(f"NZ: Uploading {len(newzealand_prices)} records...")

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
            print(f"  {r['livestock_class']} ({r['weight_category']}) | "
                  f"NZ${r['price_per_kg_local']:.2f} = US${usd_price:.2f}/kg")

        conn.commit()
        print(f"NZ: {uploaded} uploaded, {skipped} skipped")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    upload_to_database()
