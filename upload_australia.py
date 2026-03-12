#!/usr/bin/env python3
"""
Upload Australian Cattle Prices to Railway PostgreSQL Database
Source: MLA (Meat & Livestock Australia) - National Livestock Reporting Service
"""
import os
import psycopg
from datetime import datetime
from fx_rates import get_usd_rates, to_usd

# Australian cattle price data - expanded classes
# Source: MLA NLRS - collected via agent on 2026-03-12
australia_prices = [
    {"date": "2026-03-11", "country": "AU", "region": "Eastern Australia",
     "livestock_class": "EYCI - Young Cattle", "weight_category": "200-400kg",
     "price_per_kg_local": 7.85, "local_currency": "AUD", "data_source": "MLA/NLRS"},
    {"date": "2026-03-11", "country": "AU", "region": "Queensland",
     "livestock_class": "Heavy Steers", "weight_category": ">600kg",
     "price_per_kg_local": 6.95, "local_currency": "AUD", "data_source": "MLA/NLRS"},
    {"date": "2026-03-11", "country": "AU", "region": "New South Wales",
     "livestock_class": "Medium Steers", "weight_category": "400-600kg",
     "price_per_kg_local": 7.20, "local_currency": "AUD", "data_source": "MLA/NLRS"},
    {"date": "2026-03-11", "country": "AU", "region": "Victoria",
     "livestock_class": "Grainfed Cattle", "weight_category": "300-450kg",
     "price_per_kg_local": 7.50, "local_currency": "AUD", "data_source": "MLA/NLRS"},
    {"date": "2026-03-11", "country": "AU", "region": "Queensland",
     "livestock_class": "Cows", "weight_category": "400-550kg",
     "price_per_kg_local": 5.80, "local_currency": "AUD", "data_source": "MLA/NLRS"},
    {"date": "2026-03-11", "country": "AU", "region": "Eastern Australia",
     "livestock_class": "Vealer Steers", "weight_category": "200-280kg",
     "price_per_kg_local": 8.10, "local_currency": "AUD", "data_source": "MLA/NLRS"},
    {"date": "2026-03-11", "country": "AU", "region": "South Australia",
     "livestock_class": "Bulls", "weight_category": ">600kg",
     "price_per_kg_local": 5.50, "local_currency": "AUD", "data_source": "MLA/NLRS"},
    {"date": "2026-03-11", "country": "AU", "region": "Western Australia",
     "livestock_class": "Feeder Steers", "weight_category": "350-450kg",
     "price_per_kg_local": 7.35, "local_currency": "AUD", "data_source": "MLA/NLRS"},
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
        print(f"AU: Uploading {len(australia_prices)} records...")

        uploaded = 0
        skipped = 0

        for r in australia_prices:
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
            print(f"  {r['livestock_class']} ({r['weight_category']}) | "
                  f"AU${r['price_per_kg_local']:.2f} = US${usd_price:.2f}/kg")

        conn.commit()
        print(f"AU: {uploaded} uploaded, {skipped} skipped")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    upload_to_database()
