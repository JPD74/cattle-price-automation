#!/usr/bin/env python3
"""
Upload USA Cattle Prices to Railway PostgreSQL Database
Source: USDA Market News / CME Group
Prices converted from $/cwt (hundredweight) to $/kg
"""
import os
import psycopg
from datetime import datetime
from fx_rates import get_usd_rates, to_usd

# US cattle price data - converted from $/cwt to $/kg
# 1 cwt = 100 lbs = 45.3592 kg
# Source: USDA AMS Market News - collected via agent
CWT_TO_KG = 100 / 2.20462  # ~45.36 kg per cwt

def cwt_to_per_kg(price_per_cwt):
    """Convert USD/cwt to USD/kg"""
    return round(price_per_cwt / CWT_TO_KG, 2)

usa_prices = [
    {"date": "2025-07-15", "country": "US", "region": "5-Area",
     "livestock_class": "Fed Cattle", "weight_category": "500-635kg",
     "price_per_kg_local": cwt_to_per_kg(215.00), "local_currency": "USD", "data_source": "USDA/AMS"},
    {"date": "2025-07-15", "country": "US", "region": "Oklahoma City",
     "livestock_class": "Feeder Cattle", "weight_category": "320-410kg",
     "price_per_kg_local": cwt_to_per_kg(281.00), "local_currency": "USD", "data_source": "USDA/AMS"},
    {"date": "2025-07-15", "country": "US", "region": "Oklahoma City",
     "livestock_class": "Feeder Calves", "weight_category": "200-275kg",
     "price_per_kg_local": cwt_to_per_kg(347.00), "local_currency": "USD", "data_source": "USDA/AMS"},
    {"date": "2025-07-15", "country": "US", "region": "National",
     "livestock_class": "Cull Cows", "weight_category": "400-600kg",
     "price_per_kg_local": cwt_to_per_kg(155.00), "local_currency": "USD", "data_source": "USDA/AMS"},
    {"date": "2025-07-15", "country": "US", "region": "National",
     "livestock_class": "Bred Heifers", "weight_category": "400-500kg",
     "price_per_kg_local": cwt_to_per_kg(290.00), "local_currency": "USD", "data_source": "USDA/AMS"},
    {"date": "2025-07-15", "country": "US", "region": "Texas",
     "livestock_class": "Fed Cattle", "weight_category": "500-635kg",
     "price_per_kg_local": cwt_to_per_kg(213.50), "local_currency": "USD", "data_source": "USDA/AMS"},
    {"date": "2025-07-15", "country": "US", "region": "Kansas",
     "livestock_class": "Fed Cattle", "weight_category": "500-635kg",
     "price_per_kg_local": cwt_to_per_kg(216.00), "local_currency": "USD", "data_source": "USDA/AMS"},
    {"date": "2025-07-15", "country": "US", "region": "Nebraska",
     "livestock_class": "Fed Cattle", "weight_category": "500-635kg",
     "price_per_kg_local": cwt_to_per_kg(218.00), "local_currency": "USD", "data_source": "USDA/AMS"},
    {"date": "2025-07-15", "country": "US", "region": "Iowa",
     "livestock_class": "Feeder Cattle", "weight_category": "320-410kg",
     "price_per_kg_local": cwt_to_per_kg(278.00), "local_currency": "USD", "data_source": "USDA/AMS"},
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
        print(f"US: Uploading {len(usa_prices)} records...")
        uploaded = 0
        skipped = 0

        for r in usa_prices:
            cur.execute("""
                SELECT id FROM cattle_prices
                WHERE country = %s AND timestamp::date = %s::date
                AND livestock_class = %s AND region = %s
            """, (r['country'], r['date'], r['livestock_class'], r['region']))

            if cur.fetchone():
                skipped += 1
                continue

            # For USD, price_per_kg_usd = price_per_kg_local
            usd_price = r['price_per_kg_local']

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
                  f"US${r['price_per_kg_local']:.2f}/kg")

        conn.commit()
        print(f"US: {uploaded} uploaded, {skipped} skipped")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    upload_to_database()
