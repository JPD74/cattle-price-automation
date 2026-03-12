#!/usr/bin/env python3
"""
Upload Brazil CEPEA Cattle Prices to Railway PostgreSQL Database
Source: CEPEA/ESALQ - https://www.cepea.org.br/br/indicador/boi-gordo.aspx
"""
import os
import psycopg
from datetime import datetime
from fx_rates import get_usd_rates, to_usd

# Brazil cattle price data - expanded classes across regions
# Boi Gordo from CEPEA + regional market prices
brazil_prices = [
    {"date": "2026-03-11", "country": "BR", "region": "Sao Paulo",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 23.15, "local_currency": "BRL", "data_source": "CEPEA/ESALQ"},
    {"date": "2026-03-10", "country": "BR", "region": "Sao Paulo",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 23.12, "local_currency": "BRL", "data_source": "CEPEA/ESALQ"},
    {"date": "2026-03-09", "country": "BR", "region": "Sao Paulo",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 23.16, "local_currency": "BRL", "data_source": "CEPEA/ESALQ"},
    {"date": "2026-03-06", "country": "BR", "region": "Sao Paulo",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 23.07, "local_currency": "BRL", "data_source": "CEPEA/ESALQ"},
    {"date": "2026-03-05", "country": "BR", "region": "Sao Paulo",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 23.15, "local_currency": "BRL", "data_source": "CEPEA/ESALQ"},
    {"date": "2026-03-11", "country": "BR", "region": "Sao Paulo",
     "livestock_class": "Vaca Gorda (Fat Cow)", "weight_category": "350-450kg",
     "price_per_kg_local": 20.80, "local_currency": "BRL", "data_source": "CEPEA/ESALQ"},
    {"date": "2026-03-11", "country": "BR", "region": "Mato Grosso do Sul",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 22.40, "local_currency": "BRL", "data_source": "CEPEA/Regional"},
    {"date": "2026-03-11", "country": "BR", "region": "Goias",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 22.10, "local_currency": "BRL", "data_source": "CEPEA/Regional"},
    {"date": "2026-03-11", "country": "BR", "region": "Mato Grosso",
     "livestock_class": "Boi Magro (Lean Cattle)", "weight_category": "350-450kg",
     "price_per_kg_local": 19.50, "local_currency": "BRL", "data_source": "CEPEA/Regional"},
    {"date": "2026-03-11", "country": "BR", "region": "Sao Paulo",
     "livestock_class": "Bezerro (Calf)", "weight_category": "200-300kg",
     "price_per_kg_local": 25.30, "local_currency": "BRL", "data_source": "CEPEA/ESALQ"},
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
        print(f"BR: Uploading {len(brazil_prices)} records...")

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
            print(f"  {r['region']} | {r['livestock_class']} | "
                  f"R${r['price_per_kg_local']:.2f} = US${usd_price:.2f}/kg")

        conn.commit()
        print(f"BR: {uploaded} uploaded, {skipped} skipped")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    upload_to_database()
