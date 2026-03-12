#!/usr/bin/env python3
"""
Upload Brazil CEPEA Cattle Prices to Railway PostgreSQL Database
Source: CEPEA/ESALQ + Noticias Agricolas regional market data
"""
import os
import psycopg
from datetime import datetime
from fx_rates import get_usd_rates, to_usd

# Brazil cattle price data - expanded regions and classes
# Prices converted from R$/arroba (15kg) to R$/kg
# Source: noticiasagricolas.com.br + CEPEA/ESALQ indicator
brazil_prices = [
    # Sao Paulo - CEPEA/ESALQ Indicator (Boi Gordo)
    {"date": "2026-03-11", "country": "BR", "region": "Sao Paulo",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 22.90, "local_currency": "BRL", "data_source": "CEPEA/ESALQ"},
    {"date": "2026-03-11", "country": "BR", "region": "Sao Paulo",
     "livestock_class": "Vaca Gorda (Fat Cow)", "weight_category": "350-450kg",
     "price_per_kg_local": 21.27, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    # Mato Grosso do Sul
    {"date": "2026-03-11", "country": "BR", "region": "Mato Grosso do Sul",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 22.11, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    {"date": "2026-03-11", "country": "BR", "region": "Mato Grosso do Sul",
     "livestock_class": "Vaca Gorda (Fat Cow)", "weight_category": "350-450kg",
     "price_per_kg_local": 19.80, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    # Mato Grosso
    {"date": "2026-03-11", "country": "BR", "region": "Mato Grosso",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 21.97, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    {"date": "2026-03-11", "country": "BR", "region": "Mato Grosso",
     "livestock_class": "Vaca Gorda (Fat Cow)", "weight_category": "350-450kg",
     "price_per_kg_local": 20.47, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    {"date": "2026-03-11", "country": "BR", "region": "Mato Grosso",
     "livestock_class": "Boi Magro (Lean Cattle)", "weight_category": "350-450kg",
     "price_per_kg_local": 19.50, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    # Goias
    {"date": "2026-03-11", "country": "BR", "region": "Goias",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 22.00, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    {"date": "2026-03-11", "country": "BR", "region": "Goias",
     "livestock_class": "Vaca Gorda (Fat Cow)", "weight_category": "350-450kg",
     "price_per_kg_local": 19.67, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    # Minas Gerais
    {"date": "2026-03-11", "country": "BR", "region": "Minas Gerais",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 21.78, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    # Parana
    {"date": "2026-03-11", "country": "BR", "region": "Parana",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 23.10, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    # Para (North - Amazon region)
    {"date": "2026-03-11", "country": "BR", "region": "Para",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 21.27, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    # Rondonia
    {"date": "2026-03-11", "country": "BR", "region": "Rondonia",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 20.13, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    # Bahia (Northeast)
    {"date": "2026-03-11", "country": "BR", "region": "Bahia",
     "livestock_class": "Boi Gordo (Fed Cattle)", "weight_category": ">450kg",
     "price_per_kg_local": 20.91, "local_currency": "BRL", "data_source": "Noticias Agricolas"},
    # Sao Paulo - Bezerro (Calf)
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
