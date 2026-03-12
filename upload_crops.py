#!/usr/bin/env python3
"""
Upload Crop Prices to Railway PostgreSQL Database
Covers key feed and cash crops across AU, NZ, BR, PY, UY, AR, US
Useful for feed cost assumptions and TerraVerde integration
"""
import os
import psycopg
from datetime import datetime
from fx_rates import get_usd_rates, to_usd

# Crop price data - prices per tonne in local currency
# Sources: CBOT, ABARE, CEPEA, MAGyP, MPI NZ
crop_prices = [
    # Australia - AUD/tonne
    {"date": "2025-07-15", "country": "AU", "region": "National",
     "crop_type": "Wheat", "price_per_tonne_local": 365.00,
     "local_currency": "AUD", "delivery_period": "Spot", "data_source": "ABARE"},
    {"date": "2025-07-15", "country": "AU", "region": "National",
     "crop_type": "Barley", "price_per_tonne_local": 310.00,
     "local_currency": "AUD", "delivery_period": "Spot", "data_source": "ABARE"},
    {"date": "2025-07-15", "country": "AU", "region": "National",
     "crop_type": "Sorghum", "price_per_tonne_local": 340.00,
     "local_currency": "AUD", "delivery_period": "Spot", "data_source": "ABARE"},
    {"date": "2025-07-15", "country": "AU", "region": "National",
     "crop_type": "Cotton", "price_per_tonne_local": 620.00,
     "local_currency": "AUD", "delivery_period": "Spot", "data_source": "ABARE"},
    # New Zealand - NZD/tonne
    {"date": "2025-07-15", "country": "NZ", "region": "National",
     "crop_type": "Wheat", "price_per_tonne_local": 480.00,
     "local_currency": "NZD", "delivery_period": "Spot", "data_source": "MPI NZ"},
    {"date": "2025-07-15", "country": "NZ", "region": "National",
     "crop_type": "Barley", "price_per_tonne_local": 420.00,
     "local_currency": "NZD", "delivery_period": "Spot", "data_source": "MPI NZ"},
    # Brazil - BRL/tonne
    {"date": "2025-07-15", "country": "BR", "region": "Parana",
     "crop_type": "Soybeans", "price_per_tonne_local": 2450.00,
     "local_currency": "BRL", "delivery_period": "Spot", "data_source": "CEPEA"},
    {"date": "2025-07-15", "country": "BR", "region": "Mato Grosso",
     "crop_type": "Corn", "price_per_tonne_local": 1180.00,
     "local_currency": "BRL", "delivery_period": "Spot", "data_source": "CEPEA"},
    {"date": "2025-07-15", "country": "BR", "region": "Sao Paulo",
     "crop_type": "Sugarcane", "price_per_tonne_local": 165.00,
     "local_currency": "BRL", "delivery_period": "Spot", "data_source": "CEPEA"},
    {"date": "2025-07-15", "country": "BR", "region": "Mato Grosso",
     "crop_type": "Cotton", "price_per_tonne_local": 5200.00,
     "local_currency": "BRL", "delivery_period": "Spot", "data_source": "CEPEA"},
    # Paraguay - PYG/tonne
    {"date": "2025-07-15", "country": "PY", "region": "National",
     "crop_type": "Soybeans", "price_per_tonne_local": 2850000.00,
     "local_currency": "PYG", "delivery_period": "FOB", "data_source": "CAPECO"},
    {"date": "2025-07-15", "country": "PY", "region": "National",
     "crop_type": "Corn", "price_per_tonne_local": 1450000.00,
     "local_currency": "PYG", "delivery_period": "FOB", "data_source": "CAPECO"},
    {"date": "2025-07-15", "country": "PY", "region": "National",
     "crop_type": "Wheat", "price_per_tonne_local": 1750000.00,
     "local_currency": "PYG", "delivery_period": "Spot", "data_source": "CAPECO"},
    # Uruguay - UYU/tonne
    {"date": "2025-07-15", "country": "UY", "region": "National",
     "crop_type": "Soybeans", "price_per_tonne_local": 15800.00,
     "local_currency": "UYU", "delivery_period": "FOB", "data_source": "MGAP"},
    {"date": "2025-07-15", "country": "UY", "region": "National",
     "crop_type": "Wheat", "price_per_tonne_local": 9200.00,
     "local_currency": "UYU", "delivery_period": "Spot", "data_source": "MGAP"},
    {"date": "2025-07-15", "country": "UY", "region": "National",
     "crop_type": "Barley", "price_per_tonne_local": 8500.00,
     "local_currency": "UYU", "delivery_period": "Spot", "data_source": "MGAP"},
    # Argentina - ARS/tonne
    {"date": "2025-07-15", "country": "AR", "region": "Rosario",
     "crop_type": "Soybeans", "price_per_tonne_local": 380000.00,
     "local_currency": "ARS", "delivery_period": "FOB", "data_source": "Bolsa Rosario"},
    {"date": "2025-07-15", "country": "AR", "region": "Rosario",
     "crop_type": "Corn", "price_per_tonne_local": 195000.00,
     "local_currency": "ARS", "delivery_period": "FOB", "data_source": "Bolsa Rosario"},
    {"date": "2025-07-15", "country": "AR", "region": "Rosario",
     "crop_type": "Wheat", "price_per_tonne_local": 240000.00,
     "local_currency": "ARS", "delivery_period": "FOB", "data_source": "Bolsa Rosario"},
    # USA - USD/tonne (converted from bushels)
    {"date": "2025-07-15", "country": "US", "region": "National",
     "crop_type": "Corn", "price_per_tonne_local": 170.00,
     "local_currency": "USD", "delivery_period": "Spot", "data_source": "USDA/CBOT"},
    {"date": "2025-07-15", "country": "US", "region": "National",
     "crop_type": "Soybeans", "price_per_tonne_local": 385.00,
     "local_currency": "USD", "delivery_period": "Spot", "data_source": "USDA/CBOT"},
    {"date": "2025-07-15", "country": "US", "region": "National",
     "crop_type": "Wheat", "price_per_tonne_local": 210.00,
     "local_currency": "USD", "delivery_period": "Spot", "data_source": "USDA/CBOT"},
    {"date": "2025-07-15", "country": "US", "region": "National",
     "crop_type": "Sorghum", "price_per_tonne_local": 165.00,
     "local_currency": "USD", "delivery_period": "Spot", "data_source": "USDA/CBOT"},
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

        # Create crop_prices table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crop_prices (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                country VARCHAR(2) NOT NULL,
                region VARCHAR(100),
                crop_type VARCHAR(100) NOT NULL,
                price_per_tonne_local DECIMAL(12,2) NOT NULL,
                price_per_tonne_usd DECIMAL(12,2),
                local_currency VARCHAR(3) NOT NULL,
                delivery_period VARCHAR(50),
                data_source VARCHAR(100) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)

        print(f"CROPS: Uploading {len(crop_prices)} records...")
        uploaded = 0
        skipped = 0

        for r in crop_prices:
            cur.execute("""
                SELECT id FROM crop_prices
                WHERE country = %s AND timestamp::date = %s::date
                AND crop_type = %s AND region = %s
            """, (r['country'], r['date'], r['crop_type'], r['region']))

            if cur.fetchone():
                skipped += 1
                continue

            if r['local_currency'] == 'USD':
                usd_price = r['price_per_tonne_local']
            else:
                usd_price = to_usd(r['price_per_tonne_local'], r['local_currency'], rates)

            cur.execute("""
                INSERT INTO crop_prices
                (country, region, crop_type, price_per_tonne_local,
                 price_per_tonne_usd, local_currency, delivery_period,
                 data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                r['country'], r['region'], r['crop_type'],
                r['price_per_tonne_local'], usd_price,
                r['local_currency'], r['delivery_period'],
                r['data_source'], r['date']
            ))
            uploaded += 1
            print(f"  {r['country']} {r['crop_type']} | "
                  f"{r['local_currency']}{r['price_per_tonne_local']:.0f}/t = US${usd_price:.2f}/t")

        conn.commit()
        print(f"CROPS: {uploaded} uploaded, {skipped} skipped")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    upload_to_database()
