#!/usr/bin/env python3
"""
Upload Real Brazil CEPEA Cattle Prices to Railway PostgreSQL Database

This script uploads the cattle price data collected from CEPEA on March 12, 2026
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
        "region": "São Paulo",
        "livestock_class": "Boi Gordo (Fed Cattle)",
        "price_per_kg_brl": 23.15,  # R$ 347.25/arroba ÷ 15
        "price_per_kg_usd": 4.48,   # $67.24/arroba ÷ 15
        "local_currency": "BRL",
        "data_source": "CEPEA/ESALQ"
    },
    {
        "date": "2026-03-10",
        "country": "BR",
        "region": "São Paulo",
        "livestock_class": "Boi Gordo (Fed Cattle)",
        "price_per_kg_brl": 23.12,  # R$ 346.80/arroba ÷ 15
        "price_per_kg_usd": 4.48,   # $67.20/arroba ÷ 15
        "local_currency": "BRL",
        "data_source": "CEPEA/ESALQ"
    },
    {
        "date": "2026-03-09",
        "country": "BR",
        "region": "São Paulo",
        "livestock_class": "Boi Gordo (Fed Cattle)",
        "price_per_kg_brl": 23.16,  # R$ 347.40/arroba ÷ 15
        "price_per_kg_usd": 4.48,   # $67.17/arroba ÷ 15
        "local_currency": "BRL",
        "data_source": "CEPEA/ESALQ"
    },
    {
        "date": "2026-03-06",
        "country": "BR",
        "region": "São Paulo",
        "livestock_class": "Boi Gordo (Fed Cattle)",
        "price_per_kg_brl": 23.07,  # R$ 346.05/arroba ÷ 15
        "price_per_kg_usd": 4.39,   # $65.91/arroba ÷ 15
        "local_currency": "BRL",
        "data_source": "CEPEA/ESALQ"
    },
    {
        "date": "2026-03-05",
        "country": "BR",
        "region": "São Paulo",
        "livestock_class": "Boi Gordo (Fed Cattle)",
        "price_per_kg_brl": 23.15,  # R$ 347.30/arroba ÷ 15
        "price_per_kg_usd": 4.38,   # $65.75/arroba ÷ 15
        "local_currency": "BRL",
        "data_source": "CEPEA/ESALQ"
    }
]

def upload_to_database():
    """Upload Brazil CEPEA cattle prices to Railway PostgreSQL database"""
    
    # Get database connection from Railway environment variable
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL not found")
        return
    
    try:
        # Connect to Railway PostgreSQL database
        conn = psycopg.connect(DATABASE_URL)
        cur = conn.cursor()
        
        print("✅ Connected to Railway PostgreSQL database")
        print(f"🇧🇷 Uploading {len(brazil_prices)} Brazil CEPEA cattle price records...\n")
        
        uploaded_count = 0
        
        # Insert each price record
        for record in brazil_prices:
            cur.execute("""
                INSERT INTO cattle_prices 
                (country, region, livestock_class, price_per_kg_local, 
                 price_per_kg_usd, local_currency, data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record['country'],
                record['region'],
                record['livestock_class'],
                record['price_per_kg_brl'],
                record['price_per_kg_usd'],
                record['local_currency'],
                record['data_source'],
                record['date']  # Use the actual date from CEPEA
            ))
            
            uploaded_count += 1
            print(f"✅ Uploaded: {record['date']} | {record['livestock_class']} | "
                  f"R$ {record['price_per_kg_brl']:.2f}/kg | ${record['price_per_kg_usd']:.2f}/kg")
        
        # Commit the transaction
        conn.commit()
        
        print(f"\n🎉 SUCCESS! Uploaded {uploaded_count} Brazil CEPEA price records to database")
        print(f"📊 Source: CEPEA/ESALQ - Collected on March 12, 2026")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ ERROR uploading to database: {e}")
        return

if __name__ == "__main__":
    upload_to_database()
