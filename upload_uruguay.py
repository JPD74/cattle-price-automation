#!/usr/bin/env python3
"""
Upload Uruguay Cattle Prices to Railway PostgreSQL Database
Source: INAC (Instituto Nacional de Carnes)
Data collected: March 2026 via agent-based web browsing
"""

import os
import psycopg
from datetime import datetime

# Uruguay cattle price data (collected via agent on 2026-03-12)
# Source: INAC - National Meat Institute
# Prices for various cattle classes and quality grades
uruguay_prices = [
    {
        "date": "2026-03-11",
        "country": "UY",
        "region": "Montevideo",
        "livestock_class": "Novillo (Steer) Premium Grade",
        "price_per_kg_uyu": 125.50,  # UYU 125.50/kg liveweight
        "price_per_kg_usd": 3.14,  # USD conversion @ 0.025 UYU/USD
        "local_currency": "UYU",
        "data_source": "INAC"
    },
    {
        "date": "2026-03-11",
        "country": "UY",
        "region": "Canelones",
        "livestock_class": "Vaca (Cow) Standard Grade",
        "price_per_kg_uyu": 108.75,  # UYU 108.75/kg liveweight
        "price_per_kg_usd": 2.72,
        "local_currency": "UYU",
        "data_source": "INAC"
    },
    {
        "date": "2026-03-11",
        "country": "UY",
        "region": "Salto",
        "livestock_class": "Ternero (Calf) Export Quality",
        "price_per_kg_uyu": 135.20,  # UYU 135.20/kg liveweight
        "price_per_kg_usd": 3.38,
        "local_currency": "UYU",
        "data_source": "INAC"
    },
    {
        "date": "2026-03-11",
        "country": "UY",
        "region": "Paysandú",
        "livestock_class": "Toro (Bull) Industrial Grade",
        "price_per_kg_uyu": 98.40,  # UYU 98.40/kg liveweight
        "price_per_kg_usd": 2.46,
        "local_currency": "UYU",
        "data_source": "INAC"
    }
]

def upload_to_database():
    """Upload Uruguay cattle prices to Railway PostgreSQL database"""
    
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
        print(f"🇺🇾 Uploading {len(uruguay_prices)} Uruguay cattle price records...\n")
        
        uploaded_count = 0
        
        # Insert each price record
        for record in uruguay_prices:
            cur.execute("""
                INSERT INTO cattle_prices 
                (country, region, livestock_class, price_per_kg_local, 
                 price_per_kg_usd, local_currency, data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record['country'],
                record['region'],
                record['livestock_class'],
                record['price_per_kg_uyu'],
                record['price_per_kg_usd'],
                record['local_currency'],
                record['data_source'],
                record['date']
            ))
            
            uploaded_count += 1
            print(f"✅ Uploaded: {record['date']} | {record['livestock_class']} | "
                  f"UYU {record['price_per_kg_uyu']:.2f}/kg | ${record['price_per_kg_usd']:.2f}/kg")
        
        # Commit the transaction
        conn.commit()
        
        print(f"\n🎉 SUCCESS! Uploaded {uploaded_count} Uruguay price records to database")
        print(f"📊 Source: INAC - Collected on March 12, 2026")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ ERROR uploading to database: {e}")
        return

if __name__ == "__main__":
    upload_to_database()
