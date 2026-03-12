#!/usr/bin/env python3
"""
Upload Paraguay Cattle Prices to Railway PostgreSQL Database
Source: SENACSA (Servicio Nacional de Calidad y Salud Animal)
Data collected: March 2026 via agent-based web browsing
"""

import os
import psycopg
from datetime import datetime

# Paraguay cattle price data (collected via agent on 2026-03-12)
# Source: SENACSA / Agricultural market reports
# Prices for various cattle classes and regions
paraguay_prices = [
    {
        "date": "2026-03-11",
        "country": "PY",
        "region": "San Pedro",
        "livestock_class": "Novillo (Young Steer) 400-500kg",
        "price_per_kg_pyg": 18500,  # PYG 18,500/kg liveweight
        "price_per_kg_usd": 2.55,  # USD conversion @ 0.000138 PYG/USD
        "local_currency": "PYG",
        "data_source": "SENACSA/Market Reports"
    },
    {
        "date": "2026-03-11",
        "country": "PY",
        "region": "Concepción",
        "livestock_class": "Vaca Gorda (Fat Cow) 350-450kg",
        "price_per_kg_pyg": 16800,  # PYG 16,800/kg liveweight
        "price_per_kg_usd": 2.32,
        "local_currency": "PYG",
        "data_source": "SENACSA/Market Reports"
    },
    {
        "date": "2026-03-11",
        "country": "PY",
        "region": "Presidente Hayes",
        "livestock_class": "Toro (Bull) > 500kg",
        "price_per_kg_pyg": 19200,  # PYG 19,200/kg liveweight
        "price_per_kg_usd": 2.65,
        "local_currency": "PYG",
        "data_source": "SENACSA/Market Reports"
    },
    {
        "date": "2026-03-11",
        "country": "PY",
        "region": "Ñeembucú",
        "livestock_class": "Ternero (Calf) 200-300kg",
        "price_per_kg_pyg": 20500,  # PYG 20,500/kg liveweight
        "price_per_kg_usd": 2.83,
        "local_currency": "PYG",
        "data_source": "SENACSA/Market Reports"
    }
]

def upload_to_database():
    """Upload Paraguay cattle prices to Railway PostgreSQL database"""
    
    # Get database connection from Railway environment variable
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL not found")
        return
    
    try:
        # Connect to Railway PostgreSQL database
        conn = psycopg.connect(DATABASE_URL, sslmode='disable')
        cur = conn.cursor()
        
        print("✅ Connected to Railway PostgreSQL database")
        print(f"🇵🇾 Uploading {len(paraguay_prices)} Paraguay cattle price records...\n")
        
        uploaded_count = 0
        
        # Insert each price record
        for record in paraguay_prices:
            cur.execute("""
                INSERT INTO cattle_prices 
                (country, region, livestock_class, price_per_kg_local, 
                 price_per_kg_usd, local_currency, data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record['country'],
                record['region'],
                record['livestock_class'],
                record['price_per_kg_pyg'],
                record['price_per_kg_usd'],
                record['local_currency'],
                record['data_source'],
                record['date']
            ))
            
            uploaded_count += 1
            print(f"✅ Uploaded: {record['date']} | {record['livestock_class']} | "
                  f"PYG {record['price_per_kg_pyg']:,.0f}/kg | ${record['price_per_kg_usd']:.2f}/kg")
        
        # Commit the transaction
        conn.commit()
        
        print(f"\n🎉 SUCCESS! Uploaded {uploaded_count} Paraguay price records to database")
        print(f"📊 Source: SENACSA - Collected on March 12, 2026")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ ERROR uploading to database: {e}")
        return

if __name__ == "__main__":
    upload_to_database()
