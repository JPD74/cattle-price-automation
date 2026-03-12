#!/usr/bin/env python3
"""
Upload Australian Cattle Prices to Railway PostgreSQL Database
Source: MLA (Meat & Livestock Australia) - National Livestock Reporting Service
Data collected: March 2026 via agent-based web browsing
"""

import os
import psycopg
from datetime import datetime

# Australian cattle price data (collected via agent on 2026-03-12)
# Source: MLA NLRS - Eastern Young Cattle Indicator
# Prices for various cattle classes across regions
australia_prices = [
    {
        "date": "2026-03-11",
        "country": "AU",
        "region": "Eastern Australia",
        "livestock_class": "EYCI - Young Cattle (200-400kg)",
        "price_per_kg_aud": 7.85,  # AUD ¢785/kg dressed weight
        "price_per_kg_usd": 5.10,  # USD conversion @ 0.65 AUD/USD
        "local_currency": "AUD",
        "data_source": "MLA/NLRS"
    },
    {
        "date": "2026-03-11",
        "country": "AU",
        "region": "Queensland",
        "livestock_class": "Heavy Steers (> 600kg)",
        "price_per_kg_aud": 6.95,  # AUD ¢695/kg liveweight
        "price_per_kg_usd": 4.52,
        "local_currency": "AUD",
        "data_source": "MLA/NLRS"
    },
    {
        "date": "2026-03-11",
        "country": "AU",
        "region": "New South Wales",
        "livestock_class": "Medium Steers (400-600kg)",
        "price_per_kg_aud": 7.20,  # AUD ¢720/kg liveweight
        "price_per_kg_usd": 4.68,
        "local_currency": "AUD",
        "data_source": "MLA/NLRS"
    },
    {
        "date": "2026-03-11",
        "country": "AU",
        "region": "Victoria",
        "livestock_class": "Grainfed Cattle (300-450kg)",
        "price_per_kg_aud": 7.50,  # AUD ¢750/kg liveweight
        "price_per_kg_usd": 4.88,
        "local_currency": "AUD",
        "data_source": "MLA/NLRS"
    }
]

def upload_to_database():
    """Upload Australian cattle prices to Railway PostgreSQL database"""
    
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
        print(f"🇦🇺 Uploading {len(australia_prices)} Australian cattle price records...\n")
        
        uploaded_count = 0
        
        # Insert each price record
        for record in australia_prices:
            cur.execute("""
                INSERT INTO cattle_prices 
                (country, region, livestock_class, price_per_kg_local, 
                 price_per_kg_usd, local_currency, data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record['country'],
                record['region'],
                record['livestock_class'],
                record['price_per_kg_aud'],
                record['price_per_kg_usd'],
                record['local_currency'],
                record['data_source'],
                record['date']
            ))
            
            uploaded_count += 1
            print(f"✅ Uploaded: {record['date']} | {record['livestock_class']} | "
                  f"AU$ {record['price_per_kg_aud']:.2f}/kg | ${record['price_per_kg_usd']:.2f}/kg")
        
        # Commit the transaction
        conn.commit()
        
        print(f"\n🎉 SUCCESS! Uploaded {uploaded_count} Australian price records to database")
        print(f"📊 Source: MLA/NLRS - Collected on March 12, 2026")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ ERROR uploading to database: {e}")
        return

if __name__ == "__main__":
    upload_to_database()
