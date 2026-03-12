#!/usr/bin/env python3
"""
Upload New Zealand Cattle Prices to Railway PostgreSQL Database
Source: https://www.interest.co.nz/rural/beef/steer-p2
Data collected: March 2026 via agent-based web browsing
"""

import os
import psycopg
from datetime import datetime

# New Zealand cattle price data (collected via agent on 2026-03-12)
# Source: interest.co.nz - Silver Fern Farms processor schedules
# Steer Prime P2 grade, cartage paid prices
newzealand_prices = [
    {
        "date": "2026-03-09",
        "country": "NZ",
        "region": "South Island",
        "livestock_class": "Steer Prime P2 (270-295kg)",
        "price_per_kg_nzd": 8.65,  # NZ$ 865c/kg CWT
        "price_per_kg_usd": 5.19,  # USD conversion @ 0.60 NZD/USD
        "local_currency": "NZD",
        "data_source": "interest.co.nz/Silver Fern Farms"
    },
    {
        "date": "2026-03-09",
        "country": "NZ",
        "region": "South Island",
        "livestock_class": "Steer Prime P2 (220-245kg)",
        "price_per_kg_nzd": 8.65,  # NZ$ 865c/kg CWT
        "price_per_kg_usd": 5.19,
        "local_currency": "NZD",
        "data_source": "interest.co.nz/Silver Fern Farms"
    },
    {
        "date": "2026-03-09",
        "country": "NZ",
        "region": "South Island",
        "livestock_class": "Steer Prime P2 (195-220kg)",
        "price_per_kg_nzd": 8.35,  # NZ$ 835c/kg CWT
        "price_per_kg_usd": 5.01,
        "local_currency": "NZD",
        "data_source": "interest.co.nz/Silver Fern Farms"
    }
]

def upload_to_database():
    """Upload New Zealand cattle prices to Railway PostgreSQL database"""
    
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
        print(f"🇳🇿 Uploading {len(newzealand_prices)} New Zealand cattle price records...\n")
        
        uploaded_count = 0
        
        # Insert each price record
        for record in newzealand_prices:
            cur.execute("""
                INSERT INTO cattle_prices 
                (country, region, livestock_class, price_per_kg_local, 
                 price_per_kg_usd, local_currency, data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record['country'],
                record['region'],
                record['livestock_class'],
                record['price_per_kg_nzd'],
                record['price_per_kg_usd'],
                record['local_currency'],
                record['data_source'],
                record['date']
            ))
            
            uploaded_count += 1
            print(f"✅ Uploaded: {record['date']} | {record['livestock_class']} | "
                  f"NZ$ {record['price_per_kg_nzd']:.2f}/kg | ${record['price_per_kg_usd']:.2f}/kg")
        
        # Commit the transaction
        conn.commit()
        
        print(f"\n🎉 SUCCESS! Uploaded {uploaded_count} New Zealand price records to database")
        print(f"📊 Source: interest.co.nz - Collected on March 12, 2026")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ ERROR uploading to database: {e}")
        return

if __name__ == "__main__":
    upload_to_database()
