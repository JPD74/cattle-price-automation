#!/usr/bin/env python3
"""Cattle Price Automation - Australia MLA Data Collector"""

import os
import requests
import psycopg
from datetime import datetime
import time
import json

# Get database connection from Railway environment
DATABASE_URL = os.getenv('DATABASE_URL')

print("=" * 60)
print("🐄 Cattle Price Automation Service")
print("=" * 60)
print(f"Started at: {datetime.now()}")
print(f"Database: {DATABASE_URL[:40] if DATABASE_URL else 'NOT SET'}...")
print("=" * 60)

def setup_database():
    """Create tables if they don't exist"""
    print("\n[SETUP] Setting up database tables...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Read and execute setup SQL
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cattle_prices (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                country VARCHAR(2) NOT NULL,
                region VARCHAR(100),
                livestock_class VARCHAR(100) NOT NULL,
                weight_category VARCHAR(50),
                price_per_kg_local DECIMAL(10,2) NOT NULL,
                price_per_kg_usd DECIMAL(10,2) NOT NULL,
                local_currency VARCHAR(3) NOT NULL,
                data_source VARCHAR(100) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS collection_log (
                id SERIAL PRIMARY KEY,
                country VARCHAR(2) NOT NULL,
                data_source VARCHAR(100) NOT NULL,
                records_collected INTEGER DEFAULT 0,
                status VARCHAR(20) NOT NULL,
                error_message TEXT,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ [SETUP] Database tables ready!")
        return True
    except Exception as e:
        print(f"❌ [SETUP] Error: {e}")
        return False

def get_exchange_rate(currency='AUD'):
    """Get current USD exchange rate"""
    try:
        response = requests.get(f'https://api.exchangerate-api.com/v4/latest/{currency}', timeout=10)
        data = response.json()
        return data['rates']['USD']
    except:
        # Fallback rates
        rates = {'AUD': 0.65, 'NZD': 0.60, 'BRL': 0.20, 'PYG': 0.00013, 'UYU': 0.026}
        return rates.get(currency, 1.0)

def collect_test_data():
    """Collect test/demo cattle price data"""
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🇦🇺 Starting Australia data collection...")
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Get current exchange rate
        aud_to_usd = get_exchange_rate('AUD')
        print(f"   💱 Exchange rate: 1 AUD = {aud_to_usd:.4f} USD")
        
        # Demo data - representing typical Australian cattle prices
        demo_prices = [
            {'class': 'Heavy Steers', 'region': 'Queensland', 'price_aud': 4.50},
            {'class': 'Medium Steers', 'region': 'New South Wales', 'price_aud': 4.20},
            {'class': 'Heavy Cows', 'region': 'Victoria', 'price_aud': 3.80},
            {'class': 'Medium Cows', 'region': 'South Australia', 'price_aud': 3.50},
        ]
        
        records_count = 0
        for price_data in demo_prices:
            cur.execute("""
                INSERT INTO cattle_prices 
                (country, region, livestock_class, price_per_kg_local, 
                 price_per_kg_usd, local_currency, data_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                'AU',
                price_data['region'],
                price_data['class'],
                price_data['price_aud'],
                price_data['price_aud'] * aud_to_usd,
                'AUD',
                'Demo Data'
            ))
            records_count += 1
        
        # Log successful collection
        cur.execute("""
            INSERT INTO collection_log 
            (country, data_source, records_collected, status)
            VALUES (%s, %s, %s, %s)
        """, ('AU', 'Demo Data', records_count, 'success'))
        
        conn.commit()
        print(f"   ✅ Successfully collected {records_count} price records")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        try:
            cur.execute("""
                INSERT INTO collection_log 
                (country, data_source, status, error_message)
                VALUES (%s, %s, %s, %s)
            """, ('AU', 'Demo Data', 'failed', str(e)))
            conn.commit()
        except:
            pass
        return False

def main():
    """Main automation loop"""
    
    # Setup database first
    if not setup_database():
        print("❌ Failed to setup database. Exiting.")
        return
    
    print("\n🚀 Starting automated data collection loop...")
    print("   Collection runs every 5 minutes")
    print("   Press Ctrl+C to stop\n")
    
    iteration = 0
    while True:
        try:
            iteration += 1
            print(f"\n{'='*60}")
            print(f"Iteration #{iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")
            
            # Collect data
            collect_test_data()
            
            # Wait 5 minutes (300 seconds)
            print(f"\n⏰ Waiting 5 minutes until next collection...")
            print(f"   Next run at: {datetime.fromtimestamp(time.time() + 300).strftime('%H:%M:%S')}")
            time.sleep(300)
            
        except KeyboardInterrupt:
            print("\n\n🛑 Service stopped by user")
            print("="*60)
            break
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            print("   Waiting 1 minute before retry...")
            time.sleep(60)

if __name__ == "__main__":
    main()
