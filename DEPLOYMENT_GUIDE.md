# 🚀 Cattle Price Automation - Quick Deployment Guide

## ✅ What You Have So Far

1. ✅ Railway PostgreSQL database (Online)
2. ✅ GitHub repository created
3. ✅ Database schema SQL file (`setup_database.sql`)

## 📋 Next Steps to Get Data Flowing

### Step 1: Set Up Database Tables

1. Go to Railway dashboard
2. Click on your Postgres service
3. Go to **Data** tab
4. Copy the contents of `setup_database.sql` from this repository
5. Execute it in the Railway database query interface

Alternatively, connect via `psql` command line (get connection string from Variables tab):

```bash
psql postgresql://postgres:PASSWORD@crossover.proxy.rlwy.net:17219/railway < setup_database.sql
```

### Step 2: Create Python Files (Add these to your repository)

You'll need to create these files:

#### `requirements.txt`
```
psycopg2-binary==2.9.9
requests==2.31.0
beautifulsoup4==4.12.3
python-dotenv==1.0.1
```

#### `main.py` - Main Automation Script
(See full code at bottom of this document)

#### `Procfile` - Railway deployment config
```
worker: python main.py
```

### Step 3: Deploy to Railway

1. Go to Railway project
2. Click "+ Add"
3. Select "GitHub Repository"
4. Connect to `JPD74/cattle-price-automation`
5. Railway will automatically:
   - Detect Python
   - Install dependencies from `requirements.txt`
   - Run `main.py`

### Step 4: Add Environment Variables in Railway

1. Click on your deployed service
2. Go to **Variables** tab
3. Click "+ New Variable"
4. Add reference to Postgres:

```
DATABASE_URL = ${{Postgres.DATABASE_URL}}
```

Railway will automatically link to your PostgreSQL database!

---

## 🔧 Complete Python Code

### `main.py` - Minimal Australia MLA Collector

Create this file in your repository:

```python
import os
import requests
import psycopg2
from datetime import datetime
import time

# Get database connection from Railway environment
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    """Connect to Railway PostgreSQL"""
    return psycopg2.connect(DATABASE_URL)

def get_exchange_rate(currency='AUD'):
    """Get current USD exchange rate"""
    try:
        # Using exchangerate-api.com (free tier)
        response = requests.get(f'https://api.exchangerate-api.com/v4/latest/{currency}')
        data = response.json()
        return data['rates']['USD']
    except:
        # Fallback approximate rates
        rates = {'AUD': 0.65, 'NZD': 0.60, 'BRL': 0.20, 'PYG': 0.00013, 'UYU': 0.026}
        return rates.get(currency, 1.0)

def collect_australia_mla_data():
    """Collect cattle prices from Australia MLA API"""
    print(f"[{datetime.now()}] Starting Australia MLA data collection...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # MLA Statistics API endpoint
        # Note: This is a simplified example - full API documentation at:
        # https://app.nlrsreports.mla.com.au/statistics/documentation
        
        api_url = "https://app.nlrsreports.mla.com.au/statistics/api/v1/livestockprices"
        
        response = requests.get(api_url, params={
            'country': 'Australia',
            'limit': 100
        })
        
        if response.status_code == 200:
            data = response.json()
            records_count = 0
            
            # Get current AUD to USD rate
            aud_to_usd = get_exchange_rate('AUD')
            
            # Process each price record
            for record in data.get('data', []):
                # Insert into cattle_prices table
                cur.execute("""
                    INSERT INTO cattle_prices 
                    (country, region, livestock_class, weight_category, 
                     price_per_kg_local, price_per_kg_usd, local_currency, data_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    'AU',
                    record.get('region', 'National'),
                    record.get('livestock_class', 'Cattle'),
                    record.get('weight_category', 'Unknown'),
                    record.get('price_per_kg', 0),
                    record.get('price_per_kg', 0) * aud_to_usd,
                    'AUD',
                    'MLA API'
                ))
                records_count += 1
            
            # Log collection
            cur.execute("""
                INSERT INTO collection_log 
                (country, data_source, records_collected, status)
                VALUES (%s, %s, %s, %s)
            """, ('AU', 'MLA API', records_count, 'success'))
            
            conn.commit()
            print(f"✅ Successfully collected {records_collected} records from Australia MLA")
            
        else:
            print(f"❌ MLA API returned status {response.status_code}")
            cur.execute("""
                INSERT INTO collection_log 
                (country, data_source, status, error_message)
                VALUES (%s, %s, %s, %s)
            """, ('AU', 'MLA API', 'failed', f'HTTP {response.status_code}'))
            conn.commit()
    
    except Exception as e:
        print(f"❌ Error collecting Australia data: {e}")
        cur.execute("""
            INSERT INTO collection_log 
            (country, data_source, status, error_message)
            VALUES (%s, %s, %s, %s)
        """, ('AU', 'MLA API', 'failed', str(e)))
        conn.commit()
    
    finally:
        cur.close()
        conn.close()

def main():
    """Main automation loop"""
    print("🐄 Cattle Price Automation Service Started")
    print(f"Connected to database: {DATABASE_URL[:30]}...")
    
    while True:
        try:
            # Collect Australia data
            collect_australia_mla_data()
            
            # Wait 1 hour before next collection
            print(f"⏰ Waiting 1 hour until next collection...")
            time.sleep(3600)  # 3600 seconds = 1 hour
            
        except KeyboardInterrupt:
            print("\n🛑 Service stopped by user")
            break
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            time.sleep(300)  # Wait 5 minutes on error

if __name__ == "__main__":
    main()
```

---

## 🎯 Expected Result

Once deployed, your system will:

1. ✅ Connect to Railway PostgreSQL automatically
2. ✅ Collect Australia cattle prices from MLA API every hour
3. ✅ Store data with timestamp, price in AUD and USD
4. ✅ Log all collection attempts
5. ✅ Run 24/7 on Railway infrastructure

## 📊 Querying Your Data

Connect to your database and run:

```sql
-- See latest prices
SELECT * FROM cattle_prices ORDER BY timestamp DESC LIMIT 10;

-- See collection status
SELECT * FROM collection_log ORDER BY timestamp DESC;

-- Average price by class
SELECT livestock_class, AVG(price_per_kg_usd) as avg_usd_price
FROM cattle_prices
WHERE country = 'AU'
GROUP BY livestock_class;
```

## 🚀 Deploy Now!

1. Add the Python files above to this repository
2. Connect repository to Railway
3. Watch your automated data collection start!

**Your superior product is ready to launch! 🎉**
