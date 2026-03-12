#!/usr/bin/env python3
"""
Live USA Cattle Price Scraper
Source: USDA LMR Datamart API (public, no auth required)
Slug 2477 = 5-Area Weekly Weighted Average Direct Slaughter Cattle (LM_CT150)
Converts from $/cwt to $/kg for standardized database storage
"""
import os
import json
import requests
import psycopg
from datetime import datetime
from fx_rates import get_usd_rates

USDA_API_BASE = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports"
CWT_TO_KG = 100 / 2.20462  # ~45.36 kg per cwt

def cwt_to_per_kg(price_per_cwt):
    return round(price_per_cwt / CWT_TO_KG, 2)

def fetch_usda_cattle():
    """Fetch latest 5-Area slaughter cattle data from USDA LMR API."""
    prices = []
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Slug 2477 = 5-Area Weekly Weighted Average Direct Slaughter Cattle
    url = f"{USDA_API_BASE}/2477"
    print(f"US LIVE: Fetching from {url}")
    
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        if not data.get("results"):
            print("  No results returned")
            return prices
        
        # Get latest report
        latest = data["results"][0]
        report_date = latest.get("report_date", today)
        head_count = latest.get("previous_week_head_count", "N/A")
        print(f"  Report date: {report_date}, Head count: {head_count}")
        
        # The summary gives us the 5-area weighted avg
        # We parse the main price from the report metadata
        # For detailed prices, we query the Detail section
        detail_url = f"{USDA_API_BASE}/2477/Detail?lastDays=7"
        detail_resp = requests.get(detail_url, timeout=60)
        detail_resp.raise_for_status()
        detail_data = detail_resp.json()
        
        if detail_data.get("results"):
            for record in detail_data["results"][:20]:  # Limit to recent
                try:
                    purchase_type = record.get("purchase_type", "")
                    class_name = record.get("class", "")
                    price_low = record.get("price_low")
                    price_high = record.get("price_high")
                    avg_weight = record.get("avg_weight")
                    head = record.get("head_count", 0)
                    rec_date = record.get("report_date", report_date)
                    
                    if not price_low or not price_high:
                        continue
                    
                    avg_price_cwt = (float(price_low) + float(price_high)) / 2
                    avg_price_kg = cwt_to_per_kg(avg_price_cwt)
                    
                    # Determine livestock class
                    if "STEER" in class_name.upper():
                        livestock_class = "Fed Cattle"
                    elif "HEIFER" in class_name.upper():
                        livestock_class = "Fed Cattle"
                    else:
                        livestock_class = class_name or "Fed Cattle"
                    
                    # Determine weight category from avg_weight
                    if avg_weight:
                        wt_kg = round(float(avg_weight) / 2.20462)
                        weight_cat = f"{wt_kg-50}-{wt_kg+50}kg"
                    else:
                        weight_cat = "500-635kg"
                    
                    region = "5-Area"
                    if purchase_type:
                        region = f"5-Area ({purchase_type})"
                    
                    prices.append({
                        "date": rec_date,
                        "country": "US",
                        "region": region,
                        "livestock_class": livestock_class,
                        "weight_category": weight_cat,
                        "price_per_kg_local": avg_price_kg,
                        "local_currency": "USD",
                        "data_source": "USDA/LMR-CT150"
                    })
                except (ValueError, TypeError, KeyError) as e:
                    continue
        
        print(f"  Parsed {len(prices)} price records")
    
    except Exception as e:
        print(f"  ERROR fetching USDA data: {e}")
    
    # Also fetch feeder cattle from CME/USDA feeder report
    # Slug 2490 = National Feeder & Stocker Cattle Summary
    try:
        feeder_url = f"{USDA_API_BASE}/2490"
        resp = requests.get(feeder_url, timeout=30)
        if resp.status_code == 200:
            fdata = resp.json()
            if fdata.get("results"):
                latest_f = fdata["results"][0]
                print(f"  Feeder report date: {latest_f.get('report_date')}")
    except Exception as e:
        print(f"  Feeder data not available: {e}")
    
    return prices


def upload_to_database():
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not found")
        return
    
    prices = fetch_usda_cattle()
    if not prices:
        print("US LIVE: No prices fetched, using fallback static data")
        # Import fallback
        from upload_usa import usa_prices as prices
        from upload_usa import upload_to_database as fallback_upload
        fallback_upload()
        return
    
    rates = get_usd_rates()
    
    try:
        conn = psycopg.connect(DATABASE_URL, sslmode='disable')
        cur = conn.cursor()
        print(f"US LIVE: Uploading {len(prices)} records...")
        uploaded = 0
        skipped = 0
        
        for r in prices:
            cur.execute("""
                SELECT id FROM cattle_prices
                WHERE country = %s AND timestamp::date = %s::date
                AND livestock_class = %s AND region = %s
            """, (r['country'], r['date'], r['livestock_class'], r['region']))
            
            if cur.fetchone():
                skipped += 1
                continue
            
            usd_price = r['price_per_kg_local']
            
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
            print(f"  {r['livestock_class']} ({r['region']}) | US${usd_price:.2f}/kg")
        
        conn.commit()
        print(f"US LIVE: {uploaded} uploaded, {skipped} skipped")
        cur.close()
        conn.close()
    
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    upload_to_database()
