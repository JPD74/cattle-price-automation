#!/usr/bin/env python3
"""Live Australia cattle price scraper - MLA Statistics API
Uses the MLA NLRS Livestock Indicators API (report/5).
Indicators: EYCI (0), Heavy Steer (4), Medium Cow (6), Feeder Steer (2), etc.
Source: app.nlrsreports.mla.com.au
"""
import os
import psycopg
import requests
from datetime import datetime, timedelta

INDICATORS = {
    0: {"class": "Eastern Young Cattle", "unit": "c/kg cwt"},
    2: {"class": "Feeder Steer", "unit": "c/kg lwt"},
    4: {"class": "Heavy Steer", "unit": "c/kg cwt"},
    6: {"class": "Medium Cow", "unit": "c/kg cwt"},
    8: {"class": "Restocker Yearling Steer", "unit": "c/kg lwt"},
    14: {"class": "National Trade Steer", "unit": "c/kg cwt"},
}

BASE_URL = "https://app.nlrsreports.mla.com.au"

def get_fx_rate():
    try:
        r = requests.get("https://open.er-api.com/v6/latest/AUD", timeout=15)
        data = r.json()
        return data["rates"]["USD"]
    except Exception as e:
        print(f"FX rate error: {e}")
        return 0.65

def fetch_indicator(indicator_id, from_date, to_date):
    url = f"{BASE_URL}/report/5"
    params = {"indicatorID": indicator_id, "fromDate": from_date, "toDate": to_date, "page": 1}
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 404:
            alt_url = f"{BASE_URL}/statistics/report/5"
            r = requests.get(alt_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("data", [])
    except Exception as e:
        print(f"  MLA API error for indicator {indicator_id}: {e}")
        return []

def scrape_mla_cattle():
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    records = []
    aud_to_usd = get_fx_rate()
    
    for ind_id, info in INDICATORS.items():
        print(f"  Fetching indicator {ind_id}: {info['class']}...")
        data = fetch_indicator(ind_id, from_date, to_date)
        for row in data:
            try:
                date_str = row.get("calendar_date", "")[:10]
                value = float(row.get("indicator_value", 0))
                unit = row.get("indicator_units", info["unit"])
                price_aud_per_kg = value / 100.0
                price_usd_per_kg = round(price_aud_per_kg * aud_to_usd, 4)
                records.append({
                    "date": date_str,
                    "livestock_class": info["class"],
                    "weight_category": unit,
                    "price_per_kg_local": round(price_aud_per_kg, 4),
                    "price_per_kg_usd": price_usd_per_kg,
                })
            except (ValueError, TypeError, KeyError) as e:
                print(f"    Parse error: {e}")
                continue
    return records

def upload_to_db(records):
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return
    try:
        conn = psycopg.connect(db_url)
        cur = conn.cursor()
        uploaded = 0
        skipped = 0
        for r in records:
            cur.execute("""
                SELECT 1 FROM cattle_prices 
                WHERE country = 'Australia' AND region = 'National'
                AND livestock_class = %s 
                AND timestamp = %s AND data_source = 'MLA_LIVE'
            """, (r["livestock_class"], r["date"]))
            if cur.fetchone():
                skipped += 1
                continue
            cur.execute("""
                INSERT INTO cattle_prices
                (country, region, livestock_class, weight_category,
                 price_per_kg_local, price_per_kg_usd, local_currency,
                 data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                "Australia", "National", r["livestock_class"],
                r["weight_category"],
                r["price_per_kg_local"], r["price_per_kg_usd"], "AUD",
                "MLA_LIVE", r["date"]
            ))
            uploaded += 1
        conn.commit()
        print(f"AUSTRALIA LIVE: {uploaded} uploaded, {skipped} skipped")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    print("=" * 50)
    print("AUSTRALIA LIVE SCRAPER - MLA Statistics API")
    print("=" * 50)
    records = scrape_mla_cattle()
    print(f"Scraped {len(records)} price records from MLA")
    if records:
        upload_to_db(records)
    else:
        print("No records - MLA API may be down or indicators unavailable")
