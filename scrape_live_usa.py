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

        if not isinstance(data, dict) or not data.get("results"):
            print("  No results returned")
            return prices

        # Get latest report
        latest = data["results"][0]
        report_date = latest.get("report_date", today)
        head_count = latest.get("previous_week_head_count", "N/A")
        print(f"  Report date: {report_date}, Head count: {head_count}")

        # Query the Detail section for per-class pricing
        detail_url = f"{USDA_API_BASE}/2477/Detail?lastDays=7"
        detail_resp = requests.get(detail_url, timeout=60)
        detail_resp.raise_for_status()
        detail_data = detail_resp.json()

        if isinstance(detail_data, dict) and detail_data.get("results"):
            # Group by class + selling_basis to get unique records for latest report
            seen = set()
            for record in detail_data["results"]:
                try:
                    rec_date = record.get("report_date", report_date)
                    # Only process latest report date
                    if rec_date != report_date:
                        continue

                    class_desc = record.get("class_description", "")
                    selling_basis = record.get("selling_basis_description", "")
                    grade = record.get("grade_description", "")
                    price_low = record.get("price_range_low")
                    price_high = record.get("price_range_high")
                    weighted_avg = record.get("weighted_avg_price")
                    weight_avg = record.get("weight_range_avg")
                    head = record.get("head_count", "0")

                    # Use weighted average if available, otherwise calculate from range
                    if weighted_avg:
                        avg_price_cwt = float(str(weighted_avg).replace(',', ''))
                    elif price_low and price_high:
                        avg_price_cwt = (float(str(price_low).replace(',', '')) + float(str(price_high).replace(',', ''))) / 2
                    else:
                        continue

                    avg_price_kg = cwt_to_per_kg(avg_price_cwt)

                    # Determine livestock class
                    if "STEER" in class_desc.upper():
                        if "HEIFER" in class_desc.upper():
                            livestock_class = "Choice Steers (Fed Cattle)"
                        else:
                            livestock_class = "Choice Steers (Fed Cattle)"
                    elif "HEIFER" in class_desc.upper():
                        livestock_class = "Bred Heifers"
                    else:
                        livestock_class = class_desc or "Fed Cattle"

                    # Add grade info to class name
                    if grade and "Choice" in grade:
                        if "STEER" in class_desc.upper():
                            livestock_class = "Choice Steers (Fed Cattle)"
                        elif "HEIFER" in class_desc.upper():
                            livestock_class = "Bred Heifers"
                    elif grade and "Select" in grade:
                        if "STEER" in class_desc.upper():
                            livestock_class = "Fed Cattle"

                    # Determine weight category
                    if weight_avg:
                        wt_lbs = float(str(weight_avg).replace(',', ''))
                        wt_kg = round(wt_lbs / 2.20462)
                        weight_cat = f"{wt_kg-50}-{wt_kg+50}kg"
                    else:
                        weight_cat = "500-635kg"

                    # Build region from selling basis
                    region = "5-Area"
                    if selling_basis:
                        region = f"5-Area"

                    # Dedup key
                    key = (rec_date, livestock_class, grade, selling_basis)
                    if key in seen:
                        continue
                    seen.add(key)

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

    # Also try feeder cattle - slug 2487 (Feeder Cattle Weighted Avg)
    try:
        feeder_url = f"{USDA_API_BASE}/2487"
        resp = requests.get(feeder_url, timeout=30)
        if resp.status_code == 200:
            fdata = resp.json()
            if isinstance(fdata, dict) and fdata.get("results"):
                latest_f = fdata["results"][0]
                if isinstance(latest_f, dict):
                    print(f"  Feeder report date: {latest_f.get('report_date', 'N/A')}")
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
