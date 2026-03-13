#!/usr/bin/env python3
"""
Live Argentina cattle price scraper - Mercado Agroganadero (MAG)
Scrapes daily cattle prices from the Mercado Agroganadero de Canuelas
(formerly Mercado de Liniers), Argentina's primary cattle auction market.
Source: https://www.mercadoagroganadero.com.ar
Prices are per kg live weight in ARS, converted to USD.
"""
import os
import re
import requests
import psycopg
from datetime import datetime, date
from bs4 import BeautifulSoup

# Category mapping: Spanish name -> English livestock class + weight category
CATEGORY_MAP = {
    "Novillos": ("Novillo", "400-500kg"),
    "Novillitos": ("Novillito", "300-400kg"),
    "Vaquillonas": ("Vaquillona", "300-400kg"),
    "Terneros": ("Ternero", "150-250kg"),
    "Terneras": ("Ternera", "150-250kg"),
    "Vacas": ("Vaca", "350-450kg"),
    "Toros": ("Toro", "500-600kg"),
}

def get_fx_rate():
    """Get ARS to USD exchange rate."""
    try:
        r = requests.get("https://open.er-api.com/v6/latest/ARS", timeout=15)
        data = r.json()
        return data["rates"]["USD"]
    except Exception as e:
        print(f"FX rate error: {e}")
        return None

def scrape_mag_prices():
    """Scrape Mercado Agroganadero daily price summary page."""
    url = "https://www.mercadoagroganadero.com.ar/dll/hacienda1.dll/haciinfo000002"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        r = requests.get(url, headers=headers, timeout=60)
        r.raise_for_status()
        r.encoding = "latin-1"
    except Exception as e:
        print(f"Failed to fetch MAG page: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    records = []

    # Find all tables with price data
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            text = cells[0].get_text(strip=True).upper()
            # Match known categories
            for cat_es, (cls_en, weight) in CATEGORY_MAP.items():
                if cat_es.upper() in text:
                    # Try to extract average price from the row
                    for cell in cells[1:]:
                        val = cell.get_text(strip=True)
                        val = val.replace(".", "").replace(",", ".")
                        try:
                            price = float(val)
                            if 100 < price < 50000:  # Reasonable ARS/kg range
                                records.append({
                                    "category": cls_en,
                                    "weight_category": weight,
                                    "price_ars_kg": price,
                                })
                                break
                        except (ValueError, TypeError):
                            continue
                    break

    # If table parsing fails, try regex fallback on full page text
    if not records:
        print("Table parsing found no records, trying regex fallback...")
        page_text = soup.get_text()
        for cat_es, (cls_en, weight) in CATEGORY_MAP.items():
            pattern = rf"{cat_es}[^\d]*(\d[\d.,]+)"
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                val = match.group(1).replace(".", "").replace(",", ".")
                try:
                    price = float(val)
                    if 100 < price < 50000:
                        records.append({
                            "category": cls_en,
                            "weight_category": weight,
                            "price_ars_kg": price,
                        })
                except (ValueError, TypeError):
                    pass

    return records

def upload_to_database(records, fx_rate):
    """Upload scraped prices to Railway PostgreSQL."""
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not found")
        return

    today = date.today().isoformat()
    conn = psycopg.connect(DATABASE_URL, sslmode="disable")
    cur = conn.cursor()
    inserted = 0

    for rec in records:
        price_usd = round(rec["price_ars_kg"] * fx_rate, 4)
        try:
            cur.execute("""
                INSERT INTO cattle_prices
                (timestamp, country, region, livestock_class, weight_category,
                 price_per_kg_local, price_per_kg_usd, local_currency, data_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                today, "AR", "Buenos Aires", rec["category"],
                rec["weight_category"], rec["price_ars_kg"], price_usd,
                "ARS", "Live Scrape - Mercado Agroganadero"
            ))
            inserted += 1
        except Exception as e:
            print(f"Insert error for {rec['category']}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Argentina: Inserted {inserted} records from Mercado Agroganadero")

def main():
    print("=== Argentina Live Cattle Price Scraper ===")
    print(f"Date: {date.today().isoformat()}")

    # Get FX rate
    fx_rate = get_fx_rate()
    if not fx_rate:
        print("FATAL: Could not get ARS/USD exchange rate")
        return
    print(f"ARS -> USD rate: {fx_rate}")

    # Scrape prices
    records = scrape_mag_prices()
    if not records:
        print("WARNING: No prices scraped from Mercado Agroganadero")
        print("Site may be down or format changed. Skipping upload.")
        return

    print(f"Scraped {len(records)} price records:")
    for rec in records:
        print(f"  {rec['category']} ({rec['weight_category']}): {rec['price_ars_kg']:.2f} ARS/kg")

    # Upload to database
    upload_to_database(records, fx_rate)
    print("=== Argentina scraper complete ===")

if __name__ == "__main__":
    main()
