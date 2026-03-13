#!/usr/bin/env python3
"""
Live Argentina cattle price scraper - Mercado Agroganadero (MAG)
Scrapes from MAG homepage which has latest remate prices embedded.
Source: https://www.mercadoagroganadero.com.ar
Prices are per kg live weight in ARS, converted to USD.
"""
import os
import re
import requests
import psycopg
from datetime import datetime, date
from bs4 import BeautifulSoup

CATEGORY_MAP = {
    "NOVILLOS": ("Novillo", "400-500kg"),
    "NOVILLITOS": ("Novillito", "300-400kg"),
    "VAQUILLONAS": ("Vaquillona", "300-400kg"),
    "TERNEROS": ("Ternero", "150-250kg"),
    "TERNERAS": ("Ternera", "150-250kg"),
    "VACAS": ("Vaca", "350-450kg"),
    "TOROS": ("Toro", "500-600kg"),
    "MEJ": ("MEJ", "Mixed"),
}

def get_fx_rate():
    try:
        r = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=15)
        rate = r.json()["rates"]["ARS"]
        usd_per_ars = 1.0 / rate
        print(f"ARS -> USD rate: {usd_per_ars:.6f}")
        return usd_per_ars
    except Exception as e:
        print(f"FX rate error: {e}")
        return None

def scrape_mag_homepage():
    print("=== Argentina Live Cattle Price Scraper ===")
    today = date.today()
    print(f"Date: {today}")

    fx = get_fx_rate()
    if not fx:
        print("ERROR: Could not get FX rate. Aborting.")
        return []

    url = "https://www.mercadoagroganadero.com.ar/dll/inicio.dll"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    try:
        r = requests.get(url, headers=headers, timeout=60)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"Error fetching MAG homepage: {e}")
        return []

    prices = []
    tables = soup.find_all("table")
    print(f"Found {len(tables)} tables on homepage")

    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            cat_text = cells[0].get_text(strip=True).upper()
            if not cat_text:
                continue

            try:
                min_text = cells[1].get_text(strip=True).replace(".", "").replace(",", ".")
                max_text = cells[2].get_text(strip=True).replace(".", "").replace(",", ".")
                min_price = float(min_text)
                max_price = float(max_text)
                avg_price = (min_price + max_price) / 2.0
            except (ValueError, IndexError):
                continue

            if avg_price <= 0:
                continue

            matched_key = None
            for key in CATEGORY_MAP:
                if cat_text.startswith(key):
                    matched_key = key
                    break

            if not matched_key:
                continue

            eng_name, weight_cat = CATEGORY_MAP[matched_key]
            sub_cat = cat_text.replace(matched_key, "").strip()
            if sub_cat:
                full_name = f"{eng_name} ({sub_cat})"
            else:
                full_name = eng_name

            price_usd = round(avg_price * fx, 4)

            prices.append({
                "timestamp": today.isoformat(),
                "country": "AR",
                "region": "Buenos Aires",
                "livestock_class": full_name,
                "weight_category": weight_cat,
                "price_per_kg_local": round(avg_price, 4),
                "price_per_kg_usd": price_usd,
                "local_currency": "ARS",
                "data_source": "MAG_LIVE",
            })

    if not prices:
        print("No table data found, trying regex fallback...")
        html = r.text
        pattern = r'(NOVILLOS|NOVILLITOS|VAQUILLONAS|TERNEROS|TERNERAS|VACAS|TOROS)[^<]*?([\d.]+)[^<]*?([\d.]+)'
        matches = re.findall(pattern, html, re.IGNORECASE)
        for cat, min_p, max_p in matches:
            try:
                min_val = float(min_p.replace(".", ""))
                max_val = float(max_p.replace(".", ""))
                if min_val < 100 or max_val < 100:
                    continue
                avg = (min_val + max_val) / 2.0
                cat_upper = cat.upper()
                if cat_upper in CATEGORY_MAP:
                    eng_name, weight_cat = CATEGORY_MAP[cat_upper]
                    price_usd = round(avg * fx, 4)
                    prices.append({
                        "timestamp": today.isoformat(),
                        "country": "AR",
                        "region": "Buenos Aires",
                        "livestock_class": eng_name,
                        "weight_category": weight_cat,
                        "price_per_kg_local": round(avg, 4),
                        "price_per_kg_usd": price_usd,
                        "local_currency": "ARS",
                        "data_source": "MAG_LIVE",
                    })
            except ValueError:
                continue

    print(f"Found {len(prices)} price records")
    return prices

def upload_to_database(prices):
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return

    conn = psycopg.connect(db_url)
    cur = conn.cursor()
    inserted = 0
    skipped = 0

    for p in prices:
        try:
            # Check if already exists
            cur.execute("""
                SELECT 1 FROM cattle_prices
                WHERE country = 'AR'
                AND livestock_class = %s
                AND timestamp = %s
                AND data_source = 'MAG_LIVE'
            """, (p["livestock_class"], p["timestamp"]))

            if cur.fetchone():
                skipped += 1
                continue

            cur.execute("""
                INSERT INTO cattle_prices (country, region, livestock_class, weight_category,
                    price_per_kg_local, price_per_kg_usd, local_currency, data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                p["country"], p["region"], p["livestock_class"], p["weight_category"],
                p["price_per_kg_local"], p["price_per_kg_usd"], p["local_currency"],
                p["data_source"], p["timestamp"]
            ))
            inserted += 1
        except Exception as e:
            print(f"DB error for {p['livestock_class']}: {e}")
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()
    print(f"ARGENTINA LIVE: {inserted} uploaded, {skipped} skipped")

def main():
    prices = scrape_mag_homepage()
    if prices:
        for p in prices:
            print(f"  {p['livestock_class']}: ARS {p['price_per_kg_local']:,.2f}/kg = USD {p['price_per_kg_usd']:.4f}/kg")
        upload_to_database(prices)
    else:
        print("WARNING: No prices scraped from Mercado Agroganadero")

if __name__ == "__main__":
    main()
