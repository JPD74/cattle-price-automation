#!/usr/bin/env python3
"""
Live Argentina cattle price scraper - Mercado Agroganadero (MAG)
Scrapes daily cattle prices from MAG Precios por Categoria page.
Source: https://www.mercadoagroganadero.com.ar
Prices are per kg live weight in ARS, converted to USD.
"""
import os
import re
import requests
import psycopg
from datetime import datetime, date, timedelta
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

def scrape_mag_prices():
    print("=== Argentina Live Cattle Price Scraper ===")
    today = date.today()
    print(f"Date: {today}")
    
    fx = get_fx_rate()
    if not fx:
        print("ERROR: Could not get FX rate. Aborting.")
        return []
    
    prices = []
    for days_back in range(0, 7):
        check_date = today - timedelta(days=days_back)
        date_str = check_date.strftime("%d/%m/%Y")
        
        url = "https://www.mercadoagroganadero.com.ar/dll/hacienda1.dll/haciinfo000502"
        form_data = {
            "datepicker1": date_str,
            "datepicker2": date_str,
            "txtFechaInicial": date_str,
            "txtFechaFinal": date_str,
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": url,
        }
        
        print(f"Trying date: {date_str}")
        try:
            # Try POST first (form submission)
            r = requests.post(url, data=form_data, headers=headers, timeout=60)
            if r.status_code != 200:
                # Fallback to GET with params
                r = requests.get(url, params=form_data, headers=headers, timeout=60)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")
            
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 8:
                        continue
                    
                    cat_text = cells[0].get_text(strip=True).upper()
                    if not cat_text or "---" in cat_text:
                        continue
                    
                    try:
                        promedio_text = cells[3].get_text(strip=True).replace(".", "").replace(",", ".")
                        promedio = float(promedio_text)
                    except (ValueError, IndexError):
                        continue
                    
                    if promedio <= 0:
                        continue
                    
                    matched_key = None
                    for key in CATEGORY_MAP:
                        if cat_text.startswith(key):
                            matched_key = key
                            break
                    
                    if not matched_key:
                        continue
                    
                    eng_name, weight_cat = CATEGORY_MAP[matched_key]
                    
                    try:
                        avg_kg_text = cells[-1].get_text(strip=True).replace(".", "").replace(",", ".")
                        avg_kg = float(avg_kg_text)
                    except (ValueError, IndexError):
                        avg_kg = None
                    
                    sub_cat = cat_text.replace(matched_key, "").strip()
                    if sub_cat:
                        full_name = f"{eng_name} ({sub_cat})"
                    else:
                        full_name = eng_name
                    
                    price_usd = round(promedio * fx, 4)
                    
                    prices.append({
                        "date": check_date.isoformat(),
                        "country": "Argentina",
                        "livestock_class": full_name,
                        "weight_category": weight_cat,
                        "price_local": round(promedio, 2),
                        "currency": "ARS",
                        "price_usd": price_usd,
                        "unit": "per_kg_live",
                        "source": "Mercado Agroganadero",
                    })
            
            if prices:
                print(f"Found {len(prices)} price records for {date_str}")
                break
            else:
                print(f"No data for {date_str}, trying previous day...")
                
        except Exception as e:
            print(f"Error fetching {date_str}: {e}")
            continue
    
    return prices

def upload_to_database(prices):
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return
    
    conn = psycopg.connect(db_url)
    cur = conn.cursor()
    
    inserted = 0
    for p in prices:
        try:
            cur.execute("""
                INSERT INTO cattle_prices (date, country, livestock_class, weight_category,
                    price_local, currency, price_usd, unit, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, country, livestock_class) DO UPDATE SET
                    price_local = EXCLUDED.price_local,
                    price_usd = EXCLUDED.price_usd,
                    source = EXCLUDED.source
            """, (
                p["date"], p["country"], p["livestock_class"], p["weight_category"],
                p["price_local"], p["currency"], p["price_usd"], p["unit"], p["source"]
            ))
            inserted += 1
        except Exception as e:
            print(f"DB error for {p['livestock_class']}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Uploaded {inserted} Argentina price records")

def main():
    prices = scrape_mag_prices()
    if prices:
        for p in prices:
            print(f"  {p['livestock_class']}: ARS {p['price_local']:,.2f}/kg = USD {p['price_usd']:.4f}/kg")
        upload_to_database(prices)
    else:
        print("WARNING: No prices scraped from Mercado Agroganadero")
        print("Site may be down or format changed. Skipping upload.")

if __name__ == "__main__":
    main()
