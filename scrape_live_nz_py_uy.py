#!/usr/bin/env python3
"""Live scraper for New Zealand, Paraguay, and Uruguay cattle prices.
NZ: MLA Global Cattle Prices API (report/7)
Uruguay: INAC weekly price reports
Paraguay: SENACSA/industry price data
"""
import os
import psycopg2
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

def get_fx_rates():
    """Get exchange rates for NZD, PYG, UYU to USD"""
    rates = {"NZD": 0.60, "PYG": 0.00013, "UYU": 0.024}
    for currency in ["NZD", "PYG", "UYU"]:
        try:
            r = requests.get(f"https://open.er-api.com/v6/latest/{currency}", timeout=15)
            data = r.json()
            rates[currency] = data["rates"]["USD"]
        except:
            pass
    return rates

# --- NEW ZEALAND via MLA Global Cattle Prices ---
def scrape_nz_prices():
    """Fetch NZ cattle prices from MLA Global Cattle Prices API (report/7)"""
    url = "https://app.nlrsreports.mla.com.au/report/7"
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    params = {"fromDate": from_date, "toDate": to_date, "page": 1}
    records = []
    
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 404:
            r = requests.get(f"{url.replace('/report/', '/statistics/report/')}", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        for row in data.get("data", []):
            country = row.get("country", "")
            if "New Zealand" in country or "NZ" in country:
                records.append({
                    "country": "New Zealand",
                    "region": row.get("region", "National"),
                    "livestock_class": row.get("category", "Cattle"),
                    "date": row.get("calendar_date", "")[:10],
                    "price_usd_per_kg": float(row.get("price_usc_kg", 0)) / 100.0,
                    "price_local": float(row.get("price_local", 0))
                })
    except Exception as e:
        print(f"  NZ MLA API error: {e}")
        print("  Falling back to NZ MPI data...")
        records = scrape_nz_fallback()
    
    return records

def scrape_nz_fallback():
    """Fallback: scrape NZ beef+lamb schedule prices"""
    url = "https://beeflambnz.com/data-tools/schedule-prices"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    records = []
    
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            # Look for schedule price tables
            tables = soup.find_all("table")
            for table in tables:
                text = table.get_text().lower()
                if "steer" in text or "bull" in text or "cow" in text:
                    rows = table.find_all("tr")
                    for row in rows[1:]:
                        cells = row.find_all("td")
                        if len(cells) >= 2:
                            try:
                                class_name = cells[0].get_text(strip=True)
                                price = float(cells[1].get_text(strip=True).replace("$", "").replace(",", ""))
                                records.append({
                                    "country": "New Zealand",
                                    "region": "National",
                                    "livestock_class": class_name,
                                    "date": datetime.now().strftime("%Y-%m-%d"),
                                    "price_usd_per_kg": None,
                                    "price_local": price
                                })
                            except:
                                continue
    except Exception as e:
        print(f"  NZ fallback error: {e}")
    
    return records

# --- URUGUAY via INAC ---
def scrape_uruguay_prices():
    """Scrape Uruguay cattle prices from INAC"""
    url = "https://www.inac.uy/innovaportal/v/20129/17/innova.front/precios-del-ganado-en-pie"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    records = []
    
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            tables = soup.find_all("table")
            
            for table in tables:
                text = table.get_text().lower()
                if "novillo" in text or "vaca" in text or "vaquillona" in text:
                    rows = table.find_all("tr")
                    for row in rows:
                        cells = row.find_all("td")
                        if len(cells) >= 2:
                            try:
                                category = cells[0].get_text(strip=True)
                                price_text = cells[-1].get_text(strip=True)
                                price = float(price_text.replace("US$", "").replace("$", "").replace(",", ".").strip())
                                
                                class_map = {
                                    "novillo": "Steer",
                                    "vaca": "Cow",
                                    "vaquillona": "Heifer",
                                    "ternero": "Calf",
                                    "toro": "Bull"
                                }
                                livestock_class = category
                                for es, en in class_map.items():
                                    if es in category.lower():
                                        livestock_class = en
                                        break
                                
                                records.append({
                                    "country": "Uruguay",
                                    "region": "National",
                                    "livestock_class": livestock_class,
                                    "date": datetime.now().strftime("%Y-%m-%d"),
                                    "price_usd_per_kg": price,
                                    "price_local": None
                                })
                            except:
                                continue
    except Exception as e:
        print(f"  Uruguay INAC error: {e}")
    
    return records

# --- PARAGUAY ---
def scrape_paraguay_prices():
    """Scrape Paraguay cattle prices from ARP or industry sources"""
    url = "https://www.arp.org.py/precios-del-ganado/"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    records = []
    
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            tables = soup.find_all("table")
            
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        try:
                            category = cells[0].get_text(strip=True)
                            price_text = cells[-1].get_text(strip=True)
                            price = float(price_text.replace("Gs.", "").replace(".", "").replace(",", ".").strip())
                            
                            records.append({
                                "country": "Paraguay",
                                "region": "National",
                                "livestock_class": category,
                                "date": datetime.now().strftime("%Y-%m-%d"),
                                "price_usd_per_kg": None,
                                "price_local": price
                            })
                        except:
                            continue
    except Exception as e:
        print(f"  Paraguay ARP error: {e}")
    
    return records

def upload_to_db(all_records, fx_rates):
    """Upload all records to Railway PostgreSQL"""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return
    
    currency_map = {
        "New Zealand": ("NZD", fx_rates.get("NZD", 0.60)),
        "Uruguay": ("UYU", fx_rates.get("UYU", 0.024)),
        "Paraguay": ("PYG", fx_rates.get("PYG", 0.00013))
    }
    
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        uploaded = 0
        skipped = 0
        
        for r in all_records:
            country = r["country"]
            currency, fx = currency_map.get(country, ("USD", 1.0))
            
            # Check duplicates
            cur.execute("""
                SELECT 1 FROM cattle_prices 
                WHERE country = %s AND livestock_class = %s 
                AND timestamp = %s AND data_source = %s
            """, (country, r["livestock_class"], r["date"], f"{country.upper()[:2]}_LIVE"))
            
            if cur.fetchone():
                skipped += 1
                continue
            
            price_usd = r.get("price_usd_per_kg")
            price_local = r.get("price_local")
            
            if price_usd and not price_local and fx > 0:
                price_local = round(price_usd / fx, 4)
            elif price_local and not price_usd and fx > 0:
                price_usd = round(price_local * fx, 4)
            
            cur.execute("""
                INSERT INTO cattle_prices
                (country, region, livestock_class, weight_category,
                 price_per_kg_local, price_per_kg_usd, local_currency,
                 data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                country, r["region"], r["livestock_class"],
                "Standard",
                price_local, price_usd, currency,
                f"{country.upper()[:2]}_LIVE", r["date"]
            ))
            uploaded += 1
            print(f"  {country} | {r['livestock_class']} | US${price_usd:.2f}/kg" if price_usd else f"  {country} | {r['livestock_class']}")
        
        conn.commit()
        print(f"NZ/PY/UY LIVE: {uploaded} uploaded, {skipped} skipped")
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    print("=" * 50)
    print("NZ / PARAGUAY / URUGUAY LIVE SCRAPERS")
    print("=" * 50)
    
    fx_rates = get_fx_rates()
    print(f"FX Rates: NZD={fx_rates['NZD']:.4f}, PYG={fx_rates['PYG']:.6f}, UYU={fx_rates['UYU']:.4f}")
    
    all_records = []
    
    print("\n--- New Zealand ---")
    nz = scrape_nz_prices()
    print(f"  {len(nz)} records")
    all_records.extend(nz)
    
    print("\n--- Uruguay ---")
    uy = scrape_uruguay_prices()
    print(f"  {len(uy)} records")
    all_records.extend(uy)
    
    print("\n--- Paraguay ---")
    py_rec = scrape_paraguay_prices()
    print(f"  {len(py_rec)} records")
    all_records.extend(py_rec)
    
    print(f"\nTotal: {len(all_records)} records")
    if all_records:
        upload_to_db(all_records, fx_rates)
