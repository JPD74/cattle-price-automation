#!/usr/bin/env python3
"""Live Brazil cattle price scraper - CEPEA/ESALQ Fed Cattle Index
Scrapes the CEPEA indicator page for daily fed cattle prices.
Price is per arroba (15 kg) converted to per kg.
Source: cepea.org.br/en/indicator/cattle.aspx
"""
import os
import re
import psycopg
import requests
from datetime import datetime, date
from bs4 import BeautifulSoup

def get_fx_rate():
    """Get BRL to USD exchange rate"""
    try:
        r = requests.get("https://open.er-api.com/v6/latest/BRL", timeout=15)
        data = r.json()
        return data["rates"]["USD"]
    except Exception as e:
        print(f"FX rate error: {e}")
        return None

def scrape_cepea_cattle():
    """Scrape CEPEA cattle indicator page"""
    url = "https://cepea.org.br/en/indicator/cattle.aspx"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch CEPEA page: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    records = []

    # Find the price table
    table = soup.find("table", {"id": "imagenet-indicador1"})
    if not table:
        tables = soup.find_all("table")
        for t in tables:
            text = t.get_text()
            if "PRICE" in text.upper() and "DAILY" in text.upper():
                table = t
                break

    if not table:
        print("Could not find CEPEA price table")
        pattern = r'(\d{2}/\d{2}/\d{4})\s*\s*]*>\s*([\d.,]+)'
        matches = re.findall(pattern, r.text)
        for date_str, price_usd in matches:
            try:
                dt = datetime.strptime(date_str, "%m/%d/%Y")
                price_per_arroba_usd = float(price_usd.replace(",", ""))
                price_per_kg_usd = round(price_per_arroba_usd / 15.0, 4)
                records.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "price_per_kg_usd": price_per_kg_usd,
                    "price_per_arroba_usd": price_per_arroba_usd
                })
            except:
                continue
        return records

    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 2:
            date_text = cells[0].get_text(strip=True)
            price_text = cells[1].get_text(strip=True)
            try:
                dt = datetime.strptime(date_text, "%m/%d/%Y")
                price_per_arroba_usd = float(price_text.replace(",", ""))
                price_per_kg_usd = round(price_per_arroba_usd / 15.0, 4)
                records.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "price_per_kg_usd": price_per_kg_usd,
                    "price_per_arroba_usd": price_per_arroba_usd
                })
            except (ValueError, IndexError):
                continue

    return records

def upload_to_db(records):
    """Upload scraped records to Railway PostgreSQL"""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return

    brl_to_usd = get_fx_rate()

    try:
        conn = psycopg.connect(db_url)
        cur = conn.cursor()
        uploaded = 0
        skipped = 0

        for r in records:
            cur.execute("""
                SELECT 1 FROM cattle_prices
                WHERE country = 'BR'
                AND region = 'Sao Paulo'
                AND livestock_class = 'Fed Cattle'
                AND timestamp = %s
                AND data_source = 'CEPEA_LIVE'
            """, (r["date"],))

            if cur.fetchone():
                skipped += 1
                continue

            price_brl = None
            if brl_to_usd and brl_to_usd > 0:
                price_brl = round(r["price_per_kg_usd"] / brl_to_usd, 4)

            cur.execute("""
                INSERT INTO cattle_prices (country, region, livestock_class, weight_category,
                    price_per_kg_local, price_per_kg_usd, local_currency, data_source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                "BR", "Sao Paulo", "Fed Cattle", "Standard (per arroba/15kg)",
                price_brl, r["price_per_kg_usd"], "BRL", "CEPEA_LIVE", r["date"]
            ))
            uploaded += 1

        conn.commit()
        print(f"BRAZIL LIVE: {uploaded} uploaded, {skipped} skipped")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    print("=" * 50)
    print("BRAZIL LIVE SCRAPER - CEPEA/ESALQ Fed Cattle Index")
    print("=" * 50)

    records = scrape_cepea_cattle()
    print(f"Scraped {len(records)} price records from CEPEA")
    if records:
        for r in records:
            print(f"  {r['date']}: US${r['price_per_kg_usd']:.4f}/kg")
        upload_to_db(records)
    else:
        print("No records scraped - CEPEA may be blocked or page structure changed")
