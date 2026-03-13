#!/usr/bin/env python3
"""
Live Brazil cattle price scraper - CEPEA/ESALQ Fed Cattle Index
Scrapes the CEPEA indicator page for daily fed cattle prices.
Price is per arroba (15 kg) converted to per kg.
Source: cepea.org.br/en/indicator/cattle.aspx
"""
import os
import re
import time
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
    """Scrape CEPEA cattle indicator page with enhanced headers"""
    urls = [
        "https://cepea.org.br/en/indicator/cattle.aspx",
        "https://www.cepea.esalq.usp.br/en/indicator/cattle.aspx",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

    r = None
    for url in urls:
        try:
            session = requests.Session()
            # First do a GET to the main page to get cookies
            session.get("https://cepea.org.br/en/", headers=headers, timeout=15)
            time.sleep(1)
            r = session.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                print(f"  Successfully fetched from {url}")
                break
            else:
                print(f"  {url} returned status {r.status_code}")
                r = None
        except Exception as e:
            print(f"  Failed to fetch {url}: {e}")
            r = None

    if not r or r.status_code != 200:
        print(f"Failed to fetch CEPEA page from any URL")
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
        # Try finding any table with price data
        for t in soup.find_all("table"):
            if "US$" in t.get_text() or "R$" in t.get_text():
                table = t
                break

    if not table:
        print("  No price table found on CEPEA page")
        print(f"  Page title: {soup.title.string if soup.title else 'N/A'}")
        print(f"  Page length: {len(r.text)} chars")
        return []

    rows = table.find_all("tr")
    print(f"  Found {len(rows)} table rows")

    for row in rows[1:]:  # Skip header
        cells = row.find_all("td")
        if len(cells) >= 3:
            try:
                date_text = cells[0].get_text(strip=True)
                price_text = cells[1].get_text(strip=True)

                # Parse date (format: MM/DD/YYYY)
                try:
                    price_date = datetime.strptime(date_text, "%m/%d/%Y").strftime("%Y-%m-%d")
                except:
                    try:
                        price_date = datetime.strptime(date_text, "%d/%m/%Y").strftime("%Y-%m-%d")
                    except:
                        continue

                # Parse price (USD per arroba)
                price_usd = float(price_text.replace(",", "").replace("$", "").strip())

                # Convert from per arroba (15kg) to per kg
                price_per_kg_usd = round(price_usd / 15.0, 4)

                records.append({
                    "date": price_date,
                    "price_usd_per_arroba": price_usd,
                    "price_usd_per_kg": price_per_kg_usd,
                })
            except (ValueError, IndexError) as e:
                continue

    print(f"  Scraped {len(records)} price records from CEPEA")
    return records

def upload_to_database(records):
    if not records:
        print("No records scraped - CEPEA may be blocked or page structure changed")
        return

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return

    brl_to_usd = get_fx_rate()
    if not brl_to_usd:
        brl_to_usd = 0.19  # fallback

    try:
        conn = psycopg.connect(db_url, sslmode="disable")
        cur = conn.cursor()
        uploaded = 0
        skipped = 0

        # Use latest record only
        latest = records[0]

        # CEPEA gives us USD price per arroba from Sao Paulo
        price_usd_kg = latest["price_usd_per_kg"]
        price_brl_kg = round(price_usd_kg / brl_to_usd, 2) if brl_to_usd > 0 else 0

        regions = ["Sao Paulo"]
        classes = ["Boi Gordo (Fed Cattle)"]

        for region in regions:
            for livestock_class in classes:
                cur.execute("""
                    SELECT 1 FROM cattle_prices
                    WHERE country = 'BR' AND livestock_class = %s
                    AND timestamp::date = %s::date
                    AND data_source = 'CEPEA/ESALQ'
                """, (livestock_class, latest["date"]))

                if cur.fetchone():
                    skipped += 1
                    continue

                cur.execute("""
                    INSERT INTO cattle_prices
                    (country, region, livestock_class, weight_category,
                     price_per_kg_local, price_per_kg_usd, local_currency,
                     data_source, timestamp)
                    VALUES ('BR', %s, %s, '450-550kg', %s, %s, 'BRL',
                            'CEPEA/ESALQ', %s)
                """, (region, livestock_class, price_brl_kg,
                       price_usd_kg, latest["date"]))
                uploaded += 1
                print(f"  {livestock_class} ({region}): BRL {price_brl_kg}/kg = USD {price_usd_kg}/kg")

        conn.commit()
        print(f"BR LIVE: {uploaded} uploaded, {skipped} skipped")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    print("=" * 50)
    print("BRAZIL LIVE SCRAPER - CEPEA/ESALQ Fed Cattle Index")
    print("=" * 50)

    records = scrape_cepea_cattle()
    upload_to_database(records)
