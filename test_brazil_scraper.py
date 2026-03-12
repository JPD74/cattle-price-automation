#!/usr/bin/env python3
"""Test Brazil CEPEA Cattle Price Scraper"""

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import json

def scrape_cepea_brazil():
    """
    Scrape cattle prices from CEPEA Brazil
    Based on actual website structure at https://www.cepea.org.br/br/indicador/boi-gordo.aspx
    """
    url = "https://www.cepea.org.br/br/indicador/boi-gordo.aspx"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        print(f"\n[CEPEA] Connecting to {url}...")
        response = requests.get(url, headers=headers, timeout=30)
        print(f"[CEPEA] Status code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"[CEPEA] Error: HTTP {response.status_code}")
            return []
        
        soup = BeautifulSoup(response.content, 'lxml')
        prices = []
        
        # Find the main indicator table
        # Looking for "INDICADOR DO BOI GORDO CEPEA/ESALQ"
        tables = soup.find_all('table')
        print(f"[CEPEA] Found {len(tables)} tables on page")
        
        for i, table in enumerate(tables):
            print(f"\n[CEPEA] Analyzing table {i+1}...")
            rows = table.find_all('tr')
            
            if len(rows) < 2:
                continue
                
            # Check if this is the price table by looking for header
            header = rows[0].get_text()
            if 'VALOR R$' in header or 'R$' in header:
                print(f"[CEPEA] Found price table!")
                
                # Skip header row
                for row in rows[1:]:
                    cols = row.find_all('td')
                    
                    if len(cols) >= 2:
                        date_text = cols[0].get_text(strip=True)
                        price_text = cols[1].get_text(strip=True)
                        
                        # Extract numeric price
                        price_match = re.search(r'(\d+[,.]\d+)', price_text)
                        
                        if price_match and date_text:
                            price_brl_arroba = float(price_match.group(1).replace(',', '.'))
                            
                            # Convert from arroba (15kg) to per kg
                            price_per_kg_brl = price_brl_arroba / 15
                            
                            price_data = {
                                'date': date_text,
                                'country': 'BR',
                                'region': 'São Paulo',
                                'livestock_class': 'Boi Gordo (Fed Cattle)',
                                'price_per_arroba_brl': round(price_brl_arroba, 2),
                                'price_per_kg_brl': round(price_per_kg_brl, 2),
                                'local_currency': 'BRL',
                                'data_source': 'CEPEA/ESALQ'
                            }
                            
                            prices.append(price_data)
                            print(f"  ✓ {date_text}: R$ {price_brl_arroba}/arroba = R$ {price_per_kg_brl:.2f}/kg")
        
        print(f"\n[CEPEA] ✅ Successfully scraped {len(prices)} price records")            
        return prices
        
    except requests.exceptions.Timeout:
        print("[CEPEA] ❌ Error: Request timed out")
        return []
    except Exception as e:
        print(f"[CEPEA] ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    print("="*60)
    print("🇧🇷 CEPEA Brazil Cattle Price Scraper Test")
    print("="*60)
    
    prices = scrape_cepea_brazil()
    
    if prices:
        print(f"\n📊 Results Summary:")
        print(f"   Total records: {len(prices)}")
        print(f"\n📋 Sample data (first 3 records):")
        for p in prices[:3]:
            print(json.dumps(p, indent=2, ensure_ascii=False))
    else:
        print("\n❌ No prices collected")
