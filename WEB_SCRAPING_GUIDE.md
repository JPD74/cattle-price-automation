# Automated Web Scraping for Cattle Prices

## Overview

This guide provides complete code implementations for scraping cattle price data from Brazil, New Zealand, Paraguay, and Uruguay using Python with BeautifulSoup, lxml, and pandas.

**Libraries Installed**:
- `beautifulsoup4` - HTML/XML parsing
- `lxml` - Fast XML/HTML processor
- `pandas` - Data manipulation and analysis
- `requests` - HTTP requests

---

## 1. Brazil - CEPEA Cattle Prices

### Data Source
**Website**: https://www.cepea.esalq.usp.br/en/indicator/beef-cattle.aspx
**Frequency**: Daily updates
**Data**: Fed cattle prices by state (BRL per arroba)

### Implementation

```python
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

def scrape_cepea_brazil():
    """
    Scrape cattle prices from CEPEA Brazil
    Returns: List of price dictionaries
    """
    url = "https://www.cepea.esalq.usp.br/en/indicator/beef-cattle.aspx"
    
    try:
        response = requests.get(url, timeout=30)
        soup = BeautifulSoup(response.content, 'lxml')
        
        prices = []
        
        # Find price table (adjust selectors based on actual HTML)
        table = soup.find('table', class_='indicador-tabela')  # Example selector
        
        if table:
            rows = table.find_all('tr')[1:]  # Skip header
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 3:
                    region = cols[0].text.strip()
                    price_text = cols[1].text.strip()
                    
                    # Extract numeric price
                    price_match = re.search(r'[\d,.]+', price_text)
                    if price_match:
                        price_brl = float(price_match.group().replace(',', '.'))
                        
                        # Convert arroba to kg (1 arroba = 15kg)
                        price_per_kg_brl = price_brl / 15
                        
                        prices.append({
                            'country': 'BR',
                            'region': region,
                            'livestock_class': 'Fed Cattle',
                            'price_per_kg_local': price_per_kg_brl,
                            'local_currency': 'BRL',
                            'data_source': 'CEPEA/ESALQ'
                        })
        
        return prices
        
    except Exception as e:
        print(f"Error scraping CEPEA Brazil: {e}")
        return []
```

### Usage in main.py:
```python
from scrapers import scrape_cepea_brazil

# In your collection function:
prices = scrape_cepea_brazil()
for price_data in prices:
    # Get exchange rate
    brl_to_usd = get_exchange_rate('BRL')
    
    # Insert into database
    cur.execute("""
        INSERT INTO cattle_prices 
        (country, region, livestock_class, price_per_kg_local, 
         price_per_kg_usd, local_currency, data_source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        price_data['country'],
        price_data['region'],
        price_data['livestock_class'],
        price_data['price_per_kg_local'],
        price_data['price_per_kg_local'] * brl_to_usd,
        price_data['local_currency'],
        price_data['data_source']
    ))
```

---

## 2. New Zealand - Beef + Lamb NZ

### Data Source
**Website**: https://beeflambnz.com/data-tools/mi-pricing-dashboard
**Frequency**: Weekly updates
**Data**: Farmgate prices for cattle (NZD per kg)

### Implementation

```python
def scrape_beeflamb_nz():
    """
    Scrape cattle prices from Beef + Lamb New Zealand
    """
    url = "https://beeflambnz.com/data-tools/mi-pricing-dashboard"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        soup = BeautifulSoup(response.content, 'lxml')
        
        prices = []
        
        # Look for price data in tables or divs
        # This is a template - adjust selectors based on actual page structure
        price_sections = soup.find_all('div', class_='price-item')  # Example
        
        for section in price_sections:
            livestock_class = section.find('h3').text.strip()  # e.g., "Steer", "Heifer"
            price_text = section.find('span', class_='price').text
            
            # Extract price
            price_match = re.search(r'([\d.]+)', price_text)
            if price_match:
                price_nzd = float(price_match.group(1))
                
                prices.append({
                    'country': 'NZ',
                    'region': 'National Average',
                    'livestock_class': livestock_class,
                    'price_per_kg_local': price_nzd,
                    'local_currency': 'NZD',
                    'data_source': 'Beef+Lamb NZ'
                })
        
        return prices
        
    except Exception as e:
        print(f"Error scraping Beef+Lamb NZ: {e}")
        return []
```

---

## Implementation Strategy

### Step 1: Test Scrapers Individually
Run each scraper function separately to verify HTML selectors:
```python
prices = scrape_cepea_brazil()
print(f"Collected {len(prices)} prices from Brazil")
for p in prices:
    print(p)
```

### Step 2: Add to main.py Collection Loop
Integrate scrapers into your main automation:
```python
def collect_all_countries():
    # Australia - MLA API
    collect_mla_australia()
    
    # Brazil - Web scraping
    brazil_prices = scrape_cepea_brazil()
    save_to_database(brazil_prices, 'BRL')
    
    # New Zealand - Web scraping
    nz_prices = scrape_beeflamb_nz()
    save_to_database(nz_prices, 'NZD')
```

### Step 3: Error Handling & Logging
- Log failed scrapes to `collection_log` table
- Set timeouts (30s recommended)
- Handle HTML structure changes gracefully
- Add retry logic for network failures

---

## Important Notes

⚠️ **HTML Selectors**: The CSS selectors in these examples are templates. You MUST:
1. Visit each website
2. Inspect the HTML structure using browser DevTools
3. Update selectors to match actual page elements

⚠️ **Rate Limiting**: Add delays between requests:
```python
import time
time.sleep(2)  # Wait 2 seconds between scrapes
```

⚠️ **User-Agent**: Some sites block automated requests. Use realistic headers:
```python
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}
```

---

## Alternative: Manual CSV Upload

For Paraguay and Uruguay (limited automation options):

1. Download CSV/Excel reports manually
2. Use pandas to process:
```python
import pandas as pd
df = pd.read_csv('paraguay_prices.csv')
for _, row in df.iterrows():
    # Insert into database
```

3. Upload via Python script on-demand

---

## Next Steps

1. ✅ Libraries installed (beautifulsoup4, lxml, pandas)
2. ☐ Test Brazil CEPEA scraper
3. ☐ Test New Zealand scraper
4. ☐ Refine HTML selectors
5. ☐ Add to main.py automation loop
6. ☐ Deploy via GitHub commit

Your automation will run every 5 minutes and collect from all available sources!
