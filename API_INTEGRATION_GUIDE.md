# Real-Time Cattle Price API Integration Guide

## Current Status ✅

Your cattle price automation system is **LIVE and running** on Railway!

- **Database**: PostgreSQL deployed and connected
- **Service**: Python automation collecting data every 5 minutes
- **Status**: Active and operational
- **Repository**: https://github.com/JPD74/cattle-price-automation
- **Deployment**: Auto-deploys on every GitHub push

## What's Working Now

1. ✅ Database tables created (cattle_prices, collection_log)
2. ✅ Demo data collection running every 5 minutes
3. ✅ Exchange rate conversion (AUD to USD)
4. ✅ Data persistence in PostgreSQL
5. ✅ Automated deployment pipeline (GitHub → Railway)

---

## Next Steps: Real API Integration

### Phase 1: Australia - MLA Statistics API

**API Documentation**: https://app.nlrsreports.mla.com.au/statistics/documentation

**Key Features**:
- ✅ **NO AUTHENTICATION REQUIRED** - Public API
- RESTful JSON responses
- Paginated results (100 records per page)
- Historical livestock pricing data
- Saleyard information across Australia

**MLA API Endpoints**:
```
GET https://app.nlrsreports.mla.com.au/api/livestock-prices
GET https://app.nlrsreports.mla.com.au/api/saleyard
GET https://app.nlrsreports.mla.com.au/api/indicators
```

**Implementation Priority**:
1. Livestock Prices endpoint - Real-time cattle pricing by:
   - Livestock class (Heavy Steers, Medium Steers, Cows, etc.)
   - Region/Saleyard (QLD, NSW, VIC, SA, WA, etc.)
   - Weight categories
   - Price per kg (AUD)

**Code Example**:
```python
def collect_mla_australia():
    url = "https://app.nlrsreports.mla.com.au/api/livestock-prices"
    params = {
        'species': 'cattle',
        'page': 1
    }
    response = requests.get(url, params=params, timeout=30)
    data = response.json()
    # Process and insert into database
```

---

### Phase 2: New Zealand - Beef + Lamb NZ

**Data Source**: https://beeflambnz.com/industry-data/farm-data-and-industry-production/price-trend-graphs

**Status**: Web scraping required (no public API found)

**Data Available**:
- Farmgate prices for steer, heifer, cow
- Monthly price trends
- NZD pricing

**Alternative**: Manual CSV export + upload to database

---

### Phase 3: Brazil - CEPEA/ESALQ

**Organization**: Centro de Estudos Avançados em Economia Aplicada (CEPEA)
**Data Source**: https://www.cepea.esalq.usp.br/en/

**Key Indicators**:
- **CEPEA/B3 Index** - São Paulo fed cattle prices
- Regional pricing across 28 surveyed areas
- Price per arroba (BRL)
- Carcass prices (BRL/kg)

**Status**: Manual data collection or web scraping

**Data Points**:
- Fed cattle prices by state (SP, MS, GO, MT, etc.)
- Calf prices
- Price gaps between regions
- Weekly/monthly trends

---

### Phase 4: Paraguay

**Challenge**: Limited public API availability

**Potential Sources**:
1. SENACSA (National Animal Health Service) - May require contact
2. Industry associations
3. Manual data collection from market reports

**Data Needed**:
- Cattle prices by class
- Regional pricing (Concepción, San Pedro, etc.)
- PYG pricing

---

### Phase 5: Uruguay

**Primary Source**: INAC (Instituto Nacional de Carnes)
**Website**: https://www.inac.uy/

**Data Available**:
- Weekly cattle prices
- Price by category and quality
- Export statistics
- UYU pricing

**Status**: Manual data collection or web scraping likely required

---

## Implementation Roadmap

### Week 1: MLA Australia Integration
- [ ] Test MLA API endpoints
- [ ] Update `main.py` with real MLA data collection
- [ ] Map MLA response fields to database schema
- [ ] Deploy and verify data quality
- [ ] Monitor collection logs

### Week 2: Additional Countries Research
- [ ] Contact CEPEA Brazil for API access
- [ ] Research Paraguay SENACSA data availability
- [ ] Investigate INAC Uruguay data access
- [ ] Explore Beef + Lamb NZ data options

### Week 3-4: Expand Coverage
- [ ] Implement Brazil data collection
- [ ] Add NZ, Paraguay, Uruguay as data becomes available
- [ ] Build data validation and quality checks
- [ ] Create reporting dashboards

---

## Database Query Examples

Once data is collecting, you can query your PostgreSQL database:

```sql
-- Latest cattle prices by country
SELECT country, livestock_class, AVG(price_per_kg_usd) as avg_price_usd
FROM cattle_prices
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY country, livestock_class
ORDER BY country, avg_price_usd DESC;

-- Price trends over time (Australia)
SELECT DATE(timestamp) as date, 
       livestock_class,
       AVG(price_per_kg_local) as avg_aud,
       AVG(price_per_kg_usd) as avg_usd
FROM cattle_prices
WHERE country = 'AU'
  AND timestamp > NOW() - INTERVAL '30 days'
GROUP BY DATE(timestamp), livestock_class
ORDER BY date DESC;

-- Collection success rate
SELECT country, 
       COUNT(*) as total_runs,
       SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
       AVG(records_collected) as avg_records
FROM collection_log
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY country;
```

---

## Accessing Your Database

**Connection String** (in Railway Variables tab):
```
postgresql://postgres:PASSWORD@HOST:PORT/railway
```

**Tools to Connect**:
1. **pgAdmin** - GUI database manager
2. **DBeaver** - Universal database tool  
3. **psql** - Command-line PostgreSQL client
4. **Python** - Use psycopg library (already in your code)

---

## Monitoring Your Service

1. **Railway Dashboard**: https://railway.com/project/YOUR_PROJECT_ID
   - View deployment logs
   - Monitor service health
   - Check resource usage

2. **GitHub Repository**: https://github.com/JPD74/cattle-price-automation
   - All code is version controlled
   - Auto-deploys on every commit to `main` branch

3. **Service Logs**: Click "View logs" in Railway deployment
   - See real-time collection activity
   - Monitor errors and warnings
   - Track data collection success

---

## Quick Wins - What You Can Do Today

### 1. Test MLA API
Run this in your terminal or Python:
```python
import requests
url = "https://app.nlrsreports.mla.com.au/api/livestock-prices"
response = requests.get(url, params={'species': 'cattle'}, timeout=30)
print(response.json())
```

### 2. View Your Current Data
Connect to your Railway PostgreSQL database and run:
```sql
SELECT * FROM cattle_prices ORDER BY timestamp DESC LIMIT 20;
```

### 3. Check Collection Status
```sql
SELECT * FROM collection_log ORDER BY timestamp DESC LIMIT 10;
```

---

## Support & Resources

- **MLA API Support**: insights@mla.com.au
- **Railway Support**: https://railway.com/help
- **Your GitHub Repo**: https://github.com/JPD74/cattle-price-automation

---

## Summary

🎉 **Congratulations!** You now have a fully operational, cloud-hosted cattle price automation system!

**What's Live**:
- ✅ PostgreSQL database on Railway
- ✅ Python automation service collecting data every 5 minutes
- ✅ GitHub repository with auto-deployment
- ✅ Demo data collection for Australia

**Next Action**: Integrate real MLA API to start collecting actual market data from Australia, then expand to the other 4 countries as data sources become available.

Your system is designed to scale - each new country/API integration just requires adding a new collection function and deploying via GitHub commit!
