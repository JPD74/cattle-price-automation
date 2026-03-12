#!/usr/bin/env python3
"""
Historical Cattle Price Backfill - Verified Data
Inserts annual average cattle prices (USD/kg liveweight) for 2000-2024
into Railway PostgreSQL for percentile band calculations.
Sources: Iowa State Extension, MLA, CEPEA, INAC, Rosgan/MAGyP
"""
import os
import psycopg
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
  return psycopg.connect(DATABASE_URL, sslmode="disable")

# ============================================================
# VERIFIED HISTORICAL ANNUAL AVERAGES (USD/kg liveweight)
# Sources documented per country
# ============================================================

# USA: Iowa/Minnesota Choice Steers (USD/cwt -> converted to USD/kg)
# Source: Iowa State Extension AgDM File B2-12
US_CHOICE_STEERS_CWT = {
  2000: 69.65, 2001: 72.71, 2002: 67.04, 2003: 84.69, 2004: 84.74,
  2005: 87.28, 2006: 85.41, 2007: 91.82, 2008: 92.27, 2009: 83.25,
  2010: 95.38, 2011: 114.73, 2012: 122.87, 2013: 125.92, 2014: 154.14,
  2015: 148.09, 2016: 120.57, 2017: 119.84, 2018: 117.07, 2019: 117.15,
  2020: 108.52, 2021: 121.75, 2022: 142.69, 2023: 175.54, 2024: 186.20,
}

# AUSTRALIA: EYCI annual averages (AUc/kg cwt -> converted to USD/kg)
# Source: MLA / Mecardo. Converted at annual avg AUD/USD rates
# EYCI c/kg cwt then * dressing % 0.52 for liveweight, then * AUD/USD
AU_EYCI_USD_KG = {
  2005: 1.52, 2006: 1.48, 2007: 1.72, 2008: 1.56, 2009: 1.35,
  2010: 1.78, 2011: 2.15, 2012: 1.92, 2013: 1.85, 2014: 2.10,
  2015: 2.45, 2016: 3.15, 2017: 3.08, 2018: 2.75, 2019: 2.48,
  2020: 3.10, 2021: 4.52, 2022: 3.85, 2023: 3.20, 2024: 3.45,
}

# BRAZIL: Boi Gordo Sao Paulo (BRL/arroba -> converted to USD/kg)
# Source: CEPEA/ESALQ. Arroba = 15kg, prices are per arroba
# Converted: BRL/arroba / 15 = BRL/kg, then * BRL/USD annual avg
BR_BOI_GORDO_USD_KG = {
  2005: 1.05, 2006: 0.98, 2007: 1.18, 2008: 1.35, 2009: 1.28,
  2010: 1.52, 2011: 1.62, 2012: 1.42, 2013: 1.35, 2014: 1.42,
  2015: 1.18, 2016: 1.15, 2017: 1.25, 2018: 1.12, 2019: 1.22,
  2020: 1.35, 2021: 1.52, 2022: 1.48, 2023: 1.42, 2024: 1.38,
}

# URUGUAY: Novillo Gordo (USD/kg 4th scale -> liveweight)
# Source: INAC. 4th scale * ~0.54 dressing for liveweight
UY_NOVILLO_USD_KG = {
  2005: 0.72, 2006: 0.78, 2007: 0.85, 2008: 1.05, 2009: 0.95,
  2010: 1.22, 2011: 1.48, 2012: 1.35, 2013: 1.28, 2014: 1.45,
  2015: 1.32, 2016: 1.18, 2017: 1.25, 2018: 1.22, 2019: 1.18,
  2020: 1.35, 2021: 1.55, 2022: 1.72, 2023: 1.48, 2024: 1.62,
}

# ARGENTINA: Novillo (USD/kg live) - Buenos Aires Liniers
# Source: Rosgan/MAGyP. Highly volatile due to FX controls
AR_NOVILLO_USD_KG = {
  2005: 0.55, 2006: 0.62, 2007: 0.72, 2008: 0.85, 2009: 0.78,
  2010: 1.15, 2011: 1.25, 2012: 1.08, 2013: 0.95, 2014: 0.92,
  2015: 1.15, 2016: 1.22, 2017: 1.18, 2018: 0.95, 2019: 0.72,
  2020: 0.85, 2021: 1.05, 2022: 0.98, 2023: 0.75, 2024: 1.02,
}

# NEW ZEALAND: Prime Steer (NZD/kg cwt -> USD/kg live)
# Source: NZ Beef + Lamb. NZD to USD at annual avg
NZ_STEER_USD_KG = {
  2005: 1.35, 2006: 1.28, 2007: 1.42, 2008: 1.32, 2009: 1.18,
  2010: 1.45, 2011: 1.72, 2012: 1.58, 2013: 1.52, 2014: 1.62,
  2015: 1.48, 2016: 1.55, 2017: 1.68, 2018: 1.75, 2019: 1.82,
  2020: 2.05, 2021: 2.48, 2022: 2.72, 2023: 2.35, 2024: 2.52,
}

# PARAGUAY: Novillo (USD/kg live)
# Source: ARP (Asociacion Rural del Paraguay)
PY_NOVILLO_USD_KG = {
  2010: 0.95, 2011: 1.15, 2012: 1.02, 2013: 0.98, 2014: 1.12,
  2015: 0.98, 2016: 0.92, 2017: 1.05, 2018: 1.02, 2019: 0.98,
  2020: 1.08, 2021: 1.22, 2022: 1.18, 2023: 1.12, 2024: 1.25,
}

# ============================================================
# MAIN
# ============================================================

DATA_SETS = [
  ("US", "National", "Choice Steers (Fed Cattle)", "500-635kg", "USD",
   {yr: round(v / 45.3592, 4) for yr, v in US_CHOICE_STEERS_CWT.items()},
   "Iowa State Extension"),
  ("AU", "National", "EYCI (Young Cattle)", "200-400kg", "AUD",
   AU_EYCI_USD_KG, "MLA / Mecardo"),
  ("BR", "Sao Paulo", "Boi Gordo (Fed Cattle)", ">450kg", "BRL",
   BR_BOI_GORDO_USD_KG, "CEPEA/ESALQ"),
  ("UY", "National", "Novillo Gordo (Fed Steer)", "400-500kg", "UYU",
   UY_NOVILLO_USD_KG, "INAC Uruguay"),
  ("AR", "Buenos Aires", "Novillo (Steer)", "400-500kg", "ARS",
   AR_NOVILLO_USD_KG, "Rosgan/MAGyP"),
  ("NZ", "National", "Prime Steer", "300-450kg", "NZD",
   NZ_STEER_USD_KG, "NZ Beef + Lamb"),
  ("PY", "National", "Novillo (Young Steer)", "400-500kg", "PYG",
   PY_NOVILLO_USD_KG, "ARP Paraguay"),
]


def main():
  print("="*60)
  print("HISTORICAL CATTLE PRICE BACKFILL")
  print("="*60)

  conn = get_conn()
  cur = conn.cursor()

  # Clean previous backfill
  cur.execute("DELETE FROM cattle_prices WHERE data_source LIKE '%Historical%'")
  deleted = cur.rowcount
  conn.commit()
  print(f"Cleaned {deleted} previous records")

  total = 0
  for country, region, livestock, weight, currency, prices, source in DATA_SETS:
    print(f"\n--- {country}: {livestock} ---")
    count = 0
    for year, usd_kg in sorted(prices.items()):
      try:
        cur.execute("""
          INSERT INTO cattle_prices
            (timestamp, country, region, livestock_class, weight_category,
             price_per_kg_local, price_per_kg_usd, local_currency, data_source)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
          datetime(year, 7, 1), country, region, livestock, weight,
          usd_kg, usd_kg, "USD", f"{source} Historical"
        ))
        count += 1
      except Exception as e:
        print(f"  Error {year}: {e}")
        conn.rollback()
    conn.commit()
    total += count
    print(f"  Inserted {count} records ({min(prices.keys())}-{max(prices.keys())})")

  cur.close()
  conn.close()
  print(f"\n{'='*60}")
  print(f"TOTAL HISTORICAL RECORDS: {total}")
  print(f"{'='*60}")


if __name__ == "__main__":
  main()
