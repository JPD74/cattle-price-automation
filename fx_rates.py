#!/usr/bin/env python3
"""
FX Rates Helper - Live currency conversion using ExchangeRate-API
Open access endpoint (no API key required, updates daily)
Source: https://open.er-api.com/v6/latest/USD
"""
import requests

# Fallback rates if API is unavailable
FALLBACK_RATES = {
    "AUD": 1.54, "NZD": 1.67, "BRL": 5.15, "ARS": 1070.0,
    "PYG": 7250.0, "UYU": 39.90, "USD": 1.0
}

def get_usd_rates():
    """Fetch latest USD-based exchange rates. Returns dict of currency: rate."""
    try:
        resp = requests.get("https://open.er-api.com/v6/latest/USD", timeout=10)
        data = resp.json()
        if data.get("result") == "success":
            rates = data["rates"]
            print(f"FX rates fetched (updated: {data.get('time_last_update_utc', 'N/A')})")
            print(f"  AUD={rates.get('AUD')}, NZD={rates.get('NZD')}, BRL={rates.get('BRL')}, PYG={rates.get('PYG')}, UYU={rates.get('UYU')}, ARS={rates.get('ARS')}")
            return rates
    except Exception as e:
        print(f"FX API error: {e}")
    print("Using fallback FX rates")
    return FALLBACK_RATES

def to_usd(local_price, currency_code, rates):
    """Convert local currency price to USD."""
    rate = rates.get(currency_code, FALLBACK_RATES.get(currency_code, 1.0))
    if rate == 0:
        return 0.0
    return round(local_price / rate, 2)

if __name__ == "__main__":
    rates = get_usd_rates()
    # Test conversions
    print(f"\nTest: AUD 7.85/kg = USD {to_usd(7.85, 'AUD', rates)}/kg")
    print(f"Test: BRL 23.15/kg = USD {to_usd(23.15, 'BRL', rates)}/kg")
    print(f"Test: NZD 8.65/kg = USD {to_usd(8.65, 'NZD', rates)}/kg")
    print(f"Test: PYG 18500/kg = USD {to_usd(18500, 'PYG', rates)}/kg")
    print(f"Test: UYU 125.50/kg = USD {to_usd(125.50, 'UYU', rates)}/kg")
