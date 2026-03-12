#!/usr/bin/env python3
"""
Cattle & Crop Price API - FastAPI service for querying cattle and crop prices
across Australia, New Zealand, Brazil, Paraguay, Uruguay, Argentina and USA.
Deployed on Railway, backed by PostgreSQL.
"""
import os
from datetime import datetime, date
from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import psycopg

app = FastAPI(
    title="Cattle & Crop Price API",
    description="Cross-country cattle and crop price database covering AU, NZ, BR, PY, UY, AR, US",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg.connect(DATABASE_URL, sslmode="disable")


@app.get("/")
def root():
    return {
        "service": "Cattle & Crop Price API",
        "version": "2.0.0",
        "endpoints": [
            "/prices",
            "/prices/latest",
            "/prices/compare",
            "/prices/trend",
            "/countries",
            "/summary",
            "/crops",
            "/crops/compare",
        ],
    }


@app.get("/countries")
def list_countries():
    """List all countries and their available regions/classes."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT country,
               array_agg(DISTINCT region) AS regions,
               array_agg(DISTINCT livestock_class) AS classes,
               COUNT(*) AS total_records
        FROM cattle_prices
        WHERE weight_category IS NOT NULL
        GROUP BY country
        ORDER BY country
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {"country": r[0], "regions": r[1], "livestock_classes": r[2], "total_records": r[3]}
        for r in rows
    ]


@app.get("/prices")
def get_prices(
    country: Optional[str] = Query(None, description="Filter by country code"),
    region: Optional[str] = Query(None, description="Filter by region"),
    livestock_class: Optional[str] = Query(None, description="Filter by livestock class"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=500),
):
    """Query cattle prices with optional filters."""
    conn = get_conn()
    cur = conn.cursor()
    conditions = ["weight_category IS NOT NULL"]
    params = []
    if country:
        conditions.append("country = %s")
        params.append(country.upper())
    if region:
        conditions.append("region ILIKE %s")
        params.append(f"%{region}%")
    if livestock_class:
        conditions.append("livestock_class ILIKE %s")
        params.append(f"%{livestock_class}%")
    if date_from:
        conditions.append("timestamp::date >= %s::date")
        params.append(date_from)
    if date_to:
        conditions.append("timestamp::date <= %s::date")
        params.append(date_to)
    where = " AND ".join(conditions)
    params.append(limit)
    cur.execute(f"""
        SELECT id, timestamp, country, region, livestock_class, weight_category,
               price_per_kg_local, price_per_kg_usd, local_currency, data_source
        FROM cattle_prices WHERE {where}
        ORDER BY timestamp DESC, country, region LIMIT %s
    """, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {"id": r[0], "date": str(r[1].date()) if r[1] else None, "country": r[2],
         "region": r[3], "livestock_class": r[4], "weight_category": r[5],
         "price_per_kg_local": float(r[6]), "price_per_kg_usd": float(r[7]),
         "local_currency": r[8], "data_source": r[9]}
        for r in rows
    ]


@app.get("/prices/latest")
def get_latest_prices(
    country: Optional[str] = Query(None, description="Filter by country code"),
):
    """Get the most recent price for each country/class combination."""
    conn = get_conn()
    cur = conn.cursor()
    cond = "WHERE weight_category IS NOT NULL"
    params = []
    if country:
        cond += " AND country = %s"
        params.append(country.upper())
    cur.execute(f"""
        SELECT DISTINCT ON (country, region, livestock_class)
               id, timestamp, country, region, livestock_class, weight_category,
               price_per_kg_local, price_per_kg_usd, local_currency, data_source
        FROM cattle_prices {cond}
        ORDER BY country, region, livestock_class, timestamp DESC
    """, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {"id": r[0], "date": str(r[1].date()) if r[1] else None, "country": r[2],
         "region": r[3], "livestock_class": r[4], "weight_category": r[5],
         "price_per_kg_local": float(r[6]), "price_per_kg_usd": float(r[7]),
         "local_currency": r[8], "data_source": r[9]}
        for r in rows
    ]


@app.get("/prices/compare")
def compare_countries(
    livestock_class: str = Query("Fed Cattle", description="Livestock class keyword to compare"),
):
    """Compare latest prices across countries for a given livestock class."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (country)
               country, region, livestock_class, weight_category,
               price_per_kg_local, price_per_kg_usd, local_currency, timestamp
        FROM cattle_prices
        WHERE livestock_class ILIKE %s AND weight_category IS NOT NULL
        ORDER BY country, timestamp DESC
    """, (f"%{livestock_class}%",))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {"country": r[0], "region": r[1], "livestock_class": r[2],
         "weight_category": r[3], "price_per_kg_local": float(r[4]),
         "price_per_kg_usd": float(r[5]), "local_currency": r[6],
         "date": str(r[7].date()) if r[7] else None}
        for r in rows
    ]


@app.get("/prices/trend")
def price_trend(
    country: Optional[str] = Query(None, description="Filter by country code"),
    livestock_class: Optional[str] = Query(None, description="Filter by livestock class"),
    months: int = Query(12, ge=1, le=60, description="Number of months of history"),
):
    """Get price trend data by livestock class - time series for trend analysis."""
    conn = get_conn()
    cur = conn.cursor()
    conditions = ["weight_category IS NOT NULL"]
    params = []
    if country:
        conditions.append("country = %s")
        params.append(country.upper())
    if livestock_class:
        conditions.append("livestock_class ILIKE %s")
        params.append(f"%{livestock_class}%")
    conditions.append("timestamp >= CURRENT_DATE - INTERVAL '%s months'")
    params.append(months)
    where = " AND ".join(conditions)
    cur.execute(f"""
        SELECT country, livestock_class,
               DATE_TRUNC('month', timestamp)::date AS month,
               ROUND(AVG(price_per_kg_usd)::numeric, 2) AS avg_usd,
               ROUND(AVG(price_per_kg_local)::numeric, 2) AS avg_local,
               local_currency,
               COUNT(*) AS data_points
        FROM cattle_prices
        WHERE {where}
        GROUP BY country, livestock_class, month, local_currency
        ORDER BY country, livestock_class, month
    """, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {"country": r[0], "livestock_class": r[1], "month": str(r[2]),
         "avg_price_usd_per_kg": float(r[3]), "avg_price_local_per_kg": float(r[4]),
         "local_currency": r[5], "data_points": r[6]}
        for r in rows
    ]


@app.get("/summary")
def summary():
    """Database summary stats."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) AS total_records,
               COUNT(DISTINCT country) AS countries,
               COUNT(DISTINCT region) AS regions,
               COUNT(DISTINCT livestock_class) AS classes,
               MIN(timestamp)::date AS earliest_date,
               MAX(timestamp)::date AS latest_date
        FROM cattle_prices WHERE weight_category IS NOT NULL
    """)
    r = cur.fetchone()
    # Also get crop stats
    try:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT crop_type) FROM crop_prices")
        cr = cur.fetchone()
        crop_records = cr[0] if cr else 0
        crop_types = cr[1] if cr else 0
    except Exception:
        crop_records = 0
        crop_types = 0
    cur.close()
    conn.close()
    return {
        "cattle_records": r[0],
        "countries": r[1],
        "regions": r[2],
        "livestock_classes": r[3],
        "date_range": {"from": str(r[4]), "to": str(r[5])},
        "crop_records": crop_records,
        "crop_types": crop_types,
    }


@app.get("/crops")
def get_crops(
    country: Optional[str] = Query(None, description="Filter by country code"),
    crop_type: Optional[str] = Query(None, description="Filter by crop type"),
    limit: int = Query(100, ge=1, le=500),
):
    """Query crop prices with optional filters."""
    conn = get_conn()
    cur = conn.cursor()
    conditions = ["1=1"]
    params = []
    if country:
        conditions.append("country = %s")
        params.append(country.upper())
    if crop_type:
        conditions.append("crop_type ILIKE %s")
        params.append(f"%{crop_type}%")
    where = " AND ".join(conditions)
    params.append(limit)
    try:
        cur.execute(f"""
            SELECT id, timestamp, country, region, crop_type,
                   price_per_tonne_local, price_per_tonne_usd,
                   local_currency, delivery_period, data_source
            FROM crop_prices WHERE {where}
            ORDER BY timestamp DESC, country LIMIT %s
        """, params)
        rows = cur.fetchall()
    except Exception:
        rows = []
    cur.close()
    conn.close()
    return [
        {"id": r[0], "date": str(r[1].date()) if r[1] else None, "country": r[2],
         "region": r[3], "crop_type": r[4],
         "price_per_tonne_local": float(r[5]), "price_per_tonne_usd": float(r[6]),
         "local_currency": r[7], "delivery_period": r[8], "data_source": r[9]}
        for r in rows
    ]


@app.get("/crops/compare")
def compare_crop_prices(
    crop_type: str = Query("Soybeans", description="Crop type to compare across countries"),
):
    """Compare latest crop prices across countries."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT ON (country)
                   country, region, crop_type,
                   price_per_tonne_local, price_per_tonne_usd,
                   local_currency, delivery_period, timestamp
            FROM crop_prices
            WHERE crop_type ILIKE %s
            ORDER BY country, timestamp DESC
        """, (f"%{crop_type}%",))
        rows = cur.fetchall()
    except Exception:
        rows = []
    cur.close()
    conn.close()
    return [
        {"country": r[0], "region": r[1], "crop_type": r[2],
         "price_per_tonne_local": float(r[3]), "price_per_tonne_usd": float(r[4]),
         "local_currency": r[5], "delivery_period": r[6],
         "date": str(r[7].date()) if r[7] else None}
        for r in rows
    ]


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    """Interactive dashboard for cattle and crop price data."""
    html = open(os.path.join(os.path.dirname(__file__), "dashboard.html")).read()
    return HTMLResponse(content=html)
