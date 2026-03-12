#!/usr/bin/env python3
"""
Cattle Price API - FastAPI service for querying cattle prices across
Australia, New Zealand, Brazil, Paraguay and Uruguay.
Deployed on Railway, backed by PostgreSQL.
"""
import os
from datetime import datetime, date
from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg

app = FastAPI(
    title="Cattle Price API",
    description="Cross-country cattle price database covering AU, NZ, BR, PY, UY",
    version="1.0.0",
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
        "service": "Cattle Price API",
        "version": "1.0.0",
        "endpoints": [
            "/prices",
            "/prices/latest",
            "/prices/compare",
            "/countries",
            "/summary",
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
        GROUP BY country ORDER BY country
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
    country: Optional[str] = Query(None, description="Filter by country code (AU, NZ, BR, PY, UY)"),
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
        FROM cattle_prices
        WHERE {where}
        ORDER BY timestamp DESC, country, region
        LIMIT %s
    """, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {
            "id": r[0], "date": str(r[1].date()) if r[1] else None,
            "country": r[2], "region": r[3],
            "livestock_class": r[4], "weight_category": r[5],
            "price_per_kg_local": float(r[6]), "price_per_kg_usd": float(r[7]),
            "local_currency": r[8], "data_source": r[9],
        }
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
        FROM cattle_prices
        {cond}
        ORDER BY country, region, livestock_class, timestamp DESC
    """, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {
            "id": r[0], "date": str(r[1].date()) if r[1] else None,
            "country": r[2], "region": r[3],
            "livestock_class": r[4], "weight_category": r[5],
            "price_per_kg_local": float(r[6]), "price_per_kg_usd": float(r[7]),
            "local_currency": r[8], "data_source": r[9],
        }
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
            price_per_kg_local, price_per_kg_usd, local_currency,
            timestamp
        FROM cattle_prices
        WHERE livestock_class ILIKE %s AND weight_category IS NOT NULL
        ORDER BY country, timestamp DESC
    """, (f"%{livestock_class}%",))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {
            "country": r[0], "region": r[1],
            "livestock_class": r[2], "weight_category": r[3],
            "price_per_kg_local": float(r[4]), "price_per_kg_usd": float(r[5]),
            "local_currency": r[6], "date": str(r[7].date()) if r[7] else None,
        }
        for r in rows
    ]


@app.get("/summary")
def summary():
    """Database summary stats."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT country) AS countries,
            COUNT(DISTINCT region) AS regions,
            COUNT(DISTINCT livestock_class) AS classes,
            MIN(timestamp)::date AS earliest_date,
            MAX(timestamp)::date AS latest_date
        FROM cattle_prices
        WHERE weight_category IS NOT NULL
    """)
    r = cur.fetchone()
    cur.close()
    conn.close()
    return {
        "total_records": r[0], "countries": r[1],
        "regions": r[2], "livestock_classes": r[3],
        "date_range": {"from": str(r[4]), "to": str(r[5])},
    }
