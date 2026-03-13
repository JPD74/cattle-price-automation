"""
Intelligence Engine API - Livestock class mapping, stage-trade margins,
herd valuation, and market signals.
"""
import os
from typing import Optional
from datetime import date
from fastapi import APIRouter, Query, HTTPException
import psycopg

router = APIRouter(prefix="/api/intelligence", tags=["intelligence"])

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg.connect(DATABASE_URL, sslmode="disable")


@router.get("/canonical-classes")
def list_canonical_classes():
    """List all canonical livestock classes with AE equivalents."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.id, c.canonical_name, c.species, c.stage, c.sex,
               c.weight_range_kg, c.description, c.ae_equivalent,
               COALESCE(a.ae_value, c.ae_equivalent) AS ae,
               a.dse_value
        FROM canonical_livestock_classes c
        LEFT JOIN animal_unit_equivalents a ON a.canonical_class_id = c.id
        ORDER BY c.stage, c.canonical_name
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {"id": r[0], "canonical_name": r[1], "species": r[2],
         "stage": r[3], "sex": r[4], "weight_range_kg": r[5],
         "description": r[6], "ae_equivalent": float(r[7]) if r[7] else None,
         "ae": float(r[8]) if r[8] else None,
         "dse": float(r[9]) if r[9] else None}
        for r in rows
    ]


@router.get("/class-mapping")
def list_class_mappings(
    country: Optional[str] = Query(None, description="Filter by country code"),
):
    """List source-to-canonical class mappings."""
    conn = get_conn()
    cur = conn.cursor()
    cond = "WHERE 1=1"
    params = []
    if country:
        cond += " AND m.country = %s"
        params.append(country.upper())
    cur.execute(f"""
        SELECT m.id, m.country, m.source_class, c.canonical_name,
               c.stage, c.sex, m.data_source, m.confidence
        FROM livestock_class_mapping m
        JOIN canonical_livestock_classes c ON c.id = m.canonical_class_id
        {cond}
        ORDER BY m.country, m.source_class
    """, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {"id": r[0], "country": r[1], "source_class": r[2],
         "canonical_name": r[3], "stage": r[4], "sex": r[5],
         "data_source": r[6], "confidence": r[7]}
        for r in rows
    ]


@router.get("/stage-trades")
def list_stage_trades(
    country: Optional[str] = Query(None, description="Filter by country code"),
):
    """List defined stage trades with buy/sell class info."""
    conn = get_conn()
    cur = conn.cursor()
    cond = "WHERE 1=1"
    params = []
    if country:
        cond += " AND st.country = %s"
        params.append(country.upper())
    cur.execute(f"""
        SELECT st.id, st.country, st.trade_name,
               bc.canonical_name AS buy_class, sc.canonical_name AS sell_class,
               st.typical_duration_months, st.typical_weight_gain_kg
        FROM stage_trades st
        JOIN canonical_livestock_classes bc ON bc.id = st.buy_class_id
        JOIN canonical_livestock_classes sc ON sc.id = st.sell_class_id
        {cond}
        ORDER BY st.country, st.trade_name
    """, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {"id": r[0], "country": r[1], "trade_name": r[2],
         "buy_class": r[3], "sell_class": r[4],
         "duration_months": r[5],
         "weight_gain_kg": float(r[6]) if r[6] else None}
        for r in rows
    ]
