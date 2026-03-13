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


@router.get("/spread")
def price_spread(
    country: Optional[str] = Query(None, description="Filter by country code"),
):
    """Cross-country price spread for equivalent canonical classes."""
    conn = get_conn()
    cur = conn.cursor()
    cond = ""
    params = []
    if country:
        cond = "AND cp.country = %s"
        params.append(country.upper())
    cur.execute(f"""
        SELECT cp.country, m.source_class, c.canonical_name, c.stage,
               cp.price_per_kg_usd, cp.price_per_kg_local,
               cp.local_currency, cp.timestamp::date AS price_date
        FROM cattle_prices cp
        JOIN livestock_class_mapping m
            ON m.country = cp.country AND m.source_class = cp.livestock_class
        JOIN canonical_livestock_classes c ON c.id = m.canonical_class_id
        WHERE cp.weight_category IS NOT NULL {cond}
        AND cp.timestamp = (
            SELECT MAX(cp2.timestamp) FROM cattle_prices cp2
            WHERE cp2.country = cp.country
              AND cp2.livestock_class = cp.livestock_class
              AND cp2.weight_category IS NOT NULL
        )
        ORDER BY c.canonical_name, cp.price_per_kg_usd DESC
    """, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    results = []
    for r in rows:
        results.append({
            "country": r[0], "source_class": r[1],
            "canonical_name": r[2], "stage": r[3],
            "price_usd_kg": round(float(r[4]), 4),
            "price_local_kg": float(r[5]),
            "currency": r[6], "date": str(r[7])
        })
    return results


@router.get("/margin")
def stage_trade_margin(
    country: str = Query(..., description="Country code (AU, BR, AR, etc)"),
    trade: Optional[str] = Query(None, description="Trade name filter"),
):
    """Calculate current stage-trade margins using latest prices."""
    conn = get_conn()
    cur = conn.cursor()
    cond = "WHERE st.country = %s"
    params = [country.upper()]
    if trade:
        cond += " AND st.trade_name ILIKE %s"
        params.append(f"%{trade}%")
    cur.execute(f"""
        SELECT st.id, st.trade_name, st.country,
               bc.canonical_name AS buy_class,
               sc.canonical_name AS sell_class,
               st.typical_duration_months, st.typical_weight_gain_kg
        FROM stage_trades st
        JOIN canonical_livestock_classes bc ON bc.id = st.buy_class_id
        JOIN canonical_livestock_classes sc ON sc.id = st.sell_class_id
        {cond}
    """, params)
    trades = cur.fetchall()
    results = []
    for t in trades:
        tid, tname, tcountry, buy_cls, sell_cls, dur, gain = t
        # Get latest buy price
        cur.execute("""
            SELECT cp.price_per_kg_usd FROM cattle_prices cp
            JOIN livestock_class_mapping m
                ON m.country = cp.country AND m.source_class = cp.livestock_class
            JOIN canonical_livestock_classes c ON c.id = m.canonical_class_id
            WHERE cp.country = %s AND c.canonical_name = %s
              AND cp.weight_category IS NOT NULL
            ORDER BY cp.timestamp DESC LIMIT 1
        """, (tcountry, buy_cls))
        buy_row = cur.fetchone()
        # Get latest sell price
        cur.execute("""
            SELECT cp.price_per_kg_usd FROM cattle_prices cp
            JOIN livestock_class_mapping m
                ON m.country = cp.country AND m.source_class = cp.livestock_class
            JOIN canonical_livestock_classes c ON c.id = m.canonical_class_id
            WHERE cp.country = %s AND c.canonical_name = %s
              AND cp.weight_category IS NOT NULL
            ORDER BY cp.timestamp DESC LIMIT 1
        """, (tcountry, sell_cls))
        sell_row = cur.fetchone()
        if buy_row and sell_row and gain:
            buy_price = float(buy_row[0])
            sell_price = float(sell_row[0])
            buy_weight = 300  # approximate entry weight
            sell_weight = buy_weight + float(gain)
            buy_cost = buy_price * buy_weight
            sell_revenue = sell_price * sell_weight
            gross_margin = sell_revenue - buy_cost
            roi = (gross_margin / buy_cost * 100) if buy_cost > 0 else 0
            results.append({
                "trade_name": tname, "country": tcountry,
                "buy_class": buy_cls, "sell_class": sell_cls,
                "buy_price_usd_kg": round(buy_price, 4),
                "sell_price_usd_kg": round(sell_price, 4),
                "buy_cost_usd": round(buy_cost, 2),
                "sell_revenue_usd": round(sell_revenue, 2),
                "gross_margin_usd": round(gross_margin, 2),
                "roi_percent": round(roi, 1),
                "duration_months": dur,
                "weight_gain_kg": float(gain),
            })
    cur.close()
    conn.close()
    return results


@router.get("/herd-valuation")
def herd_valuation(
    country: str = Query(..., description="Country code"),
):
    """Value a herd portfolio using latest prices and AE equivalents."""
    conn = get_conn()
    cur = conn.cursor()
    # Get herd inventory for this country
    cur.execute("""
        SELECT h.id, h.portfolio_name, c.canonical_name,
               h.head_count, h.avg_weight_kg,
               COALESCE(a.ae_value, c.ae_equivalent, 1.0) AS ae,
               COALESCE(a.dse_value, 8.0) AS dse
        FROM herd_inventory h
        JOIN canonical_livestock_classes c ON c.id = h.canonical_class_id
        LEFT JOIN animal_unit_equivalents a ON a.canonical_class_id = c.id
        WHERE h.country = %s
        ORDER BY h.portfolio_name, c.canonical_name
    """, (country.upper(),))
    herd_rows = cur.fetchall()
    if not herd_rows:
        cur.close()
        conn.close()
        return {"message": "No herd inventory found. POST to /herd-inventory first.", "items": []}
    results = []
    total_head = 0
    total_ae = 0.0
    total_value = 0.0
    for h in herd_rows:
        hid, portfolio, canon_name, head, avg_wt, ae, dse = h
        # Get latest price for this canonical class in this country
        cur.execute("""
            SELECT cp.price_per_kg_usd FROM cattle_prices cp
            JOIN livestock_class_mapping m
                ON m.country = cp.country AND m.source_class = cp.livestock_class
            JOIN canonical_livestock_classes c ON c.id = m.canonical_class_id
            WHERE cp.country = %s AND c.canonical_name = %s
              AND cp.weight_category IS NOT NULL
            ORDER BY cp.timestamp DESC LIMIT 1
        """, (country.upper(), canon_name))
        price_row = cur.fetchone()
        price_usd = float(price_row[0]) if price_row else 0
        weight = float(avg_wt) if avg_wt else 450
        head_value = price_usd * weight
        line_value = head_value * head
        line_ae = float(ae) * head
        total_head += head
        total_ae += line_ae
        total_value += line_value
        results.append({
            "portfolio": portfolio, "class": canon_name,
            "head": head, "avg_weight_kg": weight,
            "price_usd_kg": round(price_usd, 4),
            "value_per_head_usd": round(head_value, 2),
            "line_value_usd": round(line_value, 2),
            "ae_per_head": float(ae), "total_ae": round(line_ae, 1),
        })
    cur.close()
    conn.close()
    return {
        "country": country.upper(),
        "total_head": total_head,
        "total_ae": round(total_ae, 1),
        "total_value_usd": round(total_value, 2),
        "items": results,
    }


@router.get("/signal")
def market_signals(
    country: Optional[str] = Query(None, description="Filter by country"),
):
    """Generate market signals: percentile position + mispricing detection."""
    conn = get_conn()
    cur = conn.cursor()
    cond = "WHERE cp.weight_category IS NOT NULL"
    params = []
    if country:
        cond += " AND cp.country = %s"
        params.append(country.upper())
    # Get latest prices mapped to canonical classes
    cur.execute(f"""
        SELECT DISTINCT ON (cp.country, c.canonical_name)
            cp.country, c.canonical_name, c.stage,
            cp.price_per_kg_usd, cp.timestamp::date
        FROM cattle_prices cp
        JOIN livestock_class_mapping m
            ON m.country = cp.country AND m.source_class = cp.livestock_class
        JOIN canonical_livestock_classes c ON c.id = m.canonical_class_id
        {cond}
        ORDER BY cp.country, c.canonical_name, cp.timestamp DESC
    """, params)
    latest = cur.fetchall()
    signals = []
    for row in latest:
        ctry, canon, stage, price_usd, pdate = row
        price_usd = float(price_usd)
        # Get historical percentile for this country+class
        cur.execute("""
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cp.price_per_kg_usd),
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY cp.price_per_kg_usd),
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cp.price_per_kg_usd),
                MIN(cp.price_per_kg_usd), MAX(cp.price_per_kg_usd),
                COUNT(*)
            FROM cattle_prices cp
            JOIN livestock_class_mapping m
                ON m.country = cp.country AND m.source_class = cp.livestock_class
            JOIN canonical_livestock_classes c ON c.id = m.canonical_class_id
            WHERE cp.country = %s AND c.canonical_name = %s
              AND cp.weight_category IS NOT NULL
        """, (ctry, canon))
        hist = cur.fetchone()
        if hist and hist[5] > 2:
            p25, p50, p75 = float(hist[0]), float(hist[1]), float(hist[2])
            hmin, hmax = float(hist[3]), float(hist[4])
            rng = hmax - hmin if hmax > hmin else 1
            pct_position = round((price_usd - hmin) / rng * 100, 1)
            if price_usd < p25:
                label = "cheap"
            elif price_usd > p75:
                label = "expensive"
            else:
                label = "fair"
            signals.append({
                "country": ctry, "canonical_name": canon,
                "stage": stage, "current_price_usd": round(price_usd, 4),
                "date": str(pdate),
                "percentile_position": pct_position,
                "signal": label,
                "p25": round(p25, 4), "p50": round(p50, 4), "p75": round(p75, 4),
                "historical_min": round(hmin, 4), "historical_max": round(hmax, 4),
                "data_points": hist[5],
            })
    cur.close()
    conn.close()
    return signals
