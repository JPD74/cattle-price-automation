#!/usr/bin/env python3
"""
Persist trade signals to the signals table.
Run daily after scrapers to build signal history.
Calculates stage-trade margins with cost benchmarks and writes to signals table.
"""
import os
import json
from datetime import date
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")


def persist():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    today = date.today()
    signals_written = 0

    # Get all stage trades
    cur.execute("""
        SELECT st.id, st.country, st.trade_name,
               bc.id AS buy_class_id, bc.canonical_name AS buy_class,
               sc.id AS sell_class_id, sc.canonical_name AS sell_class,
               st.typical_duration_months, st.typical_weight_gain_kg,
               COALESCE(bae.ae_value, bc.ae_equivalent, 1.0) AS buy_ae
        FROM stage_trades st
        JOIN canonical_livestock_classes bc ON bc.id = st.buy_class_id
        JOIN canonical_livestock_classes sc ON sc.id = st.sell_class_id
        LEFT JOIN animal_unit_equivalents bae ON bae.canonical_class_id = bc.id
    """)
    trades = cur.fetchall()

    for trade in trades:
        (tid, country, trade_name, buy_cid, buy_cls, sell_cid, sell_cls,
         duration, weight_gain, buy_ae) = trade

        if not weight_gain or not duration:
            continue

        weight_gain = float(weight_gain)
        buy_ae = float(buy_ae)

        # Get latest buy price
        cur.execute("""
            SELECT cp.price_per_kg_usd FROM cattle_prices cp
            JOIN livestock_class_mapping m ON m.country = cp.country AND m.source_class = cp.livestock_class
            JOIN canonical_livestock_classes c ON c.id = m.canonical_class_id
            WHERE cp.country = %s AND c.canonical_name = %s AND cp.weight_category IS NOT NULL
            ORDER BY cp.timestamp DESC LIMIT 1
        """, (country, buy_cls))
        buy_row = cur.fetchone()

        # Get latest sell price
        cur.execute("""
            SELECT cp.price_per_kg_usd FROM cattle_prices cp
            JOIN livestock_class_mapping m ON m.country = cp.country AND m.source_class = cp.livestock_class
            JOIN canonical_livestock_classes c ON c.id = m.canonical_class_id
            WHERE cp.country = %s AND c.canonical_name = %s AND cp.weight_category IS NOT NULL
            ORDER BY cp.timestamp DESC LIMIT 1
        """, (country, sell_cls))
        sell_row = cur.fetchone()

        if not buy_row or not sell_row:
            continue

        buy_price = float(buy_row[0])
        sell_price = float(sell_row[0])
        buy_weight = 300.0  # approximate entry weight
        sell_weight = buy_weight + weight_gain
        buy_cost = buy_price * buy_weight
        sell_revenue = sell_price * sell_weight
        gross_margin = sell_revenue - buy_cost

        # Get cost benchmarks for this country
        cur.execute("""
            SELECT cost_type, cost_per_kg_usd, cost_per_head_usd
            FROM cost_benchmarks WHERE country = %s
        """, (country,))
        costs = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

        # Calculate total costs
        feedlot_cog = float(costs.get('feedlot_cost_per_kg_gain', (0, 0))[0] or 0)
        health = float(costs.get('health_cost_per_head', (0, 0))[1] or 0)
        transport = float(costs.get('transport_cost_per_head', (0, 0))[1] or 0)
        overhead = float(costs.get('overhead_cost_per_head', (0, 0))[1] or 0)
        total_production_cost = (feedlot_cog * weight_gain) + health + transport + overhead
        net_margin = gross_margin - total_production_cost

        roi = (gross_margin / buy_cost * 100) if buy_cost > 0 else 0
        weeks = duration * 4.33
        gm_per_ae_week = (gross_margin / buy_ae / weeks) if buy_ae > 0 and weeks > 0 else 0

        # Determine signal label
        if roi > 30:
            label = 'strong_buy'
        elif roi > 15:
            label = 'buy'
        elif roi > 0:
            label = 'hold'
        elif roi > -10:
            label = 'watch'
        else:
            label = 'avoid'

        detail = {
            'trade_name': trade_name,
            'buy_class': buy_cls,
            'sell_class': sell_cls,
            'buy_price_usd_kg': round(buy_price, 4),
            'sell_price_usd_kg': round(sell_price, 4),
            'buy_cost_usd': round(buy_cost, 2),
            'sell_revenue_usd': round(sell_revenue, 2),
            'gm_per_head': round(gross_margin, 2),
            'net_margin_per_head': round(net_margin, 2),
            'total_production_cost': round(total_production_cost, 2),
            'roi_percent': round(roi, 1),
            'gm_per_ae_week': round(gm_per_ae_week, 2),
            'capital_required': round(buy_cost, 2),
            'duration_months': duration,
            'weight_gain_kg': weight_gain,
        }

        # Insert signal
        cur.execute("""
            INSERT INTO signals (country, signal_type, canonical_class_id, signal_value,
                                signal_label, detail, calculation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            country,
            'stage_trade_margin',
            sell_cid,
            round(roi, 2),
            label,
            json.dumps(detail),
            today,
        ))
        signals_written += 1

    conn.commit()
    print(f"Persisted {signals_written} trade signals for {today}")

    # Summary
    cur.execute("""
        SELECT country, signal_label, COUNT(*)
        FROM signals WHERE calculation_date = %s
        GROUP BY country, signal_label ORDER BY country
    """, (today,))
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} x{row[2]}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    persist()
