#!/usr/bin/env python3
"""
Seed cost benchmarks for all 7 countries.
Uses existing cost_benchmarks table with cost_type key-value pattern.
Sources: USDA GAIN reports, MLA, CEPEA, INAC, ARP industry data (2024-2025).
"""
import os
import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL")

# (country, cost_type, cost_per_kg_usd, cost_per_head_usd, source)
# cost_per_kg_usd used for per-kg metrics, cost_per_head_usd for per-head costs
BENCHMARKS = [
    # === BRAZIL ===
    # Pasture: ~R$3.50/AE/day at 5.70 BRL/USD = $0.61/AE/day
    ('BR', 'pasture_cost_per_ae_day', 0.61, None, 'CEPEA/Embrapa 2025'),
    # Feedlot cost of gain: R$11.40-12.50/kg => ~$2.05/kg
    ('BR', 'feedlot_cost_per_kg_gain', 2.05, None, 'CEPEA feedlot survey 2025'),
    ('BR', 'health_cost_per_head', None, 18.00, 'Embrapa beef cost model 2024'),
    ('BR', 'transport_cost_per_head', None, 25.00, 'CEPEA logistics index 2025'),
    ('BR', 'overhead_cost_per_head', None, 35.00, 'CEPEA/CNA benchmark 2024'),

    # === PARAGUAY ===
    # Lowest cost pasture system in region
    ('PY', 'pasture_cost_per_ae_day', 0.35, None, 'ARP/USDA GAIN 2024'),
    ('PY', 'feedlot_cost_per_kg_gain', 1.75, None, 'ARP industry estimate 2024'),
    ('PY', 'health_cost_per_head', None, 12.00, 'ARP veterinary survey 2024'),
    ('PY', 'transport_cost_per_head', None, 30.00, 'ARP logistics - long haul Chaco'),
    ('PY', 'overhead_cost_per_head', None, 20.00, 'ARP small producer model 2024'),

    # === URUGUAY ===
    # Cost of gain $2.00-2.15/kg liveweight (USDA GAIN 2025)
    ('UY', 'pasture_cost_per_ae_day', 0.55, None, 'INAC/INIA pastoral cost 2025'),
    ('UY', 'feedlot_cost_per_kg_gain', 2.10, None, 'USDA GAIN Uruguay 2025'),
    ('UY', 'health_cost_per_head', None, 15.00, 'INIA sanitary cost model 2024'),
    ('UY', 'transport_cost_per_head', None, 22.00, 'INAC logistics benchmark 2025'),
    ('UY', 'overhead_cost_per_head', None, 28.00, 'INIA/INAC cost structure 2024'),

    # === ARGENTINA ===
    # Feedlot margins slim per USDA GAIN, corn cheap but peso volatile
    ('AR', 'pasture_cost_per_ae_day', 0.45, None, 'INTA pastoral model 2025'),
    ('AR', 'feedlot_cost_per_kg_gain', 1.85, None, 'USDA GAIN Argentina 2025'),
    ('AR', 'health_cost_per_head', None, 14.00, 'SENASA/INTA health costs 2024'),
    ('AR', 'transport_cost_per_head', None, 28.00, 'Rosgan logistics index 2025'),
    ('AR', 'overhead_cost_per_head', None, 25.00, 'INTA cost benchmark 2024'),

    # === AUSTRALIA ===
    # Feedlot COG ~A$3.20-3.54/kg at 0.65 AUD/USD = $2.08-2.30/kg
    ('AU', 'pasture_cost_per_ae_day', 0.75, None, 'MLA/NSW DPI cost model 2025'),
    ('AU', 'feedlot_cost_per_kg_gain', 2.30, None, 'MLA/Beef Central feedlot COG 2025'),
    ('AU', 'health_cost_per_head', None, 22.00, 'MLA health cost survey 2024'),
    ('AU', 'transport_cost_per_head', None, 35.00, 'MLA transport index - Downs avg'),
    ('AU', 'overhead_cost_per_head', None, 45.00, 'ABARES farm survey 2024'),

    # === USA ===
    # Cow-calf cost $2.91/head/day (2025). Feedlot COG ~$2.40/kg
    ('US', 'pasture_cost_per_ae_day', 2.91, None, 'USDA ERS cow-calf cost 2025'),
    ('US', 'feedlot_cost_per_kg_gain', 2.40, None, 'USDA feedlot COG 2025'),
    ('US', 'health_cost_per_head', None, 45.00, 'USDA ERS veterinary costs 2024'),
    ('US', 'transport_cost_per_head', None, 55.00, 'USDA AMS livestock transport 2025'),
    ('US', 'overhead_cost_per_head', None, 65.00, 'USDA ERS overhead benchmark 2024'),

    # === NEW ZEALAND ===
    # Pastoral system, lower overhead than AU
    ('NZ', 'pasture_cost_per_ae_day', 0.65, None, 'B+LNZ economic service 2025'),
    ('NZ', 'feedlot_cost_per_kg_gain', 2.20, None, 'B+LNZ/NZX finishing cost 2025'),
    ('NZ', 'health_cost_per_head', None, 20.00, 'B+LNZ animal health survey 2024'),
    ('NZ', 'transport_cost_per_head', None, 28.00, 'B+LNZ transport benchmark 2025'),
    ('NZ', 'overhead_cost_per_head', None, 40.00, 'B+LNZ farm expense model 2024'),
]


def seed():
    conn = psycopg.connect(DATABASE_URL)
    cur = conn.cursor()
    inserted = 0
    for country, cost_type, cost_kg, cost_head, source in BENCHMARKS:
        cur.execute("""
            INSERT INTO cost_benchmarks (country, cost_type, cost_per_kg_usd, cost_per_head_usd, source, valid_from)
            VALUES (%s, %s, %s, %s, %s, CURRENT_DATE)
            ON CONFLICT DO NOTHING
        """, (country, cost_type, cost_kg, cost_head, source))
        inserted += 1
    conn.commit()
    print(f"Seeded {inserted} cost benchmark records across 7 countries")
    cur.execute("SELECT country, COUNT(*) FROM cost_benchmarks GROUP BY country ORDER BY country")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} benchmarks")
    cur.close()
    conn.close()

if __name__ == "__main__":
    seed()
