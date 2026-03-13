import os
import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn

# ============================================================
# MIGRATION: Intelligence Engine Schema Upgrade
# Adds 9 new tables per Development Direction Brief
# ============================================================

CREATE_TABLES_SQL = """

-- 1. Canonical Livestock Classes
CREATE TABLE IF NOT EXISTS canonical_livestock_classes (
    id SERIAL PRIMARY KEY,
    canonical_name VARCHAR(100) NOT NULL UNIQUE,
    species VARCHAR(50) NOT NULL DEFAULT 'cattle',
    stage VARCHAR(50) NOT NULL,
    sex VARCHAR(20),
    weight_range_kg VARCHAR(50),
    description TEXT,
    ae_equivalent DECIMAL(4,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Livestock Class Mapping (source -> canonical)
CREATE TABLE IF NOT EXISTS livestock_class_mapping (
    id SERIAL PRIMARY KEY,
    country VARCHAR(2) NOT NULL,
    source_class VARCHAR(100) NOT NULL,
    canonical_class_id INTEGER REFERENCES canonical_livestock_classes(id),
    data_source VARCHAR(100),
    confidence VARCHAR(20) DEFAULT 'high',
    notes TEXT,
    UNIQUE(country, source_class, data_source)
);

-- 3. Cost Benchmarks
CREATE TABLE IF NOT EXISTS cost_benchmarks (
    id SERIAL PRIMARY KEY,
    country VARCHAR(2) NOT NULL,
    canonical_class_id INTEGER REFERENCES canonical_livestock_classes(id),
    cost_type VARCHAR(50) NOT NULL,
    cost_per_kg_usd DECIMAL(10,4),
    cost_per_head_usd DECIMAL(10,2),
    source VARCHAR(200),
    valid_from DATE,
    valid_to DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Animal Unit Equivalents
CREATE TABLE IF NOT EXISTS animal_unit_equivalents (
    id SERIAL PRIMARY KEY,
    canonical_class_id INTEGER REFERENCES canonical_livestock_classes(id),
    ae_value DECIMAL(4,2) NOT NULL,
    dse_value DECIMAL(4,2),
    source VARCHAR(200),
    notes TEXT,
    UNIQUE(canonical_class_id)
);

-- 5. Stage Trades
CREATE TABLE IF NOT EXISTS stage_trades (
    id SERIAL PRIMARY KEY,
    country VARCHAR(2) NOT NULL,
    trade_name VARCHAR(100) NOT NULL,
    buy_class_id INTEGER REFERENCES canonical_livestock_classes(id),
    sell_class_id INTEGER REFERENCES canonical_livestock_classes(id),
    typical_duration_months INTEGER,
    typical_weight_gain_kg DECIMAL(8,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(country, trade_name)
);

-- 6. Stage Trade Margins (calculated)
CREATE TABLE IF NOT EXISTS stage_trade_margins (
    id SERIAL PRIMARY KEY,
    stage_trade_id INTEGER REFERENCES stage_trades(id),
    calculation_date DATE NOT NULL,
    buy_price_per_kg_usd DECIMAL(10,4),
    sell_price_per_kg_usd DECIMAL(10,4),
    gross_margin_per_head_usd DECIMAL(10,2),
    net_margin_per_head_usd DECIMAL(10,2),
    cost_of_gain_per_kg_usd DECIMAL(10,4),
    roi_percent DECIMAL(8,2),
    data_quality VARCHAR(20) DEFAULT 'actual',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 7. Herd Inventory Templates
CREATE TABLE IF NOT EXISTS herd_inventory (
    id SERIAL PRIMARY KEY,
    portfolio_name VARCHAR(100) NOT NULL,
    country VARCHAR(2) NOT NULL,
    canonical_class_id INTEGER REFERENCES canonical_livestock_classes(id),
    head_count INTEGER NOT NULL DEFAULT 0,
    avg_weight_kg DECIMAL(8,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 8. Signals
CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    country VARCHAR(2) NOT NULL,
    signal_type VARCHAR(50) NOT NULL,
    canonical_class_id INTEGER REFERENCES canonical_livestock_classes(id),
    signal_value DECIMAL(10,4),
    signal_label VARCHAR(50),
    detail JSONB,
    calculation_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 9. Portfolio Snapshots
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id SERIAL PRIMARY KEY,
    portfolio_name VARCHAR(100) NOT NULL,
    snapshot_date DATE NOT NULL,
    total_head INTEGER,
    total_ae DECIMAL(10,2),
    total_value_usd DECIMAL(14,2),
    total_value_local DECIMAL(14,2),
    local_currency VARCHAR(3),
    detail JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_lcm_country ON livestock_class_mapping(country);
CREATE INDEX IF NOT EXISTS idx_lcm_canonical ON livestock_class_mapping(canonical_class_id);
CREATE INDEX IF NOT EXISTS idx_cost_country ON cost_benchmarks(country);
CREATE INDEX IF NOT EXISTS idx_stage_trades_country ON stage_trades(country);
CREATE INDEX IF NOT EXISTS idx_stm_trade ON stage_trade_margins(stage_trade_id);
CREATE INDEX IF NOT EXISTS idx_stm_date ON stage_trade_margins(calculation_date);
CREATE INDEX IF NOT EXISTS idx_signals_country ON signals(country);
CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(calculation_date);
CREATE INDEX IF NOT EXISTS idx_signals_type ON signals(signal_type);
CREATE INDEX IF NOT EXISTS idx_portfolio_snap ON portfolio_snapshots(portfolio_name, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_herd_portfolio ON herd_inventory(portfolio_name);

"""

# Canonical classes: the universal livestock class taxonomy
CANONICAL_CLASSES = [
    # (canonical_name, species, stage, sex, weight_range_kg, description, ae_equivalent)
    ('Weaner Steer', 'cattle', 'weaner', 'male', '200-280', 'Weaned male calf', 0.50),
    ('Weaner Heifer', 'cattle', 'weaner', 'female', '200-280', 'Weaned female calf', 0.50),
    ('Yearling Steer', 'cattle', 'yearling', 'male', '280-400', 'Yearling male', 0.70),
    ('Yearling Heifer', 'cattle', 'yearling', 'female', '280-400', 'Yearling female', 0.70),
    ('Feeder Steer', 'cattle', 'feeder', 'male', '350-500', 'Backgrounded/feeder steer', 0.85),
    ('Feeder Heifer', 'cattle', 'feeder', 'female', '350-500', 'Backgrounded/feeder heifer', 0.85),
    ('Finished Steer', 'cattle', 'finished', 'male', '500-700', 'Slaughter-ready steer', 1.20),
    ('Finished Heifer', 'cattle', 'finished', 'female', '450-600', 'Slaughter-ready heifer', 1.10),
    ('Heavy Steer', 'cattle', 'finished', 'male', '600-800', 'Heavy export steer', 1.40),
    ('Cull Cow', 'cattle', 'cull', 'female', '400-550', 'Cull/manufacturing cow', 1.00),
    ('Breeding Cow', 'cattle', 'breeding', 'female', '450-600', 'Breeding female', 1.00),
    ('PTIC Heifer', 'cattle', 'breeding', 'female', '400-550', 'Pregnant tested in-calf heifer', 1.00),
    ('Bull', 'cattle', 'breeding', 'male', '600-900', 'Breeding bull', 1.50),
    ('Veal Calf', 'cattle', 'veal', 'mixed', '100-200', 'Veal/bobby calf', 0.30),
]

# Source class -> canonical mapping for all countries
CLASS_MAPPINGS = [
    # Australia (AU) - MLA source classes
    ('AU', 'Heavy Steers', 'Heavy Steer', 'MLA', 'high'),
    ('AU', 'Medium Steers', 'Finished Steer', 'MLA', 'high'),
    ('AU', 'Heavy Cows', 'Cull Cow', 'MLA', 'high'),
    ('AU', 'Medium Cows', 'Cull Cow', 'MLA', 'medium'),
    ('AU', 'Vealer Steers', 'Weaner Steer', 'MLA', 'high'),
    ('AU', 'Vealer Heifers', 'Weaner Heifer', 'MLA', 'high'),
    ('AU', 'Yearling Steers', 'Yearling Steer', 'MLA', 'high'),
    ('AU', 'Yearling Heifers', 'Yearling Heifer', 'MLA', 'high'),
    ('AU', 'Light Steers', 'Feeder Steer', 'MLA', 'medium'),
    ('AU', 'Light Heifers', 'Feeder Heifer', 'MLA', 'medium'),
    ('AU', 'Grown Steers', 'Finished Steer', 'MLA', 'high'),
    ('AU', 'Grown Heifers', 'Finished Heifer', 'MLA', 'high'),
    ('AU', 'EYCI', 'Yearling Steer', 'MLA', 'medium'),
    # New Zealand (NZ)
    ('NZ', 'Prime Steer', 'Finished Steer', 'NZX/MPI', 'high'),
    ('NZ', 'Prime Heifer', 'Finished Heifer', 'NZX/MPI', 'high'),
    ('NZ', 'Manufacturing Cow', 'Cull Cow', 'NZX/MPI', 'high'),
    ('NZ', 'Manufacturing Bull', 'Bull', 'NZX/MPI', 'medium'),
    ('NZ', 'Store Cattle', 'Feeder Steer', 'NZX/MPI', 'medium'),
    ('NZ', 'Bobby Calf', 'Veal Calf', 'NZX/MPI', 'high'),
    # Brazil (BR) - CEPEA / B3
    ('BR', 'Boi Gordo', 'Finished Steer', 'CEPEA', 'high'),
    ('BR', 'Vaca Gorda', 'Cull Cow', 'CEPEA', 'high'),
    ('BR', 'Bezerro', 'Weaner Steer', 'CEPEA', 'high'),
    ('BR', 'Bezerra', 'Weaner Heifer', 'CEPEA', 'high'),
    ('BR', 'Novilho', 'Yearling Steer', 'CEPEA', 'high'),
    ('BR', 'Garrote', 'Feeder Steer', 'CEPEA', 'high'),
    # Argentina (AR) - Rosgan/Liniers
    ('AR', 'Novillito', 'Yearling Steer', 'Rosgan', 'high'),
    ('AR', 'Novillo', 'Finished Steer', 'Rosgan', 'high'),
    ('AR', 'Vaquillona', 'Yearling Heifer', 'Rosgan', 'high'),
    ('AR', 'Vaca', 'Cull Cow', 'Rosgan', 'high'),
    ('AR', 'Ternero', 'Weaner Steer', 'Rosgan', 'high'),
    ('AR', 'Ternera', 'Weaner Heifer', 'Rosgan', 'high'),
    # Uruguay (UY) - INAC
    ('UY', 'Novillo Gordo', 'Finished Steer', 'INAC', 'high'),
    ('UY', 'Novillo', 'Finished Steer', 'INAC', 'high'),
    ('UY', 'Vaca', 'Cull Cow', 'INAC', 'high'),
    ('UY', 'Vaquillona', 'Yearling Heifer', 'INAC', 'high'),
    ('UY', 'Ternero', 'Weaner Steer', 'INAC', 'high'),
    # Paraguay (PY) - ARP
    ('PY', 'Novillo Gordo', 'Finished Steer', 'ARP', 'high'),
    ('PY', 'Vaca', 'Cull Cow', 'ARP', 'high'),
    ('PY', 'Ternero', 'Weaner Steer', 'ARP', 'high'),
    ('PY', 'Vaquillona', 'Yearling Heifer', 'ARP', 'high'),
    # USA (US) - USDA
    ('US', 'Fed Cattle', 'Finished Steer', 'USDA', 'high'),
    ('US', 'Feeder Cattle', 'Feeder Steer', 'USDA', 'high'),
    ('US', 'Feeder Calves', 'Weaner Steer', 'USDA', 'high'),
    ('US', 'Cull Cows', 'Cull Cow', 'USDA', 'high'),
    ('US', 'Bred Heifers', 'PTIC Heifer', 'USDA', 'high'),
]

# Stage trade definitions per country
STAGE_TRADES = [
    # (country, trade_name, buy_canonical, sell_canonical, duration_months, weight_gain_kg)
    ('AU', 'Weaner to Yearling', 'Weaner Steer', 'Yearling Steer', 8, 120),
    ('AU', 'Yearling to Finished', 'Yearling Steer', 'Finished Steer', 10, 200),
    ('AU', 'Weaner to Finished', 'Weaner Steer', 'Finished Steer', 18, 320),
    ('AU', 'Backgrounding Heifer', 'Weaner Heifer', 'Yearling Heifer', 8, 100),
    ('BR', 'Bezerro to Boi Gordo', 'Weaner Steer', 'Finished Steer', 24, 300),
    ('BR', 'Garrote to Boi Gordo', 'Feeder Steer', 'Finished Steer', 12, 180),
    ('AR', 'Ternero to Novillito', 'Weaner Steer', 'Yearling Steer', 8, 120),
    ('AR', 'Novillito to Novillo', 'Yearling Steer', 'Finished Steer', 10, 180),
    ('AR', 'Ternero to Novillo', 'Weaner Steer', 'Finished Steer', 18, 300),
    ('UY', 'Ternero to Novillo', 'Weaner Steer', 'Finished Steer', 20, 280),
    ('PY', 'Ternero to Novillo', 'Weaner Steer', 'Finished Steer', 22, 280),
    ('NZ', 'Store to Prime', 'Feeder Steer', 'Finished Steer', 10, 180),
    ('US', 'Calf to Feeder', 'Weaner Steer', 'Feeder Steer', 8, 150),
    ('US', 'Feeder to Fed', 'Feeder Steer', 'Finished Steer', 6, 200),
    ('US', 'Calf to Fed', 'Weaner Steer', 'Finished Steer', 14, 350),
]

# AE/DSE equivalents
AE_EQUIVALENTS = [
    # (canonical_name, ae_value, dse_value, source)
    ('Weaner Steer', 0.50, 4.0, 'NSW DPI'),
    ('Weaner Heifer', 0.50, 4.0, 'NSW DPI'),
    ('Yearling Steer', 0.70, 5.6, 'NSW DPI'),
    ('Yearling Heifer', 0.70, 5.6, 'NSW DPI'),
    ('Feeder Steer', 0.85, 6.8, 'NSW DPI'),
    ('Feeder Heifer', 0.85, 6.8, 'NSW DPI'),
    ('Finished Steer', 1.20, 9.6, 'NSW DPI'),
    ('Finished Heifer', 1.10, 8.8, 'NSW DPI'),
    ('Heavy Steer', 1.40, 11.2, 'NSW DPI'),
    ('Cull Cow', 1.00, 8.0, 'NSW DPI'),
    ('Breeding Cow', 1.00, 8.0, 'NSW DPI'),
    ('PTIC Heifer', 1.00, 8.0, 'NSW DPI'),
    ('Bull', 1.50, 12.0, 'NSW DPI'),
    ('Veal Calf', 0.30, 2.4, 'NSW DPI'),
]


def run_migration():
    conn = get_connection()
    cur = conn.cursor()
    try:
        print("[1/5] Creating intelligence engine tables...")
        cur.execute(CREATE_TABLES_SQL)
        conn.commit()
        print("  -> 9 tables created successfully")

        print("[2/5] Seeding canonical livestock classes...")
        for c in CANONICAL_CLASSES:
            cur.execute("""
                INSERT INTO canonical_livestock_classes
                    (canonical_name, species, stage, sex, weight_range_kg, description, ae_equivalent)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (canonical_name) DO NOTHING
            """, c)
        conn.commit()
        print(f"  -> {len(CANONICAL_CLASSES)} canonical classes seeded")

        print("[3/5] Seeding livestock class mappings...")
        # Build canonical name -> id lookup
        cur.execute("SELECT id, canonical_name FROM canonical_livestock_classes")
        canon_map = {row[1]: row[0] for row in cur.fetchall()}
        mapped = 0
        for country, source_class, canonical_name, data_source, confidence in CLASS_MAPPINGS:
            cid = canon_map.get(canonical_name)
            if cid:
                cur.execute("""
                    INSERT INTO livestock_class_mapping
                        (country, source_class, canonical_class_id, data_source, confidence)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (country, source_class, data_source) DO NOTHING
                """, (country, source_class, cid, data_source, confidence))
                mapped += 1
        conn.commit()
        print(f"  -> {mapped} class mappings seeded")

        print("[4/5] Seeding stage trades...")
        for country, name, buy_canon, sell_canon, dur, gain in STAGE_TRADES:
            buy_id = canon_map.get(buy_canon)
            sell_id = canon_map.get(sell_canon)
            if buy_id and sell_id:
                cur.execute("""
                    INSERT INTO stage_trades
                        (country, trade_name, buy_class_id, sell_class_id,
                         typical_duration_months, typical_weight_gain_kg)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (country, trade_name) DO NOTHING
                """, (country, name, buy_id, sell_id, dur, gain))
        conn.commit()
        print(f"  -> {len(STAGE_TRADES)} stage trades seeded")

        print("[5/5] Seeding AE/DSE equivalents...")
        for canon_name, ae_val, dse_val, source in AE_EQUIVALENTS:
            cid = canon_map.get(canon_name)
            if cid:
                cur.execute("""
                    INSERT INTO animal_unit_equivalents
                        (canonical_class_id, ae_value, dse_value, source)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (canonical_class_id) DO NOTHING
                """, (cid, ae_val, dse_val, source))
        conn.commit()
        print(f"  -> {len(AE_EQUIVALENTS)} AE equivalents seeded")

        # Verify
        tables = ['canonical_livestock_classes', 'livestock_class_mapping',
                  'cost_benchmarks', 'animal_unit_equivalents', 'stage_trades',
                  'stage_trade_margins', 'herd_inventory', 'signals', 'portfolio_snapshots']
        print("\n=== Migration Summary ===")
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            count = cur.fetchone()[0]
            print(f"  {t}: {count} rows")
        print("\nIntelligence engine migration complete!")

    except Exception as e:
        conn.rollback()
        print(f"Migration failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_migration()
