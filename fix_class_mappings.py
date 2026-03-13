"""Fix class mappings to match actual DB source class names."""
import os
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")

# Additional mappings using ACTUAL class names from cattle_prices table
# These supplement the original mappings which used idealized names
NEW_MAPPINGS = [
    # Brazil - actual CEPEA class names in DB
    ('BR', 'Boi Gordo (Fed Cattle)', 'Finished Steer', 'CEPEA', 'high'),
    ('BR', 'Vaca Gorda (Fat Cow)', 'Cull Cow', 'CEPEA', 'high'),
    ('BR', 'Bezerro (Calf)', 'Weaner Steer', 'CEPEA', 'high'),
    ('BR', 'Boi Magro (Lean Cattle)', 'Feeder Steer', 'CEPEA', 'high'),
    # Uruguay - actual INAC class names in DB
    ('UY', 'Novillo Gordo (Fed Steer)', 'Finished Steer', 'INAC', 'high'),
    ('UY', 'Novillo (Steer) Premium Grade', 'Finished Steer', 'INAC', 'medium'),
    ('UY', 'Vaca (Cow) Standard Grade', 'Cull Cow', 'INAC', 'high'),
    ('UY', 'Ternero (Calf) Export Quality', 'Weaner Steer', 'INAC', 'high'),
    ('UY', 'Toro (Bull) Industrial Grade', 'Bull', 'INAC', 'high'),
    # Paraguay - actual ARP class names in DB
    ('PY', 'Novillo (Young Steer)', 'Finished Steer', 'ARP', 'high'),
    ('PY', 'Vaca Gorda (Fat Cow)', 'Cull Cow', 'ARP', 'high'),
    ('PY', 'Ternero (Calf)', 'Weaner Steer', 'ARP', 'high'),
    ('PY', 'Toro (Bull)', 'Bull', 'ARP', 'high'),
    # New Zealand - actual NZX/MPI class names in DB
    ('NZ', 'Steer Prime P2', 'Finished Steer', 'NZX/MPI', 'high'),
    ('NZ', 'Heifer Prime P2', 'Finished Heifer', 'NZX/MPI', 'high'),
    ('NZ', 'Cow Prime P2', 'Cull Cow', 'NZX/MPI', 'high'),
    ('NZ', 'Steer M Grade', 'Feeder Steer', 'NZX/MPI', 'medium'),
    ('NZ', 'Bull', 'Bull', 'NZX/MPI', 'high'),
    # Australia - additional actual MLA class names in DB
    ('AU', 'Cows', 'Cull Cow', 'MLA', 'high'),
    ('AU', 'Bulls', 'Bull', 'MLA', 'high'),
    ('AU', 'Feeder Steers', 'Feeder Steer', 'MLA', 'high'),
    ('AU', 'EYCI (Young Cattle)', 'Yearling Steer', 'MLA', 'high'),
    ('AU', 'EYCI - Young Cattle', 'Yearling Steer', 'MLA', 'high'),
    ('AU', 'Grainfed Cattle', 'Finished Steer', 'MLA', 'medium'),
    # US - additional actual USDA class names in DB
    ('US', 'Choice Steers (Fed Cattle)', 'Finished Steer', 'USDA', 'high'),
    ('US', 'ALL BEEF TYPE', 'Finished Steer', 'USDA', 'low'),
]

def run():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    # Get canonical name -> id lookup
    cur.execute("SELECT id, canonical_name FROM canonical_livestock_classes")
    canon_map = {row[1]: row[0] for row in cur.fetchall()}
    added = 0
    for country, source_class, canonical_name, data_source, confidence in NEW_MAPPINGS:
        cid = canon_map.get(canonical_name)
        if cid:
            cur.execute("""
                INSERT INTO livestock_class_mapping
                    (country, source_class, canonical_class_id, data_source, confidence)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (country, source_class, data_source) DO NOTHING
            """, (country, source_class, cid, data_source, confidence))
            if cur.rowcount > 0:
                added += 1
                print(f"  + {country}: {source_class} -> {canonical_name}")
    conn.commit()
    # Report coverage
    cur.execute("""
        SELECT m.country, COUNT(DISTINCT m.source_class), COUNT(DISTINCT c.canonical_name)
        FROM livestock_class_mapping m
        JOIN canonical_livestock_classes c ON c.id = m.canonical_class_id
        GROUP BY m.country ORDER BY m.country
    """)
    print(f"\nAdded {added} new mappings.")
    print("\n=== Mapping Coverage ===")
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]} source classes -> {r[2]} canonical classes")
    cur.close()
    conn.close()

if __name__ == "__main__":
    run()
