#!/usr/bin/env python3
"""
Cleanup and standardize cattle_prices database.
- Remove old records with NULL weight_category
- Standardize country names to 2-letter codes
- Fix varchar columns that are too small
"""
import os
import psycopg

# Map full country names to standard 2-letter codes
COUNTRY_NAME_MAP = {
    "Australia": "AU",
    "New Zealand": "NZ",
    "Brazil": "BR",
    "Paraguay": "PY",
    "Uruguay": "UY",
    "Argentina": "AR",
    "United States": "US",
    "USA": "US",
}

def cleanup():
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not found")
        return

    try:
        conn = psycopg.connect(DATABASE_URL, sslmode='disable')
        cur = conn.cursor()
        print("Connected to Railway PostgreSQL database")

        # Fix all varchar columns that are too small
        cur.execute("""
            SELECT column_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'cattle_prices'
            AND data_type = 'character varying'
            AND character_maximum_length < 10
        """)
        short_cols = cur.fetchall()
        for col_name, max_len in short_cols:
            print(f"Widening column {col_name} from varchar({max_len}) to varchar(50)")
            cur.execute(f"ALTER TABLE cattle_prices ALTER COLUMN {col_name} TYPE varchar(50)")
        conn.commit()
        print(f"Schema check complete: fixed {len(short_cols)} columns")

        # === STANDARDIZE COUNTRY NAMES TO 2-LETTER CODES ===
        print("\n=== Standardizing country names ===")
        total_fixed = 0
        for full_name, code in COUNTRY_NAME_MAP.items():
            cur.execute("UPDATE cattle_prices SET country = %s WHERE country = %s", (code, full_name))
            count = cur.rowcount
            if count > 0:
                print(f"  Fixed {count} records: '{full_name}' -> '{code}'")
                total_fixed += count

        # Also fix crop_prices table if it exists
        try:
            for full_name, code in COUNTRY_NAME_MAP.items():
                cur.execute("UPDATE crop_prices SET country = %s WHERE country = %s", (code, full_name))
                count = cur.rowcount
                if count > 0:
                    print(f"  Fixed {count} crop records: '{full_name}' -> '{code}'")
                    total_fixed += count
        except Exception:
            pass

        conn.commit()
        print(f"Country standardization complete: {total_fixed} records updated")

        # Count records before cleanup
        cur.execute("SELECT COUNT(*) FROM cattle_prices")
        total_before = cur.fetchone()[0]
        print(f"\nTotal records: {total_before}")

        cur.execute("SELECT COUNT(*) FROM cattle_prices WHERE weight_category IS NULL")
        null_count = cur.fetchone()[0]
        print(f"Records with NULL weight_category: {null_count}")

        if null_count == 0:
            print("No cleanup needed - all records have weight_category")
        else:
            cur.execute("DELETE FROM cattle_prices WHERE weight_category IS NULL")
            deleted = cur.rowcount
            conn.commit()
            print(f"Deleted {deleted} old records with NULL weight_category")

        # Show remaining records summary
        cur.execute("""
            SELECT country, COUNT(*), MIN(timestamp::date), MAX(timestamp::date)
            FROM cattle_prices
            GROUP BY country
            ORDER BY country
        """)
        print("\nRecords by country:")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]} records ({row[2]} to {row[3]})")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    cleanup()
