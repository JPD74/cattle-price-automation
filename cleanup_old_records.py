#!/usr/bin/env python3
"""
One-time cleanup: Remove old records with NULL weight_category
These are duplicates from before the weight_category fix was applied.
"""
import os
import psycopg

def cleanup():
    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not found")
        return

    try:
        conn = psycopg.connect(DATABASE_URL, sslmode='disable')
        cur = conn.cursor()
        print("Connected to Railway PostgreSQL database")

        # Count records before cleanup
        cur.execute("SELECT COUNT(*) FROM cattle_prices")
        total_before = cur.fetchone()[0]
        print(f"Total records before cleanup: {total_before}")

        cur.execute("SELECT COUNT(*) FROM cattle_prices WHERE weight_category IS NULL")
        null_count = cur.fetchone()[0]
        print(f"Records with NULL weight_category: {null_count}")

        if null_count == 0:
            print("No cleanup needed - all records have weight_category")
            cur.close()
            conn.close()
            return

        # Delete old records with NULL weight_category
        cur.execute("DELETE FROM cattle_prices WHERE weight_category IS NULL")
        deleted = cur.rowcount
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM cattle_prices")
        total_after = cur.fetchone()[0]

        print(f"\nDeleted {deleted} old records with NULL weight_category")
        print(f"Total records after cleanup: {total_after}")

        # Show remaining records summary
        cur.execute("""
            SELECT country, COUNT(*), MIN(timestamp::date), MAX(timestamp::date)
            FROM cattle_prices
            GROUP BY country ORDER BY country
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
