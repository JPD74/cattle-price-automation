#!/usr/bin/env python3
"""
Migration: Add farm_profiles table.
Run once to create the table.
"""
import os
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")


def migrate():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS farm_profiles (
            id SERIAL PRIMARY KEY,
            farm_name VARCHAR(200) NOT NULL,
            country VARCHAR(2) NOT NULL,
            region VARCHAR(100),
            hectares DECIMAL(10,2),
            carrying_capacity_ae DECIMAL(10,2),
            default_system VARCHAR(50) DEFAULT 'pasture',
            owner_name VARCHAR(200),
            notes TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_farm_profiles_country ON farm_profiles(country);
        CREATE INDEX IF NOT EXISTS idx_farm_profiles_region ON farm_profiles(country, region);
    """)
    conn.commit()
    print("farm_profiles table created successfully")

    cur.execute("SELECT COUNT(*) FROM farm_profiles")
    print(f"  Current rows: {cur.fetchone()[0]}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    migrate()
