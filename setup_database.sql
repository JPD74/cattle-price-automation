-- Cattle Price Automation Database Schema
-- Run this SQL in your Railway PostgreSQL database

-- Table 1: Cattle Prices
CREATE TABLE IF NOT EXISTS cattle_prices (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    country VARCHAR(2) NOT NULL,  -- AU, NZ, BR, PY, UY
    region VARCHAR(100),
    livestock_class VARCHAR(100) NOT NULL,
    weight_category VARCHAR(50),
    price_per_kg_local DECIMAL(10,2) NOT NULL,
    price_per_kg_usd DECIMAL(10,2) NOT NULL,
    local_currency VARCHAR(3) NOT NULL,  -- AUD, NZD, BRL, PYG, UYU
    data_source VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table 2: Exchange Rates
CREATE TABLE IF NOT EXISTS exchange_rates (
    id SERIAL PRIMARY KEY,
    currency VARCHAR(3) NOT NULL,
    usd_rate DECIMAL(10,6) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(currency, timestamp)
);

-- Table 3: Livestock Classes (Reference Data)
CREATE TABLE IF NOT EXISTS livestock_classes (
    id SERIAL PRIMARY KEY,
    country VARCHAR(2) NOT NULL,
    class_name VARCHAR(100) NOT NULL,
    description TEXT,
    UNIQUE(country, class_name)
);

-- Table 4: Data Collection Log
CREATE TABLE IF NOT EXISTS collection_log (
    id SERIAL PRIMARY KEY,
    country VARCHAR(2) NOT NULL,
    data_source VARCHAR(100) NOT NULL,
    records_collected INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL,  -- success, failed, partial
    error_message TEXT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_cattle_prices_country ON cattle_prices(country);
CREATE INDEX idx_cattle_prices_timestamp ON cattle_prices(timestamp);
CREATE INDEX idx_cattle_prices_class ON cattle_prices(livestock_class);
CREATE INDEX idx_exchange_rates_currency ON exchange_rates(currency);

-- Insert initial livestock classes for Australia
INSERT INTO livestock_classes (country, class_name, description) VALUES
('AU', 'Heavy Steers', 'Finished steers over 500kg'),
('AU', 'Medium Steers', 'Steers 400-500kg'),
('AU', 'Heavy Cows', 'Cows over 500kg'),
('AU', 'Medium Cows', 'Cows 400-500kg')
ON CONFLICT (country, class_name) DO NOTHING;

COMMIT;
