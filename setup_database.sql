-- Cattle Price Automation Database Schema
-- Run this SQL in your Railway PostgreSQL database

-- Table 1: Cattle Prices
CREATE TABLE IF NOT EXISTS cattle_prices (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    country VARCHAR(2) NOT NULL,-- AU, NZ, BR, PY, UY, AR, US
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

-- Table 5: Crop Prices
CREATE TABLE IF NOT EXISTS crop_prices (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    country VARCHAR(2) NOT NULL,  -- AU, NZ, BR, PY, UY, AR, US
    region VARCHAR(100),
    crop_type VARCHAR(100) NOT NULL,  -- Soybeans, Corn, Wheat, Barley, Sorghum, Sugarcane, Cotton
    price_per_tonne_local DECIMAL(12,2) NOT NULL,
    price_per_tonne_usd DECIMAL(12,2) NOT NULL,
    local_currency VARCHAR(3) NOT NULL,
    delivery_period VARCHAR(50),  -- e.g. 'Mar 2026', 'Spot', 'FOB'
    data_source VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crop_prices_country ON crop_prices(country);
CREATE INDEX IF NOT EXISTS idx_crop_prices_timestamp ON crop_prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_crop_prices_crop ON crop_prices(crop_type);

-- Insert livestock classes for Argentina
INSERT INTO livestock_classes (country, class_name, description) VALUES
('AR', 'Novillito', 'Young steer 300-400kg'),
('AR', 'Novillo', 'Steer 400-500kg+'),
('AR', 'Vaquillona', 'Heifer 300-400kg'),
('AR', 'Vaca', 'Cow'),
('AR', 'Ternero', 'Calf')
ON CONFLICT (country, class_name) DO NOTHING;

-- Insert livestock classes for USA
INSERT INTO livestock_classes (country, class_name, description) VALUES
('US', 'Fed Cattle', 'Finished cattle 1100-1400lbs'),
('US', 'Feeder Cattle', 'Feeder cattle 700-900lbs'),
('US', 'Feeder Calves', 'Calves 400-600lbs'),
('US', 'Cull Cows', 'Cull cows'),
('US', 'Bred Heifers', 'Bred heifers')
ON CONFLICT (country, class_name) DO NOTHING;
