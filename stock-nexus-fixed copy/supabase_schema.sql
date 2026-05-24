-- ════════════════════════════════════════════════════════════════
-- NEXUS STOCK AI — Supabase Database Schema
-- ════════════════════════════════════════════════════════════════
-- Run this ONCE in your Supabase SQL Editor:
-- https://app.supabase.com → SQL Editor → New Query → paste & run
-- ════════════════════════════════════════════════════════════════

-- NGX daily price history
-- One row per ticker per day — UNIQUE constraint auto-deduplicates
CREATE TABLE IF NOT EXISTS ngx_price_history (
    id         BIGSERIAL PRIMARY KEY,
    ticker     TEXT          NOT NULL,
    date       DATE          NOT NULL,
    close      DECIMAL(18,4) NOT NULL,
    volume     BIGINT        DEFAULT 0,
    updated_at TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE (ticker, date)          -- prevents duplicate entries from poll restarts
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_ngx_ticker_date
    ON ngx_price_history (ticker, date);

CREATE INDEX IF NOT EXISTS idx_ngx_date
    ON ngx_price_history (date DESC);

-- Row Level Security (enable for production apps)
ALTER TABLE ngx_price_history ENABLE ROW LEVEL SECURITY;

-- Allow read/write from the anon key (used by the app)
-- For production: replace with a more restrictive policy
CREATE POLICY IF NOT EXISTS "Allow all operations"
    ON ngx_price_history
    FOR ALL
    USING (true)
    WITH CHECK (true);

-- Verify: run this to check setup
-- SELECT COUNT(*) as total_rows, COUNT(DISTINCT ticker) as tickers
-- FROM ngx_price_history;
