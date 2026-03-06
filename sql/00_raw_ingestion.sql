-- =====================================================
-- STEP 1: Create schemas
-- =====================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marts;



-- =====================================================
-- RAW LAYER
-- Dirty ingestion tables (simulate product event logs)
-- =====================================================

DROP TABLE IF EXISTS raw.raw_events;

CREATE TABLE raw.raw_events (
    event_id TEXT,
    user_id TEXT,
    event_name TEXT,
    event_time TIMESTAMPTZ,
    feature_name TEXT,
    session_id TEXT,
    device_type TEXT,
    country TEXT,
    properties_json JSONB,
    ingested_at TIMESTAMPTZ DEFAULT now()
);



-- =====================================================
-- DIMENSION TABLES
-- =====================================================

DROP TABLE IF EXISTS core.dim_users;

CREATE TABLE core.dim_users (
    user_id TEXT PRIMARY KEY,
    signup_at TIMESTAMPTZ NOT NULL,
    industry TEXT,
    company_size TEXT,
    country TEXT,
    acquisition_channel TEXT,
    is_internal BOOLEAN DEFAULT FALSE
);


DROP TABLE IF EXISTS core.dim_plans;

CREATE TABLE core.dim_plans (
    plan_id TEXT PRIMARY KEY,
    plan_name TEXT,
    monthly_price NUMERIC(10,2),
    seat_based BOOLEAN DEFAULT FALSE
);


DROP TABLE IF EXISTS core.dim_features;

CREATE TABLE core.dim_features (
    feature_name TEXT PRIMARY KEY,
    launched_at TIMESTAMPTZ,
    rollout_start_at TIMESTAMPTZ,
    rollout_pct_initial NUMERIC(5,2),
    notes TEXT
);



-- =====================================================
-- FACT TABLES
-- =====================================================

DROP TABLE IF EXISTS core.fct_subscriptions;

CREATE TABLE core.fct_subscriptions (
    subscription_id TEXT PRIMARY KEY,
    user_id TEXT REFERENCES core.dim_users(user_id),
    plan_id TEXT REFERENCES core.dim_plans(plan_id),
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    status TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);



DROP TABLE IF EXISTS core.fct_billing_invoices;

CREATE TABLE core.fct_billing_invoices (
    invoice_id TEXT PRIMARY KEY,
    user_id TEXT REFERENCES core.dim_users(user_id),
    invoice_period_start TIMESTAMPTZ,
    invoice_period_end TIMESTAMPTZ,
    amount_usd NUMERIC(10,2),
    issued_at TIMESTAMPTZ,
    paid_at TIMESTAMPTZ,
    status TEXT
);
