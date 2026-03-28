-- Complete Schema Migration for Turso
-- This script drops and recreates all tables with complete schema
-- Safe to run - it clears all data but ensures schema is 100% correct

-- Drop existing tables (if they exist)
DROP TABLE IF EXISTS account_pricing_rate_history;
DROP TABLE IF EXISTS account_pricing_profiles;
DROP TABLE IF EXISTS account_custom_names;
DROP TABLE IF EXISTS account_alias_rules;
DROP TABLE IF EXISTS generated_files;
DROP TABLE IF EXISTS retrieval_runs;
DROP TABLE IF EXISTS auth_audit_log;
DROP TABLE IF EXISTS portal_credentials;
DROP TABLE IF EXISTS app_users;

-- Create app_users (base user table)
CREATE TABLE app_users (
    user_id TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user',
    is_active INTEGER NOT NULL DEFAULT 1,
    failed_attempts INTEGER NOT NULL DEFAULT 0,
    locked_until TEXT,
    updated_at TEXT NOT NULL,
    must_change_password INTEGER NOT NULL DEFAULT 0,
    password_changed_at TEXT,
    email TEXT,
    company_name TEXT,
    company_address TEXT,
    company_phone TEXT,
    company_logo_path TEXT
);

-- Create portal_credentials
CREATE TABLE portal_credentials (
    user_id TEXT PRIMARY KEY,
    portal_username TEXT NOT NULL,
    portal_password_encrypted TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Create auth_audit_log
CREATE TABLE auth_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_time TEXT NOT NULL,
    user_id TEXT,
    event_type TEXT NOT NULL,
    outcome TEXT NOT NULL,
    source_ip TEXT,
    details TEXT
);

-- Create retrieval_runs
CREATE TABLE retrieval_runs (
    job_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL,
    last_message TEXT,
    row_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    ended_at TEXT,
    payload_json TEXT
);

-- Create generated_files
CREATE TABLE generated_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    file_key TEXT NOT NULL,
    file_name TEXT NOT NULL,
    mime_type TEXT,
    file_blob BLOB NOT NULL,
    created_at TEXT NOT NULL
);

-- Create account_alias_rules
CREATE TABLE account_alias_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    ref_key TEXT NOT NULL,
    canonical_account TEXT NOT NULL,
    decision_type TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Create account_custom_names
CREATE TABLE account_custom_names (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    account_name TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Create account_pricing_profiles
CREATE TABLE account_pricing_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    file_key TEXT NOT NULL,
    account_name TEXT NOT NULL,
    pricing_mode TEXT NOT NULL,
    fixed_price REAL NOT NULL,
    line_prices_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    currency_code TEXT NOT NULL,
    manual_rate REAL,
    conversion_note TEXT
);

-- Create account_pricing_rate_history
CREATE TABLE account_pricing_rate_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    file_key TEXT NOT NULL,
    account_name TEXT NOT NULL,
    pricing_mode TEXT NOT NULL,
    currency_code TEXT NOT NULL,
    manual_rate REAL,
    conversion_note TEXT,
    report_total REAL NOT NULL,
    created_at TEXT NOT NULL
);

-- Verification (query should show all 9 tables)
-- SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
