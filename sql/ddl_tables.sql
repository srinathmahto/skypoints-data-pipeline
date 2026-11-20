-- ============================================================================
-- SKYPOINTS GLOBAL AIRLINE LOYALTY PROGRAM: WAREHOUSE DDL SPECIFICATION
-- Database Platform: Snowflake (Compatible with Delta Lake / Lakehouse Metastore)
-- Scale Target: Billions of records daily across heterogeneous feeds
-- Architecture: Medallion Architecture (Raw Landing -> Staging -> Country Gold)
-- ============================================================================

CREATE DATABASE IF NOT EXISTS skypoints_db;
USE DATABASE skypoints_db;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================================
-- 1. RAW / LANDING LAYER
-- ============================================================================

-- Raw landing table for pipe-delimited member profile records
CREATE OR REPLACE TABLE raw.raw_members (
    raw_record_id        NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    source_file          VARCHAR(255) NOT NULL,
    file_row_number      NUMBER NOT NULL,
    raw_payload          VARCHAR NOT NULL,
    record_type          VARCHAR(10),       -- 'H' (Header) or 'D' (Detail)
    member_name_raw      VARCHAR(255),
    member_id_raw        VARCHAR(50),
    enrollment_date_raw  VARCHAR(50),
    last_flight_date_raw VARCHAR(50),
    tier_code_raw        VARCHAR(50),
    agent_name_raw       VARCHAR(255),
    state_raw            VARCHAR(50),
    country_raw          VARCHAR(50),
    post_code_raw        VARCHAR(50),       -- Nullable to tolerate schema drift (missing in sample)
    dob_raw              VARCHAR(50),       -- Stored as string to preserve leading zeros (e.g. 03051985)
    is_active_raw        VARCHAR(10),
    ingested_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (DATE_TRUNC('DAY', ingested_at));

-- Raw semi-structured landing table for partner redemptions feed
CREATE OR REPLACE TABLE raw.raw_redemptions (
    raw_id               NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    source_file          VARCHAR(255) NOT NULL,
    ingested_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    json_payload         VARIANT NOT NULL   -- Semi-structured native JSON payload
)
CLUSTER BY (DATE_TRUNC('DAY', ingested_at));

-- ============================================================================
-- 2. SILVER / STAGING LAYER & QUALITY QUARANTINE
-- ============================================================================

-- Cleaned, typed staging table with derived business columns
-- Derived columns:
--   - age: Computed in calendar years from DOB to CURRENT_DATE()
--   - stale_member: 'Y' if last_flight_date is null or > 90 days ago, else 'N'
CREATE OR REPLACE TABLE staging.stg_members (
    member_id            VARCHAR(50) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(10),
    agent_name           VARCHAR(255),
    state                VARCHAR(20),
    country_code         VARCHAR(10) NOT NULL,    -- Canonical ISO-3 code (e.g. USA, IND, PHL)
    post_code            VARCHAR(20),
    dob                  DATE,                    -- Parsed assuming MMddyyyy format for 8-char string
    is_active            VARCHAR(1) DEFAULT 'A',

    -- Virtual Derived Columns
    age                  NUMBER(3,0) AS (
                             FLOOR(DATEDIFF('month', dob, CURRENT_DATE()) / 12)
                         ),
    stale_member         VARCHAR(1) AS (
                             CASE 
                                 WHEN last_flight_date IS NULL THEN 'Y'
                                 WHEN DATEDIFF('day', last_flight_date, CURRENT_DATE()) > 90 THEN 'Y'
                                 ELSE 'N'
                             END
                         ),

    -- Lineage metadata
    source_file          VARCHAR(255) NOT NULL,
    feed_date            DATE,
    ingested_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Business Primary Key enforcement (Member_Id is the true unique key)
    CONSTRAINT pk_stg_members PRIMARY KEY (member_id) RELY
)
CLUSTER BY (country_code, member_id);

-- Dead-letter quarantine table for rejected / defective records
CREATE OR REPLACE TABLE staging.quarantine_members (
    quarantine_id        NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    source_file          VARCHAR(255) NOT NULL,
    raw_payload          VARCHAR NOT NULL,
    error_reason         VARCHAR(500) NOT NULL,   -- e.g. MISSING_MEMBER_ID, INVALID_DATE, UNKNOWN_COUNTRY
    failed_record_type   VARCHAR(10),
    ingested_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (DATE_TRUNC('DAY', ingested_at), error_reason);

-- Staging table for flattened partner airline redemptions
CREATE OR REPLACE TABLE staging.stg_redemptions (
    redemption_sk        VARCHAR(64) NOT NULL,    -- SHA-256 hash of (member_id, txn_id)
    member_id            VARCHAR(50) NOT NULL,
    feed_date            DATE NOT NULL,
    txn_id               VARCHAR(50) NOT NULL,
    txn_date             DATE NOT NULL,
    partner              VARCHAR(100) NOT NULL,
    miles_redeemed       NUMBER(12, 0) NOT NULL,
    status               VARCHAR(30) NOT NULL,
    ingested_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_stg_redemptions PRIMARY KEY (txn_id) RELY
)
CLUSTER BY (feed_date, member_id);

-- ============================================================================
-- 3. GOLD LAYER: COUNTRY-SPECIFIC TARGET TABLES
-- Optimized for billion-row scale with cluster keys on member_id
-- ============================================================================

-- Target Table: India
CREATE OR REPLACE TABLE gold.table_india (
    member_id            VARCHAR(50) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(10),
    agent_name           VARCHAR(255),
    state                VARCHAR(20),
    country_code         VARCHAR(10) DEFAULT 'IND',
    post_code            VARCHAR(20),
    dob                  DATE,
    is_active            VARCHAR(1),
    age                  NUMBER(3,0),
    stale_member         VARCHAR(1),
    last_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_table_india PRIMARY KEY (member_id) RELY
)
CLUSTER BY (member_id);

-- Target Table: United States
CREATE OR REPLACE TABLE gold.table_usa (
    member_id            VARCHAR(50) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(10),
    agent_name           VARCHAR(255),
    state                VARCHAR(20),
    country_code         VARCHAR(10) DEFAULT 'USA',
    post_code            VARCHAR(20),
    dob                  DATE,
    is_active            VARCHAR(1),
    age                  NUMBER(3,0),
    stale_member         VARCHAR(1),
    last_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_table_usa PRIMARY KEY (member_id) RELY
)
CLUSTER BY (member_id);

-- Target Table: Philippines
CREATE OR REPLACE TABLE gold.table_philippines (
    member_id            VARCHAR(50) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(10),
    agent_name           VARCHAR(255),
    state                VARCHAR(20),
    country_code         VARCHAR(10) DEFAULT 'PHL',
    post_code            VARCHAR(20),
    dob                  DATE,
    is_active            VARCHAR(1),
    age                  NUMBER(3,0),
    stale_member         VARCHAR(1),
    last_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_table_philippines PRIMARY KEY (member_id) RELY
)
CLUSTER BY (member_id);

-- Target Table: Canada
CREATE OR REPLACE TABLE gold.table_canada (
    member_id            VARCHAR(50) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(10),
    agent_name           VARCHAR(255),
    state                VARCHAR(20),
    country_code         VARCHAR(10) DEFAULT 'CAN',
    post_code            VARCHAR(20),
    dob                  DATE,
    is_active            VARCHAR(1),
    age                  NUMBER(3,0),
    stale_member         VARCHAR(1),
    last_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_table_canada PRIMARY KEY (member_id) RELY
)
CLUSTER BY (member_id);

-- Target Table: Australia
CREATE OR REPLACE TABLE gold.table_australia (
    member_id            VARCHAR(50) NOT NULL,
    member_name          VARCHAR(255) NOT NULL,
    enrollment_date      DATE NOT NULL,
    last_flight_date     DATE,
    tier_code            VARCHAR(10),
    agent_name           VARCHAR(255),
    state                VARCHAR(20),
    country_code         VARCHAR(10) DEFAULT 'AUS',
    post_code            VARCHAR(20),
    dob                  DATE,
    is_active            VARCHAR(1),
    age                  NUMBER(3,0),
    stale_member         VARCHAR(1),
    last_updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_table_australia PRIMARY KEY (member_id) RELY
)
CLUSTER BY (member_id);
