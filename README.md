# SkyPoints Global Airline Loyalty Program Data Pipeline

A production-grade, highly scalable PySpark ETL/ELT data pipeline (with Delta Lake / Parquet support and Snowflake-compatible SQL DDLs) designed to process **billions of records daily** across daily member profile feeds and semi-structured partner airline redemption streams.

---

## 1. Architecture & Medallion Design

The pipeline follows the enterprise **Medallion Architecture (Bronze → Silver → Gold)**:

```
                            DAILY INGESTION FEEDS
                                      │
            ┌─────────────────────────┴─────────────────────────┐
            ▼                                                   ▼
   [Pipe-Delimited Members Feed]                      [Semi-Structured JSON Redemptions]
   (data/raw/members_*.txt)                           (data/raw/redemptions_*.json)
            │                                                   │
            ▼                                                   ▼
 ┌────────────────────────────────────┐              ┌────────────────────────────────────┐
 │       BRONZE: RAW LANDING          │              │       BRONZE: RAW LANDING          │
 │ - raw.raw_members                  │              │ - raw.raw_redemptions (VARIANT)    │
 └─────────────────┬──────────────────┘              └─────────────────┬──────────────────┘
                   │                                                   │
                   ▼                                                   ▼
 ┌────────────────────────────────────┐              ┌────────────────────────────────────┐
 │       SILVER: STAGING & DQ         │              │       SILVER: STAGING & FLATTEN    │
 │ - staging.stg_members              │              │ - staging.stg_redemptions          │
 │   • Country ISO-3 standardization  │              │   • Exploded nested transactions   │
 │   • Derived: age (from DOB)        │              │   • SHA-256 surrogate key          │
 │   • Derived: stale_member (>90d)   │              │   • Key normalization (member_id)  │
 │ - staging.quarantine_members       │              └─────────────────┬──────────────────┘
 │   • Segregated defective records   │                                │
 └─────────────────┬──────────────────┘                                │
                   │                                                   │
                   ▼                                                   │
 ┌────────────────────────────────────┐                                │
 │       GOLD: PER-COUNTRY ROUTING    │                                │
 │ - gold.table_india                 │                                │
 │ - gold.table_usa                   │                                │
 │ - gold.table_philippines           │                                │
 │ - gold.table_canada                │                                │
 │ - gold.table_australia             │                                │
 │   • SCD-1 Latest Record Wins       │                                │
 │   • Relocation reconciliation      │                                │
 └─────────────────┬──────────────────┘                                │
                   │                                                   │
                   └─────────────────────────┬─────────────────────────┘
                                             ▼
                             ┌────────────────────────────────┐
                             │   GOLD: ENRICHED ANALYTICS     │
                             │ - 1:1 Skew-resistant join      │
                             │ - Member lifetime miles summary│
                             └────────────────────────────────┘
```

---

## 2. Input Feeds & Anomaly Specifications

### A. Pipe-Delimited Member Profiles (`data/raw/members_YYYYMMDD_HHMMSSTT.txt`)
- **Header:** `|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|Agent_Name|State|Country|DOB|Is_Active`
- **Known Anomalies Handled in Step 1 Sample Data:**
  1. **Schema Drift (Missing Post Code):** The specification mentions `Post Code` at position 9, but actual source feeds omit it (10 detail tokens).
  2. **Delimiter & Whitespace Noise:** Inconsistent trailing pipes (`...|A|`), missing leading pipes (`D|Nora...`), and whitespace padding (`|D | Elena | 223457|...`).
  3. **DOB Leading-Zero Preservation:** Values like `03051985` lose leading zeros (`3051985`) if cast to integer. Parsed strictly as zero-padded 8-character string with documented **`MMddyyyy`** (March 5, 1985) assumption.
  4. **Country Code Heterogeneity:** Mixes ISO-2, ISO-3, and informal names (`USA`, `IND`, `PHIL`, `CAN`, `AU`).
  5. **Member Relocation:** Members moving across countries (e.g. `Arjun` moving from `IND` to `USA`) with newer flight dates.
  6. **Intra-feed Duplicates:** Multiple records for the same `Member_Id` where latest `Last_Flight_Date` wins.
  7. **Corrupt / Defective Rows:** Null mandatory fields (`Member_Id`, `Member_Name`) or unparseable dates routed to `staging.quarantine_members`.

### B. Semi-Structured JSON Redemptions (`data/raw/redemptions_YYYYMMDD.json`)
- **Structure:** JSON Lines with top-level `feed_date`, variant member keys (`"member id"` vs `"member_id"`), and nested `redemptions` arrays.
- **Edge Cases:** Duplicate transaction IDs (`txn_id`), multiple partner airlines (`AeroLink`, `SkyPoints`, `IndiSky`, etc.), and transaction status tracking (`COMPLETED`, `PENDING`).

---

## 3. Warehouse SQL DDLs (`sql/ddl_tables.sql`)

Snowflake-compatible DDLs are organized across three schemas:
1. `raw`:
   - `raw.raw_members`: Raw string columns preserving payloads and lineage metadata.
   - `raw.raw_redemptions`: Semi-structured `VARIANT` table clustered by ingestion day.
2. `staging`:
   - `staging.stg_members`: Cleansed types, business key `member_id`, and virtual derived columns `age` (`FLOOR(DATEDIFF('month', dob, CURRENT_DATE()) / 12)`) and `stale_member` (`DATEDIFF('day', last_flight_date, CURRENT_DATE()) > 90`). Clustered by `(country_code, member_id)`.
   - `staging.quarantine_members`: Segregated error records with `error_reason`, `raw_payload`, and `ingested_at`.
   - `staging.stg_redemptions`: Exploded tabular transactions with SHA-256 surrogate key.
3. `gold`:
   - Country tables (`gold.table_india`, `gold.table_usa`, `gold.table_philippines`, `gold.table_canada`, `gold.table_australia`) clustered by `member_id` for billion-row partition pruning.

---

## 4. Five-Step Implementation Roadmap

- [x] **Step 1: Scaffolding, Sample Raw Data & Warehouse DDLs** (Current step)
- [ ] **Step 2: Resilient Raw Ingestion Engine (Bronze Layer)**
- [ ] **Step 3: Staging Transformations, Derived Columns & DQ Quarantine (Silver Layer)**
- [ ] **Step 4: Country Routing & Latest-Record-Wins SCD-1 Relocation (Gold Layer)**
- [ ] **Step 5: JSON Redemption Explosion, Member Join, Pipeline Orchestrator & CLI**
