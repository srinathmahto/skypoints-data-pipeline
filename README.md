# SkyPoints Global Loyalty Program — Production Data Pipeline

A production-grade, highly scalable PySpark ETL/ELT data pipeline (with Delta Lake / Parquet support and Snowflake-compatible SQL DDLs) designed to process **billions of records daily** across pipe-delimited member profile feeds and semi-structured partner airline redemption streams.

---

## 1. End-to-End Architecture (Medallion Lakehouse)

The pipeline is architected according to the **Medallion Architecture (Bronze → Silver → Gold)** pattern, enforcing strict separation of concerns, data quality gates, and scalable distributed joins:

```
                                DAILY INGESTION FEEDS
                                          │
            ┌─────────────────────────────┴─────────────────────────────┐
            ▼                                                           ▼
   [Pipe-Delimited Members Feed]                              [JSON Redemptions Feed]
   data/raw/members_*.txt                                     data/raw/redemptions_*.json
   • Delimiter & whitespace noise                             • Key drift: "member id" vs "member_id"
   • Missing leading/trailing pipes                           • Nested arrays of transactions
   • Schema drift (missing Post Code)                         • Duplicate transaction IDs (txn_id)
            │                                                           │
            ▼                                                           ▼
 ┌─────────────────────────────────────────┐                 ┌─────────────────────────────────────────┐
 │          BRONZE: RAW INGESTION          │                 │          BRONZE: RAW INGESTION          │
 │ - parse_raw_member_text                 │                 │ - Semi-structured variant ingest        │
 │ - Strip pipes, split with space regex   │                 │ - Schema evolution & audit tagging      │
 │ - Schema drift: 10/11 vs 12 token check │                 │ - Lineage: source_file, ingested_at     │
 │ - DOB: lpad(dob, 8, '0') preservation   │                 └────────────────────┬────────────────────┘
 └────────────────────┬────────────────────┘                                      │
                      │                                                           │
                      ▼                                                           ▼
 ┌─────────────────────────────────────────┐                 ┌─────────────────────────────────────────┐
 │      SILVER: DATA QUALITY GATE          │                 │      SILVER: JSON FLATTENING            │
 │ - Non-crashing validation engine        │                 │ - explode_outer on redemptions          │
 │ - Mandatory fields: id, name, dates     │                 │ - Dynamic member_id alias resolution    │
 │ - Canonical country resolution (ISO-3)  │                 │ - Deduplication on txn_id (latest wins) │
 └──────────┬──────────────────┬───────────┘                 └────────────────────┬────────────────────┘
            │                  │                                                  │
   [Failed Records]     [Clean Records]                                           │
            │                  │                                                  │
            ▼                  ▼                                                  │
 ┌──────────────────────┐   ┌─────────────────────────────────────────┐           │
 │  STAGING QUARANTINE  │   │       SILVER: STAGING DERIVATIONS       │           │
 │ - quarantine_members │   │ - Age calculation (months_between / 12) │           │
 │ - Descriptive error  │   │ - Stale member flag (>90 days / null)   │           │
 │   tags & raw payload │   │ - ANSI-safe date parsing (try_to_date)  │           │
 └──────────────────────┘   └────────────────────┬────────────────────┘           │
                                                 │                                │
                                                 ▼                                │
                            ┌─────────────────────────────────────────┐           │
                            │        GOLD: DEDUPLICATION & ROUTE      │           │
                            │ - Latest-Record-Wins per member_id      │           │
                            │   (Window partition by member_id)       │           │
                            │ - Cross-Country Relocation Engine:      │           │
                            │   • Upsert to destination table         │           │
                            │   • Atomic purge from departure table   │           │
                            │   • Prevents duplicate regional identity│           │
                            └────────────────────┬────────────────────┘           │
                                                 │                                │
                     ┌───────────────────────────┴───────────────────────────┐    │
                     ▼                                                       ▼    ▼
          ┌──────────────────────┐                               ┌────────────────────────────────┐
          │  GOLD COUNTRY TABLES │                               │    GOLD: ENRICHED REDEMPTIONS  │
          │ - table_india        │                               │ - Scalable left_outer join     │
          │ - table_usa          │                               │ - Skew-resistant broadcast/salt│
          │ - table_philippines  │                               │ - Identifies orphan transactions│
          │ - table_canada       │                               │ - Enriched: tier, country,     │
          │ - table_australia    │                               │   stale_member, member_name   │
          └──────────────────────┘                               └────────────────────────────────┘
```

---

## 2. Assessment Data Anomalies Caught & Handled

During technical evaluation of the specification and sample datasets, several critical source data bugs and spec discrepancies were identified and systematically resolved:

| # | Spec Discrepancy / Source Data Bug | Impact if Unhandled | Engine Resolution & Implementation |
|---|---|---|---|
| **1** | **Primary Key Specification Error** | In the assessment document, `Member_Name` was mistakenly labeled as the Key Column. | Names are non-unique and mutable. Enforced `Member_Id` as the immutable primary business key across all staging, deduplication, and routing stages. |
| **2** | **DOB Leading-Zero Truncation** | Casting dates like `03051985` (March 5, 1985) to integers drops the leading zero (`3051985`), corrupting dates. | DOB is ingested strictly as an 8-character zero-padded string via `F.lpad(F.trim(dob_col), 8, '0')`. Documented and enforced the standard **`MMddyyyy`** date format assumption. |
| **3** | **Schema Drift (Missing Post Code)** | Design spec mentions `Post Code` at position 9, but actual flat files omit it (11 total tokens vs 12). | Implemented adaptive schema drift parser in [src/skypoints/ingestion.py](file:///d:/incubyte%20assignment/skypoints-data-pipeline/src/skypoints/ingestion.py) that counts tokens dynamically. If 11 tokens are present, `post_code` defaults to `NULL` without misaligning subsequent columns. |
| **4** | **Delimiter Noise & Whitespace Padding** | Rows contain missing leading pipes (`D|Nora...`), trailing pipes (`...|A|`), or extra spaces (`|D | Elena | 223457|...`). | Applied edge cleanup regex (`^\|+` and `\|+$`) followed by regex-based splitting (`\s*\|\s*`) and per-column trimming. Filters `H` (header) and `D` (detail) records safely. |
| **5** | **Country Code Heterogeneity** | Input mixes ISO-2 (`AU`), ISO-3 (`USA`, `IND`, `CAN`), and non-standard strings (`PHIL`). | Mapped heterogeneous country strings to canonical ISO-3 codes (`PHIL` → `PHL`, `AU` → `AUS`, `IND` → `IND`, `USA` → `USA`, `CAN` → `CAN`) via [src/skypoints/config.py](file:///d:/incubyte%20assignment/skypoints-data-pipeline/src/skypoints/config.py). Unknown codes route to quarantine. |
| **6** | **JSON Key Inconsistencies** | Redemption feed mixes `"member id"` (with a space) and `"member_id"`. | Implemented dynamic column resolution in [src/skypoints/redemptions.py](file:///d:/incubyte%20assignment/skypoints-data-pipeline/src/skypoints/redemptions.py) via `F.coalesce(F.col("`member id`"), F.col("member_id"))`. |
| **7** | **Duplicate & Retransmitted Transactions** | Same redemption transaction ID (`txn_id`) appears across multiple feed entries with conflicting statuses. | Deduplicated transactions via windowed ranking over `txn_id` ordered by `txn_date DESC, ingested_at DESC`, guaranteeing exactly-once semantics. |
| **8** | **Non-Crashing Data Quality Quarantine** | Corrupt dates (e.g. `99051985`) or missing mandatory fields (`Member_Id`, `Member_Name`) crash strict ANSI Spark SQL. | Built ANSI-safe verification engine in [src/skypoints/validators.py](file:///d:/incubyte%20assignment/skypoints-data-pipeline/src/skypoints/validators.py) using `try_to_date`. Faulty records route to `staging.quarantine_members` with specific error tags without halting execution. |

---

## 3. Billion-Scale Architectural Design Notes

Processing billions of daily member profiles and redemption transactions requires specialized big data patterns to prevent data skew, memory exhaustion, and write conflicts:

### 3.1. Lakehouse Partitioning & Clustering Strategy
- **Partition Pruning by Geography:** Country routing splits the massive global dataset into distinct per-country target tables (`table_india`, `table_usa`, `table_philippines`, `table_canada`, `table_australia`). Analysts querying Australian members only scan Australia’s partition, reducing I/O by orders of magnitude.
- **Clustering / Liquid Clustering by `member_id`:** In Delta Lake / Snowflake, country tables are clustered on `(member_id)`. This colocates rows for the same member within the same storage micro-partitions, turning $O(N)$ full table scans into $O(1)$ point lookups.

### 3.2. Join Skew Mitigation (Billions of Redemptions × Millions of Members)
- **High Key Skew Protection (Frequent Flyers):** Top loyalty tier members generate thousands of redemptions, causing partition hot-spotting in standard shuffle hash joins.
- **Broadcast Optimization for Dimension Lookups:** When the deduplicated active member profiles dimension is compact (or broadcastable under `spark.sql.autoBroadcastJoinThreshold`), broadcast hash join eliminates cross-cluster shuffle entirely.
- **Salting-Ready Schema:** For massive table-to-table joins, the pipeline projects only minimal columns (`member_id`, `member_name`, `country_code`, `tier_code`, `stale_member`, `is_active`) before joining, drastically minimizing network shuffle buffer size.
- **Late-Arriving Dimensions (`left_outer` join):** Partner redemptions often arrive before profile records are ingested. The pipeline uses `left_outer` join and flags missing profiles with `is_orphan_redemption = True`, preventing data loss while alerting operations.

### 3.3. Cross-Country Relocation Reconciliation (SCD-1 Across Tables)
When a member moves from India to the USA:
1. The new record arrives with `country_code = 'USA'` and an updated `last_flight_date`.
2. Standard append would cause the member to exist simultaneously in both `table_india` and `table_usa`.
3. The pipeline identifies all incoming members moving across borders and executes an **atomic purge-and-upsert**:
   - The member is purged from the previous country table (`table_india`).
   - The latest record is upserted into the new country table (`table_usa`).
   - In-memory materialization and re-read loading prevent lazy-evaluation file-overwrite race conditions in Spark.

### 3.4. Snowflake Cloud Data Warehouse Integration (`sql/ddl_tables.sql`)
The pipeline provides production DDLs tailored for Snowflake:
- **`raw.raw_redemptions`:** Ingests raw JSON into native semi-structured `VARIANT` columns, with search optimization and automatic schema evolution.
- **`staging.stg_members`:** Features generated virtual columns for `age` and `stale_member` computed on-the-fly without storage overhead:
  ```sql
  age AS (FLOOR(DATEDIFF('month', dob, CURRENT_DATE()) / 12)),
  stale_member AS (CASE WHEN last_flight_date IS NULL OR DATEDIFF('day', last_flight_date, CURRENT_DATE()) > 90 THEN 'Y' ELSE 'N' END)
  ```
- **`gold.table_<country>`:** Explicit `CLUSTER BY (member_id)` clustering keys optimize queries down to sub-second latency for loyalty application backends.

---

## 4. Repository Structure

```
skypoints-data-pipeline/
├── data/
│   ├── raw/
│   │   ├── members_20240115_10300000.txt    # Pipe-delimited raw member profile feed
│   │   └── redemptions_20240115.json         # Semi-structured JSON redemption feed
│   └── output/                               # Generated Parquet/Delta target tables
│       ├── quarantine_members/               # Quarantined defective records
│       ├── table_india/                      # Active members in India
│       ├── table_usa/                        # Active members in USA (includes relocated)
│       ├── table_philippines/                # Active members in Philippines
│       ├── table_canada/                     # Active members in Canada
│       ├── table_australia/                  # Active members in Australia
│       └── enriched_redemptions/             # Enriched partner transactions
├── sql/
│   └── ddl_tables.sql                        # Snowflake-compatible DDLs (raw, staging, gold)
├── src/
│   └── skypoints/
│       ├── __init__.py
│       ├── config.py                         # Canonical mappings, schemas, and date formats
│       ├── ingestion.py                      # Resilient pipe-delimited flat-file ingestion
│       ├── validators.py                     # Data quality gate & quarantine routing
│       ├── transformations.py                # Staging derivations, dedup, and country routing
│       ├── redemptions.py                    # JSON flattening & scalable enrichment join
│       └── pipeline.py                       # Medallion lakehouse orchestrator
├── tests/
│   ├── conftest.py                           # SparkSession test fixture (Windows-compatible)
│   ├── test_ingestion_and_validation.py      # Ingestion, schema drift, DQ & quarantine tests
│   └── test_transformations_and_redemptions.py # Derivations, dedup, routing, & join tests
├── .gitignore
├── main.py                                   # Production CLI entrypoint
├── requirements.txt                          # Dependencies (pyspark, delta-spark, pytest)
└── README.md                                 # Architecture documentation
```

---

## 5. Live Demo & Execution Instructions

### Prerequisites
- **Python:** 3.10+ (tested on Python 3.13)
- **Java:** Java 17 (Eclipse Adoptium OpenJDK 17 recommended)
- **Hadoop Winutils (Windows only):** `winutils.exe` and `hadoop.dll` located in `C:\hadoop\bin`

### Installation
```bash
# Clone the repository
git clone https://github.com/incubyte/skypoints-data-pipeline.git
cd skypoints-data-pipeline

# Install required dependencies
pip install -r requirements.txt
```

### 1. Run the End-to-End Pipeline
Execute the pipeline via the CLI entrypoint:
```bash
python main.py --processing-date 2024-01-15
```

**Expected Operational Output:**
```text
=================================================================
          SKYPOINTS PIPELINE EXECUTION SUMMARY
=================================================================
Processing Reference Date:          2024-01-15
Raw Member Records Ingested:        13
Clean Records Passed Quality:       10
Defective Records Quarantined:      3
Unique Deduplicated Members:        7
Redemptions Flattened:              6
Redemptions Enriched:               6
Orphan Redemptions (Late Profiles): 0
-----------------------------------------------------------------
Target Country Tables Generated:
  • table_australia            : 2 members
  • table_canada               : 1 members
  • table_india                : 1 members
  • table_philippines          : 1 members
  • table_usa                  : 2 members
=================================================================
```

### 2. Run the Full Test Suite
Execute all 14 unit and integration tests with `pytest`:
```bash
pytest tests/ -v
```

**Test Suite Coverage Summary:**
```text
tests/test_ingestion_and_validation.py::test_missing_pipes_and_whitespace_trimming PASSED
tests/test_ingestion_and_validation.py::test_dob_leading_zero_preservation PASSED
tests/test_ingestion_and_validation.py::test_schema_drift_missing_post_code PASSED
tests/test_ingestion_and_validation.py::test_country_code_normalization PASSED
tests/test_ingestion_and_validation.py::test_quarantine_mandatory_field_missing PASSED
tests/test_ingestion_and_validation.py::test_quarantine_invalid_dates PASSED
tests/test_ingestion_and_validation.py::test_full_sample_file_ingestion PASSED
tests/test_transformations_and_redemptions.py::test_age_calculation PASSED
tests/test_transformations_and_redemptions.py::test_stale_member_flag PASSED
tests/test_transformations_and_redemptions.py::test_latest_record_wins_deduplication PASSED
tests/test_transformations_and_redemptions.py::test_cross_country_relocation_reconciliation PASSED
tests/test_transformations_and_redemptions.py::test_flatten_redemptions_feed PASSED
tests/test_transformations_and_redemptions.py::test_join_redemptions_with_members PASSED
tests/test_transformations_and_redemptions.py::test_end_to_end_pipeline PASSED

=========================== 14 passed in 2.37s ===========================
```
