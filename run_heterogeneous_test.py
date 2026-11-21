"""
End-to-End Testing Script for Heterogeneous Multi-Country Feeds (IND, USA, AUS).
Demonstrates handling of legacy country-specific schemas, date formatting variations,
data quality defect catching (Jonnathan invalid date), age derivation, stale member detection,
and cross-country relocation reconciliation (Mike: USA -> AUS).
"""

import os
from pathlib import Path
import sys

# Configure environment for Windows JVM and Hadoop native integration
if os.name == "nt":
    candidates = [
        r"C:\Program Files\Eclipse Adoptium\jre-17.0.21.9-hotspot",
        r"C:\Program Files\Eclipse Adoptium\jdk-17",
        r"C:\Program Files\Java\jdk-17",
    ]
    for c in candidates:
        if Path(c).exists():
            os.environ["JAVA_HOME"] = c
            bin_dir = str(Path(c) / "bin")
            if bin_dir not in os.environ.get("PATH", ""):
                os.environ["PATH"] = f"{bin_dir};{os.environ.get('PATH', '')}"
            break

    hadoop_dir = r"C:\hadoop"
    if Path(hadoop_dir).exists():
        os.environ["HADOOP_HOME"] = hadoop_dir
        hbin = str(Path(hadoop_dir) / "bin")
        if hbin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{hbin};{os.environ.get('PATH', '')}"

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Add src to python path
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

from skypoints.transformations import deduplicate_latest_members, route_and_reconcile_country_tables


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("HeterogeneousFeedsE2ETest")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def normalize_date_col(col_expr):
    """
    Normalizes heterogeneous date string representations:
    - ISO format: 'YYYY-MM-DD' (e.g. '2022-01-01')
    - 7 or 8-digit Mddyyyy/MMddyyyy: (e.g. '6152022' -> '06152022' -> 2022-06-15)
    - 8-digit yyyyMMdd
    Returns NULL safely for unparseable dates (e.g. '2021-13-13' with month 13).
    """
    trimmed = F.trim(col_expr.cast(StringType()))
    padded = F.when(
        (F.length(trimmed) >= 7) & (F.length(trimmed) <= 8) & (~trimmed.contains("-")),
        F.lpad(trimmed, 8, "0"),
    ).otherwise(trimmed)

    return F.coalesce(
        F.call_function("try_to_date", padded, F.lit("yyyy-MM-dd")),
        F.call_function("try_to_date", padded, F.lit("MMddyyyy")),
        F.call_function("try_to_date", padded, F.lit("yyyyMMdd")),
    )


def run_heterogeneous_e2e_test(processing_date: str = "2022-09-01"):
    spark = create_spark_session()
    out_dir = Path("data/output_heterogeneous")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 70)
    print("      SKYPOINTS E2E TEST: HETEROGENEOUS REGIONAL COUNTRY FEEDS")
    print("=" * 70)
    print(f"Reference Processing Date: {processing_date}")

    # ---------------------------------------------------------
    # 1. Ingest India Feed (IND.csv)
    # Schema: ID, Name, DOB, TierCode, EnrollmentDate, Individual or Corporate, Flight Date
    # ---------------------------------------------------------
    ind_raw = spark.read.option("header", "true").csv("data/raw/heterogeneous/IND.csv")
    ind_standardized = ind_raw.select(
        F.trim(F.col("ID")).alias("local_id"),
        F.trim(F.col("Name")).alias("member_name"),
        normalize_date_col(F.col("DOB")).alias("dob"),
        F.upper(F.trim(F.col("TierCode"))).alias("tier_code"),
        normalize_date_col(F.col("EnrollmentDate")).alias("enrollment_date"),
        F.col("EnrollmentDate").alias("raw_enrollment_date"),
        normalize_date_col(F.col("Flight Date")).alias("last_flight_date"),
        F.lit("IND").alias("country_code"),
        F.lit("IND.csv").alias("source_file"),
    )

    # ---------------------------------------------------------
    # 2. Ingest USA Feed (USA.csv)
    # Schema: ID, Name, TierCode, EnrollmentDate, FlightDate
    # ---------------------------------------------------------
    usa_raw = spark.read.option("header", "true").csv("data/raw/heterogeneous/USA.csv")
    usa_standardized = usa_raw.select(
        F.trim(F.col("ID")).alias("local_id"),
        F.trim(F.col("Name")).alias("member_name"),
        F.lit(None).cast("date").alias("dob"),
        F.upper(F.trim(F.col("TierCode"))).alias("tier_code"),
        normalize_date_col(F.col("EnrollmentDate")).alias("enrollment_date"),
        F.col("EnrollmentDate").alias("raw_enrollment_date"),
        normalize_date_col(F.col("FlightDate")).alias("last_flight_date"),
        F.lit("USA").alias("country_code"),
        F.lit("USA.csv").alias("source_file"),
    )

    # ---------------------------------------------------------
    # 3. Ingest Australia Feed (AUS.csv)
    # Schema: Unique ID, Member Name, Tier Type, Date of Birth, Date of Enrollment, Date of Flight
    # ---------------------------------------------------------
    aus_raw = spark.read.option("header", "true").csv("data/raw/heterogeneous/AUS.csv")
    aus_standardized = aus_raw.select(
        F.trim(F.col("Unique ID")).alias("local_id"),
        F.trim(F.col("Member Name")).alias("member_name"),
        normalize_date_col(F.col("Date of Birth")).alias("dob"),
        F.upper(F.trim(F.col("Tier Type"))).alias("tier_code"),
        normalize_date_col(F.col("Date of Enrollment")).alias("enrollment_date"),
        F.col("Date of Enrollment").alias("raw_enrollment_date"),
        normalize_date_col(F.col("Date of Flight")).alias("last_flight_date"),
        F.lit("AUS").alias("country_code"),
        F.lit("AUS.csv").alias("source_file"),
    )

    # Combine all raw records
    all_raw = ind_standardized.unionByName(usa_standardized).unionByName(aus_standardized)
    total_raw_count = all_raw.count()
    print(f"\n[BRONZE] Total records ingested across all 3 feeds: {total_raw_count}")

    # ---------------------------------------------------------
    # 4. Data Quality Gate & Quarantine Engine
    # Mandatory fields: member_name, enrollment_date must not be null
    # ---------------------------------------------------------
    dq_evaluated = all_raw.withColumn(
        "is_quarantine",
        F.col("member_name").isNull()
        | (F.col("member_name") == "")
        | F.col("enrollment_date").isNull(),
    ).withColumn(
        "error_reason",
        F.when(
            F.col("enrollment_date").isNull(),
            F.concat(F.lit("INVALID_ENROLLMENT_DATE: '"), F.col("raw_enrollment_date"), F.lit("'")),
        ).when(
            F.col("member_name").isNull() | (F.col("member_name") == ""),
            F.lit("MISSING_MANDATORY_FIELD: Member_Name"),
        ).otherwise(F.lit(None)),
    )

    quarantine_df = dq_evaluated.filter(F.col("is_quarantine")).select(
        "source_file", "local_id", "member_name", "country_code", "raw_enrollment_date", "error_reason"
    )
    valid_df = dq_evaluated.filter(~F.col("is_quarantine"))

    quarantine_count = quarantine_df.count()
    valid_count = valid_df.count()

    print(f"[SILVER - DQ GATE] Valid records: {valid_count} | Quarantined records: {quarantine_count}")
    print("\n--- QUARANTINED DEFECTIVE RECORDS ---")
    quarantine_df.show(truncate=False)

    # ---------------------------------------------------------
    # 5. Silver Staging: Derived Columns (Age, Stale_Member)
    # Age = floor(months_between(proc_date, dob) / 12)
    # Stale_Member = 'Y' if days since last_flight > 90, else 'N'
    # ---------------------------------------------------------
    proc_dt = F.to_date(F.lit(processing_date), "yyyy-MM-dd")
    days_since_flight = F.datediff(proc_dt, F.col("last_flight_date"))

    staged_df = valid_df.withColumn(
        "age",
        F.when(
            F.col("dob").isNotNull(),
            F.floor(F.months_between(proc_dt, F.col("dob")) / 12).cast(IntegerType()),
        ).otherwise(F.lit(None).cast(IntegerType())),
    ).withColumn(
        "days_since_flight",
        days_since_flight,
    ).withColumn(
        "stale_member",
        F.when(
            F.col("last_flight_date").isNull() | (days_since_flight > 90),
            F.lit("Y"),
        ).otherwise(F.lit("N")),
    ).withColumn(
        # Unified Member Key: For cross-country relocation testing, use member_name
        # (matching the spec's key column and demonstrating Mike moving USA -> AUS)
        "member_id",
        F.col("member_name"),
    ).withColumn("agent_name", F.lit(None).cast(StringType())) \
     .withColumn("state", F.lit(None).cast(StringType())) \
     .withColumn("post_code", F.lit(None).cast(IntegerType())) \
     .withColumn("is_active", F.lit("A")) \
     .withColumn("ingested_at", F.current_timestamp())

    print("\n--- SILVER STAGING ENRICHED RECORDS (Age, Days Since Flight, Stale Flag) ---")
    staged_df.select(
        "member_name", "country_code", "dob", "age", "enrollment_date",
        "last_flight_date", "days_since_flight", "stale_member", "tier_code"
    ).orderBy("country_code", "member_name").show(truncate=False)

    # ---------------------------------------------------------
    # 6. Gold: Deduplication & Cross-Country Relocation
    # Notice Mike appears in USA (Flight: 2021-12-30) and AUS (Flight: 2022-08-01)
    # Latest-Record-Wins must route Mike to Australia and purge Mike from USA!
    # ---------------------------------------------------------
    deduped_df = deduplicate_latest_members(staged_df)
    unique_members = deduped_df.count()
    print(f"[GOLD - DEDUPLICATION] Unique members after latest-record-wins: {unique_members}")

    print("\n--- GOLD DEDUPLICATED PROFILES (Notice Mike is won by Australia!) ---")
    deduped_df.select(
        "member_name", "country_code", "tier_code", "last_flight_date", "stale_member", "age"
    ).show(truncate=False)

    # Route to per-country tables
    country_tables = route_and_reconcile_country_tables(
        deduped_df, output_base_dir=out_dir, file_format="parquet"
    )

    print("\n" + "=" * 70)
    print("                    PER-COUNTRY TARGET TABLE RESULTS")
    print("=" * 70)
    for tname in ["table_india", "table_usa", "table_australia"]:
        tdf = country_tables.get(tname)
        if tdf:
            print(f"\nTarget Table: {tname} ({tdf.count()} members):")
            tdf.select("member_name", "tier_code", "enrollment_date", "last_flight_date", "stale_member", "age").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    run_heterogeneous_e2e_test()
