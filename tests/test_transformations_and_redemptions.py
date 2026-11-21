"""
Unit and integration tests for Staging Transformations, Country Routing, and JSON Redemptions.
"""

from datetime import date, datetime
from pathlib import Path
import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from skypoints.transformations import (
    transform_to_staging,
    deduplicate_latest_members,
    route_and_reconcile_country_tables,
)
from skypoints.redemptions import (
    flatten_redemptions_feed,
    join_redemptions_with_members,
    read_and_flatten_redemptions,
)
from skypoints.pipeline import SkyPointsPipeline


def make_valid_member_df(spark, rows_data):
    """Helper to create a validated member DataFrame with raw date strings ready for staging."""
    schema = StructType([
        StructField("member_id", StringType(), False),
        StructField("member_name", StringType(), False),
        StructField("enrollment_date_raw", StringType(), False),
        StructField("last_flight_date_raw", StringType(), True),
        StructField("tier_code", StringType(), True),
        StructField("agent_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country_code", StringType(), False),
        StructField("post_code_raw", StringType(), True),
        StructField("dob_raw", StringType(), True),
        StructField("is_active_raw", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ])
    now = datetime(2024, 1, 15, 10, 30, 0)
    full_rows = [r + (now,) if len(r) == 12 else r for r in rows_data]
    return spark.createDataFrame(full_rows, schema)


def test_age_calculation(spark):
    """Verifies that integer age is accurately computed from DOB relative to processing date."""
    data = [
        # Born 1985-03-05 (03051985), proc date 2024-01-15 -> 38 years
        ("M1", "Elena", "20100101", "20230101", "GLD", "Sam", "CA", "USA", None, "03051985", "A", "f1"),
        # Born 1990-01-15 (01151990), proc date 2024-01-15 -> exactly 34 years (birthday today)
        ("M2", "BirthdayToday", "20100101", "20230101", "SLV", "Sam", "CA", "USA", None, "01151990", "A", "f1"),
        # Born 1990-01-16 (01161990), proc date 2024-01-15 -> 33 years (birthday tomorrow)
        ("M3", "BirthdayTomorrow", "20100101", "20230101", "BRZ", "Sam", "CA", "USA", None, "01161990", "A", "f1"),
    ]
    df = make_valid_member_df(spark, data)
    staged_df = transform_to_staging(df, processing_date="2024-01-15")

    results = {row.member_id: row.age for row in staged_df.collect()}
    assert results["M1"] == 38
    assert results["M2"] == 34
    assert results["M3"] == 33


def test_stale_member_flag(spark):
    """Verifies stale_member is 'Y' when last flight > 90 days or null, 'N' otherwise."""
    data = [
        # Flight 2023-10-10 -> 97 days prior to 2024-01-15 (>90d) -> Y
        ("M1", "OldFlight", "20100101", "20231010", "GLD", "Sam", "CA", "USA", None, "03051985", "A", "f1"),
        # Flight 2024-01-05 -> 10 days prior (<=90d) -> N
        ("M2", "RecentFlight", "20100101", "20240105", "SLV", "Sam", "CA", "USA", None, "01151990", "A", "f1"),
        # Never flown (null last flight date) -> Y
        ("M3", "NeverFlown", "20100101", None, "BRZ", "Sam", "CA", "USA", None, "01161990", "A", "f1"),
    ]
    df = make_valid_member_df(spark, data)
    staged_df = transform_to_staging(df, processing_date="2024-01-15")

    results = {row.member_id: row.stale_member for row in staged_df.collect()}
    assert results["M1"] == "Y"
    assert results["M2"] == "N"
    assert results["M3"] == "Y"


def test_latest_record_wins_deduplication(spark):
    """Verifies latest-record-wins deduplication per member_id based on last_flight_date."""
    data = [
        # Older flight
        ("223457", "Elena", "20101012", "20121013", "GLD", "Sam", "CA", "USA", None, "03051985", "A", "f1"),
        # Newer flight (latest)
        ("223457", "Elena Updated", "20101012", "20230820", "PLT", "Sam", "CA", "USA", None, "03051985", "A", "f2"),
    ]
    df = make_valid_member_df(spark, data)
    staged = transform_to_staging(df, processing_date="2024-01-15")
    deduped = deduplicate_latest_members(staged)

    assert deduped.count() == 1
    winner = deduped.first()
    assert winner.member_name == "Elena Updated"
    assert winner.tier_code == "PLT"
    assert winner.last_flight_date == date(2023, 8, 20)


def test_cross_country_relocation_reconciliation(spark, tmp_path):
    """Verifies member relocating from IND to USA is added to table_usa and purged from table_india."""
    out_dir = tmp_path / "country_output"

    # Batch 1: Member 556677 resides in India
    batch1_data = [
        ("556677", "Arjun", "20150601", "20210510", "SLV", "Raj", "MH", "IND", None, "11201988", "A", "f1"),
        ("100001", "Priya", "20180101", "20230901", "GLD", "Raj", "DL", "IND", None, "04151992", "A", "f1"),
    ]
    df_batch1 = transform_to_staging(make_valid_member_df(spark, batch1_data), processing_date="2024-01-15")
    tables_batch1 = route_and_reconcile_country_tables(df_batch1, output_base_dir=out_dir, file_format="parquet")

    assert tables_batch1["table_india"].count() == 2
    ind_members_b1 = {row.member_id for row in tables_batch1["table_india"].collect()}
    assert "556677" in ind_members_b1

    # Batch 2: Arjun relocates to USA with newer flight date
    batch2_data = [
        ("556677", "Arjun", "20150601", "20240110", "GLD", "John", "NY", "USA", None, "11201988", "A", "f2"),
    ]
    df_batch2 = transform_to_staging(make_valid_member_df(spark, batch2_data), processing_date="2024-01-15")
    tables_batch2 = route_and_reconcile_country_tables(df_batch2, output_base_dir=out_dir, file_format="parquet")

    # Arjun must be present in table_usa
    usa_members = {row.member_id for row in tables_batch2["table_usa"].collect()}
    assert "556677" in usa_members

    # Arjun must be PURGED from table_india, leaving only Priya
    ind_members_b2 = {row.member_id for row in tables_batch2["table_india"].collect()}
    assert "556677" not in ind_members_b2
    assert "100001" in ind_members_b2
    assert len(ind_members_b2) == 1


def test_flatten_redemptions_feed(spark):
    """Verifies JSON redemption explosion, key alias handling, and transaction deduplication."""
    json_lines = [
        # Uses 'member id' with space + 2 redemptions (one with txn_id duplicate in next row)
        '{"member id": "223457", "feed_date": "20240115", "redemptions": ['
        '  {"txn_id": "TXN01", "txn_date": "20240110", "partner": "AeroLink", "miles_redeemed": 10000, "status": "COMPLETED"},'
        '  {"txn_id": "TXN02", "txn_date": "20240111", "partner": "SkyPoints", "miles_redeemed": 5000, "status": "PENDING"}'
        ']}',
        # Retransmitted TXN01 with updated status
        '{"member_id": "223457", "feed_date": "20240115", "redemptions": ['
        '  {"txn_id": "TXN01", "txn_date": "20240112", "partner": "AeroLink", "miles_redeemed": 10000, "status": "COMPLETED"}'
        ']}',
        # Uses 'member_id'
        '{"member_id": "223458", "feed_date": "20240115", "redemptions": ['
        '  {"txn_id": "TXN03", "txn_date": "20240114", "partner": "IndiSky", "miles_redeemed": 8000, "status": "COMPLETED"}'
        ']}',
    ]
    rdd = spark.sparkContext.parallelize(json_lines)
    df_raw_json = spark.read.json(rdd)

    flattened = flatten_redemptions_feed(df_raw_json)
    # Total unique transactions should be 3 (TXN01, TXN02, TXN03)
    assert flattened.count() == 3

    txns = {row.txn_id: row for row in flattened.collect()}
    assert set(txns.keys()) == {"TXN01", "TXN02", "TXN03"}
    # TXN01 was deduplicated to the newer date 2024-01-12
    assert txns["TXN01"].txn_date == date(2024, 1, 12)
    assert txns["TXN02"].miles_redeemed == 5000
    assert txns["TXN03"].partner == "IndiSky"


def test_join_redemptions_with_members(spark):
    """Verifies scalable left_outer enrichment and orphan redemption detection."""
    # Redemptions DataFrame
    rdd_redemptions = spark.sparkContext.parallelize([
        '{"member id": "223457", "feed_date": "20240115", "redemptions": [{"txn_id": "TXN10", "txn_date": "20240110", "partner": "Aero", "miles_redeemed": 5000, "status": "COMPLETED"}]}',
        # Member 999999 has no profile yet (orphan / late arriving)
        '{"member_id": "999999", "feed_date": "20240115", "redemptions": [{"txn_id": "TXN11", "txn_date": "20240111", "partner": "Aero", "miles_redeemed": 2000, "status": "COMPLETED"}]}',
    ])
    df_redemptions = flatten_redemptions_feed(spark.read.json(rdd_redemptions))

    # Members DataFrame (only contains 223457, flown recently so not stale)
    members_data = [
        ("223457", "Elena", "20101012", "20240105", "PLT", "Sam", "CA", "USA", None, "03051985", "A", "f1"),
    ]
    df_members = transform_to_staging(make_valid_member_df(spark, members_data), processing_date="2024-01-15")

    enriched = join_redemptions_with_members(df_redemptions, df_members, broadcast_members=True)
    assert enriched.count() == 2

    records = {row.txn_id: row for row in enriched.collect()}
    # Enriched member profile
    r10 = records["TXN10"]
    assert r10.member_name == "Elena"
    assert r10.country_code == "USA"
    assert r10.tier_code == "PLT"
    assert r10.stale_member == "N"
    assert r10.is_orphan_redemption is False

    # Orphan redemption
    r11 = records["TXN11"]
    assert r11.member_name is None
    assert r11.is_orphan_redemption is True


def test_end_to_end_pipeline(spark, tmp_path):
    """Full integration test of SkyPointsPipeline using the raw sample files."""
    members_file = Path("data/raw/members_20240115_10300000.txt")
    redemptions_file = Path("data/raw/redemptions_20240115.json")
    out_dir = tmp_path / "e2e_output"

    pipeline = SkyPointsPipeline(spark)
    metrics = pipeline.run(
        members_file_path=members_file,
        redemptions_file_path=redemptions_file,
        output_dir=out_dir,
        processing_date="2024-01-15",
    )

    assert metrics.raw_members_ingested == 13
    assert metrics.valid_members_count == 10
    assert metrics.quarantined_members_count == 3
    assert metrics.unique_members_deduped == 7
    assert metrics.redemptions_flattened == 6
    assert metrics.redemptions_enriched == 6
    assert metrics.orphan_redemptions_count == 0

    assert metrics.country_tables_written["table_india"] == 1
    assert metrics.country_tables_written["table_usa"] == 2
    assert metrics.country_tables_written["table_philippines"] == 1
    assert metrics.country_tables_written["table_canada"] == 1
    assert metrics.country_tables_written["table_australia"] == 2
