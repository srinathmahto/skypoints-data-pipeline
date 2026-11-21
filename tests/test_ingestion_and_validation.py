"""
Unit and integration tests for Ingestion and Data Quality Quarantine Engine.
"""

from datetime import date
from pathlib import Path
import pytest
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType

from skypoints.ingestion import parse_raw_member_text, read_raw_members
from skypoints.validators import validate_member_records
from skypoints.transformations import transform_to_staging


def create_raw_df_from_lines(spark, lines):
    """Helper to construct raw text DataFrame from a list of strings."""
    schema = StructType([StructField("value", StringType(), True)])
    rows = [Row(value=line) for line in lines]
    return spark.createDataFrame(rows, schema)


def test_missing_pipes_and_whitespace_trimming(spark):
    """Verifies that missing leading pipes, trailing pipes, and whitespace padding are handled."""
    raw_lines = [
        "|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|Agent_Name|State|Country|DOB|Is_Active|",
        "D|Nora|998877|20190505|20231120|SLV|Dave|NSW|AU|09091992|A|",
        "|D | Elena | 223457| 20101012 | 20121013 | GLD | Sam | CA | USA | 03051985 | A |",
    ]
    raw_df = create_raw_df_from_lines(spark, raw_lines)
    parsed_df = parse_raw_member_text(raw_df, "test_file.txt")

    records = {row.member_id: row.asDict() for row in parsed_df.collect()}
    assert len(records) == 2
    assert "H" not in records

    # Nora had missing leading pipe and trailing pipe
    nora = records["998877"]
    assert nora["member_name"] == "Nora"
    assert nora["agent_name"] == "Dave"
    assert nora["country_raw"] == "AU"
    assert nora["is_active_raw"] == "A"

    # Elena had surrounding spaces
    elena = records["223457"]
    assert elena["member_name"] == "Elena"
    assert elena["tier_code"] == "GLD"
    assert elena["state"] == "CA"
    assert elena["country_raw"] == "USA"


def test_dob_leading_zero_preservation(spark):
    """Verifies that zero-prefixed DOB (e.g. 03051985) is not truncated to 3051985."""
    raw_lines = [
        "|D|Elena|223457|20101012|20121013|GLD|Sam|CA|USA|03051985|A|",
        "|D|ZeroDay|112233|20200101|20230101|SLV|Bob|NY|USA|01051995|A|",
    ]
    raw_df = create_raw_df_from_lines(spark, raw_lines)
    parsed_df = parse_raw_member_text(raw_df, "test_dob.txt")

    row_dict = {row.member_id: row for row in parsed_df.collect()}
    assert row_dict["223457"].dob_raw == "03051985"
    assert row_dict["112233"].dob_raw == "01051995"

    valid_df, quarantine_df = validate_member_records(parsed_df)
    assert quarantine_df.count() == 0

    staged_df = transform_to_staging(valid_df, processing_date="2024-01-15")
    staged_records = {row.member_id: row for row in staged_df.collect()}
    # MMDDYYYY: 03051985 -> March 5, 1985
    assert staged_records["223457"].dob == date(1985, 3, 5)
    # 01051995 -> Jan 5, 1995
    assert staged_records["112233"].dob == date(1995, 1, 5)


def test_schema_drift_missing_post_code(spark):
    """Verifies handling of schema evolution: 11 tokens (no Post Code) vs 12 tokens (with Post Code)."""
    raw_lines = [
        # 11 tokens: no post code
        "|D|NoPostCode|100001|20200101|20230101|GLD|Agent1|CA|USA|03051985|A|",
        # 12 tokens: post code included at position 9
        "|D|WithPostCode|100002|20200101|20230101|GLD|Agent2|CA|USA|90210|03051985|A|",
    ]
    raw_df = create_raw_df_from_lines(spark, raw_lines)
    parsed_df = parse_raw_member_text(raw_df, "test_drift.txt")

    records = {row.member_id: row.asDict() for row in parsed_df.collect()}
    assert records["100001"]["post_code_raw"] is None
    assert records["100001"]["dob_raw"] == "03051985"
    assert records["100001"]["is_active_raw"] == "A"

    assert records["100002"]["post_code_raw"] == "90210"
    assert records["100002"]["dob_raw"] == "03051985"
    assert records["100002"]["is_active_raw"] == "A"


def test_country_code_normalization(spark):
    """Verifies that non-standard country representations are normalized to canonical codes."""
    raw_lines = [
        "|D|UserUS|101|20200101|20230101|SLV|Ag|CA|USA|03051985|A|",
        "|D|UserIN|102|20200101|20230101|SLV|Ag|MH|IND|03051985|A|",
        "|D|UserPH|103|20200101|20230101|SLV|Ag|MNL|PHIL|03051985|A|",
        "|D|UserCA|104|20200101|20230101|SLV|Ag|ON|CAN|03051985|A|",
        "|D|UserAU|105|20200101|20230101|SLV|Ag|NSW|AU|03051985|A|",
    ]
    raw_df = create_raw_df_from_lines(spark, raw_lines)
    parsed_df = parse_raw_member_text(raw_df, "test_countries.txt")
    valid_df, quarantine_df = validate_member_records(parsed_df)

    assert quarantine_df.count() == 0
    records = {row.member_id: row.country_code for row in valid_df.collect()}
    assert records["101"] == "USA"
    assert records["102"] == "IND"
    assert records["103"] == "PHL"
    assert records["104"] == "CAN"
    assert records["105"] == "AUS"


def test_quarantine_mandatory_field_missing(spark):
    """Verifies that records missing Member_Id, Member_Name, or Enrollment_Date route to quarantine."""
    raw_lines = [
        # Missing Member_Name
        "|D||300001|20200101|20230101|SLV|Ag|CA|USA|03051985|A|",
        # Missing Member_Id
        "|D|NoId||20200101|20230101|SLV|Ag|CA|USA|03051985|A|",
        # Missing Enrollment_Date
        "|D|NoEnroll|300003||20230101|SLV|Ag|CA|USA|03051985|A|",
        # Valid record
        "|D|ValidUser|300004|20200101|20230101|SLV|Ag|CA|USA|03051985|A|",
    ]
    raw_df = create_raw_df_from_lines(spark, raw_lines)
    parsed_df = parse_raw_member_text(raw_df, "test_quarantine.txt")
    valid_df, quarantine_df = validate_member_records(parsed_df)

    assert valid_df.count() == 1
    assert quarantine_df.count() == 3

    errors = [row.error_reason for row in quarantine_df.collect()]
    assert any("MISSING_MEMBER_NAME" in e for e in errors)
    assert any("MISSING_MEMBER_ID" in e for e in errors)
    assert any("INVALID_ENROLLMENT_DATE" in e for e in errors)


def test_quarantine_invalid_dates(spark):
    """Verifies that invalid date formats route safely to quarantine without throwing exceptions."""
    raw_lines = [
        # Invalid DOB (month 99)
        "|D|BadDOB|400001|20200101|20230101|SLV|Ag|CA|USA|99051985|A|",
        # Invalid Enrollment_Date
        "|D|BadEnroll|400002|20209999|20230101|SLV|Ag|CA|USA|03051985|A|",
        # Invalid Country
        "|D|BadCountry|400003|20200101|20230101|SLV|Ag|CA|XYZ_INVALID|03051985|A|",
    ]
    raw_df = create_raw_df_from_lines(spark, raw_lines)
    parsed_df = parse_raw_member_text(raw_df, "test_dates.txt")
    valid_df, quarantine_df = validate_member_records(parsed_df)

    assert valid_df.count() == 0
    assert quarantine_df.count() == 3

    errors = [row.error_reason for row in quarantine_df.collect()]
    assert any("INVALID_DOB" in e for e in errors)
    assert any("INVALID_ENROLLMENT_DATE" in e for e in errors)
    assert any("UNKNOWN_COUNTRY" in e for e in errors)


def test_full_sample_file_ingestion(spark):
    """Integration test: validates ingestion and DQ on the actual assessment sample file."""
    sample_file = Path("data/raw/members_20240115_10300000.txt")
    assert sample_file.exists(), f"Sample file not found at {sample_file}"

    df_raw = read_raw_members(spark, sample_file)
    assert df_raw.count() == 13

    valid_df, quarantine_df = validate_member_records(df_raw)
    assert valid_df.count() == 10
    assert quarantine_df.count() == 3
