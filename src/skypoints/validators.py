"""
Data Quality Validation and Dead-Letter Quarantine Engine.
Performs non-crashing data quality gates, mandatory field validation,
date format verification, and canonical country code resolution.
"""

import logging
from typing import Tuple
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from skypoints.config import (
    CANONICAL_COUNTRY_MAP,
    DATE_FORMAT_DOB,
    DATE_FORMAT_ENROLLMENT,
    DATE_FORMAT_FLIGHT,
)

logger = logging.getLogger(__name__)


def build_canonical_country_expr(country_raw_col: Column) -> Column:
    """Builds a SQL CASE expression mapping heterogeneous country strings to canonical ISO-3."""
    c = F.upper(F.trim(country_raw_col))

    when_expr = None
    for raw_code, canonical in CANONICAL_COUNTRY_MAP.items():
        cond = (c == raw_code)
        if when_expr is None:
            when_expr = F.when(cond, F.lit(canonical))
        else:
            when_expr = when_expr.when(cond, F.lit(canonical))

    # Fallback: if already 3-char code keep as is, else 'UNKNOWN'
    return when_expr.when(F.length(c) == 3, c).otherwise(F.lit("UNKNOWN"))


def validate_member_records(df_ingested: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Validates ingested raw member records against strict data quality rules.
    Splits output cleanly into (valid_df, quarantine_df) without crashing.

    Validation Rules:
    1. Mandatory fields: Member_Name, Member_Id, Enrollment_Date must not be null/empty.
    2. Primary Key enforcement: Member_Id must be valid non-empty string.
    3. DOB format: Zero-padded 8 chars matching MMddyyyy date pattern.
    4. Enrollment Date format: 8 chars matching yyyyMMdd date pattern.
    5. Last Flight Date format: If present, must match yyyyMMdd date pattern.
    6. Country code: Must resolve to known canonical country code.
    """
    canonical_country = build_canonical_country_expr(F.col("country_raw"))

    # ANSI-safe date parsing checks using try_to_date (returns null on parse failure instead of throwing)
    enroll_dt_valid = F.expr(f"try_to_date(enrollment_date_raw, '{DATE_FORMAT_ENROLLMENT}')").isNotNull()

    flight_dt_valid = (
        F.col("last_flight_date_raw").isNull()
        | (F.trim(F.col("last_flight_date_raw")) == "")
        | F.expr(f"try_to_date(last_flight_date_raw, '{DATE_FORMAT_FLIGHT}')").isNotNull()
    )

    dob_valid = (
        F.col("dob_raw").isNotNull()
        & (F.length(F.col("dob_raw")) == 8)
        & F.expr(f"try_to_date(dob_raw, '{DATE_FORMAT_DOB}')").isNotNull()
    )

    # Collect diagnostic error tags
    rule_checks = F.array(
        F.when(
            F.col("member_name").isNull() | (F.trim(F.col("member_name")) == ""),
            F.lit("MISSING_MEMBER_NAME"),
        ),
        F.when(
            F.col("member_id").isNull() | (F.trim(F.col("member_id")) == ""),
            F.lit("MISSING_MEMBER_ID"),
        ),
        F.when(~enroll_dt_valid, F.lit("INVALID_ENROLLMENT_DATE")),
        F.when(~flight_dt_valid, F.lit("INVALID_LAST_FLIGHT_DATE")),
        F.when(~dob_valid, F.lit("INVALID_DOB")),
        F.when(canonical_country == "UNKNOWN", F.lit("UNKNOWN_COUNTRY")),
    )

    # Filter non-null error codes
    errors = F.filter(rule_checks, lambda x: x.isNotNull())

    df_evaluated = (
        df_ingested.withColumn("country_code", canonical_country)
        .withColumn("validation_errors", errors)
        .withColumn("has_errors", F.size(F.col("validation_errors")) > 0)
    )

    # Valid records output
    valid_df = (
        df_evaluated.filter(~F.col("has_errors"))
        .select(
            "member_id",
            "member_name",
            "enrollment_date_raw",
            "last_flight_date_raw",
            "tier_code",
            "agent_name",
            "state",
            "country_code",
            "post_code_raw",
            "dob_raw",
            "is_active_raw",
            "source_file",
            "ingested_at",
        )
    )

    # Quarantine records output with descriptive concatenated error reason
    quarantine_df = (
        df_evaluated.filter(F.col("has_errors"))
        .select(
            F.col("source_file"),
            F.col("raw_payload"),
            F.array_join(F.col("validation_errors"), "; ").alias("error_reason"),
            F.col("record_type").alias("failed_record_type"),
            F.col("ingested_at"),
        )
    )

    return valid_df, quarantine_df
