"""
Flat-File Raw Ingestion Engine for SkyPoints Member Profiles.
Engineered for scale to ingest billions of pipe-delimited records.

Handles source anomalies:
1. Strips leading/trailing delimiter pipes and edge whitespace.
2. Trims whitespace across all columns.
3. Separates Header ('H') and Detail ('D') records.
4. Schema drift: gracefully handles missing 'Post Code' (10 vs 11 detail tokens).
5. Zero-padded DOB: guarantees zero-padded 8-character string (lpad(..., 8, '0')).
"""

import logging
from pathlib import Path
from typing import Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def parse_raw_member_text(df_raw_text: DataFrame, file_name: str) -> DataFrame:
    """
    Parses a single-column raw text DataFrame into strongly structured landing records.
    Tolerates delimiter noise, whitespace padding, and missing Post Code drift.
    """
    raw_col = F.col("value")

    # Step 1: Clean line edges by stripping leading/trailing pipes and surrounding whitespace
    cleaned_line = F.regexp_replace(
        F.regexp_replace(F.trim(raw_col), r"^\|+", ""),
        r"\|+$",
        "",
    )

    # Step 2: Split line on pipe with surrounding whitespace tolerance
    parts = F.split(cleaned_line, r"\s*\|\s*")
    num_parts = F.size(parts)

    base = df_raw_text.withColumn("parts", parts).withColumn("num_parts", num_parts)

    # Step 3: Separate Header ('H') vs Detail ('D') records
    detail_records = base.filter(
        (F.col("num_parts") >= 10) & (F.upper(F.trim(F.col("parts")[0])) == "D")
    )

    # Step 4: Extract columns with adaptive Schema Drift resolution:
    # 11 tokens (index 0..10): Post Code missing (actual sample feed format)
    # 12+ tokens (index 0..11): Post Code present at index 9 (design spec format)
    def clean_token(idx: int):
        val = F.trim(F.col("parts")[idx])
        return F.when(val != "", val).otherwise(F.lit(None))

    parsed = detail_records.select(
        F.lit("D").alias("record_type"),
        clean_token(1).alias("member_name"),
        clean_token(2).alias("member_id"),
        clean_token(3).alias("enrollment_date_raw"),
        clean_token(4).alias("last_flight_date_raw"),
        clean_token(5).alias("tier_code"),
        clean_token(6).alias("agent_name"),
        clean_token(7).alias("state"),
        clean_token(8).alias("country_raw"),
        # Schema evolution: Post Code fallback
        F.when(F.col("num_parts") >= 12, clean_token(9)).otherwise(F.lit(None)).alias("post_code_raw"),
        # DOB Zero-Padding:
        # Strictly preserves 8-character zero-padded string (e.g. '03051985')
        F.when(
            F.col("num_parts") >= 12,
            F.when(
                F.trim(F.col("parts")[10]) != "",
                F.lpad(F.trim(F.col("parts")[10]), 8, "0"),
            ).otherwise(F.lit(None)),
        )
        .otherwise(
            F.when(
                F.trim(F.col("parts")[9]) != "",
                F.lpad(F.trim(F.col("parts")[9]), 8, "0"),
            ).otherwise(F.lit(None))
        )
        .alias("dob_raw"),
        # Active status flag
        F.when(F.col("num_parts") >= 12, clean_token(11))
        .otherwise(clean_token(10))
        .alias("is_active_raw"),
        # Audit metadata
        F.lit(file_name).alias("source_file"),
        raw_col.alias("raw_payload"),
        F.current_timestamp().alias("ingested_at"),
    )

    return parsed


def read_raw_members(
    spark: SparkSession,
    file_path: Union[str, Path],
) -> DataFrame:
    """
    Reads a raw pipe-delimited member profile file into a PySpark DataFrame.
    """
    path_str = str(file_path)
    file_name = Path(path_str).name
    logger.info(f"Ingesting raw member feed: {path_str}")

    df_raw = spark.read.text(path_str)
    return parse_raw_member_text(df_raw, file_name=file_name)
