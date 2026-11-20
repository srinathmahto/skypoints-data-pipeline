"""
Staging Transformations & Cross-Country Routing Engine.
Engineered for scale to handle billions of records with:
1. Staging Derived Columns (Age, Stale_Member).
2. Latest-Record-Wins Deduplication via Windowing.
3. Dynamic Country Table Splitting (table_india, table_usa, etc.).
4. Cross-Country Relocation Reconciliation (SCD-1 across country tables).
"""

from datetime import date
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType

from skypoints.config import COUNTRY_TARGET_TABLE_MAP

logger = logging.getLogger(__name__)


def _resolve_processing_date_expr(processing_date: Optional[Union[str, date]] = None):
    """Returns a Spark Column expression for the processing reference date."""
    if processing_date is None:
        return F.current_date()
    elif isinstance(processing_date, str):
        return F.to_date(F.lit(processing_date), "yyyy-MM-dd")
    else:
        return F.lit(processing_date)


def transform_to_staging(
    valid_df: DataFrame,
    processing_date: Optional[Union[str, date]] = None,
) -> DataFrame:
    """
    Transforms validated clean records into the Silver Staging schema.

    Derived Columns:
    - age: Computed in calendar years from DOB to processing_date using floor(months_between / 12).
    - stale_member: 'Y' if days since last_flight_date > 90 (or if last_flight_date is null), else 'N'.
    """
    proc_date_col = _resolve_processing_date_expr(processing_date)

    # Date parsing (ANSI-safe using try_to_date)
    enrollment_date = F.expr("try_to_date(enrollment_date_raw, 'yyyyMMdd')")
    last_flight_date = F.expr("try_to_date(last_flight_date_raw, 'yyyyMMdd')")
    dob = F.expr("try_to_date(dob_raw, 'MMddyyyy')")

    # Derived Column: Age (calendar years)
    age_col = F.when(
        dob.isNotNull(),
        F.floor(F.months_between(proc_date_col, dob) / 12).cast(IntegerType()),
    ).otherwise(F.lit(None).cast(IntegerType()))

    # Derived Column: Stale_Member (days since last flight > 90)
    days_since_flight = F.datediff(proc_date_col, last_flight_date)
    stale_member_col = F.when(
        last_flight_date.isNull() | (days_since_flight > 90),
        F.lit("Y"),
    ).otherwise(F.lit("N"))

    # Post Code integer conversion
    post_code_col = (
        F.when(
            F.col("post_code_raw").isNotNull() & (F.trim(F.col("post_code_raw")) != ""),
            F.col("post_code_raw").cast(IntegerType()),
        ).otherwise(F.lit(None).cast(IntegerType()))
    )

    staged_df = valid_df.select(
        F.col("member_id"),
        F.col("member_name"),
        enrollment_date.alias("enrollment_date"),
        last_flight_date.alias("last_flight_date"),
        F.col("tier_code"),
        F.col("agent_name"),
        F.col("state"),
        F.col("country_code"),
        post_code_col.alias("post_code"),
        dob.alias("dob"),
        F.coalesce(F.col("is_active_raw"), F.lit("A")).alias("is_active"),
        age_col.alias("age"),
        stale_member_col.alias("stale_member"),
        F.col("source_file"),
        F.col("ingested_at"),
    )

    return staged_df


def deduplicate_latest_members(df: DataFrame) -> DataFrame:
    """
    Enforces 'Latest Record Wins' deduplication per member_id.
    Window specification:
        Window.partitionBy("member_id")
              .orderBy(col("last_flight_date").desc_nulls_last(),
                       col("enrollment_date").desc_nulls_last())
    """
    window_spec = (
        Window.partitionBy("member_id")
        .orderBy(
            F.col("last_flight_date").desc_nulls_last(),
            F.col("enrollment_date").desc_nulls_last(),
        )
    )

    deduped_df = (
        df.withColumn("row_rank", F.row_number().over(window_spec))
        .filter(F.col("row_rank") == 1)
        .drop("row_rank")
    )

    return deduped_df


def route_and_reconcile_country_tables(
    df_incoming_members: DataFrame,
    output_base_dir: Union[str, Path] = "data/output",
    file_format: str = "parquet",
) -> Dict[str, DataFrame]:
    """
    Routes deduplicated records into separate per-country target tables.

    Handles Cross-Country Relocation (SCD-1):
    If an existing member moves from IND to USA:
    1. Incoming member batch is deduplicated (latest flight/enrollment date wins).
    2. Any existing country table (e.g. table_india) is inspected: members whose
       latest country_code is now different (e.g. USA) are removed from the old table.
    3. The latest record is written into the new country table (e.g. table_usa).
    4. Guarantees no member exists in multiple country tables simultaneously.

    Returns:
        Dict mapping table_name -> DataFrame of current country members.
    """
    spark = df_incoming_members.sparkSession
    out_base = Path(output_base_dir)
    out_base.mkdir(parents=True, exist_ok=True)

    # 1. Deduplicate incoming batch first
    df_incoming_latest = deduplicate_latest_members(df_incoming_members)

    # 2. Extract incoming members and their target country code
    # Format: member_id -> target_country_code
    incoming_member_locations = {
        row.member_id: row.country_code
        for row in df_incoming_latest.select("member_id", "country_code").distinct().collect()
    }

    country_tables_result: Dict[str, DataFrame] = {}

    # Define all supported country tables
    all_target_tables = set(COUNTRY_TARGET_TABLE_MAP.values())

    # Check for existing tables on disk or in current batch
    for country_code, table_name in COUNTRY_TARGET_TABLE_MAP.items():
        table_path = out_base / table_name
        incoming_for_country = df_incoming_latest.filter(F.col("country_code") == country_code)

        existing_df: Optional[DataFrame] = None
        if table_path.exists() and any(table_path.glob(f"*.{file_format}")):
            try:
                existing_df = spark.read.format(file_format).load(str(table_path))
            except Exception as e:
                logger.warning(f"Could not read existing table at {table_path}: {e}")
                existing_df = None

        if existing_df is not None:
            # Cross-Country Relocation Removal:
            # Exclude members that have relocated to ANY other country in the incoming batch
            relocated_member_ids = [
                mid for mid, target_cc in incoming_member_locations.items()
                if target_cc != country_code
            ]

            if relocated_member_ids:
                retained_existing = existing_df.filter(
                    ~F.col("member_id").isin(relocated_member_ids)
                )
            else:
                retained_existing = existing_df

            # Combine retained existing members with incoming updates
            combined = retained_existing.unionByName(incoming_for_country)
            reconciled = deduplicate_latest_members(combined)
        else:
            reconciled = incoming_for_country

        # Add last_updated_at audit timestamp
        final_country_df = reconciled.withColumn(
            "last_updated_at",
            F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"),
        )

        country_tables_result[table_name] = final_country_df

        # Persist table to disk if it contains rows
        if final_country_df.count() > 0:
            logger.info(f"Writing {final_country_df.count()} records to {table_name} at {table_path}")
            (
                final_country_df.write.mode("overwrite")
                .format(file_format)
                .save(str(table_path))
            )

    return country_tables_result
