"""
Semi-Structured JSON Redemption Feed Flattening & Scalable Enrichment.
Designed to process billions of partner redemption transactions.

Features:
1. Normalizes 'member id' vs 'member_id' key variants.
2. Explodes nested redemptions array via explode_outer.
3. Strongly types transaction attributes and deduplicates on txn_id.
4. Scale-aware join with member profiles (broadcast/salting-compatible, left_outer for late arrivals).
"""

import logging
from pathlib import Path
from typing import Optional, Union

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, StringType

logger = logging.getLogger(__name__)


def flatten_redemptions_feed(df_json: DataFrame) -> DataFrame:
    """
    Flattens raw JSON redemptions DataFrame into clean relational rows.

    Handles:
    - Key variants: 'member id' (with space) vs 'member_id'
    - Nested arrays: explode_outer on 'redemptions'
    - Duplicate txn_id records: deduplicated keeping the newest record
    """
    raw_cols = df_json.columns

    # Dynamic resolution of member ID column name variants
    if "member id" in raw_cols and "member_id" in raw_cols:
        member_id_col = F.coalesce(F.col("`member id`"), F.col("member_id"))
    elif "member id" in raw_cols:
        member_id_col = F.col("`member id`")
    elif "member_id" in raw_cols:
        member_id_col = F.col("member_id")
    else:
        member_id_col = F.lit(None).cast(StringType())

    feed_date_col = F.expr("try_to_date(feed_date, 'yyyyMMdd')")

    # Explode nested array of structs
    exploded = (
        df_json.withColumn("member_id_clean", F.trim(member_id_col))
        .withColumn("feed_date_clean", feed_date_col)
        .withColumn("redemption", F.explode_outer(F.col("redemptions")))
    )

    # Project relational columns
    flattened_df = exploded.select(
        F.col("member_id_clean").alias("member_id"),
        F.col("feed_date_clean").alias("feed_date"),
        F.trim(F.col("redemption.txn_id")).alias("txn_id"),
        F.expr("try_to_date(redemption.txn_date, 'yyyyMMdd')").alias("txn_date"),
        F.trim(F.col("redemption.partner")).alias("partner"),
        F.col("redemption.miles_redeemed").cast(LongType()).alias("miles_redeemed"),
        F.upper(F.trim(F.col("redemption.status"))).alias("status"),
        F.current_timestamp().alias("ingested_at"),
    ).filter(
        F.col("member_id").isNotNull()
        & (F.col("member_id") != "")
        & F.col("txn_id").isNotNull()
        & (F.col("txn_id") != "")
    )

    # Deduplicate on txn_id: in event of retransmitted or duplicate feeds, keep latest
    txn_window = (
        Window.partitionBy("txn_id")
        .orderBy(F.col("txn_date").desc_nulls_last(), F.col("ingested_at").desc_nulls_last())
    )

    deduped_redemptions = (
        flattened_df.withColumn("txn_rank", F.row_number().over(txn_window))
        .filter(F.col("txn_rank") == 1)
        .drop("txn_rank")
    )

    return deduped_redemptions


def read_and_flatten_redemptions(
    spark: SparkSession,
    json_path: Union[str, Path],
) -> DataFrame:
    """Reads a JSON line feed of redemptions and flattens it into a relational DataFrame."""
    path_str = str(json_path)
    logger.info(f"Ingesting JSON redemptions feed from: {path_str}")
    df_raw = spark.read.json(path_str)
    return flatten_redemptions_feed(df_raw)


def join_redemptions_with_members(
    df_redemptions: DataFrame,
    df_members: DataFrame,
    broadcast_members: bool = False,
) -> DataFrame:
    """
    Enriches redemptions with member profile attributes (country, tier, stale status).

    Scalability Design for Billions of Records:
    - Uses left_outer join: late-arriving member profiles never drop redemption events.
    - Supports broadcast join optimization when member profile dimension is within broadcast threshold.
    - Identifies orphan redemptions (member profile not yet ingested) for auditing.
    """
    # Select only required member profile columns to minimize shuffle footprint
    members_projection = df_members.select(
        F.col("member_id"),
        F.col("member_name"),
        F.col("country_code"),
        F.col("tier_code"),
        F.col("stale_member"),
        F.col("is_active"),
    )

    if broadcast_members:
        member_join_target = F.broadcast(members_projection)
    else:
        member_join_target = members_projection

    enriched_df = df_redemptions.join(
        member_join_target,
        on="member_id",
        how="left_outer",
    ).select(
        df_redemptions["txn_id"],
        df_redemptions["txn_date"],
        df_redemptions["member_id"],
        F.col("member_name"),
        F.col("country_code"),
        F.col("tier_code"),
        F.col("stale_member"),
        F.col("is_active"),
        df_redemptions["partner"],
        df_redemptions["miles_redeemed"],
        df_redemptions["status"],
        df_redemptions["feed_date"],
        F.when(F.col("member_name").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_orphan_redemption"),
        F.current_timestamp().alias("enriched_at"),
    )

    return enriched_df
