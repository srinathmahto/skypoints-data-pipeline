"""
SkyPoints End-to-End Pipeline Orchestrator.
Coordinates Medallion Lakehouse Flow:
Bronze (Raw Ingest) -> Silver (DQ & Quarantine, Staging) -> Gold (Country Tables, Redemptions Join).
"""

from dataclasses import dataclass
from datetime import date
import logging
from pathlib import Path
from typing import Dict, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from skypoints.ingestion import read_raw_members
from skypoints.validators import validate_member_records
from skypoints.transformations import (
    transform_to_staging,
    deduplicate_latest_members,
    route_and_reconcile_country_tables,
)
from skypoints.redemptions import (
    read_and_flatten_redemptions,
    join_redemptions_with_members,
)

logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Audit and operational metrics captured during pipeline execution."""
    raw_members_ingested: int
    valid_members_count: int
    quarantined_members_count: int
    unique_members_deduped: int
    country_tables_written: Dict[str, int]
    redemptions_flattened: int
    redemptions_enriched: int
    orphan_redemptions_count: int


class SkyPointsPipeline:
    """Production-grade pipeline orchestrator for SkyPoints Global Loyalty Data."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(
        self,
        members_file_path: Union[str, Path],
        redemptions_file_path: Optional[Union[str, Path]] = None,
        output_dir: Union[str, Path] = "data/output",
        processing_date: Optional[Union[str, date]] = None,
    ) -> PipelineMetrics:
        """Executes the full end-to-end Medallion data pipeline."""
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Starting SkyPoints Pipeline execution (processing_date={processing_date})...")

        # 1. Bronze Ingestion: Raw Member Profile Flat File
        df_raw_members = read_raw_members(self.spark, members_file_path)
        raw_members_count = df_raw_members.count()
        logger.info(f"Ingested {raw_members_count} raw member detail records.")

        # 2. Silver Quality Gate: Mandatory field checks, PK validation, country code resolution
        valid_members_df, quarantine_df = validate_member_records(df_raw_members)
        valid_count = valid_members_df.count()
        quarantine_count = quarantine_df.count()
        logger.info(f"Data Quality Gate complete: {valid_count} valid, {quarantine_count} quarantined.")

        # Persist Quarantine table if defective records found
        if quarantine_count > 0:
            quarantine_path = out_path / "quarantine_members"
            quarantine_df.write.mode("overwrite").parquet(str(quarantine_path))
            logger.info(f"Persisted {quarantine_count} quarantine records to {quarantine_path}")

        # 3. Silver Staging: Derived columns (Age, Stale_Member)
        staged_df = transform_to_staging(valid_members_df, processing_date=processing_date)

        # 4. Gold Deduplication: Latest-Record-Wins per member_id
        deduped_members_df = deduplicate_latest_members(staged_df)
        unique_members_count = deduped_members_df.count()
        logger.info(f"Deduplication complete: {unique_members_count} unique member profiles.")

        # 5. Gold Country Routing & Relocation Reconciliation
        country_tables = route_and_reconcile_country_tables(
            deduped_members_df, output_base_dir=out_path, file_format="parquet"
        )
        table_counts = {tname: tdf.count() for tname, tdf in country_tables.items()}
        for tname, cnt in table_counts.items():
            logger.info(f"Country Target Table: {tname:<20} -> {cnt} active members")

        # 6. JSON Redemptions Flattening & Member Enrichment Join
        redemptions_flattened_count = 0
        redemptions_enriched_count = 0
        orphan_redemptions_count = 0

        if redemptions_file_path:
            # Flatten semi-structured feed & deduplicate on txn_id
            df_redemptions = read_and_flatten_redemptions(self.spark, redemptions_file_path)
            redemptions_flattened_count = df_redemptions.count()
            logger.info(f"Flattened {redemptions_flattened_count} partner redemption transactions.")

            # Scale-aware join with member profiles (left_outer to preserve late arrivals)
            # Use broadcast join optimization since unique members dimension is compact
            enriched_df = join_redemptions_with_members(
                df_redemptions, deduped_members_df, broadcast_members=True
            )
            redemptions_enriched_count = enriched_df.count()
            orphan_redemptions_count = enriched_df.filter(F.col("is_orphan_redemption")).count()

            # Persist enriched redemptions
            enriched_path = out_path / "enriched_redemptions"
            enriched_df.write.mode("overwrite").parquet(str(enriched_path))
            logger.info(f"Persisted {redemptions_enriched_count} enriched redemptions to {enriched_path}")

        metrics = PipelineMetrics(
            raw_members_ingested=raw_members_count,
            valid_members_count=valid_count,
            quarantined_members_count=quarantine_count,
            unique_members_deduped=unique_members_count,
            country_tables_written=table_counts,
            redemptions_flattened=redemptions_flattened_count,
            redemptions_enriched=redemptions_enriched_count,
            orphan_redemptions_count=orphan_redemptions_count,
        )

        logger.info("SkyPoints Pipeline execution completed successfully.")
        return metrics

