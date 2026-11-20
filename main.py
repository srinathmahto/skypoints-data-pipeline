"""
SkyPoints Global Airline Loyalty Program Data Pipeline CLI Entrypoint.
Usage:
    python main.py --processing-date 2024-01-15
"""

import argparse
import logging
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
from skypoints.pipeline import SkyPointsPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("skypoints.main")


def create_spark_session(app_name: str = "SkyPointsPipelineCLI") -> SparkSession:
    """Builds an optimized SparkSession for local execution and production scale."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser(
        description="SkyPoints Global Airline Loyalty Program Medallion Pipeline"
    )
    parser.add_argument(
        "--processing-date",
        default="2024-01-15",
        help="Reference processing date (YYYY-MM-DD) for age and stale member calculation",
    )
    parser.add_argument(
        "--members-file",
        default="data/raw/members_20240115_10300000.txt",
        help="Path to raw pipe-delimited members flat file",
    )
    parser.add_argument(
        "--redemptions-file",
        default="data/raw/redemptions_20240115.json",
        help="Path to raw JSON redemptions feed",
    )
    parser.add_argument(
        "--output-dir",
        default="data/output",
        help="Target base directory for country tables, quarantine, and enriched datasets",
    )

    args = parser.parse_args()

    spark = create_spark_session()
    try:
        pipeline = SkyPointsPipeline(spark)
        metrics = pipeline.run(
            members_file_path=args.members_file,
            redemptions_file_path=args.redemptions_file,
            output_dir=args.output_dir,
            processing_date=args.processing_date,
        )

        # Print structured operational summary
        print("\n" + "=" * 65)
        print("          SKYPOINTS PIPELINE EXECUTION SUMMARY")
        print("=" * 65)
        print(f"Processing Reference Date:       {args.processing_date}")
        print(f"Raw Member Records Ingested:     {metrics.raw_members_ingested}")
        print(f"Clean Records Passed Quality:    {metrics.valid_members_count}")
        print(f"Defective Records Quarantined:   {metrics.quarantined_members_count}")
        print(f"Unique Deduplicated Members:     {metrics.unique_members_deduped}")
        print(f"Redemptions Flattened:           {metrics.redemptions_flattened}")
        print(f"Redemptions Enriched:            {metrics.redemptions_enriched}")
        print(f"Orphan Redemptions (Late Profiles): {metrics.orphan_redemptions_count}")
        print("-" * 65)
        print("Target Country Tables Generated:")
        for tname, cnt in sorted(metrics.country_tables_written.items()):
            print(f"  • {tname:<26} : {cnt} members")
        print("=" * 65 + "\n")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
