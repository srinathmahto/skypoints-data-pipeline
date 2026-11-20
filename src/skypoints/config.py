"""
Centralized Configuration, Schemas, and Mappings for SkyPoints ETL.
"""

from typing import Dict
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Canonical 3-letter ISO-3166-1 alpha-3 code mapping
CANONICAL_COUNTRY_MAP: Dict[str, str] = {
    "USA": "USA",
    "US": "USA",
    "UNITED STATES": "USA",
    "IND": "IND",
    "IN": "IND",
    "INDIA": "IND",
    "CAN": "CAN",
    "CA": "CAN",
    "CANADA": "CAN",
    "AU": "AUS",
    "AUS": "AUS",
    "AUSTRALIA": "AUS",
    "PHIL": "PHL",
    "PHL": "PHL",
    "PH": "PHL",
    "PHILIPPINES": "PHL",
    "GBR": "GBR",
    "UK": "GBR",
    "GB": "GBR",
    "UNITED KINGDOM": "GBR",
    "SGP": "SGP",
    "SG": "SGP",
    "SINGAPORE": "SGP",
    "DEU": "DEU",
    "DE": "DEU",
    "GERMANY": "DEU",
    "ARE": "ARE",
    "UAE": "ARE",
}

# Country code to warehouse target table mapping
COUNTRY_TARGET_TABLE_MAP: Dict[str, str] = {
    "IND": "table_india",
    "USA": "table_usa",
    "PHL": "table_philippines",
    "CAN": "table_canada",
    "AUS": "table_australia",
}

# Standard date format constants
DATE_FORMAT_DOB: str = "MMddyyyy"
DATE_FORMAT_ENROLLMENT: str = "yyyyMMdd"
DATE_FORMAT_FLIGHT: str = "yyyyMMdd"

# Raw Landing Ingestion Schema
RAW_MEMBERS_SCHEMA = StructType([
    StructField("record_type", StringType(), False),
    StructField("member_name", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("enrollment_date_raw", StringType(), True),
    StructField("last_flight_date_raw", StringType(), True),
    StructField("tier_code", StringType(), True),
    StructField("agent_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country_raw", StringType(), True),
    StructField("post_code_raw", StringType(), True),
    StructField("dob_raw", StringType(), True),
    StructField("is_active_raw", StringType(), True),
    StructField("source_file", StringType(), False),
    StructField("raw_payload", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
])

# Validated Ingestion Output Schema
VALID_MEMBERS_SCHEMA = StructType([
    StructField("member_id", StringType(), False),
    StructField("member_name", StringType(), False),
    StructField("enrollment_date_raw", StringType(), False),
    StructField("last_flight_date_raw", StringType(), True),
    StructField("tier_code", StringType(), True),
    StructField("agent_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country_code", StringType(), False),
    StructField("post_code_raw", StringType(), True),
    StructField("dob_raw", StringType(), False),
    StructField("is_active_raw", StringType(), True),
    StructField("source_file", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
])

# Quarantine Output Schema
QUARANTINE_MEMBERS_SCHEMA = StructType([
    StructField("source_file", StringType(), False),
    StructField("raw_payload", StringType(), False),
    StructField("error_reason", StringType(), False),
    StructField("failed_record_type", StringType(), True),
    StructField("ingested_at", TimestampType(), False),
])
