"""
Pytest configuration and shared fixtures for SkyPoints test suite.
"""

import os
from pathlib import Path
import sys
import pytest

# Ensure Windows JVM and Hadoop environment variables are set before PySpark import
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
src_dir = Path(__file__).resolve().parent.parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Builds a lightweight local SparkSession for testing."""
    session = (
        SparkSession.builder.appName("SkyPointsTestSuite")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
