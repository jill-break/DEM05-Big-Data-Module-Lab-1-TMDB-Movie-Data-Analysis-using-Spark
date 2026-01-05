# src/utils.py
import sys
import os
from pyspark.sql import SparkSession

def get_spark_session(app_name="TMDB_Analysis"):
    """
    Creates or retrieves a SparkSession.
    Configures Arrow for efficient conversion to Pandas for visualization.
    """
    
    # --- CRITICAL WINDOWS FIX ---
    # Force Spark to use the currently active Python executable (inside .venv)
    # for both the driver and the worker processes. This prevents the "worker failed to connect" error.
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # Windows specific fix: locate hadoop binaries if not set
    if sys.platform == "win32" and "HADOOP_HOME" not in os.environ:
         pass

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.network.timeout", "600s") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark