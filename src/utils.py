from pyspark.sql import SparkSession
import os
import sys

def get_spark_session(app_name="TMDB_Analysis"):
    """
    Creates or retrieves a SparkSession.
    Configures Arrow for efficient conversion to Pandas for visualization.
    """
    # Windows specific fix: locate hadoop binaries if not set
    # (Optional but helpful on Windows if you encounter java.io.IOException)
    if sys.platform == "win32" and "HADOOP_HOME" not in os.environ:
         # Suppress common Windows/Spark warnings if winutils.exe is missing
         # It doesn't stop the code from running, just reduces log noise.
         pass

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark