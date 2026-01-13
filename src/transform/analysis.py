from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import os
import logging  

# --- 1. SETUP LOGGING ---
# Create a 'logs' directory if it doesn't exist
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)

# Configure the logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "analysis.log")), # Log to file
        logging.StreamHandler()                                      # Log to console
    ]
)
logger = logging.getLogger(__name__)

def get_ranked_movies(df: DataFrame, metric: str, ascending: bool = False, limit: int = 5):
    """
    Generic ranking function.
    metric: Column to rank by (e.g., 'revenue_musd', 'roi')
    """
    logger.info(f"Ranking top {limit} movies by {metric}, ascending={ascending}")
    # Filter out nulls for the metric being ranked to ensure data quality
    df_filtered = df.filter(F.col(metric).isNotNull())
    
    if ascending:
        return df_filtered.orderBy(F.col(metric).asc()).limit(limit)
    else:
        return df_filtered.orderBy(F.col(metric).desc()).limit(limit)

def analyze_franchises(df: DataFrame):
    """
    Compares Franchise vs. Standalone movies.
    """
    # Create a flag: Is it a franchise?
    df_flagged = df.withColumn("is_franchise", F.col("belongs_to_collection").isNotNull())
    
    stats = df_flagged.groupBy("is_franchise").agg(
        F.count("id").alias("count"),
        F.mean("revenue_musd").alias("avg_revenue"),
        F.median("roi").alias("median_roi"),
        F.mean("budget_musd").alias("avg_budget"),
        F.mean("popularity").alias("avg_popularity"),
        F.mean("vote_average").alias("avg_rating")
    )
    logger.info("Analyzed franchise vs standalone movie statistics")
    return stats

def get_successful_directors(df: DataFrame):
    """
    Identifies successful directors based on Total Revenue and Movie Count.
    """
    # Assuming 'production_companies' for the demo as 'director' might be missing
    # In a real scenario, you'd explode the crew array.
    logger.info("Analyzing successful directors based on production companies")
    
    return df.groupBy("production_companies").agg(
        F.count("id").alias("movie_count"),
        F.sum("revenue_musd").alias("total_revenue"),
        F.mean("vote_average").alias("avg_rating")
    ).orderBy(F.col("total_revenue").desc())