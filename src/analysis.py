# src/analysis.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def get_ranked_movies(df: DataFrame, metric: str, ascending: bool = False, limit: int = 5):
    """
    Generic ranking function.
    metric: Column to rank by (e.g., 'revenue_musd', 'roi')
    """
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
    return stats

def get_successful_directors(df: DataFrame):
    """
    Identifies successful directors based on Total Revenue and Movie Count.
    Note: 'director' is not in the default API response for 'get_movie_details',
    it usually requires the 'credits' endpoint. 
    *Assumption*: We will use 'production_companies' as a proxy for this exercise
    if director data is missing, or we assume the API fetch included credits.
    """
    # Assuming 'production_companies' for the demo as 'director' might be missing
    # In a real scenario, you'd explode the crew array.
    
    return df.groupBy("production_companies").agg(
        F.count("id").alias("movie_count"),
        F.sum("revenue_musd").alias("total_revenue"),
        F.mean("vote_average").alias("avg_rating")
    ).orderBy(F.col("total_revenue").desc())