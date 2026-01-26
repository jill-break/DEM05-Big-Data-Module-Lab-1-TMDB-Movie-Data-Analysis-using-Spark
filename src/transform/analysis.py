import os
import logging
from pyspark.sql import DataFrame, functions as F

class MovieAnalyzer:
    """
    Complete modular analysis engine for TMDB data using only PySpark.
    """
    
    def __init__(self, spark_df: DataFrame):
        self.logger = self._setup_logging()
        self.spark_df = spark_df

    def _setup_logging(self):
        log_dir = os.path.join(os.getcwd(), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(log_dir, "analysis.log")),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(self.__class__.__name__)

    # --- 1. Generic Ranking (Spark) ---
    def get_ranked_movies(self, metric: str, ascending: bool = False, limit: int = 5) -> DataFrame:
        """Ranks movies by a specific metric (e.g., 'roi', 'revenue_musd')."""
        self.logger.info(f"Ranking top {limit} movies by {metric} (Spark)")
        
        df_filtered = self.spark_df.filter(F.col(metric).isNotNull())
        order_fn = F.col(metric).asc() if ascending else F.col(metric).desc()
        
        return df_filtered.orderBy(order_fn).limit(limit)

    # --- 2. Franchise Analysis (Spark) ---
    def analyze_franchises(self) -> DataFrame:
        """Compares Franchise vs. Standalone using Spark aggregation."""
        self.logger.info("Analyzing franchise statistics (Spark)")
        df_flagged = self.spark_df.withColumn("is_franchise", F.col("belongs_to_collection").isNotNull())
        
        return df_flagged.groupBy("is_franchise").agg(
            F.count("id").alias("count"),
            F.mean("revenue_musd").alias("avg_revenue"),
            F.median("roi").alias("median_roi"),
            F.mean("budget_musd").alias("avg_budget"),
            F.mean("popularity").alias("avg_popularity"),
            F.mean("vote_average").alias("avg_rating")
        )

    # --- 3. Director/Production Analysis (Spark) ---
    def get_successful_entities_spark(self) -> DataFrame:
        """Identifies successful production companies/directors by total revenue."""
        self.logger.info("Analyzing success by production companies (Spark)")
        
        return self.spark_df.groupBy("production_companies").agg(
            F.count("id").alias("movie_count"),
            F.sum("revenue_musd").alias("total_revenue"),
            F.mean("vote_average").alias("avg_rating")
        ).orderBy(F.col("total_revenue").desc())

    # --- 4. Formatted Reporting (Converted to Spark) ---
    def get_franchise_comparison(self) -> DataFrame:
        """Formatted Spark table for Franchise vs Standalone comparison."""
        self.logger.info("Logging Franchise comparison report")
        
        df_flagged = self.spark_df.withColumn(
            "category", 
            F.when(F.col("belongs_to_collection").isNotNull(), "Franchise").otherwise("Standalone")
        )
        
        return df_flagged.groupBy("category").agg(
            F.count("title").alias("movie_count"),
            F.round(F.mean("revenue_musd"), 2).alias("revenue_musd"),
            F.round(F.median("roi"), 2).alias("roi"),
            F.round(F.mean("budget_musd"), 2).alias("budget_musd"),
            F.round(F.mean("popularity"), 2).alias("popularity"),
            F.round(F.mean("vote_average"), 2).alias("vote_average")
        )

    def get_top_performing_franchises(self, n: int = 5) -> DataFrame:
        """Formatted Spark table for top-earning franchises."""
        
        return self.spark_df.filter(F.col("belongs_to_collection").isNotNull()) \
            .groupBy("belongs_to_collection").agg(
                F.count("title").alias("title_count"),
                F.round(F.sum("revenue_musd"), 2).alias("revenue_musd"),
                F.round(F.median("roi"), 2).alias("roi"),
                F.round(F.mean("vote_average"), 2).alias("vote_average")
            ).orderBy(F.col("revenue_musd").desc()).limit(n)
    

    def get_detailed_franchise_success(self, n: int = 10) -> DataFrame:
        """
        Analyzes franchises by movie count, total/mean financials, and ratings.
        """
        self.logger.info(f"Logging detailed success report for top {n} franchises.")
        
        return self.spark_df.filter(F.col("belongs_to_collection").isNotNull()) \
            .groupBy("belongs_to_collection").agg(
                F.count("title").alias("movie_count"),
                F.round(F.sum("budget_musd"), 2).alias("total_budget_musd"),
                F.round(F.mean("budget_musd"), 2).alias("mean_budget_musd"),
                F.round(F.sum("revenue_musd"), 2).alias("total_revenue_musd"),
                F.round(F.mean("revenue_musd"), 2).alias("mean_revenue_musd"),
                F.round(F.mean("vote_average"), 2).alias("mean_rating")
            ).orderBy(F.col("total_revenue_musd").desc()).limit(n)
    
    def get_most_successful_directors(self, n: int = 10) -> DataFrame:
        """
        Identifies top directors by analyzing movie counts, 
        total revenue, and average user ratings.
        """
        self.logger.info(f"Analyzing success for the top {n} directors.")
        
        # 1. Handle movies with multiple directors (pipe-separated)
        # Explode the 'director' column so each director gets credit for the film
        # Note: We need to split by the pipe character. escape it properly.
        df_exploded = self.spark_df.withColumn("director", F.explode(F.split(F.col("director"), "\|")))
        
        # 2. Group and Aggregate
        return df_exploded.groupBy("director").agg(
            F.count("title").alias("movies_directed"),
            F.round(F.sum("revenue_musd"), 2).alias("total_revenue_musd"),
            F.round(F.mean("vote_average"), 2).alias("mean_rating")
        ).orderBy(F.col("total_revenue_musd").desc()).limit(n)