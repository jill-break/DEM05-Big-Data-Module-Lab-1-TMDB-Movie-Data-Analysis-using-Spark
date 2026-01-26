import os
import logging
import pandas as pd
from pyspark.sql import DataFrame, functions as F

class MovieAnalyzer:
    """
    Complete modular analysis engine for TMDB data.
    """
    
    def __init__(self, spark_df: DataFrame = None, pandas_df: pd.DataFrame = None): # type: ignore
        self.logger = self._setup_logging()
        self.spark_df = spark_df
        self.pdf = pandas_df

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

    # --- 4. Formatted Reporting ---
    def get_franchise_comparison(self) -> pd.DataFrame:
        """Formatted Pandas table for Franchise vs Standalone comparison."""
        self.logger.info("Logging Franchise comparison report")
        df = self.pdf.copy()
        df['is_franchise'] = df['belongs_to_collection'].notna()
        
        stats = df.groupby('is_franchise').agg({
            'revenue_musd': 'mean',
            'roi': 'median',
            'budget_musd': 'mean',
            'popularity': 'mean',
            'vote_average': 'mean',
            'title': 'count'
        }).rename(columns={'title': 'movie_count'})
        
        label_map = {False: 'Standalone', True: 'Franchise'}
        stats.index = stats.index.map(label_map)
        return stats.round(2)

    def get_top_performing_franchises(self, n: int = 5) -> pd.DataFrame:
        """Formatted Pandas table for top-earning franchises."""
        franchises = self.pdf[self.pdf['belongs_to_collection'].notna()]
        return franchises.groupby('belongs_to_collection').agg({
            'title': 'count',
            'revenue_musd': 'sum',
            'roi': 'median',
            'vote_average': 'mean'
        }).sort_values('revenue_musd', ascending=False).head(n).round(2)
    

    def get_detailed_franchise_success(self, n: int = 10) -> pd.DataFrame:
        """
        Analyzes franchises by movie count, total/mean financials, and ratings.
        """
        self.logger.info(f"Logging detailed success report for top {n} franchises.")
        
        # Filter for only movies that belong to a franchise
        franchises = self.pdf[self.pdf['belongs_to_collection'].notna()]
        
        # Perform multi-metric aggregation
        success_df = franchises.groupby('belongs_to_collection').agg({
            'title': 'count',
            'budget_musd': ['sum', 'mean'],
            'revenue_musd': ['sum', 'mean'],
            'vote_average': 'mean'
        })
        
        # Flatten the MultiIndex columns for easier use in the notebook
        success_df.columns = [
            'movie_count', 
            'total_budget_musd', 'mean_budget_musd', 
            'total_revenue_musd', 'mean_revenue_musd', 
            'mean_rating'
        ]
        
        # Sort by total revenue as the primary 'success' metric
        return success_df.sort_values('total_revenue_musd', ascending=False).head(n).round(2)
    
    def get_most_successful_directors(self, n: int = 10) -> pd.DataFrame:
        """
        Identifies top directors by analyzing movie counts, 
        total revenue, and average user ratings.
        """
        self.logger.info(f"Analyzing success for the top {n} directors.")
        
        # 1. Handle movies with multiple directors (pipe-separated)
        # We explode the 'director' column so each director gets credit for the film
        df_exploded = self.pdf.assign(director=self.pdf['director'].str.split('|')).explode('director')
        
        # 2. Group and Aggregate
        director_stats = df_exploded.groupby('director').agg({
            'title': 'count',
            'revenue_musd': 'sum',
            'vote_average': 'mean'
        }).rename(columns={
            'title': 'movies_directed',
            'revenue_musd': 'total_revenue_musd',
            'vote_average': 'mean_rating'
        })
        
        # 3. Sort by Total Revenue as the primary success indicator
        return director_stats.sort_values('total_revenue_musd', ascending=False).head(n).round(2)