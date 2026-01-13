import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import year, explode, split, col, median, mean, count
import os

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
        logging.FileHandler(os.path.join(log_dir, "visualizationPrep.log")), # Log to file
        logging.StreamHandler()                                      # Log to console
    ]
)
logger = logging.getLogger(__name__)

class VisualizationPreparer:
    """
    Handles PySpark processing for visualization datasets and 
    converts them to Pandas DataFrames for plotting.
    """
    
    def __init__(self, df_clean: DataFrame):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.df = df_clean
        self.logger.info("VisualizationPreparer initialized with Spark DataFrame")

    def prepare_yearly_trends(self) -> DataFrame:
        """Extracts year from release_date and aggregates performance metrics."""
        logger.info("Preparing yearly trends data")
        return self.df.withColumn("year", year("release_date")) \
            .groupBy("year") \
            .agg(
                count("id").alias("movie_count"),
                mean("revenue_musd").alias("mean_revenue"),
                mean("budget_musd").alias("mean_budget"),
                mean("roi").alias("mean_roi")
            ).orderBy("year")

    def prepare_genre_data(self) -> DataFrame:
        """Explodes pipe-separated genres and calculates Median ROI per genre."""
        logger.info("Preparing genre-wise median ROI data")
        return self.df.withColumn("genre", explode(split("genres", "\|"))) \
            .groupBy("genre") \
            .agg(median("roi").alias("median_roi")) \
            .orderBy(col("median_roi").desc())

    def prepare_franchise_data(self) -> DataFrame:
        """Categorizes movies as Franchise vs Standalone and calculates means."""
        logger.info("Preparing franchise vs standalone data")
        return self.df.withColumn("type", 
                    F.when(col("belongs_to_collection").isNotNull(), "Franchise")
                    .otherwise("Standalone")) \
            .groupBy("type") \
            .agg(
                mean("revenue_musd").alias("mean_revenue"),
                mean("budget_musd").alias("mean_budget"),
                mean("roi").alias("mean_roi"),
                mean("vote_average").alias("mean_rating")
            )

    def get_all_viz_data(self):
        """
        Orchestrates all preparations and returns a dictionary 
        containing Pandas DataFrames.
        """
        self.logger.info("Beginning Spark aggregation and collection to Pandas...")
        
        try:
            # Prepare Spark DFs
            df_yearly = self.prepare_yearly_trends()
            df_genre = self.prepare_genre_data()
            df_franchise = self.prepare_franchise_data()
            df_scatter = self.df.select("title", "budget_musd", "revenue_musd", "popularity", "vote_average")

            # Collect to Pandas
            payload = {
                "yearly": df_yearly.toPandas(),
                "genre": df_genre.toPandas(),
                "scatter": df_scatter.toPandas(),
                "franchise": df_franchise.toPandas()
            }
            
            self.logger.info("Data successfully collected to Pandas payloads.")
            return payload
            
        except Exception as e:
            self.logger.error(f"Error during Spark-to-Pandas collection: {str(e)}")
            raise