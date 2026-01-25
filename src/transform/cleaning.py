import logging
import os
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType
from src.extraction.config import DROP_COLS, FINAL_COL_ORDER

class MovieTransformer:
    """
    Production-grade Spark transformer for movie data.
    Uses a functional approach to clean and enrich datasets.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def drop_irrelevant_columns(self, df: DataFrame) -> DataFrame:
        cols_to_drop = [c for c in DROP_COLS if c in df.columns]
        self.logger.info(f"Dropping {len(cols_to_drop)} irrelevant columns.")
        return df.drop(*cols_to_drop)

    def handle_nested_json(self, df: DataFrame) -> DataFrame:
        """Flattens structs and joins arrays into pipe-separated strings."""
        # 1. Handle Collection Struct
        self.logger.info(f"Schema before nested json handling: {df.schema.simpleString()}")
        if "belongs_to_collection" in df.columns:
            df = df.withColumn("belongs_to_collection", F.col("belongs_to_collection.name"))

        # 2. Handle cast (from credits)
        if "credits" in df.columns:
            df = df.withColumn("cast", F.col("credits.cast")) \
                .withColumn("crew", F.col("credits.crew")) \
                .withColumn("director", F.expr("element_at(filter(credits.crew, x -> x.job == 'Director'), 1).name"))

        # 3. Handle Arrays of Structs
        array_cols = ['genres', 'production_countries', 'production_companies', 'spoken_languages', 'cast', 'crew']
        for col_name in array_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, 
                    F.array_join(F.expr(f"transform({col_name}, x -> x.name)"), "|"))
        
        self.logger.info("Nested JSON structures flattened and joined.")
        return df

    def convert_financials(self, df: DataFrame) -> DataFrame:
        """Converts budget/revenue to MUSD and handles zero-value placeholders."""
        for col_name in ['budget', 'revenue']:
            if col_name in df.columns:
                df = df.withColumn(f"{col_name}_musd", 
                    F.when(F.col(col_name) == 0, None)
                    .otherwise(F.col(col_name) / 1000000).cast(DoubleType()))
        
        if "runtime" in df.columns:
            df = df.withColumn("runtime", 
                F.when(F.col("runtime") == 0, None)
                .otherwise(F.col("runtime").cast(DoubleType())))
        
        return df

    def handle_dates_and_text(self, df: DataFrame) -> DataFrame:
        """Normalizes date formats and clears text placeholders."""
        if "release_date" in df.columns:
            df = df.withColumn("release_date", F.to_date(F.col("release_date"), "yyyy-MM-dd"))
        
        text_cols = ['overview', 'tagline']
        for col in text_cols:
            if col in df.columns:
                df = df.withColumn(col, 
                    F.when(F.col(col).isin("No Data", ""), None).otherwise(F.col(col)))
        
        return df

    def calculate_kpis(self, df: DataFrame) -> DataFrame:
        """Calculates Profit and ROI metrics."""
        if "revenue_musd" in df.columns and "budget_musd" in df.columns:
            df = df.withColumn("profit", F.col("revenue_musd") - F.col("budget_musd"))
            df = df.withColumn("roi", F.col("revenue_musd") / F.col("budget_musd"))
        return df

    def run_pipeline(self, df: DataFrame) -> DataFrame:
        """
        Orchestrates the cleaning pipeline. 
        Uses Spark's .transform() to chain methods cleanly.
        """
        self.logger.info("Starting Movie Data Transformation Pipeline...")
        
        # Chaining transformations for a clean "recipe" view
        df_cleaned = (df
            .transform(self.drop_irrelevant_columns)
            .transform(self.handle_nested_json)
            .transform(self.convert_financials)
            .transform(self.handle_dates_and_text)
            .filter(F.col("status") == "Released") # Requirement: Must be released
            .transform(self.calculate_kpis)
        )

        # Final column selection
        existing_cols = [c for c in FINAL_COL_ORDER if c in df_cleaned.columns]
        extra_kpis = [k for k in ["profit", "roi"] if k in df_cleaned.columns]
        
        self.logger.info("Pipeline completed successfully.")
        return df_cleaned.select(existing_cols + extra_kpis)