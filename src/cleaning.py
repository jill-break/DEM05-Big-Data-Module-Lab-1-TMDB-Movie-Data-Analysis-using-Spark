# src/cleaning.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType

from src.config import DROP_COLS, FINAL_COL_ORDER

def clean_movie_data(df: DataFrame) -> DataFrame:
    """
    Main driver function for cleaning the movie dataset.
    """
    # 1. Drop irrelevant columns immediately to save memory
    df = df.drop(*DROP_COLS)

    # 2. Parse Nested JSON Structures
    # Spark automatically infers nested JSON as Structs or Arrays of Structs.
    # We extract the 'name' field and join them with pipes '|'.
    
    # Extract Collection Name (Struct -> String)
    df = df.withColumn("belongs_to_collection", F.col("belongs_to_collection.name"))

    # Extract Array Items (Array<Struct> -> String "A|B|C")
    # We grab the 'name' field from the array of structs
    array_cols = ['genres', 'production_countries', 'production_companies', 'spoken_languages']
    
    for col_name in array_cols:
        # col_name.name extracts an array of names ["Action", "Thriller"]
        # array_join converts it to "Action|Thriller"
        df = df.withColumn(col_name, F.array_join(F.col(f"{col_name}.name"), "|"))

    # 3. Handle Numeric Conversions & Missing Values
    # Convert Budget/Revenue to Millions and Handle 0s
    for col in ['budget', 'revenue']:
        df = df.withColumn(f"{col}_musd", 
                           F.when(F.col(col) == 0, None)
                            .otherwise(F.col(col) / 1000000).cast(DoubleType()))
    
    # Handle Runtime (0 -> NaN equivalent)
    df = df.withColumn("runtime", 
                       F.when(F.col("runtime") == 0, None)
                        .otherwise(F.col("runtime").cast(DoubleType())))

    # 4. Handle Dates
    df = df.withColumn("release_date", F.to_date(F.col("release_date"), "yyyy-MM-dd"))
    
    # 5. Clean Text Placeholders
    text_cols = ['overview', 'tagline']
    for col in text_cols:
        df = df.withColumn(col, 
                           F.when(F.col(col).isin("No Data", ""), None)
                            .otherwise(F.col(col)))

    # 6. Filter: Must be Released and have minimal data
    # (Simplified "10 non-nulls" logic: check critical columns)
    df = df.filter(F.col("status") == "Released")
    
    # 7. Add Calculated Columns for KPIs (ROI)
    # ROI = Revenue / Budget
    df = df.withColumn("profit", F.col("revenue_musd") - F.col("budget_musd"))
    df = df.withColumn("roi", F.col("revenue_musd") / F.col("budget_musd"))

    # 8. Final Selection and Reordering
    # Select only columns that exist in the dataframe to avoid errors
    existing_cols = [c for c in FINAL_COL_ORDER if c in df.columns]
    # Add profit and roi to the final selection
    final_cols = existing_cols + ['profit', 'roi']
    
    return df.select(final_cols)