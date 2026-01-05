from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType

from src.config import DROP_COLS, FINAL_COL_ORDER

def clean_movie_data(df: DataFrame) -> DataFrame:
    """
    Main driver function for cleaning the movie dataset.
    """
    # 1. Drop irrelevant columns immediately to save memory
    # Check if columns exist before dropping to avoid errors if run twice
    cols_to_drop = [c for c in DROP_COLS if c in df.columns]
    df = df.drop(*cols_to_drop)

    # 2. Parse Nested JSON Structures
    # Spark automatically infers nested JSON as Structs or Arrays of Structs.
    
    # Extract Collection Name (Struct -> String)
    # Check if column exists first (some movies might have no collection info at all)
    if "belongs_to_collection" in df.columns:
        df = df.withColumn("belongs_to_collection", F.col("belongs_to_collection.name"))

    # Extract Array Items (Array<Struct> -> String "A|B|C")
    # We use 'transform' (SQL expression) which is safer than dot notation for Arrays
    array_cols = ['genres', 'production_countries', 'production_companies', 'spoken_languages']
    
    for col_name in array_cols:
        if col_name in df.columns:
            # transform(genres, x -> x.name) takes the array 'genres', 
            # loops through it as 'x', and extracts 'x.name'
            df = df.withColumn(col_name, 
                               F.array_join(F.expr(f"transform({col_name}, x -> x.name)"), "|"))

    # 3. Handle Numeric Conversions & Missing Values
    # Convert Budget/Revenue to Millions and Handle 0s
    for col in ['budget', 'revenue']:
        if col in df.columns:
            df = df.withColumn(f"{col}_musd", 
                            F.when(F.col(col) == 0, None)
                                .otherwise(F.col(col) / 1000000).cast(DoubleType()))
    
    # Handle Runtime (0 -> NaN equivalent)
    if "runtime" in df.columns:
        df = df.withColumn("runtime", 
                        F.when(F.col("runtime") == 0, None)
                            .otherwise(F.col("runtime").cast(DoubleType())))

    # 4. Handle Dates
    if "release_date" in df.columns:
        df = df.withColumn("release_date", F.to_date(F.col("release_date"), "yyyy-MM-dd"))
    
    # 5. Clean Text Placeholders
    text_cols = ['overview', 'tagline']
    for col in text_cols:
        if col in df.columns:
            df = df.withColumn(col, 
                            F.when(F.col(col).isin("No Data", ""), None)
                                .otherwise(F.col(col)))

    # 6. Filter: Must be Released
    if "status" in df.columns:
        df = df.filter(F.col("status") == "Released")
    
    # 7. Add Calculated Columns for KPIs (ROI)
    # ROI = Revenue / Budget
    # Ensure columns exist before calculation
    if "revenue_musd" in df.columns and "budget_musd" in df.columns:
        df = df.withColumn("profit", F.col("revenue_musd") - F.col("budget_musd"))
        df = df.withColumn("roi", F.col("revenue_musd") / F.col("budget_musd"))

    # 8. Final Selection and Reordering
    # Select only columns that exist in the dataframe to avoid errors
    existing_cols = [c for c in FINAL_COL_ORDER if c in df.columns]
    
    # Add profit and roi to the final selection if they were created
    extras = []
    if "profit" in df.columns: extras.append("profit")
    if "roi" in df.columns: extras.append("roi")
    
    return df.select(existing_cols + extras)