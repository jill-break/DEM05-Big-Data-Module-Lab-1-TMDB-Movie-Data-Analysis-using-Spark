import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.transform.cleaning import MovieTransformer
import os
import sys

# Forces Spark to use the virtual environment's Python
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

@pytest.fixture(scope="session")
def spark():
    """Fixture to create a local SparkSession for testing."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("PyTest-Spark-Cleaning") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()  

@pytest.fixture
def transformer():
    """Fixture to initialize our modular MovieTransformer."""
    return MovieTransformer()

def test_financial_conversion(spark, transformer):
    """Verify budget and revenue are correctly converted to Millions (MUSD)."""
    # Create mock data: 1,000,000 USD should become 1.0 MUSD
    data = [("Movie A", 1000000, 2000000), ("Movie B", 0, 500000)]
    schema = ["title", "budget", "revenue"]
    df_raw = spark.createDataFrame(data, schema)
    
    # Run the specific modular method
    df_clean = transformer.convert_financials(df_raw)
    
    results = df_clean.collect()
    
    # Check Movie A: 1.0 MUSD
    assert results[0]["budget_musd"] == 1.0
    assert results[0]["revenue_musd"] == 2.0
    
    # Check Movie B: 0 should become None/Null
    assert results[1]["budget_musd"] is None
    assert results[1]["revenue_musd"] == 0.5

def test_json_flattening(spark, transformer):
    """Verify that nested structs and arrays are correctly joined."""
    # Mock the complex TMDB structure
    data = [
        (1, [{"name": "Action"}, {"name": "Sci-Fi"}], {"name": "Marvel Collection"})
    ]
    schema = "id INT, genres ARRAY<STRUCT<name:STRING>>, belongs_to_collection STRUCT<name:STRING>"
    df_raw = spark.createDataFrame(data, schema)
    
    df_clean = transformer.handle_nested_json(df_raw)
    result = df_clean.first()
    
    assert result["genres"] == "Action|Sci-Fi"
    assert result["belongs_to_collection"] == "Marvel Collection"

def test_full_pipeline_output(spark, transformer):
    """Verify that the full run_pipeline returns the expected final columns."""
    # Corrected: data now provides an array of structs for genres
    data = [(1, "Released", 1000000, 5000000, "2023-01-01", [{"name": "Action"}])]
    
    # Corrected: schema explicitly defines genres as an array of structs
    schema = "id INT, status STRING, budget LONG, revenue LONG, release_date STRING, genres ARRAY<STRUCT<name:STRING>>"
    
    df_raw = spark.createDataFrame(data, schema)
    
    df_final = transformer.run_pipeline(df_raw)
    
    assert "profit" in df_final.columns
    assert "roi" in df_final.columns