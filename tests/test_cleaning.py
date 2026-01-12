import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, IntegerType
from transform.cleaning import clean_movie_data

# 1. Create a reusable Spark Session for tests
@pytest.fixture(scope="session")
def spark():
    # Use the same Windows fix for the test session
    import sys, os
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    return SparkSession.builder \
        .appName("TestSession") \
        .master("local[1]") \
        .getOrCreate()

def test_clean_movie_data(spark):
    """
    Test that clean_movie_data correctly parses genres and calculates profit.
    """
    # 2. Define Schema matching what we expect from the API
    
    # Nested Array for genres
    genre_schema = ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]))
    
    # Nested Struct for Collection (THIS WAS THE MISSING PIECE)
    collection_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("revenue", LongType(), True),
        StructField("budget", LongType(), True),
        StructField("genres", genre_schema, True),
        StructField("status", StringType(), True),
        # Fix: belongs_to_collection must be a Struct, not a String
        StructField("belongs_to_collection", collection_schema, True),
        # Add dummy cols
        StructField("adult", StringType(), True)
    ])

    # 3. Create Fake Data
    # We pass a Dictionary/Row for the struct fields
    data = [
        # Movie 1: Good data
        (
            1, 
            "Hit Movie", 
            200000000, 
            100000000, 
            [{"id": 1, "name": "Action"}], 
            "Released", 
            {"id": 99, "name": "Hit Collection"}, # Struct Data
            "False"
        ),
        # Movie 2: Flop, Unreleased
        (
            2, 
            "Flop Movie", 
            0, 
            0, 
            [{"id": 2, "name": "Comedy"}], 
            "Rumored", 
            None, # Null is valid for Structs
            "False"
        )
    ]

    df_raw = spark.createDataFrame(data, schema=schema)

    # 4. Run the Function
    df_cleaned = clean_movie_data(df_raw)

    # 5. Assertions
    results = df_cleaned.collect()

    # Check that we filtered out the "Rumored" movie (Count should be 1)
    assert len(results) == 1
    
    row = results[0]
    assert row['title'] == "Hit Movie"
    assert row['profit'] == 100.0        # 200 - 100
    assert row['genres'] == "Action"     # Parsed from Array -> String
    assert row['belongs_to_collection'] == "Hit Collection" # Parsed from Struct -> String