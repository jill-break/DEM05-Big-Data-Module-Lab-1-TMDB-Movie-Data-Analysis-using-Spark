import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.transform.analysis import MovieAnalyzer
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
        .appName("PyTest-Spark-Analysis") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

@pytest.fixture
def mock_clean_data(spark):
    """Creates a mock cleaned DataFrame for analysis tests."""
    data = [
        # id, title, budget_musd, revenue_musd, roi, popularity, vote_average, belongs_to_collection, director
        (1, "Hit Movie", 100.0, 500.0, 5.0, 50.0, 8.5, "The Franchise", "Director A"),
        (2, "Flop Movie", 200.0, 100.0, 0.5, 10.0, 4.0, None, "Director B"),
        (3, "Indie Gem", 5.0, 25.0, 5.0, 20.0, 9.0, None, "Director A|Director C"),
        (4, "Franchise Sequel", 150.0, 450.0, 3.0, 45.0, 7.5, "The Franchise", "Director B"),
        (5, "Null Metrics", None, None, None, 5.0, 6.0, None, "Director D")
    ]
    
    schema = [
        "id", "title", "budget_musd", "revenue_musd", "roi", 
        "popularity", "vote_average", "belongs_to_collection", "director"
    ]
    return spark.createDataFrame(data, schema)

def test_get_ranked_movies(spark, mock_clean_data):
    """Test generic ranking logic."""
    analyzer = MovieAnalyzer(mock_clean_data)
    
    # Test Revenue Ranking (Descending)
    ranked = analyzer.get_ranked_movies("revenue_musd", ascending=False, limit=3)
    results = ranked.collect()
    
    assert len(results) == 3
    assert results[0]["title"] == "Hit Movie"      # 500.0
    assert results[1]["title"] == "Franchise Sequel" # 450.0
    assert results[2]["title"] == "Flop Movie"     # 100.0
    
    # Verify Nulls are filtered out (Metric 5 has None)
    all_ranked = analyzer.get_ranked_movies("revenue_musd", limit=10).collect()
    assert len(all_ranked) == 4 # Should exclude the one with Null revenue

def test_analyze_franchises(spark, mock_clean_data):
    """Test Franchise vs Standalone comparison logic."""
    analyzer = MovieAnalyzer(mock_clean_data)
    # We expect analyze_franchises to return a Spark DataFrame with 'is_franchise' column
    df_stats = analyzer.analyze_franchises()
    results = {row['is_franchise']: row for row in df_stats.collect()}
    
    # Check Franchise Stats (2 movies: Hit Movie, Franchise Sequel)
    # Avg Revenue = (500 + 450) / 2 = 475.0
    assert results[True]['count'] == 2
    assert results[True]['avg_revenue'] == 475.0
    
    # Check Standalone Stats (2 movies: Flop Movie, Indie Gem) (Null Metrics is standalone but has null revenue, spark agg handles nulls)
    # Avg Revenue = (100 + 25) / 2 = 62.5
    assert results[False]['avg_revenue'] == 62.5

def test_get_most_successful_directors(spark, mock_clean_data):
    """Test director explosion and revenue aggregation."""
    analyzer = MovieAnalyzer(mock_clean_data)
    
    df_directors = analyzer.get_most_successful_directors(n=5)
    results = df_directors.collect()
    
    # Logic Verification:
    # Director A: Hit Movie (500) + Indie Gem (25) = 525.0
    # Director B: Flop Movie (100) + Franchise Sequel (450) = 550.0
    # Director C: Indie Gem (25) = 25.0
    
    # Sort order is descending revenue
    assert results[0]["director"] == "Director B"
    assert results[0]["total_revenue_musd"] == 550.0
    
    assert results[1]["director"] == "Director A"
    assert results[1]["total_revenue_musd"] == 525.0
    
    # Director C gets credit for split
    director_c = next(r for r in results if r["director"] == "Director C")
    assert director_c["total_revenue_musd"] == 25.0
