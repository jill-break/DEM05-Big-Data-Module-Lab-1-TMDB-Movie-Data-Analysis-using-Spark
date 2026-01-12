import os
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    ArrayType, LongType, DoubleType, BooleanType, MapType
)
from dotenv import load_dotenv

# --- .ENV LOADING ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.themoviedb.org/3/movie/"

# Movie IDs
MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397, 
    420818, 24428, 168259, 99861, 284054, 12445, 
    181808, 330457, 351286, 109445, 321612, 260513
]

# --- Column Management ---
DROP_COLS = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']

FINAL_COL_ORDER = [
    'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
    'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
    'production_countries', 'vote_count', 'vote_average', 'popularity',
    'runtime', 'overview', 'spoken_languages', 'poster_path'
]

# --- STRICT SCHEMAS ---
GENRE_STRUCT = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

COMPANY_STRUCT = StructType([
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("logo_path", StringType(), True),
    StructField("origin_country", StringType(), True)
])

COUNTRY_STRUCT = StructType([
    StructField("iso_3166_1", StringType(), True),
    StructField("name", StringType(), True)
])

LANGUAGE_STRUCT = StructType([
    StructField("iso_639_1", StringType(), True),
    StructField("name", StringType(), True),
    StructField("english_name", StringType(), True)
])

COLLECTION_STRUCT = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("poster_path", StringType(), True),
    StructField("backdrop_path", StringType(), True)
])

# --- MASTER RAW SCHEMA ---
# This matches the full JSON response from TMDB
RAW_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("status", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", LongType(), True),
    StructField("budget", LongType(), True),
    StructField("runtime", LongType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", LongType(), True),
    StructField("original_language", StringType(), True),
    StructField("poster_path", StringType(), True),
    
    # Complex Nested Fields
    StructField("belongs_to_collection", COLLECTION_STRUCT, True),
    StructField("genres", ArrayType(GENRE_STRUCT), True),
    StructField("production_companies", ArrayType(COMPANY_STRUCT), True),
    StructField("production_countries", ArrayType(COUNTRY_STRUCT), True),
    StructField("spoken_languages", ArrayType(LANGUAGE_STRUCT), True),
    
    # Fields drop later but need to read first
    StructField("adult", BooleanType(), True),
    StructField("imdb_id", StringType(), True),
    StructField("homepage", StringType(), True),
    StructField("video", BooleanType(), True),
    StructField("original_title", StringType(), True)
])