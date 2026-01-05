# src/config.py
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from dotenv import load_dotenv

# --- FIX: ROBUST .ENV LOADING ---
# 1. Get the directory where THIS file (config.py) lives (e.g., .../src)
current_dir = os.path.dirname(os.path.abspath(__file__))
# 2. Go up one level to the project root
project_root = os.path.dirname(current_dir)
# 3. Point explicitly to the .env file
env_path = os.path.join(project_root, '.env')

# Load the specific file
load_dotenv(env_path)

# Check if key loaded (for debugging)
api_key_check = os.getenv("API_KEY")
if not api_key_check:
    print("WARNING: API Key not found! Check your .env path.")
else:
    print("SUCCESS: API Key loaded.")

# --- Configuration Constants ---
API_KEY = api_key_check
BASE_URL = "https://api.themoviedb.org/3/movie/"

# The list of Movie IDs provided in the requirements
MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397, 
    420818, 24428, 168259, 99861, 284054, 12445, 
    181808, 330457, 351286, 109445, 321612, 260513
]

# --- Column Groups ---
DROP_COLS = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
JSON_COLS = ['belongs_to_collection', 'genres', 'production_countries', 'production_companies', 'spoken_languages']

FINAL_COL_ORDER = [
    'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
    'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
    'production_countries', 'vote_count', 'vote_average', 'popularity',
    'runtime', 'overview', 'spoken_languages', 'poster_path'
]

# --- Spark Schemas ---
GENRE_SCHEMA = ArrayType(StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
]))

COMPANY_SCHEMA = ArrayType(StructType([
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True)
]))

COUNTRY_SCHEMA = ArrayType(StructType([
    StructField("iso_3166_1", StringType(), True),
    StructField("name", StringType(), True)
]))

LANGUAGE_SCHEMA = ArrayType(StructType([
    StructField("iso_639_1", StringType(), True),
    StructField("name", StringType(), True)
]))