import requests
import time
import logging
import os
from typing import List, Dict, Any, Optional

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
        logging.FileHandler(os.path.join(log_dir, "fetch.log")), # Log to file
        logging.StreamHandler()                                      # Log to console
    ]
)
logger = logging.getLogger(__name__)

def fetch_single_movie(session: requests.Session, movie_id: int, base_url: str, params: dict, max_retries: int = 3) -> Optional[Dict]:
    """
    Helper function to fetch a single movie with retries and error handling.
    """
    url = f"{base_url}{movie_id}"
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Fetching ID {movie_id} (Attempt {attempt}/{max_retries})...")
            response = session.get(url, params=params, timeout=10) # 10s timeout
            
            # --- 2. RELEVANT ERROR CODES ---
            
            # Success
            if response.status_code == 200:
                return response.json()
            
            # Client Errors (Do not retry)
            elif response.status_code == 404:
                logger.warning(f"Movie ID {movie_id} not found (404). Skipping.")
                return None
            elif response.status_code == 401:
                logger.critical("API Key Invalid (401). Stopping execution.")
                raise PermissionError("Invalid API Key - Check your .env file")
            
            # Rate Limiting (Wait and Retry)
            elif response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", 5))
                logger.warning(f"Rate limit hit (429). Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue # Retry loop
            
            # Server Errors (Retry)
            elif 500 <= response.status_code < 600:
                logger.error(f"Server error {response.status_code} for ID {movie_id}. Retrying...")
                time.sleep(1 * attempt) # Exponential backoff (1s, 2s, 3s...)
                continue
            
            else:
                logger.error(f"Unexpected error {response.status_code} for ID {movie_id}.")
                return None

        except requests.exceptions.Timeout:
            logger.error(f"Timeout connecting to TMDb for ID {movie_id}. Retrying...")
            time.sleep(1)
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error for ID {movie_id}. Check internet.")
            time.sleep(2)
        except Exception as e:
            logger.exception(f"Critical exception for ID {movie_id}: {e}")
            return None
            
    logger.error(f"Failed to fetch ID {movie_id} after {max_retries} attempts.")
    return None

def fetch_movie_data(movie_ids: List[int], api_key: str, base_url: str) -> List[Dict[str, Any]]:
    """
    Main orchestrator for fetching movie data.
    """
    movies_data = []
    
    # Use a session for connection pooling (Performance boost)
    with requests.Session() as session:
        for idx, movie_id in enumerate(movie_ids):
            
            # Call the helper function with retry logic
            data = fetch_single_movie(session, movie_id, base_url, {"api_key": api_key, "language": "en-US"})
            
            if data:
                movies_data.append(data)
            
            # --- 3. RATE LIMITING ---
            time.sleep(0.2) 

    logger.info(f"Ingestion complete. Successfully fetched {len(movies_data)}/{len(movie_ids)} movies.")
    return movies_data