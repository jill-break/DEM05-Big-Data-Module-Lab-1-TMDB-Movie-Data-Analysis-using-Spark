import requests
import time
from typing import List, Dict, Any

def fetch_movie_data(movie_ids: List[int], api_key: str, base_url: str) -> List[Dict[str, Any]]:
    """
    Fetches movie details for a list of movie IDs from the TMDb API.
    
    Args:
        movie_ids: List of integer movie IDs.
        api_key: TMDb API Key.
        base_url: Base URL for the API.
        
    Returns:
        List of dictionaries containing raw movie data.
    """
    movies_data = []
    
    # Use a session for connection pooling (faster requests)
    with requests.Session() as session:
        for idx, movie_id in enumerate(movie_ids):
            url = f"{base_url}{movie_id}"
            params = {
                "api_key": api_key,
                "language": "en-US"
            }
            
            try:
                print(f"Fetching {idx+1}/{len(movie_ids)}: ID {movie_id}...")
                response = session.get(url, params=params)
                
                if response.status_code == 200:
                    movies_data.append(response.json())
                elif response.status_code == 404:
                    print(f"  Warning: Movie ID {movie_id} not found.")
                else:
                    print(f"  Error: Failed to fetch ID {movie_id}. Status: {response.status_code}")
            
            except Exception as e:
                print(f"  Exception occurred for ID {movie_id}: {e}")
            
            # Rate limiting handling (very basic)
            # TMDb allows ~4 requests/sec per IP, so a tiny sleep helps
            time.sleep(0.25)

    return movies_data