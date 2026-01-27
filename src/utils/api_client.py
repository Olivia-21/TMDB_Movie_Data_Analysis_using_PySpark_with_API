"""
API Client module for TMDB Movie Pipeline.
Handles HTTP requests to the TMDB API.
"""

import requests
import yaml


class TMDBClient:
    """
    Client for making requests to the TMDB API.
    """
    
    def __init__(self, config_path='config/settings.yaml'):
        """
        Initialize the TMDB API client.
        
        Args:
            config_path: Path to the configuration YAML file.
        """
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self.base_url = config['api']['base_url']
        self.api_key = config['api']['api_key']
        self.timeout = config['api']['timeout']
    
    def get_movie_with_credits(self, movie_id):
        """
        Fetch movie details along with credits in a single API call.
        
        Uses TMDB's append_to_response feature for efficiency.
        
        Args:
            movie_id: TMDB movie ID.
        
        Returns:
            Dictionary containing movie details with cast and crew included.
        """
        url = f"{self.base_url}/movie/{movie_id}"
        params = {
            'api_key': self.api_key,
            'append_to_response': 'credits'
        }
        
        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        
        movie_data = response.json()
        
        # Extract credits from the nested 'credits' key
        credits = movie_data.pop('credits', {})
        movie_data['cast'] = credits.get('cast', [])
        movie_data['crew'] = credits.get('crew', [])
        
        return movie_data
