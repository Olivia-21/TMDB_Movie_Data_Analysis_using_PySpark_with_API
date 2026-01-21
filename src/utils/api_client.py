"""
API Client module for TMDB Movie Pipeline.
Handles HTTP requests to the TMDB API with rate limiting.
"""

import time
import requests
import yaml
import os


class TMDBClient:
    """
    Client for making requests to the TMDB API.
    Includes rate limiting to avoid hitting API limits.
    """
    
    def __init__(self, config_path='config/settings.yaml'):
        """
        Initialize the TMDB API client.
        
        Args:
            config_path: Path to the configuration YAML file.
        """
        self.config = self._load_config(config_path)
        self.base_url = self.config['api']['base_url']
        self.api_key = self.config['api']['api_key']
        self.timeout = self.config['api']['timeout']
        self.rate_limit_delay = self.config['api']['rate_limit_delay']
        self.last_request_time = 0
    
    def _load_config(self, config_path):
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to the YAML configuration file.
        
        Returns:
            Dictionary containing configuration values.
        """
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _rate_limit(self):
        """
        Apply rate limiting by waiting if necessary.
        Ensures minimum delay between consecutive API requests.
        """
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint):
        """
        Make a GET request to the TMDB API.
        
        Args:
            endpoint: API endpoint (without base URL).
        
        Returns:
            JSON response as dictionary.
        
        Raises:
            requests.HTTPError: If the request fails.
        """
        self._rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        params = {'api_key': self.api_key}
        
        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        
        return response.json()
    
    def get_movie(self, movie_id):
        """
        Fetch movie details from TMDB.
        
        Args:
            movie_id: TMDB movie ID.
        
        Returns:
            Dictionary containing movie details.
        """
        return self._make_request(f"/movie/{movie_id}")
    
    def get_credits(self, movie_id):
        """
        Fetch movie credits (cast and crew) from TMDB.
        
        Args:
            movie_id: TMDB movie ID.
        
        Returns:
            Dictionary containing cast and crew information.
        """
        return self._make_request(f"/movie/{movie_id}/credits")
    
    def get_movie_with_credits(self, movie_id):
        """
        Fetch movie details along with credits.
        
        Args:
            movie_id: TMDB movie ID.
        
        Returns:
            Dictionary containing movie details merged with credits.
        """
        movie_data = self.get_movie(movie_id)
        credits_data = self.get_credits(movie_id)
        
        # Merge credits into movie data
        movie_data['cast'] = credits_data.get('cast', [])
        movie_data['crew'] = credits_data.get('crew', [])
        
        return movie_data
