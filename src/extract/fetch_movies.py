"""
Fetch Movies module for TMDB Movie Pipeline.
Handles extraction of movie data from the TMDB API.
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

from src.utils.api_client import TMDBClient
from src.utils.constants import MOVIE_IDS
from orchestrator.logger import get_step_logger


def get_movie_schema():
    """
    Define the schema for raw movie data.
    
    Returns:
        PySpark StructType for movie data.
    """
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("adult", StringType(), True),
        StructField("imdb_id", StringType(), True),
        StructField("original_title", StringType(), True),
        StructField("video", StringType(), True),
        StructField("homepage", StringType(), True),
        StructField("genres", StringType(), True),
        StructField("belongs_to_collection", StringType(), True),
        StructField("original_language", StringType(), True),
        StructField("budget", DoubleType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("production_companies", StringType(), True),
        StructField("production_countries", StringType(), True),
        StructField("spoken_languages", StringType(), True),
        StructField("vote_count", IntegerType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("runtime", IntegerType(), True),
        StructField("poster_path", StringType(), True),
        StructField("cast", StringType(), True),
        StructField("cast_size", IntegerType(), True),
        StructField("director", StringType(), True),
        StructField("crew_size", IntegerType(), True),
    ])


def extract_cast_info(cast_list, limit=5):
    """
    Extract top cast member names from cast list.
    
    Args:
        cast_list: List of cast dictionaries from API.
        limit: Maximum number of cast members to include.
    
    Returns:
        Pipe-separated string of actor names.
    """
    if not cast_list:
        return None
    
    # Sort by order (most prominent first) and take top N
    sorted_cast = sorted(cast_list, key=lambda x: x.get('order', 999))[:limit]
    names = [actor.get('name', '') for actor in sorted_cast if actor.get('name')]
    
    return '|'.join(names) if names else None


def extract_director(crew_list):
    """
    Extract director name from crew list.
    
    Args:
        crew_list: List of crew dictionaries from API.
    
    Returns:
        Director name or None if not found.
    """
    if not crew_list:
        return None
    
    for crew_member in crew_list:
        if crew_member.get('job') == 'Director':
            return crew_member.get('name')
    
    return None


def process_movie_data(movie_data):
    """
    Process raw API response into a flat dictionary for DataFrame.
    
    Args:
        movie_data: Dictionary containing movie data from API.
    
    Returns:
        Flat dictionary ready for DataFrame row.
    """
    # Extract cast and crew info
    cast_list = movie_data.get('cast', [])
    crew_list = movie_data.get('crew', [])
    
    # Convert JSON fields to strings for storage
    def json_to_str(obj):
        if obj is None:
            return None
        return json.dumps(obj)
    
    return {
        'id': movie_data.get('id'),
        'title': movie_data.get('title'),
        'tagline': movie_data.get('tagline'),
        'overview': movie_data.get('overview'),
        'release_date': movie_data.get('release_date'),
        'status': movie_data.get('status'),
        'adult': str(movie_data.get('adult', '')),
        'imdb_id': movie_data.get('imdb_id'),
        'original_title': movie_data.get('original_title'),
        'video': str(movie_data.get('video', '')),
        'homepage': movie_data.get('homepage'),
        'genres': json_to_str(movie_data.get('genres')),
        'belongs_to_collection': json_to_str(movie_data.get('belongs_to_collection')),
        'original_language': movie_data.get('original_language'),
        'budget': float(movie_data.get('budget', 0)),
        'revenue': float(movie_data.get('revenue', 0)),
        'production_companies': json_to_str(movie_data.get('production_companies')),
        'production_countries': json_to_str(movie_data.get('production_countries')),
        'spoken_languages': json_to_str(movie_data.get('spoken_languages')),
        'vote_count': movie_data.get('vote_count'),
        'vote_average': float(movie_data.get('vote_average', 0)),
        'popularity': float(movie_data.get('popularity', 0)),
        'runtime': movie_data.get('runtime'),
        'poster_path': movie_data.get('poster_path'),
        'cast': extract_cast_info(cast_list),
        'cast_size': len(cast_list) if cast_list else 0,
        'director': extract_director(crew_list),
        'crew_size': len(crew_list) if crew_list else 0,
    }


def fetch_movies(spark, movie_ids=None, config_path='config/settings.yaml'):
    """
    Fetch movie data from TMDB API and return as PySpark DataFrame.
    
    Args:
        spark: SparkSession instance.
        movie_ids: List of movie IDs to fetch. If None, uses default list.
        config_path: Path to configuration file.
    
    Returns:
        PySpark DataFrame containing raw movie data.
    """
    logger = get_step_logger('extract')
    
    if movie_ids is None:
        movie_ids = MOVIE_IDS
    
    logger.info(f"Starting extraction for {len(movie_ids)} movies")
    
    # Initialize API client
    client = TMDBClient(config_path)
    
    # Fetch data for each movie
    movies_data = []
    failed_ids = []
    
    for movie_id in movie_ids:
        try:
            logger.info(f"Fetching movie ID: {movie_id}")
            
            # Use retry logic for individual movie fetches
            def fetch_single_movie():
                return client.get_movie_with_credits(movie_id)
            
            # Retry with exponential backoff (1s, 2s, 4s delays)
            raw_data = None
            max_retries = 3
            delay = 1
            backoff = 2
            last_error = None
            
            for attempt in range(1, max_retries + 1):
                try:
                    logger.info(f"  Attempt {attempt} of {max_retries} for movie ID {movie_id}")
                    raw_data = client.get_movie_with_credits(movie_id)
                    break
                except Exception as e:
                    last_error = e
                    logger.warning(f"  Attempt {attempt} failed for movie ID {movie_id}: {str(e)}")
                    if attempt < max_retries:
                        import time
                        logger.info(f"  Retrying in {delay} seconds...")
                        time.sleep(delay)
                        delay *= backoff
            
            if raw_data is None:
                raise last_error
            
            processed_data = process_movie_data(raw_data)
            movies_data.append(processed_data)
            logger.info(f"Successfully fetched: {processed_data.get('title', 'Unknown')}")
        
        except Exception as e:
            logger.warning(f"Failed to fetch movie ID {movie_id} after all retries: {str(e)}")
            failed_ids.append(movie_id)
    
    if failed_ids:
        logger.warning(f"Failed to fetch {len(failed_ids)} movies: {failed_ids}")
    
    logger.info(f"Successfully fetched {len(movies_data)} movies")
    
    # Create DataFrame from collected data
    if not movies_data:
        logger.error("No movies were fetched successfully")
        raise ValueError("No movies were fetched from the API")
    
    # Create DataFrame with schema
    df = spark.createDataFrame(movies_data, schema=get_movie_schema())
    
    logger.info(f"Created DataFrame with {df.count()} rows")
    
    return df
