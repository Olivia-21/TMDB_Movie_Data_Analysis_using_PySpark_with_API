"""
Advanced Filters module for TMDB Movie Pipeline.
Provides specialized search queries for finding specific movies.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, desc, asc

from orchestrator.logger import get_step_logger


def search_by_genre_and_actor(df, genres, actor_name, sort_by='vote_average', ascending=False):
    """
    Search for movies matching specific genres and featuring a specific actor.
    
    Args:
        df: PySpark DataFrame.
        genres: List of genre names to match (all must be present).
        actor_name: Name of actor to search for in cast.
        sort_by: Column to sort results by.
        ascending: Sort order.
    
    Returns:
        Filtered and sorted DataFrame.
    """
    result_df = df
    
    # Filter by genres (case-insensitive, all genres must be present)
    for genre in genres:
        result_df = result_df.filter(
            lower(col('genres')).contains(genre.lower())
        )
    
    # Filter by actor (case-insensitive)
    result_df = result_df.filter(
        lower(col('cast')).contains(actor_name.lower())
    )
    
    # Sort results
    if ascending:
        result_df = result_df.orderBy(asc(sort_by))
    else:
        result_df = result_df.orderBy(desc(sort_by))
    
    return result_df


def search_by_actor_and_director(df, actor_name, director_name, sort_by='runtime', ascending=True):
    """
    Search for movies starring a specific actor and directed by a specific director.
    
    Args:
        df: PySpark DataFrame.
        actor_name: Name of actor to search for.
        director_name: Name of director to search for.
        sort_by: Column to sort results by.
        ascending: Sort order.
    
    Returns:
        Filtered and sorted DataFrame.
    """
    result_df = df
    
    # Filter by actor (case-insensitive)
    result_df = result_df.filter(
        lower(col('cast')).contains(actor_name.lower())
    )
    
    # Filter by director (case-insensitive)
    result_df = result_df.filter(
        lower(col('director')).contains(director_name.lower())
    )
    
    # Sort results
    if ascending:
        result_df = result_df.orderBy(asc(sort_by))
    else:
        result_df = result_df.orderBy(desc(sort_by))
    
    return result_df


def search_scifi_action_bruce_willis(df):
    """
    Search 1: Find best-rated Science Fiction Action movies starring Bruce Willis.
    Results sorted by rating (highest to lowest).
    
    Args:
        df: PySpark DataFrame.
    
    Returns:
        Filtered DataFrame sorted by vote_average descending.
    """
    logger = get_step_logger('analysis')
    logger.info("Search 1: Science Fiction + Action movies starring Bruce Willis")
    
    result_df = search_by_genre_and_actor(
        df,
        genres=['Science Fiction', 'Action'],
        actor_name='Bruce Willis',
        sort_by='vote_average',
        ascending=False
    )
    
    count = result_df.count()
    logger.info(f"  Found {count} matching movies")
    
    return result_df


def search_uma_thurman_tarantino(df):
    """
    Search 2: Find movies starring Uma Thurman directed by Quentin Tarantino.
    Results sorted by runtime (shortest to longest).
    
    Args:
        df: PySpark DataFrame.
    
    Returns:
        Filtered DataFrame sorted by runtime ascending.
    """
    logger = get_step_logger('analysis')
    logger.info("Search 2: Uma Thurman movies directed by Quentin Tarantino")
    
    result_df = search_by_actor_and_director(
        df,
        actor_name='Uma Thurman',
        director_name='Quentin Tarantino',
        sort_by='runtime',
        ascending=True
    )
    
    count = result_df.count()
    logger.info(f"  Found {count} matching movies")
    
    return result_df
