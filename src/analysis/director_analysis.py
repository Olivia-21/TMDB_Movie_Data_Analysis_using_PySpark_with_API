"""
Director Analysis module for TMDB Movie Pipeline.
Analyzes director performance based on their filmography.
"""

from pyspark.sql.functions import col, count, sum as spark_sum, avg, desc

from config.logger.logger import get_step_logger


def get_top_directors(df, n=10):
    """
    Find the most successful directors based on their filmography.
    
    Metrics reported per director:
    - Total number of movies directed
    - Total revenue generated
    - Mean rating of their movies
    - Mean popularity
    
    Args:
        df: Enriched PySpark DataFrame with director information.
        n: Number of top directors to return.
    
    Returns:
        PySpark DataFrame with director statistics.
    """
    logger = get_step_logger('analysis')
    logger.info(f"Finding top {n} directors")
    
    # Filter to movies with director information
    director_df = df.filter(col('director').isNotNull())
    
    # Aggregate by director
    director_stats = director_df.groupBy('director').agg(
        count('*').alias('movie_count'),
        spark_sum('revenue_musd').alias('total_revenue_musd'),
        avg('revenue_musd').alias('mean_revenue_musd'),
        spark_sum('budget_musd').alias('total_budget_musd'),
        avg('vote_average').alias('mean_rating'),
        avg('popularity').alias('mean_popularity'),
        avg('roi').alias('mean_roi')
    )
    
    # Sort by total revenue and limit
    top_directors = director_stats.orderBy(desc('total_revenue_musd')).limit(n)
    
    # Log summary
    total_directors = director_stats.count()
    logger.info(f"  Found {total_directors} directors in dataset")
    logger.info(f"  Returning top {n} by total revenue")
    
    return top_directors


def get_director_filmography(df, director_name):
    """
    Get the complete filmography for a specific director.
    
    Args:
        df: PySpark DataFrame with movie data.
        director_name: Name of the director to search for.
    
    Returns:
        DataFrame with all movies directed by the specified director.
    """
    logger = get_step_logger('analysis')
    logger.info(f"Getting filmography for director: {director_name}")
    
    # Case-insensitive search for director
    filmography = df.filter(
        col('director').isNotNull()
    ).filter(
        col('director').contains(director_name)
    ).orderBy(desc('release_date'))
    
    count = filmography.count()
    logger.info(f"  Found {count} movies by {director_name}")
    
    return filmography
