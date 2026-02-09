"""
KPI Rankings module for TMDB Movie Pipeline.
Provides functions for ranking movies based on various performance metrics.
"""

from pyspark.sql.functions import col, desc, asc

from config.logger.logger import get_step_logger

# Minimum budget in millions for ROI calculations
MIN_BUDGET_FOR_ROI = 10.0

# Minimum votes required for rating-based rankings
MIN_VOTES_FOR_RATING = 10


def get_top_movies_by(df, column, n=10, ascending=False, filter_condition=None):
    """
    Get top N movies ranked by a specific column.
    
    Args:
        df: PySpark DataFrame.
        column: Column name to rank by.
        n: Number of top movies to return.
        ascending: If True, return lowest values first.
        filter_condition: Optional filter condition to apply before ranking.
    
    Returns:
        PySpark DataFrame with top N movies.
    """
    result_df = df
    
    # Apply filter if provided
    if filter_condition is not None:
        result_df = result_df.filter(filter_condition)
    
    # Filter out nulls in the ranking column
    result_df = result_df.filter(col(column).isNotNull())
    
    # Sort and limit
    if ascending:
        result_df = result_df.orderBy(asc(column))
    else:
        result_df = result_df.orderBy(desc(column))
    
    return result_df.limit(n)


def get_highest_revenue_movies(df, n=10):
    """Get movies with highest revenue."""
    return get_top_movies_by(df, 'revenue_musd', n, ascending=False)


def get_highest_budget_movies(df, n=10):
    """Get movies with highest budget."""
    return get_top_movies_by(df, 'budget_musd', n, ascending=False)


def get_highest_profit_movies(df, n=10):
    """Get movies with highest profit (revenue - budget)."""
    return get_top_movies_by(df, 'profit_musd', n, ascending=False)


def get_lowest_profit_movies(df, n=10):
    """Get movies with lowest profit (biggest losses)."""
    return get_top_movies_by(df, 'profit_musd', n, ascending=True)


def get_highest_roi_movies(df, n=10):
    """
    Get movies with highest ROI.
    Only includes movies with budget >= 10M.
    """
    filter_cond = col('budget_musd') >= MIN_BUDGET_FOR_ROI
    return get_top_movies_by(df, 'roi', n, ascending=False, filter_condition=filter_cond)


def get_lowest_roi_movies(df, n=10):
    """
    Get movies with lowest ROI.
    Only includes movies with budget >= 10M.
    """
    filter_cond = col('budget_musd') >= MIN_BUDGET_FOR_ROI
    return get_top_movies_by(df, 'roi', n, ascending=True, filter_condition=filter_cond)


def get_most_voted_movies(df, n=10):
    """Get movies with most votes."""
    return get_top_movies_by(df, 'vote_count', n, ascending=False)


def get_highest_rated_movies(df, n=10):
    """
    Get highest rated movies.
    Only includes movies with >= 10 votes.
    """
    filter_cond = col('vote_count') >= MIN_VOTES_FOR_RATING
    return get_top_movies_by(df, 'vote_average', n, ascending=False, filter_condition=filter_cond)


def get_lowest_rated_movies(df, n=10):
    """
    Get lowest rated movies.
    Only includes movies with >= 10 votes.
    """
    filter_cond = col('vote_count') >= MIN_VOTES_FOR_RATING
    return get_top_movies_by(df, 'vote_average', n, ascending=True, filter_condition=filter_cond)


def get_most_popular_movies(df, n=10):
    """Get most popular movies based on TMDB popularity score."""
    return get_top_movies_by(df, 'popularity', n, ascending=False)


def get_best_worst_performers(df, n=10):
    """
    Get all best and worst performing movies across different metrics.
    
    Args:
        df: Enriched PySpark DataFrame.
        n: Number of movies for each category.
    
    Returns:
        Dictionary containing DataFrames for each ranking category.
    """
    logger = get_step_logger('analysis')
    logger.info("Calculating best/worst performers across all metrics")
    
    results = {
        'highest_revenue': get_highest_revenue_movies(df, n),
        'highest_budget': get_highest_budget_movies(df, n),
        'highest_profit': get_highest_profit_movies(df, n),
        'lowest_profit': get_lowest_profit_movies(df, n),
        'highest_roi': get_highest_roi_movies(df, n),
        'lowest_roi': get_lowest_roi_movies(df, n),
        'most_voted': get_most_voted_movies(df, n),
        'highest_rated': get_highest_rated_movies(df, n),
        'lowest_rated': get_lowest_rated_movies(df, n),
        'most_popular': get_most_popular_movies(df, n),
    }
    
    for category, result_df in results.items():
        count = result_df.count()
        logger.info(f"  {category}: {count} movies")
    
    return results
