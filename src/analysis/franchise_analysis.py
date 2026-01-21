"""
Franchise Analysis module for TMDB Movie Pipeline.
Analyzes performance differences between franchise and standalone movies.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum, avg, 
    percentile_approx, desc
)

from orchestrator.logger import get_step_logger


def compare_franchise_vs_standalone(df):
    """
    Compare franchise movies vs standalone movies on key metrics.
    
    Metrics compared:
    - Mean Revenue
    - Median ROI
    - Mean Budget
    - Mean Popularity
    - Mean Rating
    
    Args:
        df: Enriched PySpark DataFrame.
    
    Returns:
        PySpark DataFrame with comparison statistics.
    """
    logger = get_step_logger('analysis')
    logger.info("Comparing franchise vs standalone movie performance")
    
    # Add is_franchise flag
    df_with_flag = df.withColumn(
        'is_franchise',
        when(col('belongs_to_collection').isNotNull(), lit('Franchise')).otherwise(lit('Standalone'))
    )
    
    # Calculate aggregate statistics
    comparison_df = df_with_flag.groupBy('is_franchise').agg(
        count('*').alias('movie_count'),
        avg('revenue_musd').alias('mean_revenue_musd'),
        percentile_approx('roi', 0.5).alias('median_roi'),
        avg('budget_musd').alias('mean_budget_musd'),
        avg('popularity').alias('mean_popularity'),
        avg('vote_average').alias('mean_rating')
    )
    
    # Log results
    results = comparison_df.collect()
    for row in results:
        logger.info(f"  {row['is_franchise']}:")
        logger.info(f"    - Movie count: {row['movie_count']}")
        logger.info(f"    - Mean revenue: ${row['mean_revenue_musd']:.2f}M" if row['mean_revenue_musd'] else "    - Mean revenue: N/A")
        logger.info(f"    - Median ROI: {row['median_roi']:.2f}x" if row['median_roi'] else "    - Median ROI: N/A")
    
    return comparison_df


def get_top_franchises(df, n=10):
    """
    Find the most successful movie franchises.
    
    Metrics reported per franchise:
    - Total number of movies
    - Total and Mean Budget
    - Total and Mean Revenue
    - Mean Rating
    
    Args:
        df: Enriched PySpark DataFrame.
        n: Number of top franchises to return.
    
    Returns:
        PySpark DataFrame with franchise statistics.
    """
    logger = get_step_logger('analysis')
    logger.info(f"Finding top {n} movie franchises")
    
    # Filter to only franchise movies
    franchise_df = df.filter(col('belongs_to_collection').isNotNull())
    
    # Aggregate by franchise
    franchise_stats = franchise_df.groupBy('belongs_to_collection').agg(
        count('*').alias('movie_count'),
        spark_sum('budget_musd').alias('total_budget_musd'),
        avg('budget_musd').alias('mean_budget_musd'),
        spark_sum('revenue_musd').alias('total_revenue_musd'),
        avg('revenue_musd').alias('mean_revenue_musd'),
        avg('vote_average').alias('mean_rating'),
        avg('popularity').alias('mean_popularity')
    )
    
    # Sort by total revenue and limit
    top_franchises = franchise_stats.orderBy(desc('total_revenue_musd')).limit(n)
    
    # Log summary
    logger.info(f"  Found {franchise_stats.count()} total franchises")
    logger.info(f"  Returning top {n} by total revenue")
    
    return top_franchises
