"""
Enrich Movies module for TMDB Movie Pipeline.
Adds derived metrics and calculated columns to the movie data.
"""

from pyspark.sql.functions import col, when, lit, year, month

from orchestrator.logger import get_step_logger

# Minimum budget in millions for ROI calculations
MIN_BUDGET_FOR_ROI = 10.0


def enrich_movies(df):
    """
    Add derived metrics to the cleaned movie data.
    
    Adds: profit_musd, roi, release_year, release_month
    
    Args:
        df: Cleaned PySpark DataFrame.
    
    Returns:
        Enriched PySpark DataFrame with derived columns.
    """
    logger = get_step_logger('transform')
    logger.info("Starting data enrichment")
    
    # Add all derived columns in a chain
    df = (df
        # Calculate profit (revenue - budget) in millions USD
        .withColumn('profit_musd', when(
            col('revenue_musd').isNotNull() & col('budget_musd').isNotNull(),
            col('revenue_musd') - col('budget_musd')
        ).otherwise(lit(None)))
        
        # Calculate ROI (only for movies with budget >= 10M)
        .withColumn('roi', when(
            (col('budget_musd').isNotNull()) & 
            (col('revenue_musd').isNotNull()) & 
            (col('budget_musd') >= MIN_BUDGET_FOR_ROI) &
            (col('budget_musd') > 0),
            col('revenue_musd') / col('budget_musd')
        ).otherwise(lit(None)))
        
        # Extract release year and month
        .withColumn('release_year', when(
            col('release_date').isNotNull(), year(col('release_date'))
        ).otherwise(lit(None)))
        .withColumn('release_month', when(
            col('release_date').isNotNull(), month(col('release_date'))
        ).otherwise(lit(None)))
    )
    
    logger.info("Enrichment complete")
    return df
