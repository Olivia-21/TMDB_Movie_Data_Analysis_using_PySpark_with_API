"""
Enrich Movies module for TMDB Movie Pipeline.
Adds derived metrics and calculated columns to the movie data.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, year, month

from src.utils.constants import MIN_BUDGET_FOR_ROI
from orchestrator.logger import get_step_logger


def enrich_movies(df):
    """
    Add derived metrics to the cleaned movie data.
    
    Adds the following columns:
    - profit_musd: Revenue minus Budget (in millions)
    - roi: Return on Investment (Revenue / Budget) for movies with budget >= 10M
    - release_year: Year extracted from release_date
    - release_month: Month extracted from release_date
    
    Args:
        df: Cleaned PySpark DataFrame.
    
    Returns:
        Enriched PySpark DataFrame with derived columns.
    """
    logger = get_step_logger('transform')
    logger.info("Starting data enrichment")
    
    # Calculate profit (revenue - budget) in millions USD
    logger.info("Calculating profit")
    df = df.withColumn(
        'profit_musd',
        when(
            col('revenue_musd').isNotNull() & col('budget_musd').isNotNull(),
            col('revenue_musd') - col('budget_musd')
        ).otherwise(lit(None))
    )
    
    # Calculate ROI (only for movies with budget >= 10M)
    logger.info("Calculating ROI for movies with budget >= 10M")
    df = df.withColumn(
        'roi',
        when(
            (col('budget_musd').isNotNull()) & 
            (col('revenue_musd').isNotNull()) & 
            (col('budget_musd') >= MIN_BUDGET_FOR_ROI) &
            (col('budget_musd') > 0),
            col('revenue_musd') / col('budget_musd')
        ).otherwise(lit(None))
    )
    
    # Extract release year and month
    logger.info("Extracting release year and month")
    df = df.withColumn(
        'release_year',
        when(col('release_date').isNotNull(), year(col('release_date'))).otherwise(lit(None))
    )
    df = df.withColumn(
        'release_month',
        when(col('release_date').isNotNull(), month(col('release_date'))).otherwise(lit(None))
    )
    
    # Log summary statistics
    total_count = df.count()
    with_profit = df.filter(col('profit_musd').isNotNull()).count()
    with_roi = df.filter(col('roi').isNotNull()).count()
    
    logger.info(f"Enrichment complete:")
    logger.info(f"  - Total movies: {total_count}")
    logger.info(f"  - Movies with profit calculated: {with_profit}")
    logger.info(f"  - Movies with ROI calculated: {with_roi}")
    
    return df
