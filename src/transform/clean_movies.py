"""
Clean Movies module for TMDB Movie Pipeline.
Handles data cleaning and preprocessing of raw movie data.
"""

import json
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, udf, trim, regexp_replace, to_date, coalesce
)
from pyspark.sql.types import StringType, DoubleType, IntegerType

from src.utils.constants import DROP_COLUMNS, FINAL_COLUMN_ORDER, MIN_NON_NULL_COLUMNS
from orchestrator.logger import get_step_logger


# UDF to parse JSON array and extract names with pipe separator
def parse_json_names(json_str, key='name'):
    """
    Parse JSON array and extract values for a specific key.
    
    Args:
        json_str: JSON string to parse.
        key: Key to extract from each object in the array.
    
    Returns:
        Pipe-separated string of values, or None if parsing fails.
    """
    if not json_str or json_str == 'null':
        return None
    
    try:
        data = json.loads(json_str)
        if isinstance(data, list):
            names = [item.get(key, '') for item in data if item.get(key)]
            return '|'.join(names) if names else None
        elif isinstance(data, dict):
            return data.get(key)
        return None
    except (json.JSONDecodeError, TypeError):
        return None


# Register UDFs
parse_genres_udf = udf(lambda x: parse_json_names(x, 'name'), StringType())
parse_companies_udf = udf(lambda x: parse_json_names(x, 'name'), StringType())
parse_countries_udf = udf(lambda x: parse_json_names(x, 'name'), StringType())
parse_languages_udf = udf(lambda x: parse_json_names(x, 'english_name'), StringType())


def parse_collection_name(json_str):
    """
    Parse collection JSON and extract collection name.
    
    Args:
        json_str: JSON string of collection data.
    
    Returns:
        Collection name or None.
    """
    if not json_str or json_str == 'null':
        return None
    
    try:
        data = json.loads(json_str)
        if isinstance(data, dict):
            return data.get('name')
        return None
    except (json.JSONDecodeError, TypeError):
        return None


parse_collection_udf = udf(parse_collection_name, StringType())


def clean_movies(df):
    """
    Clean and preprocess raw movie data.
    
    Steps:
    1. Drop irrelevant columns
    2. Parse JSON-like columns
    3. Convert data types
    4. Handle missing/invalid values
    5. Convert budget/revenue to millions
    6. Remove duplicates
    7. Filter to released movies only
    8. Reorder columns
    
    Args:
        df: Raw PySpark DataFrame from extraction.
    
    Returns:
        Cleaned PySpark DataFrame.
    """
    logger = get_step_logger('transform')
    
    initial_count = df.count()
    logger.info(f"Starting cleaning with {initial_count} rows")
    
    # Step 1: Drop irrelevant columns
    logger.info("Dropping irrelevant columns")
    columns_to_drop = [c for c in DROP_COLUMNS if c in df.columns]
    df = df.drop(*columns_to_drop)
    
    # Step 2: Parse JSON-like columns
    logger.info("Parsing JSON columns")
    
    # Parse genres
    df = df.withColumn('genres', parse_genres_udf(col('genres')))
    
    # Parse belongs_to_collection
    df = df.withColumn('belongs_to_collection', parse_collection_udf(col('belongs_to_collection')))
    
    # Parse production_companies
    df = df.withColumn('production_companies', parse_companies_udf(col('production_companies')))
    
    # Parse production_countries
    df = df.withColumn('production_countries', parse_countries_udf(col('production_countries')))
    
    # Parse spoken_languages
    df = df.withColumn('spoken_languages', parse_languages_udf(col('spoken_languages')))
    
    # Step 3: Convert release_date to proper date type
    logger.info("Converting data types")
    df = df.withColumn('release_date', to_date(col('release_date'), 'yyyy-MM-dd'))
    
    # Step 4: Handle missing and invalid values
    logger.info("Handling missing and invalid values")
    
    # Replace 0 values with null for budget, revenue, runtime
    df = df.withColumn(
        'budget',
        when(col('budget') == 0, lit(None)).otherwise(col('budget'))
    )
    df = df.withColumn(
        'revenue',
        when(col('revenue') == 0, lit(None)).otherwise(col('revenue'))
    )
    df = df.withColumn(
        'runtime',
        when(col('runtime') == 0, lit(None)).otherwise(col('runtime'))
    )
    
    # Clean overview and tagline - replace empty or placeholder values with null
    df = df.withColumn(
        'overview',
        when(
            (col('overview').isNull()) | 
            (trim(col('overview')) == '') | 
            (col('overview') == 'No Data'),
            lit(None)
        ).otherwise(col('overview'))
    )
    df = df.withColumn(
        'tagline',
        when(
            (col('tagline').isNull()) | 
            (trim(col('tagline')) == '') | 
            (col('tagline') == 'No Data'),
            lit(None)
        ).otherwise(col('tagline'))
    )
    
    # Step 5: Convert budget and revenue to millions USD
    logger.info("Converting budget/revenue to millions USD")
    df = df.withColumn(
        'budget_musd',
        when(col('budget').isNotNull(), col('budget') / 1000000.0).otherwise(lit(None))
    )
    df = df.withColumn(
        'revenue_musd',
        when(col('revenue').isNotNull(), col('revenue') / 1000000.0).otherwise(lit(None))
    )
    
    # Drop original budget and revenue columns
    df = df.drop('budget', 'revenue')
    
    # Step 6: Remove duplicates
    logger.info("Removing duplicates")
    before_dedup = df.count()
    df = df.dropDuplicates(['id'])
    after_dedup = df.count()
    logger.info(f"Removed {before_dedup - after_dedup} duplicate rows")
    
    # Step 7: Drop rows with unknown id or title
    df = df.filter(col('id').isNotNull() & col('title').isNotNull())
    
    # Step 8: Filter to only Released movies
    logger.info("Filtering to released movies only")
    if 'status' in df.columns:
        before_filter = df.count()
        df = df.filter(col('status') == 'Released')
        after_filter = df.count()
        logger.info(f"Filtered out {before_filter - after_filter} non-released movies")
        df = df.drop('status')
    
    # Step 9: Reorder columns
    logger.info("Reordering columns")
    available_columns = [c for c in FINAL_COLUMN_ORDER if c in df.columns]
    remaining_columns = [c for c in df.columns if c not in available_columns]
    final_columns = available_columns + remaining_columns
    df = df.select(*final_columns)
    
    final_count = df.count()
    logger.info(f"Cleaning complete. {initial_count} -> {final_count} rows")
    
    return df
