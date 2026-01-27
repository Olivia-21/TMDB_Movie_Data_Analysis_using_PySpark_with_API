"""
Clean Movies module for TMDB Movie Pipeline.
Handles data cleaning and preprocessing of raw movie data.
"""

import json
from pyspark.sql.functions import col, when, lit, udf, trim, to_date
from pyspark.sql.types import StringType

from orchestrator.logger import get_step_logger

# Columns to drop during data cleaning
DROP_COLUMNS = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']

# Final column order for cleaned data
FINAL_COLUMN_ORDER = [
    'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
    'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
    'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
    'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size'
]


def parse_json_field(json_str, key='name'):
    """Parse JSON and extract values for a specific key."""
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


# Single parameterized UDF factory
def make_json_udf(key='name'):
    return udf(lambda x: parse_json_field(x, key), StringType())


def clean_movies(df):
    """
    Clean and preprocess raw movie data.
    
    Args:
        df: Raw PySpark DataFrame from extraction.
    
    Returns:
        Cleaned PySpark DataFrame.
    """
    logger = get_step_logger('transform')
    logger.info("Starting data cleaning")
    
    # Step 1: Drop irrelevant columns
    columns_to_drop = [c for c in DROP_COLUMNS if c in df.columns]
    df = df.drop(*columns_to_drop)
    
    # Step 2: Parse JSON columns using single UDF factory
    df = (df
        .withColumn('genres', make_json_udf('name')(col('genres')))
        .withColumn('belongs_to_collection', make_json_udf('name')(col('belongs_to_collection')))
        .withColumn('production_companies', make_json_udf('name')(col('production_companies')))
        .withColumn('production_countries', make_json_udf('name')(col('production_countries')))
        .withColumn('spoken_languages', make_json_udf('english_name')(col('spoken_languages')))
    )
    
    # Step 3: Convert release_date to proper date type
    df = df.withColumn('release_date', to_date(col('release_date'), 'yyyy-MM-dd'))
    
    # Step 4: Handle missing/invalid values - replace 0 with null, clean text fields
    df = (df
        .withColumn('budget', when(col('budget') == 0, lit(None)).otherwise(col('budget')))
        .withColumn('revenue', when(col('revenue') == 0, lit(None)).otherwise(col('revenue')))
        .withColumn('runtime', when(col('runtime') == 0, lit(None)).otherwise(col('runtime')))
        .withColumn('overview', when(
            (col('overview').isNull()) | (trim(col('overview')) == '') | (col('overview') == 'No Data'),
            lit(None)
        ).otherwise(col('overview')))
        .withColumn('tagline', when(
            (col('tagline').isNull()) | (trim(col('tagline')) == '') | (col('tagline') == 'No Data'),
            lit(None)
        ).otherwise(col('tagline')))
    )
    
    # Step 5: Convert budget/revenue to millions USD and drop originals
    df = (df
        .withColumn('budget_musd', col('budget') / 1000000.0)
        .withColumn('revenue_musd', col('revenue') / 1000000.0)
        .drop('budget', 'revenue')
    )
    
    # Step 6: Remove duplicates and filter invalid rows
    df = (df
        .dropDuplicates(['id'])
        .filter(col('id').isNotNull() & col('title').isNotNull())
    )
    
    # Step 7: Filter to released movies only
    if 'status' in df.columns:
        df = df.filter(col('status') == 'Released').drop('status')
    
    # Step 8: Reorder columns
    available_columns = [c for c in FINAL_COLUMN_ORDER if c in df.columns]
    remaining_columns = [c for c in df.columns if c not in available_columns]
    df = df.select(*(available_columns + remaining_columns))
    
    # Single count at the end
    logger.info(f"Cleaning complete. Final row count: {df.count()}")
    
    return df
