"""
Validators module for TMDB Movie Pipeline.
Provides schema and data validation helpers.
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def get_raw_movie_schema():
    """
    Get the expected schema for raw movie data.
    
    Returns:
        PySpark StructType defining the raw movie schema.
    """
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("status", StringType(), True),
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


def validate_movie_schema(df, required_columns=None):
    """
    Validate that a DataFrame contains all required columns.
    
    Args:
        df: PySpark DataFrame to validate.
        required_columns: List of required column names. If None, uses default set.
    
    Returns:
        Tuple of (is_valid, missing_columns).
    """
    if required_columns is None:
        required_columns = ['id', 'title', 'release_date', 'budget', 'revenue']
    
    existing_columns = set(df.columns)
    missing_columns = [col for col in required_columns if col not in existing_columns]
    
    return len(missing_columns) == 0, missing_columns


def validate_numeric_ranges(df, logger=None):
    """
    Validate that numeric columns contain valid values.
    
    Args:
        df: PySpark DataFrame to validate.
        logger: Optional logger for reporting issues.
    
    Returns:
        Dictionary with validation results for each column.
    """
    validation_results = {}
    
    # Check for negative values in columns that should be non-negative
    non_negative_columns = ['budget', 'revenue', 'runtime', 'vote_count', 'popularity']
    
    for col_name in non_negative_columns:
        if col_name in df.columns:
            negative_count = df.filter(f"{col_name} < 0").count()
            validation_results[col_name] = {
                'has_negatives': negative_count > 0,
                'negative_count': negative_count
            }
            
            if logger and negative_count > 0:
                logger.warning(
                    f"Column '{col_name}' has {negative_count} rows with negative values"
                )
    
    # Check vote_average is in valid range (0-10)
    if 'vote_average' in df.columns:
        invalid_ratings = df.filter(
            "(vote_average < 0 OR vote_average > 10)"
        ).count()
        validation_results['vote_average'] = {
            'has_invalid': invalid_ratings > 0,
            'invalid_count': invalid_ratings
        }
        
        if logger and invalid_ratings > 0:
            logger.warning(
                f"Column 'vote_average' has {invalid_ratings} rows outside 0-10 range"
            )
    
    return validation_results


def validate_dataframe_not_empty(df, step_name, logger=None):
    """
    Validate that a DataFrame is not empty.
    
    Args:
        df: PySpark DataFrame to validate.
        step_name: Name of the step for error messages.
        logger: Optional logger for reporting.
    
    Returns:
        True if DataFrame has rows, False otherwise.
    
    Raises:
        ValueError: If DataFrame is empty.
    """
    count = df.count()
    
    if count == 0:
        error_msg = f"{step_name}: DataFrame is empty"
        if logger:
            logger.error(error_msg)
        raise ValueError(error_msg)
    
    if logger:
        logger.info(f"{step_name}: DataFrame has {count} rows")
    
    return True
