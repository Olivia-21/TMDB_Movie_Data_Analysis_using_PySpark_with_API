"""
Main Pipeline Orchestrator for TMDB Movie Pipeline.
Entry point that coordinates all ETL stages and analysis.
"""

import os
import sys
import time
import yaml
import glob
import shutil

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession

from config.logger.logger import get_pipeline_logger
from config.retries.retry import run_with_retry

from src.extract.fetch_movies import fetch_movies
from src.transform.clean_movies import clean_movies
from src.transform.derived_metrics import add_derived_metrics
from src.analysis.kpi_rankings import get_best_worst_performers
from src.analysis.advanced_filters import (
    search_scifi_action_bruce_willis,
    search_uma_thurman_tarantino
)
from src.analysis.franchise_analysis import (
    compare_franchise_vs_standalone,
    get_top_franchises
)
from src.analysis.director_analysis import get_top_directors
from src.visualization.plots import create_all_visualizations


def validate_dataframe_not_empty(df, step_name, logger):
    """Validate that a DataFrame is not empty.
    """
    if df.isEmpty():
        raise ValueError(f"{step_name}: DataFrame is empty")
    logger.info(f"{step_name}: DataFrame validation passed (not empty)")
    return True


def load_config(config_path='config/settings.yaml'):
    """Load pipeline configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_spark_session(config):
    """
    Create and configure SparkSession.
    
    Args:
        config: Configuration dictionary.
    
    Returns:
        Configured SparkSession.
    """
    return SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .master(config['spark']['master']) \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()


def ensure_directories(config):
    """Ensure all required directories exist."""
    directories = [
        os.path.dirname(config['paths']['raw_data']),
        os.path.dirname(config['paths']['processed_data']),
        os.path.dirname(config['paths']['analytics_data']),
        config['paths']['visualizations'],
        config['paths']['logs']
    ]
    
    for directory in directories:
        if directory and not os.path.exists(directory):
            os.makedirs(directory)


def save_dataframe(df, path, logger):
    """
    Save PySpark DataFrame to CSV.
    
    Args:
        df: PySpark DataFrame.
        path: Output file path.
        logger: Logger instance.
    """
    logger.info(f"Saving DataFrame to {path}")
    
    # Convert to single partition for single CSV file
    df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(path + '_temp')
    
    # Rename the part file to the target name
    temp_dir = path + '_temp'
    part_file = glob.glob(os.path.join(temp_dir, 'part-*.csv'))[0]
    
    # Move and rename
    shutil.move(part_file, path)
    shutil.rmtree(temp_dir)
    
    logger.info(f"Saved to {path}")


def main():
    """
    Main pipeline orchestration function.
    Executes all ETL stages in sequence with retry logic.
    """
    # Initialize
    start_time = time.time()
    logger = get_pipeline_logger()
    logger.info("=" * 60)
    logger.info("TMDB Movie Pipeline - Starting")
    logger.info("=" * 60)
    
    try:
        # Load configuration
        config = load_config()
        ensure_directories(config)
        
        # Create Spark session
        logger.info("Creating Spark session")
        spark = create_spark_session(config)
        logger.info("Spark session created successfully")
        
        # ==========================================
        # STEP 1: EXTRACT
        # ==========================================
        logger.info("-" * 40)
        logger.info("STEP 1: EXTRACT - Fetching movie data from TMDB API")
        logger.info("-" * 40)
        
        raw_df = run_with_retry(
            lambda: fetch_movies(spark, config['movie_ids']),
            retries=3,
            delay=2,
            logger=logger,
            step_name="API Extraction"
        )
        
        validate_dataframe_not_empty(raw_df, "Extraction", logger)
        save_dataframe(raw_df, config['paths']['raw_data'], logger)
        
        # ==========================================
        # STEP 2: TRANSFORM - CLEAN
        # ==========================================
        logger.info("-" * 40)
        logger.info("STEP 2: TRANSFORM - Cleaning movie data")
        logger.info("-" * 40)
        
        cleaned_df = run_with_retry(
            lambda: clean_movies(raw_df),
            retries=2,
            delay=1,
            logger=logger,
            step_name="Data Cleaning"
        )
        
        validate_dataframe_not_empty(cleaned_df, "Cleaning", logger)
        save_dataframe(cleaned_df, config['paths']['processed_data'], logger)
        
        # ==========================================
        # STEP 3: TRANSFORM - ENRICH
        # ==========================================
        logger.info("-" * 40)
        logger.info("STEP 3: TRANSFORM - Enriching movie data")
        logger.info("-" * 40)
        
        enriched_df = run_with_retry(
            lambda: add_derived_metrics(cleaned_df),
            retries=2,
            delay=1,
            logger=logger,
            step_name="Data Enrichment"
        )
        
        validate_dataframe_not_empty(enriched_df, "Enrichment", logger)
        save_dataframe(enriched_df, config['paths']['analytics_data'], logger)
        
        # ==========================================
        # STEP 4: ANALYSIS
        # ==========================================
        logger.info("-" * 40)
        logger.info("STEP 4: ANALYSIS - Computing KPIs and rankings")
        logger.info("-" * 40)
        
        # Best/Worst Performers
        logger.info("Computing best/worst performers...")
        rankings = get_best_worst_performers(enriched_df)
        
        # Log top performers
        for category, ranking_df in rankings.items():
            logger.info(f"\n{category.upper()}:")
            top_movies = ranking_df.select('title').limit(3).collect()
            for i, row in enumerate(top_movies, 1):
                logger.info(f"  {i}. {row['title']}")
        
        # Advanced Filters
        logger.info("\nRunning advanced search queries...")
        bruce_willis_movies = search_scifi_action_bruce_willis(enriched_df)
        tarantino_movies = search_uma_thurman_tarantino(enriched_df)
        
        # Franchise Analysis
        logger.info("\nAnalyzing franchise performance...")
        franchise_comparison = compare_franchise_vs_standalone(enriched_df)
        top_franchises = get_top_franchises(enriched_df, n=5)
        
        # Director Analysis
        logger.info("\nAnalyzing director performance...")
        top_directors = get_top_directors(enriched_df, n=5)
        
        # ==========================================
        # STEP 5: VISUALIZATION
        # ==========================================
        logger.info("-" * 40)
        logger.info("STEP 5: VISUALIZATION - Creating charts")
        logger.info("-" * 40)
        
        plot_paths = create_all_visualizations(
            enriched_df,
            config['paths']['visualizations'],
            comparison_df=franchise_comparison
        )
        
        for path in plot_paths:
            logger.info(f"  Created: {path}")
        
        # ==========================================
        # COMPLETE
        # ==========================================
        elapsed_time = time.time() - start_time
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Total execution time: {elapsed_time:.2f} seconds")
        logger.info("=" * 60)
        
        # Clean up
        spark.stop()
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        logger.exception("Full traceback:")
        return 1


if __name__ == "__main__":
    sys.exit(main())
