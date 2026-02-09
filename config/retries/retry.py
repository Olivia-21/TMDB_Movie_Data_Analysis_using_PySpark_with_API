"""
Retry module for TMDB Movie Pipeline.
Provides retry logic with exponential backoff for handling transient failures.
"""

import time
import logging


def run_with_retry(func, retries=3, delay=1, backoff=2, logger=None, step_name="Operation"):
    """
    Execute a function with retry logic and exponential backoff.
    
    Args:
        func: Callable to execute. Should be a zero-argument function (use lambda for args).
        retries: Maximum number of retry attempts (default: 3).
        delay: Initial delay between retries in seconds (default: 1).
        backoff: Multiplier for delay after each retry (default: 2).
        logger: Logger instance for logging attempts and failures.
        step_name: Name of the step for logging purposes.
    
    Returns:
        Result of the function call if successful.
    
    Raises:
        Exception: Re-raises the last exception if all retries are exhausted.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    current_delay = delay
    last_exception = None
    
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"{step_name}: Attempt {attempt} of {retries}")
            result = func()
            logger.info(f"{step_name}: Completed successfully on attempt {attempt}")
            return result
        
        except Exception as e:
            last_exception = e
            logger.warning(
                f"{step_name}: Attempt {attempt} failed with error: {str(e)}"
            )
            
            if attempt < retries:
                logger.info(f"{step_name}: Retrying in {current_delay} seconds...")
                time.sleep(current_delay)
                current_delay *= backoff
            else:
                logger.error(
                    f"{step_name}: All {retries} attempts failed. Last error: {str(e)}"
                )
    
    # Re-raise the last exception after all retries are exhausted
    raise last_exception
