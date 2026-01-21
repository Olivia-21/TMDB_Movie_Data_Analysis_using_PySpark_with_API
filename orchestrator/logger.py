"""
Logging module for TMDB Movie Pipeline.
Provides centralized logging configuration for all pipeline components.
"""

import logging
import os
from datetime import datetime


def setup_logger(name, log_file=None, level=logging.INFO):
    """
    Create and configure a logger instance.
    
    Args:
        name: Logger name (typically the module name).
        log_file: Optional path to log file. If None, only console logging is used.
        level: Logging level (default: INFO).
    
    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Clear existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if log file path provided)
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_pipeline_logger():
    """
    Get the main pipeline logger instance.
    
    Returns:
        Logger configured for pipeline-level logging.
    """
    log_file = os.path.join('logs', 'pipeline.log')
    return setup_logger('pipeline', log_file)


def get_step_logger(step_name):
    """
    Get a logger for a specific pipeline step.
    
    Args:
        step_name: Name of the pipeline step (e.g., 'extract', 'transform').
    
    Returns:
        Logger configured for step-level logging.
    """
    log_file = os.path.join('logs', f'{step_name}.log')
    return setup_logger(step_name, log_file)
