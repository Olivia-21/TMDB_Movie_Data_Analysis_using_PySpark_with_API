"""
Constants module for TMDB Movie Pipeline.
Contains shared constants used across the pipeline.
"""

# Movie IDs to fetch from TMDB API
MOVIE_IDS = [
    299534,   
    19995,    
    140607,   
    299536,   
    597,      
    135397,   
    420818,   
    24428,    
    168259,   
    99861,    
    284054,   
    12445,   
    181808,   
    330457,   
    351286,  
    109445,  
    321612,   
    260513,   
]

# Columns to drop during data cleaning
DROP_COLUMNS = [
    'adult',
    'imdb_id',
    'original_title',
    'video',
    'homepage'
]

# JSON-like columns that need parsing
JSON_COLUMNS = [
    'belongs_to_collection',
    'genres',
    'production_countries',
    'production_companies',
    'spoken_languages'
]

# Final column order for cleaned data
FINAL_COLUMN_ORDER = [
    'id',
    'title',
    'tagline',
    'release_date',
    'genres',
    'belongs_to_collection',
    'original_language',
    'budget_musd',
    'revenue_musd',
    'production_companies',
    'production_countries',
    'vote_count',
    'vote_average',
    'popularity',
    'runtime',
    'overview',
    'spoken_languages',
    'poster_path',
    'cast',
    'cast_size',
    'director',
    'crew_size'
]

# Numeric columns that should be converted
NUMERIC_COLUMNS = [
    'budget',
    'id',
    'popularity',
    'revenue',
    'runtime',
    'vote_average',
    'vote_count'
]

# Minimum non-null columns required to keep a row
MIN_NON_NULL_COLUMNS = 10

# Minimum budget in millions for ROI calculations
MIN_BUDGET_FOR_ROI = 10.0

# Minimum votes required for rating-based rankings
MIN_VOTES_FOR_RATING = 10
