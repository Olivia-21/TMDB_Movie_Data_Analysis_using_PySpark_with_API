"""
Plots module for TMDB Movie Pipeline.
Creates visualizations for movie data analysis using Matplotlib.
"""

import os
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for Docker

from config.logger.logger import get_step_logger


def setup_plot_style():
    """Configure consistent plot style for all visualizations."""
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.rcParams['figure.figsize'] = (10, 6)
    plt.rcParams['font.size'] = 10
    plt.rcParams['axes.titlesize'] = 14
    plt.rcParams['axes.labelsize'] = 12


def plot_revenue_vs_budget(df_pandas, output_path):
    """
    Create scatter plot of Revenue vs Budget.
    
    Args:
        df_pandas: Pandas DataFrame with movie data.
        output_path: Path to save the plot image.
    """
    setup_plot_style()
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Filter data with valid budget and revenue
    plot_data = df_pandas[
        (df_pandas['budget_musd'].notna()) & 
        (df_pandas['revenue_musd'].notna())
    ].copy()
    
    scatter = ax.scatter(
        plot_data['budget_musd'],
        plot_data['revenue_musd'],
        c=plot_data['vote_average'],
        cmap='viridis',
        alpha=0.7,
        s=100
    )
    
    # Add break-even line
    max_val = max(plot_data['budget_musd'].max(), plot_data['revenue_musd'].max())
    ax.plot([0, max_val], [0, max_val], 'r--', label='Break-even', alpha=0.5)
    
    ax.set_xlabel('Budget (Million USD)')
    ax.set_ylabel('Revenue (Million USD)')
    ax.set_title('Movie Revenue vs Budget')
    
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label('Rating')
    
    ax.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def plot_roi_by_genre(df_pandas, output_path):
    """
    Create bar chart of average ROI by genre.
    
    Args:
        df_pandas: Pandas DataFrame with movie data.
        output_path: Path to save the plot image.
    """
    setup_plot_style()
    
    # Explode genres (pipe-separated)
    plot_data = df_pandas[df_pandas['genres'].notna()].copy()
    
    # Split genres and explode
    genre_roi = []
    for _, row in plot_data.iterrows():
        if row['roi'] is not None and row['genres']:
            for genre in str(row['genres']).split('|'):
                genre_roi.append({'genre': genre.strip(), 'roi': row['roi']})
    
    if not genre_roi:
        # Create empty plot with message if no data
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, 'No ROI data available for genres', 
                ha='center', va='center', transform=ax.transAxes)
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        return
    
    import pandas as pd
    genre_df = pd.DataFrame(genre_roi)
    avg_roi = genre_df.groupby('genre')['roi'].mean().sort_values(ascending=False)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    bars = ax.bar(range(len(avg_roi)), avg_roi.values, color='steelblue')
    ax.set_xticks(range(len(avg_roi)))
    ax.set_xticklabels(avg_roi.index, rotation=45, ha='right')
    
    ax.set_xlabel('Genre')
    ax.set_ylabel('Average ROI')
    ax.set_title('Average Return on Investment by Genre')
    ax.axhline(y=1, color='red', linestyle='--', label='Break-even (1x)', alpha=0.7)
    ax.legend()
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def plot_popularity_vs_rating(df_pandas, output_path):
    """
    Create scatter plot of Popularity vs Rating.
    
    Args:
        df_pandas: Pandas DataFrame with movie data.
        output_path: Path to save the plot image.
    """
    setup_plot_style()
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    plot_data = df_pandas[
        (df_pandas['popularity'].notna()) & 
        (df_pandas['vote_average'].notna())
    ].copy()
    
    scatter = ax.scatter(
        plot_data['vote_average'],
        plot_data['popularity'],
        c=plot_data['revenue_musd'],
        cmap='plasma',
        alpha=0.7,
        s=100
    )
    
    ax.set_xlabel('Average Rating (0-10)')
    ax.set_ylabel('Popularity Score')
    ax.set_title('Movie Popularity vs Rating')
    
    if plot_data['revenue_musd'].notna().any():
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Revenue (Million USD)')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def plot_yearly_box_office(df_pandas, output_path):
    """
    Create line chart of yearly box office performance.
    
    Args:
        df_pandas: Pandas DataFrame with movie data.
        output_path: Path to save the plot image.
    """
    setup_plot_style()
    
    plot_data = df_pandas[
        (df_pandas['release_year'].notna()) & 
        (df_pandas['revenue_musd'].notna())
    ].copy()
    
    if plot_data.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, 'No yearly data available', 
                ha='center', va='center', transform=ax.transAxes)
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        return
    
    yearly_stats = plot_data.groupby('release_year').agg({
        'revenue_musd': ['sum', 'mean', 'count']
    }).reset_index()
    yearly_stats.columns = ['year', 'total_revenue', 'avg_revenue', 'movie_count']
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Total revenue bars
    ax1.bar(yearly_stats['year'], yearly_stats['total_revenue'], 
            alpha=0.7, label='Total Revenue', color='steelblue')
    ax1.set_xlabel('Year')
    ax1.set_ylabel('Total Revenue (Million USD)', color='steelblue')
    ax1.tick_params(axis='y', labelcolor='steelblue')
    
    # Average revenue line on secondary axis
    ax2 = ax1.twinx()
    ax2.plot(yearly_stats['year'], yearly_stats['avg_revenue'], 
             color='crimson', marker='o', linewidth=2, label='Avg Revenue')
    ax2.set_ylabel('Average Revenue (Million USD)', color='crimson')
    ax2.tick_params(axis='y', labelcolor='crimson')
    
    ax1.set_title('Yearly Box Office Performance')
    
    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def plot_franchise_vs_standalone(comparison_df, output_path):
    """
    Create grouped bar chart comparing franchise vs standalone movies.
    
    Args:
        comparison_df: Pandas DataFrame with comparison statistics.
        output_path: Path to save the plot image.
    """
    setup_plot_style()
    
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    categories = comparison_df['is_franchise'].tolist()
    colors = ['#2ecc71', '#3498db']  # Green for franchise, blue for standalone
    
    # Plot 1: Mean Revenue
    ax1 = axes[0]
    values = comparison_df['mean_revenue_musd'].tolist()
    bars1 = ax1.bar(categories, values, color=colors)
    ax1.set_title('Mean Revenue (Million USD)')
    ax1.set_ylabel('Revenue (M USD)')
    for bar, val in zip(bars1, values):
        if val:
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height(), 
                    f'${val:.1f}M', ha='center', va='bottom', fontsize=10)
    
    # Plot 2: Mean Rating
    ax2 = axes[1]
    values = comparison_df['mean_rating'].tolist()
    bars2 = ax2.bar(categories, values, color=colors)
    ax2.set_title('Mean Rating')
    ax2.set_ylabel('Rating (0-10)')
    ax2.set_ylim(0, 10)
    for bar, val in zip(bars2, values):
        if val:
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height(), 
                    f'{val:.1f}', ha='center', va='bottom', fontsize=10)
    
    # Plot 3: Mean Popularity
    ax3 = axes[2]
    values = comparison_df['mean_popularity'].tolist()
    bars3 = ax3.bar(categories, values, color=colors)
    ax3.set_title('Mean Popularity')
    ax3.set_ylabel('Popularity Score')
    for bar, val in zip(bars3, values):
        if val:
            ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height(), 
                    f'{val:.1f}', ha='center', va='bottom', fontsize=10)
    
    plt.suptitle('Franchise vs Standalone Movie Comparison', fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()


def create_all_visualizations(df, output_dir, comparison_df=None):
    """
    Create all visualizations and save to output directory.
    
    Args:
        df: PySpark DataFrame with derived metrice movie data.
        output_dir: Directory to save plot images.
        comparison_df: Optional PySpark DataFrame with franchise comparison data.
    
    Returns:
        List of paths to created plot files.
    """
    logger = get_step_logger('visualization')
    logger.info(f"Creating visualizations in {output_dir}")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert to Pandas for plotting
    df_pandas = df.toPandas()
    
    created_plots = []
    
    # Plot 1: Revenue vs Budget
    logger.info("Creating Revenue vs Budget plot")
    path = os.path.join(output_dir, 'revenue_vs_budget.png')
    plot_revenue_vs_budget(df_pandas, path)
    created_plots.append(path)
    
    # Plot 2: ROI by Genre
    logger.info("Creating ROI by Genre plot")
    path = os.path.join(output_dir, 'roi_by_genre.png')
    plot_roi_by_genre(df_pandas, path)
    created_plots.append(path)
    
    # Plot 3: Popularity vs Rating
    logger.info("Creating Popularity vs Rating plot")
    path = os.path.join(output_dir, 'popularity_vs_rating.png')
    plot_popularity_vs_rating(df_pandas, path)
    created_plots.append(path)
    
    # Plot 4: Yearly Box Office
    logger.info("Creating Yearly Box Office plot")
    path = os.path.join(output_dir, 'yearly_box_office.png')
    plot_yearly_box_office(df_pandas, path)
    created_plots.append(path)
    
    # Plot 5: Franchise vs Standalone
    if comparison_df is not None:
        logger.info("Creating Franchise vs Standalone plot")
        comparison_pandas = comparison_df.toPandas()
        path = os.path.join(output_dir, 'franchise_vs_standalone.png')
        plot_franchise_vs_standalone(comparison_pandas, path)
        created_plots.append(path)
    
    logger.info(f"Created {len(created_plots)} visualizations")
    return created_plots
