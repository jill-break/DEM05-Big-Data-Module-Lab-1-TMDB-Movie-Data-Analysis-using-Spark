import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Set global style for all plots
sns.set_style("whitegrid", {'grid.linestyle': '--', 'grid.alpha': 0.5})

def plot_yearly_trends(pdf_yearly):
    """
    Plots a 4-grid summary of yearly box office trends.
    """
    fig, axes = plt.subplots(2, 2, figsize=(20, 10))
    fig.suptitle('Yearly Box Office Performance Trends', fontsize=16, weight='bold')

    # Plot 1: Movie Count
    axes[0, 0].plot(pdf_yearly['year'], pdf_yearly['movie_count'], marker='o', linestyle='-', color='#1f77b4', label='Movie Count')
    axes[0, 0].set_title('Number of Movies Released per Year')
    axes[0, 0].set_ylabel('Count')
    axes[0, 0].legend()

    # Plot 2: Revenue
    axes[0, 1].plot(pdf_yearly['year'], pdf_yearly['mean_revenue'], marker='o', linestyle='-', color='green', label='Mean Revenue')
    axes[0, 1].set_title('Average Revenue per Year (M USD)')
    axes[0, 1].set_ylabel('Revenue (M USD)')
    axes[0, 1].legend()

    # Plot 3: Budget
    axes[1, 0].plot(pdf_yearly['year'], pdf_yearly['mean_budget'], marker='o', linestyle='-', color='orange', label='Mean Budget')
    axes[1, 0].set_title('Average Budget per Year (M USD)')
    axes[1, 0].set_xlabel('Year')
    axes[1, 0].set_ylabel('Budget (M USD)')
    axes[1, 0].legend()

    # Plot 4: ROI
    axes[1, 1].plot(pdf_yearly['year'], pdf_yearly['mean_roi'], marker='o', linestyle='-', color='purple', label='Mean ROI')
    axes[1, 1].set_title('Average ROI per Year')
    axes[1, 1].set_xlabel('Year')
    axes[1, 1].set_ylabel('ROI')
    axes[1, 1].legend()

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.show()

def plot_genre_roi(pdf_genre):
    """
    Plots a horizontal bar chart of Median ROI by Genre.
    """
    plt.figure(figsize=(14, 7))
    sns.barplot(data=pdf_genre, x="median_roi", y="genre", color='lightgreen', edgecolor='black')

    plt.title('Median ROI Distribution by Genre', fontsize=16)
    plt.xlabel('Median ROI (x Times Budget)', fontsize=12)
    plt.ylabel('Genre', fontsize=12)
    plt.grid(axis='x', linestyle='--', alpha=0.7)

    # Add labels
    for i, v in enumerate(pdf_genre["median_roi"]):
        plt.text(v + 0.1, i, f"{v:.2f}x", color='black', va='center', fontsize=10)

    plt.show()

def plot_revenue_vs_budget(pdf_scatter):
    """
    Plots a scatter chart of Revenue vs Budget.
    """
    plt.figure(figsize=(10, 6))
    plt.scatter(pdf_scatter['budget_musd'], pdf_scatter['revenue_musd'], 
                alpha=0.6, s=100, color='skyblue', edgecolors='grey', label='Individual Movies')

    plt.title('Revenue vs. Budget Trends', fontsize=16)
    plt.xlabel('Budget (Million USD)', fontsize=12)
    plt.ylabel('Revenue (Million USD)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend()
    plt.show()

def plot_franchise_comparison(pdf_franchise):
    """
    Plots a 4-grid comparison of Franchise vs Standalone movies.
    """
    fig, axes = plt.subplots(2, 2, figsize=(18, 10))
    fig.suptitle('Franchise vs Standalone Movie Performance', fontsize=14, weight='bold')

    def _plot_sub(ax, y_col, title, ylabel, is_rating=False):
        # FIX: Added hue='type' and legend=False to silence warnings
        sns.barplot(
            data=pdf_franchise, 
            x='type', 
            y=y_col, 
            hue='type', 
            ax=ax, 
            palette=['#2b8cbe', '#a53e74'], 
            edgecolor='black',
            legend=False
        )
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.set_xlabel('')
        ax.grid(axis='y', linestyle='-', alpha=0.3)
        if is_rating:
            ax.set_ylim(0, 10)

    _plot_sub(axes[0, 0], 'mean_revenue', 'Average Revenue (M USD)', 'Million USD')
    _plot_sub(axes[0, 1], 'mean_roi', 'Average ROI', 'ROI Multiplier')
    _plot_sub(axes[1, 0], 'mean_budget', 'Average Budget (M USD)', 'Million USD')
    _plot_sub(axes[1, 1], 'mean_rating', 'Average Rating', 'Rating (out of 10)', is_rating=True)

    plt.show()

def plot_popularity_vs_rating(pdf_scatter):
    """
    Plots a scatter chart of Popularity vs Vote Average.
    """
    plt.figure(figsize=(10, 6))
    plt.scatter(pdf_scatter['popularity'], pdf_scatter['vote_average'], 
                alpha=0.7, s=100, color='#a05195', edgecolors='black', label='Individual Movies')

    plt.title('Relationship: Popularity vs. User Rating', fontsize=16)
    plt.xlabel('Popularity Score', fontsize=12)
    plt.ylabel('Vote Average (0-10)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend()
    plt.show()