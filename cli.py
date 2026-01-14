#!/usr/bin/env python3
"""
Social Media ETL Pipeline - Interactive CLI

A beautiful, interactive command-line interface for scraping social media.
Features:
- Gemini-style interactive prompts
- DuckDB database backend (microsecond queries)
- Full ETL pipeline with sentiment analysis
- Rich styled output
"""

import asyncio
import os
import sys
from datetime import datetime

# Rich for beautiful terminal output
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.prompt import Prompt, IntPrompt, Confirm
from rich.text import Text
from rich.markdown import Markdown
from rich import box
from rich.live import Live
from rich.layout import Layout
from rich.style import Style

# Import scrapers and ETL components
import pandas as pd
from scrapers import InstagramScraper, YouTubeScraper, RedditScraper, TwitterScraper, load_config

# Initialize console
console = Console()

# Try to import database and ETL (graceful fallback if not available)
try:
    from database import Database
    from etl import ETLPipeline, Transformer
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False
    console.print("[yellow]Note: Install duckdb for database features: pip install duckdb[/]")

# ASCII Art Banner
BANNER = """
[bold cyan]
  ╔═══════════════════════════════════════════════════════════════╗
  ║                                                               ║
  ║   [bold white]███████╗ ██████╗  ██████╗██╗ █████╗ ██╗                 [/]║
  ║   [bold white]██╔════╝██╔═══██╗██╔════╝██║██╔══██╗██║                 [/]║
  ║   [bold white]███████╗██║   ██║██║     ██║███████║██║                 [/]║
  ║   [bold white]╚════██║██║   ██║██║     ██║██╔══██║██║                 [/]║
  ║   [bold white]███████║╚██████╔╝╚██████╗██║██║  ██║███████╗            [/]║
  ║   [bold white]╚══════╝ ╚═════╝  ╚═════╝╚═╝╚═╝  ╚═╝╚══════╝            [/]║
  ║                                                               ║
  ║   [bold magenta]███████╗████████╗██╗                                   [/]║
  ║   [bold magenta]██╔════╝╚══██╔══╝██║                                   [/]║
  ║   [bold magenta]█████╗     ██║   ██║                                   [/]║
  ║   [bold magenta]██╔══╝     ██║   ██║                                   [/]║
  ║   [bold magenta]███████╗   ██║   ███████╗                              [/]║
  ║   [bold magenta]╚══════╝   ╚═╝   ╚══════╝                              [/]║
  ║                                                               ║
  ║   [dim]Social Media Scraper • DuckDB • Sentiment Analysis[/]        ║
  ╚═══════════════════════════════════════════════════════════════╝
[/bold cyan]
"""

# Platform info
PLATFORMS = {
    "1": {"name": "Reddit", "icon": "🟠", "description": "Subreddits & search (FREE)", "color": "orange1"},
    "2": {"name": "Twitter", "icon": "🐦", "description": "Tweet search (FREE)", "color": "cyan"},
    "3": {"name": "YouTube", "icon": "▶️ ", "description": "Video search (FREE API)", "color": "red"},
    "4": {"name": "Instagram", "icon": "📸", "description": "Hashtag scraping", "color": "magenta"}
}


def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def show_banner():
    console.print(BANNER)


def show_main_menu():
    """Display the main menu with all options."""
    table = Table(
        title="[bold]Main Menu[/bold]",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan"
    )
    table.add_column("#", style="dim", width=3)
    table.add_column("Option", style="bold")
    table.add_column("Description", style="dim")
    
    # Scraping options
    for key, platform in PLATFORMS.items():
        table.add_row(key, f"{platform['icon']} [{platform['color']}]{platform['name']}[/]", platform['description'])
    
    # Data options
    table.add_row("5", "📊 [green]Query Data[/]", "SQL queries on scraped data")
    table.add_row("6", "📈 [blue]Analytics[/]", "Sentiment & engagement stats")
    table.add_row("7", "💾 [magenta]Export[/]", "Export to CSV/Parquet")
    table.add_row("8", "⚙️  [yellow]Settings[/]", "Configure credentials")
    table.add_row("0", "🚪 [red]Exit[/]", "Quit")
    
    console.print(table)
    console.print()


def get_menu_choice():
    choice = Prompt.ask(
        "[bold cyan]>[/] Choose an option",
        choices=["0", "1", "2", "3", "4", "5", "6", "7", "8"],
        default="1"
    )
    return choice


# ============ SCRAPING FUNCTIONS ============

async def scrape_reddit():
    """Interactive Reddit scraping."""
    console.print(Panel("[bold orange1]🟠 Reddit Scraper[/]", box=box.DOUBLE))
    
    console.print("\n[bold]Choose mode:[/]")
    console.print("  [1] Subreddit - Scrape a specific subreddit")
    console.print("  [2] Search - Search across Reddit")
    
    mode = Prompt.ask("[bold cyan]>[/] Mode", choices=["1", "2"], default="1")
    
    if mode == "1":
        subreddit = Prompt.ask("[bold cyan]>[/] Subreddit name (without r/)")
        sort = Prompt.ask("[bold cyan]>[/] Sort by", choices=["hot", "new", "top", "rising"], default="hot")
        limit = IntPrompt.ask("[bold cyan]>[/] Number of posts", default=25)
        
        console.print(Panel(f"[bold]r/{subreddit}[/] • {sort} • {limit} posts", box=box.ROUNDED))
        
        if not Confirm.ask("[bold cyan]>[/] Start?", default=True):
            return []
        
        with console.status(f"[bold green]Scraping r/{subreddit}...", spinner="dots"):
            scraper = RedditScraper()
            posts = scraper.search_subreddit(subreddit, sort=sort, limit=limit)
        return posts
    else:
        query = Prompt.ask("[bold cyan]>[/] Search query")
        limit = IntPrompt.ask("[bold cyan]>[/] Number of posts", default=25)
        
        if not Confirm.ask(f"[bold cyan]>[/] Search '{query}'?", default=True):
            return []
        
        with console.status(f"[bold green]Searching...", spinner="dots"):
            scraper = RedditScraper()
            posts = scraper.search_posts(query, limit=limit)
        return posts


async def scrape_twitter():
    """Interactive Twitter scraping."""
    console.print(Panel("[bold cyan]🐦 Twitter/X Scraper[/]", box=box.DOUBLE))
    
    query = Prompt.ask("[bold cyan]>[/] Search query or hashtag")
    limit = IntPrompt.ask("[bold cyan]>[/] Number of tweets", default=25)
    
    console.print(Panel(f"[bold]Query:[/] {query}\n[dim]Browser window will open[/]", box=box.ROUNDED))
    
    if not Confirm.ask("[bold cyan]>[/] Start?", default=True):
        return []
    
    console.print("[yellow]Opening browser...[/]")
    try:
        posts = await TwitterScraper.scrape(query, limit)
        return posts
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")
        return []


async def scrape_youtube():
    """Interactive YouTube scraping."""
    console.print(Panel("[bold red]▶️  YouTube Scraper[/]", box=box.DOUBLE))
    
    config = load_config()
    if not config.get('youtube_api_key') or config.get('youtube_api_key') == 'YOUR_YOUTUBE_API_KEY':
        console.print("[red]⚠️  YouTube API key not configured![/]")
        console.print("[dim]Edit config.json to add your API key[/]")
        Prompt.ask("[dim]Press Enter[/]")
        return []
    
    query = Prompt.ask("[bold cyan]>[/] Search query")
    limit = IntPrompt.ask("[bold cyan]>[/] Number of videos", default=25)
    
    if not Confirm.ask(f"[bold cyan]>[/] Search '{query}'?", default=True):
        return []
    
    with console.status("[bold green]Searching YouTube...", spinner="dots"):
        posts = await YouTubeScraper.scrape(query, limit)
    return posts


async def scrape_instagram():
    """Interactive Instagram scraping."""
    console.print(Panel("[bold magenta]📸 Instagram Scraper[/]", box=box.DOUBLE))
    
    config = load_config()
    ig_config = config.get('instagram', {})
    if not ig_config.get('username') or ig_config.get('username') == 'YOUR_INSTAGRAM_USERNAME':
        console.print("[red]⚠️  Instagram credentials not configured![/]")
        Prompt.ask("[dim]Press Enter[/]")
        return []
    
    hashtag = Prompt.ask("[bold cyan]>[/] Hashtag (without #)")
    limit = IntPrompt.ask("[bold cyan]>[/] Number of posts", default=25)
    
    console.print(Panel(f"[bold]#{hashtag}[/] • {limit} posts\n[dim]Browser window will open[/]", box=box.ROUNDED))
    
    if not Confirm.ask("[bold cyan]>[/] Start?", default=True):
        return []
    
    console.print("[yellow]Opening browser...[/]")
    try:
        posts = await InstagramScraper.scrape(hashtag, limit)
        return posts
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")
        return []


# ============ ETL & DATABASE FUNCTIONS ============

def run_etl_pipeline(posts, platform):
    """Run the ETL pipeline on scraped data."""
    if not posts:
        return None
    
    if not DB_AVAILABLE:
        console.print("[yellow]Database not available. Falling back to CSV.[/]")
        return None
    
    console.print()
    console.print("[bold]🔄 Running ETL Pipeline...[/]")
    
    with console.status("[bold green]Transforming and loading data...", spinner="dots"):
        db = Database()
        pipeline = ETLPipeline(db)
        results = pipeline.run(posts)
        db.close()
    
    # Display results
    table = Table(title="[bold]ETL Pipeline Results[/]", box=box.ROUNDED)
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    
    table.add_row("Input Posts", str(results['input_count']))
    table.add_row("Transformed", str(results['transformed_count']))
    table.add_row("Loaded to DB", str(results['loaded_count']))
    table.add_row("Duration", f"{results['duration_seconds']}s")
    table.add_row("Speed", f"{results['records_per_second']} rec/s")
    
    console.print(table)
    
    # Show sentiment breakdown
    sentiment = results['transform_stats']['sentiment']
    console.print(f"\n[bold]Sentiment Analysis:[/]")
    console.print(f"  [green]Positive:[/] {sentiment['positive']}")
    console.print(f"  [yellow]Neutral:[/] {sentiment['neutral']}")
    console.print(f"  [red]Negative:[/] {sentiment['negative']}")
    
    return results


def query_data():
    """Interactive SQL query interface."""
    if not DB_AVAILABLE:
        console.print("[red]Database not available. Install duckdb: pip install duckdb[/]")
        Prompt.ask("[dim]Press Enter[/]")
        return
    
    console.print(Panel("[bold green]📊 Query Data[/]", box=box.DOUBLE))
    
    db = Database()
    stats = db.get_stats()
    
    # Show database stats
    console.print(f"\n[bold]Database Stats:[/]")
    console.print(f"  Total posts: [cyan]{stats['total_posts']}[/]")
    
    if stats['by_platform']:
        console.print(f"\n[bold]By Platform:[/]")
        for platform, count in stats['by_platform'].items():
            console.print(f"  • {platform}: {count}")
    
    console.print("\n[bold]Query Options:[/]")
    console.print("  [1] Recent posts")
    console.print("  [2] Top posts by likes")
    console.print("  [3] Search posts")
    console.print("  [4] Custom SQL")
    console.print("  [5] Back")
    
    choice = Prompt.ask("[bold cyan]>[/] Choose", choices=["1", "2", "3", "4", "5"], default="1")
    
    if choice == "1":
        platform = Prompt.ask("[bold cyan]>[/] Filter by platform (or 'all')", default="all")
        limit = IntPrompt.ask("[bold cyan]>[/] Number of posts", default=20)
        
        platform_filter = None if platform == "all" else platform
        
        with console.status("[bold green]Querying...", spinner="dots"):
            results = db.get_posts(platform=platform_filter, limit=limit)
        
        display_query_results(results)
    
    elif choice == "2":
        limit = IntPrompt.ask("[bold cyan]>[/] Number of posts", default=20)
        
        with console.status("[bold green]Querying...", spinner="dots"):
            results = db.query(f"SELECT * FROM posts ORDER BY likes DESC LIMIT {limit}")
        
        display_query_results(results)
    
    elif choice == "3":
        query = Prompt.ask("[bold cyan]>[/] Search term")
        
        with console.status("[bold green]Searching...", spinner="dots"):
            results = db.search(query, limit=50)
        
        display_query_results(results)
    
    elif choice == "4":
        console.print("\n[dim]Enter SQL query (table: posts)[/]")
        console.print("[dim]Example: SELECT * FROM posts WHERE platform = 'reddit' LIMIT 10[/]")
        sql = Prompt.ask("[bold cyan]SQL>[/]")
        
        try:
            with console.status("[bold green]Executing...", spinner="dots"):
                start = datetime.now()
                results = db.query(sql)
                duration = (datetime.now() - start).total_seconds() * 1000
            
            console.print(f"\n[dim]Query completed in {duration:.2f}ms[/]")
            display_query_results(results)
        except Exception as e:
            console.print(f"[red]Error: {e}[/]")
    
    db.close()
    Prompt.ask("\n[dim]Press Enter[/]")


def display_query_results(results):
    """Display query results in a table."""
    if not results:
        console.print("[yellow]No results found[/]")
        return
    
    console.print(f"\n[bold green]Found {len(results)} results[/]\n")
    
    table = Table(box=box.ROUNDED, show_lines=True)
    table.add_column("#", style="dim", width=3)
    table.add_column("Platform", style="cyan", width=10)
    table.add_column("Author", width=12)
    table.add_column("Content", width=35)
    table.add_column("👍", justify="right", width=6)
    table.add_column("Sentiment", width=10)
    
    for i, row in enumerate(results[:20], 1):
        text = str(row.get('post_text', ''))[:35]
        if len(str(row.get('post_text', ''))) > 35:
            text += "..."
        
        sentiment = row.get('sentiment_label', '')
        sentiment_color = {'positive': 'green', 'negative': 'red', 'neutral': 'yellow'}.get(sentiment, 'dim')
        
        table.add_row(
            str(i),
            str(row.get('platform', '')),
            str(row.get('author', ''))[:12],
            text,
            str(row.get('likes', 0)),
            f"[{sentiment_color}]{sentiment}[/]" if sentiment else ""
        )
    
    if len(results) > 20:
        table.add_row("...", f"[dim]+{len(results) - 20} more[/]", "", "", "", "")
    
    console.print(table)


def show_analytics():
    """Show analytics dashboard."""
    if not DB_AVAILABLE:
        console.print("[red]Database not available[/]")
        Prompt.ask("[dim]Press Enter[/]")
        return
    
    console.print(Panel("[bold blue]📈 Analytics Dashboard[/]", box=box.DOUBLE))
    
    db = Database()
    
    with console.status("[bold green]Analyzing data...", spinner="dots"):
        stats = db.get_stats()
        sentiment_dist = db.get_sentiment_distribution()
        top_hashtags = db.get_top_hashtags(limit=10)
    
    # Overview
    console.print(f"\n[bold]📊 Overview[/]")
    console.print(f"  Total Posts: [cyan]{stats['total_posts']}[/]")
    
    # Platform breakdown
    if stats['by_platform']:
        console.print(f"\n[bold]🌐 By Platform[/]")
        for platform, count in stats['by_platform'].items():
            pct = round(count / max(stats['total_posts'], 1) * 100, 1)
            console.print(f"  {platform}: {count} ({pct}%)")
    
    # Sentiment distribution
    if sentiment_dist:
        console.print(f"\n[bold]😊 Sentiment Distribution[/]")
        for row in sentiment_dist:
            label = row['sentiment_label']
            count = row['count']
            pct = row['percentage']
            color = {'positive': 'green', 'negative': 'red', 'neutral': 'yellow'}.get(label, 'dim')
            console.print(f"  [{color}]{label}[/]: {count} ({pct}%)")
    
    # Top hashtags
    if top_hashtags:
        console.print(f"\n[bold]#️⃣  Top Hashtags[/]")
        for i, row in enumerate(top_hashtags[:10], 1):
            console.print(f"  {i}. #{row['hashtag']} ({row['count']})")
    
    # Top authors
    if stats.get('top_authors'):
        console.print(f"\n[bold]👤 Top Authors[/]")
        for i, row in enumerate(stats['top_authors'][:5], 1):
            console.print(f"  {i}. @{row['author']} ({row['count']} posts)")
    
    db.close()
    console.print()
    Prompt.ask("[dim]Press Enter[/]")


def export_data():
    """Export data to files."""
    if not DB_AVAILABLE:
        console.print("[red]Database not available[/]")
        Prompt.ask("[dim]Press Enter[/]")
        return
    
    console.print(Panel("[bold magenta]💾 Export Data[/]", box=box.DOUBLE))
    
    console.print("\n[bold]Export Format:[/]")
    console.print("  [1] CSV (readable)")
    console.print("  [2] Parquet (compressed, fast)")
    console.print("  [3] Both")
    
    choice = Prompt.ask("[bold cyan]>[/] Choose", choices=["1", "2", "3"], default="1")
    
    platform = Prompt.ask("[bold cyan]>[/] Filter by platform (or 'all')", default="all")
    platform_filter = None if platform == "all" else platform
    
    db = Database()
    
    try:
        if choice in ["1", "3"]:
            filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            db.export_csv(filename, platform=platform_filter)
            console.print(f"[green]✓ Exported to {filename}[/]")
        
        if choice in ["2", "3"]:
            filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            db.export_parquet(filename, platform=platform_filter)
            console.print(f"[green]✓ Exported to {filename}[/]")
    
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")
    
    db.close()
    Prompt.ask("\n[dim]Press Enter[/]")


def show_settings():
    """Display settings."""
    console.print(Panel("[bold yellow]⚙️  Settings[/]", box=box.DOUBLE))
    
    config = load_config()
    
    table = Table(title="[bold]Configuration[/]", box=box.ROUNDED)
    table.add_column("Service", style="bold")
    table.add_column("Status")
    table.add_column("Details", style="dim")
    
    # YouTube
    yt_key = config.get('youtube_api_key', '')
    if yt_key and yt_key != 'YOUR_YOUTUBE_API_KEY':
        table.add_row("YouTube", "[green]✓[/]", f"Key: {yt_key[:8]}...")
    else:
        table.add_row("YouTube", "[red]✗[/]", "Add API key")
    
    # Instagram
    ig = config.get('instagram', {})
    if ig.get('username') and ig.get('username') != 'YOUR_INSTAGRAM_USERNAME':
        table.add_row("Instagram", "[green]✓[/]", f"@{ig.get('username')}")
    else:
        table.add_row("Instagram", "[red]✗[/]", "Add credentials")
    
    # Twitter
    tw = config.get('twitter', {})
    if tw.get('username') and tw.get('username') != 'YOUR_TWITTER_USERNAME':
        table.add_row("Twitter", "[green]✓[/]", f"@{tw.get('username')}")
    else:
        table.add_row("Twitter", "[yellow]○[/]", "Optional")
    
    # Reddit & Database
    table.add_row("Reddit", "[green]✓[/]", "No config needed")
    table.add_row("Database", "[green]✓[/]" if DB_AVAILABLE else "[red]✗[/]", 
                  "DuckDB" if DB_AVAILABLE else "pip install duckdb")
    
    console.print(table)
    console.print("\n[dim]Edit config.json to update[/]")
    Prompt.ask("\n[dim]Press Enter[/]")


# ============ MAIN LOOP ============

async def main_loop():
    """Main interactive loop."""
    clear_screen()
    show_banner()
    
    while True:
        show_main_menu()
        choice = get_menu_choice()
        
        if choice == "0":
            console.print("\n[bold cyan]👋 Goodbye![/]\n")
            break
        
        clear_screen()
        show_banner()
        
        posts = []
        platform = ""
        
        try:
            if choice == "1":
                platform = "Reddit"
                posts = await scrape_reddit()
            elif choice == "2":
                platform = "Twitter"
                posts = await scrape_twitter()
            elif choice == "3":
                platform = "YouTube"
                posts = await scrape_youtube()
            elif choice == "4":
                platform = "Instagram"
                posts = await scrape_instagram()
            elif choice == "5":
                query_data()
                clear_screen()
                show_banner()
                continue
            elif choice == "6":
                show_analytics()
                clear_screen()
                show_banner()
                continue
            elif choice == "7":
                export_data()
                clear_screen()
                show_banner()
                continue
            elif choice == "8":
                show_settings()
                clear_screen()
                show_banner()
                continue
            
            # Process scraped data
            if posts:
                console.print(f"\n[bold green]✓ Scraped {len(posts)} posts from {platform}[/]")
                
                # Run ETL pipeline
                if DB_AVAILABLE:
                    run_etl_pipeline(posts, platform)
                else:
                    # Fallback to CSV
                    if Confirm.ask("\n[bold cyan]>[/] Save to CSV?", default=True):
                        df = pd.DataFrame(posts)
                        df.to_csv('metadata.csv', mode='a', header=not os.path.exists('metadata.csv'), 
                                  index=False, encoding='utf-8')
                        console.print("[green]✓ Saved to metadata.csv[/]")
            
        except KeyboardInterrupt:
            console.print("\n[yellow]Cancelled[/]")
        except Exception as e:
            console.print(f"\n[red]Error: {e}[/]")
        
        console.print()
        Prompt.ask("[dim]Press Enter to continue[/]")
        clear_screen()
        show_banner()


def main():
    """Entry point."""
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        console.print("\n[bold cyan]👋 Goodbye![/]\n")


if __name__ == "__main__":
    main()
