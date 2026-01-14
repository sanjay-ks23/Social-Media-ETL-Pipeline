#!/usr/bin/env python3
"""
Social Media ETL Pipeline - Command Line Interface

Supports scraping from:
- Instagram (Playwright browser automation)
- YouTube (YouTube Data API)
- Twitter/X (Playwright browser automation)
- Reddit (free .json endpoints)
"""

import asyncio
import argparse
import os
import pandas as pd
from datetime import datetime
from scrapers import InstagramScraper, YouTubeScraper, RedditScraper, TwitterScraper


async def run_instagram_scraper(target, limit):
    """Run Instagram hashtag scraper."""
    print(f"Starting Instagram scraper for '{target}' with limit {limit}")
    posts = await InstagramScraper.scrape(target, limit)
    
    if posts:
        print(f"Successfully scraped {len(posts)} Instagram posts")
    else:
        print("No Instagram posts were scraped")
    
    return posts


async def run_youtube_scraper(target, limit):
    """Run YouTube search scraper."""
    print(f"Starting YouTube scraper for '{target}' with limit {limit}")
    posts = await YouTubeScraper.scrape(target, limit)
    
    if posts:
        print(f"Successfully scraped {len(posts)} YouTube videos")
    else:
        print("No YouTube videos were scraped")
    
    return posts


async def run_reddit_scraper(target, limit, subreddit=None, sort='hot'):
    """Run Reddit scraper using free .json endpoints."""
    if subreddit:
        print(f"Starting Reddit scraper for r/{subreddit} ({sort}) with limit {limit}")
    else:
        print(f"Starting Reddit scraper for '{target}' with limit {limit}")
    
    # Reddit scraper is synchronous (uses requests, not Playwright)
    scraper = RedditScraper()
    
    if subreddit:
        posts = scraper.search_subreddit(subreddit, sort=sort, limit=limit)
    else:
        posts = scraper.search_posts(target, sort='relevance', limit=limit)
    
    if posts:
        print(f"Successfully scraped {len(posts)} Reddit posts")
    else:
        print("No Reddit posts were scraped")
    
    return posts


async def run_twitter_scraper(target, limit):
    """Run Twitter/X scraper using Playwright."""
    print(f"Starting Twitter scraper for '{target}' with limit {limit}")
    posts = await TwitterScraper.scrape(target, limit)
    
    if posts:
        print(f"Successfully scraped {len(posts)} tweets")
    else:
        print("No tweets were scraped")
    
    return posts


def save_to_metadata_csv(posts, filename='metadata.csv'):
    """Save or append scraped posts to the metadata.csv file."""
    if not posts:
        print("No data to save")
        return
    
    # Convert posts to DataFrame
    df_new = pd.DataFrame(posts)
    
    # Check if metadata.csv exists
    if os.path.exists(filename):
        try:
            # Read existing data
            df_existing = pd.read_csv(filename, encoding='utf-8')
            print(f"Found existing metadata.csv with {len(df_existing)} records")
            
            # Append new data
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            
            # Save combined data
            df_combined.to_csv(filename, index=False, encoding='utf-8')
            print(f"Added {len(df_new)} new posts to {filename}")
            print(f"Total posts in {filename}: {len(df_combined)}")
        except Exception as e:
            print(f"Error reading existing file: {e}")
            df_new.to_csv(filename, index=False, encoding='utf-8')
            print(f"Saved {len(df_new)} posts to new {filename}")
    else:
        # Create new file
        df_new.to_csv(filename, index=False, encoding='utf-8')
        print(f"Created new {filename} with {len(df_new)} posts")


async def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description='Social Media ETL Pipeline - Scrape content from multiple platforms',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Instagram hashtag scraping
  python scrape_posts.py --platform instagram --target puppy --limit 25
  
  # YouTube search
  python scrape_posts.py --platform youtube --target "machine learning" --limit 25
  
  # Reddit subreddit scraping (free, no API key needed)
  python scrape_posts.py --platform reddit --subreddit MachineLearning --limit 50
  
  # Reddit search (free)
  python scrape_posts.py --platform reddit --target "sentiment analysis" --limit 50
  
  # Twitter/X search (uses Playwright)
  python scrape_posts.py --platform twitter --target "AI news" --limit 50
        """
    )
    
    parser.add_argument(
        '--platform', 
        choices=['instagram', 'youtube', 'twitter', 'reddit'],
        required=True,
        help='Platform to scrape'
    )
    parser.add_argument(
        '--target', 
        required=False,
        help='Search term, hashtag, or query to scrape'
    )
    parser.add_argument(
        '--limit', 
        type=int, 
        default=50,
        help='Maximum number of posts to retrieve (default: 50)'
    )
    parser.add_argument(
        '--subreddit',
        help='For Reddit: specific subreddit to scrape (e.g., MachineLearning)'
    )
    parser.add_argument(
        '--sort',
        choices=['hot', 'new', 'top', 'rising'],
        default='hot',
        help='Sort order for Reddit posts (default: hot)'
    )
    parser.add_argument(
        '--output',
        default='metadata.csv',
        help='Output CSV file (default: metadata.csv)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.platform == 'reddit' and not args.target and not args.subreddit:
        parser.error("Reddit requires either --target (search query) or --subreddit")
    elif args.platform != 'reddit' and not args.target:
        parser.error(f"{args.platform} requires --target argument")
    
    all_posts = []
    
    # Run the selected scraper
    if args.platform == 'instagram':
        posts = await run_instagram_scraper(args.target, args.limit)
        if posts:
            all_posts.extend(posts)
    
    elif args.platform == 'youtube':
        posts = await run_youtube_scraper(args.target, args.limit)
        if posts:
            all_posts.extend(posts)
    
    elif args.platform == 'reddit':
        posts = await run_reddit_scraper(
            args.target, 
            args.limit, 
            subreddit=args.subreddit,
            sort=args.sort
        )
        if posts:
            all_posts.extend(posts)
    
    elif args.platform == 'twitter':
        posts = await run_twitter_scraper(args.target, args.limit)
        if posts:
            all_posts.extend(posts)
    
    # Save all collected data
    if all_posts:
        save_to_metadata_csv(all_posts, filename=args.output)
    
    # Print summary
    print("\n" + "="*50)
    print("Scraping Summary:")
    print(f"  Platform: {args.platform}")
    print(f"  Target: {args.target or args.subreddit}")
    print(f"  Total posts scraped: {len(all_posts)}")
    print("="*50)


if __name__ == "__main__":
    asyncio.run(main())