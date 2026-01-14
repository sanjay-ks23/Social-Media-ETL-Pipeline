"""
Production utilities for Social Media ETL Pipeline.
Provides rate limiting, retry logic, and text processing for sentiment analysis.
"""

import time
import re
import asyncio
from datetime import datetime
from functools import wraps
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Simple token bucket rate limiter to avoid API/scraping bans.
    
    Usage:
        limiter = RateLimiter(requests_per_minute=30)
        limiter.wait()  # Call before each request
    """
    
    def __init__(self, requests_per_minute: int = 30):
        self.min_interval = 60.0 / requests_per_minute
        self.last_request_time = 0.0
        
    def wait(self):
        """Block until it's safe to make the next request."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            time.sleep(sleep_time)
        self.last_request_time = time.time()
        
    async def async_wait(self):
        """Async version of wait()."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            await asyncio.sleep(sleep_time)
        self.last_request_time = time.time()


class RetryHandler:
    """
    Retry decorator with exponential backoff for handling transient failures.
    
    Usage:
        @RetryHandler.retry(max_attempts=3, base_delay=1.0)
        def fetch_data():
            ...
    """
    
    @staticmethod
    def retry(max_attempts: int = 3, base_delay: float = 1.0, exceptions: tuple = (Exception,)):
        """
        Decorator that retries a function with exponential backoff.
        
        Args:
            max_attempts: Maximum number of retry attempts
            base_delay: Initial delay between retries (seconds)
            exceptions: Tuple of exceptions to catch and retry
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(max_attempts):
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        if attempt < max_attempts - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                            time.sleep(delay)
                        else:
                            logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
                raise last_exception
            return wrapper
        return decorator
    
    @staticmethod
    def async_retry(max_attempts: int = 3, base_delay: float = 1.0, exceptions: tuple = (Exception,)):
        """Async version of retry decorator."""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(max_attempts):
                    try:
                        return await func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        if attempt < max_attempts - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                            await asyncio.sleep(delay)
                        else:
                            logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
                raise last_exception
            return wrapper
        return decorator


class TextProcessor:
    """
    Unified text processing for all social media platforms.
    Cleans noisy text and prepares it for sentiment/behavior modeling.
    """
    
    # Common patterns
    URL_PATTERN = re.compile(r'https?://\S+|www\.\S+')
    MENTION_PATTERN = re.compile(r'@\w+')
    HASHTAG_PATTERN = re.compile(r'#(\w+)')
    EMOJI_PATTERN = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+", 
        flags=re.UNICODE
    )
    
    # Special character replacements
    CHAR_REPLACEMENTS = {
        '\u2122': '™',
        '\u00a9': '©',
        '\u00ae': '®',
        '\u2018': "'",
        '\u2019': "'",
        '\u201c': '"',
        '\u201d': '"',
        '\u2013': '-',
        '\u2014': '--',
        '\u200b': '',  # Zero-width space
        '\ufeff': '',  # BOM
    }
    
    @classmethod
    def clean(cls, text: str) -> str:
        """
        Clean and normalize text for storage.
        Removes problematic characters while preserving meaning.
        """
        if not text:
            return ""
            
        # Apply character replacements
        for old, new in cls.CHAR_REPLACEMENTS.items():
            text = text.replace(old, new)
        
        # Normalize whitespace
        text = ' '.join(text.split())
        
        return text.strip()
    
    @classmethod
    def extract_hashtags(cls, text: str) -> list:
        """Extract hashtags from text and return as list."""
        if not text:
            return []
        return cls.HASHTAG_PATTERN.findall(text)
    
    @classmethod
    def remove_hashtags(cls, text: str) -> str:
        """Remove hashtags from text."""
        if not text:
            return ""
        text = re.sub(r'#\w+\s*', '', text)
        return ' '.join(text.split())
    
    @classmethod
    def extract_mentions(cls, text: str) -> list:
        """Extract @mentions from text."""
        if not text:
            return []
        return cls.MENTION_PATTERN.findall(text)
    
    @classmethod
    def remove_urls(cls, text: str) -> str:
        """Remove URLs from text."""
        if not text:
            return ""
        return cls.URL_PATTERN.sub('', text).strip()
    
    @classmethod
    def prepare_for_sentiment(cls, text: str, include_emojis: bool = True) -> dict:
        """
        Prepare text for downstream sentiment/behavior modeling.
        
        Returns:
            dict with:
                - cleaned_text: Text ready for analysis
                - hashtags: Extracted hashtags
                - mentions: Extracted mentions
                - has_urls: Whether text contained URLs
                - emoji_count: Number of emojis in text
        """
        if not text:
            return {
                'cleaned_text': '',
                'hashtags': [],
                'mentions': [],
                'has_urls': False,
                'emoji_count': 0
            }
        
        # Extract metadata before cleaning
        hashtags = cls.extract_hashtags(text)
        mentions = cls.extract_mentions(text)
        has_urls = bool(cls.URL_PATTERN.search(text))
        emoji_count = len(cls.EMOJI_PATTERN.findall(text))
        
        # Clean text
        cleaned = cls.clean(text)
        cleaned = cls.remove_urls(cleaned)
        cleaned = cls.remove_hashtags(cleaned)
        
        # Optionally remove emojis
        if not include_emojis:
            cleaned = cls.EMOJI_PATTERN.sub('', cleaned)
        
        # Remove mentions for cleaner sentiment analysis
        cleaned = re.sub(r'@\w+', '', cleaned)
        cleaned = ' '.join(cleaned.split())
        
        return {
            'cleaned_text': cleaned,
            'hashtags': hashtags,
            'mentions': mentions,
            'has_urls': has_urls,
            'emoji_count': emoji_count
        }


class DataLabeler:
    """
    Prepare structured data for sentiment and behavior modeling.
    Labels and categorizes scraped data for downstream ML pipelines.
    """
    
    # Sentiment indicators (basic lexicon-based)
    POSITIVE_INDICATORS = {
        'love', 'great', 'amazing', 'awesome', 'excellent', 'good', 'best',
        'happy', 'wonderful', 'fantastic', 'brilliant', 'perfect', 'thank',
        '❤️', '😊', '👍', '🎉', '💯', '🙏', '😍', '🔥'
    }
    
    NEGATIVE_INDICATORS = {
        'hate', 'bad', 'terrible', 'awful', 'worst', 'horrible', 'angry',
        'sad', 'disappointed', 'frustrating', 'annoying', 'useless',
        '😢', '😡', '👎', '💔', '😤', '🤮'
    }
    
    @classmethod
    def estimate_sentiment(cls, text: str) -> str:
        """
        Basic sentiment estimation for labeling.
        Returns: 'positive', 'negative', or 'neutral'
        
        Note: For production ML, use a proper sentiment model.
        This is just for initial data labeling/filtering.
        """
        if not text:
            return 'neutral'
            
        text_lower = text.lower()
        
        positive_count = sum(1 for word in cls.POSITIVE_INDICATORS if word in text_lower)
        negative_count = sum(1 for word in cls.NEGATIVE_INDICATORS if word in text_lower)
        
        if positive_count > negative_count:
            return 'positive'
        elif negative_count > positive_count:
            return 'negative'
        return 'neutral'
    
    @classmethod
    def estimate_engagement_level(cls, likes: int, comments: int, platform: str) -> str:
        """
        Estimate engagement level based on platform norms.
        Returns: 'low', 'medium', 'high', or 'viral'
        """
        # Platform-specific thresholds
        thresholds = {
            'instagram': {'medium': 100, 'high': 1000, 'viral': 10000},
            'youtube': {'medium': 1000, 'high': 10000, 'viral': 100000},
            'twitter': {'medium': 50, 'high': 500, 'viral': 5000},
            'reddit': {'medium': 100, 'high': 1000, 'viral': 10000},
        }
        
        levels = thresholds.get(platform, thresholds['twitter'])
        total_engagement = likes + (comments * 2)  # Weight comments higher
        
        if total_engagement >= levels['viral']:
            return 'viral'
        elif total_engagement >= levels['high']:
            return 'high'
        elif total_engagement >= levels['medium']:
            return 'medium'
        return 'low'
    
    @classmethod
    def label_post(cls, post_data: dict) -> dict:
        """
        Add sentiment and engagement labels to a post.
        
        Args:
            post_data: Dictionary with post data (from any scraper)
            
        Returns:
            Original dict with added label fields
        """
        # Safely get values
        text = post_data.get('post_text', '')
        likes = int(post_data.get('likes', 0) or 0)
        comments = int(post_data.get('comments', 0) or 0)
        platform = post_data.get('platform', 'unknown')
        
        # Add labels
        post_data['sentiment_label'] = cls.estimate_sentiment(text)
        post_data['engagement_level'] = cls.estimate_engagement_level(likes, comments, platform)
        
        return post_data
