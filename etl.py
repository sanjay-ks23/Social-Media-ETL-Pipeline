"""
ETL Pipeline for Social Media Data

Complete Extract-Transform-Load pipeline with:
- Transform: Text cleaning, sentiment labeling, data enrichment
- Load: Batch loading into DuckDB with validation

This module handles the T and L in ETL (scrapers handle E).
"""

import re
import json
from datetime import datetime
from typing import List, Dict, Optional, Generator, Any
import logging

logger = logging.getLogger(__name__)


class Transformer:
    """
    Transform raw scraped data into clean, enriched, labeled datasets.
    
    Pipeline stages:
    1. Clean: Remove noise, normalize text
    2. Enrich: Add metadata, extract entities
    3. Label: Sentiment, engagement level, topics
    4. Validate: Ensure data quality
    """
    
    # Text patterns
    URL_PATTERN = re.compile(r'https?://\S+|www\.\S+')
    MENTION_PATTERN = re.compile(r'@\w+')
    HASHTAG_PATTERN = re.compile(r'#(\w+)')
    EMOJI_PATTERN = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+", 
        flags=re.UNICODE
    )
    
    # Sentiment lexicons
    POSITIVE_WORDS = {
        'love', 'loved', 'loving', 'great', 'amazing', 'awesome', 'excellent',
        'good', 'best', 'happy', 'wonderful', 'fantastic', 'brilliant', 'perfect',
        'beautiful', 'incredible', 'outstanding', 'superb', 'magnificent',
        'thank', 'thanks', 'grateful', 'appreciate', 'excited', 'joy', 'blessed',
        'recommend', 'recommended', 'favorite', 'favourite', 'impressive'
    }
    
    NEGATIVE_WORDS = {
        'hate', 'hated', 'hating', 'bad', 'terrible', 'awful', 'worst', 'horrible',
        'angry', 'sad', 'disappointed', 'disappointing', 'frustrating', 'frustrated',
        'annoying', 'annoyed', 'useless', 'waste', 'poor', 'pathetic', 'disgusting',
        'ugly', 'stupid', 'boring', 'fail', 'failed', 'failing', 'sucks', 'broken',
        'scam', 'fake', 'trash', 'garbage', 'nightmare', 'disappoints'
    }
    
    # Engagement thresholds by platform
    ENGAGEMENT_THRESHOLDS = {
        'instagram': {'low': 50, 'medium': 500, 'high': 5000, 'viral': 50000},
        'youtube': {'low': 100, 'medium': 1000, 'high': 10000, 'viral': 100000},
        'twitter': {'low': 10, 'medium': 100, 'high': 1000, 'viral': 10000},
        'reddit': {'low': 10, 'medium': 100, 'high': 1000, 'viral': 10000},
    }
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize transformer with optional config.
        
        Args:
            config: Optional configuration dict
        """
        self.config = config or {}
        self.stats = {
            'processed': 0,
            'errors': 0,
            'sentiment': {'positive': 0, 'negative': 0, 'neutral': 0}
        }
    
    def transform(self, posts: List[Dict]) -> List[Dict]:
        """
        Main transformation pipeline.
        
        Args:
            posts: List of raw post dictionaries
            
        Returns:
            List of transformed, enriched post dictionaries
        """
        transformed = []
        
        for post in posts:
            try:
                # Run transformation pipeline
                result = self._transform_single(post)
                if result:
                    transformed.append(result)
                    self.stats['processed'] += 1
            except Exception as e:
                logger.warning(f"Transform error for post {post.get('post_id')}: {e}")
                self.stats['errors'] += 1
                continue
        
        logger.info(f"Transformed {len(transformed)} posts ({self.stats['errors']} errors)")
        return transformed
    
    def _transform_single(self, post: Dict) -> Optional[Dict]:
        """Transform a single post through the pipeline."""
        if not post.get('post_id'):
            return None
        
        result = post.copy()
        
        # 1. Clean text
        result['post_text'] = self._clean_text(post.get('post_text', ''))
        
        # 2. Extract and normalize hashtags
        result['hashtags'] = self._normalize_hashtags(post.get('hashtags', ''))
        
        # 3. Normalize numeric fields
        result['likes'] = self._safe_int(post.get('likes', 0))
        result['comments'] = self._safe_int(post.get('comments', 0))
        result['retweet_count'] = self._safe_int(post.get('retweet_count', 0))
        result['view_count'] = self._safe_int(post.get('view_count', 0))
        
        # 4. Parse and normalize timestamp
        result['timestamp'] = self._normalize_timestamp(post.get('timestamp'))
        
        # 5. Add sentiment label
        result['sentiment_label'] = self._analyze_sentiment(result['post_text'])
        self.stats['sentiment'][result['sentiment_label']] += 1
        
        # 6. Add engagement level
        result['engagement_level'] = self._calculate_engagement(
            result['likes'],
            result['comments'],
            post.get('platform', 'unknown')
        )
        
        # 7. Extract metadata
        result['word_count'] = len(result['post_text'].split())
        result['has_media'] = bool(post.get('image_url'))
        result['mention_count'] = len(self.MENTION_PATTERN.findall(post.get('post_text', '')))
        result['url_count'] = len(self.URL_PATTERN.findall(post.get('post_text', '')))
        
        # 8. Set processing timestamp
        result['processed_at'] = datetime.now().isoformat()
        
        return result
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""
        
        # Decode HTML entities
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = text.replace('&quot;', '"')
        text = text.replace('&#39;', "'")
        text = text.replace('&#x200B;', '')
        
        # Replace special Unicode characters
        replacements = {
            '\u2018': "'", '\u2019': "'",  # Smart quotes
            '\u201c': '"', '\u201d': '"',
            '\u2013': '-', '\u2014': '--',
            '\u200b': '', '\ufeff': '',     # Zero-width chars
            '\u00a0': ' ',                   # Non-breaking space
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # Normalize whitespace
        text = ' '.join(text.split())
        
        return text.strip()
    
    def _normalize_hashtags(self, hashtags: str) -> str:
        """Normalize hashtag string."""
        if not hashtags:
            return ""
        
        # Split, clean, and rejoin
        tags = [tag.strip().lower() for tag in hashtags.split(',')]
        tags = [tag for tag in tags if tag and len(tag) > 1]
        
        return ','.join(tags)
    
    def _safe_int(self, value: Any) -> int:
        """Safely convert value to integer."""
        if value is None or value == '':
            return 0
        try:
            return int(float(str(value).replace(',', '')))
        except (ValueError, TypeError):
            return 0
    
    def _normalize_timestamp(self, timestamp: Any) -> Optional[str]:
        """Normalize timestamp to ISO format."""
        if not timestamp:
            return None
        
        if isinstance(timestamp, datetime):
            return timestamp.isoformat()
        
        if isinstance(timestamp, str):
            # Try parsing common formats
            formats = [
                '%Y-%m-%dT%H:%M:%S.%fZ',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%dT%H:%M:%S.%f%z',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
            ]
            
            # Handle timezone suffix
            ts = timestamp.replace('Z', '+00:00')
            
            for fmt in formats:
                try:
                    return datetime.strptime(timestamp, fmt).isoformat()
                except ValueError:
                    continue
            
            # Return as-is if can't parse
            return timestamp
        
        return None
    
    def _analyze_sentiment(self, text: str) -> str:
        """
        Analyze text sentiment using lexicon-based approach.
        
        Returns: 'positive', 'negative', or 'neutral'
        """
        if not text:
            return 'neutral'
        
        text_lower = text.lower()
        words = set(re.findall(r'\b\w+\b', text_lower))
        
        positive_count = len(words & self.POSITIVE_WORDS)
        negative_count = len(words & self.NEGATIVE_WORDS)
        
        # Check for emoji sentiment
        emojis = self.EMOJI_PATTERN.findall(text)
        positive_emojis = {'😊', '😍', '❤️', '👍', '🎉', '💯', '🙏', '😁', '🔥', '💪'}
        negative_emojis = {'😢', '😡', '👎', '💔', '😤', '🤮', '😭', '😠', '🙄'}
        
        for emoji in emojis:
            if emoji in positive_emojis:
                positive_count += 1
            elif emoji in negative_emojis:
                negative_count += 1
        
        if positive_count > negative_count:
            return 'positive'
        elif negative_count > positive_count:
            return 'negative'
        return 'neutral'
    
    def _calculate_engagement(self, likes: int, comments: int, platform: str) -> str:
        """
        Calculate engagement level based on platform norms.
        
        Returns: 'low', 'medium', 'high', or 'viral'
        """
        thresholds = self.ENGAGEMENT_THRESHOLDS.get(
            platform, 
            self.ENGAGEMENT_THRESHOLDS['twitter']
        )
        
        # Weight comments higher than likes
        engagement_score = likes + (comments * 3)
        
        if engagement_score >= thresholds['viral']:
            return 'viral'
        elif engagement_score >= thresholds['high']:
            return 'high'
        elif engagement_score >= thresholds['medium']:
            return 'medium'
        return 'low'
    
    def get_stats(self) -> Dict:
        """Get transformation statistics."""
        return self.stats.copy()


class Loader:
    """
    Load transformed data into the database.
    
    Features:
    - Batch loading for performance
    - Duplicate detection
    - Validation before insert
    - Transaction support
    """
    
    def __init__(self, database):
        """
        Initialize loader with database connection.
        
        Args:
            database: Database instance from database.py
        """
        self.db = database
        self.stats = {
            'loaded': 0,
            'duplicates': 0,
            'errors': 0
        }
    
    def load(self, posts: List[Dict], batch_size: int = 100) -> int:
        """
        Load posts into database.
        
        Args:
            posts: List of transformed post dictionaries
            batch_size: Records per batch
            
        Returns:
            Number of records loaded
        """
        if not posts:
            return 0
        
        loaded = self.db.insert_posts(posts)
        self.stats['loaded'] += loaded
        
        logger.info(f"Loaded {loaded} posts into database")
        return loaded
    
    def load_with_validation(self, posts: List[Dict]) -> Dict:
        """
        Load posts with validation and return detailed results.
        
        Args:
            posts: List of transformed post dictionaries
            
        Returns:
            Dict with loading results and errors
        """
        results = {
            'success': [],
            'errors': [],
            'duplicates': []
        }
        
        for post in posts:
            # Validate required fields
            if not post.get('post_id') or not post.get('platform'):
                results['errors'].append({
                    'post': post,
                    'error': 'Missing required fields (post_id, platform)'
                })
                continue
            
            # Check for duplicate
            existing = self.db.query(
                "SELECT id FROM posts WHERE post_id = ? AND platform = ?",
                [post['post_id'], post['platform']]
            )
            
            if existing:
                results['duplicates'].append(post['post_id'])
                continue
            
            try:
                self.db.insert_posts([post])
                results['success'].append(post['post_id'])
            except Exception as e:
                results['errors'].append({
                    'post_id': post['post_id'],
                    'error': str(e)
                })
        
        return results
    
    def get_stats(self) -> Dict:
        """Get loader statistics."""
        return self.stats.copy()


class ETLPipeline:
    """
    Complete ETL Pipeline orchestrator.
    
    Combines:
    - Extract: Scrapers (handled externally)
    - Transform: Text cleaning, enrichment, labeling
    - Load: Database insertion
    
    Usage:
        from etl import ETLPipeline
        from database import Database
        
        pipeline = ETLPipeline(Database())
        results = pipeline.run(scraped_posts)
    """
    
    def __init__(self, database, config: Optional[Dict] = None):
        """
        Initialize ETL pipeline.
        
        Args:
            database: Database instance
            config: Optional configuration
        """
        self.db = database
        self.transformer = Transformer(config)
        self.loader = Loader(database)
        self.config = config or {}
    
    def run(self, posts: List[Dict]) -> Dict:
        """
        Run the complete ETL pipeline.
        
        Args:
            posts: Raw scraped posts (from any scraper)
            
        Returns:
            Pipeline results with stats
        """
        start_time = datetime.now()
        
        logger.info(f"Starting ETL pipeline with {len(posts)} posts")
        
        # Transform
        transformed = self.transformer.transform(posts)
        
        # Load
        loaded_count = self.loader.load(transformed)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        results = {
            'input_count': len(posts),
            'transformed_count': len(transformed),
            'loaded_count': loaded_count,
            'duration_seconds': round(duration, 2),
            'records_per_second': round(len(posts) / max(duration, 0.001), 2),
            'transform_stats': self.transformer.get_stats(),
            'loader_stats': self.loader.get_stats()
        }
        
        logger.info(f"ETL pipeline completed in {duration:.2f}s")
        return results
    
    def run_streaming(self, posts_generator: Generator) -> Generator:
        """
        Run pipeline in streaming mode for large datasets.
        
        Args:
            posts_generator: Generator yielding post dicts
            
        Yields:
            Transformed and loaded posts
        """
        batch = []
        batch_size = self.config.get('batch_size', 100)
        
        for post in posts_generator:
            batch.append(post)
            
            if len(batch) >= batch_size:
                transformed = self.transformer.transform(batch)
                self.loader.load(transformed)
                
                for t in transformed:
                    yield t
                
                batch = []
        
        # Process remaining
        if batch:
            transformed = self.transformer.transform(batch)
            self.loader.load(transformed)
            
            for t in transformed:
                yield t


def create_pipeline(db_path: str = "social_media.duckdb") -> ETLPipeline:
    """
    Create a ready-to-use ETL pipeline.
    
    Args:
        db_path: Path to database file
        
    Returns:
        Configured ETLPipeline instance
    """
    from database import Database
    return ETLPipeline(Database(db_path))
