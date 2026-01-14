"""
Database Layer for Social Media ETL Pipeline

Uses DuckDB - an embedded analytical database that provides:
- Microsecond query response times
- SQL interface for complex analytics
- Columnar storage for efficient aggregations
- Zero configuration (embedded, no server needed)
- Native Parquet/CSV import/export
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Optional, Any
import logging

try:
    import duckdb
except ImportError:
    raise ImportError("DuckDB is required. Install with: pip install duckdb")

logger = logging.getLogger(__name__)


class Database:
    """
    Production-grade embedded database for social media data.
    
    Uses DuckDB for:
    - Microsecond query performance
    - SQL analytics (aggregations, joins, window functions)
    - Efficient columnar storage
    - No server setup required
    
    Usage:
        db = Database()
        db.insert_posts(posts)
        results = db.query("SELECT * FROM posts WHERE platform = 'reddit'")
    """
    
    # Schema definition
    SCHEMA = """
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY,
            post_id VARCHAR NOT NULL,
            platform VARCHAR NOT NULL,
            post_text TEXT,
            hashtags VARCHAR,
            timestamp TIMESTAMP,
            image_url VARCHAR,
            likes INTEGER DEFAULT 0,
            comments INTEGER DEFAULT 0,
            author VARCHAR,
            url VARCHAR,
            scraped_at TIMESTAMP,
            
            -- Platform-specific fields
            subreddit VARCHAR,              -- Reddit
            upvote_ratio FLOAT,             -- Reddit
            retweet_count INTEGER,          -- Twitter
            view_count INTEGER,             -- YouTube
            duration VARCHAR,               -- YouTube
            channel_id VARCHAR,             -- YouTube
            
            -- ETL metadata
            sentiment_label VARCHAR,
            engagement_level VARCHAR,
            processed_at TIMESTAMP,
            
            -- Unique constraint to avoid duplicates
            UNIQUE(post_id, platform)
        );
        
        -- Indexes for fast queries
        CREATE INDEX IF NOT EXISTS idx_platform ON posts(platform);
        CREATE INDEX IF NOT EXISTS idx_timestamp ON posts(timestamp);
        CREATE INDEX IF NOT EXISTS idx_author ON posts(author);
        CREATE INDEX IF NOT EXISTS idx_sentiment ON posts(sentiment_label);
    """
    
    def __init__(self, db_path: str = "social_media.duckdb"):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()
        logger.info(f"Database initialized: {db_path}")
    
    def _init_schema(self):
        """Create tables and indexes if they don't exist."""
        self.conn.execute(self.SCHEMA)
        self.conn.commit()
    
    def insert_posts(self, posts: List[Dict], batch_size: int = 1000) -> int:
        """
        Insert posts into database with upsert logic.
        
        Args:
            posts: List of post dictionaries
            batch_size: Number of records per batch
            
        Returns:
            Number of records inserted/updated
        """
        if not posts:
            return 0
        
        inserted = 0
        
        for post in posts:
            try:
                # Parse timestamp
                timestamp = None
                if post.get('timestamp'):
                    try:
                        timestamp = datetime.fromisoformat(
                            post['timestamp'].replace('Z', '+00:00')
                        )
                    except (ValueError, AttributeError):
                        timestamp = None
                
                scraped_at = None
                if post.get('scraped_at'):
                    try:
                        scraped_at = datetime.fromisoformat(post['scraped_at'])
                    except (ValueError, AttributeError):
                        scraped_at = datetime.now()
                
                # Insert with conflict handling
                self.conn.execute("""
                    INSERT INTO posts (
                        post_id, platform, post_text, hashtags, timestamp,
                        image_url, likes, comments, author, url, scraped_at,
                        subreddit, upvote_ratio, retweet_count, view_count,
                        duration, channel_id, sentiment_label, engagement_level,
                        processed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (post_id, platform) DO UPDATE SET
                        likes = excluded.likes,
                        comments = excluded.comments,
                        scraped_at = excluded.scraped_at
                """, [
                    post.get('post_id'),
                    post.get('platform'),
                    post.get('post_text'),
                    post.get('hashtags'),
                    timestamp,
                    post.get('image_url'),
                    int(post.get('likes', 0) or 0),
                    int(post.get('comments', 0) or 0),
                    post.get('author'),
                    post.get('url'),
                    scraped_at,
                    post.get('subreddit'),
                    float(post.get('upvote_ratio', 0) or 0),
                    int(post.get('retweet_count', 0) or 0),
                    int(post.get('view_count', 0) or 0),
                    post.get('duration'),
                    post.get('channel_id'),
                    post.get('sentiment_label'),
                    post.get('engagement_level'),
                    datetime.now()
                ])
                inserted += 1
                
            except Exception as e:
                logger.warning(f"Error inserting post {post.get('post_id')}: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Inserted/updated {inserted} posts")
        return inserted
    
    def query(self, sql: str, params: Optional[List] = None) -> List[Dict]:
        """
        Execute SQL query and return results as list of dicts.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
            
        Returns:
            List of result dictionaries
        """
        try:
            if params:
                result = self.conn.execute(sql, params)
            else:
                result = self.conn.execute(sql)
            
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            
            return [dict(zip(columns, row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Query error: {e}")
            return []
    
    def get_posts(
        self, 
        platform: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "scraped_at DESC"
    ) -> List[Dict]:
        """
        Get posts with optional filtering.
        
        Args:
            platform: Filter by platform (optional)
            limit: Maximum records to return
            offset: Skip this many records
            order_by: SQL ORDER BY clause
            
        Returns:
            List of post dictionaries
        """
        sql = "SELECT * FROM posts"
        params = []
        
        if platform:
            sql += " WHERE platform = ?"
            params.append(platform)
        
        sql += f" ORDER BY {order_by} LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        return self.query(sql, params)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get database statistics.
        
        Returns:
            Dictionary with stats (counts, platforms, etc.)
        """
        stats = {}
        
        # Total count
        result = self.query("SELECT COUNT(*) as count FROM posts")
        stats['total_posts'] = result[0]['count'] if result else 0
        
        # Count by platform
        result = self.query("""
            SELECT platform, COUNT(*) as count 
            FROM posts 
            GROUP BY platform 
            ORDER BY count DESC
        """)
        stats['by_platform'] = {row['platform']: row['count'] for row in result}
        
        # Count by sentiment
        result = self.query("""
            SELECT sentiment_label, COUNT(*) as count 
            FROM posts 
            WHERE sentiment_label IS NOT NULL
            GROUP BY sentiment_label
        """)
        stats['by_sentiment'] = {row['sentiment_label']: row['count'] for row in result}
        
        # Top authors
        result = self.query("""
            SELECT author, COUNT(*) as count 
            FROM posts 
            WHERE author IS NOT NULL AND author != '[deleted]'
            GROUP BY author 
            ORDER BY count DESC 
            LIMIT 10
        """)
        stats['top_authors'] = result
        
        # Date range
        result = self.query("""
            SELECT 
                MIN(timestamp) as oldest,
                MAX(timestamp) as newest
            FROM posts
            WHERE timestamp IS NOT NULL
        """)
        if result and result[0]['oldest']:
            stats['date_range'] = {
                'oldest': str(result[0]['oldest']),
                'newest': str(result[0]['newest'])
            }
        
        return stats
    
    def search(self, query: str, limit: int = 100) -> List[Dict]:
        """
        Full-text search across post content.
        
        Args:
            query: Search term
            limit: Maximum results
            
        Returns:
            Matching posts
        """
        return self.query("""
            SELECT * FROM posts 
            WHERE post_text ILIKE ?
            ORDER BY likes DESC
            LIMIT ?
        """, [f'%{query}%', limit])
    
    def get_sentiment_distribution(self, platform: Optional[str] = None) -> List[Dict]:
        """Get sentiment distribution, optionally filtered by platform."""
        sql = """
            SELECT 
                sentiment_label,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM posts
            WHERE sentiment_label IS NOT NULL
        """
        params = []
        
        if platform:
            sql += " AND platform = ?"
            params.append(platform)
        
        sql += " GROUP BY sentiment_label ORDER BY count DESC"
        
        return self.query(sql, params) if params else self.query(sql)
    
    def get_top_hashtags(self, limit: int = 20) -> List[Dict]:
        """Get most common hashtags across all posts."""
        # DuckDB supports string splitting
        return self.query("""
            SELECT 
                TRIM(hashtag) as hashtag,
                COUNT(*) as count
            FROM (
                SELECT UNNEST(STRING_SPLIT(hashtags, ',')) as hashtag
                FROM posts
                WHERE hashtags IS NOT NULL AND hashtags != ''
            )
            WHERE TRIM(hashtag) != ''
            GROUP BY TRIM(hashtag)
            ORDER BY count DESC
            LIMIT ?
        """, [limit])
    
    def export_csv(self, filepath: str, platform: Optional[str] = None):
        """Export data to CSV file."""
        sql = "SELECT * FROM posts"
        if platform:
            sql += f" WHERE platform = '{platform}'"
        
        self.conn.execute(f"COPY ({sql}) TO '{filepath}' (HEADER, DELIMITER ',')")
        logger.info(f"Exported to {filepath}")
    
    def export_parquet(self, filepath: str, platform: Optional[str] = None):
        """Export data to Parquet file (compressed, fast to load)."""
        sql = "SELECT * FROM posts"
        if platform:
            sql += f" WHERE platform = '{platform}'"
        
        self.conn.execute(f"COPY ({sql}) TO '{filepath}' (FORMAT PARQUET)")
        logger.info(f"Exported to {filepath}")
    
    def import_csv(self, filepath: str):
        """Import data from CSV file."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"CSV file not found: {filepath}")
        
        # Read CSV and insert
        result = self.conn.execute(f"""
            SELECT * FROM read_csv_auto('{filepath}')
        """).fetchall()
        
        logger.info(f"Imported {len(result)} records from {filepath}")
        return len(result)
    
    def close(self):
        """Close database connection."""
        self.conn.close()
        logger.info("Database connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Convenience function
def get_database(db_path: str = "social_media.duckdb") -> Database:
    """Get a database instance."""
    return Database(db_path)
