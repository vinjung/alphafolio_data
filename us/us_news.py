"""
    
AlphaVantage News API      PostgreSQL 

 :
  pip install asyncpg pandas numpy aiohttp

:
  ALPHAVANTAGE_API_KEY

API :
- sort=RELEVANCE, limit=30
- time_range:    
- ticker   
- API : 300/, 0.2  
"""

import asyncpg
import aiohttp
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass
from pathlib import Path
import asyncio
import time
from dotenv import load_dotenv

#   
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

#    (Railway Volume path support)
if os.getenv('RAILWAY_ENVIRONMENT'):
    LOG_DIR = Path('/app/log')
else:
    LOG_DIR = Path('log')

COLLECTION_LOG_FILE = LOG_DIR / f"us_news_collection_{datetime.now().strftime('%Y%m%d')}.json"

# ===========================
# 1.   
# ===========================

@dataclass
class NewsArticle:
    """   """
    title: str
    url: str
    time_published: datetime
    authors: List[str]
    summary: str
    banner_image: str
    source: str
    category_within_source: str
    source_domain: str
    topics: Dict[str, Any]
    overall_sentiment_score: float
    overall_sentiment_label: str
    ticker: str
    relevance_score: float
    ticker_sentiment_score: float
    ticker_sentiment_label: str

# ===========================
# 2. AlphaVantage API 
# ===========================

class AlphaVantageClient:
    """AlphaVantage News API """

    def __init__(self):
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY   .")

        self.base_url = "https://www.alphavantage.co/query"
        self.session = None
        print(" AlphaVantage API   ")

    async def initialize(self):
        """aiohttp  """
        self.session = aiohttp.ClientSession()

    async def close(self):
        """aiohttp  """
        if self.session:
            await self.session.close()

    async def get_news_for_ticker(self, ticker: str, limit: int = 30,
                                   time_from: datetime = None,
                                   time_to: datetime = None) -> Dict[str, Any]:
        """
         ticker     (  )

        Args:
            ticker (str):   (: 'AAPL', 'MSFT')
            limit (int):    (: 30)
            time_from (datetime):   (None  06:00)
            time_to (datetime):   (None  )

        Returns:
            Dict: AlphaVantage API
        """
        #  None
        if time_from is None:
            today = datetime.now()
            yesterday = today - timedelta(days=1)
            time_from = yesterday.replace(hour=6, minute=0, second=0, microsecond=0)

        if time_to is None:
            time_to = datetime.now()

        # API   ( limit )
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker,
            "time_from": time_from.strftime("%Y%m%dT%H%M"),
            "time_to": time_to.strftime("%Y%m%dT%H%M"),
            "sort": "RELEVANCE",
            "limit": str(limit),  #
            "apikey": self.api_key
        }

        try:
            async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()

                    # API  
                    if "Error Message" in data:
                        print(f" [US_NEWS] API error for {ticker}: {data['Error Message']}")
                        return {"feed": []}

                    if "Information" in data:
                        print(f" [US_NEWS] API limit for {ticker}: {data['Information']}")
                        return {"feed": []}

                    return data
                else:
                    print(f" [US_NEWS] API failed for {ticker}: Status {response.status}")
                    return {"feed": []}

        except aiohttp.ClientError as e:
            print(f" [US_NEWS] API error for {ticker}: {e}")
            return {"feed": []}
        except json.JSONDecodeError as e:
            print(f" [US_NEWS] JSON parse error for {ticker}: {e}")
            return {"feed": []}
        except Exception as e:
            print(f" [US_NEWS] Unexpected error for {ticker}: {e}")
            return {"feed": []}

# ===========================
# 3.   
# ===========================

class NewsDataProcessor:
    """    """
    
    @staticmethod
    def parse_time_published(time_str: str) -> datetime:
        """  datetime  """
        try:
            return datetime.strptime(time_str, "%Y%m%dT%H%M%S")
        except ValueError:
            try:
                return datetime.strptime(time_str, "%Y%m%dT%H%M")
            except ValueError:
                print(f"   : {time_str},   ")
                return datetime.now()
    
    @staticmethod
    def extract_articles_by_ticker(api_response: Dict[str, Any], target_ticker: str = None) -> List[NewsArticle]:
        """
        API  ticker   
        target_ticker   ticker 
        
        Args:
            api_response: AlphaVantage API  
            target_ticker:   ticker (: 'AAPL')
        """
        articles = []
        feed = api_response.get("feed", [])
        
        for news_item in feed:
            try:
                #    
                base_info = {
                    "title": news_item.get("title", ""),
                    "url": news_item.get("url", ""),
                    "time_published": NewsDataProcessor.parse_time_published(
                        news_item.get("time_published", "")
                    ),
                    "authors": news_item.get("authors", []),
                    "summary": news_item.get("summary", ""),
                    "banner_image": news_item.get("banner_image", ""),
                    "source": news_item.get("source", ""),
                    "category_within_source": news_item.get("category_within_source", "n/a"),
                    "source_domain": news_item.get("source_domain", ""),
                    "topics": news_item.get("topics", []),
                    "overall_sentiment_score": float(news_item.get("overall_sentiment_score", 0)),
                    "overall_sentiment_label": news_item.get("overall_sentiment_label", "Neutral")
                }
                
                # ticker    ( )
                ticker_sentiments = news_item.get("ticker_sentiment", [])
                for ticker_info in ticker_sentiments:
                    ticker = ticker_info.get("ticker", "")
                    
                    # target_ticker    ticker 
                    if target_ticker and ticker != target_ticker:
                        continue
                    
                    article = NewsArticle(
                        **base_info,
                        ticker=ticker,
                        relevance_score=float(ticker_info.get("relevance_score", 0)),
                        ticker_sentiment_score=float(ticker_info.get("ticker_sentiment_score", 0)),
                        ticker_sentiment_label=ticker_info.get("ticker_sentiment_label", "Neutral")
                    )
                    articles.append(article)
                    
            except Exception as e:
                print(f"    : {e}")
                continue
        
        return articles

# ===========================
# 4.   
# ===========================

class DatabaseManager:
    """ PostgreSQL  """
    
    def __init__(self):
        self.connection_pool = None
    
    async def initialize(self):
        """  """
        db_url = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
        self.connection_pool = await asyncpg.create_pool(db_url, min_size=1, max_size=10)
        print(" PostgreSQL    ")
    
    async def execute_query(self, query: str, *params) -> List[Dict]:
        """    """
        async with self.connection_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]
    
    async def execute_command(self, query: str, *params) -> str:
        """INSERT/UPDATE/DELETE  """
        async with self.connection_pool.acquire() as conn:
            result = await conn.execute(query, *params)
            return result
    
    async def close(self):
        """  """
        if self.connection_pool:
            await self.connection_pool.close()

# ===========================
# 5.   ( )
# ===========================

class NewsCollectionLogger:
    """    ( )"""

    def __init__(self, log_file: Path = COLLECTION_LOG_FILE):
        self.log_file = log_file
        self.collected_items: Set[str] = set()  # (url, ticker) 
        self.completed_tickers: Set[str] = set()  # ticker    
        self._load_log()

    def _load_log(self):
        """  """
        try:
            if self.log_file.exists():
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.collected_items = set(data.get('collected_items', []))
                    self.completed_tickers = set(data.get('completed_tickers', []))
                print(f"   : {len(self.collected_items):,}  (  skip)")
                print(f"   ticker: {len(self.completed_tickers):,}")
            else:
                self.log_file.parent.mkdir(parents=True, exist_ok=True)
                print("     ( )")
        except Exception as e:
            print(f"   : {e}")
            self.collected_items = set()
            self.completed_tickers = set()

    def is_ticker_completed(self, ticker: str) -> bool:
        """Ticker   """
        return ticker in self.completed_tickers

    def is_collected(self, url: str, ticker: str) -> bool:
        """   """
        key = f"{url}|{ticker}"
        return key in self.collected_items

    def mark_collected(self, url: str, ticker: str):
        """  """
        key = f"{url}|{ticker}"
        self.collected_items.add(key)

    def mark_ticker_completed(self, ticker: str):
        """Ticker   """
        self.completed_tickers.add(ticker)

    def save_log(self):
        """  """
        try:
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'last_updated': datetime.now().isoformat(),
                    'total_items': len(self.collected_items),
                    'completed_tickers': list(self.completed_tickers),
                    'collected_items': list(self.collected_items)
                }, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"   : {e}")

# ===========================
# 6. US  
# ===========================

class USNewsCollector:
    """     """

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.alpha_client = AlphaVantageClient()
        self.processor = NewsDataProcessor()
        self.collection_logger = NewsCollectionLogger()

    async def initialize(self):
        """ """
        await self.db_manager.initialize()
        await self.alpha_client.initialize()
        print(" US    ")
    
    async def collect_news_for_ticker(self, ticker: str, limit: int = 30,
                                      time_from: datetime = None,
                                      time_to: datetime = None) -> int:
        """
         ticker

        Args:
            ticker (str):   (: 'AAPL')
            limit (int):
            time_from (datetime):   (None  06:00)
            time_to (datetime):   (None  )

        Returns:
            int:
        """
        # 0. Ticker     (API  !)
        if self.collection_logger.is_ticker_completed(ticker):
            return 0

        # 1. API
        api_response = await self.alpha_client.get_news_for_ticker(ticker, limit, time_from, time_to)

        if not api_response.get("feed"):
            #   ticker   (   skip)
            self.collection_logger.mark_ticker_completed(ticker)
            return 0

        # 2.   ( ticker )
        articles = self.processor.extract_articles_by_ticker(api_response, ticker)

        if not articles:
            #    ticker  
            self.collection_logger.mark_ticker_completed(ticker)
            return 0

        # 3.  
        saved_count = await self._save_articles_to_db(articles, ticker)

        # 4. Ticker   
        self.collection_logger.mark_ticker_completed(ticker)

        return saved_count
    
    async def _save_articles_to_db(self, articles: List[NewsArticle], ticker: str) -> int:
        """    (Batch Insert + Transaction)"""

        # 1.    ( skip)
        articles_to_save = []
        skipped_count = 0

        for article in articles:
            if self.collection_logger.is_collected(article.url, article.ticker):
                skipped_count += 1
                continue
            articles_to_save.append(article)

        if not articles_to_save:
            return 0

        # 2. Batch Insert with Transaction
        upsert_query = """
        INSERT INTO us_news (
            title, url, time_published, authors, summary, banner_image,
            source, category_within_source, source_domain, topics,
            overall_sentiment_score, overall_sentiment_label,
            ticker, relevance_score_t, ticker_sentiment_score,
            ticker_sentiment_label, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        ON CONFLICT (url, ticker) DO UPDATE SET
            title = EXCLUDED.title,
            time_published = EXCLUDED.time_published,
            authors = EXCLUDED.authors,
            summary = EXCLUDED.summary,
            banner_image = EXCLUDED.banner_image,
            source = EXCLUDED.source,
            category_within_source = EXCLUDED.category_within_source,
            source_domain = EXCLUDED.source_domain,
            topics = EXCLUDED.topics,
            overall_sentiment_score = EXCLUDED.overall_sentiment_score,
            overall_sentiment_label = EXCLUDED.overall_sentiment_label,
            relevance_score_t = EXCLUDED.relevance_score_t,
            ticker_sentiment_score = EXCLUDED.ticker_sentiment_score,
            ticker_sentiment_label = EXCLUDED.ticker_sentiment_label,
            created_at = EXCLUDED.created_at
        """

        # 3. Prepare batch values
        batch_values = []
        for article in articles_to_save:
            batch_values.append((
                article.title,
                article.url,
                article.time_published,
                article.authors,
                article.summary,
                article.banner_image,
                article.source,
                article.category_within_source,
                article.source_domain,
                json.dumps(article.topics),
                article.overall_sentiment_score,
                article.overall_sentiment_label,
                article.ticker,
                article.relevance_score,
                article.ticker_sentiment_score,
                article.ticker_sentiment_label,
                datetime.now()
            ))

        # 4. Execute batch insert with transaction
        try:
            async with self.db_manager.connection_pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(upsert_query, batch_values)

            # 5. Mark all as collected in log
            for article in articles_to_save:
                self.collection_logger.mark_collected(article.url, article.ticker)

            saved_count = len(articles_to_save)
            return saved_count

        except Exception as e:
            print(f" [US_NEWS] Batch save error for {ticker}: {e}")
            return 0
    
    async def collect_news_for_multiple_tickers(self, tickers: List[str], delay: float = 0.2,
                                                time_from: datetime = None,
                                                time_to: datetime = None) -> Dict[str, int]:
        """
         ticker
        API (300/)

        Args:
            tickers (List[str]):
            delay (float): ticker    (, : 0.2)
            time_from (datetime):   (None  06:00)
            time_to (datetime):   (None  )

        Returns:
            Dict[str, int]: ticker
        """
        results = {}

        #  ticker 
        uncollected_tickers = [
            t for t in tickers
            if not self.collection_logger.is_ticker_completed(t)
        ]

        skipped_count = len(tickers) - len(uncollected_tickers)

        print(f"  ticker: {len(tickers)}")
        print(f"  : {skipped_count} ( skip)")
        print(f"  : {len(uncollected_tickers)}")
        print("")

        total_saved = 0

        for i, ticker in enumerate(uncollected_tickers, 1):
            try:
                #
                count = await self.collect_news_for_ticker(ticker, time_from=time_from, time_to=time_to)
                results[ticker] = count
                total_saved += count

                #   (VWAP )
                if count > 0:
                    print(f"[US_NEWS API] [{i}/{len(uncollected_tickers)}] Saved {ticker} ({count} articles)")
                else:
                    print(f"[US_NEWS API] [{i}/{len(uncollected_tickers)}] Queued {ticker}")

                # API   
                if i < len(uncollected_tickers):
                    await asyncio.sleep(delay)

            except Exception as e:
                print(f" [US_NEWS] Error for {ticker}: {e}")
                results[ticker] = 0
                continue

        # Skip ticker  0 
        for ticker in tickers:
            if ticker not in results:
                results[ticker] = 0

        #  
        self.collection_logger.save_log()

        #  
        print(f"\n[US_NEWS DB] Total saved: {total_saved} articles")
        print(f"[US_NEWS] Completed {len(uncollected_tickers)} tickers")

        return results

    async def close(self):
        """ """
        self.collection_logger.save_log()
        await self.alpha_client.close()
        await self.db_manager.close()

# ===========================
# 6.    
# ===========================

async def test_single_ticker(ticker: str = "AAPL"):
    """ ticker   """
    collector = USNewsCollector()
    
    try:
        await collector.initialize()
        
        print(f" {ticker}    ...")
        count = await collector.collect_news_for_ticker(ticker)
        
        print(f"  : {count}  ")
        
    except Exception as e:
        print(f"  : {e}")
    finally:
        await collector.close()

async def test_multiple_tickers():
    """ ticker   """
    collector = USNewsCollector()
    
    #   
    test_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    
    try:
        await collector.initialize()
        
        print("  ticker    ...")
        results = await collector.collect_news_for_multiple_tickers(
            test_tickers,
            delay=0.2  # 0.2  (300 calls/)
        )
        
        print("  !")
        return results
        
    except Exception as e:
        print(f"  : {e}")
    finally:
        await collector.close()

async def collect_all_us_stocks(news_delay: float = 0.2):
    """
        

    Args:
        news_delay (float): API   ()
            - 0.2 = 300 calls/min
            - 0.6 = 100 calls/min
            - 0.8 = 75 calls/min
    """
    collector = USNewsCollector()

    try:
        await collector.initialize()

        # 1. us_stock_basic    
        query = """
        SELECT symbol FROM us_stock_basic
        WHERE is_active = true
        ORDER BY symbol
        """

        result = await collector.db_manager.execute_query(query)
        all_symbols = [row['symbol'] for row in result]

        calls_per_min = int(60 / news_delay) if news_delay > 0 else 60
        print(f"\n  {len(all_symbols):,}    ")
        print(f"⏱  API : {calls_per_min} calls/ ({news_delay} )")
        print(f"   :  {len(all_symbols) * news_delay / 60:.1f} ({len(all_symbols) * news_delay / 3600:.1f})")
        print("")

        # 2.    
        results = await collector.collect_news_for_multiple_tickers(
            all_symbols,
            delay=news_delay
        )

        print("\n   !")
        print(f"    : {len(all_symbols)}")
        print(f"    : {sum(results.values())} ")

        return results

    except Exception as e:
        print(f"  : {e}")
    finally:
        await collector.close()

async def main():
    """  """
    print(" US    ")
    print("")

    #     
    await collect_all_us_stocks()

#  
def check_dependencies():
    """   """
    required_libs = ["asyncpg", "aiohttp"]
    
    missing_deps = []
    for lib in required_libs:
        try:
            __import__(lib)
        except ImportError:
            missing_deps.append(lib)
    
    if missing_deps:
        print("  :")
        for dep in missing_deps:
            print(f"   pip install {dep}")
        return False
    
    print("     .")
    return True

if __name__ == "__main__":
    #  
    if check_dependencies():
        asyncio.run(main())
    else:
        print("\n   .")