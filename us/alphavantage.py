# alpha/data/us/alphavantage.py
import requests
import time
import json
import asyncpg
import logging
import os
import pandas as pd
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, List
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv
import aiohttp
import asyncio
from asyncio import Queue
from collection_logger import CollectionLogger

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('log/alphavantage_data_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AlphaVantageCollector:
    def __init__(self, api_key: str, database_url: str):
        self.api_key = api_key
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1  # seconds
        self.session = None  # Will be initialized when needed

    async def get_connection(self):
        """Get PostgreSQL database connection"""
        return await asyncpg.connect(self.database_url)

    async def init_session(self):
        """Initialize aiohttp session"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    async def get_overview_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch overview data from Alpha Vantage API with retry logic"""
        await self.init_session()

        params = {
            'function': 'OVERVIEW',
            'symbol': symbol,
            'apikey': self.api_key
        }

        for attempt in range(self.retry_count):
            try:
                async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Check if API returned an error
                        if "Error Message" in data:
                            logger.error(f"API Error for {symbol}: {data['Error Message']}")
                            return None

                        # Check if we hit rate limit
                        if "Information" in data and "rate limit" in data["Information"].lower():
                            logger.warning(f"Rate limit hit for {symbol}, waiting...")
                            await asyncio.sleep(60)  # Wait 1 minute
                            continue

                        # Check if symbol exists
                        if not data or data == {} or "Symbol" not in data:
                            logger.warning(f"No data found for symbol: {symbol}")
                            return None

                        return data
                    else:
                        logger.error(f"Request failed for {symbol} (attempt {attempt + 1}): Status {response.status}")
                        if attempt < self.retry_count - 1:
                            await asyncio.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                        else:
                            logger.error(f"All retry attempts failed for {symbol}")
                            return None

            except asyncio.TimeoutError:
                logger.error(f"Timeout for {symbol} (attempt {attempt + 1})")
                if attempt < self.retry_count - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    logger.error(f"All retry attempts failed for {symbol}")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error for {symbol}: {str(e)}")
                return None

        return None

    def safe_convert(self, value: str, data_type: str) -> Any:
        """Safely convert string values to appropriate data types"""
        if not value or value == "None" or value == "-":
            return None

        try:
            if data_type == "int":
                return int(float(value))
            elif data_type == "float":
                return float(value)
            elif data_type == "date":
                return datetime.strptime(value, "%Y-%m-%d").date()
            else:
                return value
        except (ValueError, TypeError):
            return None

    def transform_data(self, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform API response data to match database schema"""
        transformed = {}

        # Direct mappings
        field_mappings = {
            'symbol': ('Symbol', str),
            'assettype': ('AssetType', str),
            'stock_name': ('Name', str),
            'description': ('Description', str),
            'cik': ('CIK', str),
            'exchange': ('Exchange', str),
            'currency': ('Currency', str),
            'country': ('Country', str),
            'sector': ('Sector', str),
            'industry': ('Industry', str),
            'address': ('Address', str),
            'officialSite': ('OfficialSite', str),
            'fiscalyearend': ('FiscalYearEnd', str),
            'latestquarter': ('LatestQuarter', 'date'),
            'market_cap': ('MarketCapitalization', 'int'),
            'ebitda': ('EBITDA', 'int'),
            'per': ('PERatio', 'float'),
            'peg': ('PEGRatio', 'float'),
            'bookvalue': ('BookValue', 'float'),
            'dividendpershare': ('DividendPerShare', 'float'),
            'dividendyield': ('DividendYield', 'float'),
            'eps': ('EPS', 'float'),
            'revenuepersharettm': ('RevenuePerShareTTM', 'float'),
            'profitmargin': ('ProfitMargin', 'float'),
            'operatingmarginttm': ('OperatingMarginTTM', 'float'),
            'returnonassetsttm': ('ReturnOnAssetsTTM', 'float'),
            'returnonequityttm': ('ReturnOnEquityTTM', 'float'),
            'revenuettm': ('RevenueTTM', 'int'),
            'grossprofitttm': ('GrossProfitTTM', 'int'),
            'dilutedepsttm': ('DilutedEPSTTM', 'float'),
            'quarterlyearningsgrowthyoy': ('QuarterlyEarningsGrowthYOY', 'float'),
            'quarterlyrevenuegrowthyoy': ('QuarterlyRevenueGrowthYOY', 'float'),
            'analysttargetprice': ('AnalystTargetPrice', 'float'),
            'analystratingstrongbuy': ('AnalystRatingStrongBuy', 'int'),
            'analystratingbuy': ('AnalystRatingBuy', 'int'),
            'analystratinghold': ('AnalystRatingHold', 'int'),
            'analystratingsell': ('AnalystRatingSell', 'int'),
            'analystratingstrongsell': ('AnalystRatingStrongSell', 'int'),
            'trailingpe': ('TrailingPE', 'float'),
            'forwardpe': ('ForwardPE', 'float'),
            'pricetosalesratiottm': ('PriceToSalesRatioTTM', 'float'),
            'pricetobookratio': ('PriceToBookRatio', 'float'),
            'evtorevenue': ('EVToRevenue', 'float'),
            'evtoebitda': ('EVToEBITDA', 'float'),
            'beta': ('Beta', 'float'),
            'week52high': ('52WeekHigh', 'float'),
            'week52low': ('52WeekLow', 'float'),
            'day50movingaverage': ('50DayMovingAverage', 'float'),
            'day200movingaverage': ('200DayMovingAverage', 'float'),
            'sharesoutstanding': ('SharesOutstanding', 'int'),
            'sharesfloat': ('SharesFloat', 'int'),
            'percentinsiders': ('PercentInsiders', 'float'),
            'percentinstitutions': ('PercentInstitutions', 'float'),
            'dividenddate': ('DividendDate', 'date'),
            'exdividenddate': ('ExDividendDate', 'date')
        }

        for db_field, (api_field, data_type) in field_mappings.items():
            api_value = api_data.get(api_field, '')
            transformed[db_field] = self.safe_convert(api_value, data_type)

        # Add timestamp
        transformed['updated_at'] = datetime.now()

        return transformed

    async def save_to_database(self, data: Dict[str, Any]) -> bool:
        """Save transformed data to database"""
        try:
            conn = await self.get_connection()

            # Prepare INSERT ON CONFLICT UPDATE query
            fields = list(data.keys())
            placeholders = ', '.join([f'${i+1}' for i in range(len(fields))])
            field_names = ', '.join(fields)

            # Create UPDATE clause for ON CONFLICT
            update_clauses = ', '.join([f'{field} = EXCLUDED.{field}' for field in fields if field != 'symbol'])

            query = f"""
                INSERT INTO us_stock_basic ({field_names})
                VALUES ({placeholders})
                ON CONFLICT (symbol) DO UPDATE SET
                {update_clauses}
            """
            values = [data[field] for field in fields]

            await conn.execute(query, *values)
            await conn.close()

            return True

        except Exception as e:
            logger.error(f"Database error for {data.get('symbol', 'unknown')}: {str(e)}")
            return False

    async def update_progress(self, symbol: str, status: str, error_message: str = None):
        """Update collection progress in database"""
        try:
            conn = await self.get_connection()

            # Cast parameters to proper types to avoid type inference issues
            await conn.execute('''
                INSERT INTO us_collection_progress
                (symbol, status, attempts, last_attempt, error_message)
                VALUES ($1::VARCHAR(10), $2::VARCHAR(20),
                    COALESCE((SELECT attempts FROM us_collection_progress WHERE symbol = $1::VARCHAR(10)), 0) + 1,
                    $3::TIMESTAMP, $4::TEXT)
                ON CONFLICT (symbol) DO UPDATE SET
                status = EXCLUDED.status,
                attempts = EXCLUDED.attempts,
                last_attempt = EXCLUDED.last_attempt,
                error_message = EXCLUDED.error_message
            ''', symbol, status, datetime.now(), error_message)

            await conn.close()

        except Exception as e:
            logger.error(f"Failed to update progress for {symbol}: {str(e)}")

    def load_symbols_from_csv(self, csv_file_path: str) -> List[str]:
        """Load stock symbols from CSV file (supports glob patterns like *.csv)"""
        try:
            import glob as glob_module

            # Check if path contains wildcard
            if '*' in csv_file_path:
                # Handle glob pattern
                matching_files = glob_module.glob(csv_file_path)
                if matching_files:
                    # Use the most recent file
                    csv_file_path = max(matching_files, key=os.path.getmtime)
                    logger.info(f"Found CSV file: {csv_file_path}")
                else:
                    logger.error(f"No CSV file matching pattern: {csv_file_path}")
                    return []

            # Try different common CSV formats
            possible_paths = [
                csv_file_path,
                Path(__file__).parent.parent / csv_file_path,  # data directory
                Path(__file__).parent / csv_file_path,  # us directory
            ]

            symbols = []
            df = None

            for path in possible_paths:
                if Path(path).exists():
                    df = pd.read_csv(path)
                    logger.info(f"Loaded CSV from: {path}")
                    break

            if df is None:
                logger.error(f"CSV file not found: {csv_file_path}")
                return []


            # Try common column names for stock symbols
            symbol_columns = ['Symbol', 'symbol', 'SYMBOL', 'ACT Symbol', 'ticker', 'Ticker', 'TICKER']
            symbol_column = None

            for col in symbol_columns:
                if col in df.columns:
                    symbol_column = col
                    break

            if symbol_column is None:
                logger.error(f"No symbol column found in CSV. Available columns: {list(df.columns)}")
                return []


            # Find company name column for filtering Common Stock
            name_columns = ['Security Name', 'Company Name', 'security_name', 'company_name', 'Name', 'name']
            name_column = None

            for col in name_columns:
                if col in df.columns:
                    name_column = col
                    break

            if name_column:
                # Filter for entries containing "Common Stock"
                original_count = len(df)
                df = df[df[name_column].str.contains('Common Stock', case=False, na=False)]
            else:
                logger.warning("No company name column found for filtering Common Stock")

            # Extract symbols
            symbols = df[symbol_column].dropna().unique().tolist()
            symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]  # Clean symbols

            return symbols

        except Exception as e:
            logger.error(f"Failed to load symbols from CSV: {str(e)}")
            return []

    async def get_existing_symbols(self) -> set:
        """Get all symbols currently in the database"""
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true')
            await conn.close()
            return {row['symbol'] for row in rows}
        except Exception as e:
            logger.error(f"Failed to get existing symbols: {str(e)}")
            return set()

    async def mark_delisted_symbols(self, delisted_symbols: set):
        """Mark delisted symbols as inactive"""
        if not delisted_symbols:
            return

        try:
            conn = await self.get_connection()
            for symbol in delisted_symbols:
                await conn.execute(
                    'UPDATE us_stock_basic SET is_active = false, updated_at = $1 WHERE symbol = $2',
                    datetime.now(), symbol
                )
            await conn.close()
        except Exception as e:
            logger.error(f"Failed to mark delisted symbols: {str(e)}")

    async def analyze_symbol_changes(self, csv_symbols: set) -> tuple:
        """Analyze symbol changes between CSV and database"""
        db_symbols = await self.get_existing_symbols()

        new_symbols = csv_symbols - db_symbols      #  
        delisted = db_symbols - csv_symbols         # 
        existing = csv_symbols & db_symbols         #  


        if new_symbols:
            pass
        if delisted:
            pass

        return new_symbols, delisted, existing

    async def get_outdated_symbols(self, existing_symbols: set, days_threshold: int = 30) -> set:
        """Get existing symbols that need data refresh (older than threshold days)"""
        try:
            conn = await self.get_connection()
            threshold_date = datetime.now() - pd.Timedelta(days=days_threshold)

            rows = await conn.fetch('''
                SELECT symbol FROM us_stock_basic
                WHERE symbol = ANY($1::text[])
                AND is_active = true
                AND (updated_at IS NULL OR updated_at < $2)
            ''', list(existing_symbols), threshold_date)

            await conn.close()
            outdated = {row['symbol'] for row in rows}

            if outdated:
                pass

            return outdated

        except Exception as e:
            logger.error(f"Failed to get outdated symbols: {str(e)}")
            return set()

    async def get_symbols_from_us_symbol_table(self) -> List[str]:
        """Get all symbols from us_symbol table"""
        try:
            conn = await self.get_connection()

            query = """
                SELECT DISTINCT symbol
                FROM us_symbol
                WHERE symbol IS NOT NULL AND symbol != ''
                ORDER BY symbol
            """

            rows = await conn.fetch(query)
            symbols = [row['symbol'] for row in rows]

            await conn.close()

            logger.info(f"Loaded {len(symbols)} symbols from us_symbol table")
            return symbols

        except Exception as e:
            logger.error(f"Failed to get symbols from us_symbol table: {str(e)}")
            return []

    async def get_pending_symbols(self, csv_file_path: str = None) -> List[str]:
        """Get list of symbols that need to be processed"""
        try:
            # Load symbols from us_symbol table
            all_symbols = await self.get_symbols_from_us_symbol_table()

            if not all_symbols:
                logger.warning("No symbols found in us_symbol table")
                return []

            # Check which symbols haven't been completed
            conn = await self.get_connection()

            # Get completed symbols from us_collection_progress
            completed_symbols = await conn.fetch('SELECT symbol FROM us_collection_progress WHERE status = $1::VARCHAR(20)', 'completed')
            completed_set = {row['symbol'] for row in completed_symbols}

            await conn.close()

            # Return symbols that need processing
            pending_symbols = [symbol for symbol in all_symbols if symbol not in completed_set]

            logger.info(f"Found {len(pending_symbols)} pending symbols out of {len(all_symbols)} total")

            return pending_symbols

        except Exception as e:
            logger.error(f"Failed to get pending symbols: {str(e)}")
            return []

    async def collect_stock_data(self, symbols: List[str] = None, csv_file_path: str = None):
        """Main method to collect stock data"""
        if symbols is None:
            symbols = await self.get_pending_symbols(csv_file_path)

        if not symbols:
            return


        success_count = 0
        failed_count = 0

        for i, symbol in enumerate(symbols, 1):

            # Get data from API
            api_data = self.get_overview_data(symbol)

            if api_data:
                # Transform data
                transformed_data = self.transform_data(api_data)

                # Save to database
                if await self.save_to_database(transformed_data):
                    await self.update_progress(symbol, 'completed')
                    success_count += 1
                else:
                    await self.update_progress(symbol, 'failed', 'Database save failed')
                    failed_count += 1
            else:
                await self.update_progress(symbol, 'failed', 'API data retrieval failed')
                failed_count += 1

            # Rate limiting - 300 calls per minute (consistent with other collectors)
            await asyncio.sleep(0.2)  # 300 requests per minute = 0.2 seconds between requests


    async def get_collection_status(self):
        """Get current collection status"""
        try:
            conn = await self.get_connection()

            status_counts_rows = await conn.fetch('''
                SELECT status, COUNT(*) as count
                FROM us_collection_progress
                GROUP BY status
            ''')

            status_counts = {row['status']: row['count'] for row in status_counts_rows}

            total_records_row = await conn.fetchrow('SELECT COUNT(*) FROM us_stock_basic')
            total_records = total_records_row[0]

            await conn.close()


            return status_counts, total_records

        except Exception as e:
            logger.error(f"Failed to get collection status: {str(e)}")
            return {}, 0


async def main():
    # Initialize collector with API key and database URL from environment
    API_KEY = os.getenv('ALPHAVANTAGE_API_KEY', 'demo')
    DATABASE_URL = os.getenv('DATABASE_URL')

    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable is required")
        return

    collector = AlphaVantageCollector(API_KEY, DATABASE_URL)

    # Get current status
    await collector.get_collection_status()

    # Start collection from us_symbol table
    logger.info("Starting us_stock_basic collection from us_symbol table")
    await collector.collect_stock_data()

    # Final status
    await collector.get_collection_status()


class DailyCollector:
    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.6, target_date: date = None):
        self.api_key = api_key
        # asyncpg postgresql://  
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1  # seconds
        self.call_interval = call_interval
        self.start_date = target_date if target_date else self.get_latest_business_day()
        self.end_date = self.start_date

        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_daily_collected.json'
        else:
            log_file_path = 'log/us_daily_collected.json'

        self.collection_logger = CollectionLogger(log_file_path)
        self.pool = None
        self.session = None
        self.fundamental_cache = {}

    async def init_pool(self):
        """Initialize connection pool - OPTIMIZED"""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=10,  # 5 → 10 (optimized)
            max_size=50,  # 20 → 50 (optimized)
            command_timeout=120,  # 60 → 120 (optimized)
            max_queries=50000,
            max_cached_statement_lifetime=0,
            max_cacheable_statement_size=0
        )
        self.session = aiohttp.ClientSession()
        logger.info("[US_DAILY] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[US_DAILY] Database connection pool closed")

    async def get_connection(self):
        """Get PostgreSQL database connection"""
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)


    async def get_existing_symbols(self) -> List[str]:
        """Get all symbols from us_stock_basic table"""
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[US_DAILY] Found {len(symbols)} symbols")
            return symbols
        except Exception as e:
            logger.error(f"Failed to get existing symbols: {str(e)}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return []

    async def load_fundamental_data(self):
        """Load all fundamental data into cache - OPTIMIZED"""
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol, eps, bookvalue FROM us_stock_basic')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

            for row in rows:
                self.fundamental_cache[row['symbol']] = {
                    'eps': float(row['eps']) if row['eps'] is not None else None,
                    'bookvalue': float(row['bookvalue']) if row['bookvalue'] is not None else None
                }
            logger.info(f"[US_DAILY] Loaded fundamental data for {len(self.fundamental_cache)} symbols")
        except Exception as e:
            logger.error(f"[US_DAILY] Failed to load fundamental data: {e}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            else:
                try:
                    await conn.close()
                except:
                    pass

    async def get_stock_fundamental_data(self, symbol: str) -> Dict[str, Optional[float]]:
        """Get fundamental data from cache - OPTIMIZED"""
        return self.fundamental_cache.get(symbol, {'eps': None, 'bookvalue': None})

    async def get_daily_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch Daily data from Alpha Vantage API with async aiohttp - OPTIMIZED"""
        try:
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol,
                'apikey': self.api_key,
                'outputsize': 'compact',
                'datatype': 'json'
            }
            async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'Error Message' in data:
                        logger.error(f"[US_DAILY] API error for {symbol}: {data['Error Message']}")
                        return None
                    elif 'Note' in data:
                        logger.warning(f"[US_DAILY] API limit reached: {data['Note']}")
                        return None
                    elif 'Time Series (Daily)' in data:
                        return data
                    else:
                        logger.warning(f"[US_DAILY] Unexpected response format for {symbol}")
                        return None
                else:
                    logger.error(f"[US_DAILY] API request failed for {symbol}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"[US_DAILY] Error fetching data for {symbol}: {e}")
            return None

    def get_latest_business_day(self) -> date:
        """Get the latest business day (Monday-Friday)"""
        today = date.today()

        # Go back to find the latest business day
        days_back = 0
        while True:
            check_date = today - timedelta(days=days_back)
            # Monday=0, Sunday=6, so Monday-Friday = 0-4
            if check_date.weekday() < 5:  # Monday to Friday
                return check_date
            days_back += 1
            # Safety check to prevent infinite loop
            if days_back > 10:
                return today

    def get_date_range_list(self) -> List[str]:
        """Get list of dates in collection range"""
        dates = []
        current = self.start_date
        while current <= self.end_date:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        return dates

    def transform_daily_data(self, api_data: Dict[str, Any], symbol: str, eps: Optional[float] = None, bookvalue: Optional[float] = None) -> List[Dict[str, Any]]:
        """Transform Daily data to match database schema - OPTIMIZED"""
        try:
            transformed_records = []

            # Get time series data
            time_series = api_data.get("Time Series (Daily)", {})

            for date_str, ohlcv_data in time_series.items():
                try:
                    # Parse date
                    parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()

                    # Filter: only include data within date range
                    if not (self.start_date <= parsed_date <= self.end_date):
                        continue

                    # Parse OHLCV data
                    open_price = self.safe_decimal(ohlcv_data.get("1. open"))
                    high_price = self.safe_decimal(ohlcv_data.get("2. high"))
                    low_price = self.safe_decimal(ohlcv_data.get("3. low"))
                    close_price = self.safe_decimal(ohlcv_data.get("4. close"))
                    volume = self.safe_int(ohlcv_data.get("5. volume"))

                    # Calculate change amount and rate (compared to previous close, if available)
                    change_amount = None
                    change_rate = None
                    if open_price is not None and close_price is not None:
                        change_amount = close_price - open_price
                        if open_price != 0:
                            change_rate = (change_amount / open_price) * 100

                    # Calculate PER (Price-to-Earnings Ratio)
                    per = None
                    if close_price is not None and eps is not None and eps > 0:
                        per = close_price / eps
                        # Limit PER to database precision (DECIMAL(6,2) = max 9999.99)
                        if per > 9999.99:
                            per = 9999.99
                            logger.warning(f"PER capped at 9999.99 for {symbol}: calculated {close_price}/{eps}")
                    else:
                        pass

                    # Calculate PBR (Price-to-Book Ratio)
                    pbr = None
                    if close_price is not None and bookvalue is not None and bookvalue > 0:
                        pbr = close_price / bookvalue
                        # Limit PBR to reasonable range
                        if pbr > 999999.99:
                            pbr = 999999.99
                            logger.warning(f"PBR capped at 999999.99 for {symbol}: calculated {close_price}/{bookvalue}")
                    else:
                        pass

                    transformed = {
                        'date': parsed_date,
                        'symbol': symbol,
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': close_price,
                        'volume': volume,
                        'change_amount': change_amount,
                        'change_rate': change_rate,
                        'per': per,
                        'pbr': pbr,
                        'updated_at': datetime.now()
                    }

                    transformed_records.append(transformed)

                except Exception as e:
                    logger.error(f"Error transforming record for {symbol}: {str(e)}, Date: {date_str}")
                    continue

            if transformed_records:
                record = transformed_records[0]

            return transformed_records

        except Exception as e:
            logger.error(f"Error transforming Daily data for {symbol}: {str(e)}")
            return []

    def safe_decimal(self, value: str) -> Optional[float]:
        """Safely convert string to decimal"""
        if not value or value.strip() in ['', 'N/A', '-', 'null']:
            return None

        try:
            return float(value.strip())
        except ValueError:
            logger.warning(f"Could not convert to decimal: {value}")
            return None

    def safe_int(self, value: str) -> Optional[int]:
        """Safely convert string to integer"""
        if not value or value.strip() in ['', 'N/A', '-', 'null']:
            return None

        try:
            return int(float(value.strip()))  # Convert to float first to handle decimal strings
        except ValueError:
            logger.warning(f"Could not convert to integer: {value}")
            return None

    async def save_daily_data(self, daily_data: List[Dict[str, Any]]) -> int:
        """Save Daily data to database"""
        if not daily_data:
            return 0

        try:
            conn = await self.get_connection()
            saved_count = 0
            updated_count = 0

            for record in daily_data:
                try:
                    # Insert or update Daily record
                    result = await conn.execute('''
                        INSERT INTO us_daily
                        (date, symbol, open, high, low, close, volume, change_amount,
                         change_rate, per, pbr, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        ON CONFLICT (symbol, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        change_amount = EXCLUDED.change_amount,
                        change_rate = EXCLUDED.change_rate,
                        per = EXCLUDED.per,
                        pbr = EXCLUDED.pbr,
                        updated_at = EXCLUDED.updated_at
                        RETURNING (xmax = 0) AS inserted
                    ''',
                    record['date'],
                    record['symbol'],
                    record['open'],
                    record['high'],
                    record['low'],
                    record['close'],
                    record['volume'],
                    record['change_amount'],
                    record['change_rate'],
                    record['per'],
                    record['pbr'],
                    record['updated_at']
                    )

                    # Check if it was an insert or update
                    if result == "INSERT 0 1":
                        saved_count += 1
                    else:
                        updated_count += 1


                except Exception as e:
                    logger.error(f"Failed to save Daily record for {record.get('symbol', 'unknown')}: {str(e)}")

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return saved_count + updated_count

        except Exception as e:
            logger.error(f"Database error while saving Daily data: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            else:
                try:
                    await conn.close()
                except:
                    pass
            return 0

    async def save_daily_data_optimized(self, daily_data: List[Dict[str, Any]]) -> int:
        """Save Daily data using COPY command - OPTIMIZED with transaction settings and fallback"""
        if not daily_data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                # Transaction optimization - OPTIMIZATION #8
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                # Create temp table for COPY - OPTIMIZATION #4
                await conn.execute('''
                    CREATE TEMP TABLE temp_us_daily (
                        date DATE,
                        symbol VARCHAR(20),
                        open DECIMAL(12,4),
                        high DECIMAL(12,4),
                        low DECIMAL(12,4),
                        close DECIMAL(12,4),
                        volume BIGINT,
                        change_amount DECIMAL(12,4),
                        change_rate DECIMAL(8,4),
                        per DECIMAL(6,2),
                        pbr DECIMAL(8,2),
                        updated_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')

                # Prepare data for COPY
                rows = [[
                    record['date'], record['symbol'], record['open'],
                    record['high'], record['low'], record['close'],
                    record['volume'], record['change_amount'], record['change_rate'],
                    record['per'], record['pbr'], record['updated_at']
                ] for record in daily_data]

                # COPY to temp table
                await conn.copy_records_to_table(
                    'temp_us_daily', records=rows,
                    columns=['date', 'symbol', 'open', 'high', 'low', 'close',
                            'volume', 'change_amount', 'change_rate', 'per', 'pbr', 'updated_at']
                )

                # Upsert from temp to main table
                await conn.execute('''
                    INSERT INTO us_daily (date, symbol, open, high, low, close, volume, change_amount, change_rate, per, pbr, updated_at)
                    SELECT * FROM temp_us_daily
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                        close = EXCLUDED.close, volume = EXCLUDED.volume,
                        change_amount = EXCLUDED.change_amount, change_rate = EXCLUDED.change_rate,
                        per = EXCLUDED.per, pbr = EXCLUDED.pbr, updated_at = EXCLUDED.updated_at
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(daily_data)
        except Exception as e:
            logger.error(f"[US_DAILY] Error in save_daily_data_optimized: {e}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            else:
                try:
                    await conn.close()
                except:
                    pass
            # Fallback to regular save - OPTIMIZATION #9
            logger.info("[US_DAILY] Falling back to regular save")
            return await self.save_daily_data(daily_data)

    async def calculate_us_daily_ma(self, symbols: List[str] = None):
        """us_daily moving average calculation and update"""
        conn = await self.get_connection()

        try:
            # Get symbols to calculate
            if symbols:
                symbol_list = symbols
            else:
                rows = await conn.fetch("SELECT DISTINCT symbol FROM us_daily")
                symbol_list = [row['symbol'] for row in rows]

            logger.info(f"[US_DAILY MA] Calculating MA for {len(symbol_list)} symbols")

            updated_count = 0
            error_count = 0

            for symbol in symbol_list:
                try:
                    # Get historical data (last 200 days)
                    history_query = """
                    SELECT date, volume, close
                    FROM us_daily
                    WHERE symbol = $1
                    ORDER BY date DESC
                    LIMIT 200
                    """
                    history = await conn.fetch(history_query, symbol)

                    if not history:
                        continue

                    # Build data lists
                    volumes = []
                    trading_values = []

                    for h in history:
                        if h['volume'] is not None:
                            volumes.append(h['volume'])
                        if h['volume'] is not None and h['close'] is not None:
                            trading_values.append(int(h['volume'] * float(h['close'])))

                    # Calculate MAs
                    def calc_ma(data_list, period):
                        if len(data_list) == 0:
                            return None
                        actual_period = min(period, len(data_list))
                        return int(sum(data_list[:actual_period]) / actual_period)

                    avg_vol_5d = calc_ma(volumes, 5)
                    avg_vol_20d = calc_ma(volumes, 20)
                    avg_vol_50d = calc_ma(volumes, 50)
                    avg_vol_200d = calc_ma(volumes, 200)

                    avg_tv_5d = calc_ma(trading_values, 5)
                    avg_tv_20d = calc_ma(trading_values, 20)

                    # Get the latest date for this symbol
                    latest_date = history[0]['date']

                    # Update us_daily for the latest record
                    update_query = """
                    UPDATE us_daily
                    SET avg_volume_5d = $1,
                        avg_volume_20d = $2,
                        avg_volume_50d = $3,
                        avg_volume_200d = $4,
                        avg_trading_value_5d = $5,
                        avg_trading_value_20d = $6
                    WHERE symbol = $7 AND date = $8
                    """
                    await conn.execute(
                        update_query,
                        avg_vol_5d, avg_vol_20d, avg_vol_50d, avg_vol_200d,
                        avg_tv_5d, avg_tv_20d,
                        symbol, latest_date
                    )
                    updated_count += 1

                except Exception as e:
                    error_count += 1
                    continue

            logger.info(f"[US_DAILY MA] Completed: {updated_count} updated, {error_count} errors")

        except Exception as e:
            logger.error(f"[US_DAILY MA] Error: {e}")
        finally:
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern - OPTIMIZATION #3"""
        for i, symbol in enumerate(symbols, 1):
            try:
                date_range_list = self.get_date_range_list()
                # Check CollectionLogger - OPTIMIZATION #6
                if self.collection_logger.is_collected('us_daily', symbol, date_range_list):
                    continue

                fundamental_data = await self.get_stock_fundamental_data(symbol)
                eps, bookvalue = fundamental_data.get('eps'), fundamental_data.get('bookvalue')

                api_data = await self.get_daily_data(symbol)
                if api_data:
                    transformed = self.transform_daily_data(api_data, symbol, eps, bookvalue)
                    if transformed:
                        await data_queue.put((symbol, transformed, date_range_list))
                        logger.info(f"[US_DAILY API] [{i}/{len(symbols)}] Queued {symbol}")

                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[US_DAILY API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 100):
        """DB worker for pipeline pattern - OPTIMIZATION #3"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_daily_data_optimized(batch)
                    total_saved += saved
                break
            symbol, data, date_range_list = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_daily_data_optimized(batch)
                if saved > 0:
                    total_saved += saved
                    for record in batch:
                        self.collection_logger.mark_collected('us_daily', record['symbol'], date_range_list, 1)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[US_DAILY DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern, COPY optimization, and all 9 optimizations"""
        logger.info(f"[US_DAILY OPTIMIZED] Starting for {self.start_date}")
        logger.info(f"[US_DAILY OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")

        await self.init_pool()  # OPTIMIZATION #1
        await self.load_fundamental_data()  # OPTIMIZATION #7

        all_symbols = await self.get_existing_symbols()
        if not all_symbols:
            logger.error("[US_DAILY] No symbols found")
            await self.close_pool()
            return

        date_range_list = self.get_date_range_list()
        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_daily', s, date_range_list)]
        logger.info(f"[US_DAILY] Total: {len(all_symbols)}, To process: {len(symbols)}")

        if not symbols:
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=100))
        await api_task
        total_saved = await db_task

        # Calculate moving averages for collected symbols
        if total_saved > 0:
            await self.calculate_us_daily_ma(symbols)

        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[US_DAILY OPTIMIZED] Completed in {duration}, saved {total_saved} records")

    async def collect_daily(self):
        """Main method to collect Daily data for all symbols"""


        # Get all symbols from us_stock_basic table
        symbols = await self.get_existing_symbols()

        if not symbols:
            logger.warning("No symbols found in us_stock_basic table")
            return

        total_processed = 0
        total_saved = 0

        for i, symbol in enumerate(symbols, 1):
            try:

                # Fetch Daily data for this symbol
                api_data = self.get_daily_data(symbol)

                if api_data:
                    # Transform data
                    transformed_data = await self.transform_daily_data(api_data, symbol)

                    if transformed_data:
                        # Save to database
                        saved_count = await self.save_daily_data(transformed_data)
                        total_saved += saved_count
                    else:
                        logger.warning(f"No Daily data to save for {symbol}")
                else:
                    logger.warning(f"No Daily data retrieved for {symbol}")

                total_processed += 1

                # Add small delay between requests to respect API limits
                if i < len(symbols):  # Don't sleep after the last symbol
                    await asyncio.sleep(0.6)  # 100 requests per minute = ~0.6 seconds between requests

            except Exception as e:
                logger.error(f"Error processing symbol {symbol}: {str(e)}")
                continue


    async def get_collection_status(self):
        """Get current Daily collection status"""
        try:
            conn = await self.get_connection()

            total_records_row = await conn.fetchrow('SELECT COUNT(*) FROM us_daily')
            total_records = total_records_row[0]

            # Get unique symbols count
            symbols_row = await conn.fetchrow('SELECT COUNT(DISTINCT symbol) FROM us_daily')
            symbols_count = symbols_row[0]

            # Get latest date
            latest_row = await conn.fetchrow('''
                SELECT date, symbol FROM us_daily
                ORDER BY date DESC LIMIT 1
            ''')

            # Get earliest date
            earliest_row = await conn.fetchrow('''
                SELECT date, symbol FROM us_daily
                ORDER BY date ASC LIMIT 1
            ''')

            await conn.close()

            if latest_row:
                pass
            if earliest_row:
                pass

            return total_records

        except Exception as e:
            logger.error(f"Failed to get collection status: {str(e)}")
            return 0


class MonthlyCollector:
    def __init__(self, api_key: str, database_url: str):
        self.api_key = api_key
        # asyncpg postgresql://  
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1  # seconds
        self.session = None  # aiohttp session

    async def init_session(self):
        """Initialize aiohttp session"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    async def get_connection(self):
        """Get PostgreSQL database connection"""
        return await asyncpg.connect(self.database_url)


    async def get_existing_symbols(self) -> set:
        """Get all symbols from us_stock_basic table"""
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true')
            await conn.close()
            return {row['symbol'] for row in rows}
        except Exception as e:
            logger.error(f"Failed to get existing symbols: {str(e)}")
            return set()

    async def get_monthly_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch Monthly data from Alpha Vantage API with retry logic"""
        await self.init_session()

        params = {
            'function': 'TIME_SERIES_MONTHLY',
            'symbol': symbol,
            'datatype': 'json',
            'apikey': self.api_key
        }

        for attempt in range(self.retry_count):
            try:
                async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Check if API returned an error
                        if "Error Message" in data:
                            logger.error(f"API Error for {symbol}: {data['Error Message']}")
                            return None

                        # Check if we hit rate limit
                        if "Information" in data and "rate limit" in data["Information"].lower():
                            logger.warning("Rate limit hit, waiting...")
                            await asyncio.sleep(60)
                            continue

                        # Validate JSON structure
                        if not data or "Monthly Time Series" not in data:
                            logger.warning(f"No Monthly data found for {symbol} in API response")
                            return None

                        time_series_data = data.get("Monthly Time Series", {})

                        return data
                    else:
                        logger.error(f"Request failed for {symbol} (attempt {attempt + 1}): Status {response.status}")
                        if attempt < self.retry_count - 1:
                            await asyncio.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                        else:
                            logger.error(f"All retry attempts failed for {symbol} Monthly data")
                            return None

            except asyncio.TimeoutError:
                logger.error(f"Timeout for {symbol} (attempt {attempt + 1})")
                if attempt < self.retry_count - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    logger.error(f"All retry attempts failed for {symbol} Monthly data")
                    return None
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for {symbol}: {str(e)}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error for {symbol}: {str(e)}")
                return None

        return None

    def transform_monthly_data(self, api_data: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
        """Transform Monthly data to match database schema"""
        try:
            transformed_records = []

            # Get time series data
            time_series = api_data.get("Monthly Time Series", {})

            # Filter for data from 2024 onwards
            start_date = date(2024, 1, 1)

            for date_str, ohlcv_data in time_series.items():
                try:
                    # Parse date
                    parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()

                    # Filter: only include data from 2024 onwards
                    if parsed_date < start_date:
                        continue

                    # Parse OHLCV data
                    open_price = self.safe_decimal(ohlcv_data.get("1. open"))
                    high_price = self.safe_decimal(ohlcv_data.get("2. high"))
                    low_price = self.safe_decimal(ohlcv_data.get("3. low"))
                    close_price = self.safe_decimal(ohlcv_data.get("4. close"))
                    volume = self.safe_int(ohlcv_data.get("5. volume"))

                    transformed = {
                        'date': parsed_date,
                        'symbol': symbol,
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': close_price,
                        'volume': volume,
                        'updated_at': datetime.now()
                    }

                    transformed_records.append(transformed)

                except Exception as e:
                    logger.error(f"Error transforming record for {symbol}: {str(e)}, Date: {date_str}")
                    continue

            if transformed_records:
                # Show earliest and latest dates
                dates = [record['date'] for record in transformed_records]
                earliest = min(dates)
                latest = max(dates)

            return transformed_records

        except Exception as e:
            logger.error(f"Error transforming Monthly data for {symbol}: {str(e)}")
            return []

    def safe_decimal(self, value: str) -> Optional[float]:
        """Safely convert string to decimal"""
        if not value or value.strip() in ['', 'N/A', '-', 'null']:
            return None

        try:
            return float(value.strip())
        except ValueError:
            logger.warning(f"Could not convert to decimal: {value}")
            return None

    def safe_int(self, value: str) -> Optional[int]:
        """Safely convert string to integer"""
        if not value or value.strip() in ['', 'N/A', '-', 'null']:
            return None

        try:
            return int(float(value.strip()))  # Convert to float first to handle decimal strings
        except ValueError:
            logger.warning(f"Could not convert to integer: {value}")
            return None

    async def save_monthly_data(self, monthly_data: List[Dict[str, Any]]) -> int:
        """Save Monthly data to database"""
        if not monthly_data:
            return 0

        try:
            conn = await self.get_connection()
            saved_count = 0

            for record in monthly_data:
                try:
                    # Insert or update Monthly record
                    result = await conn.execute('''
                        INSERT INTO us_monthly
                        (date, symbol, open, high, low, close, volume, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (symbol, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        updated_at = EXCLUDED.updated_at
                        RETURNING (xmax = 0) AS inserted
                    ''',
                    record['date'],
                    record['symbol'],
                    record['open'],
                    record['high'],
                    record['low'],
                    record['close'],
                    record['volume'],
                    record['updated_at']
                    )

                    # Count as saved (we can't distinguish insert vs update easily with partitioned tables)
                    saved_count += 1


                except Exception as e:
                    logger.error(f"Failed to save Monthly record for {record.get('symbol', 'unknown')}: {str(e)}")

            await conn.close()
            return saved_count

        except Exception as e:
            logger.error(f"Database error while saving Monthly data: {str(e)}")
            return 0

    async def collect_monthly(self):
        """Main method to collect Monthly data for all symbols"""


        # Get all symbols from us_stock_basic table
        symbols = await self.get_existing_symbols()

        if not symbols:
            logger.warning("No symbols found in us_stock_basic table")
            return

        total_processed = 0
        total_saved = 0

        for i, symbol in enumerate(symbols, 1):
            try:

                # Fetch Monthly data for this symbol
                api_data = await self.get_monthly_data(symbol)

                if api_data:
                    # Transform data
                    transformed_data = self.transform_monthly_data(api_data, symbol)

                    if transformed_data:
                        # Save to database
                        saved_count = await self.save_monthly_data(transformed_data)
                        total_saved += saved_count
                    else:
                        logger.warning(f"No Monthly data to save for {symbol}")
                else:
                    logger.warning(f"No Monthly data retrieved for {symbol}")

                total_processed += 1

                # Add small delay between requests to respect API limits
                if i < len(symbols):  # Don't sleep after the last symbol
                    await asyncio.sleep(0.6)  # 100 requests per minute = ~0.6 seconds between requests

            except Exception as e:
                logger.error(f"Error processing symbol {symbol}: {str(e)}")
                continue


    async def get_collection_status(self):
        """Get current Monthly collection status"""
        try:
            conn = await self.get_connection()

            total_records_row = await conn.fetchrow('SELECT COUNT(*) FROM us_monthly')
            total_records = total_records_row[0]

            # Get unique symbols count
            symbols_row = await conn.fetchrow('SELECT COUNT(DISTINCT symbol) FROM us_monthly')
            symbols_count = symbols_row[0]

            # Get latest date
            latest_row = await conn.fetchrow('''
                SELECT date, symbol FROM us_monthly
                ORDER BY date DESC LIMIT 1
            ''')

            # Get earliest date
            earliest_row = await conn.fetchrow('''
                SELECT date, symbol FROM us_monthly
                ORDER BY date ASC LIMIT 1
            ''')

            await conn.close()

            if latest_row:
                pass
            if earliest_row:
                pass

            return total_records

        except Exception as e:
            logger.error(f"Failed to get collection status: {str(e)}")
            return 0


class VWAPCollector:
    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2, target_date: date = None):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.start_date = target_date if target_date else self.get_latest_business_day()
        self.end_date = self.start_date
        self.call_interval = call_interval

        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_vwap_collected.json'
        else:
            log_file_path = 'log/us_vwap_collected.json'

        self.collection_logger = CollectionLogger(log_file_path)
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1  # seconds
        self.pool = None
        self.session = None

    def get_latest_business_day(self) -> date:
        """Get the latest business day (excluding weekends)"""
        today = date.today()
        # Check up to 3 days back to find a weekday
        for days_back in range(4):
            check_date = today - timedelta(days=days_back)
            if check_date.weekday() < 5:  # Monday to Friday
                return check_date
        # Fallback: return today
        return today

    def get_date_range_list(self) -> List[str]:
        """Get list of dates in collection range"""
        dates = []
        current = self.start_date
        while current <= self.end_date:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        return dates

    async def init_pool(self):
        """Initialize connection pool - OPTIMIZED"""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=10,
            max_size=50,
            command_timeout=120,
            max_queries=50000,
            max_cached_statement_lifetime=0,
            max_cacheable_statement_size=0
        )
        self.session = aiohttp.ClientSession()
        logger.info("[US_VWAP] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[US_VWAP] Database connection pool closed")

    async def get_connection(self):
        """Get PostgreSQL database connection"""
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_vwap_data(self, symbol: str) -> Optional[Dict]:
        """Fetch VWAP data from Alpha Vantage API with retry logic"""
        params = {
            'function': 'VWAP',
            'symbol': symbol,
            'interval': '60min',  # VWAP needs intraday interval
            'apikey': self.api_key,
            'datatype': 'json'
        }

        for attempt in range(self.retry_count):
            try:
                async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if 'Error Message' in data:
                            logger.error(f"[US_VWAP] API error for {symbol}: {data['Error Message']}")
                            return None
                        elif 'Note' in data:
                            logger.warning(f"[US_VWAP] API limit reached, waiting 60s...")
                            await asyncio.sleep(60)
                            continue
                        elif 'Information' in data and 'rate limit' in data['Information'].lower():
                            logger.warning(f"[US_VWAP] Rate limit hit, waiting 60s...")
                            await asyncio.sleep(60)
                            continue
                        elif 'Technical Analysis: VWAP' in data:
                            return data
                        else:
                            logger.warning(f"[US_VWAP] Unexpected response format for {symbol}")
                            return None
                    else:
                        logger.error(f"[US_VWAP] API request failed for {symbol}: Status {response.status} (attempt {attempt + 1}/{self.retry_count})")
                        if attempt < self.retry_count - 1:
                            await asyncio.sleep(self.retry_delay * (attempt + 1))
                        continue
            except aiohttp.ClientError as e:
                logger.error(f"[US_VWAP] Request failed for {symbol} (attempt {attempt + 1}/{self.retry_count}): {e}")
                if attempt < self.retry_count - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    logger.error(f"[US_VWAP] All retry attempts failed for {symbol}")
                    return None
            except json.JSONDecodeError as e:
                logger.error(f"[US_VWAP] JSON decode error for {symbol}: {e}")
                return None
            except Exception as e:
                logger.error(f"[US_VWAP] Unexpected error for {symbol}: {e}")
                return None

        return None

    def safe_decimal(self, value: str) -> Optional[float]:
        """Safely convert string to decimal"""
        if not value or value.strip() in ['', 'N/A', '-', 'null']:
            return None
        try:
            return float(value.strip())
        except ValueError:
            logger.warning(f"[US_VWAP] Could not convert to decimal: {value}")
            return None

    def transform_vwap_data(self, data: Dict, symbol: str) -> List[Dict]:
        """Transform VWAP data to match database schema - OPTIMIZED"""
        try:
            transformed = []

            # Get metadata
            meta_data = data.get("Meta Data", {})
            indicator = meta_data.get("2: Indicator", "Volume Weighted Average Price (VWAP)")
            last_refreshed = meta_data.get("3: Last Refreshed", "")
            interval = meta_data.get("4: Interval", "60min")
            time_zone = meta_data.get("5: Time Zone", "US/Eastern")

            # Parse last_refreshed date
            parsed_last_refreshed = None
            if last_refreshed:
                try:
                    if " " in last_refreshed:
                        parsed_last_refreshed = datetime.strptime(last_refreshed, "%Y-%m-%d %H:%M:%S").date()
                    else:
                        parsed_last_refreshed = datetime.strptime(last_refreshed, "%Y-%m-%d").date()
                except ValueError:
                    logger.warning(f"[US_VWAP] Could not parse last_refreshed date: {last_refreshed}")

            # Get VWAP time series data
            vwap_data = data.get("Technical Analysis: VWAP", {})

            for datetime_str, vwap_info in vwap_data.items():
                try:
                    # Parse datetime (format: "2025-09-05 19:45" or "2025-09-05")
                    if " " in datetime_str:
                        parsed_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
                        parsed_date = parsed_datetime.date()
                    else:
                        parsed_datetime = datetime.strptime(datetime_str, "%Y-%m-%d")
                        parsed_date = parsed_datetime.date()

                    # Filter by date range
                    if not (self.start_date <= parsed_date <= self.end_date):
                        continue

                    # Parse VWAP value
                    vwap_value = self.safe_decimal(vwap_info.get("VWAP"))

                    transformed.append({
                        'symbol': symbol,
                        'indicator': indicator,
                        'last_refreshed': parsed_last_refreshed,
                        'interval': interval,
                        'time_zone': time_zone,
                        'date': parsed_date,
                        'datetime': parsed_datetime,
                        'vwap': vwap_value,
                        'created_at': datetime.now()
                    })

                except Exception as e:
                    logger.error(f"[US_VWAP] Error transforming record for {symbol}: {e}, DateTime: {datetime_str}")
                    continue

            return transformed

        except Exception as e:
            logger.error(f"[US_VWAP] Error transforming VWAP data for {symbol}: {e}")
            return []

    async def save_vwap_data(self, data: List[Dict]) -> int:
        """Save VWAP data to database using bulk insert - OPTIMIZED"""
        if not data:
            return 0

        conn = await self.get_connection()
        try:
            # Use executemany for better performance
            records_to_insert = []
            for record in data:
                records_to_insert.append((
                    record['symbol'],
                    record['indicator'],
                    record['last_refreshed'],
                    record['interval'],
                    record['time_zone'],
                    record['date'],
                    record['datetime'],
                    record['vwap'],
                    record['created_at']
                ))

            # Use a single transaction for all inserts
            async with conn.transaction():
                query = """
                    INSERT INTO us_vwap_base
                    (symbol, indicator, last_refreshed, interval, time_zone, date, datetime, vwap, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (symbol, datetime) DO UPDATE SET
                        indicator = EXCLUDED.indicator,
                        last_refreshed = EXCLUDED.last_refreshed,
                        interval = EXCLUDED.interval,
                        time_zone = EXCLUDED.time_zone,
                        date = EXCLUDED.date,
                        vwap = EXCLUDED.vwap,
                        created_at = EXCLUDED.created_at
                """
                await conn.executemany(query, records_to_insert)

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

            return len(data)

        except Exception as e:
            logger.error(f"[US_VWAP] Error in save_vwap_data: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

            # Fallback: try individual inserts
            logger.info(f"[US_VWAP] Falling back to individual inserts for {len(data)} records")
            return await self.save_vwap_data_fallback(data)

    async def save_vwap_data_fallback(self, data: List[Dict]) -> int:
        """Fallback method: save each record with a fresh connection"""
        saved_count = 0

        for record in data:
            conn = None
            try:
                conn = await self.get_connection()

                await conn.execute('''
                    INSERT INTO us_vwap_base
                    (symbol, indicator, last_refreshed, interval, time_zone, date, datetime, vwap, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (symbol, datetime) DO UPDATE SET
                        indicator = EXCLUDED.indicator,
                        last_refreshed = EXCLUDED.last_refreshed,
                        interval = EXCLUDED.interval,
                        time_zone = EXCLUDED.time_zone,
                        date = EXCLUDED.date,
                        vwap = EXCLUDED.vwap,
                        created_at = EXCLUDED.created_at
                ''',
                record['symbol'],
                record['indicator'],
                record['last_refreshed'],
                record['interval'],
                record['time_zone'],
                record['date'],
                record['datetime'],
                record['vwap'],
                record['created_at']
                )

                if self.pool:
                    await self.pool.release(conn)
                else:
                    await conn.close()
                saved_count += 1

            except Exception as e:
                logger.error(f"[US_VWAP] Failed to save record for {record.get('symbol', 'unknown')}: {e}")
                if conn:
                    try:
                        if self.pool:
                            await self.pool.release(conn)
                        else:
                            await conn.close()
                    except:
                        pass

        return saved_count

    async def save_vwap_data_optimized(self, data: List[Dict]) -> int:
        """Save VWAP data using COPY command - OPTIMIZED"""
        if not data:
            return 0
        conn = await self.get_connection()
        try:
            async with conn.transaction():
                # Transaction optimization
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                # Create temp table
                await conn.execute('''
                    CREATE TEMP TABLE temp_us_vwap (
                        symbol VARCHAR(20),
                        indicator VARCHAR(100),
                        last_refreshed DATE,
                        interval VARCHAR(20),
                        time_zone VARCHAR(50),
                        date DATE,
                        datetime TIMESTAMP,
                        vwap DECIMAL(12,4),
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')

                # Prepare data for COPY
                rows = [[
                    record['symbol'], record['indicator'], record['last_refreshed'],
                    record['interval'], record['time_zone'], record['date'],
                    record['datetime'], record['vwap'], record['created_at']
                ] for record in data]

                # COPY to temp table
                await conn.copy_records_to_table(
                    'temp_us_vwap', records=rows,
                    columns=['symbol', 'indicator', 'last_refreshed', 'interval',
                            'time_zone', 'date', 'datetime', 'vwap', 'created_at']
                )

                # Upsert from temp to main
                await conn.execute('''
                    INSERT INTO us_vwap_base (symbol, indicator, last_refreshed, interval, time_zone, date, datetime, vwap, created_at)
                    SELECT * FROM temp_us_vwap
                    ON CONFLICT (symbol, datetime) DO UPDATE SET
                        indicator = EXCLUDED.indicator, last_refreshed = EXCLUDED.last_refreshed,
                        interval = EXCLUDED.interval, time_zone = EXCLUDED.time_zone,
                        date = EXCLUDED.date, vwap = EXCLUDED.vwap, created_at = EXCLUDED.created_at
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(data)
        except Exception as e:
            logger.error(f"[US_VWAP] Error in save_vwap_data_optimized: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            logger.info("[US_VWAP] Falling back to regular save")
            return await self.save_vwap_data(data)

    async def get_active_symbols(self) -> List[str]:
        """Get list of active symbols from database"""
        conn = await self.get_connection()
        try:
            query = """
                SELECT DISTINCT symbol
                FROM us_stock_basic
                WHERE symbol IS NOT NULL
                ORDER BY symbol
            """
            rows = await conn.fetch(query)
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[US_VWAP] Found {len(symbols)} symbols")
            return symbols
        except Exception as e:
            logger.error(f"[US_VWAP] Error getting active symbols: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return []

    async def collect_symbol_data(self, symbol: str) -> int:
        """Collect data for a single symbol"""
        try:
            date_range_list = self.get_date_range_list()
            if self.collection_logger.is_collected('us_vwap_base', symbol, date_range_list):
                logger.debug(f"[US_VWAP] Skip {symbol} - already in log")
                return 0
            api_data = await self.get_vwap_data(symbol)
            if api_data:
                transformed = self.transform_vwap_data(api_data, symbol)
                if transformed:
                    saved = await self.save_vwap_data(transformed)
                    if saved > 0:
                        self.collection_logger.mark_collected('us_vwap_base', symbol, date_range_list, saved)
                        self.collection_logger.save_log()
                        logger.info(f"[US_VWAP] Saved {saved} records for {symbol}")
                    return saved
                else:
                    return 0
            else:
                return 0
        except Exception as e:
            logger.error(f"[US_VWAP] Error collecting data for {symbol}: {e}")
            return 0

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern - OPTIMIZED"""
        for i, symbol in enumerate(symbols, 1):
            try:
                date_range_list = self.get_date_range_list()
                if self.collection_logger.is_collected('us_vwap_base', symbol, date_range_list):
                    continue
                api_data = await self.get_vwap_data(symbol)
                if api_data:
                    transformed = self.transform_vwap_data(api_data, symbol)
                    if transformed:
                        await data_queue.put((symbol, transformed, date_range_list))
                        logger.info(f"[US_VWAP API] [{i}/{len(symbols)}] Queued {symbol}")
                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[US_VWAP API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 100):
        """DB worker for pipeline pattern - OPTIMIZED"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_vwap_data_optimized(batch)
                    total_saved += saved
                break
            symbol, data, date_range_list = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_vwap_data_optimized(batch)
                if saved > 0:
                    total_saved += saved
                    for record in batch:
                        self.collection_logger.mark_collected('us_vwap_base', record['symbol'], date_range_list, 1)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[US_VWAP DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern"""
        logger.info(f"[US_VWAP OPTIMIZED] Starting for {self.start_date}")
        logger.info(f"[US_VWAP OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")
        await self.init_pool()
        all_symbols = await self.get_active_symbols()
        if not all_symbols:
            logger.error("[US_VWAP] No symbols found")
            await self.close_pool()
            return
        date_range_list = self.get_date_range_list()
        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_vwap_base', s, date_range_list)]
        logger.info(f"[US_VWAP] Total: {len(all_symbols)}, To process: {len(symbols)}")
        if not symbols:
            await self.close_pool()
            return
        start_time = datetime.now()
        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=100))
        await api_task
        total_saved = await db_task
        await self.close_pool()
        duration = datetime.now() - start_time
        logger.info(f"[US_VWAP OPTIMIZED] Completed in {duration}, saved {total_saved} records")

    async def collect_vwap(self):
        """Main method to collect VWAP data for all symbols"""
        logger.info(f"[US_VWAP] Starting collection for {self.start_date}")
        logger.info(f"[US_VWAP] API rate: {60/self.call_interval:.0f} calls/min")
        await self.init_pool()
        all_symbols = await self.get_active_symbols()
        if not all_symbols:
            logger.error("[US_VWAP] No symbols found")
            await self.close_pool()
            return
        date_range_list = self.get_date_range_list()
        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_vwap_base', s, date_range_list)]
        total_symbols = len(symbols)
        total_saved = 0
        start_time = datetime.now()
        logger.info(f"[US_VWAP] Total: {len(all_symbols)}, To process: {total_symbols}")
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"[US_VWAP] [{i}/{total_symbols}] Processing {symbol}")
                saved = await self.collect_symbol_data(symbol)
                total_saved += saved
                if i < total_symbols:
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[US_VWAP] Error processing {symbol}: {e}")
                continue
        await self.close_pool()
        duration = datetime.now() - start_time
        logger.info(f"[US_VWAP] Completed in {duration}, saved {total_saved} records")


class WeeklyCollector:
    """Collector for US Weekly OHLCV data from Alpha Vantage TIME_SERIES_WEEKLY API"""

    def __init__(self, api_key: str, database_url: str, max_concurrent: int = 20):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url

        # Date range for data collection
        self.start_date = date(2024, 9, 1)
        self.end_date = date.today()

        # API call rate limiting: 300 calls per minute = 0.2 seconds between calls
        self.call_interval = 0.2

        # Log file for tracking collected data - Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            self.log_file_path = '/app/log/us_weekly_collected.json'
        else:
            self.log_file_path = 'log/us_weekly_collected.json'

        self.collected_data = self.load_collected_log()

        # API endpoint
        self.base_url = "https://www.alphavantage.co/query"

        # Pool for database connections
        self.pool = None
        self.max_concurrent = max_concurrent
        self.semaphore = None

        # aiohttp session for API calls
        self.session = None

    def load_collected_log(self) -> Dict[str, set]:
        """Load previously collected symbol-date pairs from log file"""
        if os.path.exists(self.log_file_path):
            try:
                with open(self.log_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Convert list of dates back to set
                    return {symbol: set(dates) for symbol, dates in data.items()}
            except Exception as e:
                logger.warning(f"Failed to load weekly log file: {e}")
        return {}

    def save_collected_log(self):
        """Save collected symbol-date pairs to log file"""
        try:
            # Convert sets to lists for JSON serialization
            data_to_save = {symbol: list(dates) for symbol, dates in self.collected_data.items()}

            os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
            with open(self.log_file_path, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=2, default=str)
            logger.info(f"Saved weekly collection log with {len(self.collected_data)} symbols")
        except Exception as e:
            logger.error(f"Failed to save weekly log file: {e}")

    def mark_collected(self, symbol: str, date_str: str):
        """Mark a symbol-date pair as collected"""
        if symbol not in self.collected_data:
            self.collected_data[symbol] = set()
        self.collected_data[symbol].add(date_str)

    def is_already_collected(self, symbol: str, date_str: str) -> bool:
        """Check if a symbol-date pair was already collected"""
        return symbol in self.collected_data and date_str in self.collected_data.get(symbol, set())

    async def init_session(self):
        """Initialize aiohttp session"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    async def init_pool(self):
        """Initialize connection pool"""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=10,
            max_size=20,
            command_timeout=60
        )
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        logger.info(f"Weekly collector: Database connection pool initialized (max connections: 20, semaphore: {self.max_concurrent})")

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Weekly collector: Database connection pool closed")

    async def get_connection(self):
        """Get connection from pool"""
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_weekly_data(self, symbol: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Fetch weekly time series data from Alpha Vantage API with retry logic"""
        await self.init_session()

        for attempt in range(max_retries):
            try:
                params = {
                    'function': 'TIME_SERIES_WEEKLY',
                    'symbol': symbol,
                    'apikey': self.api_key,
                    'datatype': 'json'
                }

                async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Check for API error messages
                        if 'Error Message' in data:
                            logger.error(f"API error for {symbol}: {data['Error Message']}")
                            return None
                        elif 'Note' in data:
                            logger.warning(f"API limit reached: {data['Note']}")
                            if attempt < max_retries - 1:
                                wait_time = 2 ** attempt  # Exponential backoff
                                logger.info(f"Retrying {symbol} after {wait_time}s (attempt {attempt + 1}/{max_retries})")
                                await asyncio.sleep(wait_time)
                                continue
                            return None
                        elif 'Weekly Time Series' in data:
                            if attempt > 0:
                                logger.info(f"Successfully fetched {symbol} on attempt {attempt + 1}")
                            return data
                        else:
                            logger.warning(f"Unexpected response format for {symbol}")
                            return None
                    else:
                        logger.error(f"API request failed for {symbol}: Status {response.status}")
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            logger.info(f"Retrying {symbol} after {wait_time}s (attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                            continue
                        return None

            except asyncio.TimeoutError:
                logger.error(f"Timeout for {symbol} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying {symbol} after {wait_time}s")
                    await asyncio.sleep(wait_time)
                    continue
                return None
            except Exception as e:
                logger.error(f"Error fetching weekly data for {symbol} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying {symbol} after {wait_time}s")
                    await asyncio.sleep(wait_time)
                    continue
                return None

        return None

    def transform_weekly_data(self, data: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
        """Transform API response to database format"""
        transformed = []

        if 'Weekly Time Series' not in data:
            return transformed

        time_series = data['Weekly Time Series']

        for date_str, values in time_series.items():
            try:
                # Parse date
                data_date = datetime.strptime(date_str, '%Y-%m-%d').date()

                # Check if date is within our collection range
                if self.start_date <= data_date <= self.end_date:
                    # Skip if already collected (check log)
                    if self.is_already_collected(symbol, date_str):
                        logger.debug(f"Skipping {symbol} {date_str} - already in log")
                        continue

                    # Parse price data
                    open_price = float(values['1. open'])
                    high_price = float(values['2. high'])
                    low_price = float(values['3. low'])
                    close_price = float(values['4. close'])
                    volume = int(values['5. volume'])

                    transformed.append({
                        'date': data_date,
                        'symbol': symbol,
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': close_price,
                        'volume': volume,
                        'updated_at': datetime.now()
                    })

            except Exception as e:
                logger.error(f"Error transforming weekly data for {symbol} on {date_str}: {e}")
                continue

        return transformed

    async def save_weekly_data(self, data: List[Dict[str, Any]]) -> int:
        """Save weekly data to database"""
        if not data:
            return 0

        conn = await self.get_connection()
        saved_count = 0

        try:
            # Start transaction
            async with conn.transaction():
                # Prepare insert query
                query = """
                    INSERT INTO us_weekly (date, symbol, open, high, low, close, volume, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        updated_at = EXCLUDED.updated_at
                """

                # Insert each record
                for record in data:
                    try:
                        await conn.execute(
                            query,
                            record['date'],
                            record['symbol'],
                            record['open'],
                            record['high'],
                            record['low'],
                            record['close'],
                            record['volume'],
                            record['updated_at']
                        )

                        # Mark as collected in log
                        date_str = record['date'].strftime('%Y-%m-%d')
                        self.mark_collected(record['symbol'], date_str)
                        saved_count += 1

                    except Exception as e:
                        logger.error(f"Error saving weekly record for {record['symbol']} on {record['date']}: {e}")
                        raise  # Re-raise to rollback transaction

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

            # Save log after each successful batch
            if saved_count > 0:
                self.save_collected_log()

            return saved_count

        except Exception as e:
            logger.error(f"Error in save_weekly_data: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return 0

    async def check_existing_data(self, symbol: str) -> set:
        """Check which dates already exist in database for a symbol"""
        conn = await self.get_connection()

        try:
            query = """
                SELECT date
                FROM us_weekly
                WHERE symbol = $1
                AND date BETWEEN $2 AND $3
            """

            rows = await conn.fetch(query, symbol, self.start_date, self.end_date)

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

            return {row['date'].strftime('%Y-%m-%d') for row in rows}

        except Exception as e:
            logger.error(f"Error checking existing weekly data for {symbol}: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return set()

    async def get_active_symbols(self) -> List[str]:
        """Get list of active symbols from database"""
        conn = await self.get_connection()

        try:
            # Try to get symbols from us_stock_basic first
            query = """
                SELECT DISTINCT symbol
                FROM us_stock_basic
                WHERE symbol IS NOT NULL
                ORDER BY symbol
            """

            rows = await conn.fetch(query)

            if not rows:
                # If no symbols in us_stock_basic, try us_daily
                query = """
                    SELECT DISTINCT symbol
                    FROM us_daily
                    WHERE symbol IS NOT NULL
                    ORDER BY symbol
                """
                rows = await conn.fetch(query)

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

            symbols = [row['symbol'] for row in rows]
            logger.info(f"Weekly collector: Found {len(symbols)} symbols in database")
            return symbols

        except Exception as e:
            logger.error(f"Error getting active symbols for weekly collection: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return []

    async def collect_symbol_data(self, symbol: str) -> int:
        """Collect data for a single symbol"""
        try:
            # First check what dates we already have in DB
            existing_dates = await self.check_existing_data(symbol)

            # Also check what dates we have in log
            logged_dates = self.collected_data.get(symbol, set())

            # Combine both sources
            all_existing_dates = existing_dates | logged_dates

            # Calculate required dates (weekly data from start_date to end_date)
            from datetime import timedelta
            required_dates = set()
            current = self.start_date
            while current <= self.end_date:
                required_dates.add(current.strftime('%Y-%m-%d'))
                current += timedelta(days=7)  # Weekly

            # Check if we need to fetch data
            missing_dates = required_dates - all_existing_dates

            if not missing_dates:
                logger.info(f"Skip {symbol} - all weekly dates already collected")
                return 0

            logger.info(f"Fetching weekly data for {symbol} - missing {len(missing_dates)} dates")

            # Fetch data from API
            api_data = await self.get_weekly_data(symbol)

            if api_data:
                # Transform data
                transformed = self.transform_weekly_data(api_data, symbol)

                if transformed:
                    # Save to database
                    saved = await self.save_weekly_data(transformed)
                    logger.info(f"Saved {saved} weekly records for {symbol}")
                    return saved
                else:
                    # No new data to save, but mark missing dates as attempted
                    for date_str in missing_dates:
                        self.mark_collected(symbol, date_str)
                    self.save_collected_log()
                    logger.info(f"No new weekly data for {symbol}, marked {len(missing_dates)} dates as attempted")
                    return 0
            else:
                logger.warning(f"Failed to fetch weekly data for {symbol}")
                return 0

        except Exception as e:
            logger.error(f"Error collecting weekly data for {symbol}: {e}")
            return 0

    async def process_symbol_batch(self, symbols: List[str], batch_id: int):
        """Process a batch of symbols with semaphore control"""
        async with self.semaphore:
            try:
                logger.info(f"Weekly Batch {batch_id}: Starting processing {len(symbols)} symbols")

                processed_count = 0
                failed_count = 0

                for i, symbol in enumerate(symbols, 1):
                    try:
                        # Collect data for symbol
                        saved = await self.collect_symbol_data(symbol)

                        if saved > 0:
                            processed_count += 1

                        # Rate limiting
                        if i < len(symbols):
                            await asyncio.sleep(self.call_interval)

                    except Exception as e:
                        logger.error(f"Weekly Batch {batch_id}: Failed to process {symbol}: {e}")
                        failed_count += 1
                        continue

                logger.info(f"Weekly Batch {batch_id}: Completed - Processed: {processed_count}, Failed: {failed_count}")
                return processed_count

            except Exception as e:
                logger.error(f"Weekly Batch {batch_id}: Batch processing failed: {e}")
                return 0

    async def run_collection(self):
        """Main collection process"""
        logger.info("=== Starting US Weekly Data Collection ===")
        logger.info(f"Date range: {self.start_date} to {self.end_date}")
        logger.info(f"API rate: 300 calls/min (0.2s interval)")

        # Initialize database pool
        await self.init_pool()

        # Get list of symbols
        all_symbols = await self.get_active_symbols()

        if not all_symbols:
            logger.error("No symbols found to process for weekly collection")
            await self.close_pool()
            return

        symbols = all_symbols
        completed_count = 0

        # Statistics
        total_symbols = len(symbols)
        start_time = datetime.now()
        logger.info(f"Total symbols: {len(all_symbols)}, To process: {total_symbols}")

        # Split into batches of 100
        batch_size = 100
        symbol_batches = []
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            symbol_batches.append(batch)

        logger.info(f"Split {total_symbols} symbols into {len(symbol_batches)} batches of max {batch_size}")
        logger.info(f"Concurrent batch limit: {self.max_concurrent} (controlled by semaphore)")

        # Create parallel tasks
        tasks = []
        for i, batch in enumerate(symbol_batches):
            if batch:
                task = asyncio.create_task(self.process_symbol_batch(batch, i+1))
                tasks.append(task)

        # Wait for all batches to complete
        if tasks:
            logger.info(f"Starting parallel processing of {len(tasks)} weekly batches...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            total_saved = sum(r for r in results if isinstance(r, int))
        else:
            total_saved = 0

        # Final save of log
        self.save_collected_log()

        # Close database pool
        await self.close_pool()

        # Summary
        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("=== Weekly Collection Summary ===")
        logger.info(f"Total execution time: {duration}")
        logger.info(f"Total symbols: {len(all_symbols)}")
        logger.info(f"Processed: {total_symbols}")
        logger.info(f"Total records saved: {total_saved}")
        logger.info(f"Log file: {self.log_file_path}")
        logger.info("=== Weekly Collection Completed ===")


async def get_latest_available_date(api_key: str) -> Optional[date]:
    """Get the latest available date from Alpha Vantage API - OPTIMIZATION #5"""
    try:
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': 'AAPL',  # Use AAPL as reference symbol
            'apikey': api_key,
            'outputsize': 'compact',
            'datatype': 'json'
        }
        async with aiohttp.ClientSession() as session:
            async with session.get('https://www.alphavantage.co/query', params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'Time Series (Daily)' in data:
                        # Get the most recent date
                        dates = list(data['Time Series (Daily)'].keys())
                        if dates:
                            latest_date_str = dates[0]  # API returns sorted descending
                            latest_date = datetime.strptime(latest_date_str, '%Y-%m-%d').date()
                            logger.info(f"Latest available date from API: {latest_date}")
                            return latest_date
        logger.warning("Could not get latest date from API, using latest business day")
        return DailyCollector('', '').get_latest_business_day()
    except Exception as e:
        logger.error(f"Error getting latest date from API: {e}, using latest business day")
        return DailyCollector('', '').get_latest_business_day()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
