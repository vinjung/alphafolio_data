# alpha/data/us/us_stock_basic.py
"""
US Stock Basic Info Collector
Fetches stock overview data from Alpha Vantage API and stores in us_stock_basic table
"""
import asyncpg
import aiohttp
import logging
import os
from datetime import datetime, date
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from pathlib import Path
from asyncio import Queue
import asyncio

# Load environment variables
load_dotenv()

# Setup logging - Use Railway Volume (/app/log) if available
if os.getenv('RAILWAY_ENVIRONMENT'):
    log_dir = Path('/app/log')
else:
    log_dir = Path(__file__).parent.parent / 'log'

log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'us_stock_basic.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class USStockBasicCollector:
    def __init__(self, api_key: str, database_url: str, call_interval: float = 12.0):
        self.api_key = api_key
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.call_interval = call_interval  # Default: 5 calls/min (12 seconds)
        self.pool = None
        self.session = None
        self.retry_count = 3
        self.retry_delay = 1

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
        logger.info("[US_STOCK_BASIC] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[US_STOCK_BASIC] Connection pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_symbols_from_us_symbol(self) -> List[str]:
        """Get all symbols from us_symbol table"""
        conn = await self.get_connection()
        try:
            query = """
                SELECT DISTINCT symbol
                FROM us_symbol
                WHERE symbol IS NOT NULL AND symbol != ''
                ORDER BY symbol
            """
            rows = await conn.fetch(query)
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[US_STOCK_BASIC] Found {len(symbols)} symbols from us_symbol table")
            return symbols
        except Exception as e:
            logger.error(f"[US_STOCK_BASIC] Error getting symbols: {e}")
            return []
        finally:
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

    async def get_overview_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch overview data from Alpha Vantage API with retry logic"""
        params = {
            'function': 'OVERVIEW',
            'symbol': symbol,
            'apikey': self.api_key
        }

        for attempt in range(self.retry_count):
            try:
                async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Check if API returned an error
                        if "Error Message" in data:
                            logger.error(f"[US_STOCK_BASIC] API Error for {symbol}: {data['Error Message']}")
                            return None

                        # Check if we hit rate limit
                        if "Information" in data and "rate limit" in data["Information"].lower():
                            logger.warning(f"[US_STOCK_BASIC] Rate limit hit for {symbol}, waiting...")
                            await asyncio.sleep(60)  # Wait 1 minute
                            continue

                        # Check if symbol exists
                        if not data or data == {} or "Symbol" not in data:
                            logger.warning(f"[US_STOCK_BASIC] No data found for symbol: {symbol}")
                            return None

                        return data
                    else:
                        logger.error(f"[US_STOCK_BASIC] Request failed for {symbol} (attempt {attempt + 1}): Status {response.status}")
                        if attempt < self.retry_count - 1:
                            await asyncio.sleep(self.retry_delay * (attempt + 1))

            except asyncio.TimeoutError:
                logger.error(f"[US_STOCK_BASIC] Timeout for {symbol} (attempt {attempt + 1})")
                if attempt < self.retry_count - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
            except Exception as e:
                logger.error(f"[US_STOCK_BASIC] Unexpected error for {symbol}: {str(e)}")
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

    async def save_stock_basic(self, data: List[Dict[str, Any]]) -> int:
        """Save stock basic data using executemany (fallback method)"""
        if not data:
            return 0

        conn = await self.get_connection()
        try:
            # Get field names from first record
            fields = list(data[0].keys())
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

            # Prepare batch values
            batch_values = [
                tuple(record[field] for field in fields)
                for record in data
            ]

            async with conn.transaction():
                await conn.executemany(query, batch_values)

            logger.info(f"[US_STOCK_BASIC] Saved {len(batch_values)} records using executemany")
            return len(batch_values)

        except Exception as e:
            logger.error(f"[US_STOCK_BASIC] Error in save_stock_basic: {e}")
            return 0
        finally:
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

    async def save_stock_basic_optimized(self, data: List[Dict[str, Any]]) -> int:
        """Save stock basic data using COPY command - OPTIMIZED"""
        if not data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                # Transaction optimization
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                # Get field names
                fields = list(data[0].keys())

                # Create temp table with same structure as us_stock_basic
                temp_columns = []
                for field in fields:
                    if field == 'symbol':
                        temp_columns.append(f"{field} VARCHAR(20)")
                    elif field in ['assettype', 'currency', 'country']:
                        temp_columns.append(f"{field} VARCHAR(50)")
                    elif field in ['stock_name', 'exchange', 'sector', 'industry', 'fiscalyearend']:
                        temp_columns.append(f"{field} VARCHAR(100)")
                    elif field in ['description', 'address', 'cik']:
                        temp_columns.append(f"{field} TEXT")
                    elif field in ['latestquarter', 'dividenddate', 'exdividenddate']:
                        temp_columns.append(f"{field} DATE")
                    elif field in ['market_cap', 'ebitda', 'revenuettm', 'grossprofitttm', 'sharesoutstanding', 'sharesfloat',
                                  'analystratingstrongbuy', 'analystratingbuy', 'analystratinghold', 'analystratingsell', 'analystratingstrongsell']:
                        temp_columns.append(f"{field} BIGINT")
                    elif field == 'updated_at':
                        temp_columns.append(f"{field} TIMESTAMP")
                    else:
                        temp_columns.append(f"{field} DECIMAL(12,4)")

                create_temp_sql = f"CREATE TEMP TABLE temp_us_stock_basic ({', '.join(temp_columns)}) ON COMMIT DROP"
                await conn.execute(create_temp_sql)

                # Prepare data for COPY
                rows = [
                    [record[field] for field in fields]
                    for record in data
                ]

                # COPY to temp table
                await conn.copy_records_to_table(
                    'temp_us_stock_basic',
                    records=rows,
                    columns=fields
                )

                # Create UPDATE clause
                update_clauses = ', '.join([f'{field} = EXCLUDED.{field}' for field in fields if field != 'symbol'])

                # Upsert from temp to main
                await conn.execute(f"""
                    INSERT INTO us_stock_basic ({', '.join(fields)})
                    SELECT * FROM temp_us_stock_basic
                    ON CONFLICT (symbol) DO UPDATE SET
                    {update_clauses}
                """)

            logger.info(f"[US_STOCK_BASIC] Saved {len(data)} records using COPY (OPTIMIZED)")
            return len(data)

        except Exception as e:
            logger.error(f"[US_STOCK_BASIC] Error in save_stock_basic_optimized: {e}")
            logger.info("[US_STOCK_BASIC] Falling back to executemany method")
            return await self.save_stock_basic(data)
        finally:
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern - OPTIMIZED"""
        for i, symbol in enumerate(symbols, 1):
            try:
                api_data = await self.get_overview_data(symbol)
                if api_data:
                    transformed = self.transform_data(api_data)
                    if transformed.get('symbol'):
                        await data_queue.put(transformed)
                        logger.info(f"[US_STOCK_BASIC API] [{i}/{len(symbols)}] Queued {symbol}")
                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[US_STOCK_BASIC API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 50):
        """DB worker for pipeline pattern - OPTIMIZED"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_stock_basic_optimized(batch)
                    total_saved += saved
                break
            batch.append(item)
            if len(batch) >= batch_size:
                saved = await self.save_stock_basic_optimized(batch)
                total_saved += saved
                batch = []
        logger.info(f"[US_STOCK_BASIC DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern"""
        logger.info("=" * 60)
        logger.info("[US_STOCK_BASIC OPTIMIZED] Starting collection")
        logger.info(f"[US_STOCK_BASIC OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")
        logger.info(f"Timestamp: {datetime.now()}")
        logger.info("=" * 60)

        await self.init_pool()

        # Get symbols from us_symbol table
        all_symbols = await self.get_symbols_from_us_symbol()

        if not all_symbols:
            logger.error("[US_STOCK_BASIC] No symbols found")
            await self.close_pool()
            return

        logger.info(f"[US_STOCK_BASIC] Total symbols to process: {len(all_symbols)}")

        start_time = datetime.now()
        data_queue = Queue(maxsize=100)

        api_task = asyncio.create_task(self.api_worker(all_symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=50))

        await api_task
        total_saved = await db_task

        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info("=" * 60)
        logger.info("[US_STOCK_BASIC OPTIMIZED] Completed")
        logger.info("=" * 60)
        logger.info(f"Duration: {duration}")
        logger.info(f"Total saved: {total_saved}")
        logger.info(f"Success rate: {total_saved}/{len(all_symbols)} ({100*total_saved/len(all_symbols) if all_symbols else 0:.1f}%)")

async def main():
    """Entry point"""
    api_key = os.getenv('ALPHAVANTAGE_API_KEY')
    database_url = os.getenv('DATABASE_URL')

    if not api_key:
        raise ValueError("ALPHAVANTAGE_API_KEY environment variable is required")

    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    # Premium plan: 300 calls/min (0.2 seconds interval)
    collector = USStockBasicCollector(api_key, database_url, call_interval=0.2)
    await collector.run_collection_optimized()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
