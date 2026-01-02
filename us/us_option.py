# -*- coding: utf-8 -*-
import asyncio
import asyncpg
import logging
import os
import sys
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
from dotenv import load_dotenv
import aiohttp
from asyncio import Queue
from pathlib import Path

# Add parent directory to path for importing collection_logger
sys.path.insert(0, str(Path(__file__).parent.parent))
from collection_logger import CollectionLogger

# Load environment variables
load_dotenv()

# Setup logging - Use Railway Volume (/app/log) if available
if os.getenv('RAILWAY_ENVIRONMENT'):
    log_dir = Path('/app/log')
else:
    log_dir = Path(__file__).parent.parent / 'log'

log_dir.mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'us_option_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class USOptionCollector:
    """US Options Data Collector from Alpha Vantage"""

    def __init__(self, api_key: str, database_url: str, call_interval: float, target_date: date = None):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.target_date = target_date if target_date else self.get_latest_business_day()
        self.call_interval = call_interval

        # Setup collection logger path based on environment
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_option_collected.json'
        else:
            log_file_path = str(Path(__file__).parent.parent / 'log' / 'us_option_collected.json')

        self.collection_logger = CollectionLogger(log_file_path)
        self.base_url = "https://www.alphavantage.co/query"
        self.pool = None
        self.session = None

    def get_date_string(self) -> str:
        """Get target date as string"""
        return self.target_date.strftime('%Y-%m-%d')

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
        logger.info("[US_OPTION] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[US_OPTION] Database connection pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_option_data(self, symbol: str) -> Optional[Dict]:
        """Fetch historical options data from Alpha Vantage API"""
        try:
            params = {
                'function': 'HISTORICAL_OPTIONS',
                'symbol': symbol,
                'apikey': self.api_key,
            }

            # Add date parameter to fetch historical data for specific date
            if self.target_date:
                params['date'] = self.target_date.strftime('%Y-%m-%d')
                logger.info(f"[US_OPTION] Calling API for {symbol} on {params['date']}...")
            else:
                logger.info(f"[US_OPTION] Calling API for {symbol}...")

            async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                logger.info(f"[US_OPTION] Received response for {symbol}, status: {response.status}")
                if response.status == 200:
                    data = await response.json()

                    if 'Error Message' in data:
                        logger.error(f"[US_OPTION] API error for {symbol}: {data['Error Message']}")
                        return None
                    elif 'Note' in data:
                        logger.warning(f"[US_OPTION] API limit reached: {data['Note']}")
                        return None
                    elif 'data' in data:
                        # Check if it's successful response with data field
                        return data
                    else:
                        logger.warning(f"[US_OPTION] Unexpected response format for {symbol}: {list(data.keys())}")
                        return None
                else:
                    logger.error(f"[US_OPTION] API request failed for {symbol}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"[US_OPTION] Error fetching data for {symbol}: {e}")
            return None

    def safe_decimal(self, value: str, default=None) -> Optional[float]:
        """Safely convert string to decimal"""
        if not value or value.strip() in ['', 'N/A', '-', 'null', 'None']:
            return default
        try:
            return float(value.strip())
        except (ValueError, AttributeError):
            return default

    def safe_int(self, value: str, default=None) -> Optional[int]:
        """Safely convert string to integer"""
        if not value or value.strip() in ['', 'N/A', '-', 'null', 'None']:
            return default
        try:
            return int(value.strip())
        except (ValueError, AttributeError):
            return default

    def transform_option_data(self, data: Dict, symbol: str) -> List[Dict]:
        """Transform option data to match database schema"""
        transformed = []

        if 'data' not in data:
            logger.warning(f"[US_OPTION] No 'data' field in response for {symbol}")
            return transformed

        options_data = data['data']
        logger.info(f"[US_OPTION] Processing {len(options_data)} option contracts for {symbol}")

        for option in options_data:
            try:
                # Parse dates
                expiration_date = datetime.strptime(option['expiration'], '%Y-%m-%d').date()
                data_date = datetime.strptime(option['date'], '%Y-%m-%d').date()

                # Note: We don't filter by target_date because API returns the most recent trading day
                # which might not match the requested date (e.g., weekends, holidays, future dates)

                transformed.append({
                    'contract_id': option.get('contractID', ''),
                    'symbol': option.get('symbol', symbol),
                    'expiration': expiration_date,
                    'strike': self.safe_decimal(option.get('strike')),
                    'type': option.get('type', ''),
                    'last': self.safe_decimal(option.get('last')),
                    'mark': self.safe_decimal(option.get('mark')),
                    'bid': self.safe_decimal(option.get('bid')),
                    'bid_size': self.safe_int(option.get('bid_size')),
                    'ask': self.safe_decimal(option.get('ask')),
                    'ask_size': self.safe_int(option.get('ask_size')),
                    'volume': self.safe_int(option.get('volume')),
                    'open_interest': self.safe_int(option.get('open_interest')),
                    'date': data_date,
                    'implied_volatility': self.safe_decimal(option.get('implied_volatility')),
                    'delta': self.safe_decimal(option.get('delta')),
                    'gamma': self.safe_decimal(option.get('gamma')),
                    'theta': self.safe_decimal(option.get('theta')),
                    'vega': self.safe_decimal(option.get('vega')),
                    'rho': self.safe_decimal(option.get('rho')),
                })
            except Exception as e:
                logger.error(f"[US_OPTION] Error transforming option for {symbol}: {e}, Data: {option}")
                continue

        return transformed

    async def save_option_data(self, data: List[Dict]) -> int:
        """Save option data using executemany"""
        if not data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                query = """
                    INSERT INTO us_option (
                        contract_id, symbol, expiration, strike, type,
                        last, mark, bid, bid_size, ask, ask_size,
                        volume, open_interest, date,
                        implied_volatility, delta, gamma, theta, vega, rho
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                    ON CONFLICT (contract_id, date) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        expiration = EXCLUDED.expiration,
                        strike = EXCLUDED.strike,
                        type = EXCLUDED.type,
                        last = EXCLUDED.last,
                        mark = EXCLUDED.mark,
                        bid = EXCLUDED.bid,
                        bid_size = EXCLUDED.bid_size,
                        ask = EXCLUDED.ask,
                        ask_size = EXCLUDED.ask_size,
                        volume = EXCLUDED.volume,
                        open_interest = EXCLUDED.open_interest,
                        implied_volatility = EXCLUDED.implied_volatility,
                        delta = EXCLUDED.delta,
                        gamma = EXCLUDED.gamma,
                        theta = EXCLUDED.theta,
                        vega = EXCLUDED.vega,
                        rho = EXCLUDED.rho
                """

                batch_values = [
                    (
                        record['contract_id'],
                        record['symbol'],
                        record['expiration'],
                        record['strike'],
                        record['type'],
                        record['last'],
                        record['mark'],
                        record['bid'],
                        record['bid_size'],
                        record['ask'],
                        record['ask_size'],
                        record['volume'],
                        record['open_interest'],
                        record['date'],
                        record['implied_volatility'],
                        record['delta'],
                        record['gamma'],
                        record['theta'],
                        record['vega'],
                        record['rho']
                    )
                    for record in data
                ]
                await conn.executemany(query, batch_values)

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(data)
        except Exception as e:
            logger.error(f"[US_OPTION] Error in save_option_data: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return 0

    async def save_option_data_optimized(self, data: List[Dict]) -> int:
        """Save option data using COPY command - OPTIMIZED"""
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
                    CREATE TEMP TABLE temp_us_option (
                        contract_id VARCHAR(30),
                        symbol VARCHAR(10),
                        expiration DATE,
                        strike NUMERIC(10, 2),
                        type VARCHAR(10),
                        last NUMERIC(10, 2),
                        mark NUMERIC(10, 2),
                        bid NUMERIC(10, 2),
                        bid_size INTEGER,
                        ask NUMERIC(10, 2),
                        ask_size INTEGER,
                        volume INTEGER,
                        open_interest INTEGER,
                        date DATE,
                        implied_volatility NUMERIC(10, 5),
                        delta NUMERIC(10, 5),
                        gamma NUMERIC(10, 5),
                        theta NUMERIC(10, 5),
                        vega NUMERIC(10, 5),
                        rho NUMERIC(10, 5)
                    ) ON COMMIT DROP
                ''')

                # Prepare data for COPY
                rows = [[
                    record['contract_id'],
                    record['symbol'],
                    record['expiration'],
                    record['strike'],
                    record['type'],
                    record['last'],
                    record['mark'],
                    record['bid'],
                    record['bid_size'],
                    record['ask'],
                    record['ask_size'],
                    record['volume'],
                    record['open_interest'],
                    record['date'],
                    record['implied_volatility'],
                    record['delta'],
                    record['gamma'],
                    record['theta'],
                    record['vega'],
                    record['rho']
                ] for record in data]

                # COPY to temp table
                await conn.copy_records_to_table(
                    'temp_us_option', records=rows,
                    columns=[
                        'contract_id', 'symbol', 'expiration', 'strike', 'type',
                        'last', 'mark', 'bid', 'bid_size', 'ask', 'ask_size',
                        'volume', 'open_interest', 'date',
                        'implied_volatility', 'delta', 'gamma', 'theta', 'vega', 'rho'
                    ]
                )

                # Upsert from temp to main
                await conn.execute('''
                    INSERT INTO us_option (
                        contract_id, symbol, expiration, strike, type,
                        last, mark, bid, bid_size, ask, ask_size,
                        volume, open_interest, date,
                        implied_volatility, delta, gamma, theta, vega, rho
                    )
                    SELECT * FROM temp_us_option
                    ON CONFLICT (contract_id, date) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        expiration = EXCLUDED.expiration,
                        strike = EXCLUDED.strike,
                        type = EXCLUDED.type,
                        last = EXCLUDED.last,
                        mark = EXCLUDED.mark,
                        bid = EXCLUDED.bid,
                        bid_size = EXCLUDED.bid_size,
                        ask = EXCLUDED.ask,
                        ask_size = EXCLUDED.ask_size,
                        volume = EXCLUDED.volume,
                        open_interest = EXCLUDED.open_interest,
                        implied_volatility = EXCLUDED.implied_volatility,
                        delta = EXCLUDED.delta,
                        gamma = EXCLUDED.gamma,
                        theta = EXCLUDED.theta,
                        vega = EXCLUDED.vega,
                        rho = EXCLUDED.rho
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(data)
        except Exception as e:
            logger.error(f"[US_OPTION] Error in save_option_data_optimized: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            logger.info("[US_OPTION] Falling back to regular save")
            return await self.save_option_data(data)

    async def ensure_daily_partition_exists(self, target_date: date):
        """Ensure daily partition exists for target date"""
        conn = await self.get_connection()
        try:
            partition_name = f"us_option_{target_date.strftime('%Y_%m_%d')}"
            next_date = target_date + timedelta(days=1)

            # Check if partition already exists
            check_query = """
                SELECT EXISTS (
                    SELECT 1 FROM pg_tables
                    WHERE tablename = $1 AND schemaname = 'public'
                )
            """
            exists = await conn.fetchval(check_query, partition_name)

            if exists:
                logger.info(f"[US_OPTION] Partition {partition_name} already exists")
            else:
                # Create partition
                create_query = f"""
                    CREATE TABLE {partition_name} PARTITION OF us_option
                    FOR VALUES FROM ('{target_date}') TO ('{next_date}')
                """
                await conn.execute(create_query)

                # Create unique index
                unique_idx_query = f"""
                    CREATE UNIQUE INDEX {partition_name}_unique
                    ON {partition_name} (contract_id, date)
                """
                await conn.execute(unique_idx_query)

                # Create symbol index
                symbol_idx_query = f"""
                    CREATE INDEX {partition_name}_symbol_idx
                    ON {partition_name} (symbol)
                """
                await conn.execute(symbol_idx_query)

                # Create expiration index
                expiration_idx_query = f"""
                    CREATE INDEX {partition_name}_expiration_idx
                    ON {partition_name} (expiration)
                """
                await conn.execute(expiration_idx_query)

                logger.info(f"[US_OPTION] Created partition {partition_name} for date {target_date}")

        except Exception as e:
            logger.warning(f"[US_OPTION] Error creating partition for {target_date}: {e}")
        finally:
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

    async def get_active_symbols(self) -> List[str]:
        """Get US Option collection symbols from predefined list"""
        # US Option Collection Symbols - Total: 549 symbols
        # Source: US_OPTION_SYMBOLS_COLLECTION_LIST.csv
        # Last updated: 2025-10-28
        # Breakdown: ETF 49 + Individual stocks (TOP100 + SP500) 500
        US_OPTION_SYMBOLS = [
            'A', 'AAPL', 'ABBV', 'ABNB', 'ABT', 'ACGL', 'ACN', 'ADBE', 'ADI', 'ADM',
            'ADP', 'ADSK', 'AEE', 'AEM', 'AEP', 'AER', 'AFRM', 'AFL', 'AGG', 'AIG',
            'AJG', 'ALAB', 'ALNY', 'AMD', 'AME', 'AMGN', 'AMP', 'AMRZ', 'AMT', 'AMZN',
            'AMAT', 'ANET', 'AON', 'APD', 'APH', 'APO', 'APP', 'ARES', 'ARKK', 'ASTS',
            'ATO', 'AVGO', 'AVB', 'AWK', 'AXP', 'AXON', 'AZO', 'BA', 'BAC', 'BBVA',
            'BC', 'BCE', 'BCS', 'BDX', 'BE', 'BIIB', 'BK', 'BKR', 'BKNG', 'BLK',
            'BMO', 'BMY', 'BN', 'BNS', 'BP', 'BR', 'BRO', 'BSX', 'BTI', 'BX',
            'C', 'CAH', 'CARR', 'CAT', 'CB', 'CBRE', 'CCJ', 'CCEP', 'CCI', 'CCL',
            'CDNS', 'CDW', 'CEG', 'CFG', 'CG', 'CHD', 'CHTR', 'CI', 'CINF', 'CL',
            'CLS', 'CM', 'CME', 'CMG', 'CMI', 'CMCSA', 'CMS', 'CNI', 'CNP', 'CNQ',
            'COF', 'COIN', 'COP', 'COR', 'COST', 'CPAY', 'CPNG', 'CPRT', 'CRCL', 'CRDO',
            'CRH', 'CRM', 'CRWD', 'CRWV', 'CSGP', 'CSX', 'CTAS', 'CTSH', 'CTVA', 'CVE',
            'CVS', 'CVX', 'CVNA', 'CW', 'D', 'DAL', 'DASH', 'DBA', 'DB', 'DD',
            'DDOG', 'DE', 'DELL', 'DEO', 'DG', 'DHI', 'DHR', 'DIA', 'DIS', 'DLR',
            'DOV', 'DRI', 'DTE', 'DUK', 'DVN', 'DXCM', 'E', 'EA', 'EBAY', 'ECL',
            'ED', 'EEM', 'EFA', 'EFX', 'EIX', 'EL', 'ELV', 'EME', 'EMR', 'ENB',
            'EOG', 'EPD', 'EQIX', 'EQT', 'ES', 'ETR', 'EW', 'EWJ', 'EXC', 'EXE',
            'EXPE', 'EXR', 'F', 'FANG', 'FAST', 'FCNCA', 'FDX', 'FE', 'FERG', 'FI',
            'FICO', 'FIG', 'FIS', 'FITB', 'FIX', 'FLUT', 'FMX', 'FNV', 'FOX', 'FOXA',
            'FSLR', 'FTS', 'FTNT', 'FWONA', 'FWONK', 'FXI', 'GD', 'GE', 'GEHC', 'GEV',
            'GIB', 'GILD', 'GIS', 'GLD', 'GLW', 'GM', 'GOOG', 'GOOGL', 'GPN', 'GRAB',
            'GRMN', 'GS', 'GWRE', 'GWW', 'HAL', 'HBAN', 'HCA', 'HD', 'HDB', 'HEI',
            'HIG', 'HLT', 'HMC', 'HON', 'HOOD', 'HPE', 'HPQ', 'HSBC', 'HSY', 'HUBB',
            'HUBS', 'HUM', 'HWM', 'HYG', 'IBB', 'IBKR', 'IBM', 'IBN', 'ICE', 'IDXX',
            'IGV', 'IHI', 'ING', 'INSM', 'INTC', 'INTU', 'IOT', 'IP', 'IQV', 'IR',
            'IRM', 'ISRG', 'ITW', 'IWD', 'IWF', 'IWM', 'JBL', 'JNJ', 'JPM', 'K',
            'KDP', 'KEYS', 'KGC', 'KHC', 'KLAC', 'KMB', 'KMI', 'KO', 'KR', 'KRE',
            'KVUE', 'L', 'LDOS', 'LEN', 'LH', 'LHX', 'LIN', 'LLY', 'LMT', 'LNG',
            'LOW', 'LPLA', 'LQD', 'LRCX', 'LULU', 'LVS', 'LYV', 'MA', 'MAR', 'MCD',
            'MCHP', 'MCK', 'MCO', 'MDB', 'MDLZ', 'MDY', 'MELI', 'MET', 'META', 'MFC',
            'MKL', 'MMC', 'MMM', 'MNST', 'MO', 'MPC', 'MPWR', 'MRK', 'MS', 'MSCI',
            'MSFT', 'MSI', 'MSTR', 'MTB', 'MTD', 'MU', 'MUFG', 'NEE', 'NET', 'NFLX',
            'NKE', 'NOC', 'NOW', 'NDAQ', 'NTRA', 'NTRS', 'NTAP', 'NTR', 'NU', 'NUE',
            'NVDA', 'NVO', 'NVR', 'NVS', 'NXPI', 'O', 'ODFL', 'OKE', 'ON', 'ORCL',
            'ORLY', 'OTIS', 'OWL', 'OXY', 'PANW', 'PAYX', 'PBA', 'PCAR', 'PCG', 'PEG',
            'PEP', 'PFE', 'PG', 'PGR', 'PH', 'PHM', 'PINS', 'PLD', 'PLTR', 'PM',
            'PNC', 'PODD', 'PPG', 'PPL', 'PRU', 'PSA', 'PSTG', 'PSX', 'PTC', 'PTR',
            'PUK', 'PWR', 'PYPL', 'QQQ', 'QCOM', 'QSR', 'RACE', 'RBLX', 'RCI', 'RCL',
            'RDDT', 'REGN', 'RF', 'RIO', 'RKLB', 'RKT', 'RMD', 'ROK', 'ROL', 'ROP',
            'ROST', 'RPRX', 'RSG', 'RTX', 'RY', 'SBAC', 'SBUX', 'SCCO', 'SCHW', 'SHW',
            'SHY', 'SLB', 'SLF', 'SLV', 'SMCI', 'SMH', 'SNOW', 'SNPS', 'SO', 'SOFI',
            'SOXX', 'SPGI', 'SPY', 'SQQQ', 'SRE', 'STE', 'STLA', 'STLD', 'STM', 'STT',
            'STX', 'STZ', 'SU', 'SW', 'SBAC', 'SYF', 'SYK', 'SYM', 'SYY', 'T',
            'TAK', 'TDG', 'TD', 'TDY', 'TEAM', 'TECK', 'TEF', 'TEL', 'TER', 'TFC',
            'TGT', 'TJX', 'TLT', 'TM', 'TMO', 'TMUS', 'TOST', 'TPG', 'TPL', 'TPR',
            'TQQQ', 'TRGP', 'TRI', 'TRP', 'TROW', 'TRV', 'TSCO', 'TSLA', 'TT', 'TTD',
            'TTWO', 'TU', 'TW', 'TXN', 'TYL', 'U', 'UAL', 'UBER', 'UBS', 'UI',
            'UL', 'ULTA', 'UNG', 'UNH', 'UNP', 'UPS', 'URI', 'USB', 'USO', 'UVXY',
            'V', 'VEEV', 'VG', 'VICI', 'VIK', 'VLO', 'VLTO', 'VMC', 'VRT', 'VRSN',
            'VRSK', 'VRTX', 'VST', 'VTI', 'VTR', 'VTV', 'VUG', 'VWO', 'VXX', 'VZ',
            'W', 'WAB', 'WAT', 'WBD', 'WCN', 'WDC', 'WDAY', 'WEC', 'WELL', 'WFC',
            'WIT', 'WM', 'WMB', 'WMT', 'WPM', 'WRB', 'WSM', 'WST', 'WTW', 'XBI',
            'XEL', 'XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU',
            'XLV', 'XLY', 'XOM', 'XYL', 'XYZ', 'YUM', 'ZBH', 'ZM', 'ZS', 'ZTS',
        ]

        logger.info(f"[US_OPTION] Loaded {len(US_OPTION_SYMBOLS)} predefined symbols for option collection")

        return US_OPTION_SYMBOLS

    async def aggregate_daily_summary(self, target_date: date) -> int:
        """Aggregate us_option to us_option_daily_summary using single query (including M19 strategy columns)"""
        conn = await self.get_connection()

        try:
            logger.info(f"[SUMMARY] Starting aggregation for {target_date}")

            # Single query aggregation using date index
            # Includes both basic stats and M19 strategy columns
            query = """
            INSERT INTO us_option_daily_summary (
                symbol, date,
                total_call_volume, total_put_volume,
                avg_implied_volatility,
                min_implied_volatility,
                max_implied_volatility,
                avg_call_iv,
                avg_put_iv,
                call_option_count,
                put_option_count
            )
            SELECT
                symbol,
                date,
                SUM(CASE WHEN type = 'call' THEN volume ELSE 0 END),
                SUM(CASE WHEN type = 'put' THEN volume ELSE 0 END),
                AVG(implied_volatility),
                MIN(implied_volatility),
                MAX(implied_volatility),
                AVG(CASE WHEN type = 'call'
                         AND implied_volatility IS NOT NULL
                         AND implied_volatility > 0
                         AND volume > 0
                         AND open_interest > 0
                         THEN implied_volatility END),
                AVG(CASE WHEN type = 'put'
                         AND implied_volatility IS NOT NULL
                         AND implied_volatility > 0
                         AND volume > 0
                         AND open_interest > 0
                         THEN implied_volatility END),
                COUNT(CASE WHEN type = 'call'
                           AND implied_volatility IS NOT NULL
                           AND implied_volatility > 0
                           AND volume > 0
                           AND open_interest > 0
                           THEN 1 END),
                COUNT(CASE WHEN type = 'put'
                           AND implied_volatility IS NOT NULL
                           AND implied_volatility > 0
                           AND volume > 0
                           AND open_interest > 0
                           THEN 1 END)
            FROM us_option
            WHERE date = $1
            GROUP BY symbol, date
            ON CONFLICT (symbol, date) DO UPDATE SET
                total_call_volume = EXCLUDED.total_call_volume,
                total_put_volume = EXCLUDED.total_put_volume,
                avg_implied_volatility = EXCLUDED.avg_implied_volatility,
                min_implied_volatility = EXCLUDED.min_implied_volatility,
                max_implied_volatility = EXCLUDED.max_implied_volatility,
                avg_call_iv = EXCLUDED.avg_call_iv,
                avg_put_iv = EXCLUDED.avg_put_iv,
                call_option_count = EXCLUDED.call_option_count,
                put_option_count = EXCLUDED.put_option_count
            """

            await conn.execute(query, target_date)

            # Get summary count
            count_query = """
            SELECT COUNT(*)
            FROM us_option_daily_summary
            WHERE date = $1
            """
            symbol_count = await conn.fetchval(count_query, target_date)

            logger.info(f"[SUMMARY] Completed: {symbol_count} symbols aggregated (with M19 columns)")

            return symbol_count

        except Exception as e:
            logger.error(f"[SUMMARY ERROR] {e}")
            raise
        finally:
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

    async def api_worker(self, symbols: List[str], data_queue: Queue, partition_created_flag: asyncio.Event):
        """API worker for pipeline pattern - OPTIMIZED"""
        logger.info(f"[US_OPTION API] Starting API worker for {len(symbols)} symbols")
        for i, symbol in enumerate(symbols, 1):
            try:
                date_list = [self.get_date_string()]

                if self.collection_logger.is_collected('us_option', symbol, date_list):
                    logger.debug(f"[US_OPTION] Skip {symbol} - already collected")
                    continue

                logger.info(f"[US_OPTION API] [{i}/{len(symbols)}] Fetching {symbol}...")
                api_data = await self.get_option_data(symbol)

                #  First symbol: Check actual date and create partition
                if i == 1 and not partition_created_flag.is_set():
                    if api_data and 'data' in api_data and len(api_data['data']) > 0:
                        actual_date_str = api_data['data'][0]['date']
                        actual_date = datetime.strptime(actual_date_str, '%Y-%m-%d').date()
                        logger.info(f"[US_OPTION PARTITION] API returns data for actual date: {actual_date}")
                        await self.ensure_daily_partition_exists(actual_date)
                        partition_created_flag.set()
                    else:
                        logger.warning(f"[US_OPTION PARTITION] First symbol {symbol} has no data, will retry with next symbol")

                if api_data:
                    logger.info(f"[US_OPTION API] [{i}/{len(symbols)}] Received API data for {symbol}")
                    transformed = self.transform_option_data(api_data, symbol)
                    logger.info(f"[US_OPTION API] [{i}/{len(symbols)}] Transformed {len(transformed)} options for {symbol}")
                    if transformed:
                        await data_queue.put((symbol, transformed, date_list))
                        logger.info(f"[US_OPTION API] [{i}/{len(symbols)}] Queued {symbol} ({len(transformed)} options)")

                        #  Retry partition creation if first symbol failed
                        if not partition_created_flag.is_set():
                            actual_date = transformed[0]['date']
                            logger.info(f"[US_OPTION PARTITION] Creating partition from symbol {symbol}, date: {actual_date}")
                            await self.ensure_daily_partition_exists(actual_date)
                            partition_created_flag.set()
                    else:
                        logger.warning(f"[US_OPTION API] [{i}/{len(symbols)}] No option data for {symbol} after transformation")
                else:
                    logger.warning(f"[US_OPTION API] [{i}/{len(symbols)}] No API data received for {symbol}")

                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[US_OPTION API] Error {symbol}: {e}")

        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 100):
        """DB worker for pipeline pattern - OPTIMIZED"""
        logger.info(f"[US_OPTION DB] Starting DB worker, batch_size={batch_size}")
        batch = []
        total_saved = 0
        symbols_batch = []
        date_lists_batch = []
        items_received = 0

        while True:
            logger.info(f"[US_OPTION DB] Waiting for data from queue... (received {items_received} items so far, batch size: {len(batch)})")
            try:
                item = await data_queue.get()
                logger.info(f"[US_OPTION DB] Received item from queue: {type(item)}")
            except Exception as e:
                logger.error(f"[US_OPTION DB] Error getting item from queue: {e}")
                break

            if item is None:
                logger.info(f"[US_OPTION DB] Received None (end signal), processing final batch of {len(batch)} records")
                if batch:
                    logger.info(f"[US_OPTION DB] Saving final batch of {len(batch)} records")
                    saved = await self.save_option_data_optimized(batch)
                    logger.info(f"[US_OPTION DB] Final batch saved: {saved} records")
                    if saved > 0:
                        total_saved += saved
                        for symbol, date_list in zip(symbols_batch, date_lists_batch):
                            self.collection_logger.mark_collected('us_option', symbol, date_list, saved)
                        self.collection_logger.save_log()
                break

            items_received += 1
            symbol, data, date_list = item
            logger.info(f"[US_OPTION DB] Received {len(data)} records for {symbol}, adding to batch (current batch: {len(batch)})")
            batch.extend(data)
            symbols_batch.append(symbol)
            date_lists_batch.append(date_list)

            if len(batch) >= batch_size:
                logger.info(f"[US_OPTION DB] Batch size reached ({len(batch)} >= {batch_size}), saving to database...")
                try:
                    saved = await self.save_option_data_optimized(batch)
                    logger.info(f"[US_OPTION DB] Successfully saved {saved} records")
                    if saved > 0:
                        total_saved += saved
                        for sym, dl in zip(symbols_batch, date_lists_batch):
                            self.collection_logger.mark_collected('us_option', sym, dl, saved // len(symbols_batch))
                        self.collection_logger.save_log()
                    batch = []
                    symbols_batch = []
                    date_lists_batch = []
                except Exception as e:
                    logger.error(f"[US_OPTION DB] Error saving batch: {e}", exc_info=True)
                    batch = []
                    symbols_batch = []
                    date_lists_batch = []

        logger.info(f"[US_OPTION DB] DB worker finished. Total items received: {items_received}, Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern"""
        logger.info(f"[US_OPTION OPTIMIZED] Starting for {self.target_date}")
        logger.info(f"[US_OPTION OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")

        await self.init_pool()

        all_symbols = await self.get_active_symbols()
        if not all_symbols:
            logger.error("[US_OPTION] No symbols found")
            await self.close_pool()
            return

        #  Put SPY first to ensure we get reliable data for partition creation
        if 'SPY' in all_symbols:
            all_symbols.remove('SPY')
            all_symbols.insert(0, 'SPY')
            logger.info("[US_OPTION] SPY moved to first position for partition detection")

        date_list = [self.get_date_string()]
        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_option', s, date_list)]

        logger.info(f"[US_OPTION] Total: {len(all_symbols)}, To process: {len(symbols)}")

        if not symbols:
            logger.info("[US_OPTION] All symbols already collected")
            await self.close_pool()
            return

        start_time = datetime.now()

        #  Create Event flag for partition creation tracking
        partition_created = asyncio.Event()

        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue, partition_created))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=100))

        await api_task
        total_saved = await db_task

        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[US_OPTION OPTIMIZED] Completed in {duration}, saved {total_saved} records")

        # Aggregate to summary table
        logger.info(f"[US_OPTION] Starting summary aggregation for {self.target_date}")
        await self.init_pool()
        try:
            summary_count = await self.aggregate_daily_summary(self.target_date)
            logger.info(f"[US_OPTION] Summary aggregation completed: {summary_count} symbols")
        except Exception as e:
            logger.error(f"[US_OPTION] Summary aggregation failed: {e}")
        finally:
            await self.close_pool()


async def main():
    API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')
    DATABASE_URL = os.getenv('DATABASE_URL')

    if not API_KEY:
        logger.error("ALPHAVANTAGE_API_KEY environment variable is required")
        return

    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable is required")
        return

    # Collect data from 2025-10-01 to yesterday
    start_date = date(2025, 10, 1)
    end_date = date.today() - timedelta(days=1)

    print("\n" + "="*70)
    print("US Options Data Collection - Historical Range")
    print("="*70)
    print(f"Period: {start_date} to {end_date}")
    print(f"Total days: {(end_date - start_date).days + 1}")
    print(f"API rate: 300 calls/min (0.2s interval)")
    print(f"Auto partition: Daily RANGE partitioning enabled")
    print("="*70 + "\n")

    # Call interval: 0.2 seconds = 300 calls/min
    call_interval = 0.2

    # Loop through each date
    current_date = start_date
    dates_processed = 0
    dates_skipped = 0

    while current_date <= end_date:
        print(f"\n{'='*70}")
        print(f"Processing date: {current_date} ({dates_processed + 1}/{(end_date - start_date).days + 1})")
        print(f"{'='*70}")

        collector = USOptionCollector(API_KEY, DATABASE_URL, call_interval, current_date)

        try:
            await collector.run_collection_optimized()
            dates_processed += 1
        except Exception as e:
            logger.error(f"Error processing date {current_date}: {e}")
            dates_skipped += 1

        current_date += timedelta(days=1)

    print("\n" + "="*70)
    print("US Options Collection Completed")
    print("="*70)
    print(f"Dates processed: {dates_processed}")
    print(f"Dates skipped: {dates_skipped}")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())
