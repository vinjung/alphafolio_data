# alpha/data/us/indicator_recovery.py
"""
US Indicators Recovery Tool - Refactored

Automatically fills null indicator values in us_indicators table using Alpha Vantage API
Collection period: D-7 to Today (execution date)
Architecture: Sequential processing with rate limiting (300 calls/min)
"""

import asyncio
import asyncpg
import aiohttp
import logging
import json
import os
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class IndicatorRecoveryCollector:
    """
    Collect missing technical indicators from Alpha Vantage API
    and update us_indicators table with null values filled
    """

    # Indicator groups: API 1 call = multiple columns
    INDICATOR_GROUPS = {
        'rsi': ['rsi'],
        'macd': ['macd', 'macd_signal', 'macd_hist'],
        'bbands': ['real_upper_band', 'real_middle_band', 'real_lower_band'],
        'stoch': ['slowk', 'slowd'],
        'mfi': ['mfi'],
        'roc': ['roc'],
        'sma': ['sma'],
        'ema': ['ema'],
        'adx': ['adx'],
        'wma': ['wma'],
        'aroon': ['aroon'],
        'cci': ['cci'],
        'obv': ['obv'],
        'atr': ['atr']
    }

    # Alpha Vantage function names
    FUNCTION_MAP = {
        'rsi': 'RSI',
        'macd': 'MACD',
        'bbands': 'BBANDS',
        'stoch': 'STOCH',
        'mfi': 'MFI',
        'roc': 'ROC',
        'sma': 'SMA',
        'ema': 'EMA',
        'adx': 'ADX',
        'wma': 'WMA',
        'aroon': 'AROON',
        'cci': 'CCI',
        'obv': 'OBV',
        'atr': 'ATR'
    }

    def __init__(self, api_key: str, database_url: str, execution_date: date = None):
        """
        Initialize Indicator Recovery Collector

        Args:
            api_key: Alpha Vantage API key
            database_url: PostgreSQL database URL
            execution_date: Execution date (default: today)
        """
        self.api_key = api_key

        # Normalize database URL
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url

        # Set date range: D-7 to execution_date
        self.execution_date = execution_date or date.today()
        self.start_date = self.execution_date - timedelta(days=7)
        self.end_date = self.execution_date

        self.base_url = "https://www.alphavantage.co/query"
        self.call_interval = 0.2  # 300 calls/min

        # Connection pool and session
        self.pool = None
        self.session = None

        # Statistics
        self.stats = {
            'total_work_items': 0,
            'processed': 0,
            'skipped': 0,
            'failed': 0,
            'saved': 0,
            'retry_success': 0
        }

        logger.info("=" * 80)
        logger.info("US Indicators Recovery Collector Initialized")
        logger.info("=" * 80)
        logger.info(f"Execution Date: {self.execution_date}")
        logger.info(f"Collection Period: {self.start_date} to {self.end_date} (D-7 to D-0)")
        logger.info(f"API Rate: 300 calls/min (0.2s interval)")
        logger.info("=" * 80)

    async def init_resources(self):
        """Initialize database pool and HTTP session"""
        # Create connection pool
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=5,
            max_size=20,
            command_timeout=180,
            max_inactive_connection_lifetime=240,
            max_queries=50000,
            max_cached_statement_lifetime=0
        )
        logger.info("Database connection pool initialized (min=5, max=20)")

        # Create HTTP session
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(timeout=timeout)
        logger.info("HTTP session initialized")

    async def close_resources(self):
        """Close database pool and HTTP session"""
        if self.session:
            await self.session.close()
            logger.info("HTTP session closed")

        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def find_null_records(self) -> List[Tuple[date, str, str, List[str]]]:
        """
        Find records with null indicator values in us_indicators table

        Returns:
            List of work items: [(date, symbol, indicator_name, columns), ...]
        """
        logger.info("\n" + "=" * 80)
        logger.info("Step 1: Searching for null records in us_indicators table")
        logger.info("=" * 80)
        logger.info(f"Date range: {self.start_date} to {self.end_date}")

        # Build query to find rows with any null indicator columns
        null_conditions = []
        for indicator, columns in self.INDICATOR_GROUPS.items():
            condition = ' OR '.join([f'{col} IS NULL' for col in columns])
            null_conditions.append(f'({condition})')

        # Get all indicator columns
        all_columns = [col for cols in self.INDICATOR_GROUPS.values() for col in cols]

        query = f"""
            SELECT date, symbol, {', '.join(all_columns)}
            FROM us_indicators
            WHERE date >= $1 AND date <= $2
            AND ({' OR '.join(null_conditions)})
            ORDER BY date, symbol
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, self.start_date, self.end_date)

        if not rows:
            logger.info("[OK] No records with null values found")
            return []

        logger.info(f"Found {len(rows)} records with null values")

        # Organize work items by (date, symbol, indicator)
        work_items = []

        for row in rows:
            record_date = row['date']
            symbol = row['symbol']

            for indicator_name, columns in self.INDICATOR_GROUPS.items():
                # Check if any column in this indicator group is null
                needs_collection = any(row[col] is None for col in columns)
                if needs_collection:
                    work_items.append((record_date, symbol, indicator_name, columns))

        logger.info(f"Total API calls needed: {len(work_items)}")
        self.stats['total_work_items'] = len(work_items)

        return work_items

    async def fetch_indicator_data(self, work_item: Tuple, item_idx: int) -> Dict[str, Any]:
        """
        Fetch single indicator data from Alpha Vantage API

        Args:
            work_item: (record_date, symbol, indicator_name, columns)
            item_idx: Work item index for tracking

        Returns:
            Result dict: {'status': 'success'|'failed'|'skipped', 'data': {...}}
        """
        record_date, symbol, indicator_name, columns = work_item

        try:
            # Build API parameters
            params = {
                'apikey': self.api_key,
                'symbol': symbol,
                'interval': 'daily',
                'time_period': 14,
                'series_type': 'close',
                'function': self.FUNCTION_MAP[indicator_name]
            }

            # Make API request
            async with self.session.get(
                self.base_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status != 200:
                    logger.warning(f"[{symbol}] API request failed: Status {response.status}")
                    return {'status': 'failed', 'item_idx': item_idx}

                data = await response.json()

                # Check for API errors
                if 'Error Message' in data:
                    logger.warning(f"[{symbol}] API error: {data['Error Message']}")
                    return {'status': 'failed', 'item_idx': item_idx}

                if 'Note' in data:
                    logger.warning(f"[{symbol}] API rate limit: {data['Note']}")
                    return {'status': 'failed', 'item_idx': item_idx}

                # Find the correct key in response
                technical_key = None
                for key in data.keys():
                    if 'Technical Analysis' in key or indicator_name.upper() in key:
                        technical_key = key
                        break

                if not technical_key or technical_key not in data:
                    logger.debug(f"[{symbol}] No technical data found for {indicator_name}")
                    return {'status': 'skipped', 'item_idx': item_idx}

                # Extract data for the specific date
                date_str = record_date.strftime('%Y-%m-%d')
                if date_str not in data[technical_key]:
                    logger.debug(f"[{symbol}] No data for date {date_str}")
                    return {'status': 'skipped', 'item_idx': item_idx}

                values = data[technical_key][date_str]

                # Prepare update data based on indicator type
                update_dict = {'date': record_date, 'symbol': symbol}

                # Extract values based on indicator type
                if indicator_name == 'rsi':
                    update_dict['rsi'] = float(values.get('RSI', 0))
                elif indicator_name == 'macd':
                    update_dict['macd'] = float(values.get('MACD', 0))
                    update_dict['macd_signal'] = float(values.get('MACD_Signal', 0))
                    update_dict['macd_hist'] = float(values.get('MACD_Hist', 0))
                elif indicator_name == 'bbands':
                    update_dict['real_upper_band'] = float(values.get('Real Upper Band', 0))
                    update_dict['real_middle_band'] = float(values.get('Real Middle Band', 0))
                    update_dict['real_lower_band'] = float(values.get('Real Lower Band', 0))
                elif indicator_name == 'stoch':
                    update_dict['slowk'] = float(values.get('SlowK', 0))
                    update_dict['slowd'] = float(values.get('SlowD', 0))
                elif indicator_name == 'mfi':
                    update_dict['mfi'] = float(values.get('MFI', 0))
                elif indicator_name == 'roc':
                    update_dict['roc'] = float(values.get('ROC', 0))
                elif indicator_name == 'sma':
                    update_dict['sma'] = float(values.get('SMA', 0))
                elif indicator_name == 'ema':
                    update_dict['ema'] = float(values.get('EMA', 0))
                elif indicator_name == 'adx':
                    update_dict['adx'] = float(values.get('ADX', 0))
                elif indicator_name == 'wma':
                    update_dict['wma'] = float(values.get('WMA', 0))
                elif indicator_name == 'aroon':
                    # Aroon oscillator = Aroon Up - Aroon Down
                    aroon_up = float(values.get('Aroon Up', 0))
                    aroon_down = float(values.get('Aroon Down', 0))
                    update_dict['aroon'] = aroon_up - aroon_down
                elif indicator_name == 'cci':
                    update_dict['cci'] = float(values.get('CCI', 0))
                elif indicator_name == 'obv':
                    update_dict['obv'] = float(values.get('OBV', 0))
                elif indicator_name == 'atr':
                    update_dict['atr'] = float(values.get('ATR', 0))

                return {'status': 'success', 'data': update_dict, 'item_idx': item_idx}

        except Exception as e:
            logger.error(f"[{symbol}] Error fetching {indicator_name}: {e}")
            return {'status': 'failed', 'item_idx': item_idx, 'error': str(e)}

    async def update_indicators_table(self, update_data: Dict[str, Any]) -> bool:
        """
        Update us_indicators table with fetched data

        Args:
            update_data: Dict with 'date', 'symbol', and indicator columns

        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract date and symbol
            record_date = update_data['date']
            symbol = update_data['symbol']

            # Get columns to update (exclude date and symbol)
            update_columns = {k: v for k, v in update_data.items()
                            if k not in ['date', 'symbol']}

            if not update_columns:
                return False

            # Build UPDATE query
            set_clauses = []
            params = []
            param_idx = 3  # Start from $3 (after date and symbol)

            for col, value in update_columns.items():
                set_clauses.append(f'{col} = ${param_idx}')
                params.append(value)
                param_idx += 1

            query = f"""
                UPDATE us_indicators
                SET {', '.join(set_clauses)}
                WHERE date = $1 AND symbol = $2
            """

            # Execute update
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, record_date, symbol, *params)

            # Check if row was updated
            if result == "UPDATE 0":
                logger.warning(f"[{symbol}] No row found to update for date {record_date}")
                return False

            logger.debug(f"[{symbol}] Updated {record_date}: {list(update_columns.keys())}")
            return True

        except Exception as e:
            logger.error(f"Failed to update us_indicators: {e}")
            return False

    async def run_collection(self):
        """
        Main collection function - Sequential processing with rate limiting
        """
        try:
            logger.info("\n" + "=" * 80)
            logger.info("Starting Indicator Recovery Collection")
            logger.info("=" * 80)

            # Initialize resources
            await self.init_resources()

            # Find null records
            work_items = await self.find_null_records()

            if not work_items:
                logger.info("\n[OK] No work to do - all indicators are complete")
                return

            logger.info("\n" + "=" * 80)
            logger.info("Step 2: API Collection and Database Update")
            logger.info("=" * 80)
            logger.info(f"Processing {len(work_items)} work items sequentially...")
            logger.info(f"Rate limit: 300 calls/min (0.2s interval)")
            logger.info("=" * 80)

            # Store failed items for retry
            failed_items = []

            # First pass: Process all work items sequentially
            for i, work_item in enumerate(work_items, 1):
                record_date, symbol, indicator_name, columns = work_item

                # Fetch data from API
                result = await self.fetch_indicator_data(work_item, i-1)

                # Update statistics and database
                if result['status'] == 'success':
                    # Update us_indicators table
                    success = await self.update_indicators_table(result['data'])
                    if success:
                        self.stats['processed'] += 1
                        self.stats['saved'] += 1
                        logger.info(
                            f"[{i}/{len(work_items)}] [OK] {symbol} {indicator_name} "
                            f"({record_date}) - Saved"
                        )
                    else:
                        self.stats['failed'] += 1
                        failed_items.append(work_item)
                        logger.warning(
                            f"[{i}/{len(work_items)}] [FAIL] {symbol} {indicator_name} "
                            f"({record_date}) - Update failed"
                        )
                elif result['status'] == 'skipped':
                    self.stats['skipped'] += 1
                    logger.debug(
                        f"[{i}/{len(work_items)}] [SKIP] {symbol} {indicator_name} "
                        f"({record_date}) - Skipped"
                    )
                else:
                    self.stats['failed'] += 1
                    failed_items.append(work_item)
                    logger.warning(
                        f"[{i}/{len(work_items)}] [FAIL] {symbol} {indicator_name} "
                        f"({record_date}) - Failed"
                    )

                # Progress report every 50 items
                if i % 50 == 0:
                    logger.info(
                        f"\n[Progress] {i}/{len(work_items)} completed - "
                        f"Saved: {self.stats['saved']}, "
                        f"Skipped: {self.stats['skipped']}, "
                        f"Failed: {self.stats['failed']}\n"
                    )

                # Rate limit: 300 calls/min = 0.2s interval
                await asyncio.sleep(self.call_interval)

            # Retry failed items (up to 2 additional passes)
            for retry_round in range(1, 3):
                if not failed_items:
                    break

                logger.info(f"\n[Retry round {retry_round}] {len(failed_items)} items to retry")
                await asyncio.sleep(3)  # Wait before retry

                retry_list = failed_items.copy()
                failed_items = []

                for i, work_item in enumerate(retry_list, 1):
                    record_date, symbol, indicator_name, columns = work_item

                    result = await self.fetch_indicator_data(work_item, i-1)

                    if result['status'] == 'success':
                        success = await self.update_indicators_table(result['data'])
                        if success:
                            self.stats['retry_success'] += 1
                            self.stats['saved'] += 1
                            self.stats['failed'] -= 1
                            logger.info(
                                f"[Retry {retry_round}] [{i}/{len(retry_list)}] [OK] "
                                f"{symbol} {indicator_name} ({record_date}) - Saved"
                            )
                        else:
                            failed_items.append(work_item)
                    elif result['status'] == 'skipped':
                        self.stats['skipped'] += 1
                        self.stats['failed'] -= 1
                    else:
                        failed_items.append(work_item)

                    await asyncio.sleep(self.call_interval)

            # Log final failed items
            if failed_items:
                failed_info = [(w[1], w[2]) for w in failed_items[:10]]  # (symbol, indicator)
                logger.warning(
                    f"\n[Final] {len(failed_items)} items failed after all retries: "
                    f"{failed_info}{'...' if len(failed_items) > 10 else ''}"
                )

            # Print summary
            self.print_summary()

            # Save recovery log
            self.save_recovery_log()

        except Exception as e:
            logger.error(f"Collection failed: {e}")
            import traceback
            traceback.print_exc()
            raise

        finally:
            await self.close_resources()

    def print_summary(self):
        """Print collection summary"""
        logger.info("\n" + "=" * 80)
        logger.info("Indicator Recovery Collection Summary")
        logger.info("=" * 80)
        logger.info(f"Execution Date: {self.execution_date}")
        logger.info(f"Collection Period: {self.start_date} to {self.end_date} (D-7 to D-0)")
        logger.info("")
        logger.info(f"Total Work Items: {self.stats['total_work_items']}")
        logger.info(f"Processed: {self.stats['processed']}")
        logger.info(f"Saved: {self.stats['saved']}")
        logger.info(f"  - First pass: {self.stats['saved'] - self.stats['retry_success']}")
        logger.info(f"  - Retry success: {self.stats['retry_success']}")
        logger.info(f"Skipped: {self.stats['skipped']}")
        logger.info(f"Failed: {self.stats['failed']}")

        if self.stats['total_work_items'] > 0:
            success_rate = (self.stats['saved'] / self.stats['total_work_items']) * 100
            logger.info(f"Success Rate: {success_rate:.1f}%")

        logger.info("")
        logger.info("Architecture: Sequential processing with retry (max 2 rounds)")
        logger.info("API Rate: 300 calls/min (0.2s interval)")
        logger.info("=" * 80)

    def save_recovery_log(self):
        """Save recovery log to JSON file"""
        try:
            log_dir = Path(__file__).parent.parent / 'log'
            log_dir.mkdir(parents=True, exist_ok=True)

            log_file = log_dir / f'indicator_recovery_{self.execution_date.strftime("%Y%m%d")}.json'

            log_data = {
                'execution_date': self.execution_date.isoformat(),
                'collection_period': {
                    'start_date': self.start_date.isoformat(),
                    'end_date': self.end_date.isoformat()
                },
                'statistics': self.stats,
                'timestamp': datetime.now().isoformat()
            }

            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, indent=2, ensure_ascii=False)

            logger.info(f"\n[OK] Recovery log saved: {log_file}")

        except Exception as e:
            logger.error(f"Failed to save recovery log: {e}")


async def main():
    """Main execution function"""
    # Get environment variables
    api_key = os.getenv('ALPHAVANTAGE_API_KEY')
    database_url = os.getenv('DATABASE_URL')

    if not api_key:
        logger.error("ALPHAVANTAGE_API_KEY environment variable is required")
        return

    if not database_url:
        logger.error("DATABASE_URL environment variable is required")
        return

    # Create collector instance
    collector = IndicatorRecoveryCollector(
        api_key=api_key,
        database_url=database_url,
        execution_date=date.today()
    )

    # Run collection
    await collector.run_collection()


if __name__ == "__main__":
    asyncio.run(main())
