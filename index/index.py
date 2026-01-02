# alpha/data/index/index.py
import asyncio
import asyncpg
import os
from datetime import datetime, timedelta
from typing import Optional
import logging
from dotenv import load_dotenv
import FinanceDataReader as fdr

import sys
from pathlib import Path

# Add parent directory to path for kr module
parent_dir = Path(__file__).parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketIndexCollector:
    def __init__(self, database_url):
        self.database_url = database_url

        # Define indices to collect
        self.indices = {
            'KOSPI': 'KS11',
            'KOSDAQ': 'KQ11',
            'NASDAQ': 'IXIC',
            'S&P500': 'US500',
            'DOW': 'DJI'
        }

    async def get_connection(self):
        return await asyncpg.connect(self.database_url)


    async def insert_market_index_batch(self, conn, records):
        """Insert batch of records into market_index table"""
        insert_query = """
        INSERT INTO market_index (
            exchange, close, change_amount, change_rate,
            open, high, low, volume, trading_value, date
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (exchange, date) DO UPDATE SET
            close = EXCLUDED.close,
            change_amount = EXCLUDED.change_amount,
            change_rate = EXCLUDED.change_rate,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            volume = EXCLUDED.volume,
            trading_value = EXCLUDED.trading_value
        """

        # Prepare batch data
        batch_data = []
        for data in records:
            batch_data.append((
                data['exchange'],
                data['close'],
                data['change_amount'],
                data['change_rate'],
                data['open'],
                data['high'],
                data['low'],
                data['volume'],
                data['trading_value'],
                data['date']
            ))

        await conn.executemany(insert_query, batch_data)
        logger.info(f"Batch inserted {len(records)} records")

    async def run_collection(self, target_date: str = None):
        """Execute market index data collection for current day"""
        if target_date is None:
            target_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"Starting market index data collection for {target_date}")

        # Use single connection for all operations
        conn = await self.get_connection()

        try:
            total_records = 0

            for name, symbol in self.indices.items():
                try:
                    logger.info(f"Collecting {name} data for {target_date}")

                    # Get data for target date plus previous day for change calculation
                    start_date = (datetime.strptime(target_date, '%Y%m%d') - timedelta(days=7)).strftime('%Y%m%d')
                    data = fdr.DataReader(symbol, start_date, target_date)

                    if data.empty:
                        logger.warning(f"No data available for {name} on {target_date}")
                        continue

                    # Get the latest day data (target date)
                    current_row = data.iloc[-1]

                    # Calculate change if previous data exists
                    change_amount = None
                    change_rate = None

                    if len(data) >= 2:
                        previous_row = data.iloc[-2]
                        change_amount = current_row['Close'] - previous_row['Close']
                        change_rate = (change_amount / previous_row['Close']) * 100
                        logger.info(f"{name} - Close: {current_row['Close']:.2f}, Change: {change_amount:+.2f} ({change_rate:+.2f}%)")
                    else:
                        logger.info(f"{name} - Close: {current_row['Close']:.2f}, No previous data for change calculation")

                    # Get trading value (Amount) if available
                    trading_value = None
                    if 'Amount' in current_row.index and current_row['Amount'] is not None:
                        try:
                            trading_value = int(current_row['Amount'])
                        except (ValueError, TypeError):
                            trading_value = None

                    record_data = {
                        'exchange': name,
                        'close': float(current_row['Close']),
                        'change_amount': float(change_amount) if change_amount is not None else None,
                        'change_rate': float(change_rate) if change_rate is not None else None,
                        'open': float(current_row['Open']),
                        'high': float(current_row['High']),
                        'low': float(current_row['Low']),
                        'volume': int(current_row['Volume']),
                        'trading_value': trading_value,
                        'date': datetime.strptime(target_date, '%Y%m%d').date()
                    }

                    # Insert single record for this index
                    await self.insert_market_index_batch(conn, [record_data])
                    logger.info(f"{name}: 1 record upserted successfully")
                    total_records += 1

                except Exception as e:
                    logger.error(f"Error processing {name}: {e}")

            logger.info(f"Market index data collection completed. {total_records} indices processed for {target_date}")

        finally:
            await conn.close()

    async def run_bulk_collection(self, start_date: str, end_date: str):
        """Execute market index data collection for date range (batch processing)"""
        logger.info(f"Starting bulk market index data collection from {start_date} to {end_date}")

        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')

        # Use single connection for all operations
        conn = await self.get_connection()

        try:
            total_records = 0
            total_indices = len(self.indices)

            for idx, (name, symbol) in enumerate(self.indices.items(), 1):
                try:
                    logger.info(f"[{idx}/{total_indices}] Collecting {name} bulk data from {start_date} to {end_date}")

                    # Get data for entire period at once
                    # Add buffer days for change calculation
                    buffer_start = (start_dt - timedelta(days=7)).strftime('%Y%m%d')
                    data = fdr.DataReader(symbol, buffer_start, end_date)

                    if data.empty:
                        logger.warning(f"No data available for {name} in period {start_date} to {end_date}")
                        continue

                    # Filter data for target period and prepare batch records
                    batch_records = []
                    processed_dates = 0

                    for date_idx in range(len(data)):
                        current_row = data.iloc[date_idx]
                        current_date = data.index[date_idx].date()

                        # Only process dates within target range
                        if current_date < start_dt.date() or current_date > end_dt.date():
                            continue

                        # Calculate change if previous data exists
                        change_amount = None
                        change_rate = None

                        if date_idx > 0:
                            previous_row = data.iloc[date_idx - 1]
                            change_amount = current_row['Close'] - previous_row['Close']
                            change_rate = (change_amount / previous_row['Close']) * 100

                        # Get trading value (Amount) if available
                        trading_value = None
                        if 'Amount' in current_row.index and current_row['Amount'] is not None:
                            try:
                                trading_value = int(current_row['Amount'])
                            except (ValueError, TypeError):
                                trading_value = None

                        record_data = {
                            'exchange': name,
                            'close': float(current_row['Close']),
                            'change_amount': float(change_amount) if change_amount is not None else None,
                            'change_rate': float(change_rate) if change_rate is not None else None,
                            'open': float(current_row['Open']),
                            'high': float(current_row['High']),
                            'low': float(current_row['Low']),
                            'volume': int(current_row['Volume']),
                            'trading_value': trading_value,
                            'date': current_date
                        }

                        batch_records.append(record_data)
                        processed_dates += 1

                    # Insert batch records for this index
                    if batch_records:
                        await self.insert_market_index_batch(conn, batch_records)
                        total_records += len(batch_records)
                        logger.info(f"[{idx}/{total_indices}] {name}: {len(batch_records)} records upserted successfully")

                        # Progress indicator
                        progress = (idx / total_indices) * 100
                        logger.info(f"Progress: {progress:.1f}% ({idx}/{total_indices} indices completed)")
                    else:
                        logger.warning(f"[{idx}/{total_indices}] {name}: No records to insert")

                except Exception as e:
                    logger.error(f"[{idx}/{total_indices}] Error processing {name}: {e}")
                    continue

            # Calculate period statistics
            period_days = (end_dt - start_dt).days + 1
            expected_records = total_indices * period_days
            success_rate = (total_records / expected_records * 100) if expected_records > 0 else 0

            logger.info(f"\n=== Bulk Collection Summary ===")
            logger.info(f"Period: {start_date} to {end_date} ({period_days} days)")
            logger.info(f"Indices processed: {total_indices}")
            logger.info(f"Total records inserted: {total_records:,}")
            logger.info(f"Expected records: {expected_records:,}")
            logger.info(f"Success rate: {success_rate:.1f}%")
            logger.info(f"=== Collection Completed ===")

        finally:
            await conn.close()

async def main():
    # Load environment variables
    load_dotenv(Path(__file__).parent.parent / '.env')

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    if database_url.startswith('postgresql+asyncpg://'):
        database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

    collector = MarketIndexCollector(database_url)

    # Bulk collect data from 2025-04-01 to 2025-09-25
    start_date = '20251011'
    end_date = '20251014'

    logger.info(f"🚀 Starting bulk market index collection: {start_date} ~ {end_date}")
    start_time = datetime.now()

    try:
        await collector.run_bulk_collection(start_date, end_date)

        end_time = datetime.now()
        duration = end_time - start_time

        logger.info(f"\n✅ Bulk collection completed successfully!")
        logger.info(f"⏱️  Total execution time: {duration}")
        logger.info(f"📅 Period covered: {start_date} to {end_date}")

    except Exception as e:
        logger.error(f"❌ Bulk collection failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())