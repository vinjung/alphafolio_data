# alpha/data/kr/krx_index.py
import requests
import asyncio
import asyncpg
import csv
import io
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# KRX API Settings
KRX_MAIN_URL = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201"
KRX_GENERATE_URL = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
KRX_DOWNLOAD_URL = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"

KRX_HEADERS = {
    'Accept': 'text/plain, */*; q=0.01',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36',
    'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
    'Origin': 'https://data.krx.co.kr',
    'X-Requested-With': 'XMLHttpRequest',
}

# Index type configurations
INDEX_TYPES = {
    'kospi': {
        'name': 'KOSPI Index',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00101',
        'payload_key': 'idxIndMidclssCd',
        'payload_value': '02',
        'has_volume': True
    },
    'kosdaq': {
        'name': 'KOSDAQ Index',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00101',
        'payload_key': 'idxIndMidclssCd',
        'payload_value': '03',
        'has_volume': True
    },
    'futures': {
        'name': 'KOSPI Futures Index',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01001',
        'payload_key': 'clssCd',
        'payload_value': '0201',
        'has_volume': False
    },
    'volatility': {
        'name': 'Volatility Index (VKOSPI)',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01001',
        'payload_key': 'clssCd',
        'payload_value': '0202',
        'has_volume': False
    },
    'gold': {
        'name': 'Gold Spot Index',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01001',
        'payload_key': 'clssCd',
        'payload_value': '0600',
        'has_volume': False
    }
}


class KrxIndexApiClient:
    """KRX Index API Client"""

    def __init__(self):
        self.session = None

    def create_session(self) -> Optional[requests.Session]:
        """Create KRX session"""
        try:
            session = requests.Session()
            session.headers.update(KRX_HEADERS)
            session.get(KRX_MAIN_URL)

            self.session = session
            logger.info("KRX Index session initialized successfully")
            return session

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create KRX session: {e}")
            return None

    def get_session(self) -> Optional[requests.Session]:
        """Return current session, create new one if none exists"""
        if self.session is None:
            return self.create_session()
        return self.session

    def fetch_data(self, payload: Dict[str, Any]) -> Optional[str]:
        """Download CSV data from KRX"""
        session = self.get_session()
        if not session:
            logger.error("No valid session available")
            return None

        try:
            # Step 1: Generate OTP
            logger.info(f"Requesting download code for {payload.get('url', 'unknown')}")
            generate_res = session.post(KRX_GENERATE_URL, data=payload)
            generate_res.raise_for_status()
            download_code = generate_res.text.strip()

            if not download_code:
                logger.error("Empty download code received")
                return None

            logger.debug(f"Download code received: {download_code[:10]}...")

            # Step 2: Download CSV data
            download_params = {'code': download_code}
            logger.info("Downloading CSV data")
            data_res = session.get(KRX_DOWNLOAD_URL, params=download_params)
            data_res.raise_for_status()

            if not data_res.content:
                logger.error("Empty data received")
                return None

            logger.info("CSV data downloaded successfully")

            # Handle encoding
            try:
                return data_res.content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    return data_res.content.decode('cp949')
                except UnicodeDecodeError:
                    logger.error("Failed to decode response content")
                    return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch KRX data: {e}")
            return None

    def get_kospi_index_data(self, trade_date: str) -> Optional[str]:
        """Fetch KOSPI related index data"""
        payload = {
            'locale': 'ko_KR',
            'idxIndMidclssCd': '02',
            'trdDd': trade_date,
            'share': '2',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT00101'
        }
        logger.info(f"Fetching KOSPI index data for {trade_date}")
        return self.fetch_data(payload)

    def get_kosdaq_index_data(self, trade_date: str) -> Optional[str]:
        """Fetch KOSDAQ related index data"""
        payload = {
            'locale': 'ko_KR',
            'idxIndMidclssCd': '03',
            'trdDd': trade_date,
            'share': '2',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT00101'
        }
        logger.info(f"Fetching KOSDAQ index data for {trade_date}")
        return self.fetch_data(payload)

    def get_futures_index_data(self, trade_date: str) -> Optional[str]:
        """Fetch KOSPI Futures index data"""
        payload = {
            'locale': 'ko_KR',
            'clssCd': '0201',
            'trdDd': trade_date,
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01001'
        }
        logger.info(f"Fetching futures index data for {trade_date}")
        return self.fetch_data(payload)

    def get_volatility_index_data(self, trade_date: str) -> Optional[str]:
        """Fetch Volatility index (VKOSPI) data"""
        payload = {
            'locale': 'ko_KR',
            'clssCd': '0202',
            'trdDd': trade_date,
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01001'
        }
        logger.info(f"Fetching volatility index data for {trade_date}")
        return self.fetch_data(payload)

    def get_gold_index_data(self, trade_date: str) -> Optional[str]:
        """Fetch Gold spot index data"""
        payload = {
            'locale': 'ko_KR',
            'clssCd': '0600',
            'trdDd': trade_date,
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01001'
        }
        logger.info(f"Fetching gold index data for {trade_date}")
        return self.fetch_data(payload)

    def close_session(self):
        """Close session"""
        if self.session:
            self.session.close()
            self.session = None
            logger.info("KRX Index session closed")


class KrxIndexCollector:
    """KRX Index Data Collector with PostgreSQL storage"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.krx_client = KrxIndexApiClient()

    async def get_connection(self):
        """Get database connection"""
        database_url = self.database_url
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

        return await asyncpg.connect(
            database_url,
            command_timeout=60,
            server_settings={
                'application_name': 'krx_index_collector',
                'tcp_keepalives_idle': '600',
                'tcp_keepalives_interval': '30',
                'tcp_keepalives_count': '3'
            }
        )

    def parse_csv_with_volume(self, csv_data: str, index_type: str, trade_date: str) -> List[tuple]:
        """Parse CSV data for KOSPI/KOSDAQ indexes (with volume columns)"""
        data_to_insert = []

        try:
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            trade_date_obj = datetime.strptime(trade_date, '%Y%m%d').date()

            for row in csv_reader:
                index_name = row.get('지수명', '').strip()
                if not index_name:
                    continue

                def safe_decimal(value, precision=2):
                    if not value or value == '-' or value == '':
                        return None
                    try:
                        return round(float(str(value).replace(',', '')), precision)
                    except:
                        return None

                def safe_bigint(value):
                    if not value or value == '-' or value == '':
                        return None
                    try:
                        return int(str(value).replace(',', ''))
                    except:
                        return None

                processed_row = (
                    trade_date_obj,
                    index_type,
                    index_name,
                    safe_decimal(row.get('종가'), 4),
                    safe_decimal(row.get('대비'), 4),
                    safe_decimal(row.get('등락률'), 4),
                    safe_decimal(row.get('시가'), 4),
                    safe_decimal(row.get('고가'), 4),
                    safe_decimal(row.get('저가'), 4),
                    safe_bigint(row.get('거래량')),
                    safe_bigint(row.get('거래대금')),
                    safe_bigint(row.get('상장시가총액'))
                )
                data_to_insert.append(processed_row)

        except Exception as e:
            logger.error(f"Failed to parse CSV data for {index_type}: {e}")

        return data_to_insert

    def parse_csv_without_volume(self, csv_data: str, index_type: str, trade_date: str) -> List[tuple]:
        """Parse CSV data for Futures/Volatility/Gold indexes (without volume columns)"""
        data_to_insert = []

        try:
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            trade_date_obj = datetime.strptime(trade_date, '%Y%m%d').date()

            for row in csv_reader:
                index_name = row.get('지수명', '').strip()
                if not index_name:
                    continue

                def safe_decimal(value, precision=2):
                    if not value or value == '-' or value == '':
                        return None
                    try:
                        return round(float(str(value).replace(',', '')), precision)
                    except:
                        return None

                processed_row = (
                    trade_date_obj,
                    index_type,
                    index_name,
                    safe_decimal(row.get('종가'), 4),
                    safe_decimal(row.get('대비'), 4),
                    safe_decimal(row.get('등락률'), 4),
                    safe_decimal(row.get('시가'), 4),
                    safe_decimal(row.get('고가'), 4),
                    safe_decimal(row.get('저가'), 4),
                    None,  # volume
                    None,  # trading_value
                    None   # market_cap
                )
                data_to_insert.append(processed_row)

        except Exception as e:
            logger.error(f"Failed to parse CSV data for {index_type}: {e}")

        return data_to_insert

    async def insert_index_data(self, data_list: List[tuple]):
        """Insert index data into PostgreSQL"""
        if not data_list:
            return

        conn = await self.get_connection()
        try:
            insert_query = """
            INSERT INTO kr_benchmark_index (
                date, index_category, index_name, close, change_amount, change_rate,
                open, high, low, volume, trading_value, market_cap
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (index_name, date) DO UPDATE SET
                close = EXCLUDED.close,
                change_amount = EXCLUDED.change_amount,
                change_rate = EXCLUDED.change_rate,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                volume = EXCLUDED.volume,
                trading_value = EXCLUDED.trading_value,
                market_cap = EXCLUDED.market_cap
            """

            await conn.executemany(insert_query, data_list)
            logger.info(f"Inserted/Updated {len(data_list)} index records")

        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            raise
        finally:
            if not conn.is_closed():
                await conn.close()

    async def collect_single_date(self, trade_date: str):
        """Collect all index data for a single date"""
        logger.info(f"Collecting index data for {trade_date}")
        all_data = []

        # 1. KOSPI Index
        csv_data = self.krx_client.get_kospi_index_data(trade_date)
        if csv_data:
            data = self.parse_csv_with_volume(csv_data, 'kospi', trade_date)
            all_data.extend(data)
            logger.info(f"  KOSPI: {len(data)} records")
        else:
            logger.warning(f"  KOSPI: No data")

        # 2. KOSDAQ Index
        csv_data = self.krx_client.get_kosdaq_index_data(trade_date)
        if csv_data:
            data = self.parse_csv_with_volume(csv_data, 'kosdaq', trade_date)
            all_data.extend(data)
            logger.info(f"  KOSDAQ: {len(data)} records")
        else:
            logger.warning(f"  KOSDAQ: No data")

        # 3. Futures Index
        csv_data = self.krx_client.get_futures_index_data(trade_date)
        if csv_data:
            data = self.parse_csv_without_volume(csv_data, 'futures', trade_date)
            all_data.extend(data)
            logger.info(f"  Futures: {len(data)} records")
        else:
            logger.warning(f"  Futures: No data")

        # 4. Volatility Index (VKOSPI)
        csv_data = self.krx_client.get_volatility_index_data(trade_date)
        if csv_data:
            data = self.parse_csv_without_volume(csv_data, 'volatility', trade_date)
            all_data.extend(data)
            logger.info(f"  Volatility: {len(data)} records")
        else:
            logger.warning(f"  Volatility: No data")

        # 5. Gold Index
        csv_data = self.krx_client.get_gold_index_data(trade_date)
        if csv_data:
            data = self.parse_csv_without_volume(csv_data, 'gold', trade_date)
            all_data.extend(data)
            logger.info(f"  Gold: {len(data)} records")
        else:
            logger.warning(f"  Gold: No data")

        # Insert all data
        if all_data:
            await self.insert_index_data(all_data)
            logger.info(f"Total: {len(all_data)} records for {trade_date}")
        else:
            logger.warning(f"No data collected for {trade_date}")

        return len(all_data)

    async def collect_date_range(self, start_date: str, end_date: str):
        """Collect index data for a date range"""
        logger.info(f"Starting index data collection from {start_date} to {end_date}")

        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')

        total_records = 0
        total_days = 0
        current = start

        while current <= end:
            trade_date = current.strftime('%Y%m%d')

            # Skip weekends (Saturday=5, Sunday=6)
            if current.weekday() < 5:
                try:
                    records = await self.collect_single_date(trade_date)
                    total_records += records
                    total_days += 1

                    # Rate limiting: wait 1 second between requests to avoid KRX rate limit
                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"Error collecting data for {trade_date}: {e}")

            current += timedelta(days=1)

        logger.info(f"Collection completed: {total_records} records for {total_days} trading days")
        return total_records

    async def run_collection(self, start_date: str, end_date: str):
        """Run the complete collection process"""
        try:
            await self.collect_date_range(start_date, end_date)
        finally:
            self.krx_client.close_session()


async def main():
    """Main entry point for standalone execution"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    if database_url.startswith('postgresql+asyncpg://'):
        database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

    # 실행 당일 날짜만 수집
    today = datetime.now().strftime('%Y%m%d')

    collector = KrxIndexCollector(database_url)
    await collector.run_collection(today, today)


if __name__ == "__main__":
    asyncio.run(main())
