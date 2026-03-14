"""
US FRED Macro Data Collector
============================
DXY, Credit Spread, VIX, Fed RRP 데이터 수집 (FRED API)
Note: MOVE Index는 FRED에서 제공하지 않아 us_move_index_collector.py 사용

FRED Series:
- DTWEXBGS: Trade Weighted Dollar Index (달러 인덱스)
- BAMLC0A0CM: Corporate Bond OAS (신용 스프레드)
- VIXCLS: CBOE VIX Index
- RRPONTSYD: Fed Reverse Repo (유동성 지표)

Usage:
    python us_fred_macro_collector.py              # 전체 수집 (2020-01-01 ~)
    python us_fred_macro_collector.py --update     # 최근 데이터만 업데이트
    python us_fred_macro_collector.py --series VIX # 특정 시리즈만 수집

Author: Claude Code
Date: 2025-12-07
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from pathlib import Path
import argparse

import asyncpg
from fredapi import Fred
from dotenv import load_dotenv

# ============================================================
# Configuration
# ============================================================

# .env 파일 로드 (C:\project\alpha\data\.env)
ENV_PATH = Path(__file__).parent.parent / '.env'
load_dotenv(ENV_PATH)

# API Keys
FRED_API_KEY = os.getenv('FRED_API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')

# FRED Series Configuration
# Note: MOVE Index는 FRED에서 제공하지 않음 (us_move_index_collector.py 사용)
FRED_SERIES_CONFIG = {
    'DXY': {
        'series_id': 'DTWEXBGS',
        'table_name': 'us_dollar_index',
        'name': 'Trade Weighted U.S. Dollar Index',
        'interval': 'daily',
        'unit': 'index',
        'description': '달러 인덱스 (주요 통화 대비)'
    },
    'CREDIT_SPREAD': {
        'series_id': 'BAMLC0A0CM',
        'table_name': 'us_credit_spread',
        'name': 'ICE BofA US Corporate Index OAS',
        'interval': 'daily',
        'unit': 'percent',
        'description': '회사채 스프레드 (신용 리스크)'
    },
    'VIX': {
        'series_id': 'VIXCLS',
        'table_name': 'us_vix',
        'name': 'CBOE Volatility Index',
        'interval': 'daily',
        'unit': 'index',
        'description': 'S&P 500 내재변동성'
    },
    'FED_RRP': {
        'series_id': 'RRPONTSYD',
        'table_name': 'us_fed_rrp',
        'name': 'Overnight Reverse Repurchase Agreements',
        'interval': 'daily',
        'unit': 'billions',
        'description': 'Fed 역레포 잔액 (유동성 지표)'
    },
    'GDP': {
        'series_id': 'A191RL1Q225SBEA',
        'table_name': 'us_gdp',
        'name': 'Real GDP Growth Rate (Quarterly)',
        'interval': 'quarterly',
        'unit': 'percent',
        'description': 'Real GDP 분기 성장률 (%)'
    },
    'PMI': {
        'series_id': 'IPMAN',
        'table_name': 'us_pmi',
        'name': 'Industrial Production: Manufacturing',
        'interval': 'monthly',
        'unit': 'index',
        'description': '제조업 생산지수 (2017=100 기준)'
    }
}

# Logging 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ============================================================
# Database Manager
# ============================================================

class DatabaseManager:
    """PostgreSQL 비동기 연결 관리"""

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.pool = None

    async def initialize(self):
        """커넥션 풀 초기화"""
        try:
            self.pool = await asyncpg.create_pool(
                self.db_url,
                min_size=1,
                max_size=5,
                command_timeout=60
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    async def close(self):
        """커넥션 풀 종료"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def execute(self, query: str, *args):
        """단일 쿼리 실행"""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args):
        """데이터 조회"""
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchval(self, query: str, *args):
        """단일 값 조회"""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def executemany(self, query: str, args_list: List):
        """다건 INSERT"""
        async with self.pool.acquire() as conn:
            await conn.executemany(query, args_list)


# ============================================================
# FRED Data Collector
# ============================================================

class FredMacroCollector:
    """FRED API를 통한 거시경제 데이터 수집"""

    def __init__(self, api_key: str, db_manager: DatabaseManager):
        self.fred = Fred(api_key=api_key)
        self.db = db_manager

    async def get_last_date(self, table_name: str) -> Optional[date]:
        """테이블의 마지막 데이터 날짜 조회"""
        query = f"SELECT MAX(date) FROM {table_name}"
        result = await self.db.fetchval(query)
        return result

    def fetch_fred_data(self, series_id: str, start_date: date, end_date: date = None) -> Dict[date, float]:
        """FRED에서 시계열 데이터 가져오기"""
        try:
            if end_date is None:
                end_date = date.today()

            logger.info(f"Fetching FRED series {series_id}: {start_date} ~ {end_date}")

            data = self.fred.get_series(
                series_id,
                observation_start=start_date,
                observation_end=end_date
            )

            # pandas Series를 Dict로 변환
            result = {}
            for idx, value in data.items():
                if value is not None and not (isinstance(value, float) and value != value):  # NaN 체크
                    result[idx.date()] = float(value)

            logger.info(f"  -> Fetched {len(result)} records")
            return result

        except Exception as e:
            logger.error(f"FRED fetch error for {series_id}: {e}")
            return {}

    async def upsert_data(self, table_name: str, config: Dict, data: Dict[date, float]):
        """데이터 UPSERT (INSERT ON CONFLICT UPDATE)"""
        if not data:
            logger.warning(f"No data to insert for {table_name}")
            return 0

        query = f"""
        INSERT INTO {table_name} (name, interval, unit, date, value)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (date) DO UPDATE SET
            value = EXCLUDED.value,
            created_at = CURRENT_TIMESTAMP
        """

        records = [
            (config['name'], config['interval'], config['unit'], d, v)
            for d, v in sorted(data.items())
        ]

        try:
            await self.db.executemany(query, records)
            logger.info(f"  -> Inserted/Updated {len(records)} records to {table_name}")
            return len(records)
        except Exception as e:
            logger.error(f"Database insert error: {e}")
            return 0

    async def collect_series(self, series_key: str, start_date: date = None, full_refresh: bool = False):
        """특정 시리즈 수집"""
        if series_key not in FRED_SERIES_CONFIG:
            logger.error(f"Unknown series key: {series_key}")
            return 0

        config = FRED_SERIES_CONFIG[series_key]
        table_name = config['table_name']

        # 시작 날짜 결정
        if full_refresh or start_date:
            fetch_start = start_date or date(2020, 1, 1)
        else:
            # 마지막 데이터 다음날부터
            last_date = await self.get_last_date(table_name)
            if last_date:
                fetch_start = last_date + timedelta(days=1)
                logger.info(f"{series_key}: Last data date = {last_date}, fetching from {fetch_start}")
            else:
                fetch_start = date(2020, 1, 1)
                logger.info(f"{series_key}: No existing data, fetching from {fetch_start}")

        # 오늘보다 미래면 스킵
        if fetch_start > date.today():
            logger.info(f"{series_key}: Already up to date")
            return 0

        # FRED에서 데이터 가져오기
        data = self.fetch_fred_data(config['series_id'], fetch_start)

        # DB에 저장
        count = await self.upsert_data(table_name, config, data)

        return count

    async def collect_all(self, start_date: date = None, full_refresh: bool = False):
        """모든 시리즈 수집"""
        total_count = 0

        for series_key in FRED_SERIES_CONFIG.keys():
            logger.info(f"\n{'='*50}")
            logger.info(f"Collecting {series_key}...")
            logger.info(f"{'='*50}")

            count = await self.collect_series(series_key, start_date, full_refresh)
            total_count += count

        return total_count


# ============================================================
# Main Entry Point
# ============================================================

async def main():
    """메인 실행 함수"""

    parser = argparse.ArgumentParser(description='US FRED Macro Data Collector')
    parser.add_argument('--update', action='store_true', help='Update only (from last date)')
    parser.add_argument('--full', action='store_true', help='Full refresh from 2020-01-01')
    parser.add_argument('--series', type=str, help='Specific series to collect (MOVE, DXY, CREDIT_SPREAD, VIX, FED_RRP)')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)')
    args = parser.parse_args()

    # Validation
    if not FRED_API_KEY:
        logger.error("FRED_API_KEY not found in .env file")
        sys.exit(1)

    if not DATABASE_URL:
        logger.error("DATABASE_URL not found in .env file")
        sys.exit(1)

    # Parse start date
    start_date = None
    if args.start:
        try:
            start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid date format: {args.start}. Use YYYY-MM-DD")
            sys.exit(1)

    # Initialize
    db = DatabaseManager(DATABASE_URL)
    await db.initialize()

    collector = FredMacroCollector(FRED_API_KEY, db)

    try:
        logger.info("\n" + "="*60)
        logger.info("US FRED Macro Data Collector")
        logger.info("="*60)
        logger.info(f"Mode: {'Full Refresh' if args.full else 'Update'}")
        if args.series:
            logger.info(f"Series: {args.series}")
        else:
            logger.info(f"Series: ALL ({', '.join(FRED_SERIES_CONFIG.keys())})")
        logger.info("="*60 + "\n")

        if args.series:
            # 특정 시리즈만 수집
            series_key = args.series.upper()
            if series_key not in FRED_SERIES_CONFIG:
                logger.error(f"Unknown series: {series_key}")
                logger.info(f"Available: {', '.join(FRED_SERIES_CONFIG.keys())}")
                sys.exit(1)

            count = await collector.collect_series(series_key, start_date, args.full)
        else:
            # 전체 수집
            count = await collector.collect_all(start_date, args.full)

        logger.info("\n" + "="*60)
        logger.info(f"Collection Complete! Total records: {count}")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"Collection failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        await db.close()


# ============================================================
# Entry Point
# ============================================================

if __name__ == '__main__':
    # Windows 이벤트 루프 호환성
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
