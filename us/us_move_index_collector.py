"""
US MOVE Index Data Collector
============================
Yahoo Finance에서 MOVE Index (^MOVE) 데이터 수집
FRED에서 MOVE Index를 제공하지 않아 별도 수집

Usage:
    python us_move_index_collector.py              # 전체 수집 (2020-01-01 ~)
    python us_move_index_collector.py --update     # 최근 데이터만 업데이트

Author: Claude Code
Date: 2025-12-07
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import Dict, Optional
from pathlib import Path
import argparse

import asyncpg
import yfinance as yf
from dotenv import load_dotenv

# ============================================================
# Configuration
# ============================================================

# .env 파일 로드 (C:\project\alpha\data\.env)
ENV_PATH = Path(__file__).parent.parent / '.env'
load_dotenv(ENV_PATH)

DATABASE_URL = os.getenv('DATABASE_URL')

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

    async def fetchval(self, query: str, *args):
        """단일 값 조회"""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def executemany(self, query: str, args_list):
        """다건 INSERT"""
        async with self.pool.acquire() as conn:
            await conn.executemany(query, args_list)


# ============================================================
# MOVE Index Collector
# ============================================================

class MoveIndexCollector:
    """Yahoo Finance에서 MOVE Index 수집"""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.ticker = "^MOVE"
        self.table_name = "us_move_index"
        self.config = {
            'name': 'ICE BofA MOVE Index',
            'interval': 'daily',
            'unit': 'index'
        }

    async def get_last_date(self) -> Optional[date]:
        """테이블의 마지막 데이터 날짜 조회"""
        query = f"SELECT MAX(date) FROM {self.table_name}"
        result = await self.db.fetchval(query)
        return result

    def fetch_move_data(self, start_date: date, end_date: date = None) -> Dict[date, float]:
        """Yahoo Finance에서 MOVE Index 데이터 가져오기"""
        try:
            if end_date is None:
                end_date = date.today()

            logger.info(f"Fetching MOVE Index from Yahoo Finance: {start_date} ~ {end_date}")

            # yfinance로 데이터 가져오기
            move = yf.Ticker(self.ticker)

            # 히스토리 데이터 (Close 가격 = MOVE Index 값)
            hist = move.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=(end_date + timedelta(days=1)).strftime('%Y-%m-%d')  # end는 exclusive
            )

            if hist.empty:
                logger.warning("No data returned from Yahoo Finance")
                return {}

            # DataFrame을 Dict로 변환
            result = {}
            for idx, row in hist.iterrows():
                dt = idx.date()
                value = row['Close']
                if value is not None and not (isinstance(value, float) and value != value):  # NaN 체크
                    result[dt] = float(value)

            logger.info(f"  -> Fetched {len(result)} records")
            return result

        except Exception as e:
            logger.error(f"Yahoo Finance fetch error: {e}")
            return {}

    async def upsert_data(self, data: Dict[date, float]) -> int:
        """데이터 UPSERT (INSERT ON CONFLICT UPDATE)"""
        if not data:
            logger.warning(f"No data to insert for {self.table_name}")
            return 0

        query = f"""
        INSERT INTO {self.table_name} (name, interval, unit, date, value)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (date) DO UPDATE SET
            value = EXCLUDED.value,
            created_at = CURRENT_TIMESTAMP
        """

        records = [
            (self.config['name'], self.config['interval'], self.config['unit'], d, v)
            for d, v in sorted(data.items())
        ]

        try:
            await self.db.executemany(query, records)
            logger.info(f"  -> Inserted/Updated {len(records)} records to {self.table_name}")
            return len(records)
        except Exception as e:
            logger.error(f"Database insert error: {e}")
            return 0

    async def collect(self, start_date: date = None, full_refresh: bool = False) -> int:
        """MOVE Index 수집"""

        # 시작 날짜 결정
        if full_refresh or start_date:
            fetch_start = start_date or date(2020, 1, 1)
        else:
            # 마지막 데이터 다음날부터
            last_date = await self.get_last_date()
            if last_date:
                fetch_start = last_date + timedelta(days=1)
                logger.info(f"MOVE Index: Last data date = {last_date}, fetching from {fetch_start}")
            else:
                fetch_start = date(2020, 1, 1)
                logger.info(f"MOVE Index: No existing data, fetching from {fetch_start}")

        # 오늘보다 미래면 스킵
        if fetch_start > date.today():
            logger.info("MOVE Index: Already up to date")
            return 0

        # Yahoo Finance에서 데이터 가져오기
        data = self.fetch_move_data(fetch_start)

        # DB에 저장
        count = await self.upsert_data(data)

        return count


# ============================================================
# Main Entry Point
# ============================================================

async def main():
    """메인 실행 함수"""

    parser = argparse.ArgumentParser(description='US MOVE Index Collector (Yahoo Finance)')
    parser.add_argument('--update', action='store_true', help='Update only (from last date)')
    parser.add_argument('--full', action='store_true', help='Full refresh from 2020-01-01')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)')
    args = parser.parse_args()

    # Validation
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

    collector = MoveIndexCollector(db)

    try:
        logger.info("\n" + "="*60)
        logger.info("US MOVE Index Collector (Yahoo Finance)")
        logger.info("="*60)
        logger.info(f"Mode: {'Full Refresh' if args.full else 'Update'}")
        logger.info("="*60 + "\n")

        count = await collector.collect(start_date, args.full)

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
