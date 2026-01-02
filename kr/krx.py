# alpha/data/kr/krx.py
import requests
import asyncio
import asyncpg
import csv
import io
import os
import json
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List
import logging
import time

try:
    from .gcs_handler import get_gcs_handler
except ImportError:
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent))
    from gcs_handler import get_gcs_handler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# KRX API 설정
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

class KrxApiClient:
    def __init__(self):
        self.session = None

    def create_session(self) -> Optional[requests.Session]:
        """KRX 세션 생성"""
        try:
            session = requests.Session()
            session.headers.update(KRX_HEADERS)
            session.get(KRX_MAIN_URL)

            self.session = session
            logger.info("KRX session initialized successfully")
            return session

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create KRX session: {e}")
            return None

    def get_session(self) -> Optional[requests.Session]:
        """현재 세션 반환, 없으면 새로 생성"""
        if self.session is None:
            return self.create_session()
        return self.session

    def fetch_data(self, payload: Dict[str, Any]) -> Optional[str]:
        """KRX에서 CSV 데이터 다운로드"""
        session = self.get_session()
        if not session:
            logger.error("No valid session available")
            return None

        try:
            # 1단계: OTP 생성
            logger.info(f"Requesting download code for {payload.get('url', 'unknown')}")
            generate_res = session.post(KRX_GENERATE_URL, data=payload)
            generate_res.raise_for_status()
            download_code = generate_res.text.strip()

            if not download_code:
                logger.error("Empty download code received")
                return None

            logger.debug(f"Download code received: {download_code[:10]}...")

            # 2단계: CSV 데이터 다운로드
            download_params = {'code': download_code}
            logger.info("Downloading CSV data")
            data_res = session.get(KRX_DOWNLOAD_URL, params=download_params)
            data_res.raise_for_status()

            if not data_res.content:
                logger.error("Empty data received")
                return None

            logger.info("CSV data downloaded successfully")

            # 인코딩 처리
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

    def get_intraday_data(self, trade_date: str = None) -> Optional[str]:
        """장중 데이터 조회"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        payload = {
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'trdDd': trade_date,
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01501'
        }

        logger.info(f"Fetching intraday data for {trade_date}")
        return self.fetch_data(payload)

    def get_daily_stats(self, trade_date: str = None) -> Optional[str]:
        """일별 통계 데이터 조회"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        payload = {
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'trdDd': trade_date,
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01602'
        }

        logger.info(f"Fetching daily stats for {trade_date}")
        return self.fetch_data(payload)

    def get_market_data(self, market_type: str = 'ALL', trade_date: str = None) -> Optional[str]:
        """시장별 데이터 조회"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        payload = {
            'locale': 'ko_KR',
            'mktId': market_type,
            'trdDd': trade_date,
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01501'
        }

        logger.info(f"Fetching {market_type} market data for {trade_date}")
        return self.fetch_data(payload)

    def get_sector_data(self, trade_date: str = None) -> Optional[str]:
        """업종별 데이터 조회"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        payload = {
            'locale': 'ko_KR',
            'trdDd': trade_date,
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02001'
        }

        logger.info(f"Fetching sector data for {trade_date}")
        return self.fetch_data(payload)

    def get_intraday_detail_data(self, trade_date: str = None) -> Optional[str]:
        """장중 상세 데이터 조회 (EPS, PER, BPS, PBR, DPS, 배당수익률 포함)"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        # 7일 전 날짜 계산 (시작일)
        trade_date_obj = datetime.strptime(trade_date, '%Y%m%d')
        start_date = trade_date

        payload = {
            'locale': 'ko_KR',
            'searchType': '1',
            'mktId': 'ALL',
            'trdDd': trade_date,
            'tboxisuCd_finder_stkisu0_0': '005930/삼성전자',
            'isuCd': 'KR7005930003',
            'isuCd2': 'KR7005930003',
            'codeNmisuCd_finder_stkisu0_0': '삼성전자',
            'param1isuCd_finder_stkisu0_0': 'ALL',
            'strtDd': start_date,
            'endDd': trade_date,
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
        }

        logger.info(f"Fetching intraday detail data for {trade_date}")
        return self.fetch_data(payload)

    def get_investor_daily_trading_data(self, trade_date: str = None) -> Optional[str]:
        """투자자별 일별 거래 데이터 조회"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        # 7일 전 날짜 계산 (시작일)
        trade_date_obj = datetime.strptime(trade_date, '%Y%m%d')
        start_date = trade_date

        payload = {
            'locale': 'ko_KR',
            'inqTpCd': '1',
            'trdVolVal': '2',
            'askBid': '3',
            'mktId': 'ALL',
            'strtDd': start_date,
            'endDd': trade_date,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02201'
        }

        logger.info(f"Fetching investor daily trading data for {trade_date}")
        return self.fetch_data(payload)

    def get_program_daily_trading_data(self, trade_date: str = None) -> Optional[str]:
        """프로그램 일별 거래 데이터 조회"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        # 7일 전 날짜 계산 (시작일)
        trade_date_obj = datetime.strptime(trade_date, '%Y%m%d')
        start_date = trade_date

        payload = {
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'strtDd': start_date,
            'endDd': trade_date,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02601'
        }

        logger.info(f"Fetching program daily trading data for {trade_date}")
        return self.fetch_data(payload)

    def get_blocktrades_data(self, trade_date: str = None) -> Optional[str]:
        """대량 거래 데이터 조회"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        payload = {
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'share': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02501'
        }

        logger.info(f"Fetching block trades data for {trade_date}")
        return self.fetch_data(payload)

    def get_foreign_ownership_data(self, trade_date: str = None) -> Optional[str]:
        """외국인 지분율 데이터 조회"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        # 7일 전 날짜 계산 (시작일)
        trade_date_obj = datetime.strptime(trade_date, '%Y%m%d')
        start_date = trade_date

        payload = {
            'locale': 'ko_KR',
            'searchType': '1',
            'mktId': 'ALL',
            'trdDd': trade_date,
            'tboxisuCd_finder_stkisu0_4': '005930/삼성전자',
            'isuCd': 'KR7005930003',
            'isuCd2': 'KR7005930003',
            'codeNmisuCd_finder_stkisu0_4': '삼성전자',
            'param1isuCd_finder_stkisu0_4': 'ALL',
            'strtDd': start_date,
            'endDd': trade_date,
            'share': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03701'
        }

        logger.info(f"Fetching foreign ownership data for {trade_date}")
        return self.fetch_data(payload)

    def get_stock_basic_data(self) -> Optional[str]:
        """주식 기본정보 데이터 조회"""
        payload = {
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'share': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01901'
        }

        logger.info("Fetching stock basic data")
        return self.fetch_data(payload)

    def get_stock_detail_data(self) -> Optional[str]:
        """주식 상세정보 데이터 조회"""
        payload = {
            'locale': 'ko_KR',
            'mktTpCd': '0',
            'tboxisuSrtCd_finder_listisu0_6': '전체',
            'isuSrtCd': 'ALL',
            'isuSrtCd2': 'ALL',
            'codeNmisuSrtCd_finder_listisu0_6': '',
            'param1isuSrtCd_finder_listisu0_6': '',
            'sortType': 'A',
            'stdIndCd': 'ALL',
            'sectTpCd': 'ALL',
            'parval': 'ALL',
            'mktcap': 'ALL',
            'acntclsMm': 'ALL',
            'tboxmktpartcNo_finder_designadvser0_6': '',
            'mktpartcNo': '',
            'mktpartcNo2': '',
            'codeNmmktpartcNo_finder_designadvser0_6': '',
            'param1mktpartcNo_finder_designadvser0_6': '',
            'condListShrs': '1',
            'listshrs': '',
            'listshrs2': '',
            'condCap': '1',
            'cap': '',
            'cap2': '',
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03402'
        }

        logger.info("Fetching stock detail data")
        return self.fetch_data(payload)

    def get_individual_investor_daily_trading_data(self, symbol_info: dict, trade_date: str = None) -> Optional[str]:
        """개별종목 투자자별 일별 거래 데이터 조회"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        # 7일 전 날짜 계산 (시작일)
        trade_date_obj = datetime.strptime(trade_date, '%Y%m%d')
        start_date = trade_date

        payload = {
            'locale': 'ko_KR',
            'inqTpCd': '1',
            'trdVolVal': '2',
            'askBid': '3',
            'tboxisuCd_finder_stkisu0_8': f"{symbol_info['symbol']}/{symbol_info['stock_name']}",
            'isuCd': symbol_info['standard_symbol'],
            'isuCd2': symbol_info['standard_symbol'],
            'codeNmisuCd_finder_stkisu0_8': symbol_info['stock_name'],
            'param1isuCd_finder_stkisu0_8': 'ALL',
            'strtDd': start_date,
            'endDd': trade_date,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02301'
        }

        logger.info(f"Fetching individual investor daily trading data for {symbol_info['symbol']} for {trade_date}")
        return self.fetch_data(payload)

    def close_session(self):
        """세션 종료"""
        if self.session:
            self.session.close()
            self.session = None
            logger.info("KRX session closed")


class KrxCompleteCollector:
    def __init__(self, database_url):
        self.database_url = database_url
        self.gcs_handler = get_gcs_handler()
        self.krx_client = KrxApiClient()
        self.pool = None
        self.semaphore = None

    async def get_connection(self):
        return await asyncpg.connect(
            self.database_url,
            command_timeout=60,
            server_settings={
                'application_name': 'krx_data_collector',
                'tcp_keepalives_idle': '600',
                'tcp_keepalives_interval': '30',
                'tcp_keepalives_count': '3'
            }
        )

    async def init_pool_for_individual_investor(self, min_size=10, max_size=20):
        """Initialize DB connection pool for individual investor data collection"""
        if self.pool is None:
            database_url = self.database_url
            if database_url.startswith('postgresql+asyncpg://'):
                database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

            self.pool = await asyncpg.create_pool(
                database_url,
                min_size=min_size,
                max_size=max_size,
                command_timeout=60,
                server_settings={
                    'application_name': 'krx_individual_investor_collector',
                    'tcp_keepalives_idle': '600',
                    'tcp_keepalives_interval': '30',
                    'tcp_keepalives_count': '3'
                }
            )
            self.semaphore = asyncio.Semaphore(15)  # Limit concurrent operations to 15 (KRX-friendly)
            logger.info(f"Database pool initialized with min_size={min_size}, max_size={max_size}, semaphore=15")

    async def get_stock_symbols(self):
        """주식 종목 목록을 조회하는 메소드"""
        conn = await self.get_connection()
        try:
            query = """
            SELECT symbol, stock_name, standard_symbol
            FROM kr_stock_basic
            ORDER BY symbol
            """
            rows = await conn.fetch(query)
            symbols = [{'symbol': row['symbol'], 'stock_name': row['stock_name'], 'standard_symbol': row['standard_symbol']} for row in rows]
            return symbols
        except Exception as e:
            logger.error(f"Failed to get stock symbols: {e}")
            return []
        finally:
            await conn.close()

    def collect_intraday_data(self):
        """KRX 장중 데이터 수집 및 GCS 업로드"""
        csv_data = self.krx_client.get_intraday_data()
        if not csv_data:
            logger.error("Failed to fetch intraday data from KRX")
            return False

        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        file_name = f"kr_intraday_{timestamp}.csv"

        return self.gcs_handler.upload_file(file_name, csv_data, "daily")

    async def process_intraday_data(self):
        """GCS에서 최신 장중 데이터를 읽어서 PostgreSQL에 저장"""
        file_name, csv_data = self.gcs_handler.get_latest_file("daily/kr_intraday_")
        if not csv_data:
            logger.error("No data to process")
            return

        try:
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            data_to_insert = []

            for row in csv_reader:
                symbol = row.get('종목코드', '').strip()

                # Skip KONEX exchange data (대소문자 구분 없이)
                exchange = row.get('시장구분', '').strip()
                if 'KONEX' in exchange.upper():
                    continue

                # Only include KOSPI, KOSDAQ, KOSDAQ GLOBAL
                if exchange not in ['KOSPI', 'KOSDAQ', 'KOSDAQ GLOBAL']:
                    continue

                def safe_decimal(value):
                    if not value or value == '-':
                        return None
                    try:
                        return float(str(value).replace(',', ''))
                    except:
                        return None

                def safe_int(value):
                    if not value or value == '-':
                        return None
                    try:
                        return int(str(value).replace(',', ''))
                    except:
                        return None

                processed_row = (
                    row.get('종목코드', '')[:10],
                    row.get('종목명', '')[:200],
                    exchange[:20],
                    safe_decimal(row.get('종가')),
                    safe_decimal(row.get('대비')),
                    safe_decimal(row.get('등락률')),
                    safe_decimal(row.get('시가')),
                    safe_decimal(row.get('고가')),
                    safe_decimal(row.get('저가')),
                    safe_int(row.get('거래량')),
                    safe_int(row.get('거래대금')),
                    safe_decimal(row.get('시가총액')),
                    safe_int(row.get('상장주식수')),
                    datetime.now(),
                    datetime.now()
                )
                data_to_insert.append(processed_row)

            await self.insert_intraday_data(data_to_insert)
            logger.info(f"Processed {len(data_to_insert)} intraday records from {file_name}")

        except Exception as e:
            logger.error(f"Failed to process intraday data: {e}")

    async def insert_intraday_data(self, data_list):
        """장중 데이터를 PostgreSQL에 삽입"""
        if not data_list:
            return

        conn = await self.get_connection()
        try:
            insert_query = """
            INSERT INTO kr_intraday (
                symbol, stock_name, exchange, close, change_amount, change_rate,
                open, high, low, volume, trading_value, market_cap, listed_shares,
                created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (symbol) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                exchange = EXCLUDED.exchange,
                close = EXCLUDED.close,
                change_amount = EXCLUDED.change_amount,
                change_rate = EXCLUDED.change_rate,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                volume = EXCLUDED.volume,
                trading_value = EXCLUDED.trading_value,
                market_cap = EXCLUDED.market_cap,
                listed_shares = EXCLUDED.listed_shares,
                updated_at = EXCLUDED.updated_at
            """

            await conn.executemany(insert_query, data_list)
            logger.info(f"Inserted {len(data_list)} intraday records")

        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            raise
        finally:
            await conn.close()

    async def calculate_kr_intraday_ma(self):
        """kr_intraday 테이블의 이동평균 계산 및 업데이트 (PostgreSQL 윈도우 함수 사용)"""
        database_url = self.database_url
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

        conn = await asyncpg.connect(database_url)

        try:
            # Single query to calculate and update all MAs using PostgreSQL window functions
            update_query = """
            WITH history_with_current AS (
                SELECT symbol, trading_value, volume, date
                FROM kr_intraday_total
                WHERE date >= CURRENT_DATE - INTERVAL '200 days'
                UNION ALL
                SELECT symbol, trading_value, volume, CURRENT_DATE as date
                FROM kr_intraday
                WHERE trading_value IS NOT NULL AND volume IS NOT NULL
            ),
            ranked AS (
                SELECT symbol, trading_value, volume,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
                FROM history_with_current
            ),
            ma_calculated AS (
                SELECT symbol,
                       CAST(AVG(trading_value) FILTER (WHERE rn <= 5) AS BIGINT) as avg_tv_5d,
                       CAST(AVG(trading_value) FILTER (WHERE rn <= 20) AS BIGINT) as avg_tv_20d,
                       CAST(AVG(trading_value) FILTER (WHERE rn <= 60) AS BIGINT) as avg_tv_60d,
                       CAST(AVG(trading_value) FILTER (WHERE rn <= 200) AS BIGINT) as avg_tv_200d,
                       CAST(AVG(volume) FILTER (WHERE rn <= 5) AS BIGINT) as avg_vol_5d,
                       CAST(AVG(volume) FILTER (WHERE rn <= 20) AS BIGINT) as avg_vol_20d,
                       CAST(AVG(volume) FILTER (WHERE rn <= 60) AS BIGINT) as avg_vol_60d,
                       CAST(AVG(volume) FILTER (WHERE rn <= 200) AS BIGINT) as avg_vol_200d
                FROM ranked
                GROUP BY symbol
            )
            UPDATE kr_intraday i
            SET avg_trading_value_5d = m.avg_tv_5d,
                avg_trading_value_20d = m.avg_tv_20d,
                avg_trading_value_60d = m.avg_tv_60d,
                avg_trading_value_200d = m.avg_tv_200d,
                avg_volume_5d = m.avg_vol_5d,
                avg_volume_20d = m.avg_vol_20d,
                avg_volume_60d = m.avg_vol_60d,
                avg_volume_200d = m.avg_vol_200d
            FROM ma_calculated m
            WHERE i.symbol = m.symbol
            """

            result = await conn.execute(update_query)
            updated_count = int(result.split()[-1]) if result else 0
            logger.info(f"kr_intraday MA calculation completed: {updated_count} updated (single query)")

        except Exception as e:
            logger.error(f"kr_intraday MA calculation error: {e}")
        finally:
            if not conn.is_closed():
                await conn.close()

    async def run_intraday_collection(self):
        """장중 데이터 수집 실행"""
        logger.info("Starting KRX intraday data collection (COMPLETE)")

        try:
            success = self.collect_intraday_data()
            if not success:
                logger.error("Failed to collect and upload intraday data")
                return

            await self.process_intraday_data()

            # Calculate moving averages for kr_intraday
            await self.calculate_kr_intraday_ma()

            deleted_count = self.gcs_handler.cleanup_files_by_pattern("daily/kr_intraday_", keep_latest=5)
            logger.info(f"Cleanup completed: deleted {deleted_count} old intraday files")

            logger.info("KRX intraday data collection completed (COMPLETE)")

        finally:
            self.krx_client.close_session()

    def collect_intraday_detail_data(self):
        """KRX 장중 상세 데이터 수집 및 GCS 업로드"""
        csv_data = self.krx_client.get_intraday_detail_data()
        if not csv_data:
            logger.error("Failed to fetch intraday detail data from KRX")
            return False

        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        file_name = f"kr_intradaydetail_{timestamp}.csv"

        return self.gcs_handler.upload_file(file_name, csv_data, "daily")

    async def process_intraday_detail_data(self):
        """GCS에서 최신 장중 상세 데이터를 읽어서 PostgreSQL에 저장"""
        file_name, csv_data = self.gcs_handler.get_latest_file("daily/kr_intradaydetail_")
        if not csv_data:
            logger.error("No data to process")
            return

        try:
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            data_to_insert = []

            for row in csv_reader:
                def safe_decimal(value, precision=2):
                    if not value or value == '-' or value == '':
                        return None
                    try:
                        return round(float(str(value).replace(',', '')), precision)
                    except:
                        return None

                processed_row = (
                    row.get('종목코드', '')[:10],
                    row.get('종목명', '')[:200],
                    safe_decimal(row.get('종가')),
                    safe_decimal(row.get('대비')),
                    safe_decimal(row.get('등락률'), 2),
                    safe_decimal(row.get('EPS'), 2),
                    safe_decimal(row.get('PER'), 2),
                    safe_decimal(row.get('BPS'), 2),
                    safe_decimal(row.get('PBR'), 2),
                    safe_decimal(row.get('DPS'), 2),
                    safe_decimal(row.get('배당수익률'), 2),
                    datetime.now(),
                    datetime.now()
                )
                data_to_insert.append(processed_row)

            await self.insert_intraday_detail_data(data_to_insert)
            logger.info(f"Processed {len(data_to_insert)} intraday detail records from {file_name}")

        except Exception as e:
            logger.error(f"Failed to process intraday detail data: {e}")

    async def insert_intraday_detail_data(self, data_list):
        """장중 상세 데이터를 PostgreSQL에 삽입"""
        if not data_list:
            return

        conn = await self.get_connection()
        try:
            insert_query = """
            INSERT INTO kr_intraday_detail (
                symbol, stock_name, close, change_amount, change_rate,
                eps, per, bps, pbr, dps, dividend_yield,
                created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (symbol) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                close = EXCLUDED.close,
                change_amount = EXCLUDED.change_amount,
                change_rate = EXCLUDED.change_rate,
                eps = EXCLUDED.eps,
                per = EXCLUDED.per,
                bps = EXCLUDED.bps,
                pbr = EXCLUDED.pbr,
                dps = EXCLUDED.dps,
                dividend_yield = EXCLUDED.dividend_yield,
                updated_at = EXCLUDED.updated_at
            """

            await conn.executemany(insert_query, data_list)
            logger.info(f"Inserted {len(data_list)} intraday detail records")

        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            raise
        finally:
            await conn.close()

    async def run_intraday_detail_collection(self):
        """장중 상세 데이터 수집 실행"""
        logger.info("Starting KRX intraday detail data collection (COMPLETE)")

        try:
            success = self.collect_intraday_detail_data()
            if not success:
                logger.error("Failed to collect and upload intraday detail data")
                return

            await self.process_intraday_detail_data()

            deleted_count = self.gcs_handler.cleanup_files_by_pattern("daily/kr_intradaydetail_", keep_latest=5)
            logger.info(f"Cleanup completed: deleted {deleted_count} old intraday detail files")

            logger.info("KRX intraday detail data collection completed (COMPLETE)")

        finally:
            self.krx_client.close_session()

    async def process_intraday_total_data(self):
        """kr_intraday와 kr_intraday_detail을 JOIN하여 kr_intraday_total에 저장 (단일 쿼리)"""
        database_url = self.database_url
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

        conn = await asyncpg.connect(database_url)

        try:
            # Single INSERT INTO ... SELECT query with ROE calculation
            # KONEX excluded, KOSPI/KOSDAQ/KOSDAQ GLOBAL only
            upsert_query = """
            INSERT INTO kr_intraday_total (
                symbol, stock_name, exchange, close, change_amount, change_rate,
                open, high, low, volume, trading_value, market_cap, listed_shares,
                eps, per, roe, bps, pbr, dps, dividend_yield,
                avg_trading_value_5d, avg_trading_value_20d, avg_trading_value_60d, avg_trading_value_200d,
                avg_volume_5d, avg_volume_20d, avg_volume_60d, avg_volume_200d, date
            )
            SELECT
                i.symbol,
                i.stock_name,
                i.exchange,
                i.close,
                i.change_amount,
                i.change_rate,
                i.open,
                i.high,
                i.low,
                i.volume,
                i.trading_value,
                i.market_cap,
                i.listed_shares,
                d.eps,
                d.per,
                CASE WHEN d.bps > 0 THEN ROUND((d.eps / d.bps) * 100, 2) ELSE NULL END AS roe,
                d.bps,
                d.pbr,
                d.dps,
                d.dividend_yield,
                i.avg_trading_value_5d,
                i.avg_trading_value_20d,
                i.avg_trading_value_60d,
                i.avg_trading_value_200d,
                i.avg_volume_5d,
                i.avg_volume_20d,
                i.avg_volume_60d,
                i.avg_volume_200d,
                CURRENT_DATE
            FROM kr_intraday i
            LEFT JOIN kr_intraday_detail d ON i.symbol = d.symbol
            WHERE i.symbol IS NOT NULL
              AND i.exchange IN ('KOSPI', 'KOSDAQ', 'KOSDAQ GLOBAL')
            ON CONFLICT (symbol, date) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                exchange = EXCLUDED.exchange,
                close = EXCLUDED.close,
                change_amount = EXCLUDED.change_amount,
                change_rate = EXCLUDED.change_rate,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                volume = EXCLUDED.volume,
                trading_value = EXCLUDED.trading_value,
                market_cap = EXCLUDED.market_cap,
                listed_shares = EXCLUDED.listed_shares,
                eps = EXCLUDED.eps,
                per = EXCLUDED.per,
                roe = EXCLUDED.roe,
                bps = EXCLUDED.bps,
                pbr = EXCLUDED.pbr,
                dps = EXCLUDED.dps,
                dividend_yield = EXCLUDED.dividend_yield,
                avg_trading_value_5d = EXCLUDED.avg_trading_value_5d,
                avg_trading_value_20d = EXCLUDED.avg_trading_value_20d,
                avg_trading_value_60d = EXCLUDED.avg_trading_value_60d,
                avg_trading_value_200d = EXCLUDED.avg_trading_value_200d,
                avg_volume_5d = EXCLUDED.avg_volume_5d,
                avg_volume_20d = EXCLUDED.avg_volume_20d,
                avg_volume_60d = EXCLUDED.avg_volume_60d,
                avg_volume_200d = EXCLUDED.avg_volume_200d
            """

            result = await conn.execute(upsert_query)
            affected_count = int(result.split()[-1]) if result else 0
            logger.info(f"Inserted/Updated {affected_count} kr_intraday_total records (single query)")

        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            raise
        finally:
            if not conn.is_closed():
                await conn.close()

    async def run_intraday_total_collection(self):
        """장중 통합 데이터 수집 실행"""
        logger.info("Starting KRX intraday total data collection")

        try:
            await self.process_intraday_total_data()
            logger.info("KRX intraday total data collection completed")

        except Exception as e:
            logger.error(f"Intraday total collection failed: {e}")
            raise

    def collect_investor_daily_trading_data(self):
        """KRX 투자자별 일별 거래 데이터 수집 및 GCS 업로드"""
        csv_data = self.krx_client.get_investor_daily_trading_data()
        if not csv_data:
            logger.error("Failed to fetch investor daily trading data from KRX")
            return False

        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        file_name = f"kr_investor_daily_trading_{timestamp}.csv"

        return self.gcs_handler.upload_file(file_name, csv_data, "daily")

    async def process_investor_daily_trading_data(self):
        """GCS에서 최신 투자자별 일별 거래 데이터를 읽어서 PostgreSQL에 저장"""
        file_name, csv_data = self.gcs_handler.get_latest_file("daily/kr_investor_daily_trading_")
        if not csv_data:
            logger.error("No data to process")
            return

        try:
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            data_to_insert = []

            first_row = True

            for row in csv_reader:
                if first_row:
                    logger.info(f"CSV columns: {list(row.keys())}")
                    logger.info(f"First row data: {dict(row)}")
                    first_row = False
                def safe_bigint(value):
                    if not value or value == '-' or value == '':
                        return 0
                    try:
                        return int(str(value).replace(',', ''))
                    except:
                        return 0

                processed_row = (
                    row.get('투자자구분', '')[:100],
                    safe_bigint(row.get('거래량_매도')),
                    safe_bigint(row.get('거래량_매수')),
                    safe_bigint(row.get('거래량_순매수')),
                    safe_bigint(row.get('거래대금_매도')),
                    safe_bigint(row.get('거래대금_매수')),
                    safe_bigint(row.get('거래대금_순매수')),
                    datetime.now().date()
                )
                data_to_insert.append(processed_row)

            await self.insert_investor_daily_trading_data(data_to_insert)
            logger.info(f"Processed {len(data_to_insert)} investor daily trading records from {file_name}")

        except Exception as e:
            logger.error(f"Failed to process investor daily trading data: {e}")

    async def insert_investor_daily_trading_data(self, data_list):
        """투자자별 일별 거래 데이터를 PostgreSQL에 삽입"""
        if not data_list:
            return

        conn = await self.get_connection()
        try:
            insert_query = """
            INSERT INTO kr_investor_daily_trading (
                investor_type, sell_volume, buy_volume, net_buy_volume,
                sell_value, buy_value, net_buy_value, date
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (investor_type, date) DO UPDATE SET
                sell_volume = EXCLUDED.sell_volume,
                buy_volume = EXCLUDED.buy_volume,
                net_buy_volume = EXCLUDED.net_buy_volume,
                sell_value = EXCLUDED.sell_value,
                buy_value = EXCLUDED.buy_value,
                net_buy_value = EXCLUDED.net_buy_value
            """

            await conn.executemany(insert_query, data_list)
            logger.info(f"Inserted/Updated {len(data_list)} investor daily trading records")

        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            raise
        finally:
            await conn.close()

    async def run_investor_daily_trading_collection(self):
        """투자자별 일별 거래 데이터 수집 실행"""
        logger.info("Starting KRX investor daily trading data collection (COMPLETE)")

        try:
            success = self.collect_investor_daily_trading_data()
            if not success:
                logger.error("Failed to collect and upload investor daily trading data")
                return

            await self.process_investor_daily_trading_data()

            deleted_count = self.gcs_handler.cleanup_files_by_pattern("daily/kr_investor_daily_trading_", keep_latest=5)
            logger.info(f"Cleanup completed: deleted {deleted_count} old investor daily trading files")

            logger.info("KRX investor daily trading data collection completed (COMPLETE)")

        finally:
            self.krx_client.close_session()

    def collect_program_daily_trading_data(self):
        """KRX 프로그램 일별 거래 데이터 수집 및 GCS 업로드"""
        csv_data = self.krx_client.get_program_daily_trading_data()
        if not csv_data:
            logger.error("Failed to fetch program daily trading data from KRX")
            return False

        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        file_name = f"kr_program_daily_trading_{timestamp}.csv"

        return self.gcs_handler.upload_file(file_name, csv_data, "daily")

    async def process_program_daily_trading_data(self):
        """GCS에서 최신 프로그램 일별 거래 데이터를 읽어서 PostgreSQL에 저장"""
        file_name, csv_data = self.gcs_handler.get_latest_file("daily/kr_program_daily_trading_")
        if not csv_data:
            logger.error("No data to process")
            return

        try:
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            data_to_insert = []

            first_row = True

            for row in csv_reader:
                if first_row:
                    logger.info(f"CSV columns: {list(row.keys())}")
                    logger.info(f"First row data: {dict(row)}")
                    first_row = False

                def safe_bigint(value):
                    if not value or value == '-' or value == '':
                        return 0
                    try:
                        return int(str(value).replace(',', ''))
                    except:
                        return 0

                processed_row = (
                    row.get('구분', '')[:100],
                    safe_bigint(row.get('거래량_매도')),
                    safe_bigint(row.get('거래량_매수')),
                    safe_bigint(row.get('거래량_순매수')),
                    safe_bigint(row.get('거래대금_매도')),
                    safe_bigint(row.get('거래대금_매수')),
                    safe_bigint(row.get('거래대금_순매수')),
                    datetime.now().date()
                )
                data_to_insert.append(processed_row)

            await self.insert_program_daily_trading_data(data_to_insert)
            logger.info(f"Processed {len(data_to_insert)} program daily trading records from {file_name}")

        except Exception as e:
            logger.error(f"Failed to process program daily trading data: {e}")

    async def insert_program_daily_trading_data(self, data_list):
        """프로그램 일별 거래 데이터를 PostgreSQL에 삽입"""
        if not data_list:
            return

        import asyncio
        conn = None
        max_retries = 3

        try:
            for attempt in range(max_retries):
                try:
                    if conn is None or conn.is_closed():
                        if conn:
                            await conn.close()
                        conn = await self.get_connection()
                        logger.info(f"Database connection established (attempt {attempt + 1})")

                    insert_query = """
                    INSERT INTO kr_program_daily_trading (
                        category, sell_volume, buy_volume, net_buy_volume,
                        sell_value, buy_value, net_buy_value, date
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (category, date) DO UPDATE SET
                        sell_volume = EXCLUDED.sell_volume,
                        buy_volume = EXCLUDED.buy_volume,
                        net_buy_volume = EXCLUDED.net_buy_volume,
                        sell_value = EXCLUDED.sell_value,
                        buy_value = EXCLUDED.buy_value,
                        net_buy_value = EXCLUDED.net_buy_value
                    """

                    await conn.executemany(insert_query, data_list)
                    logger.info(f"Inserted/Updated {len(data_list)} program daily trading records")
                    break

                except Exception as e:
                    logger.error(f"Database insertion error (attempt {attempt + 1}): {e}")
                    if conn and not conn.is_closed():
                        await conn.close()
                    conn = None

                    if attempt == max_retries - 1:
                        raise

                    await asyncio.sleep(1 * (attempt + 1))  # Progressive delay

        finally:
            if conn and not conn.is_closed():
                await conn.close()

    async def run_program_daily_trading_collection(self):
        """프로그램 일별 거래 데이터 수집 실행"""
        logger.info("Starting KRX program daily trading data collection (COMPLETE)")

        try:
            success = self.collect_program_daily_trading_data()
            if not success:
                logger.error("Failed to collect and upload program daily trading data")
                return

            await self.process_program_daily_trading_data()

            deleted_count = self.gcs_handler.cleanup_files_by_pattern("daily/kr_program_daily_trading_", keep_latest=5)
            logger.info(f"Cleanup completed: deleted {deleted_count} old program daily trading files")

            logger.info("KRX program daily trading data collection completed (COMPLETE)")

        finally:
            self.krx_client.close_session()

    def collect_blocktrades_data(self):
        """KRX 대량 거래 데이터 수집 및 GCS 업로드"""
        csv_data = self.krx_client.get_blocktrades_data()
        if not csv_data:
            logger.error("Failed to fetch block trades data from KRX")
            return False

        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        file_name = f"kr_blocktrades_{timestamp}.csv"

        return self.gcs_handler.upload_file(file_name, csv_data, "daily")

    async def process_blocktrades_data(self):
        """GCS에서 최신 대량 거래 데이터를 읽어서 PostgreSQL에 저장"""
        file_name, csv_data = self.gcs_handler.get_latest_file("daily/kr_blocktrades_")
        if not csv_data:
            logger.error("No data to process")
            return

        try:
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            data_to_insert = []

            first_row = True

            for row in csv_reader:
                if first_row:
                    logger.info(f"CSV columns: {list(row.keys())}")
                    logger.info(f"First row data: {dict(row)}")
                    first_row = False

                def safe_decimal(value, precision=2):
                    if not value or value == '-' or value == '':
                        return None
                    try:
                        return round(float(str(value).replace(',', '')), precision)
                    except:
                        return None

                def safe_bigint(value):
                    if not value or value == '-' or value == '':
                        return 0
                    try:
                        return int(str(value).replace(',', ''))
                    except:
                        return 0

                processed_row = (
                    row.get('종목코드', '')[:10],
                    row.get('종목명', '')[:200],
                    safe_decimal(row.get('종가')),
                    safe_decimal(row.get('대비')),
                    safe_decimal(row.get('등락률'), 2),
                    safe_bigint(row.get('거래량')),
                    safe_bigint(row.get('대량매매수량')),
                    safe_decimal(row.get('대량매매비율'), 2),
                    datetime.now().date()
                )
                data_to_insert.append(processed_row)

            await self.insert_blocktrades_data(data_to_insert)
            logger.info(f"Processed {len(data_to_insert)} block trades records from {file_name}")

        except Exception as e:
            logger.error(f"Failed to process block trades data: {e}")

    async def insert_blocktrades_data(self, data_list):
        """대량 거래 데이터를 PostgreSQL에 삽입"""
        if not data_list:
            return

        conn = await self.get_connection()
        try:
            if conn.is_closed():
                logger.error("Connection was closed during partition creation, reconnecting...")
                await conn.close()
                conn = await self.get_connection()

            insert_query = """
            INSERT INTO kr_blocktrades (
                symbol, stock_name, close, change_amount, change_rate,
                volume, block_volume, block_volume_rate, date
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (symbol, date) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                close = EXCLUDED.close,
                change_amount = EXCLUDED.change_amount,
                change_rate = EXCLUDED.change_rate,
                volume = EXCLUDED.volume,
                block_volume = EXCLUDED.block_volume,
                block_volume_rate = EXCLUDED.block_volume_rate
            """

            await conn.executemany(insert_query, data_list)
            logger.info(f"Inserted/Updated {len(data_list)} block trades records")

        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            raise
        finally:
            if not conn.is_closed():
                await conn.close()

    async def run_blocktrades_collection(self):
        """대량 거래 데이터 수집 실행"""
        logger.info("Starting KRX block trades data collection (COMPLETE)")

        try:
            success = self.collect_blocktrades_data()
            if not success:
                logger.error("Failed to collect and upload block trades data")
                return

            await self.process_blocktrades_data()

            deleted_count = self.gcs_handler.cleanup_files_by_pattern("daily/kr_blocktrades_", keep_latest=5)
            logger.info(f"Cleanup completed: deleted {deleted_count} old block trades files")

            logger.info("KRX block trades data collection completed (COMPLETE)")

        finally:
            self.krx_client.close_session()

    def collect_foreign_ownership_data(self):
        """KRX 외국인 지분율 데이터 수집 및 GCS 업로드"""
        csv_data = self.krx_client.get_foreign_ownership_data()
        if not csv_data:
            logger.error("Failed to fetch foreign ownership data from KRX")
            return False

        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        file_name = f"kr_foreign_ownership_{timestamp}.csv"

        return self.gcs_handler.upload_file(file_name, csv_data, "daily")

    async def process_foreign_ownership_data(self):
        """GCS에서 최신 외국인 지분율 데이터를 읽어서 PostgreSQL에 저장"""
        file_name, csv_data = self.gcs_handler.get_latest_file("daily/kr_foreign_ownership_")
        if not csv_data:
            logger.error("No data to process")
            return

        try:
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            data_to_insert = []

            first_row = True

            for row in csv_reader:
                if first_row:
                    logger.info(f"CSV columns: {list(row.keys())}")
                    logger.info(f"First row data: {dict(row)}")
                    first_row = False

                def safe_decimal(value, precision=2):
                    if not value or value == '-' or value == '':
                        return None
                    try:
                        return round(float(str(value).replace(',', '')), precision)
                    except:
                        return None

                def safe_bigint(value):
                    if not value or value == '-' or value == '':
                        return 0
                    try:
                        return int(str(value).replace(',', ''))
                    except:
                        return 0

                processed_row = (
                    row.get('종목코드', '')[:10],
                    row.get('종목명', '')[:200],
                    safe_decimal(row.get('종가')),
                    safe_decimal(row.get('대비')),
                    safe_decimal(row.get('등락률'), 2),
                    safe_bigint(row.get('상장주식수')),
                    safe_bigint(row.get('외국인 보유수량')),
                    safe_decimal(row.get('외국인 지분율'), 2),
                    safe_bigint(row.get('외국인 한도수량')),
                    safe_decimal(row.get('외국인 한도소진율'), 2),
                    datetime.now().date()
                )
                data_to_insert.append(processed_row)

            await self.insert_foreign_ownership_data(data_to_insert)
            logger.info(f"Processed {len(data_to_insert)} foreign ownership records from {file_name}")

        except Exception as e:
            logger.error(f"Failed to process foreign ownership data: {e}")

    async def insert_foreign_ownership_data(self, data_list):
        """외국인 지분율 데이터를 PostgreSQL에 삽입"""
        if not data_list:
            return

        conn = await self.get_connection()
        try:
            if conn.is_closed():
                logger.error("Connection was closed during partition creation, reconnecting...")
                await conn.close()
                conn = await self.get_connection()

            insert_query = """
            INSERT INTO kr_foreign_ownership (
                symbol, stock_name, close, change_amount, change_rate,
                listed_shares, foreign_ownership, foreign_rate,
                foreign_limit, foreign_rate_limit, date
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (symbol, date) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                close = EXCLUDED.close,
                change_amount = EXCLUDED.change_amount,
                change_rate = EXCLUDED.change_rate,
                listed_shares = EXCLUDED.listed_shares,
                foreign_ownership = EXCLUDED.foreign_ownership,
                foreign_rate = EXCLUDED.foreign_rate,
                foreign_limit = EXCLUDED.foreign_limit,
                foreign_rate_limit = EXCLUDED.foreign_rate_limit
            """

            await conn.executemany(insert_query, data_list)
            logger.info(f"Inserted/Updated {len(data_list)} foreign ownership records")

        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            raise
        finally:
            if not conn.is_closed():
                await conn.close()

    async def run_foreign_ownership_collection(self):
        """외국인 지분율 데이터 수집 실행"""
        logger.info("Starting KRX foreign ownership data collection (COMPLETE)")

        try:
            success = self.collect_foreign_ownership_data()
            if not success:
                logger.error("Failed to collect and upload foreign ownership data")
                return

            await self.process_foreign_ownership_data()

            deleted_count = self.gcs_handler.cleanup_files_by_pattern("daily/kr_foreign_ownership_", keep_latest=5)
            logger.info(f"Cleanup completed: deleted {deleted_count} old foreign ownership files")

            logger.info("KRX foreign ownership data collection completed (COMPLETE)")

        finally:
            self.krx_client.close_session()

    # 나머지 메서드들 - krx.py에 추가할 코드

    def collect_stock_basic_data(self):
        """KRX 주식 기본정보 데이터 수집 및 GCS 업로드"""
        csv_data = self.krx_client.get_stock_basic_data()
        if not csv_data:
            logger.error("Failed to fetch stock basic data from KRX")
            return False

        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        file_name = f"kr_stock_basic_{timestamp}.csv"

        return self.gcs_handler.upload_file(file_name, csv_data, "")

    async def process_stock_basic_data(self):
        """GCS에서 최신 주식 기본정보 데이터를 읽어서 PostgreSQL에 저장"""
        file_name, csv_data = self.gcs_handler.get_latest_file("kr_stock_basic_")
        if not csv_data:
            logger.error("No data to process")
            return

        try:
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            data_to_insert = []

            first_row = True

            for row in csv_reader:
                if first_row:
                    logger.info(f"CSV columns: {list(row.keys())}")
                    logger.info(f"First row data: {dict(row)}")
                    first_row = False

                def safe_bigint(value):
                    if not value or value == '-' or value == '':
                        return None
                    try:
                        return int(str(value).replace(',', ''))
                    except:
                        return None

                def safe_date(value):
                    if not value or value == '-' or value == '':
                        return None
                    try:
                        for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y%m%d', '%m/%d/%Y', '%d/%m/%Y']:
                            try:
                                return datetime.strptime(str(value).strip(), fmt).date()
                            except ValueError:
                                continue
                        logger.warning(f"Could not parse date: {value}")
                        return None
                    except Exception as e:
                        logger.warning(f"Date parsing error for value '{value}': {e}")
                        return None

                # Skip KONEX exchange (check both '시장구분' and '증권구분' columns)
                exchange = row.get('시장구분', '').strip()
                securities_type = row.get('증권구분', '').strip()

                if 'KONEX' in exchange.upper() or 'KONEX' in securities_type.upper():
                    continue

                # Only include KOSPI, KOSDAQ, KOSDAQ GLOBAL
                if exchange not in ['KOSPI', 'KOSDAQ', 'KOSDAQ GLOBAL', '']:
                    continue

                processed_row = (
                    row.get('표준코드', '')[:20],
                    row.get('단축코드', '')[:10],
                    row.get('한글 종목명', '')[:200],
                    row.get('한글 종목약명', '')[:200],
                    row.get('영문 종목명', '')[:200],
                    safe_date(row.get('상장일')),
                    row.get('시장구분', '')[:20],
                    row.get('증권구분', '')[:200],
                    row.get('소속부', '')[:200],
                    row.get('주식종류', '')[:200],
                    safe_bigint(row.get('액면가')),
                    safe_bigint(row.get('상장주식수')),
                    datetime.now(),
                    datetime.now()
                )
                data_to_insert.append(processed_row)

            await self.insert_stock_basic_data(data_to_insert)
            logger.info(f"Processed {len(data_to_insert)} stock basic records from {file_name}")

        except Exception as e:
            logger.error(f"Failed to process stock basic data: {e}")

    async def insert_stock_basic_data(self, data_list):
        """주식 기본정보 데이터를 PostgreSQL에 삽입"""
        if not data_list:
            return

        conn = await self.get_connection()
        try:
            logger.info("Truncated existing kr_stock_basic data")

            insert_query = """
            INSERT INTO kr_stock_basic (
                standard_symbol, symbol, standard_stock_name, stock_name, stock_name_eng,
                listed_date, exchange, securities_type, department, stock_type,
                par_value, listed_shares, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (symbol) DO UPDATE SET
                standard_symbol = EXCLUDED.standard_symbol,
                standard_stock_name = EXCLUDED.standard_stock_name,
                stock_name = EXCLUDED.stock_name,
                stock_name_eng = EXCLUDED.stock_name_eng,
                listed_date = EXCLUDED.listed_date,
                exchange = EXCLUDED.exchange,
                securities_type = EXCLUDED.securities_type,
                department = EXCLUDED.department,
                stock_type = EXCLUDED.stock_type,
                par_value = EXCLUDED.par_value,
                listed_shares = EXCLUDED.listed_shares,
                updated_at = EXCLUDED.updated_at
            """

            await conn.executemany(insert_query, data_list)
            logger.info(f"Inserted {len(data_list)} stock basic records")

        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            raise
        finally:
            if not conn.is_closed():
                await conn.close()

    async def run_stock_basic_collection(self):
        """주식 기본정보 데이터 수집 실행"""
        logger.info("Starting KRX stock basic data collection (COMPLETE)")

        try:
            success = self.collect_stock_basic_data()
            if not success:
                logger.error("Failed to collect and upload stock basic data")
                return

            await self.process_stock_basic_data()

            deleted_count = self.gcs_handler.cleanup_files_by_pattern("kr_stock_basic_", keep_latest=5)
            logger.info(f"Cleanup completed: deleted {deleted_count} old stock basic files")

            logger.info("KRX stock basic data collection completed (COMPLETE)")

        finally:
            self.krx_client.close_session()

    def collect_stock_detail_data(self):
        """KRX 주식 상세정보 데이터 수집 및 GCS 업로드"""
        csv_data = self.krx_client.get_stock_detail_data()
        if not csv_data:
            logger.error("Failed to fetch stock detail data from KRX")
            return False

        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        file_name = f"kr_stock_detail_{timestamp}.csv"

        return self.gcs_handler.upload_file(file_name, csv_data, "")

    async def process_stock_detail_data(self):
        """GCS에서 최신 주식 상세정보 데이터를 읽어서 PostgreSQL에 저장"""
        file_name, csv_data = self.gcs_handler.get_latest_file("kr_stock_detail_")
        if not csv_data:
            logger.error("No data to process")
            return

        try:
            csv_reader = csv.DictReader(io.StringIO(csv_data))
            data_to_insert = []

            first_row = True

            for row in csv_reader:
                if first_row:
                    logger.info(f"CSV columns: {list(row.keys())}")
                    logger.info(f"First row data: {dict(row)}")
                    first_row = False

                def safe_bigint(value):
                    if not value or value == '-' or value == '':
                        return None
                    try:
                        return int(str(value).replace(',', ''))
                    except:
                        return None

                def safe_decimal(value, precision=0):
                    if not value or value == '-' or value == '':
                        return None
                    try:
                        return round(float(str(value).replace(',', '')), precision)
                    except:
                        return None

                columns = list(row.keys())
                department_cols = [col for col in columns if col == '소속부']
                first_department = department_cols[0] if department_cols else '소속부'

                processed_row = (
                    row.get('종목코드', '')[:10],
                    row.get('종목명', '')[:200],
                    row.get('시장구분', '')[:20],
                    row.get(first_department, '')[:200],
                    row.get('업종코드', '')[:20],
                    row.get('업종명', '')[:100],
                    safe_decimal(row.get('결산월')),
                    row.get('지정자문인', '')[:200],
                    safe_bigint(row.get('상장주식수')),
                    safe_bigint(row.get('액면가')),
                    safe_bigint(row.get('자본금')),
                    row.get('통화구분', '')[:3],
                    row.get('대표이사', '')[:100],
                    row.get('대표전화', '')[:100],
                    row.get('주소', '')[:255],
                    datetime.now(),
                    datetime.now()
                )
                data_to_insert.append(processed_row)

            await self.insert_stock_detail_data(data_to_insert)
            logger.info(f"Processed {len(data_to_insert)} stock detail records from {file_name}")

        except Exception as e:
            logger.error(f"Failed to process stock detail data: {e}")

    async def insert_stock_detail_data(self, data_list):
        """주식 상세정보 데이터를 PostgreSQL에 삽입"""
        if not data_list:
            return

        import asyncio
        conn = None
        max_retries = 3

        try:
            for attempt in range(max_retries):
                try:
                    if conn is None or conn.is_closed():
                        if conn:
                            await conn.close()
                        conn = await self.get_connection()
                        logger.info(f"Database connection established for stock detail (attempt {attempt + 1})")

                    insert_query = """
                    INSERT INTO kr_stock_detail (
                        symbol, stock_name, exchange, department, industry_code, industry,
                        fiscal_month, advisor, listed_shares, par_value, capital,
                        currency, ceo_name, phone, address, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                    ON CONFLICT (symbol) DO UPDATE SET
                        stock_name = EXCLUDED.stock_name,
                        exchange = EXCLUDED.exchange,
                        department = EXCLUDED.department,
                        industry_code = EXCLUDED.industry_code,
                        industry = EXCLUDED.industry,
                        fiscal_month = EXCLUDED.fiscal_month,
                        advisor = EXCLUDED.advisor,
                        listed_shares = EXCLUDED.listed_shares,
                        par_value = EXCLUDED.par_value,
                        capital = EXCLUDED.capital,
                        currency = EXCLUDED.currency,
                        ceo_name = EXCLUDED.ceo_name,
                        phone = EXCLUDED.phone,
                        address = EXCLUDED.address,
                        updated_at = EXCLUDED.updated_at
                    """

                    await conn.executemany(insert_query, data_list)
                    logger.info(f"Inserted {len(data_list)} stock detail records")
                    break

                except Exception as e:
                    logger.error(f"Database insertion error for stock detail (attempt {attempt + 1}): {e}")
                    if conn and not conn.is_closed():
                        await conn.close()
                    conn = None

                    if attempt == max_retries - 1:
                        raise

                    await asyncio.sleep(1 * (attempt + 1))  # Progressive delay

        finally:
            if conn and not conn.is_closed():
                await conn.close()

    async def run_stock_detail_collection(self):
        """주식 상세정보 데이터 수집 실행"""
        logger.info("Starting KRX stock detail data collection (COMPLETE)")

        try:
            success = self.collect_stock_detail_data()
            if not success:
                logger.error("Failed to collect and upload stock detail data")
                return

            await self.process_stock_detail_data()

            deleted_count = self.gcs_handler.cleanup_files_by_pattern("kr_stock_detail_", keep_latest=5)
            logger.info(f"Cleanup completed: deleted {deleted_count} old stock detail files")

            logger.info("KRX stock detail data collection completed (COMPLETE)")

        finally:
            self.krx_client.close_session()

    async def get_today_processed_symbols(self):
        """오늘 날짜로 이미 처리된 종목들 조회"""
        conn = await self.get_connection()
        try:
            today = datetime.now().date()
            query = """
            SELECT DISTINCT symbol
            FROM kr_individual_investor_daily_trading
            WHERE date = $1
            """
            rows = await conn.fetch(query, today)
            processed_symbols = set(row['symbol'] for row in rows)
            logger.info(f"Found {len(processed_symbols)} symbols already processed for {today}")
            return processed_symbols
        except Exception as e:
            logger.error(f"Failed to get today processed symbols: {e}")
            return set()
        finally:
            if not conn.is_closed():
                await conn.close()

    async def get_remaining_symbols(self):
        """아직 처리되지 않은 종목들만 조회"""
        all_symbols = await self.get_stock_symbols()
        processed_symbols = await self.get_today_processed_symbols()

        remaining_symbols = [
            symbol_info for symbol_info in all_symbols
            if symbol_info['symbol'] not in processed_symbols
        ]

        total_count = len(all_symbols)
        processed_count = len(processed_symbols)
        remaining_count = len(remaining_symbols)

        logger.info(f"Progress status: {processed_count} completed, {remaining_count} remaining, {total_count} total")

        return remaining_symbols, total_count, processed_count

    async def run(self):
        """KRX 완료된 데이터 수집 실행"""
        logger.info("Starting KRX complete data collection (COMPLETE)")
        await self.run_intraday_collection()
        await self.run_intraday_detail_collection()
        await self.run_investor_daily_trading_collection()
        await self.run_program_daily_trading_collection()
        await self.run_blocktrades_collection()
        await self.run_foreign_ownership_collection()
        await self.run_stock_basic_collection()
        await self.run_stock_detail_collection()
        # await self.run_individual_investor_daily_trading_collection()
        # await self.run_intraday_total_collection()
        logger.info("KRX complete data collection completed (COMPLETE)")

    async def get_individual_investor_data_with_retry(self, symbol_info, trade_date=None, max_retries=5, retry_delay=10):
        """Get individual investor trading data with retry logic"""
        for attempt in range(max_retries):
            try:
                result = self.get_individual_investor_data(symbol_info, trade_date)
                if result:
                    return result

                # If no result but no exception, still retry
                if attempt < max_retries - 1:
                    logger.warning(f"No data returned for symbol {symbol_info['symbol']}, attempt {attempt + 1}/{max_retries}, retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Error downloading data for symbol {symbol_info['symbol']}, attempt {attempt + 1}/{max_retries}: {e}, retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Failed to download data for symbol {symbol_info['symbol']} after {max_retries} attempts: {e}")
                    return None

        logger.error(f"Failed to get data for symbol {symbol_info['symbol']} after {max_retries} attempts")
        return None

    def get_individual_investor_data(self, symbol_info, trade_date=None):
        """Get individual investor trading data for a specific symbol"""
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')

        # Step 1: Generate OTP
        otp_url = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
        otp_payload = {
            'locale': 'ko_KR',
            'inqTpCd': '1',
            'trdVolVal': '2',
            'askBid': '3',
            'tboxisuCd_finder_stkisu0_0': f"{symbol_info['symbol']}/{symbol_info['stock_name']}",
            'isuCd': symbol_info['standard_symbol'],
            'isuCd2': '',
            'codeNmisuCd_finder_stkisu0_0': symbol_info['stock_name'],
            'param1isuCd_finder_stkisu0_0': 'ALL',
            'strtDd': trade_date,
            'endDd': trade_date,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02301'
        }

        try:
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Mobile Safari/537.36',
                'Accept': 'text/plain, */*; q=0.01',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br, zstd'
            })

            otp_response = session.post(otp_url, data=otp_payload, timeout=30)
            otp_response.raise_for_status()
            otp_code = otp_response.text.strip()

            if not otp_code:
                logger.error(f"Failed to get OTP for symbol {symbol_info['symbol']}")
                return None

            # Step 2: Download CSV
            download_url = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
            download_payload = {'code': otp_code}

            csv_response = session.post(download_url, data=download_payload, timeout=30)
            csv_response.raise_for_status()

            csv_content = csv_response.content.decode('euc-kr')
            logger.debug(f"Successfully downloaded CSV for symbol {symbol_info['symbol']}")
            return csv_content

        except Exception as e:
            # Re-raise the exception so retry logic can handle it
            raise e

    def parse_individual_investor_csv(self, csv_content, symbol_info, trade_date):
        """Parse CSV content and extract individual investor data"""
        try:
            csv_reader = csv.reader(io.StringIO(csv_content))
            rows = list(csv_reader)

            if len(rows) < 14:
                logger.warning(f"CSV has insufficient rows ({len(rows)}) for symbol {symbol_info['symbol']}")
                return None

            # Extract data from specific rows
            inst_row = rows[8] if len(rows) > 8 else None
            retail_row = rows[10] if len(rows) > 10 else None
            foreign_row = rows[11] if len(rows) > 11 else None
            total_row = rows[13] if len(rows) > 13 else None

            if not all([inst_row, retail_row, foreign_row, total_row]):
                logger.warning(f"Missing required rows for symbol {symbol_info['symbol']}")
                return None

            def safe_int(value):
                try:
                    return int(str(value).replace(',', '').replace('-', '0')) if value and str(value).strip() != '' else 0
                except:
                    return 0

            # Extract total values for ratio calculation
            total_buy_value = safe_int(total_row[5]) if len(total_row) > 5 else 0

            # Institution data (9th row - index 8)
            inst_buy_volume = safe_int(inst_row[1]) if len(inst_row) > 1 else 0
            inst_sell_volume = safe_int(inst_row[2]) if len(inst_row) > 2 else 0
            inst_net_volume = safe_int(inst_row[3]) if len(inst_row) > 3 else 0
            inst_buy_value = safe_int(inst_row[5]) if len(inst_row) > 5 else 0
            inst_sell_value = safe_int(inst_row[6]) if len(inst_row) > 6 else 0
            inst_net_value = safe_int(inst_row[7]) if len(inst_row) > 7 else 0
            inst_buy_ratio = (inst_buy_value / total_buy_value * 100) if total_buy_value > 0 else 0.0

            # Retail data (11th row - index 10)
            retail_buy_volume = safe_int(retail_row[1]) if len(retail_row) > 1 else 0
            retail_sell_volume = safe_int(retail_row[2]) if len(retail_row) > 2 else 0
            retail_net_volume = safe_int(retail_row[3]) if len(retail_row) > 3 else 0
            retail_buy_value = safe_int(retail_row[5]) if len(retail_row) > 5 else 0
            retail_sell_value = safe_int(retail_row[6]) if len(retail_row) > 6 else 0
            retail_net_value = safe_int(retail_row[7]) if len(retail_row) > 7 else 0
            retail_buy_ratio = (retail_buy_value / total_buy_value * 100) if total_buy_value > 0 else 0.0

            # Foreign data (12th row - index 11)
            foreign_buy_volume = safe_int(foreign_row[1]) if len(foreign_row) > 1 else 0
            foreign_sell_volume = safe_int(foreign_row[2]) if len(foreign_row) > 2 else 0
            foreign_net_volume = safe_int(foreign_row[3]) if len(foreign_row) > 3 else 0
            foreign_buy_value = safe_int(foreign_row[5]) if len(foreign_row) > 5 else 0
            foreign_sell_value = safe_int(foreign_row[6]) if len(foreign_row) > 6 else 0
            foreign_net_value = safe_int(foreign_row[7]) if len(foreign_row) > 7 else 0
            foreign_buy_ratio = (foreign_buy_value / total_buy_value * 100) if total_buy_value > 0 else 0.0

            # Total data (14th row - index 13)
            total_buy_volume = safe_int(total_row[1]) if len(total_row) > 1 else 0
            total_sell_volume = safe_int(total_row[2]) if len(total_row) > 2 else 0
            total_net_volume = safe_int(total_row[3]) if len(total_row) > 3 else 0
            total_buy_value_final = safe_int(total_row[5]) if len(total_row) > 5 else 0
            total_sell_value = safe_int(total_row[6]) if len(total_row) > 6 else 0
            total_net_value = safe_int(total_row[7]) if len(total_row) > 7 else 0

            # Parse trade_date to date object
            trade_date_obj = datetime.strptime(trade_date, '%Y%m%d').date()

            return {
                'date': trade_date_obj,
                'symbol': symbol_info['symbol'],
                'inst_buy_volume': inst_buy_volume,
                'inst_sell_volume': inst_sell_volume,
                'inst_net_volume': inst_net_volume,
                'inst_buy_value': inst_buy_value,
                'inst_sell_value': inst_sell_value,
                'inst_net_value': inst_net_value,
                'inst_buy_ratio': round(inst_buy_ratio, 2),
                'retail_buy_volume': retail_buy_volume,
                'retail_sell_volume': retail_sell_volume,
                'retail_net_volume': retail_net_volume,
                'retail_buy_value': retail_buy_value,
                'retail_sell_value': retail_sell_value,
                'retail_net_value': retail_net_value,
                'retail_buy_ratio': round(retail_buy_ratio, 2),
                'foreign_buy_volume': foreign_buy_volume,
                'foreign_sell_volume': foreign_sell_volume,
                'foreign_net_volume': foreign_net_volume,
                'foreign_buy_value': foreign_buy_value,
                'foreign_sell_value': foreign_sell_value,
                'foreign_net_value': foreign_net_value,
                'foreign_buy_ratio': round(foreign_buy_ratio, 2),
                'total_buy_volume': total_buy_volume,
                'total_sell_volume': total_sell_volume,
                'total_net_volume': total_net_volume,
                'total_buy_value': total_buy_value_final,
                'total_sell_value': total_sell_value,
                'total_net_value': total_net_value
            }

        except Exception as e:
            logger.error(f"Error parsing CSV for symbol {symbol_info['symbol']}: {e}")
            return None

    def get_progress_file_path(self):
        """Get progress file path for individual investor collection"""
        today = datetime.now().strftime('%Y%m%d')

        # Use Railway Volume if available
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_dir = '/app/log'
        else:
            log_dir = str(Path(__file__).parent.parent / 'log')

        return f"{log_dir}/individual_investor_progress_{today}.txt"

    def load_completed_symbols(self):
        """Load completed symbols from progress file"""
        progress_file = self.get_progress_file_path()
        completed = set()
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        symbol = line.strip()
                        if symbol:
                            completed.add(symbol)
            except Exception as e:
                logger.error(f"Error reading progress file: {e}")
        return completed

    def save_completed_symbol(self, symbol):
        """Save completed symbol to progress file"""
        progress_file = self.get_progress_file_path()
        try:
            with open(progress_file, 'a', encoding='utf-8') as f:
                f.write(f"{symbol}\n")
        except Exception as e:
            logger.error(f"Error saving progress: {e}")

    async def process_single_symbol_investor_data(self, symbol_info, trade_date):
        """Process individual investor data for a single symbol with semaphore control"""
        async with self.semaphore:
            try:
                # Get CSV data with retry logic
                csv_content = await self.get_individual_investor_data_with_retry(symbol_info, trade_date)
                if not csv_content:
                    return None

                # Parse CSV data
                parsed_data = self.parse_individual_investor_csv(csv_content, symbol_info, trade_date)
                if parsed_data:
                    # Save progress immediately after successful processing
                    self.save_completed_symbol(symbol_info['symbol'])
                    return parsed_data
                else:
                    return None

            except Exception as e:
                logger.error(f"Error processing individual investor data for symbol {symbol_info['symbol']}: {e}")
                return None

    async def process_individual_investor_daily_trading_data(self):
        """Process individual investor daily trading data for all symbols with hybrid approach"""
        # Initialize DB pool and semaphore
        await self.init_pool_for_individual_investor(min_size=10, max_size=30)

        database_url = self.database_url
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

        trade_date = datetime.now().strftime('%Y%m%d')

        # Load completed symbols from progress file
        completed_symbols = self.load_completed_symbols()
        logger.info(f"Found {len(completed_symbols)} already completed symbols")

        # Get all symbols from kr_stock_basic
        conn = await asyncpg.connect(database_url)
        try:
            query = """
            SELECT symbol, standard_symbol, stock_name
            FROM kr_stock_basic
            WHERE symbol IS NOT NULL AND standard_symbol IS NOT NULL
            ORDER BY symbol
            """
            rows = await conn.fetch(query)
            all_symbols = [{'symbol': row['symbol'], 'standard_symbol': row['standard_symbol'], 'stock_name': row['stock_name']} for row in rows]
            logger.info(f"Found {len(all_symbols)} total symbols in kr_stock_basic")
        finally:
            await conn.close()

        if not all_symbols:
            logger.error("No symbols found in kr_stock_basic table")
            return

        # Filter out completed symbols
        symbols = [s for s in all_symbols if s['symbol'] not in completed_symbols]
        logger.info(f"Processing {len(symbols)} remaining symbols (skipping {len(completed_symbols)} completed)")

        if not symbols:
            logger.info("All symbols already completed!")
            await self.close_pool()
            return

        # Fixed batch size for stable performance (middle-ground optimization)
        batch_size = 25  # Optimal balance: fast enough + stable + low failure impact
        total_symbols = len(symbols)

        logger.info(f"Using fixed batch size: {batch_size} (total symbols: {total_symbols})")

        # Split symbols into batches
        symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        logger.info(f"Created {len(symbol_batches)} batches")

        total_successful = 0
        total_failed = 0

        # Process batches sequentially
        for batch_idx, symbol_batch in enumerate(symbol_batches, 1):
            logger.info(f"Processing batch {batch_idx}/{len(symbol_batches)} ({len(symbol_batch)} symbols)")

            # Within each batch, make parallel API calls
            tasks = [
                self.process_single_symbol_investor_data(symbol_info, trade_date)
                for symbol_info in symbol_batch
            ]

            # Gather results from parallel API calls
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Collect successful data
            successful_data = []
            batch_failed = 0

            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Task failed with exception: {result}")
                    batch_failed += 1
                elif result is not None:
                    successful_data.append(result)
                else:
                    batch_failed += 1

            # Save batch data to database
            if successful_data:
                await self.save_individual_investor_data_pooled(successful_data)
                logger.info(f"Batch {batch_idx}/{len(symbol_batches)}: Saved {len(successful_data)} records, Failed: {batch_failed}")

            total_successful += len(successful_data)
            total_failed += batch_failed

            # Progress update
            total_completed = len(completed_symbols) + total_successful + total_failed
            logger.info(f"Overall progress: {total_completed}/{len(all_symbols)} ({total_successful} successful, {total_failed} failed in this run)")

        # Close the pool
        await self.close_pool()

        logger.info(f"Individual investor daily trading collection completed")
        logger.info(f"Total symbols: {len(all_symbols)}, Processed this run: {len(symbols)}, Successful: {total_successful}, Failed: {total_failed}")
        logger.info(f"Overall progress: {len(completed_symbols) + total_successful}/{len(all_symbols)} completed")

    async def save_individual_investor_data_pooled(self, data_list):
        """Save individual investor data to database using connection pool"""
        if not data_list:
            return

        async with self.pool.acquire() as conn:
            try:
                insert_query = """
                INSERT INTO kr_individual_investor_daily_trading (
                    date, symbol, inst_buy_volume, inst_sell_volume, inst_net_volume,
                    inst_buy_value, inst_sell_value, inst_net_value, inst_buy_ratio,
                    retail_buy_volume, retail_sell_volume, retail_net_volume,
                    retail_buy_value, retail_sell_value, retail_net_value, retail_buy_ratio,
                    foreign_buy_volume, foreign_sell_volume, foreign_net_volume,
                    foreign_buy_value, foreign_sell_value, foreign_net_value, foreign_buy_ratio,
                    total_buy_volume, total_sell_volume, total_net_volume,
                    total_buy_value, total_sell_value, total_net_value
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                         $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
                ON CONFLICT (date, symbol) DO UPDATE SET
                    inst_buy_volume = EXCLUDED.inst_buy_volume,
                    inst_sell_volume = EXCLUDED.inst_sell_volume,
                    inst_net_volume = EXCLUDED.inst_net_volume,
                    inst_buy_value = EXCLUDED.inst_buy_value,
                    inst_sell_value = EXCLUDED.inst_sell_value,
                    inst_net_value = EXCLUDED.inst_net_value,
                    inst_buy_ratio = EXCLUDED.inst_buy_ratio,
                    retail_buy_volume = EXCLUDED.retail_buy_volume,
                    retail_sell_volume = EXCLUDED.retail_sell_volume,
                    retail_net_volume = EXCLUDED.retail_net_volume,
                    retail_buy_value = EXCLUDED.retail_buy_value,
                    retail_sell_value = EXCLUDED.retail_sell_value,
                    retail_net_value = EXCLUDED.retail_net_value,
                    retail_buy_ratio = EXCLUDED.retail_buy_ratio,
                    foreign_buy_volume = EXCLUDED.foreign_buy_volume,
                    foreign_sell_volume = EXCLUDED.foreign_sell_volume,
                    foreign_net_volume = EXCLUDED.foreign_net_volume,
                    foreign_buy_value = EXCLUDED.foreign_buy_value,
                    foreign_sell_value = EXCLUDED.foreign_sell_value,
                    foreign_net_value = EXCLUDED.foreign_net_value,
                    foreign_buy_ratio = EXCLUDED.foreign_buy_ratio,
                    total_buy_volume = EXCLUDED.total_buy_volume,
                    total_sell_volume = EXCLUDED.total_sell_volume,
                    total_net_volume = EXCLUDED.total_net_volume,
                    total_buy_value = EXCLUDED.total_buy_value,
                    total_sell_value = EXCLUDED.total_sell_value,
                    total_net_value = EXCLUDED.total_net_value
                """

                data_tuples = []
                for data in data_list:
                    data_tuples.append((
                        data['date'], data['symbol'],
                        data['inst_buy_volume'], data['inst_sell_volume'], data['inst_net_volume'],
                        data['inst_buy_value'], data['inst_sell_value'], data['inst_net_value'], data['inst_buy_ratio'],
                        data['retail_buy_volume'], data['retail_sell_volume'], data['retail_net_volume'],
                        data['retail_buy_value'], data['retail_sell_value'], data['retail_net_value'], data['retail_buy_ratio'],
                        data['foreign_buy_volume'], data['foreign_sell_volume'], data['foreign_net_volume'],
                        data['foreign_buy_value'], data['foreign_sell_value'], data['foreign_net_value'], data['foreign_buy_ratio'],
                        data['total_buy_volume'], data['total_sell_volume'], data['total_net_volume'],
                        data['total_buy_value'], data['total_sell_value'], data['total_net_value']
                    ))

                await conn.executemany(insert_query, data_tuples)
                logger.debug(f"Saved {len(data_tuples)} individual investor records to database")

            except Exception as e:
                logger.error(f"Database save error for individual investor data: {e}")
                raise

    async def save_individual_investor_data(self, data_list):
        """Save individual investor data to database (legacy method for backward compatibility)"""
        if not data_list:
            return

        database_url = self.database_url
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

        conn = await asyncpg.connect(database_url)
        try:
            insert_query = """
            INSERT INTO kr_individual_investor_daily_trading (
                date, symbol, inst_buy_volume, inst_sell_volume, inst_net_volume,
                inst_buy_value, inst_sell_value, inst_net_value, inst_buy_ratio,
                retail_buy_volume, retail_sell_volume, retail_net_volume,
                retail_buy_value, retail_sell_value, retail_net_value, retail_buy_ratio,
                foreign_buy_volume, foreign_sell_volume, foreign_net_volume,
                foreign_buy_value, foreign_sell_value, foreign_net_value, foreign_buy_ratio,
                total_buy_volume, total_sell_volume, total_net_volume,
                total_buy_value, total_sell_value, total_net_value
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                     $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
            ON CONFLICT (date, symbol) DO UPDATE SET
                inst_buy_volume = EXCLUDED.inst_buy_volume,
                inst_sell_volume = EXCLUDED.inst_sell_volume,
                inst_net_volume = EXCLUDED.inst_net_volume,
                inst_buy_value = EXCLUDED.inst_buy_value,
                inst_sell_value = EXCLUDED.inst_sell_value,
                inst_net_value = EXCLUDED.inst_net_value,
                inst_buy_ratio = EXCLUDED.inst_buy_ratio,
                retail_buy_volume = EXCLUDED.retail_buy_volume,
                retail_sell_volume = EXCLUDED.retail_sell_volume,
                retail_net_volume = EXCLUDED.retail_net_volume,
                retail_buy_value = EXCLUDED.retail_buy_value,
                retail_sell_value = EXCLUDED.retail_sell_value,
                retail_net_value = EXCLUDED.retail_net_value,
                retail_buy_ratio = EXCLUDED.retail_buy_ratio,
                foreign_buy_volume = EXCLUDED.foreign_buy_volume,
                foreign_sell_volume = EXCLUDED.foreign_sell_volume,
                foreign_net_volume = EXCLUDED.foreign_net_volume,
                foreign_buy_value = EXCLUDED.foreign_buy_value,
                foreign_sell_value = EXCLUDED.foreign_sell_value,
                foreign_net_value = EXCLUDED.foreign_net_value,
                foreign_buy_ratio = EXCLUDED.foreign_buy_ratio,
                total_buy_volume = EXCLUDED.total_buy_volume,
                total_sell_volume = EXCLUDED.total_sell_volume,
                total_net_volume = EXCLUDED.total_net_volume,
                total_buy_value = EXCLUDED.total_buy_value,
                total_sell_value = EXCLUDED.total_sell_value,
                total_net_value = EXCLUDED.total_net_value
            """

            data_tuples = []
            for data in data_list:
                data_tuples.append((
                    data['date'], data['symbol'],
                    data['inst_buy_volume'], data['inst_sell_volume'], data['inst_net_volume'],
                    data['inst_buy_value'], data['inst_sell_value'], data['inst_net_value'], data['inst_buy_ratio'],
                    data['retail_buy_volume'], data['retail_sell_volume'], data['retail_net_volume'],
                    data['retail_buy_value'], data['retail_sell_value'], data['retail_net_value'], data['retail_buy_ratio'],
                    data['foreign_buy_volume'], data['foreign_sell_volume'], data['foreign_net_volume'],
                    data['foreign_buy_value'], data['foreign_sell_value'], data['foreign_net_value'], data['foreign_buy_ratio'],
                    data['total_buy_volume'], data['total_sell_volume'], data['total_net_volume'],
                    data['total_buy_value'], data['total_sell_value'], data['total_net_value']
                ))

            await conn.executemany(insert_query, data_tuples)
            logger.info(f"Saved {len(data_tuples)} individual investor records to database")

        except Exception as e:
            logger.error(f"Database save error for individual investor data: {e}")
            raise
        finally:
            await conn.close()

    async def close_pool(self):
        """Close the database connection pool"""
        if self.pool is not None:
            await self.pool.close()
            logger.info("Database connection pool closed")
            self.pool = None
            self.semaphore = None

    async def run_individual_investor_daily_trading_collection(self):
        """Run individual investor daily trading data collection"""
        logger.info("Starting KRX individual investor daily trading data collection")

        try:
            await self.process_individual_investor_daily_trading_data()
            logger.info("KRX individual investor daily trading data collection completed")

        except Exception as e:
            logger.error(f"Individual investor daily trading collection failed: {e}")
            raise


# 전역 인스턴스 및 함수 (krx_api.py 대체)
_krx_client = None

def get_krx_client() -> KrxApiClient:
    """KRX API 클라이언트 인스턴스 반환"""
    global _krx_client
    if _krx_client is None:
        _krx_client = KrxApiClient()
    return _krx_client

async def collect_krx_complete_data(database_url):
    """KRX 완료된 데이터 수집 실행 함수"""
    collector = KrxCompleteCollector(database_url)
    await collector.run()

if __name__ == "__main__":
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    if database_url.startswith('postgresql+asyncpg://'):
        database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

    asyncio.run(collect_krx_complete_data(database_url))