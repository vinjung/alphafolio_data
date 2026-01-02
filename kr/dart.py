# alpha/data/kr/dart.py
import requests
import zipfile
import xml.etree.ElementTree as ET
import asyncio
import asyncpg
import io
import os
import time
from datetime import datetime
import logging
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Custom Exceptions
class DartAPILimitExceeded(Exception):
    """DART API 일일 한도 초과 예외"""
    pass


# Company Info Collection Functions
def fetch_dart_company_data(api_key):
    """DART 회사 정보 XML 다운로드 및 파싱"""
    company_url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {'crtfc_key': api_key}

    try:
        response = requests.get(company_url, params=params)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            xml_files = [name for name in zip_file.namelist() if name.endswith('.xml')]

            if not xml_files:
                raise ValueError("No XML file found in zip")

            xml_content = zip_file.read(xml_files[0])
            root = ET.fromstring(xml_content)

            companies = []
            for item in root.findall('.//list'):
                company = {
                    'corp_code': item.find('corp_code').text if item.find('corp_code') is not None else None,
                    'corp_name': item.find('corp_name').text if item.find('corp_name') is not None else None,
                    'corp_eng_name': item.find('corp_eng_name').text if item.find('corp_eng_name') is not None else None,
                    'stock_code': item.find('stock_code').text if item.find('stock_code') is not None else None,
                    'modify_date': item.find('modify_date').text if item.find('modify_date') is not None else None
                }

                if company['stock_code']:
                    companies.append(company)

            logger.info(f"Downloaded {len(companies)} companies with stock codes")
            return companies

    except requests.RequestException as e:
        logger.error(f"Failed to fetch data from DART API: {e}")
        raise
    except zipfile.BadZipFile as e:
        logger.error(f"Failed to extract zip file: {e}")
        raise
    except ET.ParseError as e:
        logger.error(f"Failed to parse XML: {e}")
        raise


async def insert_dart_company_data(database_url, companies):
    """DART 회사 정보를 데이터베이스에 삽입 (kr_stock_basic의 symbol과 매칭되는 것만)"""
    if database_url.startswith('postgresql+asyncpg://'):
        database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

    conn = await asyncpg.connect(database_url)
    try:
        # kr_stock_basic에서 symbol 목록 조회
        symbols_result = await conn.fetch("SELECT symbol FROM kr_stock_basic")
        valid_symbols = {row['symbol'] for row in symbols_result}

        logger.info(f"Found {len(valid_symbols)} symbols in kr_stock_basic table")

        # kr_stock_basic의 symbol과 매칭되는 회사만 필터링
        filtered_companies = [
            company for company in companies
            if company['stock_code'] in valid_symbols
        ]

        logger.info(f"Filtered from {len(companies)} to {len(filtered_companies)} companies matching kr_stock_basic")

        if not filtered_companies:
            logger.warning("No companies matched with kr_stock_basic table")
            return

        insert_query = """
        INSERT INTO dart_company_info (corp_code, corp_name, corp_eng_name, stock_code, modify_date)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (stock_code) DO UPDATE SET
            corp_code = EXCLUDED.corp_code,
            corp_name = EXCLUDED.corp_name,
            corp_eng_name = EXCLUDED.corp_eng_name,
            modify_date = EXCLUDED.modify_date
        """

        data_to_insert = [
            (company['corp_code'], company['corp_name'], company['corp_eng_name'],
             company['stock_code'], company['modify_date'])
            for company in filtered_companies
        ]

        await conn.executemany(insert_query, data_to_insert)
        logger.info(f"Inserted/Updated {len(filtered_companies)} companies")

    except Exception as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        await conn.close()


async def collect_company_info(database_url, dart_api_key):
    """DART 회사 정보 수집"""
    logger.info("Starting DART company data collection")
    companies = fetch_dart_company_data(dart_api_key)
    logger.info(f"Fetched {len(companies)} companies with stock codes")
    await insert_dart_company_data(database_url, companies)
    logger.info("DART company data collection completed")


class DartFinancialManager:
    """DART Financial Data Collector"""

    def __init__(self, api_key, database_url):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.financial_url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"

        self.report_codes = ['11013', '11012', '11014', '11011']
        self.fs_div = 'CFS'
        # IS/CIS는 상호 대체 가능하므로 IS만 요청
        self.sj_div_list = ['BS', 'IS', 'CF', 'SCE']

        # 현재 연도부터 역순으로 시도 (첫 데이터 발견 시 중단)
        self.current_year = datetime.now().year
        self.years = [self.current_year - i for i in range(3)]  # 현재, -1년, -2년

        # File-based log system
        from pathlib import Path
        # Use Railway Volume if available
        if os.getenv('RAILWAY_ENVIRONMENT'):
            self.log_file = Path('/app/log') / "financial_processed_calls.log"
        else:
            self.log_file = Path(__file__).parent.parent / 'log' / "financial_processed_calls.log"
        self.processed_combinations = self.load_processed_combinations()

        # Connection pool
        self.pool = None

    def load_processed_combinations(self):
        """Load SUCCESS combinations only (NO_DATA allows retry)"""
        processed = set()
        if not self.log_file.exists():
            logger.info(f"No existing log file found. Starting fresh.")
            return processed

        success_count = 0
        no_data_count = 0

        with open(self.log_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 6:
                    corp_code, year, reprt_code, sj_div, timestamp, status = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
                    if status == "SUCCESS":
                        key = (corp_code, year, reprt_code, sj_div)
                        processed.add(key)
                        success_count += 1
                    elif status == "NO_DATA":
                        no_data_count += 1

        logger.info(f"Loaded {success_count} SUCCESS combinations to skip")
        logger.info(f"Found {no_data_count} NO_DATA combinations (will retry)")
        return processed

    def is_combination_processed(self, corp_code, year, reprt_code, sj_div):
        """O(1) memory lookup with IS/CIS substitution support"""
        combination_key = (corp_code, year, reprt_code, sj_div)

        # 직접 매칭 확인
        if combination_key in self.processed_combinations:
            return True

        # IS/CIS 상호 대체 확인
        # IS를 찾고 있는데 CIS가 있으면 충족된 것으로 처리
        if sj_div == 'IS':
            cis_key = (corp_code, year, reprt_code, 'CIS')
            if cis_key in self.processed_combinations:
                return True

        # CIS를 찾고 있는데 IS가 있으면 충족된 것으로 처리
        elif sj_div == 'CIS':
            is_key = (corp_code, year, reprt_code, 'IS')
            if is_key in self.processed_combinations:
                return True

        return False

    def log_processed_combination(self, corp_code, year, reprt_code, sj_div, status, records_count):
        """Log API call result with immediate flush"""
        timestamp = datetime.now().isoformat()
        log_entry = f"{corp_code}|{year}|{reprt_code}|{sj_div}|{timestamp}|{status}|{records_count}\n"

        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)
            f.flush()  # Crash safety

        if status == "SUCCESS":
            self.processed_combinations.add((corp_code, year, reprt_code, sj_div))

    async def init_pool(self):
        """Initialize connection pool"""
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Connection pool initialized (min=5, max=20)")

    async def get_connection(self):
        """Get connection from pool"""
        if not self.pool:
            await self.init_pool()
        return self.pool.acquire()

    async def close_pool(self):
        """Close pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Connection pool closed")

    def get_priority_quarters_by_season(self):
        """현재 월에 따른 분기별 우선순위 결정"""
        current_month = datetime.now().month

        if current_month in [4, 5]:      # 1분기 결산 시즌 (4-5월)
            return ['11013', '11012', '11014', '11011']  # 1분기 우선
        elif current_month in [7, 8]:   # 2분기 결산 시즌 (7-8월)
            return ['11012', '11013', '11014', '11011']  # 2분기 우선
        elif current_month in [10, 11]: # 3분기 결산 시즌 (10-11월)
            return ['11014', '11012', '11013', '11011']  # 3분기 우선
        else:                           # 4분기 결산 시즌 (12-3월)
            return ['11011', '11014', '11012', '11013']  # 4분기 우선

    async def get_symbols_from_kr_intraday(self):
        """Get active symbols from kr_intraday table"""
        async with await self.get_connection() as conn:
            query = """
            SELECT DISTINCT symbol
            FROM kr_intraday
            WHERE symbol IS NOT NULL
            ORDER BY symbol
            """
            rows = await conn.fetch(query)
            symbols = {row['symbol'] for row in rows}
            logger.info(f"Found {len(symbols)} active symbols in kr_intraday")
            return symbols

    async def get_target_companies(self):
        """Get target companies for processing"""
        active_symbols = await self.get_symbols_from_kr_intraday()

        logger.info(f"Target symbols analysis:")
        logger.info(f"  - kr_intraday total: {len(active_symbols)}")
        logger.info(f"  - Processing all symbols for {self.current_year} data collection")

        async with await self.get_connection() as conn:
            query = """
            SELECT corp_code, corp_name, stock_code
            FROM dart_company_info
            WHERE corp_code IS NOT NULL
            AND stock_code IS NOT NULL
            AND stock_code = ANY($1)
            ORDER BY corp_name, corp_code
            """
            rows = await conn.fetch(query, list(active_symbols))

            companies = []
            for row in rows:
                company_info = {
                    'corp_code': row['corp_code'],
                    'corp_name': row['corp_name'],
                    'stock_code': row['stock_code']
                }
                companies.append(company_info)

            logger.info(f"Selected {len(companies)} companies for processing")
            return companies

    def fetch_financial_data(self, corp_code, bsns_year, reprt_code, sj_div):
        """재무제표 데이터 API 호출 (특정 sj_div)"""
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bsns_year': str(bsns_year),
            'reprt_code': reprt_code,
            'fs_div': self.fs_div,
            'sj_div': sj_div
        }

        try:
            response = requests.get(self.financial_url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == '000':
                return data.get('list', [])
            elif data.get('status') == '013':  # 조회된 데이터가 없습니다
                logger.debug(f"No data found for {corp_code}, {bsns_year}, {reprt_code}")
                return []
            elif data.get('status') == '020':  # 일일 요청 한도 초과
                logger.error(f"DART API daily limit exceeded (20,000 calls/day): {data.get('message')}")
                raise DartAPILimitExceeded(f"DART API daily limit exceeded: {data.get('message')}")
            else:
                logger.warning(f"API Error for {corp_code}, {bsns_year}, {reprt_code}: {data.get('message')}")
                return []

        except requests.RequestException as e:
            logger.error(f"API request failed for {corp_code}, {bsns_year}, {reprt_code}: {e}")
            return []
        except ValueError as e:
            logger.error(f"JSON parsing failed for {corp_code}, {bsns_year}, {reprt_code}: {e}")
            return []

    async def insert_financial_data(self, financial_data):
        """Insert financial data using connection pool"""
        if not financial_data:
            return 0

        sj_div_counts = {}
        for item in financial_data:
            sj_div = item.get('sj_div', 'UNKNOWN')
            sj_div_counts[sj_div] = sj_div_counts.get(sj_div, 0) + 1

        logger.info(f"  Inserting {len(financial_data)} records with sj_div: {dict(sorted(sj_div_counts.items()))}")

        async with await self.get_connection() as conn:
            insert_query = """
            INSERT INTO kr_financial_position (
                stock_name, symbol, report_code, bsns_year, corp_code, fs_div,
                sj_div, sj_nm, account_id, account_nm, account_detail,
                thstrm_nm, thstrm_amount, thstrm_add_amount,
                frmtrm_nm, frmtrm_amount, frmtrm_q_nm, frmtrm_q_amount, frmtrm_add_amount,
                bfefrmtrm_nm, bfefrmtrm_amount, ord, currency
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
            ON CONFLICT (corp_code, bsns_year, report_code, fs_div) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                symbol = EXCLUDED.symbol,
                sj_div = EXCLUDED.sj_div,
                sj_nm = EXCLUDED.sj_nm,
                account_id = EXCLUDED.account_id,
                account_nm = EXCLUDED.account_nm,
                account_detail = EXCLUDED.account_detail,
                thstrm_nm = EXCLUDED.thstrm_nm,
                thstrm_amount = EXCLUDED.thstrm_amount,
                thstrm_add_amount = EXCLUDED.thstrm_add_amount,
                frmtrm_nm = EXCLUDED.frmtrm_nm,
                frmtrm_amount = EXCLUDED.frmtrm_amount,
                frmtrm_q_nm = EXCLUDED.frmtrm_q_nm,
                frmtrm_q_amount = EXCLUDED.frmtrm_q_amount,
                frmtrm_add_amount = EXCLUDED.frmtrm_add_amount,
                bfefrmtrm_nm = EXCLUDED.bfefrmtrm_nm,
                bfefrmtrm_amount = EXCLUDED.bfefrmtrm_amount,
                ord = EXCLUDED.ord,
                currency = EXCLUDED.currency
            """

            data_to_insert = []
            for item in financial_data:
                def safe_int(value):
                    if value is None or value == '':
                        return 0
                    try:
                        return int(str(value).replace(',', ''))
                    except:
                        return 0

                data_to_insert.append((
                    item.get('stock_name'),
                    item.get('symbol'),
                    item.get('report_code'),
                    safe_int(item.get('bsns_year')),
                    item.get('corp_code'),
                    self.fs_div,
                    item.get('sj_div'),
                    item.get('sj_nm'),
                    item.get('account_id'),
                    item.get('account_nm'),
                    item.get('account_detail'),
                    item.get('thstrm_nm'),
                    safe_int(item.get('thstrm_amount')),
                    safe_int(item.get('thstrm_add_amount')),
                    item.get('frmtrm_nm'),
                    safe_int(item.get('frmtrm_amount')),
                    item.get('frmtrm_q_nm'),
                    safe_int(item.get('frmtrm_q_amount')),
                    safe_int(item.get('frmtrm_add_amount')),
                    item.get('bfefrmtrm_nm'),
                    safe_int(item.get('bfefrmtrm_amount')),
                    safe_int(item.get('ord')),
                    item.get('currency')
                ))

            try:
                await conn.executemany(insert_query, data_to_insert)
                return len(data_to_insert)
            except Exception as e:
                logger.error(f"Database insertion error: {e}")
                return 0

    async def collect_company_data(self, company):
        """Collect all financial data for a specific company with file-based skip logic and early break"""
        corp_code = company['corp_code']
        corp_name = company['corp_name']
        stock_code = company['stock_code']

        logger.info(f"Processing {corp_name} ({corp_code}) - {stock_code}")

        total_inserted = 0
        skipped_count = 0

        for year in self.years:
            year_has_data = False
            logger.info(f"  Processing year {year} for {corp_name}")

            for report_code in self.report_codes:
                logger.info(f"  Processing quarter {report_code} for {corp_name}")

                for sj_div in self.sj_div_list:
                    # File-based skip check
                    if self.is_combination_processed(corp_code, str(year), report_code, sj_div):
                        skipped_count += 1
                        year_has_data = True
                        logger.debug(f"  Skipping {corp_name} {year} {report_code} {sj_div} - already processed (SUCCESS)")
                        continue

                    try:
                        logger.info(f"  API Call: {corp_name} {year} {report_code} {sj_div}")
                        financial_data = self.fetch_financial_data(corp_code, year, report_code, sj_div)

                        if financial_data:
                            # Add company info to financial data
                            for item in financial_data:
                                item['stock_name'] = corp_name
                                item['symbol'] = stock_code
                                item['report_code'] = report_code
                                item['fs_div'] = self.fs_div

                            saved_count = await self.insert_financial_data(financial_data)
                            total_inserted += saved_count

                            if saved_count > 0:
                                # SUCCESS log
                                self.log_processed_combination(corp_code, str(year), report_code, sj_div, "SUCCESS", saved_count)
                                logger.info(f"  Inserted {saved_count} records for {corp_name} {year} {report_code} {sj_div}")
                                year_has_data = True
                            else:
                                # NO_DATA log (allows retry)
                                self.log_processed_combination(corp_code, str(year), report_code, sj_div, "NO_DATA", 0)
                                logger.warning(f"  Data received but none saved for {corp_name} {year} {report_code} {sj_div}")
                        else:
                            # NO_DATA log (allows retry)
                            self.log_processed_combination(corp_code, str(year), report_code, sj_div, "NO_DATA", 0)
                            logger.debug(f"  No data for {corp_name} {year} {report_code} {sj_div}")

                        await asyncio.sleep(0.67)

                    except DartAPILimitExceeded:
                        # Re-raise to stop collection immediately
                        raise
                    except Exception as e:
                        logger.error(f"Error processing {corp_code} {year} {report_code} {sj_div}: {e}")
                        # No log = allows retry
                        continue

            # Stop if data found for this year
            if year_has_data:
                logger.info(f"  Found data for year {year}, stopping year iteration for {corp_name}")
                break

        logger.info(f"  Summary for {corp_name}: Inserted: {total_inserted}, Skipped: {skipped_count}")
        return total_inserted

    async def run_collection(self):
        """Execute financial data collection"""
        logger.info("Starting DART Financial Data Collection")

        companies = await self.get_target_companies()

        if not companies:
            logger.info("No companies to process")
            return

        logger.info(f"Starting collection for {len(companies)} companies")

        total_inserted = 0
        processed_companies = 0

        for i, company in enumerate(companies, 1):
            logger.info(f"[{i}/{len(companies)}] Processing company...")

            try:
                inserted = await self.collect_company_data(company)
                total_inserted += inserted
                processed_companies += 1

                if i % 5 == 0 or i == len(companies):
                    progress_pct = (i / len(companies)) * 100
                    logger.info(f"Progress: {i}/{len(companies)} companies ({progress_pct:.1f}%), {total_inserted} total records collected")

            except Exception as e:
                logger.error(f"Failed to process company {company.get('corp_name', 'Unknown')}: {e}")
                continue

        logger.info(f"=== FINANCIAL DATA COLLECTION COMPLETED ===")
        logger.info(f"Processed companies: {processed_companies}/{len(companies)}")
        logger.info(f"Total records inserted: {total_inserted}")

        # Close connection pool
        await self.close_pool()

    async def get_company_codes(self):
        """Get company codes from database"""
        async with await self.get_connection() as conn:
            query = """
            SELECT corp_code, corp_name, stock_code
            FROM dart_company_info
            WHERE corp_code IS NOT NULL AND stock_code IS NOT NULL
            ORDER BY corp_code
            """
            rows = await conn.fetch(query)
            return [{'corp_code': row['corp_code'], 'corp_name': row['corp_name'], 'stock_code': row['stock_code']} for row in rows]

    def fetch_all_financial_data(self, corp_code, bsns_year, reprt_code):
        """재무제표 데이터 API 호출 (모든 sj_div)"""
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bsns_year': str(bsns_year),
            'reprt_code': reprt_code,
            'fs_div': self.fs_div
        }

        try:
            response = requests.get(self.financial_url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == '000':
                return data.get('list', [])
            elif data.get('status') == '013':  # 조회된 데이터가 없습니다
                logger.debug(f"No data found for {corp_code}, {bsns_year}, {reprt_code}")
                return []
            elif data.get('status') == '020':  # 일일 요청 한도 초과
                logger.error(f"DART API daily limit exceeded (20,000 calls/day): {data.get('message')}")
                raise DartAPILimitExceeded(f"DART API daily limit exceeded: {data.get('message')}")
            else:
                logger.warning(f"API Error for {corp_code}, {bsns_year}, {reprt_code}: {data.get('message')}")
                return []

        except requests.RequestException as e:
            logger.error(f"API request failed for {corp_code}, {bsns_year}, {reprt_code}: {e}")
            return []
        except ValueError as e:
            logger.error(f"JSON parsing failed for {corp_code}, {bsns_year}, {reprt_code}: {e}")
            return []

    async def collect_all_financial_data(self, is_initial_run=True):
        """모든 회사의 재무제표 데이터 수집"""
        companies = await self.get_company_codes()
        logger.info(f"Starting financial data collection for {len(companies)} companies")

        total_processed = 0
        total_inserted = 0

        for company in companies:
            corp_code = company['corp_code']
            corp_name = company['corp_name']
            stock_code = company['stock_code']

            logger.info(f"Processing {corp_name} ({corp_code}) - {stock_code}")

            # 효율성을 위해 초기 실행이 아닌 경우 현재 연도만 처리
            years_to_process = self.years if is_initial_run else [self.current_year]

            for year in years_to_process:
                for report_code in self.report_codes:
                    try:
                        logger.debug(f"Fetching {corp_code} {year} {report_code}")
                        financial_data = self.fetch_all_financial_data(corp_code, year, report_code)

                        if financial_data:
                            # 회사 정보를 재무 데이터에 추가
                            for item in financial_data:
                                item['stock_name'] = corp_name
                                item['symbol'] = stock_code
                                item['report_code'] = report_code

                            await self.insert_financial_data(financial_data)
                            total_inserted += len(financial_data)
                            logger.info(f"Inserted {len(financial_data)} records for {corp_name} {year} {report_code}")

                        total_processed += 1

                        # API 호출 제한을 위한 딜레이 (90회/분)
                        await asyncio.sleep(0.67)

                    except DartAPILimitExceeded:
                        # Re-raise to stop collection immediately
                        raise
                    except Exception as e:
                        logger.error(f"Error processing {corp_code} {year} {report_code}: {e}")
                        continue

        logger.info(f"Financial data collection completed. Processed: {total_processed}, Inserted: {total_inserted}")

    async def insert_financial_data(self, financial_data):
        """재무제표 데이터를 데이터베이스에 삽입"""
        if not financial_data:
            return

        conn = await self.get_connection()
        try:
            insert_query = """
            INSERT INTO kr_financial_position (
                stock_name, symbol, report_code, bsns_year, corp_code, fs_div,
                sj_div, sj_nm, account_id, account_nm, account_detail,
                thstrm_nm, thstrm_amount, thstrm_add_amount,
                frmtrm_nm, frmtrm_amount, frmtrm_q_nm, frmtrm_q_amount, frmtrm_add_amount,
                bfefrmtrm_nm, bfefrmtrm_amount, ord, currency
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
            ON CONFLICT (corp_code, bsns_year, report_code, fs_div) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                symbol = EXCLUDED.symbol,
                fs_div = EXCLUDED.fs_div,
                sj_div = EXCLUDED.sj_div,
                sj_nm = EXCLUDED.sj_nm,
                account_nm = EXCLUDED.account_nm,
                account_detail = EXCLUDED.account_detail,
                thstrm_nm = EXCLUDED.thstrm_nm,
                thstrm_amount = EXCLUDED.thstrm_amount,
                thstrm_add_amount = EXCLUDED.thstrm_add_amount,
                frmtrm_nm = EXCLUDED.frmtrm_nm,
                frmtrm_amount = EXCLUDED.frmtrm_amount,
                frmtrm_q_nm = EXCLUDED.frmtrm_q_nm,
                frmtrm_q_amount = EXCLUDED.frmtrm_q_amount,
                frmtrm_add_amount = EXCLUDED.frmtrm_add_amount,
                bfefrmtrm_nm = EXCLUDED.bfefrmtrm_nm,
                bfefrmtrm_amount = EXCLUDED.bfefrmtrm_amount,
                currency = EXCLUDED.currency
            """

            data_to_insert = []
            for item in financial_data:
                # 숫자 필드 처리 (None이면 0, 문자열이면 정수 변환 시도)
                def safe_int(value):
                    if value is None or value == '':
                        return 0
                    try:
                        return int(str(value).replace(',', ''))
                    except:
                        return 0

                data_to_insert.append((
                    item.get('stock_name'),
                    item.get('symbol'),
                    item.get('report_code'),
                    safe_int(item.get('bsns_year')),
                    item.get('corp_code'),
                    item.get('fs_div'),
                    item.get('sj_div'),
                    item.get('sj_nm'),
                    item.get('account_id'),
                    item.get('account_nm'),
                    item.get('account_detail'),
                    item.get('thstrm_nm'),
                    safe_int(item.get('thstrm_amount')),
                    safe_int(item.get('thstrm_add_amount')),
                    item.get('frmtrm_nm'),
                    safe_int(item.get('frmtrm_amount')),
                    item.get('frmtrm_q_nm'),
                    safe_int(item.get('frmtrm_q_amount')),
                    safe_int(item.get('frmtrm_add_amount')),
                    item.get('bfefrmtrm_nm'),
                    safe_int(item.get('bfefrmtrm_amount')),
                    safe_int(item.get('ord')),
                    item.get('currency')
                ))

            await conn.executemany(insert_query, data_to_insert)

        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            raise
        finally:
            await conn.close()

    def fetch_company_data(self):
        """회사 정보 XML 다운로드 및 파싱"""
        params = {
            'crtfc_key': self.api_key
        }

        try:
            response = requests.get(self.company_url, params=params)
            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_files = [name for name in zip_file.namelist() if name.endswith('.xml')]

                if not xml_files:
                    raise ValueError("No XML file found in zip")

                xml_content = zip_file.read(xml_files[0])
                root = ET.fromstring(xml_content)

                companies = []
                for item in root.findall('.//list'):
                    company = {
                        'corp_code': item.find('corp_code').text if item.find('corp_code') is not None else None,
                        'corp_name': item.find('corp_name').text if item.find('corp_name') is not None else None,
                        'corp_eng_name': item.find('corp_eng_name').text if item.find('corp_eng_name') is not None else None,
                        'stock_code': item.find('stock_code').text if item.find('stock_code') is not None else None,
                        'modify_date': item.find('modify_date').text if item.find('modify_date') is not None else None
                    }

                    if company['stock_code']:
                        companies.append(company)

                return companies

        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from DART API: {e}")
            raise
        except zipfile.BadZipFile as e:
            logger.error(f"Failed to extract zip file: {e}")
            raise
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML: {e}")
            raise

    async def insert_company_data(self, companies):
        """회사 정보를 데이터베이스에 삽입"""
        conn = await self.get_connection()
        try:
            insert_query = """
            INSERT INTO dart_company_info (corp_code, corp_name, corp_eng_name, stock_code, modify_date)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (stock_code) DO UPDATE SET
                corp_code = EXCLUDED.corp_code,
                corp_name = EXCLUDED.corp_name,
                corp_eng_name = EXCLUDED.corp_eng_name,
                modify_date = EXCLUDED.modify_date
            """

            data_to_insert = [
                (company['corp_code'], company['corp_name'], company['corp_eng_name'],
                 company['stock_code'], company['modify_date'])
                for company in companies
            ]

            await conn.executemany(insert_query, data_to_insert)
            logger.info(f"Inserted/Updated {len(companies)} companies")

        except Exception as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            await conn.close()

    async def run_company_collection(self):
        """회사 정보 수집"""
        logger.info("Starting DART company data collection")
        companies = self.fetch_company_data()
        logger.info(f"Fetched {len(companies)} companies with stock codes")
        await self.insert_company_data(companies)
        logger.info("DART company data collection completed")

    async def run_financial_collection(self, is_initial_run=True):
        """재무제표 데이터 수집"""
        logger.info("Starting DART financial data collection")
        await self.collect_all_financial_data(is_initial_run)
        logger.info("DART financial data collection completed")

    async def run(self, collect_companies=True, collect_financials=True, is_initial_run=True):
        """전체 DART 데이터 수집 실행"""
        if collect_companies:
            await self.run_company_collection()

        if collect_financials:
            await self.run_financial_collection(is_initial_run)


class DartStockAcquisitionCollector:
    """DART 자기주식 취득 및 처분 현황 수집기"""

    def __init__(self, api_key, database_url):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.stockacquisition_api_url = "https://opendart.fss.or.kr/api/tesstkAcqsDspsSttus.json"

        # 현재 연도부터 역순으로 시도 (첫 데이터 발견 시 중단)
        current_year = datetime.now().year
        self.years = [str(current_year - i) for i in range(3)]  # 현재, -1년, -2년
        self.report_codes = ['11013', '11012', '11014', '11011']  # 1분기, 반기, 3분기, 사업보고서

        # 파일 기반 로그 시스템
        from pathlib import Path
        # Use Railway Volume if available
        if os.getenv('RAILWAY_ENVIRONMENT'):
            self.log_file = Path('/app/log') / "stockacquisition_processed_calls.log"
        else:
            self.log_file = Path(__file__).parent.parent / 'log' / "stockacquisition_processed_calls.log"
        self.processed_combinations = self.load_processed_combinations()

        # Connection pool
        self.pool = None

    def load_processed_combinations(self):
        """로그 파일에서 SUCCESS 조합만 로드"""
        processed = set()
        if not self.log_file.exists():
            logger.info(f"No existing log file found. Starting fresh.")
            return processed

        success_count = 0
        no_data_count = 0

        with open(self.log_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 5:
                    corp_code, year, reprt_code, timestamp, status = parts[0], parts[1], parts[2], parts[3], parts[4]
                    if status == "SUCCESS":
                        key = (corp_code, year, reprt_code)
                        processed.add(key)
                        success_count += 1
                    elif status == "NO_DATA":
                        no_data_count += 1

        logger.info(f"Loaded {success_count} SUCCESS combinations to skip")
        logger.info(f"Found {no_data_count} NO_DATA combinations (will retry)")
        return processed

    def is_combination_processed(self, corp_code, year, reprt_code):
        """메모리에서 O(1) 조회"""
        return (corp_code, year, reprt_code) in self.processed_combinations

    def log_processed_combination(self, corp_code, year, reprt_code, status, records_count):
        """API 호출 결과를 로그 파일에 기록"""
        timestamp = datetime.now().isoformat()
        log_entry = f"{corp_code}|{year}|{reprt_code}|{timestamp}|{status}|{records_count}\n"

        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)
            f.flush()

        if status == "SUCCESS":
            self.processed_combinations.add((corp_code, year, reprt_code))

    async def init_pool(self):
        """Connection pool 초기화"""
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Connection pool initialized (min=5, max=20)")

    async def get_connection(self):
        """Pool에서 연결 가져오기"""
        if not self.pool:
            await self.init_pool()
        return self.pool.acquire()

    async def close_pool(self):
        """Pool 종료"""
        if self.pool:
            await self.pool.close()
            logger.info("Connection pool closed")

    async def get_company_list(self):
        """dart_company_info 테이블에서 모든 회사 목록 조회"""
        async with await self.get_connection() as conn:
            query = """
            SELECT corp_code, corp_name, stock_code
            FROM dart_company_info
            WHERE corp_code IS NOT NULL
            AND corp_code != ''
            ORDER BY corp_code
            """
            rows = await conn.fetch(query)

            companies = []
            for row in rows:
                companies.append({
                    'corp_code': row['corp_code'],
                    'corp_name': row['corp_name'],
                    'stock_code': row['stock_code']
                })

            logger.info(f"Found {len(companies)} companies to process")
            return companies

    def call_stockacquisition_api(self, corp_code, bsns_year, reprt_code):
        """자기주식 취득/처분 현황 API 호출"""
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bsns_year': bsns_year,
            'reprt_code': reprt_code
        }

        try:
            response = requests.get(self.stockacquisition_api_url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == '000' and data.get('list'):
                return data['list']
            elif data.get('status') == '013':  # 검색된 데이터가 없습니다
                return []
            elif data.get('status') == '020':  # 일일 요청 한도 초과
                logger.error(f"DART API daily limit exceeded (20,000 calls/day): {data.get('message')}")
                raise DartAPILimitExceeded(f"DART API daily limit exceeded: {data.get('message')}")
            else:
                logger.warning(f"API Error for {corp_code}/{bsns_year}/{reprt_code}: {data.get('status')} - {data.get('message')}")
                return []

        except requests.RequestException as e:
            logger.error(f"Network error calling stock acquisition API: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error calling stock acquisition API: {e}")
            return []

    async def save_stockacquisition_data(self, stockacquisition_records, company_info):
        """Save stock acquisition data using connection pool (batch insert with executemany)"""
        if not stockacquisition_records:
            return 0

        # 숫자 필드 처리 함수
        def safe_bigint(value):
            if value is None or value == '' or value == '-':
                return None
            try:
                clean_value = str(value).replace(',', '').replace(' ', '')
                return int(clean_value) if clean_value else None
            except (ValueError, TypeError):
                return None

        # 데이터 검증 및 준비
        data_to_insert = []
        for record in stockacquisition_records:
            # 실제 자기주식 데이터가 있는지 확인
            acqs_mth1 = record.get('acqs_mth1', '').strip()
            acqs_mth2 = record.get('acqs_mth2', '').strip()
            acqs_mth3 = record.get('acqs_mth3', '').strip()
            stock_knd = record.get('stock_knd', '').strip()

            # 최소한 하나의 취득방법이나 주식종류가 있어야 함
            if not acqs_mth1 and not acqs_mth2 and not acqs_mth3 and not stock_knd:
                continue

            # 날짜 처리 (stlm_dt)
            stlm_dt = record.get('stlm_dt')
            stlm_dt_value = None

            if stlm_dt:
                try:
                    if len(stlm_dt) == 8:  # YYYYMMDD
                        date_str = f"{stlm_dt[:4]}-{stlm_dt[4:6]}-{stlm_dt[6:8]}"
                        stlm_dt_value = datetime.strptime(date_str, '%Y-%m-%d').date()
                    elif len(stlm_dt) == 10 and '-' in stlm_dt:  # YYYY-MM-DD
                        stlm_dt_value = datetime.strptime(stlm_dt, '%Y-%m-%d').date()
                except ValueError:
                    stlm_dt_value = None

            data_to_insert.append((
                company_info['stock_code'],
                record.get('rcept_no'),
                record.get('corp_cls'),
                record.get('corp_code'),
                record.get('corp_name'),
                acqs_mth1,
                acqs_mth2,
                acqs_mth3,
                stock_knd,
                safe_bigint(record.get('bsis_qy')),
                safe_bigint(record.get('change_qy_acqs')),
                safe_bigint(record.get('change_qy_dsps')),
                safe_bigint(record.get('change_qy_incnr')),
                safe_bigint(record.get('trmend_qy')),
                record.get('rm'),
                stlm_dt_value
            ))

        if not data_to_insert:
            return 0

        # 배치 삽입
        async with await self.get_connection() as conn:
            try:
                await conn.executemany("""
                    INSERT INTO kr_stockacquisitiondisposal (
                        symbol, rcept_no, corp_cls, corp_code, corp_name,
                        acqs_mth1, acqs_mth2, acqs_mth3, stock_knd,
                        bsis_qy, change_qy_acqs, change_qy_dsps,
                        change_qy_incnr, trmend_qy, rm, stlm_dt
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                    ON CONFLICT (corp_code, rcept_no, acqs_mth1, acqs_mth2, acqs_mth3, stock_knd) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        corp_cls = EXCLUDED.corp_cls,
                        corp_name = EXCLUDED.corp_name,
                        bsis_qy = EXCLUDED.bsis_qy,
                        change_qy_acqs = EXCLUDED.change_qy_acqs,
                        change_qy_dsps = EXCLUDED.change_qy_dsps,
                        change_qy_incnr = EXCLUDED.change_qy_incnr,
                        trmend_qy = EXCLUDED.trmend_qy,
                        rm = EXCLUDED.rm,
                        stlm_dt = EXCLUDED.stlm_dt
                """, data_to_insert)
                return len(data_to_insert)
            except Exception as e:
                logger.error(f"Failed to save stock acquisition records: {e}")
                return 0

    async def process_company_stockacquisition_data(self, company):
        """Process individual company stock acquisition data with file-based skip logic and early break"""
        corp_code = company['corp_code']
        corp_name = company['corp_name']

        logger.info(f"Processing stock acquisition data for {corp_name} ({corp_code})")

        total_saved = 0
        skipped_count = 0

        for year in self.years:
            year_has_data = False

            for reprt_code in self.report_codes:
                # File-based skip check
                if self.is_combination_processed(corp_code, year, reprt_code):
                    skipped_count += 1
                    year_has_data = True
                    logger.debug(f"  {year}/{reprt_code}: Already processed (SUCCESS), skipping")
                    continue

                try:
                    stockacquisition_data = self.call_stockacquisition_api(corp_code, year, reprt_code)

                    if stockacquisition_data:
                        saved_count = await self.save_stockacquisition_data(stockacquisition_data, company)
                        total_saved += saved_count

                        if saved_count > 0:
                            # SUCCESS log
                            self.log_processed_combination(corp_code, year, reprt_code, "SUCCESS", saved_count)
                            logger.info(f"  {year}/{reprt_code}: {saved_count} records saved")
                            year_has_data = True
                        else:
                            # NO_DATA log (allows retry)
                            self.log_processed_combination(corp_code, year, reprt_code, "NO_DATA", 0)
                            logger.warning(f"  {year}/{reprt_code}: Data received but none saved")
                    else:
                        # NO_DATA log (allows retry)
                        self.log_processed_combination(corp_code, year, reprt_code, "NO_DATA", 0)
                        logger.debug(f"  {year}/{reprt_code}: No data from API")

                except DartAPILimitExceeded:
                    # Re-raise to stop current collector
                    raise
                except Exception as e:
                    logger.error(f"Error processing {corp_code}/{year}/{reprt_code}: {e}")
                    # No log = allows retry
                    continue

                await asyncio.sleep(0.67)

            # Stop if data found for this year
            if year_has_data:
                logger.info(f"  Found data for year {year}, stopping year iteration for {corp_name}")
                break

        if skipped_count > 0:
            logger.info(f"{corp_name}: {total_saved} new records saved, {skipped_count} API calls skipped")
        else:
            logger.info(f"{corp_name}: {total_saved} total stock acquisition records collected")

        return total_saved

    async def collect_stockacquisition_data(self):
        """전체 자기주식 데이터 수집 실행"""
        logger.info("Starting comprehensive stock acquisition data collection")

        companies = await self.get_company_list()
        if not companies:
            logger.warning("No companies found to process")
            return

        total_companies = len(companies)
        total_records = 0
        processed_companies = 0

        for i, company in enumerate(companies, 1):
            logger.info(f"[{i}/{total_companies}] Processing company...")

            try:
                records = await self.process_company_stockacquisition_data(company)
                total_records += records
                processed_companies += 1

                # 더 자주 진행률 표시 (매 5개마다 또는 마지막)
                if i % 5 == 0 or i == total_companies:
                    progress_pct = (i / total_companies) * 100
                    logger.info(f"Progress: {i}/{total_companies} companies ({progress_pct:.1f}%), {total_records} total records collected")

            except Exception as e:
                logger.error(f"Failed to process company {company['corp_name']}: {e}")
                continue

        logger.info(f"=== STOCK ACQUISITION DATA COLLECTION COMPLETED ===")
        logger.info(f"Processed companies: {processed_companies}/{total_companies}")
        logger.info(f"Total stock acquisition records collected: {total_records}")

        await self.get_stockacquisition_statistics()

        # Close connection pool
        await self.close_pool()

    async def get_stockacquisition_statistics(self):
        """Stock acquisition data collection statistics"""
        async with await self.get_connection() as conn:
            # Year-wise statistics (based on stlm_dt)
            year_stats = await conn.fetch("""
                SELECT EXTRACT(YEAR FROM stlm_dt) as year, COUNT(*) as count
                FROM kr_stockacquisitiondisposal
                WHERE stlm_dt IS NOT NULL
                GROUP BY EXTRACT(YEAR FROM stlm_dt)
                ORDER BY year
            """)

            # Acquisition method statistics
            acqs_mth1_stats = await conn.fetch("""
                SELECT acqs_mth1, COUNT(*) as count
                FROM kr_stockacquisitiondisposal
                WHERE acqs_mth1 IS NOT NULL AND acqs_mth1 != ''
                GROUP BY acqs_mth1
                ORDER BY count DESC
                LIMIT 10
            """)

            # Stock type statistics
            stock_knd_stats = await conn.fetch("""
                SELECT stock_knd, COUNT(*) as count
                FROM kr_stockacquisitiondisposal
                WHERE stock_knd IS NOT NULL AND stock_knd != ''
                GROUP BY stock_knd
                ORDER BY count DESC
                LIMIT 10
            """)

            logger.info("=== STOCK ACQUISITION DATA STATISTICS ===")
            logger.info("Year-wise distribution:")
            for row in year_stats:
                year = int(row['year']) if row['year'] else 'NULL'
                logger.info(f"  {year}: {row['count']} records")

            logger.info("Top acquisition methods:")
            for row in acqs_mth1_stats:
                acqs_mth1 = row['acqs_mth1'] or 'NULL'
                logger.info(f"  {acqs_mth1[:30]}...: {row['count']} records")

            logger.info("Top stock types:")
            for row in stock_knd_stats:
                stock_knd = row['stock_knd'] or 'NULL'
                logger.info(f"  {stock_knd}: {row['count']} records")


class DartAuditCollector:
    """DART Audit Data Collector"""

    def __init__(self, api_key, database_url):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.audit_api_url = "https://opendart.fss.or.kr/api/accnutAdtorNmNdAdtOpinion.json"

        # 현재 연도부터 역순으로 시도 (첫 데이터 발견 시 중단)
        current_year = datetime.now().year
        self.years = [str(current_year - i) for i in range(3)]  # 현재, -1년, -2년
        self.report_codes = ['11013', '11012', '11014', '11011']

        # File-based log system
        from pathlib import Path
        # Use Railway Volume if available
        if os.getenv('RAILWAY_ENVIRONMENT'):
            self.log_file = Path('/app/log') / "audit_processed_calls.log"
        else:
            self.log_file = Path(__file__).parent.parent / 'log' / "audit_processed_calls.log"
        self.processed_combinations = self.load_processed_combinations()

        # Connection pool
        self.pool = None

    def load_processed_combinations(self):
        """Load SUCCESS combinations only (NO_DATA allows retry)"""
        processed = set()
        if not self.log_file.exists():
            logger.info(f"No existing log file found. Starting fresh.")
            return processed

        success_count = 0
        no_data_count = 0

        with open(self.log_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 5:
                    corp_code, year, reprt_code, timestamp, status = parts[0], parts[1], parts[2], parts[3], parts[4]
                    if status == "SUCCESS":
                        key = (corp_code, year, reprt_code)
                        processed.add(key)
                        success_count += 1
                    elif status == "NO_DATA":
                        no_data_count += 1

        logger.info(f"Loaded {success_count} SUCCESS combinations to skip")
        logger.info(f"Found {no_data_count} NO_DATA combinations (will retry)")
        return processed

    def is_combination_processed(self, corp_code, year, reprt_code):
        """O(1) memory lookup"""
        return (corp_code, year, reprt_code) in self.processed_combinations

    def log_processed_combination(self, corp_code, year, reprt_code, status, records_count):
        """Log API call result with immediate flush"""
        timestamp = datetime.now().isoformat()
        log_entry = f"{corp_code}|{year}|{reprt_code}|{timestamp}|{status}|{records_count}\n"

        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)
            f.flush()  # Crash safety

        if status == "SUCCESS":
            self.processed_combinations.add((corp_code, year, reprt_code))

    async def init_pool(self):
        """Initialize connection pool"""
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Connection pool initialized (min=5, max=20)")

    async def get_connection(self):
        """Get connection from pool"""
        if not self.pool:
            await self.init_pool()
        return self.pool.acquire()

    async def close_pool(self):
        """Close pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Connection pool closed")

    async def get_company_list(self):
        """Get all companies from dart_company_info"""
        async with await self.get_connection() as conn:
            query = """
            SELECT corp_code, corp_name, stock_code
            FROM dart_company_info
            WHERE corp_code IS NOT NULL
            AND corp_code != ''
            ORDER BY corp_code
            """
            rows = await conn.fetch(query)

            companies = []
            for row in rows:
                companies.append({
                    'corp_code': row['corp_code'],
                    'corp_name': row['corp_name'],
                    'stock_code': row['stock_code']
                })

            logger.info(f"Found {len(companies)} companies to process")
            return companies

    def call_audit_api(self, corp_code, bsns_year, reprt_code):
        """감사 정보 API 호출"""
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bsns_year': bsns_year,
            'reprt_code': reprt_code
        }

        try:
            response = requests.get(self.audit_api_url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == '000' and data.get('list'):
                return data['list']
            elif data.get('status') == '013':  # 검색된 데이터가 없습니다
                return []
            elif data.get('status') == '020':  # 일일 요청 한도 초과
                logger.error(f"DART API daily limit exceeded (20,000 calls/day): {data.get('message')}")
                raise DartAPILimitExceeded(f"DART API daily limit exceeded: {data.get('message')}")
            else:
                logger.warning(f"API Error for {corp_code}/{bsns_year}/{reprt_code}: {data.get('status')} - {data.get('message')}")
                return []

        except requests.RequestException as e:
            logger.error(f"Network error calling audit API: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error calling audit API: {e}")
            return []

    async def save_audit_data(self, audit_records, company_info):
        """Save audit data using connection pool (batch insert with executemany)"""
        if not audit_records:
            return 0

        # 데이터 준비
        data_to_insert = []
        for record in audit_records:
            # Date processing (stlm_dt)
            stlm_dt = record.get('stlm_dt')
            stlm_dt_value = None

            if stlm_dt:
                try:
                    if len(stlm_dt) == 8:  # YYYYMMDD
                        date_str = f"{stlm_dt[:4]}-{stlm_dt[4:6]}-{stlm_dt[6:8]}"
                        stlm_dt_value = datetime.strptime(date_str, '%Y-%m-%d').date()
                    elif len(stlm_dt) == 10 and '-' in stlm_dt:  # YYYY-MM-DD
                        stlm_dt_value = datetime.strptime(stlm_dt, '%Y-%m-%d').date()
                except ValueError:
                    stlm_dt_value = None

            data_to_insert.append((
                company_info['corp_name'],  # stock_name
                company_info['stock_code'],  # symbol
                record.get('rcept_no'),
                record.get('corp_cls'),
                record.get('corp_code'),
                record.get('corp_name'),
                record.get('bsns_year'),
                record.get('adtor'),
                record.get('adt_opinion'),
                record.get('adt_reprt_spcmnt_matter'),
                record.get('emphs_matter'),
                record.get('core_adt_matter'),
                stlm_dt_value
            ))

        if not data_to_insert:
            return 0

        # 배치 삽입
        async with await self.get_connection() as conn:
            try:
                await conn.executemany("""
                    INSERT INTO kr_audit (
                        stock_name, symbol, rcept_no, corp_cls, corp_code, corp_name,
                        bsns_year, adtor, adt_opinion, adt_reprt_spcmnt_matter,
                        emphs_matter, core_adt_matter, stlm_dt
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (corp_code, bsns_year, rcept_no) DO UPDATE SET
                        stock_name = EXCLUDED.stock_name,
                        symbol = EXCLUDED.symbol,
                        corp_cls = EXCLUDED.corp_cls,
                        corp_name = EXCLUDED.corp_name,
                        adtor = EXCLUDED.adtor,
                        adt_opinion = EXCLUDED.adt_opinion,
                        adt_reprt_spcmnt_matter = EXCLUDED.adt_reprt_spcmnt_matter,
                        emphs_matter = EXCLUDED.emphs_matter,
                        core_adt_matter = EXCLUDED.core_adt_matter,
                        stlm_dt = EXCLUDED.stlm_dt
                """, data_to_insert)
                return len(data_to_insert)
            except Exception as e:
                logger.error(f"Failed to save audit records: {e}")
                return 0

    async def process_company_audit_data(self, company):
        """Process individual company audit data with file-based skip logic and early break"""
        corp_code = company['corp_code']
        corp_name = company['corp_name']

        logger.info(f"Processing audit data for {corp_name} ({corp_code})")

        total_saved = 0
        skipped_count = 0

        for year in self.years:
            year_saved = 0
            found_data = False

            for reprt_code in self.report_codes:
                # File-based skip check
                if self.is_combination_processed(corp_code, year, reprt_code):
                    skipped_count += 1
                    logger.debug(f"  {year}/{reprt_code}: Already processed (SUCCESS), skipping")
                    found_data = True
                    break  # Skip other report codes for this year

                try:
                    audit_data = self.call_audit_api(corp_code, year, reprt_code)

                    if audit_data:
                        saved_count = await self.save_audit_data(audit_data, company)
                        year_saved += saved_count

                        if saved_count > 0:
                            # SUCCESS log
                            self.log_processed_combination(corp_code, year, reprt_code, "SUCCESS", saved_count)
                            logger.info(f"  {year}/{reprt_code}: {saved_count} records saved")
                            found_data = True
                            await asyncio.sleep(0.67)
                            break  # Success, skip other report codes
                        else:
                            # NO_DATA log (allows retry)
                            self.log_processed_combination(corp_code, year, reprt_code, "NO_DATA", 0)
                            logger.warning(f"  {year}/{reprt_code}: Data received but none saved")
                    else:
                        # NO_DATA log (allows retry)
                        self.log_processed_combination(corp_code, year, reprt_code, "NO_DATA", 0)
                        logger.debug(f"  {year}/{reprt_code}: No data from API")

                except DartAPILimitExceeded:
                    # Re-raise to stop current collector
                    raise
                except Exception as e:
                    logger.error(f"Error processing {corp_code}/{year}/{reprt_code}: {e}")
                    # No log = allows retry
                    continue

                await asyncio.sleep(0.67)

            total_saved += year_saved
            if year_saved > 0:
                logger.info(f"  {year}: {year_saved} total records")

            # Stop if data found for this year
            if found_data:
                logger.info(f"  Found data for year {year}, stopping year iteration for {corp_name}")
                break

        if skipped_count > 0:
            logger.info(f"{corp_name}: {total_saved} new records saved, {skipped_count} API calls skipped")
        else:
            logger.info(f"{corp_name}: {total_saved} total audit records collected")

        return total_saved

    async def collect_audit_data(self):
        """전체 감사 데이터 수집 실행"""
        logger.info("Starting comprehensive audit data collection")

        companies = await self.get_company_list()
        if not companies:
            logger.warning("No companies found in dart_company_info table")
            return

        total_companies = len(companies)
        total_records = 0
        processed_companies = 0

        for i, company in enumerate(companies, 1):
            logger.info(f"[{i}/{total_companies}] Processing company...")

            try:
                records = await self.process_company_audit_data(company)
                total_records += records
                processed_companies += 1

                # 더 자주 진행률 표시 (매 5개마다 또는 마지막)
                if i % 5 == 0 or i == total_companies:
                    progress_pct = (i / total_companies) * 100
                    logger.info(f"Progress: {i}/{total_companies} companies ({progress_pct:.1f}%), {total_records} total records collected")

            except Exception as e:
                logger.error(f"Failed to process company {company['corp_name']}: {e}")
                continue

        logger.info(f"=== AUDIT DATA COLLECTION COMPLETED ===")
        logger.info(f"Processed companies: {processed_companies}/{total_companies}")
        logger.info(f"Total audit records collected: {total_records}")

        await self.get_audit_statistics()

        # Close connection pool
        await self.close_pool()

    async def get_audit_statistics(self):
        """Audit data collection statistics"""
        async with await self.get_connection() as conn:
            # Year-wise statistics
            year_stats = await conn.fetch("""
                SELECT bsns_year, COUNT(*) as count
                FROM kr_audit
                GROUP BY bsns_year
                ORDER BY bsns_year
            """)

            # Audit opinion statistics
            opinion_stats = await conn.fetch("""
                SELECT adt_opinion, COUNT(*) as count
                FROM kr_audit
                GROUP BY adt_opinion
                ORDER BY count DESC
                LIMIT 10
            """)

            logger.info("=== AUDIT DATA STATISTICS ===")
            logger.info("Year-wise distribution:")
            for row in year_stats:
                logger.info(f"  {row['bsns_year']}: {row['count']} records")

            logger.info("Top audit opinions:")
            for row in opinion_stats:
                opinion = row['adt_opinion'] or 'NULL'
                logger.info(f"  {opinion[:50]}...: {row['count']} records")


class DartDividendsCollector:
    """DART Dividends Data Collector"""

    def __init__(self, api_key, database_url):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.dividends_api_url = "https://opendart.fss.or.kr/api/alotMatter.json"

        # 현재 연도부터 역순으로 시도 (첫 데이터 발견 시 중단)
        current_year = datetime.now().year
        self.years = [str(current_year - i) for i in range(3)]  # 현재, -1년, -2년
        self.report_codes = ['11013', '11012', '11014', '11011']

        # File-based log system
        from pathlib import Path
        # Use Railway Volume if available
        if os.getenv('RAILWAY_ENVIRONMENT'):
            self.log_file = Path('/app/log') / "dividends_processed_calls.log"
        else:
            self.log_file = Path(__file__).parent.parent / 'log' / "dividends_processed_calls.log"
        self.processed_combinations = self.load_processed_combinations()

        # Connection pool
        self.pool = None

    def load_processed_combinations(self):
        """Load SUCCESS combinations only (NO_DATA allows retry)"""
        processed = set()
        if not self.log_file.exists():
            logger.info(f"No existing log file found. Starting fresh.")
            return processed

        success_count = 0
        no_data_count = 0

        with open(self.log_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 5:
                    corp_code, year, reprt_code, timestamp, status = parts[0], parts[1], parts[2], parts[3], parts[4]
                    if status == "SUCCESS":
                        key = (corp_code, year, reprt_code)
                        processed.add(key)
                        success_count += 1
                    elif status == "NO_DATA":
                        no_data_count += 1

        logger.info(f"Loaded {success_count} SUCCESS combinations to skip")
        logger.info(f"Found {no_data_count} NO_DATA combinations (will retry)")
        return processed

    def is_combination_processed(self, corp_code, year, reprt_code):
        """O(1) memory lookup"""
        return (corp_code, year, reprt_code) in self.processed_combinations

    def log_processed_combination(self, corp_code, year, reprt_code, status, records_count):
        """Log API call result with immediate flush"""
        timestamp = datetime.now().isoformat()
        log_entry = f"{corp_code}|{year}|{reprt_code}|{timestamp}|{status}|{records_count}\n"

        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)
            f.flush()  # Crash safety

        if status == "SUCCESS":
            self.processed_combinations.add((corp_code, year, reprt_code))

    async def init_pool(self):
        """Initialize connection pool"""
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Connection pool initialized (min=5, max=20)")

    async def get_connection(self):
        """Get connection from pool"""
        if not self.pool:
            await self.init_pool()
        return self.pool.acquire()

    async def close_pool(self):
        """Close pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Connection pool closed")

    async def get_company_list(self):
        """Get all companies from dart_company_info"""
        async with await self.get_connection() as conn:
            query = """
            SELECT corp_code, corp_name, stock_code
            FROM dart_company_info
            WHERE corp_code IS NOT NULL
            AND corp_code != ''
            ORDER BY corp_code
            """
            rows = await conn.fetch(query)

            companies = []
            for row in rows:
                companies.append({
                    'corp_code': row['corp_code'],
                    'corp_name': row['corp_name'],
                    'stock_code': row['stock_code']
                })

            logger.info(f"Found {len(companies)} companies to process")
            return companies

    def call_dividends_api(self, corp_code, bsns_year, reprt_code):
        """배당 정보 API 호출"""
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bsns_year': bsns_year,
            'reprt_code': reprt_code
        }

        try:
            response = requests.get(self.dividends_api_url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == '000' and data.get('list'):
                return data['list']
            elif data.get('status') == '013':  # 검색된 데이터가 없습니다
                return []
            elif data.get('status') == '020':  # 일일 요청 한도 초과
                logger.error(f"DART API daily limit exceeded (20,000 calls/day): {data.get('message')}")
                raise DartAPILimitExceeded(f"DART API daily limit exceeded: {data.get('message')}")
            else:
                logger.warning(f"API Error for {corp_code}/{bsns_year}/{reprt_code}: {data.get('status')} - {data.get('message')}")
                return []

        except requests.RequestException as e:
            logger.error(f"Network error calling dividends API: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error calling dividends API: {e}")
            return []

    async def save_dividends_data(self, dividends_records, company_info):
        """Save dividends data using connection pool (batch insert with executemany)"""
        if not dividends_records:
            return 0

        # Numeric field processing
        def safe_decimal(value):
            if value is None or value == '' or value == '-':
                return None
            try:
                clean_value = str(value).replace(',', '').replace(' ', '')
                return float(clean_value) if clean_value else None
            except (ValueError, TypeError):
                return None

        # 데이터 검증 및 준비
        data_to_insert = []
        for record in dividends_records:
            thstrm_val = safe_decimal(record.get('thstrm'))
            frmtrm_val = safe_decimal(record.get('frmtrm'))
            lwfr_val = safe_decimal(record.get('lwfr'))

            # Only save if there is actual dividend data
            if thstrm_val is None and frmtrm_val is None and lwfr_val is None:
                continue

            # Date processing (stlm_dt)
            stlm_dt = record.get('stlm_dt')
            stlm_dt_value = None

            if stlm_dt:
                try:
                    if len(stlm_dt) == 8:  # YYYYMMDD
                        date_str = f"{stlm_dt[:4]}-{stlm_dt[4:6]}-{stlm_dt[6:8]}"
                        stlm_dt_value = datetime.strptime(date_str, '%Y-%m-%d').date()
                    elif len(stlm_dt) == 10 and '-' in stlm_dt:  # YYYY-MM-DD
                        stlm_dt_value = datetime.strptime(stlm_dt, '%Y-%m-%d').date()
                except ValueError:
                    stlm_dt_value = None

            data_to_insert.append((
                company_info['stock_code'],
                record.get('rcept_no'),
                record.get('corp_cls'),
                record.get('corp_code'),
                record.get('corp_name'),
                record.get('se'),
                record.get('stock_knd'),
                thstrm_val,
                frmtrm_val,
                lwfr_val,
                stlm_dt_value
            ))

        if not data_to_insert:
            return 0

        # 배치 삽입
        async with await self.get_connection() as conn:
            try:
                await conn.executemany("""
                    INSERT INTO kr_dividends (
                        symbol, rcept_no, corp_cls, corp_code, corp_name,
                        se, stock_knd, thstrm, frmtrm, lwfr, stlm_dt
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (corp_code, rcept_no, se, stock_knd) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        corp_cls = EXCLUDED.corp_cls,
                        corp_name = EXCLUDED.corp_name,
                        thstrm = EXCLUDED.thstrm,
                        frmtrm = EXCLUDED.frmtrm,
                        lwfr = EXCLUDED.lwfr,
                        stlm_dt = EXCLUDED.stlm_dt
                """, data_to_insert)
                return len(data_to_insert)
            except Exception as e:
                logger.error(f"Failed to save dividends records: {e}")
                return 0

    async def process_company_dividends_data(self, company):
        """Process individual company dividends data with file-based skip logic and early break"""
        corp_code = company['corp_code']
        corp_name = company['corp_name']

        logger.info(f"Processing dividends data for {corp_name} ({corp_code})")

        total_saved = 0
        skipped_count = 0

        for year in self.years:
            year_has_data = False

            for reprt_code in self.report_codes:
                # File-based skip check
                if self.is_combination_processed(corp_code, year, reprt_code):
                    skipped_count += 1
                    year_has_data = True
                    logger.debug(f"  {year}/{reprt_code}: Already processed (SUCCESS), skipping")
                    continue

                try:
                    dividends_data = self.call_dividends_api(corp_code, year, reprt_code)

                    if dividends_data:
                        saved_count = await self.save_dividends_data(dividends_data, company)
                        total_saved += saved_count

                        if saved_count > 0:
                            # SUCCESS log
                            self.log_processed_combination(corp_code, year, reprt_code, "SUCCESS", saved_count)
                            logger.info(f"  {year}/{reprt_code}: {saved_count} records saved")
                            year_has_data = True
                        else:
                            # NO_DATA log (allows retry)
                            self.log_processed_combination(corp_code, year, reprt_code, "NO_DATA", 0)
                            logger.warning(f"  {year}/{reprt_code}: Data received but none saved")
                    else:
                        # NO_DATA log (allows retry)
                        self.log_processed_combination(corp_code, year, reprt_code, "NO_DATA", 0)
                        logger.debug(f"  {year}/{reprt_code}: No data from API")

                except DartAPILimitExceeded:
                    # Re-raise to stop current collector
                    raise
                except Exception as e:
                    logger.error(f"Error processing {corp_code}/{year}/{reprt_code}: {e}")
                    # No log = allows retry
                    continue

                await asyncio.sleep(0.67)

            # Stop if data found for this year
            if year_has_data:
                logger.info(f"  Found data for year {year}, stopping year iteration for {corp_name}")
                break

        if skipped_count > 0:
            logger.info(f"{corp_name}: {total_saved} new records saved, {skipped_count} API calls skipped")
        else:
            logger.info(f"{corp_name}: {total_saved} total dividends records collected")

        return total_saved

    async def collect_dividends_data(self):
        """전체 배당 데이터 수집 실행"""
        logger.info("Starting comprehensive dividends data collection")

        companies = await self.get_company_list()
        if not companies:
            logger.warning("No companies found to process")
            return

        total_companies = len(companies)
        total_records = 0
        processed_companies = 0

        for i, company in enumerate(companies, 1):
            logger.info(f"[{i}/{total_companies}] Processing company...")

            try:
                records = await self.process_company_dividends_data(company)
                total_records += records
                processed_companies += 1

                # 더 자주 진행률 표시 (매 5개마다 또는 마지막)
                if i % 5 == 0 or i == total_companies:
                    progress_pct = (i / total_companies) * 100
                    logger.info(f"Progress: {i}/{total_companies} companies ({progress_pct:.1f}%), {total_records} total records collected")

            except Exception as e:
                logger.error(f"Failed to process company {company['corp_name']}: {e}")
                continue

        logger.info(f"=== DIVIDENDS DATA COLLECTION COMPLETED ===")
        logger.info(f"Processed companies: {processed_companies}/{total_companies}")
        logger.info(f"Total dividends records collected: {total_records}")

        await self.get_dividends_statistics()

        # Close connection pool
        await self.close_pool()

    async def get_dividends_statistics(self):
        """Dividends data collection statistics"""
        async with await self.get_connection() as conn:
            # Year-wise statistics (based on stlm_dt)
            year_stats = await conn.fetch("""
                SELECT EXTRACT(YEAR FROM stlm_dt) as year, COUNT(*) as count
                FROM kr_dividends
                WHERE stlm_dt IS NOT NULL
                GROUP BY EXTRACT(YEAR FROM stlm_dt)
                ORDER BY year
            """)

            # Category statistics
            se_stats = await conn.fetch("""
                SELECT se, COUNT(*) as count
                FROM kr_dividends
                GROUP BY se
                ORDER BY count DESC
                LIMIT 10
            """)

            # Stock type statistics
            stock_knd_stats = await conn.fetch("""
                SELECT stock_knd, COUNT(*) as count
                FROM kr_dividends
                GROUP BY stock_knd
                ORDER BY count DESC
                LIMIT 10
            """)

            logger.info("=== DIVIDENDS DATA STATISTICS ===")
            logger.info("Year-wise distribution:")
            for row in year_stats:
                year = int(row['year']) if row['year'] else 'NULL'
                logger.info(f"  {year}: {row['count']} records")

            logger.info("Top dividend types (se):")
            for row in se_stats:
                se = row['se'] or 'NULL'
                logger.info(f"  {se[:30]}...: {row['count']} records")

            logger.info("Top stock types:")
            for row in stock_knd_stats:
                stock_knd = row['stock_knd'] or 'NULL'
                logger.info(f"  {stock_knd}: {row['count']} records")


class DartShareholderCollector:
    """DART Shareholder Data Collector"""

    def __init__(self, api_key, database_url):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.shareholder_api_url = "https://opendart.fss.or.kr/api/hyslrSttus.json"

        # 현재 연도부터 역순으로 시도 (첫 데이터 발견 시 중단)
        current_year = datetime.now().year
        self.years = [str(current_year - i) for i in range(3)]  # 현재, -1년, -2년
        self.report_codes = ['11013', '11012', '11014', '11011']

        # File-based log system
        from pathlib import Path
        # Use Railway Volume if available
        if os.getenv('RAILWAY_ENVIRONMENT'):
            self.log_file = Path('/app/log') / "shareholder_processed_calls.log"
        else:
            self.log_file = Path(__file__).parent.parent / 'log' / "shareholder_processed_calls.log"
        self.processed_combinations = self.load_processed_combinations()

        # Connection pool
        self.pool = None

    def load_processed_combinations(self):
        """Load SUCCESS combinations only (NO_DATA allows retry)"""
        processed = set()
        if not self.log_file.exists():
            logger.info(f"No existing log file found. Starting fresh.")
            return processed

        success_count = 0
        no_data_count = 0

        with open(self.log_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 5:
                    corp_code, year, reprt_code, timestamp, status = parts[0], parts[1], parts[2], parts[3], parts[4]
                    if status == "SUCCESS":
                        key = (corp_code, year, reprt_code)
                        processed.add(key)
                        success_count += 1
                    elif status == "NO_DATA":
                        no_data_count += 1

        logger.info(f"Loaded {success_count} SUCCESS combinations to skip")
        logger.info(f"Found {no_data_count} NO_DATA combinations (will retry)")
        return processed

    def is_combination_processed(self, corp_code, year, reprt_code):
        """O(1) memory lookup"""
        return (corp_code, year, reprt_code) in self.processed_combinations

    def log_processed_combination(self, corp_code, year, reprt_code, status, records_count):
        """Log API call result with immediate flush"""
        timestamp = datetime.now().isoformat()
        log_entry = f"{corp_code}|{year}|{reprt_code}|{timestamp}|{status}|{records_count}\n"

        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)
            f.flush()  # Crash safety

        if status == "SUCCESS":
            self.processed_combinations.add((corp_code, year, reprt_code))

    async def init_pool(self):
        """Initialize connection pool"""
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Connection pool initialized (min=5, max=20)")

    async def get_connection(self):
        """Get connection from pool"""
        if not self.pool:
            await self.init_pool()
        return self.pool.acquire()

    async def close_pool(self):
        """Close pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Connection pool closed")

    async def get_company_list(self):
        """Get all companies from dart_company_info"""
        async with await self.get_connection() as conn:
            query = """
            SELECT corp_code, corp_name, stock_code
            FROM dart_company_info
            WHERE corp_code IS NOT NULL
            AND corp_code != ''
            ORDER BY corp_code
            """
            rows = await conn.fetch(query)

            companies = []
            for row in rows:
                companies.append({
                    'corp_code': row['corp_code'],
                    'corp_name': row['corp_name'],
                    'stock_code': row['stock_code']
                })

            logger.info(f"Found {len(companies)} companies to process")
            return companies

    def call_shareholder_api(self, corp_code, bsns_year, reprt_code):
        """최대주주 현황 API 호출"""
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bsns_year': bsns_year,
            'reprt_code': reprt_code
        }

        try:
            response = requests.get(self.shareholder_api_url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == '000' and data.get('list'):
                return data['list']
            elif data.get('status') == '013':  # 검색된 데이터가 없습니다
                return []
            elif data.get('status') == '020':  # 일일 요청 한도 초과
                logger.error(f"DART API daily limit exceeded (20,000 calls/day): {data.get('message')}")
                raise DartAPILimitExceeded(f"DART API daily limit exceeded: {data.get('message')}")
            else:
                logger.warning(f"API Error for {corp_code}/{bsns_year}/{reprt_code}: {data.get('status')} - {data.get('message')}")
                return []

        except requests.RequestException as e:
            logger.error(f"Network error calling shareholder API: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error calling shareholder API: {e}")
            return []

    async def save_shareholder_data(self, shareholder_records, company_info):
        """Save shareholder data using connection pool (batch insert with executemany)"""
        if not shareholder_records:
            return 0

        # Numeric field processing functions
        def safe_bigint(value):
            if value is None or value == '' or value == '-':
                return None
            try:
                clean_value = str(value).replace(',', '').replace(' ', '')
                return int(clean_value) if clean_value else None
            except (ValueError, TypeError):
                return None

        def safe_decimal(value):
            if value is None or value == '' or value == '-':
                return None
            try:
                clean_value = str(value).replace(',', '').replace(' ', '')
                return float(clean_value) if clean_value else None
            except (ValueError, TypeError):
                return None

        # 데이터 검증 및 준비
        data_to_insert = []
        for record in shareholder_records:
            # Check if there is actual shareholder data
            nm = record.get('nm', '').strip()
            relate = record.get('relate', '').strip()

            if not nm and not relate:
                continue

            # Date processing (stlm_dt)
            stlm_dt = record.get('stlm_dt')
            stlm_dt_value = None

            if stlm_dt:
                try:
                    if len(stlm_dt) == 8:  # YYYYMMDD
                        date_str = f"{stlm_dt[:4]}-{stlm_dt[4:6]}-{stlm_dt[6:8]}"
                        stlm_dt_value = datetime.strptime(date_str, '%Y-%m-%d').date()
                    elif len(stlm_dt) == 10 and '-' in stlm_dt:  # YYYY-MM-DD
                        stlm_dt_value = datetime.strptime(stlm_dt, '%Y-%m-%d').date()
                except ValueError:
                    stlm_dt_value = None

            data_to_insert.append((
                company_info['stock_code'],
                record.get('rcept_no'),
                record.get('corp_cls'),
                record.get('corp_code'),
                record.get('corp_name'),
                nm,
                relate,
                record.get('stock_knd'),
                safe_bigint(record.get('bsis_posesn_stock_co')),
                safe_decimal(record.get('bsis_posesn_stock_qota_rt')),
                safe_bigint(record.get('trmend_posesn_stock_co')),
                safe_decimal(record.get('trmend_posesn_stock_qota_rt')),
                record.get('rm'),
                stlm_dt_value
            ))

        if not data_to_insert:
            return 0

        # 배치 삽입
        async with await self.get_connection() as conn:
            try:
                await conn.executemany("""
                    INSERT INTO kr_largest_shareholder (
                        symbol, rcept_no, corp_cls, corp_code, corp_name,
                        nm, relate, stock_knd,
                        bsis_posesn_stock_co, bsis_posesn_stock_qota_rt,
                        trmend_posesn_stock_co, trmend_posesn_stock_qota_rt,
                        rm, stlm_dt
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (corp_code, rcept_no, nm, relate) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        corp_cls = EXCLUDED.corp_cls,
                        corp_name = EXCLUDED.corp_name,
                        stock_knd = EXCLUDED.stock_knd,
                        bsis_posesn_stock_co = EXCLUDED.bsis_posesn_stock_co,
                        bsis_posesn_stock_qota_rt = EXCLUDED.bsis_posesn_stock_qota_rt,
                        trmend_posesn_stock_co = EXCLUDED.trmend_posesn_stock_co,
                        trmend_posesn_stock_qota_rt = EXCLUDED.trmend_posesn_stock_qota_rt,
                        rm = EXCLUDED.rm,
                        stlm_dt = EXCLUDED.stlm_dt
                """, data_to_insert)
                return len(data_to_insert)
            except Exception as e:
                logger.error(f"Failed to save shareholder records: {e}")
                return 0

    async def process_company_shareholder_data(self, company):
        """Process individual company shareholder data with file-based skip logic and early break"""
        corp_code = company['corp_code']
        corp_name = company['corp_name']

        logger.info(f"Processing shareholder data for {corp_name} ({corp_code})")

        total_saved = 0
        skipped_count = 0

        for year in self.years:
            year_has_data = False

            for reprt_code in self.report_codes:
                # File-based skip check
                if self.is_combination_processed(corp_code, year, reprt_code):
                    skipped_count += 1
                    year_has_data = True
                    logger.debug(f"  {year}/{reprt_code}: Already processed (SUCCESS), skipping")
                    continue

                try:
                    shareholder_data = self.call_shareholder_api(corp_code, year, reprt_code)

                    if shareholder_data:
                        saved_count = await self.save_shareholder_data(shareholder_data, company)
                        total_saved += saved_count

                        if saved_count > 0:
                            # SUCCESS log
                            self.log_processed_combination(corp_code, year, reprt_code, "SUCCESS", saved_count)
                            logger.info(f"  {year}/{reprt_code}: {saved_count} records saved")
                            year_has_data = True
                        else:
                            # NO_DATA log (allows retry)
                            self.log_processed_combination(corp_code, year, reprt_code, "NO_DATA", 0)
                            logger.warning(f"  {year}/{reprt_code}: Data received but none saved")
                    else:
                        # NO_DATA log (allows retry)
                        self.log_processed_combination(corp_code, year, reprt_code, "NO_DATA", 0)
                        logger.debug(f"  {year}/{reprt_code}: No data from API")

                except DartAPILimitExceeded:
                    # Re-raise to stop current collector
                    raise
                except Exception as e:
                    logger.error(f"Error processing {corp_code}/{year}/{reprt_code}: {e}")
                    # No log = allows retry
                    continue

                await asyncio.sleep(0.67)

            # Stop if data found for this year
            if year_has_data:
                logger.info(f"  Found data for year {year}, stopping year iteration for {corp_name}")
                break

        if skipped_count > 0:
            logger.info(f"{corp_name}: {total_saved} new records saved, {skipped_count} API calls skipped")
        else:
            logger.info(f"{corp_name}: {total_saved} total shareholder records collected")

        return total_saved

    async def collect_shareholder_data(self):
        """전체 주주 데이터 수집 실행"""
        logger.info("Starting comprehensive shareholder data collection")

        companies = await self.get_company_list()
        if not companies:
            logger.warning("No companies found to process")
            return

        total_companies = len(companies)
        total_records = 0
        processed_companies = 0

        for i, company in enumerate(companies, 1):
            logger.info(f"[{i}/{total_companies}] Processing company...")

            try:
                records = await self.process_company_shareholder_data(company)
                total_records += records
                processed_companies += 1

                # 더 자주 진행률 표시 (매 5개마다 또는 마지막)
                if i % 5 == 0 or i == total_companies:
                    progress_pct = (i / total_companies) * 100
                    logger.info(f"Progress: {i}/{total_companies} companies ({progress_pct:.1f}%), {total_records} total records collected")

            except Exception as e:
                logger.error(f"Failed to process company {company['corp_name']}: {e}")
                continue

        logger.info(f"=== SHAREHOLDER DATA COLLECTION COMPLETED ===")
        logger.info(f"Processed companies: {processed_companies}/{total_companies}")
        logger.info(f"Total shareholder records collected: {total_records}")

        await self.get_shareholder_statistics()

        # Close connection pool
        await self.close_pool()

    async def get_shareholder_statistics(self):
        """Shareholder data collection statistics"""
        async with await self.get_connection() as conn:
            # Year-wise statistics (based on stlm_dt)
            year_stats = await conn.fetch("""
                SELECT EXTRACT(YEAR FROM stlm_dt) as year, COUNT(*) as count
                FROM kr_largest_shareholder
                WHERE stlm_dt IS NOT NULL
                GROUP BY EXTRACT(YEAR FROM stlm_dt)
                ORDER BY year
            """)

            # Relation statistics
            relate_stats = await conn.fetch("""
                SELECT relate, COUNT(*) as count
                FROM kr_largest_shareholder
                GROUP BY relate
                ORDER BY count DESC
                LIMIT 10
            """)

            # Stock type statistics
            stock_knd_stats = await conn.fetch("""
                SELECT stock_knd, COUNT(*) as count
                FROM kr_largest_shareholder
                GROUP BY stock_knd
                ORDER BY count DESC
                LIMIT 10
            """)

            logger.info("=== SHAREHOLDER DATA STATISTICS ===")
            logger.info("Year-wise distribution:")
            for row in year_stats:
                year = int(row['year']) if row['year'] else 'NULL'
                logger.info(f"  {year}: {row['count']} records")

            logger.info("Top relations:")
            for row in relate_stats:
                relate = row['relate'] or 'NULL'
                logger.info(f"  {relate[:20]}...: {row['count']} records")

            logger.info("Top stock types:")
            for row in stock_knd_stats:
                stock_knd = row['stock_knd'] or 'NULL'
                logger.info(f"  {stock_knd}: {row['count']} records")


class DartExecutiveCollector:
    """DART 임원 현황 수집기"""

    def __init__(self, api_key, database_url):
        self.api_key = api_key
        # Convert postgresql+asyncpg:// to postgresql://
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.executive_api_url = "https://opendart.fss.or.kr/api/exctvSttus.json"

        # 현재 연도부터 역순으로 시도 (첫 데이터 발견 시 중단)
        current_year = datetime.now().year
        self.years = [str(current_year - i) for i in range(3)]  # 현재, -1년, -2년
        self.report_codes = ['11013', '11012', '11014', '11011']  # 1분기, 반기, 3분기, 사업보고서

        # 파일 기반 로그 시스템
        from pathlib import Path
        # Use Railway Volume if available
        if os.getenv('RAILWAY_ENVIRONMENT'):
            self.log_file = Path('/app/log') / "executive_processed_calls.log"
        else:
            self.log_file = Path(__file__).parent.parent / 'log' / "executive_processed_calls.log"
        self.processed_combinations = self.load_processed_combinations()

        # Connection pool
        self.pool = None

    def load_processed_combinations(self):
        """로그 파일에서 SUCCESS 조합만 로드 (NO_DATA는 재시도 허용)"""
        processed = set()
        if not self.log_file.exists():
            logger.info(f"No existing log file found. Starting fresh.")
            return processed

        success_count = 0
        no_data_count = 0

        with open(self.log_file, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 5:
                    corp_code, year, reprt_code, timestamp, status = parts[0], parts[1], parts[2], parts[3], parts[4]
                    if status == "SUCCESS":
                        key = (corp_code, year, reprt_code)
                        processed.add(key)
                        success_count += 1
                    elif status == "NO_DATA":
                        no_data_count += 1

        logger.info(f"Loaded {success_count} SUCCESS combinations to skip")
        logger.info(f"Found {no_data_count} NO_DATA combinations (will retry)")
        return processed

    def is_combination_processed(self, corp_code, year, reprt_code):
        """메모리에서 O(1) 조회"""
        return (corp_code, year, reprt_code) in self.processed_combinations

    def log_processed_combination(self, corp_code, year, reprt_code, status, records_count):
        """API 호출 결과를 로그 파일에 기록 (즉시 flush)"""
        timestamp = datetime.now().isoformat()
        log_entry = f"{corp_code}|{year}|{reprt_code}|{timestamp}|{status}|{records_count}\n"

        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)
            f.flush()  # 크래시 시에도 데이터 보존

        if status == "SUCCESS":
            self.processed_combinations.add((corp_code, year, reprt_code))

    async def init_pool(self):
        """Connection pool 초기화"""
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Connection pool initialized (min=5, max=20)")

    async def get_connection(self):
        """Pool에서 연결 가져오기"""
        if not self.pool:
            await self.init_pool()
        return self.pool.acquire()

    async def close_pool(self):
        """Pool 종료"""
        if self.pool:
            await self.pool.close()
            logger.info("Connection pool closed")

    async def get_company_list(self):
        """dart_company_info 테이블에서 모든 회사 목록 조회"""
        async with await self.get_connection() as conn:
            # 모든 회사 목록 조회
            query = """
            SELECT corp_code, corp_name, stock_code
            FROM dart_company_info
            WHERE corp_code IS NOT NULL
            AND corp_code != ''
            ORDER BY corp_code
            """
            rows = await conn.fetch(query)

            companies = []
            for row in rows:
                companies.append({
                    'corp_code': row['corp_code'],
                    'corp_name': row['corp_name'],
                    'stock_code': row['stock_code']
                })

            logger.info(f"Found {len(companies)} companies to process")
            return companies

    def call_executive_api(self, corp_code, bsns_year, reprt_code):
        """임원 현황 API 호출"""
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bsns_year': bsns_year,
            'reprt_code': reprt_code
        }

        try:
            response = requests.get(self.executive_api_url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == '000' and data.get('list'):
                return data['list']
            elif data.get('status') == '013':  # 검색된 데이터가 없습니다
                return []
            elif data.get('status') == '020':  # 일일 요청 한도 초과
                logger.error(f"DART API daily limit exceeded (20,000 calls/day): {data.get('message')}")
                raise DartAPILimitExceeded(f"DART API daily limit exceeded: {data.get('message')}")
            else:
                logger.warning(f"API Error for {corp_code}/{bsns_year}/{reprt_code}: {data.get('status')} - {data.get('message')}")
                return []

        except requests.RequestException as e:
            logger.error(f"Network error calling executive API: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error calling executive API: {e}")
            return []

    async def save_executive_data(self, executive_records, company_info):
        """임원 데이터를 데이터베이스에 저장 (Connection pool 사용, batch insert with executemany)"""
        if not executive_records:
            return 0

        # 데이터 검증 및 준비
        data_to_insert = []
        for record in executive_records:
            # 실제 임원 데이터가 있는지 확인 (성명이 있으면 의미 있는 데이터)
            nm = record.get('nm', '').strip()

            if not nm:
                continue

            # 날짜 처리 (stlm_dt)
            stlm_dt = record.get('stlm_dt')
            stlm_dt_value = None

            if stlm_dt:
                try:
                    if len(stlm_dt) == 8:  # YYYYMMDD
                        date_str = f"{stlm_dt[:4]}-{stlm_dt[4:6]}-{stlm_dt[6:8]}"
                        stlm_dt_value = datetime.strptime(date_str, '%Y-%m-%d').date()
                    elif len(stlm_dt) == 10 and '-' in stlm_dt:  # YYYY-MM-DD
                        stlm_dt_value = datetime.strptime(stlm_dt, '%Y-%m-%d').date()
                except ValueError:
                    stlm_dt_value = None

            data_to_insert.append((
                company_info['stock_code'],
                record.get('rcept_no'),
                record.get('corp_cls'),
                record.get('corp_code'),
                record.get('corp_name'),
                nm,
                record.get('sexdstn'),
                record.get('birth_ym'),
                record.get('ofcps'),
                record.get('rgist_exctv_at'),
                record.get('fte_at'),
                record.get('chrg_job'),
                record.get('main_career'),
                record.get('mxmm_shrholdr_relate'),
                record.get('hffc_pd'),
                record.get('tenure_end_on'),
                stlm_dt_value
            ))

        if not data_to_insert:
            return 0

        # 배치 삽입
        async with await self.get_connection() as conn:
            try:
                await conn.executemany("""
                    INSERT INTO kr_executive (
                        symbol, rcept_no, corp_cls, corp_code, corp_name,
                        nm, sexdstn, birth_ym, ofcps, rgist_exctv_at,
                        fte_at, chrg_job, main_career, mxmm_shrholdr_relate,
                        hffc_pd, tenure_end_on, stlm_dt
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                    ON CONFLICT (corp_code, rcept_no, nm) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        corp_cls = EXCLUDED.corp_cls,
                        corp_name = EXCLUDED.corp_name,
                        sexdstn = EXCLUDED.sexdstn,
                        birth_ym = EXCLUDED.birth_ym,
                        ofcps = EXCLUDED.ofcps,
                        rgist_exctv_at = EXCLUDED.rgist_exctv_at,
                        fte_at = EXCLUDED.fte_at,
                        chrg_job = EXCLUDED.chrg_job,
                        main_career = EXCLUDED.main_career,
                        mxmm_shrholdr_relate = EXCLUDED.mxmm_shrholdr_relate,
                        hffc_pd = EXCLUDED.hffc_pd,
                        tenure_end_on = EXCLUDED.tenure_end_on,
                        stlm_dt = EXCLUDED.stlm_dt
                """, data_to_insert)
                return len(data_to_insert)
            except Exception as e:
                logger.error(f"Failed to save executive records: {e}")
                return 0

    async def process_company_executive_data(self, company):
        """개별 회사의 임원 데이터 수집 (파일 기반 skip 로직, 연도별 조기 중단)"""
        corp_code = company['corp_code']
        corp_name = company['corp_name']

        logger.info(f"Processing executive data for {corp_name} ({corp_code})")

        total_saved = 0
        skipped_count = 0

        for year in self.years:
            year_has_data = False

            for reprt_code in self.report_codes:
                # 파일 기반 로그로 이미 처리된 조합인지 확인
                if self.is_combination_processed(corp_code, year, reprt_code):
                    skipped_count += 1
                    year_has_data = True  # 이미 처리된 데이터도 "데이터 있음"으로 간주
                    logger.debug(f"  {year}/{reprt_code}: Already processed (SUCCESS), skipping")
                    continue

                try:
                    # API 호출
                    executive_data = self.call_executive_api(corp_code, year, reprt_code)

                    if executive_data:
                        saved_count = await self.save_executive_data(executive_data, company)
                        total_saved += saved_count

                        if saved_count > 0:
                            # SUCCESS 로그 기록
                            self.log_processed_combination(corp_code, year, reprt_code, "SUCCESS", saved_count)
                            logger.info(f"  {year}/{reprt_code}: {saved_count} records saved")
                            year_has_data = True
                        else:
                            # 데이터는 있었지만 저장 실패 - NO_DATA로 기록 (재시도 허용)
                            self.log_processed_combination(corp_code, year, reprt_code, "NO_DATA", 0)
                            logger.warning(f"  {year}/{reprt_code}: Data received but none saved")
                    else:
                        # NO_DATA 로그 기록 (재시도 허용)
                        self.log_processed_combination(corp_code, year, reprt_code, "NO_DATA", 0)
                        logger.debug(f"  {year}/{reprt_code}: No data from API")

                except Exception as e:
                    logger.error(f"Error processing {corp_code}/{year}/{reprt_code}: {e}")
                    # 에러 발생 시 로그 기록하지 않음 (재시도 허용)
                    continue

                # API 호출 제한 (90회/분)
                await asyncio.sleep(0.67)

            # 이 연도에서 데이터를 찾았으면 더 과거로 가지 않음
            if year_has_data:
                logger.info(f"  Found data for year {year}, stopping year iteration for {corp_name}")
                break

        if skipped_count > 0:
            logger.info(f"{corp_name}: {total_saved} new records saved, {skipped_count} API calls skipped")
        else:
            logger.info(f"{corp_name}: {total_saved} total executive records collected")

        return total_saved

    async def collect_executive_data(self):
        """전체 임원 데이터 수집 실행"""
        logger.info("Starting comprehensive executive data collection")

        companies = await self.get_company_list()
        if not companies:
            logger.warning("No companies found to process")
            return

        total_companies = len(companies)
        total_records = 0
        processed_companies = 0

        for i, company in enumerate(companies, 1):
            logger.info(f"[{i}/{total_companies}] Processing company...")

            try:
                records = await self.process_company_executive_data(company)
                total_records += records
                processed_companies += 1

                # 더 자주 진행률 표시 (매 5개마다 또는 마지막)
                if i % 5 == 0 or i == total_companies:
                    progress_pct = (i / total_companies) * 100
                    logger.info(f"Progress: {i}/{total_companies} companies ({progress_pct:.1f}%), {total_records} total records collected")

            except Exception as e:
                logger.error(f"Failed to process company {company['corp_name']}: {e}")
                continue

        logger.info(f"=== EXECUTIVE DATA COLLECTION COMPLETED ===")
        logger.info(f"Processed companies: {processed_companies}/{total_companies}")
        logger.info(f"Total executive records collected: {total_records}")

        # 최종 통계
        await self.get_executive_statistics()

        # Connection pool 종료
        await self.close_pool()

    async def get_executive_statistics(self):
        """임원 데이터 수집 통계"""
        async with await self.get_connection() as conn:
            # 연도별 통계 (stlm_dt 기준)
            year_stats = await conn.fetch("""
                SELECT EXTRACT(YEAR FROM stlm_dt) as year, COUNT(*) as count
                FROM kr_executive
                WHERE stlm_dt IS NOT NULL
                GROUP BY EXTRACT(YEAR FROM stlm_dt)
                ORDER BY year
            """)

            # 직위별 통계
            ofcps_stats = await conn.fetch("""
                SELECT ofcps, COUNT(*) as count
                FROM kr_executive
                WHERE ofcps IS NOT NULL AND ofcps != ''
                GROUP BY ofcps
                ORDER BY count DESC
                LIMIT 10
            """)

            # 성별 통계
            sex_stats = await conn.fetch("""
                SELECT sexdstn, COUNT(*) as count
                FROM kr_executive
                WHERE sexdstn IS NOT NULL AND sexdstn != ''
                GROUP BY sexdstn
                ORDER BY count DESC
            """)

            logger.info("=== EXECUTIVE DATA STATISTICS ===")
            logger.info("Year-wise distribution:")
            for row in year_stats:
                year = int(row['year']) if row['year'] else 'NULL'
                logger.info(f"  {year}: {row['count']} records")

            logger.info("Top positions:")
            for row in ofcps_stats:
                ofcps = row['ofcps'] or 'NULL'
                logger.info(f"  {ofcps[:30]}...: {row['count']} records")

            logger.info("Gender distribution:")
            for row in sex_stats:
                sexdstn = row['sexdstn'] or 'NULL'
                logger.info(f"  {sexdstn}: {row['count']} records")


# 전역 수집 함수들
async def collect_dart_data(database_url, dart_api_key, collect_companies=True, collect_financials=True, is_initial_run=True):
    """DART 기본 데이터 (회사 정보 + 재무제표) 수집"""
    if collect_companies:
        await collect_company_info(database_url, dart_api_key)

    if collect_financials:
        await collect_financial_data(database_url, dart_api_key)


async def collect_stock_acquisition_data(database_url, dart_api_key=None, is_initial_run=True):
    """DART 자기주식 취득/처분 현황 데이터 수집"""
    logger.info("DART Stock Acquisition Data Collection Started")

    if not dart_api_key:
        logger.error("DART_API_KEY is required for stock acquisition data collection")
        return

    stockacquisition_collector = DartStockAcquisitionCollector(dart_api_key, database_url)

    try:
        # 자기주식 데이터 수집 실행
        await stockacquisition_collector.collect_stockacquisition_data()
        logger.info("DART Stock Acquisition Data Collection Completed")

    except DartAPILimitExceeded as e:
        logger.warning(f"DART API daily limit reached. Collection stopped gracefully: {e}")
        logger.info("DART Stock Acquisition Data Collection Completed (with API limit)")
        return  # 정상 종료
    except Exception as e:
        logger.error(f"DART Stock Acquisition Data Collection Failed: {e}")
        raise


async def collect_audit_data(database_url, dart_api_key=None):
    """DART 감사인 및 감사의견 데이터 수집"""
    logger.info("DART Audit Data Collection Started")

    if not dart_api_key:
        logger.error("DART_API_KEY is required for audit data collection")
        return

    audit_collector = DartAuditCollector(dart_api_key, database_url)

    try:
        # 감사 데이터 수집 실행
        await audit_collector.collect_audit_data()
        logger.info("DART Audit Data Collection Completed")

    except DartAPILimitExceeded as e:
        logger.warning(f"DART API daily limit reached. Collection stopped gracefully: {e}")
        logger.info("DART Audit Data Collection Completed (with API limit)")
        return  # 정상 종료
    except Exception as e:
        logger.error(f"DART Audit Data Collection Failed: {e}")
        raise


async def collect_dividends_data(database_url, dart_api_key=None):
    """DART 배당 데이터 수집"""
    logger.info("DART Dividends Data Collection Started")

    if not dart_api_key:
        logger.error("DART_API_KEY is required for dividends data collection")
        return

    dividends_collector = DartDividendsCollector(dart_api_key, database_url)

    try:
        # 배당 데이터 수집 실행
        await dividends_collector.collect_dividends_data()
        logger.info("DART Dividends Data Collection Completed")

    except DartAPILimitExceeded as e:
        logger.warning(f"DART API daily limit reached. Collection stopped gracefully: {e}")
        logger.info("DART Dividends Data Collection Completed (with API limit)")
        return  # 정상 종료
    except Exception as e:
        logger.error(f"DART Dividends Data Collection Failed: {e}")
        raise


async def collect_shareholder_data(database_url, dart_api_key):
    """DART 최대주주 현황 데이터 수집"""
    logger.info("DART Shareholder Data Collection Started")

    if not dart_api_key:
        logger.error("DART_API_KEY is required for shareholder data collection")
        return

    shareholder_collector = DartShareholderCollector(dart_api_key, database_url)

    try:
        # 주주 데이터 수집 실행
        await shareholder_collector.collect_shareholder_data()
        logger.info("DART Shareholder Data Collection Completed")

    except DartAPILimitExceeded as e:
        logger.warning(f"DART API daily limit reached. Collection stopped gracefully: {e}")
        logger.info("DART Shareholder Data Collection Completed (with API limit)")
        return  # 정상 종료
    except Exception as e:
        logger.error(f"DART Shareholder Data Collection Failed: {e}")
        raise


async def collect_executive_data(database_url, dart_api_key):
    """DART 임원 현황 데이터 수집"""
    logger.info("DART Executive Data Collection Started")

    if not dart_api_key:
        logger.error("DART_API_KEY is required for executive data collection")
        return

    executive_collector = DartExecutiveCollector(dart_api_key, database_url)

    try:
        # 임원 데이터 수집 실행
        await executive_collector.collect_executive_data()
        logger.info("DART Executive Data Collection Completed")

    except DartAPILimitExceeded as e:
        logger.warning(f"DART API daily limit reached. Collection stopped gracefully: {e}")
        logger.info("DART Executive Data Collection Completed (with API limit)")
        return  # 정상 종료
    except Exception as e:
        logger.error(f"DART Executive Data Collection Failed: {e}")
        raise


async def collect_financial_data(database_url, dart_api_key):
    """DART 재무제표 상세 데이터 수집 (kr_financial_position 테이블용)"""
    logger.info("DART Financial Position Data Collection Started")

    if not dart_api_key:
        logger.error("DART_API_KEY is required for financial data collection")
        return

    financial_manager = DartFinancialManager(dart_api_key, database_url)

    try:
        # 재무데이터 수집 실행
        await financial_manager.run_collection()
        logger.info("DART Financial Position Data Collection Completed")

    except DartAPILimitExceeded as e:
        logger.warning(f"DART API daily limit reached. Collection stopped gracefully: {e}")
        logger.info("DART Financial Position Data Collection Completed (with API limit)")
        return  # 정상 종료
    except Exception as e:
        logger.error(f"DART Financial Position Data Collection Failed: {e}")
        raise


async def collect_complete_data(database_url, dart_api_key):
    """DART 완료된 모든 데이터 수집 실행 함수"""
    logger.info("Starting DART complete data collection")

    # 회사 정보 수집 (기본)
    await collect_dart_data(database_url, dart_api_key, collect_companies=True, collect_financials=False)

    # 각종 DART 데이터 수집
    await collect_audit_data(database_url, dart_api_key)
    await collect_dividends_data(database_url, dart_api_key)
    await collect_shareholder_data(database_url, dart_api_key)
    await collect_executive_data(database_url, dart_api_key)
    await collect_stock_acquisition_data(database_url, dart_api_key)
    await collect_financial_data(database_url, dart_api_key)

    logger.info("DART complete data collection finished")


if __name__ == "__main__":
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    if database_url.startswith('postgresql+asyncpg://'):
        database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

    dart_api_key = os.getenv('DART_API_KEY')
    if not dart_api_key:
        raise ValueError("DART_API_KEY environment variable is required")

    # 완전한 DART 데이터 수집 실행
    asyncio.run(collect_complete_data(database_url, dart_api_key))