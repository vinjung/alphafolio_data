# -*- coding: utf-8 -*-
"""
DART fnlttSinglAcnt API - Single Company Financial Statements
Collects financial statements data in clean JSON format
"""

import asyncio
import asyncpg
import requests
import os
import logging
import json
from datetime import datetime

# Use centralized logger from main module
logger = logging.getLogger(__name__)


# Custom Exceptions
class DartAPILimitExceeded(Exception):
    """DART API 일일 한도 초과 예외"""
    pass


class DartFinancialStatementsCollector:
    """DART Single Company Financial Statements Collector"""

    def __init__(self, api_key, database_url):
        self.api_key = api_key
        self.database_url = database_url
        self.financial_api_url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"

        # Use Railway volume path if available, otherwise local
        log_dir = "/app/log" if os.path.exists("/app/log") else "log"
        os.makedirs(log_dir, exist_ok=True)
        self.progress_file = os.path.join(log_dir, "dart_financial_progress.json")

        # Report codes
        self.report_codes = {
            '11013': '1st Quarter Report',
            '11012': 'Semi-Annual Report',
            '11014': '3rd Quarter Report',
            '11011': 'Annual Report'
        }

        # Financial statement types
        self.fs_types = {
            'CFS': 'Consolidated',
            'OFS': 'Separate'
        }

        # Load progress
        self.progress = self.load_progress()

    def load_progress(self):
        """Load progress from JSON file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                    print(f"[OK] Progress file loaded: {len(progress)} companies tracked")
                    return progress
            except Exception as e:
                print(f"[WARN] Failed to load progress file: {e}")
                logger.warning(f"Failed to load progress file: {e}")
                return {}
        else:
            print("[OK] No existing progress file. Starting fresh.")
            return {}

    def save_progress(self):
        """Save progress to JSON file"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")

    def should_skip(self, corp_code, year, reprt_code, fs_div):
        """Check if this task should be skipped based on progress"""
        corp_data = self.progress.get(corp_code, {})
        year_data = corp_data.get(str(year), {})
        report_data = year_data.get(reprt_code, {})
        task_info = report_data.get(fs_div)

        # Not attempted yet
        if task_info is None:
            return False

        # Extract status from task_info dict
        if isinstance(task_info, dict):
            status = task_info.get('status')
        else:
            status = task_info

        # Skip if: success or no_data
        if status in ['success', 'no_data']:
            return True

        # Process if: failed or not attempted
        return False

    def update_progress(self, corp_code, corp_name, year, reprt_code, fs_div, status, records=0):
        """Update progress for a specific task"""
        # Initialize structure if needed
        if corp_code not in self.progress:
            self.progress[corp_code] = {'corp_name': corp_name}

        if str(year) not in self.progress[corp_code]:
            self.progress[corp_code][str(year)] = {}

        if reprt_code not in self.progress[corp_code][str(year)]:
            self.progress[corp_code][str(year)][reprt_code] = {}

        # Update status
        self.progress[corp_code][str(year)][reprt_code][fs_div] = {
            'status': status,
            'records': records,
            'updated_at': datetime.now().isoformat()
        }

        # Save after each update
        self.save_progress()

    async def get_connection(self):
        """PostgreSQL connection"""
        return await asyncpg.connect(self.database_url)

    async def get_company_info(self, corp_code):
        """Get company info from dart_company_info table"""
        conn = await self.get_connection()
        try:
            query = """
            SELECT corp_code, corp_name, stock_code
            FROM dart_company_info
            WHERE corp_code = $1
            """
            row = await conn.fetchrow(query, corp_code)

            if row:
                return {
                    'corp_code': row['corp_code'],
                    'corp_name': row['corp_name'],
                    'stock_code': row['stock_code']
                }
            else:
                logger.warning(f"Company not found with corp_code: {corp_code}")
                return None

        finally:
            await conn.close()

    async def get_all_companies(self):
        """Get all companies from dart_company_info table"""
        conn = await self.get_connection()
        try:
            query = """
            SELECT corp_code, corp_name, stock_code
            FROM dart_company_info
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

            print(f"[OK] Found {len(companies)} companies in database")
            return companies

        finally:
            await conn.close()

    def get_financial_statements(self, corp_code, bsns_year, reprt_code, fs_div):
        """Get financial statements from DART API"""
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bsns_year': bsns_year,
            'reprt_code': reprt_code,
            'fs_div': fs_div
        }

        try:
            response = requests.get(self.financial_api_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == '000':
                statements = data.get('list', [])
                # Fix missing fs_div values - fill with parameter value
                for item in statements:
                    if not item.get('fs_div'):
                        item['fs_div'] = fs_div
                return statements
            elif data.get('status') == '013':
                return []  # No data available
            elif data.get('status') == '020':  # 일일 요청 한도 초과
                logger.error(f"DART API daily limit exceeded (20,000 calls/day): {data.get('message')}")
                raise DartAPILimitExceeded(f"DART API daily limit exceeded: {data.get('message')}")
            else:
                logger.warning(f"API Error for {corp_code}/{bsns_year}/{reprt_code}: {data.get('status')} - {data.get('message')}")
                return []

        except DartAPILimitExceeded:
            raise  # Re-raise to propagate up
        except Exception as e:
            logger.debug(f"API request failed: {e}")
            return []

    def find_latest_report(self, corp_code):
        """Find latest available report by trying recent periods"""
        current_year = datetime.now().year

        # Try to find the most recent quarterly/annual report
        # Order: current year Q3, Q2, Q1, previous year annual
        attempts = [
            (current_year, '11014'),  # 3Q
            (current_year, '11012'),  # Semi-annual
            (current_year, '11013'),  # 1Q
            (current_year - 1, '11011'),  # Previous year annual
        ]

        logger.info("Searching for latest available financial statements...")

        for year, reprt_code in attempts:
            # Try consolidated first
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': str(year),
                'reprt_code': reprt_code,
                'fs_div': 'CFS'
            }

            try:
                response = requests.get(self.financial_api_url, params=params, timeout=30)
                data = response.json()

                if data.get('status') == '000' and data.get('list'):
                    logger.info(f"Found: {year} {self.report_codes.get(reprt_code)} (Consolidated)")
                    return str(year), reprt_code, 'CFS'

            except:
                pass

        # If no consolidated found, try separate statements
        for year, reprt_code in attempts:
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': str(year),
                'reprt_code': reprt_code,
                'fs_div': 'OFS'
            }

            try:
                response = requests.get(self.financial_api_url, params=params, timeout=30)
                data = response.json()

                if data.get('status') == '000' and data.get('list'):
                    logger.info(f"Found: {year} {self.report_codes.get(reprt_code)} (Separate)")
                    return str(year), reprt_code, 'OFS'

            except:
                pass

        logger.error("No financial statements found")
        return None, None, None

    def log_financial_data(self, statements, bsns_year, reprt_code, fs_div):
        """Log financial statements data"""
        if not statements:
            logger.info("No statements to log")
            return

        logger.info(f"\n{'='*80}")
        logger.info(f"Financial Statements Data")
        logger.info(f"Year: {bsns_year}")
        logger.info(f"Report: {self.report_codes.get(reprt_code, reprt_code)}")
        logger.info(f"Type: {self.fs_types.get(fs_div, fs_div)}")
        logger.info(f"{'='*80}\n")

        # Group by statement type
        grouped = {}
        for item in statements:
            sj_div = item.get('sj_div', 'Unknown')
            if sj_div not in grouped:
                grouped[sj_div] = []
            grouped[sj_div].append(item)

        # Statement type names in Korean
        sj_names = {
            'BS': 'Balance Sheet (재무상태표)',
            'IS': 'Income Statement (손익계산서)',
            'CIS': 'Comprehensive Income Statement (포괄손익계산서)',
            'CF': 'Cash Flow Statement (현금흐름표)',
            'SCE': 'Statement of Changes in Equity (자본변동표)'
        }

        # Log each statement type
        for sj_div, items in grouped.items():
            logger.info(f"\n{'-'*80}")
            logger.info(f"{sj_names.get(sj_div, sj_div)} - {len(items)} items")
            logger.info(f"{'-'*80}\n")

            # Show first 20 items
            for i, item in enumerate(items[:20], 1):
                account_nm = item.get('account_nm', 'N/A')
                thstrm_amount = item.get('thstrm_amount', '0')
                frmtrm_amount = item.get('frmtrm_amount', '0')
                currency = item.get('currency', '')

                # Format amounts
                try:
                    thstrm = f"{int(thstrm_amount):,}" if thstrm_amount and thstrm_amount != '-' else '0'
                    frmtrm = f"{int(frmtrm_amount):,}" if frmtrm_amount and frmtrm_amount != '-' else '0'
                except:
                    thstrm = thstrm_amount
                    frmtrm = frmtrm_amount

                logger.info(f"{i}. {account_nm}")
                logger.info(f"   Current Period: {thstrm} {currency}")
                logger.info(f"   Previous Period: {frmtrm} {currency}")

            if len(items) > 20:
                logger.info(f"\n... and {len(items) - 20} more items")

        logger.info(f"\n{'='*80}\n")

    def save_json_summary(self, statements, company, bsns_year, reprt_code, fs_div):
        """Save financial data summary as JSON"""
        summary_dir = "financial_data"
        os.makedirs(summary_dir, exist_ok=True)

        filename = f"{company['corp_code']}_{bsns_year}_{reprt_code}_{fs_div}.json"
        filepath = os.path.join(summary_dir, filename)

        # Prepare data
        data = {
            'company': company,
            'year': bsns_year,
            'report_code': reprt_code,
            'report_name': self.report_codes.get(reprt_code, reprt_code),
            'fs_type': self.fs_types.get(fs_div, fs_div),
            'collected_at': datetime.now().isoformat(),
            'total_items': len(statements),
            'statements': statements
        }

        # Save to JSON
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"JSON file saved: {filepath}")
        return filepath

    async def save_to_database(self, statements, company):
        """Save financial statements to kr_financial_position table"""
        if not statements:
            logger.warning("No statements to save to database")
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                saved_count = 0

                for item in statements:
                    try:
                        # Parse amounts (remove commas and convert to integer)
                        def parse_amount(value):
                            if not value or value == '-':
                                return None
                            try:
                                return int(value.replace(',', ''))
                            except:
                                return None

                        thstrm_amount = parse_amount(item.get('thstrm_amount'))
                        thstrm_add_amount = parse_amount(item.get('thstrm_add_amount'))
                        frmtrm_amount = parse_amount(item.get('frmtrm_amount'))
                        frmtrm_add_amount = parse_amount(item.get('frmtrm_add_amount'))

                        # Parse ord
                        try:
                            ord_value = int(item.get('ord', 0)) if item.get('ord') else None
                        except:
                            ord_value = None

                        # Parse bsns_year
                        try:
                            bsns_year_int = int(item.get('bsns_year'))
                        except:
                            bsns_year_int = None

                        # Extract rcept_dt from rcept_no (format: "20250311001234" -> date(2025, 3, 11))
                        rcept_no = item.get('rcept_no')
                        rcept_dt = None
                        if rcept_no and len(rcept_no) >= 8:
                            try:
                                # Extract first 8 digits: YYYYMMDD and convert to date object
                                date_str = rcept_no[:8]
                                year = int(date_str[:4])
                                month = int(date_str[4:6])
                                day = int(date_str[6:8])
                                rcept_dt = datetime(year, month, day).date()
                            except:
                                rcept_dt = None

                        await conn.execute("""
                            INSERT INTO kr_financial_position (
                                stock_name, symbol, report_code, bsns_year, corp_code,
                                fs_div, sj_div, sj_nm, account_nm,
                                rcept_no, rcept_dt, fs_nm, thstrm_dt, frmtrm_dt,
                                thstrm_nm, thstrm_amount, thstrm_add_amount,
                                frmtrm_nm, frmtrm_amount, frmtrm_add_amount,
                                ord, currency
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
                            ON CONFLICT (corp_code, bsns_year, report_code, fs_div, sj_div, account_nm)
                            DO UPDATE SET
                                rcept_no = EXCLUDED.rcept_no,
                                rcept_dt = EXCLUDED.rcept_dt,
                                stock_name = EXCLUDED.stock_name,
                                symbol = EXCLUDED.symbol,
                                sj_nm = EXCLUDED.sj_nm,
                                fs_nm = EXCLUDED.fs_nm,
                                thstrm_dt = EXCLUDED.thstrm_dt,
                                frmtrm_dt = EXCLUDED.frmtrm_dt,
                                thstrm_nm = EXCLUDED.thstrm_nm,
                                thstrm_amount = EXCLUDED.thstrm_amount,
                                thstrm_add_amount = EXCLUDED.thstrm_add_amount,
                                frmtrm_nm = EXCLUDED.frmtrm_nm,
                                frmtrm_amount = EXCLUDED.frmtrm_amount,
                                frmtrm_add_amount = EXCLUDED.frmtrm_add_amount,
                                ord = EXCLUDED.ord,
                                currency = EXCLUDED.currency,
                                updated_at = CURRENT_TIMESTAMP
                        """,
                            company['corp_name'],  # stock_name
                            item.get('stock_code'),  # symbol
                            item.get('reprt_code'),  # report_code
                            bsns_year_int,  # bsns_year
                            item.get('corp_code'),  # corp_code
                            item.get('fs_div'),  # fs_div
                            item.get('sj_div'),  # sj_div
                            item.get('sj_nm'),  # sj_nm
                            item.get('account_nm'),  # account_nm
                            rcept_no,  # rcept_no
                            rcept_dt,  # rcept_dt (NEW: extracted from rcept_no)
                            item.get('fs_nm'),  # fs_nm
                            item.get('thstrm_dt'),  # thstrm_dt
                            item.get('frmtrm_dt'),  # frmtrm_dt
                            item.get('thstrm_nm'),  # thstrm_nm
                            thstrm_amount,  # thstrm_amount
                            thstrm_add_amount,  # thstrm_add_amount
                            item.get('frmtrm_nm'),  # frmtrm_nm
                            frmtrm_amount,  # frmtrm_amount
                            frmtrm_add_amount,  # frmtrm_add_amount
                            ord_value,  # ord
                            item.get('currency')  # currency
                        )

                        saved_count += 1

                    except Exception as e:
                        # Silently skip individual record errors
                        continue

                return saved_count

        except Exception as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            await conn.close()

    def find_latest_quarter(self, corp_code):
        """Find the latest available quarter for a company (returns year, reprt_code)"""
        current_year = datetime.now().year

        # Try to find the most recent quarterly/annual report
        # Order: current year Q3, Q2, Q1, previous year annual, current year annual
        attempts = [
            (current_year, '11014', '3Q'),      # Current year 3Q
            (current_year, '11012', '2Q'),      # Current year Semi-annual
            (current_year, '11013', '1Q'),      # Current year 1Q
            (current_year - 1, '11011', 'Annual'),  # Previous year annual
            (current_year, '11011', 'Annual'),  # Current year annual
        ]

        for year, reprt_code, report_name in attempts:
            # Try CFS first
            try:
                statements = self.get_financial_statements(corp_code, str(year), reprt_code, 'CFS')
                if statements:
                    return year, reprt_code, report_name
            except DartAPILimitExceeded:
                raise
            except:
                pass

        # If no CFS found, try OFS
        for year, reprt_code, report_name in attempts:
            try:
                statements = self.get_financial_statements(corp_code, str(year), reprt_code, 'OFS')
                if statements:
                    return year, reprt_code, report_name
            except DartAPILimitExceeded:
                raise
            except:
                pass

        return None, None, None

    async def collect_company_latest_quarter(self, company, progress_info=""):
        """Collect only the latest quarter data for one company (CFS + OFS if available)"""
        corp_code = company['corp_code']
        corp_name = company['corp_name']
        stock_code = company.get('stock_code', 'N/A')

        batch_data = []
        api_results = []

        # Find latest quarter
        latest_year, latest_reprt_code, latest_report_name = self.find_latest_quarter(corp_code)

        if not latest_year:
            print(f"{datetime.now().strftime('%H:%M:%S')} | {stock_code:6} | NONE | No data available {progress_info}")
            return {
                'company': company,
                'batch_data': [],
                'api_results': []
            }

        # Collect CFS for latest quarter
        if not self.should_skip(corp_code, latest_year, latest_reprt_code, 'CFS'):
            try:
                statements = self.get_financial_statements(corp_code, str(latest_year), latest_reprt_code, 'CFS')

                if statements:
                    batch_data.extend(statements)
                    api_results.append({
                        'year': latest_year,
                        'reprt_code': latest_reprt_code,
                        'report_name': latest_report_name,
                        'fs_div': 'CFS',
                        'status': 'success',
                        'records': len(statements)
                    })
                else:
                    api_results.append({
                        'year': latest_year,
                        'reprt_code': latest_reprt_code,
                        'report_name': latest_report_name,
                        'fs_div': 'CFS',
                        'status': 'no_data',
                        'records': 0
                    })

                await asyncio.sleep(0.5)

            except Exception as e:
                api_results.append({
                    'year': latest_year,
                    'reprt_code': latest_reprt_code,
                    'report_name': latest_report_name,
                    'fs_div': 'CFS',
                    'status': 'failed',
                    'records': 0,
                    'error': e
                })

        # Collect OFS for latest quarter
        if not self.should_skip(corp_code, latest_year, latest_reprt_code, 'OFS'):
            try:
                statements = self.get_financial_statements(corp_code, str(latest_year), latest_reprt_code, 'OFS')

                if statements:
                    batch_data.extend(statements)
                    api_results.append({
                        'year': latest_year,
                        'reprt_code': latest_reprt_code,
                        'report_name': latest_report_name,
                        'fs_div': 'OFS',
                        'status': 'success',
                        'records': len(statements)
                    })
                else:
                    api_results.append({
                        'year': latest_year,
                        'reprt_code': latest_reprt_code,
                        'report_name': latest_report_name,
                        'fs_div': 'OFS',
                        'status': 'no_data',
                        'records': 0
                    })

                await asyncio.sleep(0.5)

            except Exception as e:
                api_results.append({
                    'year': latest_year,
                    'reprt_code': latest_reprt_code,
                    'report_name': latest_report_name,
                    'fs_div': 'OFS',
                    'status': 'failed',
                    'records': 0,
                    'error': e
                })

        return {
            'company': company,
            'batch_data': batch_data,
            'api_results': api_results
        }


    async def collect_all_latest_quarters(self):
        """Collect only the latest quarter for all companies"""
        print("="*80)
        print(f"DART Financial Statements - Latest Quarter Collection")
        print(f"Mode: Latest quarter only (CFS + OFS if available)")
        print(f"Batch Size: 1 company per DB transaction")
        print("="*80)

        # Get all companies
        companies = await self.get_all_companies()

        if not companies:
            logger.error("No companies found in database")
            return

        # Statistics
        total_companies = len(companies)
        processed_companies = 0
        total_records = 0
        failed_companies = []

        start_time = datetime.now()

        print(f"\nTotal Companies: {total_companies}")
        print(f"Time    | Symbol | Stat | Details")
        print("-"*80)

        # Process each company
        for idx, company in enumerate(companies, 1):
            progress_info = f"[{idx}/{total_companies}]"

            try:
                # Collect latest quarter data for company
                result = await self.collect_company_latest_quarter(company, progress_info)

                corp_code = company['corp_code']
                corp_name = company['corp_name']
                stock_code = company.get('stock_code', 'N/A')
                batch_data = result['batch_data']
                api_results = result['api_results']

                # Save to database
                if batch_data:
                    try:
                        conn = await self.get_connection()

                        try:
                            async with conn.transaction():
                                saved = await self._save_batch_data(conn, batch_data, company)

                            # DB save successful
                            total_records += saved
                            print(f"{datetime.now().strftime('%H:%M:%S')} | {stock_code:6} | SAVE | {saved} records saved to DB {progress_info}")

                            # Update progress for all API calls
                            for api_result in api_results:
                                if api_result['status'] == 'success':
                                    print(f"{datetime.now().strftime('%H:%M:%S')} | {stock_code:6} | OK   | {api_result['year']} {api_result['report_name']} {api_result['fs_div']} ({api_result['records']}rec)")
                                    self.update_progress(corp_code, corp_name, api_result['year'], api_result['reprt_code'], api_result['fs_div'], 'success', api_result['records'])
                                elif api_result['status'] == 'no_data':
                                    self.update_progress(corp_code, corp_name, api_result['year'], api_result['reprt_code'], api_result['fs_div'], 'no_data', 0)
                                elif api_result['status'] == 'failed':
                                    print(f"{datetime.now().strftime('%H:%M:%S')} | {stock_code:6} | FAIL | {api_result['year']} {api_result['report_name']} {api_result['fs_div']}")
                                    logger.error(f"[ERROR DETAIL] {corp_name} ({stock_code}) {api_result['year']} {api_result['report_name']} {api_result['fs_div']}")
                                    logger.error(f"  Error Type: {type(api_result.get('error')).__name__}")
                                    logger.error(f"  Error Message: {str(api_result.get('error'))}")
                                    self.update_progress(corp_code, corp_name, api_result['year'], api_result['reprt_code'], api_result['fs_div'], 'failed', 0)

                            processed_companies += 1

                        finally:
                            await conn.close()

                    except Exception as e:
                        print(f"{datetime.now().strftime('%H:%M:%S')} | {stock_code:6} | FAIL | Batch save failed {progress_info}")
                        logger.error(f"[BATCH SAVE ERROR] {corp_name} ({stock_code})")
                        logger.error(f"  Error Type: {type(e).__name__}")
                        logger.error(f"  Error Message: {str(e)}")
                        import traceback
                        logger.error(f"  Traceback: {traceback.format_exc()}")
                        failed_companies.append(corp_name)

                        # Mark all as failed
                        for api_result in api_results:
                            if api_result['status'] == 'success':
                                self.update_progress(corp_code, corp_name, api_result['year'], api_result['reprt_code'], api_result['fs_div'], 'failed', 0)
                else:
                    # No data collected, just update progress
                    for api_result in api_results:
                        if api_result['status'] == 'no_data':
                            self.update_progress(corp_code, corp_name, api_result['year'], api_result['reprt_code'], api_result['fs_div'], 'no_data', 0)
                        elif api_result['status'] == 'failed':
                            print(f"{datetime.now().strftime('%H:%M:%S')} | {stock_code:6} | FAIL | {api_result['year']} {api_result['report_name']} {api_result['fs_div']}")
                            self.update_progress(corp_code, corp_name, api_result['year'], api_result['reprt_code'], api_result['fs_div'], 'failed', 0)

                    processed_companies += 1

            except DartAPILimitExceeded as e:
                # API 한도 초과 시 graceful exit
                print(f"\n{datetime.now().strftime('%H:%M:%S')} | LIMIT | DART API daily limit reached")
                logger.warning(f"DART API daily limit reached at company {idx}/{total_companies}. Collection stopped gracefully: {e}")
                break  # Exit loop and show summary

            except Exception as e:
                print(f"{datetime.now().strftime('%H:%M:%S')} | {company.get('stock_code', 'N/A'):6} | FAIL | Company processing failed {progress_info}")
                logger.error(f"[CRITICAL ERROR] Failed to process {company['corp_name']}")
                logger.error(f"  Error Type: {type(e).__name__}")
                logger.error(f"  Error Message: {str(e)}")
                import traceback
                logger.error(f"  Traceback: {traceback.format_exc()}")
                failed_companies.append(company['corp_name'])
                continue

        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time

        print("\n" + "="*80)
        print(f"Latest Quarter Collection Completed")
        print("="*80)
        print(f"Total Companies: {total_companies}")
        print(f"Successfully Processed: {processed_companies}")
        print(f"Failed: {len(failed_companies)}")
        print(f"Total Records Saved: {total_records:,}")
        print(f"Duration: {duration}")
        print(f"Progress file: {self.progress_file}")

        if failed_companies:
            print(f"\nFailed Companies: {len(failed_companies)}")
            for name in failed_companies[:5]:
                print(f"  - {name}")
            if len(failed_companies) > 5:
                print(f"  ... and {len(failed_companies) - 5} more")

        print("="*80)


    async def _save_batch_data(self, conn, statements, company):
        """Save statements to database using existing connection"""
        if not statements:
            return 0

        saved_count = 0

        for item in statements:
            try:
                # Parse amounts (remove commas and convert to integer)
                def parse_amount(value):
                    if not value or value == '-':
                        return None
                    try:
                        return int(value.replace(',', ''))
                    except:
                        return None

                thstrm_amount = parse_amount(item.get('thstrm_amount'))
                thstrm_add_amount = parse_amount(item.get('thstrm_add_amount'))
                frmtrm_amount = parse_amount(item.get('frmtrm_amount'))
                frmtrm_add_amount = parse_amount(item.get('frmtrm_add_amount'))

                # Parse ord
                try:
                    ord_value = int(item.get('ord', 0)) if item.get('ord') else None
                except:
                    ord_value = None

                # Parse bsns_year
                try:
                    bsns_year_int = int(item.get('bsns_year'))
                except:
                    bsns_year_int = None

                # Extract rcept_dt from rcept_no (format: "20250311001234" -> date(2025, 3, 11))
                rcept_no = item.get('rcept_no')
                rcept_dt = None
                if rcept_no and len(rcept_no) >= 8:
                    try:
                        # Extract first 8 digits: YYYYMMDD and convert to date object
                        date_str = rcept_no[:8]
                        year = int(date_str[:4])
                        month = int(date_str[4:6])
                        day = int(date_str[6:8])
                        rcept_dt = datetime(year, month, day).date()
                    except:
                        rcept_dt = None

                await conn.execute("""
                    INSERT INTO kr_financial_position (
                        stock_name, symbol, report_code, bsns_year, corp_code,
                        fs_div, sj_div, sj_nm, account_nm,
                        rcept_no, rcept_dt, fs_nm, thstrm_dt, frmtrm_dt,
                        thstrm_nm, thstrm_amount, thstrm_add_amount,
                        frmtrm_nm, frmtrm_amount, frmtrm_add_amount,
                        ord, currency
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
                    ON CONFLICT (corp_code, bsns_year, report_code, fs_div, sj_div, account_nm)
                    DO UPDATE SET
                        rcept_no = EXCLUDED.rcept_no,
                        rcept_dt = EXCLUDED.rcept_dt,
                        stock_name = EXCLUDED.stock_name,
                        symbol = EXCLUDED.symbol,
                        sj_nm = EXCLUDED.sj_nm,
                        fs_nm = EXCLUDED.fs_nm,
                        thstrm_dt = EXCLUDED.thstrm_dt,
                        frmtrm_dt = EXCLUDED.frmtrm_dt,
                        thstrm_nm = EXCLUDED.thstrm_nm,
                        thstrm_amount = EXCLUDED.thstrm_amount,
                        thstrm_add_amount = EXCLUDED.thstrm_add_amount,
                        frmtrm_nm = EXCLUDED.frmtrm_nm,
                        frmtrm_amount = EXCLUDED.frmtrm_amount,
                        frmtrm_add_amount = EXCLUDED.frmtrm_add_amount,
                        ord = EXCLUDED.ord,
                        currency = EXCLUDED.currency,
                        updated_at = CURRENT_TIMESTAMP
                """,
                    company['corp_name'],  # stock_name
                    company.get('stock_code'),  # symbol (from company, not API)
                    item.get('reprt_code'),  # report_code
                    bsns_year_int,  # bsns_year
                    item.get('corp_code'),  # corp_code
                    item.get('fs_div'),  # fs_div
                    item.get('sj_div'),  # sj_div
                    item.get('sj_nm'),  # sj_nm
                    item.get('account_nm'),  # account_nm
                    rcept_no,  # rcept_no
                    rcept_dt,  # rcept_dt (NEW: extracted from rcept_no)
                    item.get('fs_nm'),  # fs_nm
                    item.get('thstrm_dt'),  # thstrm_dt
                    item.get('frmtrm_dt'),  # frmtrm_dt
                    item.get('thstrm_nm'),  # thstrm_nm
                    thstrm_amount,  # thstrm_amount
                    thstrm_add_amount,  # thstrm_add_amount
                    item.get('frmtrm_nm'),  # frmtrm_nm
                    frmtrm_amount,  # frmtrm_amount
                    frmtrm_add_amount,  # frmtrm_add_amount
                    ord_value,  # ord
                    item.get('currency')  # currency
                )

                saved_count += 1

            except Exception as e:
                # Log individual record errors instead of silently skipping
                logger.error(f"[RECORD SAVE ERROR] Failed to save record")
                logger.error(f"  Corp: {company.get('corp_name', 'N/A')}")
                logger.error(f"  Data: {item.get('bsns_year')}/{item.get('reprt_code')}/{item.get('fs_div')}/{item.get('account_nm')}")
                logger.error(f"  Error: {type(e).__name__}: {str(e)}")
                continue

        return saved_count


async def collect_financial_data_bulk(database_url, dart_api_key, start_year=None, end_year=None):
    """
    재무제표 수집 (최신 분기만)
    FastAPI 엔드포인트에서 호출하기 위한 래퍼 함수

    각 회사별 가장 최신 분기 보고서만 수집 (CFS + OFS 둘 다 있으면 둘 다 수집)
    예: 2025년 10월이면 2025 3Q 또는 2025 2Q 등 가장 최근 공시된 분기
    """
    try:
        collector = DartFinancialStatementsCollector(dart_api_key, database_url)
        await collector.collect_all_latest_quarters()
        logger.info("DART Financial Position Latest Quarter Collection Completed")

    except DartAPILimitExceeded as e:
        logger.warning(f"DART API daily limit reached. Collection stopped gracefully: {e}")
        logger.info("DART Financial Position Latest Quarter Collection Completed (with API limit)")
        return  # 정상 종료

    except Exception as e:
        logger.error(f"DART Financial Position Latest Quarter Collection Failed: {e}")
        raise
