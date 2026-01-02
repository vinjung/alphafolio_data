# alpha/data/us/finance_data.py
import requests
import time
import json
import asyncio
import asyncpg
import aiohttp
import logging
import os
import pandas as pd
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, List
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv
from asyncio import Queue
from collection_logger import CollectionLogger

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('log/alphavantage_data_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IncomeStatementCollector:
    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2, target_date: date = None):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1
        self.call_interval = call_interval
        self.start_date = date(2021, 1, 1)  # Income statements from 2021

        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_income_statement_collected.json'
        else:
            log_file_path = 'log/us_income_statement_collected.json'

        self.collection_logger = CollectionLogger(log_file_path)
        self.pool = None
        self.session = None

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
        logger.info("[INCOME_STMT] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[INCOME_STMT] Database connection pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_existing_symbols(self) -> List[str]:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[INCOME_STMT] Found {len(symbols)} symbols")
            return symbols
        except Exception as e:
            logger.error(f"[INCOME_STMT] Failed to get existing symbols: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return []

    async def get_symbols_with_recent_data(self) -> set:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('''
                SELECT DISTINCT symbol 
                FROM us_income_statement 
                WHERE created_at >= NOW() - INTERVAL '7 days'
            ''')
            await conn.close()
            recent_symbols = {row['symbol'] for row in rows}
            return recent_symbols
        except Exception as e:
            logger.warning(f"Failed to get symbols with recent data: {str(e)}")
            return set()
    
    async def get_symbols_needing_update(self) -> set:
        try:
            all_symbols = await self.get_existing_symbols()
            recent_symbols = await self.get_symbols_with_recent_data()
            symbols_needing_update = all_symbols - recent_symbols
            
            
            return symbols_needing_update
            
        except Exception as e:
            logger.error(f"Failed to determine symbols needing update: {str(e)}")
            return await self.get_existing_symbols()
    
    async def get_income_statement_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch Income Statement data with async aiohttp - OPTIMIZED"""
        try:
            params = {
                'function': 'INCOME_STATEMENT',
                'symbol': symbol,
                'apikey': self.api_key
            }
            async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'Error Message' in data:
                        logger.error(f"[INCOME_STMT] API error for {symbol}: {data['Error Message']}")
                        return None
                    elif 'Note' in data:
                        logger.warning(f"[INCOME_STMT] API limit reached: {data['Note']}")
                        return None
                    elif 'annualReports' in data:
                        return data
                    else:
                        logger.warning(f"[INCOME_STMT] Unexpected response format for {symbol}: {list(data.keys())}")
                        return None
                else:
                    logger.error(f"[INCOME_STMT] API request failed for {symbol}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"[INCOME_STMT] Error fetching data for {symbol}: {e}")
            return None
    
    def transform_income_statement_data(self, api_data: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
        try:
            transformed_records = []

            annual_reports = api_data.get("annualReports", [])

            if not annual_reports:
                return transformed_records

            #   fiscal_date 
            latest_report = None
            latest_date = None

            for report in annual_reports:
                fiscal_date_ending = report.get("fiscalDateEnding", "")
                if not fiscal_date_ending:
                    continue

                try:
                    parsed_date = datetime.strptime(fiscal_date_ending, "%Y-%m-%d").date()
                    if latest_date is None or parsed_date > latest_date:
                        latest_date = parsed_date
                        latest_report = report
                except:
                    continue

            #   report 
            if latest_report:
                report = latest_report
                try:
                    fiscal_date_ending = report.get("fiscalDateEnding", "")
                    parsed_date = datetime.strptime(fiscal_date_ending, "%Y-%m-%d").date()
                    
                    # Safe currency code extraction (max 3 chars)
                    currency = report.get("reportedCurrency", "")
                    if currency and isinstance(currency, str):
                        currency = currency.strip()[:3]
                    else:
                        currency = ""

                    transformed = {
                        'symbol': symbol,
                        'fiscal_date_ending': parsed_date,
                        'reported_currency': currency,
                        'gross_profit': self.safe_bigint(report.get("grossProfit")),
                        'total_revenue': self.safe_bigint(report.get("totalRevenue")),
                        'cost_of_revenue': self.safe_bigint(report.get("costOfRevenue")),
                        'cost_of_goods_and_services_sold': self.safe_bigint(report.get("costofGoodsAndServicesSold")),
                        'operating_income': self.safe_bigint(report.get("operatingIncome")),
                        'selling_general_and_administrative': self.safe_bigint(report.get("sellingGeneralAndAdministrative")),
                        'research_and_development': self.safe_bigint(report.get("researchAndDevelopment")),
                        'operating_expenses': self.safe_bigint(report.get("operatingExpenses")),
                        'investment_income_net': self.safe_bigint(report.get("investmentIncomeNet")),
                        'net_interest_income': self.safe_bigint(report.get("netInterestIncome")),
                        'interest_income': self.safe_bigint(report.get("interestIncome")),
                        'interest_expense': self.safe_bigint(report.get("interestExpense")),
                        'non_interest_income': self.safe_bigint(report.get("nonInterestIncome")),
                        'other_non_operating_income': self.safe_bigint(report.get("otherNonOperatingIncome")),
                        'depreciation': self.safe_bigint(report.get("depreciation")),
                        'depreciation_and_amortization': self.safe_bigint(report.get("depreciationAndAmortization")),
                        'income_before_tax': self.safe_bigint(report.get("incomeBeforeTax")),
                        'income_tax_expense': self.safe_bigint(report.get("incomeTaxExpense")),
                        'interest_and_debt_expense': self.safe_bigint(report.get("interestAndDebtExpense")),
                        'net_income_from_continuing_operations': self.safe_bigint(report.get("netIncomeFromContinuingOperations")),
                        'comprehensive_income_net_of_tax': self.safe_bigint(report.get("comprehensiveIncomeNetOfTax")),
                        'ebit': self.safe_bigint(report.get("ebit")),
                        'ebitda': self.safe_bigint(report.get("ebitda")),
                        'net_income': self.safe_bigint(report.get("netIncome")),
                        'created_at': datetime.now()
                    }
                    
                    transformed_records.append(transformed)

                    logger.info(f"[INCOME_STMT] {symbol}: Latest fiscal date = {parsed_date}")

                except Exception as e:
                    logger.error(f"[INCOME_STMT] Error transforming latest report for {symbol}: {str(e)}, Date: {fiscal_date_ending}")

            return transformed_records

        except Exception as e:
            logger.error(f"[INCOME_STMT] Error transforming Income Statement data for {symbol}: {str(e)}")
            return []
    
    def safe_bigint(self, value: str) -> Optional[int]:
        if not value or value.strip() in ['', 'N/A', '-', 'null', 'None']:
            return None

        try:
            cleaned_value = ''.join(c for c in str(value).strip() if c.isdigit() or c == '-')
            if not cleaned_value or cleaned_value == '-':
                return None
            result = int(cleaned_value)
            # PostgreSQL BIGINT range check
            if result < -9223372036854775808 or result > 9223372036854775807:
                logger.warning(f"Value out of BIGINT range, setting to None: {value}")
                return None
            return result
        except (ValueError, TypeError):
            logger.warning(f"Could not convert to bigint: {value}")
            return None
    
    async def save_income_statement_data(self, income_statement_data: List[Dict[str, Any]]) -> int:
        if not income_statement_data:
            return 0
        
        try:
            conn = await self.get_connection()
            saved_count = 0
            
            for record in income_statement_data:
                try:
                    await conn.execute('''
                        INSERT INTO us_income_statement 
                        (symbol, fiscal_date_ending, reported_currency, gross_profit, total_revenue, cost_of_revenue,
                         cost_of_goods_and_services_sold, operating_income, selling_general_and_administrative,
                         research_and_development, operating_expenses, investment_income_net, net_interest_income,
                         interest_income, interest_expense, non_interest_income, other_non_operating_income,
                         depreciation, depreciation_and_amortization, income_before_tax, income_tax_expense,
                         interest_and_debt_expense, net_income_from_continuing_operations, comprehensive_income_net_of_tax,
                         ebit, ebitda, net_income, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)
                        ON CONFLICT (symbol, fiscal_date_ending) DO UPDATE SET
                        reported_currency = EXCLUDED.reported_currency,
                        gross_profit = EXCLUDED.gross_profit,
                        total_revenue = EXCLUDED.total_revenue,
                        cost_of_revenue = EXCLUDED.cost_of_revenue,
                        cost_of_goods_and_services_sold = EXCLUDED.cost_of_goods_and_services_sold,
                        operating_income = EXCLUDED.operating_income,
                        selling_general_and_administrative = EXCLUDED.selling_general_and_administrative,
                        research_and_development = EXCLUDED.research_and_development,
                        operating_expenses = EXCLUDED.operating_expenses,
                        investment_income_net = EXCLUDED.investment_income_net,
                        net_interest_income = EXCLUDED.net_interest_income,
                        interest_income = EXCLUDED.interest_income,
                        interest_expense = EXCLUDED.interest_expense,
                        non_interest_income = EXCLUDED.non_interest_income,
                        other_non_operating_income = EXCLUDED.other_non_operating_income,
                        depreciation = EXCLUDED.depreciation,
                        depreciation_and_amortization = EXCLUDED.depreciation_and_amortization,
                        income_before_tax = EXCLUDED.income_before_tax,
                        income_tax_expense = EXCLUDED.income_tax_expense,
                        interest_and_debt_expense = EXCLUDED.interest_and_debt_expense,
                        net_income_from_continuing_operations = EXCLUDED.net_income_from_continuing_operations,
                        comprehensive_income_net_of_tax = EXCLUDED.comprehensive_income_net_of_tax,
                        ebit = EXCLUDED.ebit,
                        ebitda = EXCLUDED.ebitda,
                        net_income = EXCLUDED.net_income,
                        created_at = EXCLUDED.created_at
                    ''', 
                    record['symbol'],
                    record['fiscal_date_ending'],
                    record['reported_currency'],
                    record['gross_profit'],
                    record['total_revenue'],
                    record['cost_of_revenue'],
                    record['cost_of_goods_and_services_sold'],
                    record['operating_income'],
                    record['selling_general_and_administrative'],
                    record['research_and_development'],
                    record['operating_expenses'],
                    record['investment_income_net'],
                    record['net_interest_income'],
                    record['interest_income'],
                    record['interest_expense'],
                    record['non_interest_income'],
                    record['other_non_operating_income'],
                    record['depreciation'],
                    record['depreciation_and_amortization'],
                    record['income_before_tax'],
                    record['income_tax_expense'],
                    record['interest_and_debt_expense'],
                    record['net_income_from_continuing_operations'],
                    record['comprehensive_income_net_of_tax'],
                    record['ebit'],
                    record['ebitda'],
                    record['net_income'],
                    record['created_at']
                    )
                    
                    saved_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to save Income Statement record for {record.get('symbol', 'unknown')}: {str(e)}")
            
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return saved_count

        except Exception as e:
            logger.error(f"[INCOME_STMT] Database error while saving Income Statement data: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return 0

    async def save_income_statement_data_optimized(self, income_statement_data: List[Dict[str, Any]]) -> int:
        """Save Income Statement data using COPY command - OPTIMIZED"""
        if not income_statement_data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                # Transaction optimization
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                # Create temp table
                await conn.execute('''
                    CREATE TEMP TABLE temp_income_statement (
                        symbol VARCHAR(20),
                        fiscal_date_ending DATE,
                        reported_currency VARCHAR(10),
                        gross_profit BIGINT,
                        total_revenue BIGINT,
                        cost_of_revenue BIGINT,
                        cost_of_goods_and_services_sold BIGINT,
                        operating_income BIGINT,
                        selling_general_and_administrative BIGINT,
                        research_and_development BIGINT,
                        operating_expenses BIGINT,
                        investment_income_net BIGINT,
                        net_interest_income BIGINT,
                        interest_income BIGINT,
                        interest_expense BIGINT,
                        non_interest_income BIGINT,
                        other_non_operating_income BIGINT,
                        depreciation BIGINT,
                        depreciation_and_amortization BIGINT,
                        income_before_tax BIGINT,
                        income_tax_expense BIGINT,
                        interest_and_debt_expense BIGINT,
                        net_income_from_continuing_operations BIGINT,
                        comprehensive_income_net_of_tax BIGINT,
                        ebit BIGINT,
                        ebitda BIGINT,
                        net_income BIGINT,
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')

                # Prepare data for COPY
                rows = [[
                    record['symbol'], record['fiscal_date_ending'], record['reported_currency'],
                    record['gross_profit'], record['total_revenue'], record['cost_of_revenue'],
                    record['cost_of_goods_and_services_sold'], record['operating_income'],
                    record['selling_general_and_administrative'], record['research_and_development'],
                    record['operating_expenses'], record['investment_income_net'], record['net_interest_income'],
                    record['interest_income'], record['interest_expense'], record['non_interest_income'],
                    record['other_non_operating_income'], record['depreciation'], record['depreciation_and_amortization'],
                    record['income_before_tax'], record['income_tax_expense'], record['interest_and_debt_expense'],
                    record['net_income_from_continuing_operations'], record['comprehensive_income_net_of_tax'],
                    record['ebit'], record['ebitda'], record['net_income'], record['created_at']
                ] for record in income_statement_data]

                # COPY to temp table
                await conn.copy_records_to_table(
                    'temp_income_statement', records=rows,
                    columns=['symbol', 'fiscal_date_ending', 'reported_currency', 'gross_profit', 'total_revenue',
                            'cost_of_revenue', 'cost_of_goods_and_services_sold', 'operating_income',
                            'selling_general_and_administrative', 'research_and_development', 'operating_expenses',
                            'investment_income_net', 'net_interest_income', 'interest_income', 'interest_expense',
                            'non_interest_income', 'other_non_operating_income', 'depreciation',
                            'depreciation_and_amortization', 'income_before_tax', 'income_tax_expense',
                            'interest_and_debt_expense', 'net_income_from_continuing_operations',
                            'comprehensive_income_net_of_tax', 'ebit', 'ebitda', 'net_income', 'created_at']
                )

                # Upsert from temp to main
                await conn.execute('''
                    INSERT INTO us_income_statement SELECT * FROM temp_income_statement
                    ON CONFLICT (symbol, fiscal_date_ending) DO UPDATE SET
                        reported_currency = EXCLUDED.reported_currency,
                        gross_profit = EXCLUDED.gross_profit,
                        total_revenue = EXCLUDED.total_revenue,
                        cost_of_revenue = EXCLUDED.cost_of_revenue,
                        cost_of_goods_and_services_sold = EXCLUDED.cost_of_goods_and_services_sold,
                        operating_income = EXCLUDED.operating_income,
                        selling_general_and_administrative = EXCLUDED.selling_general_and_administrative,
                        research_and_development = EXCLUDED.research_and_development,
                        operating_expenses = EXCLUDED.operating_expenses,
                        investment_income_net = EXCLUDED.investment_income_net,
                        net_interest_income = EXCLUDED.net_interest_income,
                        interest_income = EXCLUDED.interest_income,
                        interest_expense = EXCLUDED.interest_expense,
                        non_interest_income = EXCLUDED.non_interest_income,
                        other_non_operating_income = EXCLUDED.other_non_operating_income,
                        depreciation = EXCLUDED.depreciation,
                        depreciation_and_amortization = EXCLUDED.depreciation_and_amortization,
                        income_before_tax = EXCLUDED.income_before_tax,
                        income_tax_expense = EXCLUDED.income_tax_expense,
                        interest_and_debt_expense = EXCLUDED.interest_and_debt_expense,
                        net_income_from_continuing_operations = EXCLUDED.net_income_from_continuing_operations,
                        comprehensive_income_net_of_tax = EXCLUDED.comprehensive_income_net_of_tax,
                        ebit = EXCLUDED.ebit,
                        ebitda = EXCLUDED.ebitda,
                        net_income = EXCLUDED.net_income,
                        created_at = EXCLUDED.created_at
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(income_statement_data)
        except Exception as e:
            logger.error(f"[INCOME_STMT] Error in save_income_statement_data_optimized: {e}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            logger.info("[INCOME_STMT] Falling back to regular save")
            return await self.save_income_statement_data(income_statement_data)

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern - OPTIMIZED"""
        for i, symbol in enumerate(symbols, 1):
            try:
                if self.collection_logger.is_collected('us_income_statement', symbol, []):
                    continue

                api_data = await self.get_income_statement_data(symbol)
                if api_data:
                    transformed = self.transform_income_statement_data(api_data, symbol)
                    if transformed:
                        await data_queue.put((symbol, transformed))
                        logger.info(f"[INCOME_STMT API] [{i}/{len(symbols)}] Queued {symbol}")

                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[INCOME_STMT API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 50):
        """DB worker for pipeline pattern - OPTIMIZED"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_income_statement_data_optimized(batch)
                    total_saved += saved
                break
            symbol, data = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_income_statement_data_optimized(batch)
                if saved > 0:
                    total_saved += saved
                    for record in batch:
                        self.collection_logger.mark_collected('us_income_statement', record['symbol'], [], 1)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[INCOME_STMT DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern and all 9 optimizations"""
        logger.info(f"[INCOME_STMT OPTIMIZED] Starting collection")
        logger.info(f"[INCOME_STMT OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")

        await self.init_pool()

        all_symbols = await self.get_existing_symbols()
        if not all_symbols:
            logger.error("[INCOME_STMT] No symbols found")
            await self.close_pool()
            return

        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_income_statement', s, [])]
        logger.info(f"[INCOME_STMT] Total: {len(all_symbols)}, To process: {len(symbols)}")

        if not symbols:
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=50))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[INCOME_STMT OPTIMIZED] Completed in {duration}, saved {total_saved} records")
    
    async def collect_income_statement(self):
        
        symbols = await self.get_symbols_needing_update()
        
        if not symbols:
            return
        
        total_processed = 0
        total_saved = 0
        
        for i, symbol in enumerate(symbols, 1):
            try:
                
                api_data = self.get_income_statement_data(symbol)
                
                if api_data:
                    transformed_data = self.transform_income_statement_data(api_data, symbol)
                    
                    if transformed_data:
                        saved_count = await self.save_income_statement_data(transformed_data)
                        total_saved += saved_count
                    else:
                        logger.warning(f"No Income Statement data to save for {symbol}")
                else:
                    logger.warning(f"No Income Statement data retrieved for {symbol}")

                total_processed += 1

                if i < len(symbols):
                    await asyncio.sleep(0.2)

            except Exception as e:
                logger.error(f"Error processing symbol {symbol}: {str(e)}")
                continue
        
    
    async def get_collection_status(self):
        try:
            conn = await self.get_connection()
            
            total_records_row = await conn.fetchrow('SELECT COUNT(*) FROM us_income_statement')
            total_records = total_records_row[0]
            
            symbols_row = await conn.fetchrow('SELECT COUNT(DISTINCT symbol) FROM us_income_statement')
            symbols_count = symbols_row[0]
            
            latest_row = await conn.fetchrow('''
                SELECT fiscal_date_ending, symbol FROM us_income_statement 
                ORDER BY fiscal_date_ending DESC LIMIT 1
            ''')
            
            earliest_row = await conn.fetchrow('''
                SELECT fiscal_date_ending, symbol FROM us_income_statement 
                ORDER BY fiscal_date_ending ASC LIMIT 1
            ''')
            
            await conn.close()
            
            if latest_row:
                pass
            if earliest_row:
                pass
            
            return total_records
            
        except Exception as e:
            logger.error(f"Failed to get collection status: {str(e)}")
            return 0

class BalanceSheetCollector:
    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2, target_date: date = None):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1
        self.call_interval = call_interval
        self.start_date = date(2021, 1, 1)  # Balance sheets from 2021

        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_balance_sheet_collected.json'
        else:
            log_file_path = 'log/us_balance_sheet_collected.json'

        self.collection_logger = CollectionLogger(log_file_path)
        self.pool = None
        self.session = None

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
        logger.info("[BALANCE_SHEET] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[BALANCE_SHEET] Database connection pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_existing_symbols(self) -> List[str]:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[BALANCE_SHEET] Found {len(symbols)} symbols")
            return symbols
        except Exception as e:
            logger.error(f"[BALANCE_SHEET] Failed to get existing symbols: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return []

    async def get_symbols_with_recent_data(self) -> set:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('''
                SELECT DISTINCT symbol
                FROM us_balance_sheet
                WHERE created_at >= NOW() - INTERVAL '7 days'
            ''')
            await conn.close()
            recent_symbols = {row['symbol'] for row in rows}
            return recent_symbols
        except Exception as e:
            logger.warning(f"Failed to get symbols with recent data: {str(e)}")
            return set()
    
    async def get_symbols_needing_update(self) -> set:
        try:
            all_symbols = await self.get_existing_symbols()
            recent_symbols = await self.get_symbols_with_recent_data()
            symbols_needing_update = all_symbols - recent_symbols
            
            
            return symbols_needing_update
            
        except Exception as e:
            logger.error(f"Failed to determine symbols needing update: {str(e)}")
            return await self.get_existing_symbols()
    
    async def get_balance_sheet_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch Balance Sheet data with async aiohttp - OPTIMIZED"""
        try:
            params = {
                'function': 'BALANCE_SHEET',
                'symbol': symbol,
                'apikey': self.api_key
            }
            async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'Error Message' in data:
                        logger.error(f"[BALANCE_SHEET] API error for {symbol}: {data['Error Message']}")
                        return None
                    elif 'Note' in data:
                        logger.warning(f"[BALANCE_SHEET] API limit reached: {data['Note']}")
                        return None
                    elif 'annualReports' in data:
                        return data
                    else:
                        logger.warning(f"[BALANCE_SHEET] Unexpected response format for {symbol}: {list(data.keys())}")
                        return None
                else:
                    logger.error(f"[BALANCE_SHEET] API request failed for {symbol}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"[BALANCE_SHEET] Error fetching data for {symbol}: {e}")
            return None
    
    def transform_balance_sheet_data(self, api_data: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
        try:
            transformed_records = []
            
            annual_reports = api_data.get("annualReports", [])
            
            start_date = date(2021, 1, 1)
            
            for report in annual_reports:
                try:
                    fiscal_date_ending = report.get("fiscalDateEnding", "")
                    if not fiscal_date_ending:
                        continue
                    
                    parsed_date = datetime.strptime(fiscal_date_ending, "%Y-%m-%d").date()
                    
                    if parsed_date < start_date:
                        continue

                    # Safe currency code extraction (max 3 chars)
                    currency = report.get("reportedCurrency", "")
                    if currency and isinstance(currency, str):
                        currency = currency.strip()[:3]
                    else:
                        currency = ""

                    transformed = {
                        'symbol': symbol,
                        'fiscal_date_ending': parsed_date,
                        'reported_currency': currency,
                        'total_assets': self.safe_bigint(report.get("totalAssets")),
                        'total_current_assets': self.safe_bigint(report.get("totalCurrentAssets")),
                        'cash_and_cash_equivalents_at_carrying_value': self.safe_bigint(report.get("cashAndCashEquivalentsAtCarryingValue")),
                        'cash_and_short_term_investments': self.safe_bigint(report.get("cashAndShortTermInvestments")),
                        'inventory': self.safe_bigint(report.get("inventory")),
                        'current_net_receivables': self.safe_bigint(report.get("currentNetReceivables")),
                        'total_non_current_assets': self.safe_bigint(report.get("totalNonCurrentAssets")),
                        'property_plant_equipment': self.safe_bigint(report.get("propertyPlantEquipment")),
                        'accumulated_depreciation_amortization_ppe': self.safe_bigint(report.get("accumulatedDepreciationAmortizationPPE")),
                        'intangible_assets': self.safe_bigint(report.get("intangibleAssets")),
                        'intangible_assets_excluding_goodwill': self.safe_bigint(report.get("intangibleAssetsExcludingGoodwill")),
                        'goodwill': self.safe_bigint(report.get("goodwill")),
                        'investments': self.safe_bigint(report.get("investments")),
                        'long_term_investments': self.safe_bigint(report.get("longTermInvestments")),
                        'short_term_investments': self.safe_bigint(report.get("shortTermInvestments")),
                        'other_current_assets': self.safe_bigint(report.get("otherCurrentAssets")),
                        'other_non_current_assets': self.safe_bigint(report.get("otherNonCurrentAssets")),
                        'total_liabilities': self.safe_bigint(report.get("totalLiabilities")),
                        'total_current_liabilities': self.safe_bigint(report.get("totalCurrentLiabilities")),
                        'current_accounts_payable': self.safe_bigint(report.get("currentAccountsPayable")),
                        'deferred_revenue': self.safe_bigint(report.get("deferredRevenue")),
                        'current_debt': self.safe_bigint(report.get("currentDebt")),
                        'short_term_debt': self.safe_bigint(report.get("shortTermDebt")),
                        'total_non_current_liabilities': self.safe_bigint(report.get("totalNonCurrentLiabilities")),
                        'capital_lease_obligations': self.safe_bigint(report.get("capitalLeaseObligations")),
                        'long_term_debt': self.safe_bigint(report.get("longTermDebt")),
                        'current_long_term_debt': self.safe_bigint(report.get("currentLongTermDebt")),
                        'long_term_debt_noncurrent': self.safe_bigint(report.get("longTermDebtNoncurrent")),
                        'short_long_term_debt_total': self.safe_bigint(report.get("shortLongTermDebtTotal")),
                        'other_current_liabilities': self.safe_bigint(report.get("otherCurrentLiabilities")),
                        'other_non_current_liabilities': self.safe_bigint(report.get("otherNonCurrentLiabilities")),
                        'total_shareholder_equity': self.safe_bigint(report.get("totalShareholderEquity")),
                        'treasury_stock': self.safe_bigint(report.get("treasuryStock")),
                        'retained_earnings': self.safe_bigint(report.get("retainedEarnings")),
                        'common_stock': self.safe_bigint(report.get("commonStock")),
                        'common_stock_shares_outstanding': self.safe_bigint(report.get("commonStockSharesOutstanding")),
                        'created_at': datetime.now()
                    }
                    
                    transformed_records.append(transformed)
                    
                except Exception as e:
                    logger.error(f"Error transforming record for {symbol}: {str(e)}, Date: {fiscal_date_ending}")
                    continue
            
            if transformed_records:
                dates = [record['fiscal_date_ending'] for record in transformed_records]
                earliest = min(dates)
                latest = max(dates)
            
            return transformed_records
            
        except Exception as e:
            logger.error(f"Error transforming Balance Sheet data for {symbol}: {str(e)}")
            return []
    
    def safe_bigint(self, value: str) -> Optional[int]:
        if not value or value.strip() in ['', 'N/A', '-', 'null', 'None']:
            return None

        try:
            cleaned_value = ''.join(c for c in str(value).strip() if c.isdigit() or c == '-')
            if not cleaned_value or cleaned_value == '-':
                return None
            result = int(cleaned_value)
            # PostgreSQL BIGINT range check
            if result < -9223372036854775808 or result > 9223372036854775807:
                logger.warning(f"Value out of BIGINT range, setting to None: {value}")
                return None
            return result
        except (ValueError, TypeError):
            logger.warning(f"Could not convert to bigint: {value}")
            return None
    
    async def save_balance_sheet_data(self, balance_sheet_data: List[Dict[str, Any]]) -> int:
        if not balance_sheet_data:
            return 0
        
        try:
            conn = await self.get_connection()
            saved_count = 0
            
            for record in balance_sheet_data:
                try:
                    await conn.execute('''
                        INSERT INTO us_balance_sheet 
                        (symbol, fiscal_date_ending, reported_currency, total_assets, total_current_assets, 
                         cash_and_cash_equivalents_at_carrying_value, cash_and_short_term_investments, inventory, 
                         current_net_receivables, total_non_current_assets, property_plant_equipment, 
                         accumulated_depreciation_amortization_ppe, intangible_assets, intangible_assets_excluding_goodwill, 
                         goodwill, investments, long_term_investments, short_term_investments, other_current_assets, 
                         other_non_current_assets, total_liabilities, total_current_liabilities, current_accounts_payable, 
                         deferred_revenue, current_debt, short_term_debt, total_non_current_liabilities, 
                         capital_lease_obligations, long_term_debt, current_long_term_debt, long_term_debt_noncurrent, 
                         short_long_term_debt_total, other_current_liabilities, other_non_current_liabilities, 
                         total_shareholder_equity, treasury_stock, retained_earnings, common_stock, 
                         common_stock_shares_outstanding, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40)
                        ON CONFLICT (symbol, fiscal_date_ending) DO UPDATE SET
                        reported_currency = EXCLUDED.reported_currency,
                        total_assets = EXCLUDED.total_assets,
                        total_current_assets = EXCLUDED.total_current_assets,
                        cash_and_cash_equivalents_at_carrying_value = EXCLUDED.cash_and_cash_equivalents_at_carrying_value,
                        cash_and_short_term_investments = EXCLUDED.cash_and_short_term_investments,
                        inventory = EXCLUDED.inventory,
                        current_net_receivables = EXCLUDED.current_net_receivables,
                        total_non_current_assets = EXCLUDED.total_non_current_assets,
                        property_plant_equipment = EXCLUDED.property_plant_equipment,
                        accumulated_depreciation_amortization_ppe = EXCLUDED.accumulated_depreciation_amortization_ppe,
                        intangible_assets = EXCLUDED.intangible_assets,
                        intangible_assets_excluding_goodwill = EXCLUDED.intangible_assets_excluding_goodwill,
                        goodwill = EXCLUDED.goodwill,
                        investments = EXCLUDED.investments,
                        long_term_investments = EXCLUDED.long_term_investments,
                        short_term_investments = EXCLUDED.short_term_investments,
                        other_current_assets = EXCLUDED.other_current_assets,
                        other_non_current_assets = EXCLUDED.other_non_current_assets,
                        total_liabilities = EXCLUDED.total_liabilities,
                        total_current_liabilities = EXCLUDED.total_current_liabilities,
                        current_accounts_payable = EXCLUDED.current_accounts_payable,
                        deferred_revenue = EXCLUDED.deferred_revenue,
                        current_debt = EXCLUDED.current_debt,
                        short_term_debt = EXCLUDED.short_term_debt,
                        total_non_current_liabilities = EXCLUDED.total_non_current_liabilities,
                        capital_lease_obligations = EXCLUDED.capital_lease_obligations,
                        long_term_debt = EXCLUDED.long_term_debt,
                        current_long_term_debt = EXCLUDED.current_long_term_debt,
                        long_term_debt_noncurrent = EXCLUDED.long_term_debt_noncurrent,
                        short_long_term_debt_total = EXCLUDED.short_long_term_debt_total,
                        other_current_liabilities = EXCLUDED.other_current_liabilities,
                        other_non_current_liabilities = EXCLUDED.other_non_current_liabilities,
                        total_shareholder_equity = EXCLUDED.total_shareholder_equity,
                        treasury_stock = EXCLUDED.treasury_stock,
                        retained_earnings = EXCLUDED.retained_earnings,
                        common_stock = EXCLUDED.common_stock,
                        common_stock_shares_outstanding = EXCLUDED.common_stock_shares_outstanding,
                        created_at = EXCLUDED.created_at
                    ''', 
                    record['symbol'],
                    record['fiscal_date_ending'],
                    record['reported_currency'],
                    record['total_assets'],
                    record['total_current_assets'],
                    record['cash_and_cash_equivalents_at_carrying_value'],
                    record['cash_and_short_term_investments'],
                    record['inventory'],
                    record['current_net_receivables'],
                    record['total_non_current_assets'],
                    record['property_plant_equipment'],
                    record['accumulated_depreciation_amortization_ppe'],
                    record['intangible_assets'],
                    record['intangible_assets_excluding_goodwill'],
                    record['goodwill'],
                    record['investments'],
                    record['long_term_investments'],
                    record['short_term_investments'],
                    record['other_current_assets'],
                    record['other_non_current_assets'],
                    record['total_liabilities'],
                    record['total_current_liabilities'],
                    record['current_accounts_payable'],
                    record['deferred_revenue'],
                    record['current_debt'],
                    record['short_term_debt'],
                    record['total_non_current_liabilities'],
                    record['capital_lease_obligations'],
                    record['long_term_debt'],
                    record['current_long_term_debt'],
                    record['long_term_debt_noncurrent'],
                    record['short_long_term_debt_total'],
                    record['other_current_liabilities'],
                    record['other_non_current_liabilities'],
                    record['total_shareholder_equity'],
                    record['treasury_stock'],
                    record['retained_earnings'],
                    record['common_stock'],
                    record['common_stock_shares_outstanding'],
                    record['created_at']
                    )
                    
                    saved_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to save Balance Sheet record for {record.get('symbol', 'unknown')}: {str(e)}")

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return saved_count

        except Exception as e:
            logger.error(f"[BALANCE_SHEET] Database error while saving Balance Sheet data: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return 0

    async def save_balance_sheet_data_optimized(self, balance_sheet_data: List[Dict[str, Any]]) -> int:
        """Save Balance Sheet data using COPY command - OPTIMIZED"""
        if not balance_sheet_data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                # Transaction optimization
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                # Create temp table
                await conn.execute('''
                    CREATE TEMP TABLE temp_balance_sheet (
                        symbol VARCHAR(20),
                        fiscal_date_ending DATE,
                        reported_currency VARCHAR(10),
                        total_assets BIGINT,
                        total_current_assets BIGINT,
                        cash_and_cash_equivalents_at_carrying_value BIGINT,
                        cash_and_short_term_investments BIGINT,
                        inventory BIGINT,
                        current_net_receivables BIGINT,
                        total_non_current_assets BIGINT,
                        property_plant_equipment BIGINT,
                        accumulated_depreciation_amortization_ppe BIGINT,
                        intangible_assets BIGINT,
                        intangible_assets_excluding_goodwill BIGINT,
                        goodwill BIGINT,
                        investments BIGINT,
                        long_term_investments BIGINT,
                        short_term_investments BIGINT,
                        other_current_assets BIGINT,
                        other_non_current_assets BIGINT,
                        total_liabilities BIGINT,
                        total_current_liabilities BIGINT,
                        current_accounts_payable BIGINT,
                        deferred_revenue BIGINT,
                        current_debt BIGINT,
                        short_term_debt BIGINT,
                        total_non_current_liabilities BIGINT,
                        capital_lease_obligations BIGINT,
                        long_term_debt BIGINT,
                        current_long_term_debt BIGINT,
                        long_term_debt_noncurrent BIGINT,
                        short_long_term_debt_total BIGINT,
                        other_current_liabilities BIGINT,
                        other_non_current_liabilities BIGINT,
                        total_shareholder_equity BIGINT,
                        treasury_stock BIGINT,
                        retained_earnings BIGINT,
                        common_stock BIGINT,
                        common_stock_shares_outstanding BIGINT,
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')

                # Prepare data for COPY
                rows = [[
                    record['symbol'], record['fiscal_date_ending'], record['reported_currency'],
                    record['total_assets'], record['total_current_assets'],
                    record['cash_and_cash_equivalents_at_carrying_value'],
                    record['cash_and_short_term_investments'], record['inventory'],
                    record['current_net_receivables'], record['total_non_current_assets'],
                    record['property_plant_equipment'], record['accumulated_depreciation_amortization_ppe'],
                    record['intangible_assets'], record['intangible_assets_excluding_goodwill'],
                    record['goodwill'], record['investments'], record['long_term_investments'],
                    record['short_term_investments'], record['other_current_assets'],
                    record['other_non_current_assets'], record['total_liabilities'],
                    record['total_current_liabilities'], record['current_accounts_payable'],
                    record['deferred_revenue'], record['current_debt'], record['short_term_debt'],
                    record['total_non_current_liabilities'], record['capital_lease_obligations'],
                    record['long_term_debt'], record['current_long_term_debt'],
                    record['long_term_debt_noncurrent'], record['short_long_term_debt_total'],
                    record['other_current_liabilities'], record['other_non_current_liabilities'],
                    record['total_shareholder_equity'], record['treasury_stock'],
                    record['retained_earnings'], record['common_stock'],
                    record['common_stock_shares_outstanding'], record['created_at']
                ] for record in balance_sheet_data]

                # COPY to temp table
                await conn.copy_records_to_table(
                    'temp_balance_sheet', records=rows,
                    columns=['symbol', 'fiscal_date_ending', 'reported_currency', 'total_assets',
                            'total_current_assets', 'cash_and_cash_equivalents_at_carrying_value',
                            'cash_and_short_term_investments', 'inventory', 'current_net_receivables',
                            'total_non_current_assets', 'property_plant_equipment',
                            'accumulated_depreciation_amortization_ppe', 'intangible_assets',
                            'intangible_assets_excluding_goodwill', 'goodwill', 'investments',
                            'long_term_investments', 'short_term_investments', 'other_current_assets',
                            'other_non_current_assets', 'total_liabilities', 'total_current_liabilities',
                            'current_accounts_payable', 'deferred_revenue', 'current_debt', 'short_term_debt',
                            'total_non_current_liabilities', 'capital_lease_obligations', 'long_term_debt',
                            'current_long_term_debt', 'long_term_debt_noncurrent', 'short_long_term_debt_total',
                            'other_current_liabilities', 'other_non_current_liabilities',
                            'total_shareholder_equity', 'treasury_stock', 'retained_earnings', 'common_stock',
                            'common_stock_shares_outstanding', 'created_at']
                )

                # Upsert from temp to main
                await conn.execute('''
                    INSERT INTO us_balance_sheet SELECT * FROM temp_balance_sheet
                    ON CONFLICT (symbol, fiscal_date_ending) DO UPDATE SET
                        reported_currency = EXCLUDED.reported_currency,
                        total_assets = EXCLUDED.total_assets,
                        total_current_assets = EXCLUDED.total_current_assets,
                        cash_and_cash_equivalents_at_carrying_value = EXCLUDED.cash_and_cash_equivalents_at_carrying_value,
                        cash_and_short_term_investments = EXCLUDED.cash_and_short_term_investments,
                        inventory = EXCLUDED.inventory,
                        current_net_receivables = EXCLUDED.current_net_receivables,
                        total_non_current_assets = EXCLUDED.total_non_current_assets,
                        property_plant_equipment = EXCLUDED.property_plant_equipment,
                        accumulated_depreciation_amortization_ppe = EXCLUDED.accumulated_depreciation_amortization_ppe,
                        intangible_assets = EXCLUDED.intangible_assets,
                        intangible_assets_excluding_goodwill = EXCLUDED.intangible_assets_excluding_goodwill,
                        goodwill = EXCLUDED.goodwill,
                        investments = EXCLUDED.investments,
                        long_term_investments = EXCLUDED.long_term_investments,
                        short_term_investments = EXCLUDED.short_term_investments,
                        other_current_assets = EXCLUDED.other_current_assets,
                        other_non_current_assets = EXCLUDED.other_non_current_assets,
                        total_liabilities = EXCLUDED.total_liabilities,
                        total_current_liabilities = EXCLUDED.total_current_liabilities,
                        current_accounts_payable = EXCLUDED.current_accounts_payable,
                        deferred_revenue = EXCLUDED.deferred_revenue,
                        current_debt = EXCLUDED.current_debt,
                        short_term_debt = EXCLUDED.short_term_debt,
                        total_non_current_liabilities = EXCLUDED.total_non_current_liabilities,
                        capital_lease_obligations = EXCLUDED.capital_lease_obligations,
                        long_term_debt = EXCLUDED.long_term_debt,
                        current_long_term_debt = EXCLUDED.current_long_term_debt,
                        long_term_debt_noncurrent = EXCLUDED.long_term_debt_noncurrent,
                        short_long_term_debt_total = EXCLUDED.short_long_term_debt_total,
                        other_current_liabilities = EXCLUDED.other_current_liabilities,
                        other_non_current_liabilities = EXCLUDED.other_non_current_liabilities,
                        total_shareholder_equity = EXCLUDED.total_shareholder_equity,
                        treasury_stock = EXCLUDED.treasury_stock,
                        retained_earnings = EXCLUDED.retained_earnings,
                        common_stock = EXCLUDED.common_stock,
                        common_stock_shares_outstanding = EXCLUDED.common_stock_shares_outstanding,
                        created_at = EXCLUDED.created_at
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(balance_sheet_data)
        except Exception as e:
            logger.error(f"[BALANCE_SHEET] Error in save_balance_sheet_data_optimized: {e}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            logger.info("[BALANCE_SHEET] Falling back to regular save")
            return await self.save_balance_sheet_data(balance_sheet_data)

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern - OPTIMIZED"""
        for i, symbol in enumerate(symbols, 1):
            try:
                if self.collection_logger.is_collected('us_balance_sheet', symbol, []):
                    continue

                api_data = await self.get_balance_sheet_data(symbol)
                if api_data:
                    transformed = self.transform_balance_sheet_data(api_data, symbol)
                    if transformed:
                        await data_queue.put((symbol, transformed))
                        logger.info(f"[BALANCE_SHEET API] [{i}/{len(symbols)}] Queued {symbol}")

                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[BALANCE_SHEET API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 50):
        """DB worker for pipeline pattern - OPTIMIZED"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_balance_sheet_data_optimized(batch)
                    total_saved += saved
                break
            symbol, data = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_balance_sheet_data_optimized(batch)
                if saved > 0:
                    total_saved += saved
                    for record in batch:
                        self.collection_logger.mark_collected('us_balance_sheet', record['symbol'], [], 1)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[BALANCE_SHEET DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern and all 9 optimizations"""
        logger.info(f"[BALANCE_SHEET OPTIMIZED] Starting collection")
        logger.info(f"[BALANCE_SHEET OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")

        await self.init_pool()

        all_symbols = await self.get_existing_symbols()
        if not all_symbols:
            logger.error("[BALANCE_SHEET] No symbols found")
            await self.close_pool()
            return

        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_balance_sheet', s, [])]
        logger.info(f"[BALANCE_SHEET] Total: {len(all_symbols)}, To process: {len(symbols)}")

        if not symbols:
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=50))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[BALANCE_SHEET OPTIMIZED] Completed in {duration}, saved {total_saved} records")

    async def collect_balance_sheet(self):

        symbols = await self.get_symbols_needing_update()
        
        if not symbols:
            return
        
        total_processed = 0
        total_saved = 0
        
        for i, symbol in enumerate(symbols, 1):
            try:
                
                api_data = self.get_balance_sheet_data(symbol)
                
                if api_data:
                    transformed_data = self.transform_balance_sheet_data(api_data, symbol)
                    
                    if transformed_data:
                        saved_count = await self.save_balance_sheet_data(transformed_data)
                        total_saved += saved_count
                    else:
                        logger.warning(f"No Balance Sheet data to save for {symbol}")
                else:
                    logger.warning(f"No Balance Sheet data retrieved for {symbol}")

                total_processed += 1

                if i < len(symbols):
                    await asyncio.sleep(0.2)

            except Exception as e:
                logger.error(f"Error processing symbol {symbol}: {str(e)}")
                continue
        
    
    async def get_collection_status(self):
        try:
            conn = await self.get_connection()
            
            total_records_row = await conn.fetchrow('SELECT COUNT(*) FROM us_balance_sheet')
            total_records = total_records_row[0]
            
            symbols_row = await conn.fetchrow('SELECT COUNT(DISTINCT symbol) FROM us_balance_sheet')
            symbols_count = symbols_row[0]
            
            latest_row = await conn.fetchrow('''
                SELECT fiscal_date_ending, symbol FROM us_balance_sheet 
                ORDER BY fiscal_date_ending DESC LIMIT 1
            ''')
            
            earliest_row = await conn.fetchrow('''
                SELECT fiscal_date_ending, symbol FROM us_balance_sheet 
                ORDER BY fiscal_date_ending ASC LIMIT 1
            ''')
            
            await conn.close()
            
            if latest_row:
                pass
            if earliest_row:
                pass
            
            return total_records
            
        except Exception as e:
            logger.error(f"Failed to get collection status: {str(e)}")
            return 0

class CashFlowCollector:
    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2, target_date: date = None):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1
        self.call_interval = call_interval
        self.start_date = date(2021, 1, 1)  # Cash flow statements from 2021

        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_cash_flow_collected.json'
        else:
            log_file_path = 'log/us_cash_flow_collected.json'

        self.collection_logger = CollectionLogger(log_file_path)
        self.pool = None
        self.session = None

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
        logger.info("[CASH_FLOW] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[CASH_FLOW] Database connection pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_existing_symbols(self) -> List[str]:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[CASH_FLOW] Found {len(symbols)} symbols")
            return symbols
        except Exception as e:
            logger.error(f"[CASH_FLOW] Failed to get existing symbols: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return []

    async def get_symbols_with_recent_data(self) -> set:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('''
                SELECT DISTINCT symbol 
                FROM us_cash_flow 
                WHERE created_at >= NOW() - INTERVAL '7 days'
            ''')
            await conn.close()
            recent_symbols = {row['symbol'] for row in rows}
            return recent_symbols
        except Exception as e:
            logger.warning(f"Failed to get symbols with recent data: {str(e)}")
            return set()
    
    async def get_symbols_needing_update(self) -> set:
        try:
            all_symbols = await self.get_existing_symbols()
            recent_symbols = await self.get_symbols_with_recent_data()
            symbols_needing_update = all_symbols - recent_symbols


            return symbols_needing_update

        except Exception as e:
            logger.error(f"Failed to determine symbols needing update: {str(e)}")
            return await self.get_existing_symbols()

    async def get_cash_flow_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch Cash Flow data with async aiohttp - OPTIMIZED"""
        try:
            params = {
                'function': 'CASH_FLOW',
                'symbol': symbol,
                'apikey': self.api_key
            }
            async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'Error Message' in data:
                        logger.error(f"[CASH_FLOW] API error for {symbol}: {data['Error Message']}")
                        return None
                    elif 'Note' in data:
                        logger.warning(f"[CASH_FLOW] API limit reached: {data['Note']}")
                        return None
                    elif 'annualReports' in data:
                        return data
                    else:
                        logger.warning(f"[CASH_FLOW] Unexpected response format for {symbol}: {list(data.keys())}")
                        return None
                else:
                    logger.error(f"[CASH_FLOW] API request failed for {symbol}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"[CASH_FLOW] Error fetching data for {symbol}: {e}")
            return None
    
    def transform_cash_flow_data(self, api_data: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
        try:
            transformed_records = []
            
            annual_reports = api_data.get("annualReports", [])
            
            start_date = date(2021, 1, 1)
            
            for report in annual_reports:
                try:
                    fiscal_date_ending = report.get("fiscalDateEnding", "")
                    if not fiscal_date_ending:
                        continue
                    
                    parsed_date = datetime.strptime(fiscal_date_ending, "%Y-%m-%d").date()
                    
                    if parsed_date < start_date:
                        continue
                    
                    # Safe currency code extraction (max 3 chars)
                    currency = report.get("reportedCurrency", "")
                    if currency and isinstance(currency, str):
                        currency = currency.strip()[:3]
                    else:
                        currency = ""

                    transformed = {
                        'symbol': symbol,
                        'fiscal_date_ending': parsed_date,
                        'reported_currency': currency,
                        'operating_cashflow': self.safe_bigint(report.get("operatingCashflow")),
                        'payments_for_operating_activities': self.safe_bigint(report.get("paymentsForOperatingActivities")),
                        'proceeds_from_operating_activities': self.safe_bigint(report.get("proceedsFromOperatingActivities")),
                        'change_in_operating_liabilities': self.safe_bigint(report.get("changeInOperatingLiabilities")),
                        'change_in_operating_assets': self.safe_bigint(report.get("changeInOperatingAssets")),
                        'depreciation_depletion_and_amortization': self.safe_bigint(report.get("depreciationDepletionAndAmortization")),
                        'capital_expenditures': self.safe_bigint(report.get("capitalExpenditures")),
                        'change_in_receivables': self.safe_bigint(report.get("changeInReceivables")),
                        'change_in_inventory': self.safe_bigint(report.get("changeInInventory")),
                        'profit_loss': self.safe_bigint(report.get("profitLoss")),
                        'cashflow_from_investment': self.safe_bigint(report.get("cashflowFromInvestment")),
                        'cashflow_from_financing': self.safe_bigint(report.get("cashflowFromFinancing")),
                        'proceeds_from_repayments_of_short_term_debt': self.safe_bigint(report.get("proceedsFromRepaymentsOfShortTermDebt")),
                        'payments_for_repurchase_of_common_stock': self.safe_bigint(report.get("paymentsForRepurchaseOfCommonStock")),
                        'payments_for_repurchase_of_equity': self.safe_bigint(report.get("paymentsForRepurchaseOfEquity")),
                        'payments_for_repurchase_of_preferred_stock': self.safe_bigint(report.get("paymentsForRepurchaseOfPreferredStock")),
                        'dividend_payout': self.safe_bigint(report.get("dividendPayout")),
                        'dividend_payout_common_stock': self.safe_bigint(report.get("dividendPayoutCommonStock")),
                        'dividend_payout_preferred_stock': self.safe_bigint(report.get("dividendPayoutPreferredStock")),
                        'proceeds_from_issuance_of_common_stock': self.safe_bigint(report.get("proceedsFromIssuanceOfCommonStock")),
                        'proceeds_from_issuance_of_long_term_debt_and_capital_securities': self.safe_bigint(report.get("proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet")),
                        'proceeds_from_issuance_of_preferred_stock': self.safe_bigint(report.get("proceedsFromIssuanceOfPreferredStock")),
                        'proceeds_from_repurchase_of_equity': self.safe_bigint(report.get("proceedsFromRepurchaseOfEquity")),
                        'proceeds_from_sale_of_treasury_stock': self.safe_bigint(report.get("proceedsFromSaleOfTreasuryStock")),
                        'change_in_cash_and_cash_equivalents': self.safe_bigint(report.get("changeInCashAndCashEquivalents")),
                        'change_in_exchange_rate': self.safe_bigint(report.get("changeInExchangeRate")),
                        'net_income': self.safe_bigint(report.get("netIncome")),
                        'created_at': datetime.now()
                    }
                    
                    transformed_records.append(transformed)
                    
                except Exception as e:
                    logger.error(f"Error transforming record for {symbol}: {str(e)}, Date: {fiscal_date_ending}")
                    continue
            
            if transformed_records:
                dates = [record['fiscal_date_ending'] for record in transformed_records]
                earliest = min(dates)
                latest = max(dates)
            
            return transformed_records
            
        except Exception as e:
            logger.error(f"Error transforming Cash Flow data for {symbol}: {str(e)}")
            return []
    
    def safe_bigint(self, value: str) -> Optional[int]:
        if not value or value.strip() in ['', 'N/A', '-', 'null', 'None']:
            return None

        try:
            cleaned_value = ''.join(c for c in str(value).strip() if c.isdigit() or c == '-')
            if not cleaned_value or cleaned_value == '-':
                return None
            result = int(cleaned_value)
            # PostgreSQL BIGINT range check
            if result < -9223372036854775808 or result > 9223372036854775807:
                logger.warning(f"Value out of BIGINT range, setting to None: {value}")
                return None
            return result
        except (ValueError, TypeError):
            logger.warning(f"Could not convert to bigint: {value}")
            return None
    
    async def save_cash_flow_data(self, cash_flow_data: List[Dict[str, Any]]) -> int:
        if not cash_flow_data:
            return 0
        
        try:
            conn = await self.get_connection()
            saved_count = 0
            
            for record in cash_flow_data:
                try:
                    await conn.execute('''
                        INSERT INTO us_cash_flow 
                        (symbol, fiscal_date_ending, reported_currency, operating_cashflow, payments_for_operating_activities,
                         proceeds_from_operating_activities, change_in_operating_liabilities, change_in_operating_assets,
                         depreciation_depletion_and_amortization, capital_expenditures, change_in_receivables,
                         change_in_inventory, profit_loss, cashflow_from_investment, cashflow_from_financing,
                         proceeds_from_repayments_of_short_term_debt, payments_for_repurchase_of_common_stock,
                         payments_for_repurchase_of_equity, payments_for_repurchase_of_preferred_stock, dividend_payout,
                         dividend_payout_common_stock, dividend_payout_preferred_stock, proceeds_from_issuance_of_common_stock,
                         proceeds_from_issuance_of_long_term_debt_and_capital_securities, proceeds_from_issuance_of_preferred_stock,
                         proceeds_from_repurchase_of_equity, proceeds_from_sale_of_treasury_stock, change_in_cash_and_cash_equivalents,
                         change_in_exchange_rate, net_income, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31)
                        ON CONFLICT (symbol, fiscal_date_ending) DO UPDATE SET
                        reported_currency = EXCLUDED.reported_currency,
                        operating_cashflow = EXCLUDED.operating_cashflow,
                        payments_for_operating_activities = EXCLUDED.payments_for_operating_activities,
                        proceeds_from_operating_activities = EXCLUDED.proceeds_from_operating_activities,
                        change_in_operating_liabilities = EXCLUDED.change_in_operating_liabilities,
                        change_in_operating_assets = EXCLUDED.change_in_operating_assets,
                        depreciation_depletion_and_amortization = EXCLUDED.depreciation_depletion_and_amortization,
                        capital_expenditures = EXCLUDED.capital_expenditures,
                        change_in_receivables = EXCLUDED.change_in_receivables,
                        change_in_inventory = EXCLUDED.change_in_inventory,
                        profit_loss = EXCLUDED.profit_loss,
                        cashflow_from_investment = EXCLUDED.cashflow_from_investment,
                        cashflow_from_financing = EXCLUDED.cashflow_from_financing,
                        proceeds_from_repayments_of_short_term_debt = EXCLUDED.proceeds_from_repayments_of_short_term_debt,
                        payments_for_repurchase_of_common_stock = EXCLUDED.payments_for_repurchase_of_common_stock,
                        payments_for_repurchase_of_equity = EXCLUDED.payments_for_repurchase_of_equity,
                        payments_for_repurchase_of_preferred_stock = EXCLUDED.payments_for_repurchase_of_preferred_stock,
                        dividend_payout = EXCLUDED.dividend_payout,
                        dividend_payout_common_stock = EXCLUDED.dividend_payout_common_stock,
                        dividend_payout_preferred_stock = EXCLUDED.dividend_payout_preferred_stock,
                        proceeds_from_issuance_of_common_stock = EXCLUDED.proceeds_from_issuance_of_common_stock,
                        proceeds_from_issuance_of_long_term_debt_and_capital_securities = EXCLUDED.proceeds_from_issuance_of_long_term_debt_and_capital_securities,
                        proceeds_from_issuance_of_preferred_stock = EXCLUDED.proceeds_from_issuance_of_preferred_stock,
                        proceeds_from_repurchase_of_equity = EXCLUDED.proceeds_from_repurchase_of_equity,
                        proceeds_from_sale_of_treasury_stock = EXCLUDED.proceeds_from_sale_of_treasury_stock,
                        change_in_cash_and_cash_equivalents = EXCLUDED.change_in_cash_and_cash_equivalents,
                        change_in_exchange_rate = EXCLUDED.change_in_exchange_rate,
                        net_income = EXCLUDED.net_income,
                        created_at = EXCLUDED.created_at
                    ''', 
                    record['symbol'],
                    record['fiscal_date_ending'],
                    record['reported_currency'],
                    record['operating_cashflow'],
                    record['payments_for_operating_activities'],
                    record['proceeds_from_operating_activities'],
                    record['change_in_operating_liabilities'],
                    record['change_in_operating_assets'],
                    record['depreciation_depletion_and_amortization'],
                    record['capital_expenditures'],
                    record['change_in_receivables'],
                    record['change_in_inventory'],
                    record['profit_loss'],
                    record['cashflow_from_investment'],
                    record['cashflow_from_financing'],
                    record['proceeds_from_repayments_of_short_term_debt'],
                    record['payments_for_repurchase_of_common_stock'],
                    record['payments_for_repurchase_of_equity'],
                    record['payments_for_repurchase_of_preferred_stock'],
                    record['dividend_payout'],
                    record['dividend_payout_common_stock'],
                    record['dividend_payout_preferred_stock'],
                    record.get('proceeds_from_issuance_of_common_stock'),
                    record.get('proceeds_from_issuance_of_long_term_debt_and_capital_securities'),
                    record.get('proceeds_from_issuance_of_preferred_stock'),
                    record.get('proceeds_from_repurchase_of_equity'),
                    record.get('proceeds_from_sale_of_treasury_stock'),
                    record['change_in_cash_and_cash_equivalents'],
                    record['change_in_exchange_rate'],
                    record['net_income'],
                    record['created_at']
                    )
                    
                    saved_count += 1
                    
                except KeyError as e:
                    logger.error(f"Failed to save Cash Flow record for {record.get('symbol', 'unknown')}: Missing key {str(e)}")
                    logger.error(f"Available keys in record: {list(record.keys())}")
                except Exception as e:
                    logger.error(f"Failed to save Cash Flow record for {record.get('symbol', 'unknown')}: {type(e).__name__}: {str(e)}")

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return saved_count

        except Exception as e:
            logger.error(f"[CASH_FLOW] Database error while saving Cash Flow data: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return 0

    async def save_cash_flow_data_optimized(self, cash_flow_data: List[Dict[str, Any]]) -> int:
        """Save Cash Flow data using COPY command - OPTIMIZED"""
        if not cash_flow_data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                # Transaction optimization
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                # Create temp table
                await conn.execute('''
                    CREATE TEMP TABLE temp_cash_flow (
                        symbol VARCHAR(20),
                        fiscal_date_ending DATE,
                        reported_currency VARCHAR(10),
                        operating_cashflow BIGINT,
                        payments_for_operating_activities BIGINT,
                        proceeds_from_operating_activities BIGINT,
                        change_in_operating_liabilities BIGINT,
                        change_in_operating_assets BIGINT,
                        depreciation_depletion_and_amortization BIGINT,
                        capital_expenditures BIGINT,
                        change_in_receivables BIGINT,
                        change_in_inventory BIGINT,
                        profit_loss BIGINT,
                        cashflow_from_investment BIGINT,
                        cashflow_from_financing BIGINT,
                        proceeds_from_repayments_of_short_term_debt BIGINT,
                        payments_for_repurchase_of_common_stock BIGINT,
                        payments_for_repurchase_of_equity BIGINT,
                        payments_for_repurchase_of_preferred_stock BIGINT,
                        dividend_payout BIGINT,
                        dividend_payout_common_stock BIGINT,
                        dividend_payout_preferred_stock BIGINT,
                        proceeds_from_issuance_of_common_stock BIGINT,
                        proceeds_from_issuance_of_long_term_debt_and_capital_securities BIGINT,
                        proceeds_from_issuance_of_preferred_stock BIGINT,
                        proceeds_from_repurchase_of_equity BIGINT,
                        proceeds_from_sale_of_treasury_stock BIGINT,
                        change_in_cash_and_cash_equivalents BIGINT,
                        change_in_exchange_rate BIGINT,
                        net_income BIGINT,
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')

                # Prepare data for COPY
                rows = [[
                    record['symbol'], record['fiscal_date_ending'], record['reported_currency'],
                    record['operating_cashflow'], record['payments_for_operating_activities'],
                    record['proceeds_from_operating_activities'], record['change_in_operating_liabilities'],
                    record['change_in_operating_assets'], record['depreciation_depletion_and_amortization'],
                    record['capital_expenditures'], record['change_in_receivables'], record['change_in_inventory'],
                    record['profit_loss'], record['cashflow_from_investment'], record['cashflow_from_financing'],
                    record['proceeds_from_repayments_of_short_term_debt'], record['payments_for_repurchase_of_common_stock'],
                    record['payments_for_repurchase_of_equity'], record['payments_for_repurchase_of_preferred_stock'],
                    record['dividend_payout'], record['dividend_payout_common_stock'], record['dividend_payout_preferred_stock'],
                    record['proceeds_from_issuance_of_common_stock'], record['proceeds_from_issuance_of_long_term_debt_and_capital_securities'],
                    record['proceeds_from_issuance_of_preferred_stock'], record['proceeds_from_repurchase_of_equity'],
                    record['proceeds_from_sale_of_treasury_stock'], record['change_in_cash_and_cash_equivalents'],
                    record['change_in_exchange_rate'], record['net_income'], record['created_at']
                ] for record in cash_flow_data]

                # COPY to temp table
                await conn.copy_records_to_table(
                    'temp_cash_flow', records=rows,
                    columns=['symbol', 'fiscal_date_ending', 'reported_currency', 'operating_cashflow',
                            'payments_for_operating_activities', 'proceeds_from_operating_activities',
                            'change_in_operating_liabilities', 'change_in_operating_assets',
                            'depreciation_depletion_and_amortization', 'capital_expenditures',
                            'change_in_receivables', 'change_in_inventory', 'profit_loss',
                            'cashflow_from_investment', 'cashflow_from_financing',
                            'proceeds_from_repayments_of_short_term_debt', 'payments_for_repurchase_of_common_stock',
                            'payments_for_repurchase_of_equity', 'payments_for_repurchase_of_preferred_stock',
                            'dividend_payout', 'dividend_payout_common_stock', 'dividend_payout_preferred_stock',
                            'proceeds_from_issuance_of_common_stock', 'proceeds_from_issuance_of_long_term_debt_and_capital_securities',
                            'proceeds_from_issuance_of_preferred_stock', 'proceeds_from_repurchase_of_equity',
                            'proceeds_from_sale_of_treasury_stock', 'change_in_cash_and_cash_equivalents',
                            'change_in_exchange_rate', 'net_income', 'created_at']
                )

                # Upsert from temp to main
                await conn.execute('''
                    INSERT INTO us_cash_flow SELECT * FROM temp_cash_flow
                    ON CONFLICT (symbol, fiscal_date_ending) DO UPDATE SET
                        reported_currency = EXCLUDED.reported_currency,
                        operating_cashflow = EXCLUDED.operating_cashflow,
                        payments_for_operating_activities = EXCLUDED.payments_for_operating_activities,
                        proceeds_from_operating_activities = EXCLUDED.proceeds_from_operating_activities,
                        change_in_operating_liabilities = EXCLUDED.change_in_operating_liabilities,
                        change_in_operating_assets = EXCLUDED.change_in_operating_assets,
                        depreciation_depletion_and_amortization = EXCLUDED.depreciation_depletion_and_amortization,
                        capital_expenditures = EXCLUDED.capital_expenditures,
                        change_in_receivables = EXCLUDED.change_in_receivables,
                        change_in_inventory = EXCLUDED.change_in_inventory,
                        profit_loss = EXCLUDED.profit_loss,
                        cashflow_from_investment = EXCLUDED.cashflow_from_investment,
                        cashflow_from_financing = EXCLUDED.cashflow_from_financing,
                        proceeds_from_repayments_of_short_term_debt = EXCLUDED.proceeds_from_repayments_of_short_term_debt,
                        payments_for_repurchase_of_common_stock = EXCLUDED.payments_for_repurchase_of_common_stock,
                        payments_for_repurchase_of_equity = EXCLUDED.payments_for_repurchase_of_equity,
                        payments_for_repurchase_of_preferred_stock = EXCLUDED.payments_for_repurchase_of_preferred_stock,
                        dividend_payout = EXCLUDED.dividend_payout,
                        dividend_payout_common_stock = EXCLUDED.dividend_payout_common_stock,
                        dividend_payout_preferred_stock = EXCLUDED.dividend_payout_preferred_stock,
                        proceeds_from_issuance_of_common_stock = EXCLUDED.proceeds_from_issuance_of_common_stock,
                        proceeds_from_issuance_of_long_term_debt_and_capital_securities = EXCLUDED.proceeds_from_issuance_of_long_term_debt_and_capital_securities,
                        proceeds_from_issuance_of_preferred_stock = EXCLUDED.proceeds_from_issuance_of_preferred_stock,
                        proceeds_from_repurchase_of_equity = EXCLUDED.proceeds_from_repurchase_of_equity,
                        proceeds_from_sale_of_treasury_stock = EXCLUDED.proceeds_from_sale_of_treasury_stock,
                        change_in_cash_and_cash_equivalents = EXCLUDED.change_in_cash_and_cash_equivalents,
                        change_in_exchange_rate = EXCLUDED.change_in_exchange_rate,
                        net_income = EXCLUDED.net_income,
                        created_at = EXCLUDED.created_at
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(cash_flow_data)
        except Exception as e:
            logger.error(f"[CASH_FLOW] Error in save_cash_flow_data_optimized: {e}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            logger.info("[CASH_FLOW] Falling back to regular save")
            return await self.save_cash_flow_data(cash_flow_data)

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern - OPTIMIZED"""
        for i, symbol in enumerate(symbols, 1):
            try:
                if self.collection_logger.is_collected('us_cash_flow', symbol, []):
                    continue

                api_data = await self.get_cash_flow_data(symbol)
                if api_data:
                    transformed = self.transform_cash_flow_data(api_data, symbol)
                    if transformed:
                        await data_queue.put((symbol, transformed))
                        logger.info(f"[CASH_FLOW API] [{i}/{len(symbols)}] Queued {symbol}")

                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[CASH_FLOW API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 50):
        """DB worker for pipeline pattern - OPTIMIZED"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_cash_flow_data_optimized(batch)
                    total_saved += saved
                break
            symbol, data = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_cash_flow_data_optimized(batch)
                if saved > 0:
                    total_saved += saved
                    for record in batch:
                        self.collection_logger.mark_collected('us_cash_flow', record['symbol'], [], 1)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[CASH_FLOW DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern and all 9 optimizations"""
        logger.info(f"[CASH_FLOW OPTIMIZED] Starting collection")
        logger.info(f"[CASH_FLOW OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")

        await self.init_pool()

        all_symbols = await self.get_existing_symbols()
        if not all_symbols:
            logger.error("[CASH_FLOW] No symbols found")
            await self.close_pool()
            return

        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_cash_flow', s, [])]
        logger.info(f"[CASH_FLOW] Total: {len(all_symbols)}, To process: {len(symbols)}")

        if not symbols:
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=50))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[CASH_FLOW OPTIMIZED] Completed in {duration}, saved {total_saved} records")

    async def collect_cash_flow(self):
        
        symbols = await self.get_symbols_needing_update()
        
        if not symbols:
            return
        
        total_processed = 0
        total_saved = 0
        
        for i, symbol in enumerate(symbols, 1):
            try:
                
                api_data = self.get_cash_flow_data(symbol)
                
                if api_data:
                    transformed_data = self.transform_cash_flow_data(api_data, symbol)
                    
                    if transformed_data:
                        saved_count = await self.save_cash_flow_data(transformed_data)
                        total_saved += saved_count
                    else:
                        logger.warning(f"No Cash Flow data to save for {symbol}")
                else:
                    logger.warning(f"No Cash Flow data retrieved for {symbol}")

                total_processed += 1

                if i < len(symbols):
                    await asyncio.sleep(0.2)

            except Exception as e:
                logger.error(f"Error processing symbol {symbol}: {str(e)}")
                continue
        
    
    async def get_collection_status(self):
        try:
            conn = await self.get_connection()
            
            total_records_row = await conn.fetchrow('SELECT COUNT(*) FROM us_cash_flow')
            total_records = total_records_row[0]
            
            symbols_row = await conn.fetchrow('SELECT COUNT(DISTINCT symbol) FROM us_cash_flow')
            symbols_count = symbols_row[0]
            
            latest_row = await conn.fetchrow('''
                SELECT fiscal_date_ending, symbol FROM us_cash_flow 
                ORDER BY fiscal_date_ending DESC LIMIT 1
            ''')
            
            earliest_row = await conn.fetchrow('''
                SELECT fiscal_date_ending, symbol FROM us_cash_flow 
                ORDER BY fiscal_date_ending ASC LIMIT 1
            ''')
            
            await conn.close()
            
            if latest_row:
                pass
            if earliest_row:
                pass
            
            return total_records
            
        except Exception as e:
            logger.error(f"Failed to get collection status: {str(e)}")
            return 0

class EarningsEstimatesCollector:
    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2, target_date: date = None):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1
        self.call_interval = call_interval
        self.start_date = date(2021, 1, 1)

        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_earnings_estimates_collected.json'
        else:
            log_file_path = 'log/us_earnings_estimates_collected.json'

        self.collection_logger = CollectionLogger(log_file_path)
        self.pool = None
        self.session = None

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
        logger.info("[EARNINGS_EST] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[EARNINGS_EST] Database connection pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_existing_symbols(self) -> List[str]:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[EARNINGS_EST] Found {len(symbols)} symbols")
            return symbols
        except Exception as e:
            logger.error(f"[EARNINGS_EST] Failed to get existing symbols: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return []
    
    async def get_symbols_with_recent_data(self) -> set:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('''
                SELECT DISTINCT symbol 
                FROM us_earnings_estimates 
                WHERE created_at >= NOW() - INTERVAL '7 days'
            ''')
            await conn.close()
            recent_symbols = {row['symbol'] for row in rows}
            return recent_symbols
        except Exception as e:
            logger.warning(f"Failed to get symbols with recent data: {str(e)}")
            return set()
    
    async def get_symbols_needing_update(self) -> set:
        try:
            all_symbols = await self.get_existing_symbols()
            recent_symbols = await self.get_symbols_with_recent_data()
            symbols_needing_update = all_symbols - recent_symbols
            
            
            return symbols_needing_update
            
        except Exception as e:
            logger.error(f"Failed to determine symbols needing update: {str(e)}")
            return await self.get_existing_symbols()
    
    async def get_earnings_estimates_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch Earnings Estimates data with async aiohttp - OPTIMIZED"""
        try:
            params = {
                'function': 'EARNINGS_ESTIMATES',
                'symbol': symbol,
                'apikey': self.api_key
            }
            async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'Error Message' in data:
                        logger.error(f"[EARNINGS_EST] API error for {symbol}: {data['Error Message']}")
                        return None
                    elif 'Note' in data:
                        logger.warning(f"[EARNINGS_EST] API limit reached: {data['Note']}")
                        return None
                    elif 'annualEstimates' in data or 'quarterlyEstimates' in data:
                        return data
                    else:
                        logger.warning(f"[EARNINGS_EST] Unexpected response format for {symbol}: {list(data.keys())}")
                        return None
                else:
                    logger.error(f"[EARNINGS_EST] API request failed for {symbol}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"[EARNINGS_EST] Error fetching data for {symbol}: {e}")
            return None
    
    def transform_earnings_estimates_data(self, api_data: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
        try:
            transformed_records = []

            # Handle both annualEstimates and quarterlyEstimates
            annual_estimates = api_data.get("annualEstimates", [])
            quarterly_estimates = api_data.get("quarterlyEstimates", [])
            estimates_data = annual_estimates + quarterly_estimates

            start_date = date(2021, 1, 1)

            for estimate in estimates_data:
                try:
                    estimate_date = estimate.get("date", "")
                    if not estimate_date:
                        continue

                    try:
                        parsed_date = datetime.strptime(estimate_date, "%Y-%m-%d").date()
                    except ValueError:
                        try:
                            parsed_date = datetime.strptime(estimate_date, "%Y-%m").date()
                        except ValueError:
                            logger.warning(f"Could not parse date: {estimate_date}")
                            continue

                    if parsed_date < start_date:
                        continue

                    transformed = {
                        'symbol': symbol,
                        'estimate_date': parsed_date,
                        'horizon': estimate.get("horizon", "")[:50],
                        'eps_estimate_average': self.safe_decimal(estimate.get("eps_estimate_average")),
                        'eps_estimate_high': self.safe_decimal(estimate.get("eps_estimate_high")),
                        'eps_estimate_low': self.safe_decimal(estimate.get("eps_estimate_low")),
                        'eps_estimate_analyst_count': self.safe_int(estimate.get("eps_estimate_analyst_count")),
                        'eps_estimate_average_7_days_ago': self.safe_decimal(estimate.get("eps_estimate_average_7_days_ago")),
                        'eps_estimate_average_30_days_ago': self.safe_decimal(estimate.get("eps_estimate_average_30_days_ago")),
                        'eps_estimate_average_60_days_ago': self.safe_decimal(estimate.get("eps_estimate_average_60_days_ago")),
                        'eps_estimate_average_90_days_ago': self.safe_decimal(estimate.get("eps_estimate_average_90_days_ago")),
                        'eps_estimate_revision_up_trailing_7_days': self.safe_int(estimate.get("eps_estimate_revision_up_trailing_7_days")),
                        'eps_estimate_revision_down_trailing_7_days': self.safe_int(estimate.get("eps_estimate_revision_down_trailing_7_days")),
                        'eps_estimate_revision_up_trailing_30_days': self.safe_int(estimate.get("eps_estimate_revision_up_trailing_30_days")),
                        'eps_estimate_revision_down_trailing_30_days': self.safe_int(estimate.get("eps_estimate_revision_down_trailing_30_days")),
                        'revenue_estimate_average': self.safe_decimal(estimate.get("revenue_estimate_average"), scale=2),
                        'revenue_estimate_high': self.safe_decimal(estimate.get("revenue_estimate_high"), scale=2),
                        'revenue_estimate_low': self.safe_decimal(estimate.get("revenue_estimate_low"), scale=2),
                        'revenue_estimate_analyst_count': self.safe_int(estimate.get("revenue_estimate_analyst_count")),
                        'created_at': datetime.now()
                    }
                    
                    transformed_records.append(transformed)
                    
                except Exception as e:
                    logger.error(f"Error transforming record for {symbol}: {str(e)}, Date: {estimate_date}")
                    continue
            
            if transformed_records:
                dates = [record['estimate_date'] for record in transformed_records]
                earliest = min(dates)
                latest = max(dates)
            
            return transformed_records
            
        except Exception as e:
            logger.error(f"Error transforming Earnings Estimates data for {symbol}: {str(e)}")
            return []
    
    def safe_decimal(self, value: str, scale: int = 4) -> Optional[Decimal]:
        if not value or value.strip() in ['', 'N/A', '-', 'null', 'None']:
            return None
        
        try:
            # Remove any non-numeric characters except decimal point and minus sign
            cleaned_value = ''.join(c for c in str(value).strip() if c.isdigit() or c in '.-')
            if not cleaned_value or cleaned_value in ['-', '.', '-.']:
                return None
            return Decimal(cleaned_value)
        except (InvalidOperation, ValueError, TypeError):
            logger.warning(f"Could not convert to decimal: {value}")
            return None
    
    def safe_int(self, value: str) -> Optional[int]:
        if not value or value.strip() in ['', 'N/A', '-', 'null', 'None']:
            return None
        
        try:
            # Remove any non-numeric characters except minus sign
            cleaned_value = ''.join(c for c in str(value).strip() if c.isdigit() or c == '-')
            if not cleaned_value or cleaned_value == '-':
                return None
            return int(cleaned_value)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert to int: {value}")
            return None
    
    async def save_earnings_estimates_data(self, earnings_estimates_data: List[Dict[str, Any]]) -> int:
        if not earnings_estimates_data:
            return 0
        
        try:
            conn = await self.get_connection()
            saved_count = 0
            
            for record in earnings_estimates_data:
                try:
                    await conn.execute('''
                        INSERT INTO us_earnings_estimates 
                        (symbol, estimate_date, horizon, eps_estimate_average, eps_estimate_high, eps_estimate_low,
                         eps_estimate_analyst_count, eps_estimate_average_7_days_ago, eps_estimate_average_30_days_ago,
                         eps_estimate_average_60_days_ago, eps_estimate_average_90_days_ago, 
                         eps_estimate_revision_up_trailing_7_days, eps_estimate_revision_down_trailing_7_days,
                         eps_estimate_revision_up_trailing_30_days, eps_estimate_revision_down_trailing_30_days,
                         revenue_estimate_average, revenue_estimate_high, revenue_estimate_low,
                         revenue_estimate_analyst_count, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                        ON CONFLICT (symbol, estimate_date, horizon) DO UPDATE SET
                        eps_estimate_average = EXCLUDED.eps_estimate_average,
                        eps_estimate_high = EXCLUDED.eps_estimate_high,
                        eps_estimate_low = EXCLUDED.eps_estimate_low,
                        eps_estimate_analyst_count = EXCLUDED.eps_estimate_analyst_count,
                        eps_estimate_average_7_days_ago = EXCLUDED.eps_estimate_average_7_days_ago,
                        eps_estimate_average_30_days_ago = EXCLUDED.eps_estimate_average_30_days_ago,
                        eps_estimate_average_60_days_ago = EXCLUDED.eps_estimate_average_60_days_ago,
                        eps_estimate_average_90_days_ago = EXCLUDED.eps_estimate_average_90_days_ago,
                        eps_estimate_revision_up_trailing_7_days = EXCLUDED.eps_estimate_revision_up_trailing_7_days,
                        eps_estimate_revision_down_trailing_7_days = EXCLUDED.eps_estimate_revision_down_trailing_7_days,
                        eps_estimate_revision_up_trailing_30_days = EXCLUDED.eps_estimate_revision_up_trailing_30_days,
                        eps_estimate_revision_down_trailing_30_days = EXCLUDED.eps_estimate_revision_down_trailing_30_days,
                        revenue_estimate_average = EXCLUDED.revenue_estimate_average,
                        revenue_estimate_high = EXCLUDED.revenue_estimate_high,
                        revenue_estimate_low = EXCLUDED.revenue_estimate_low,
                        revenue_estimate_analyst_count = EXCLUDED.revenue_estimate_analyst_count,
                        created_at = EXCLUDED.created_at
                    ''', 
                    record['symbol'],
                    record['estimate_date'],
                    record['horizon'],
                    record['eps_estimate_average'],
                    record['eps_estimate_high'],
                    record['eps_estimate_low'],
                    record['eps_estimate_analyst_count'],
                    record['eps_estimate_average_7_days_ago'],
                    record['eps_estimate_average_30_days_ago'],
                    record['eps_estimate_average_60_days_ago'],
                    record['eps_estimate_average_90_days_ago'],
                    record['eps_estimate_revision_up_trailing_7_days'],
                    record['eps_estimate_revision_down_trailing_7_days'],
                    record['eps_estimate_revision_up_trailing_30_days'],
                    record['eps_estimate_revision_down_trailing_30_days'],
                    record['revenue_estimate_average'],
                    record['revenue_estimate_high'],
                    record['revenue_estimate_low'],
                    record['revenue_estimate_analyst_count'],
                    record['created_at']
                    )
                    
                    saved_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to save Earnings Estimates record for {record.get('symbol', 'unknown')}: {str(e)}")

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return saved_count

        except Exception as e:
            logger.error(f"[EARNINGS_EST] Database error while saving Earnings Estimates data: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return 0

    async def save_earnings_estimates_data_optimized(self, earnings_estimates_data: List[Dict[str, Any]]) -> int:
        """Save Earnings Estimates data using COPY command - OPTIMIZED"""
        if not earnings_estimates_data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                await conn.execute('''
                    CREATE TEMP TABLE temp_earnings_estimates (
                        symbol VARCHAR(20),
                        estimate_date DATE,
                        horizon VARCHAR(50),
                        eps_estimate_average NUMERIC(18,4),
                        eps_estimate_high NUMERIC(18,4),
                        eps_estimate_low NUMERIC(18,4),
                        eps_estimate_analyst_count INTEGER,
                        eps_estimate_average_7_days_ago NUMERIC(18,4),
                        eps_estimate_average_30_days_ago NUMERIC(18,4),
                        eps_estimate_average_60_days_ago NUMERIC(18,4),
                        eps_estimate_average_90_days_ago NUMERIC(18,4),
                        eps_estimate_revision_up_trailing_7_days INTEGER,
                        eps_estimate_revision_down_trailing_7_days INTEGER,
                        eps_estimate_revision_up_trailing_30_days INTEGER,
                        eps_estimate_revision_down_trailing_30_days INTEGER,
                        revenue_estimate_average NUMERIC(18,2),
                        revenue_estimate_high NUMERIC(18,2),
                        revenue_estimate_low NUMERIC(18,2),
                        revenue_estimate_analyst_count INTEGER,
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')

                rows = [[
                    record['symbol'], record['estimate_date'], record['horizon'],
                    record['eps_estimate_average'], record['eps_estimate_high'], record['eps_estimate_low'],
                    record['eps_estimate_analyst_count'], record['eps_estimate_average_7_days_ago'],
                    record['eps_estimate_average_30_days_ago'], record['eps_estimate_average_60_days_ago'],
                    record['eps_estimate_average_90_days_ago'], record['eps_estimate_revision_up_trailing_7_days'],
                    record['eps_estimate_revision_down_trailing_7_days'], record['eps_estimate_revision_up_trailing_30_days'],
                    record['eps_estimate_revision_down_trailing_30_days'], record['revenue_estimate_average'],
                    record['revenue_estimate_high'], record['revenue_estimate_low'],
                    record['revenue_estimate_analyst_count'], record['created_at']
                ] for record in earnings_estimates_data]

                await conn.copy_records_to_table(
                    'temp_earnings_estimates', records=rows,
                    columns=['symbol', 'estimate_date', 'horizon', 'eps_estimate_average', 'eps_estimate_high',
                            'eps_estimate_low', 'eps_estimate_analyst_count', 'eps_estimate_average_7_days_ago',
                            'eps_estimate_average_30_days_ago', 'eps_estimate_average_60_days_ago',
                            'eps_estimate_average_90_days_ago', 'eps_estimate_revision_up_trailing_7_days',
                            'eps_estimate_revision_down_trailing_7_days', 'eps_estimate_revision_up_trailing_30_days',
                            'eps_estimate_revision_down_trailing_30_days', 'revenue_estimate_average',
                            'revenue_estimate_high', 'revenue_estimate_low', 'revenue_estimate_analyst_count',
                            'created_at']
                )

                await conn.execute('''
                    INSERT INTO us_earnings_estimates SELECT * FROM temp_earnings_estimates
                    ON CONFLICT (symbol, estimate_date, horizon) DO UPDATE SET
                        eps_estimate_average = EXCLUDED.eps_estimate_average,
                        eps_estimate_high = EXCLUDED.eps_estimate_high,
                        eps_estimate_low = EXCLUDED.eps_estimate_low,
                        eps_estimate_analyst_count = EXCLUDED.eps_estimate_analyst_count,
                        eps_estimate_average_7_days_ago = EXCLUDED.eps_estimate_average_7_days_ago,
                        eps_estimate_average_30_days_ago = EXCLUDED.eps_estimate_average_30_days_ago,
                        eps_estimate_average_60_days_ago = EXCLUDED.eps_estimate_average_60_days_ago,
                        eps_estimate_average_90_days_ago = EXCLUDED.eps_estimate_average_90_days_ago,
                        eps_estimate_revision_up_trailing_7_days = EXCLUDED.eps_estimate_revision_up_trailing_7_days,
                        eps_estimate_revision_down_trailing_7_days = EXCLUDED.eps_estimate_revision_down_trailing_7_days,
                        eps_estimate_revision_up_trailing_30_days = EXCLUDED.eps_estimate_revision_up_trailing_30_days,
                        eps_estimate_revision_down_trailing_30_days = EXCLUDED.eps_estimate_revision_down_trailing_30_days,
                        revenue_estimate_average = EXCLUDED.revenue_estimate_average,
                        revenue_estimate_high = EXCLUDED.revenue_estimate_high,
                        revenue_estimate_low = EXCLUDED.revenue_estimate_low,
                        revenue_estimate_analyst_count = EXCLUDED.revenue_estimate_analyst_count,
                        created_at = EXCLUDED.created_at
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(earnings_estimates_data)
        except Exception as e:
            logger.error(f"[EARNINGS_EST] Error in save_earnings_estimates_data_optimized: {e}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            logger.info("[EARNINGS_EST] Falling back to regular save")
            return await self.save_earnings_estimates_data(earnings_estimates_data)

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern - OPTIMIZED"""
        for i, symbol in enumerate(symbols, 1):
            try:
                if self.collection_logger.is_collected('us_earnings_estimates', symbol, []):
                    continue

                api_data = await self.get_earnings_estimates_data(symbol)
                if api_data:
                    transformed = self.transform_earnings_estimates_data(api_data, symbol)
                    if transformed:
                        await data_queue.put((symbol, transformed))
                        logger.info(f"[EARNINGS_EST API] [{i}/{len(symbols)}] Queued {symbol}")

                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[EARNINGS_EST API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 50):
        """DB worker for pipeline pattern - OPTIMIZED"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_earnings_estimates_data_optimized(batch)
                    total_saved += saved
                break
            symbol, data = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_earnings_estimates_data_optimized(batch)
                if saved > 0:
                    total_saved += saved
                    for record in batch:
                        self.collection_logger.mark_collected('us_earnings_estimates', record['symbol'], [], 1)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[EARNINGS_EST DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern and all 9 optimizations"""
        logger.info(f"[EARNINGS_EST OPTIMIZED] Starting collection")
        logger.info(f"[EARNINGS_EST OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")

        await self.init_pool()

        all_symbols = await self.get_existing_symbols()
        if not all_symbols:
            logger.error("[EARNINGS_EST] No symbols found")
            await self.close_pool()
            return

        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_earnings_estimates', s, [])]
        logger.info(f"[EARNINGS_EST] Total: {len(all_symbols)}, To process: {len(symbols)}")

        if not symbols:
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=50))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[EARNINGS_EST OPTIMIZED] Completed in {duration}, saved {total_saved} records")

    async def collect_earnings_estimates(self):

        symbols = await self.get_symbols_needing_update()

        if not symbols:
            return
        
        total_processed = 0
        total_saved = 0
        
        for i, symbol in enumerate(symbols, 1):
            try:
                
                api_data = self.get_earnings_estimates_data(symbol)
                
                if api_data:
                    transformed_data = self.transform_earnings_estimates_data(api_data, symbol)
                    
                    if transformed_data:
                        saved_count = await self.save_earnings_estimates_data(transformed_data)
                        total_saved += saved_count
                    else:
                        logger.warning(f"No Earnings Estimates data to save for {symbol}")
                else:
                    logger.warning(f"No Earnings Estimates data retrieved for {symbol}")

                total_processed += 1

                if i < len(symbols):
                    await asyncio.sleep(0.2)

            except Exception as e:
                logger.error(f"Error processing symbol {symbol}: {str(e)}")
                continue
        
    
    async def get_collection_status(self):
        try:
            conn = await self.get_connection()
            
            total_records_row = await conn.fetchrow('SELECT COUNT(*) FROM us_earnings_estimates')
            total_records = total_records_row[0]
            
            symbols_row = await conn.fetchrow('SELECT COUNT(DISTINCT symbol) FROM us_earnings_estimates')
            symbols_count = symbols_row[0]
            
            latest_row = await conn.fetchrow('''
                SELECT estimate_date, symbol, horizon FROM us_earnings_estimates 
                ORDER BY estimate_date DESC LIMIT 1
            ''')
            
            earliest_row = await conn.fetchrow('''
                SELECT estimate_date, symbol, horizon FROM us_earnings_estimates 
                ORDER BY estimate_date ASC LIMIT 1
            ''')
            
            await conn.close()
            
            if latest_row:
                pass
            if earliest_row:
                pass
            
            return total_records
            
        except Exception as e:
            logger.error(f"Failed to get collection status: {str(e)}")
            return 0

class EarningsCalendarCollector:
    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2, horizon: str = '3month'):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1
        self.call_interval = call_interval
        self.horizon = horizon
        self.start_date = date(2024, 1, 1)

        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_earnings_calendar_collected.json'
        else:
            log_file_path = 'log/us_earnings_calendar_collected.json'

        self.collection_logger = CollectionLogger(log_file_path)
        self.pool = None
        self.session = None

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
        logger.info("[EARNINGS_CAL] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[EARNINGS_CAL] Database connection pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_existing_symbols(self) -> List[str]:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[EARNINGS_CAL] Found {len(symbols)} symbols")
            return symbols
        except Exception as e:
            logger.error(f"[EARNINGS_CAL] Failed to get existing symbols: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return []

    async def get_symbols_with_recent_data(self) -> set:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('''
                SELECT DISTINCT symbol 
                FROM us_earnings_calendar 
                WHERE created_at >= NOW() - INTERVAL '7 days'
            ''')
            await conn.close()
            recent_symbols = {row['symbol'] for row in rows}
            return recent_symbols
        except Exception as e:
            logger.warning(f"Failed to get symbols with recent data: {str(e)}")
            return set()
    
    async def get_symbols_needing_update(self) -> set:
        try:
            all_symbols = await self.get_existing_symbols()
            recent_symbols = await self.get_symbols_with_recent_data()
            symbols_needing_update = all_symbols - recent_symbols
            
            
            return symbols_needing_update
            
        except Exception as e:
            logger.error(f"Failed to determine symbols needing update: {str(e)}")
            return await self.get_existing_symbols()
    
    async def get_earnings_calendar_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch Earnings Calendar data with async aiohttp - OPTIMIZED"""
        try:
            params = {
                'function': 'EARNINGS_CALENDAR',
                'symbol': symbol,
                'horizon': self.horizon,
                'apikey': self.api_key
            }
            async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    csv_data = await response.text()
                    if 'Error Message' in csv_data:
                        logger.error(f"[EARNINGS_CAL] API error for {symbol}: {csv_data}")
                        return None
                    elif 'Note' in csv_data or 'rate limit' in csv_data.lower():
                        logger.warning(f"[EARNINGS_CAL] API limit reached: {csv_data[:100]}")
                        return None

                    # Parse CSV data
                    parsed_data = self.parse_csv_data(csv_data)
                    if parsed_data:
                        return {"data": parsed_data}
                    else:
                        logger.warning(f"[EARNINGS_CAL] No valid data for {symbol}")
                        return None
                else:
                    logger.error(f"[EARNINGS_CAL] API request failed for {symbol}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"[EARNINGS_CAL] Error fetching data for {symbol}: {e}")
            return None

    
    def parse_csv_data(self, csv_data: str) -> List[Dict[str, Any]]:
        try:
            lines = csv_data.strip().split('\n')
            if len(lines) < 2:
                return []
            
            #  
            headers = [h.strip() for h in lines[0].split(',')]
            
            records = []
            for line in lines[1:]:
                if not line.strip():
                    continue
                
                values = [v.strip() for v in line.split(',')]
                if len(values) != len(headers):
                    continue
                
                record = dict(zip(headers, values))
                records.append(record)
            
            return records
            
        except Exception as e:
            logger.error(f"Error parsing CSV data: {str(e)}")
            return []
    
    def transform_earnings_calendar_data(self, api_data: Dict[str, Any], symbol_filter: str = None) -> List[Dict[str, Any]]:
        try:
            transformed_records = []
            
            calendar_data = api_data.get("data", [])
            
            for record in calendar_data:
                try:
                    symbol = record.get("symbol", "")
                    if not symbol:
                        continue
                    
                    #    
                    if symbol_filter and symbol != symbol_filter:
                        continue
                    
                    #   
                    report_date_str = record.get("reportDate", "")
                    fiscal_date_ending_str = record.get("fiscalDateEnding", "")
                    
                    report_date = None
                    fiscal_date_ending = None
                    
                    if report_date_str:
                        try:
                            report_date = datetime.strptime(report_date_str, "%Y-%m-%d").date()
                        except ValueError:
                            logger.warning(f"Could not parse reportDate: {report_date_str}")
                    
                    if fiscal_date_ending_str:
                        try:
                            fiscal_date_ending = datetime.strptime(fiscal_date_ending_str, "%Y-%m-%d").date()
                        except ValueError:
                            logger.warning(f"Could not parse fiscalDateEnding: {fiscal_date_ending_str}")
                    
                    #     
                    if not report_date and not fiscal_date_ending:
                        continue
                    
                    transformed = {
                        'symbol': symbol[:10],  # VARCHAR(10) 
                        'name': record.get("name", record.get("companyName", ""))[:100],  # VARCHAR(100) 
                        'reportdate': report_date,
                        'fiscaldateending': fiscal_date_ending,
                        'estimate': self.safe_decimal(record.get("estimate", record.get("epsEstimate", "")), precision=18, scale=4),
                        'currency': record.get("currency", "")[:10],  # VARCHAR(10) 
                        'created_at': datetime.now()
                    }
                    
                    transformed_records.append(transformed)
                    
                except Exception as e:
                    logger.error(f"Error transforming record: {str(e)}, Record: {record}")
                    continue
            
            if transformed_records:
                symbols = list(set([record['symbol'] for record in transformed_records]))
            
            return transformed_records
            
        except Exception as e:
            logger.error(f"Error transforming Earnings Calendar data: {str(e)}")
            return []
    
    def safe_decimal(self, value: str, precision: int = 18, scale: int = 4) -> Optional[Decimal]:
        if not value or value.strip() in ['', 'N/A', '-', 'null', 'None']:
            return None
        
        try:
            # Remove any non-numeric characters except decimal point and minus sign
            cleaned_value = ''.join(c for c in str(value).strip() if c.isdigit() or c in '.-')
            if not cleaned_value or cleaned_value in ['-', '.', '-.']:
                return None
            return Decimal(cleaned_value)
        except (InvalidOperation, ValueError, TypeError):
            logger.warning(f"Could not convert to decimal: {value}")
            return None
    
    async def save_earnings_calendar_data(self, earnings_calendar_data: List[Dict[str, Any]]) -> int:
        if not earnings_calendar_data:
            return 0

        try:
            conn = await self.get_connection()
            saved_count = 0

            for record in earnings_calendar_data:
                try:
                    await conn.execute('''
                        INSERT INTO us_earnings_calendar
                        (symbol, name, reportdate, fiscaldateending, estimate, currency, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (symbol, reportdate) DO UPDATE SET
                            name = EXCLUDED.name,
                            fiscaldateending = EXCLUDED.fiscaldateending,
                            estimate = EXCLUDED.estimate,
                            currency = EXCLUDED.currency,
                            created_at = EXCLUDED.created_at
                    ''',
                    record['symbol'],
                    record['name'],
                    record['reportdate'],
                    record['fiscaldateending'],
                    record['estimate'],
                    record['currency'],
                    record['created_at']
                    )

                    saved_count += 1

                except Exception as e:
                    logger.error(f"[EARNINGS_CAL] Failed to save record for {record.get('symbol', 'unknown')}: {str(e)}")
                    continue

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return saved_count

        except Exception as e:
            logger.error(f"[EARNINGS_CAL] Database error while saving data: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return 0

    async def save_earnings_calendar_data_optimized(self, earnings_calendar_data: List[Dict[str, Any]]) -> int:
        """Save Earnings Calendar data using COPY command - OPTIMIZED"""
        if not earnings_calendar_data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                # Transaction optimization
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                # Create temp table
                await conn.execute('''
                    CREATE TEMP TABLE temp_earnings_calendar (
                        symbol VARCHAR(10),
                        name VARCHAR(100),
                        reportdate DATE,
                        fiscaldateending DATE,
                        estimate NUMERIC(18,4),
                        currency VARCHAR(10),
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')

                # Prepare data for COPY
                rows = [[
                    record['symbol'], record['name'], record['reportdate'],
                    record['fiscaldateending'], record['estimate'], record['currency'],
                    record['created_at']
                ] for record in earnings_calendar_data]

                # COPY to temp table
                await conn.copy_records_to_table(
                    'temp_earnings_calendar', records=rows,
                    columns=['symbol', 'name', 'reportdate', 'fiscaldateending',
                            'estimate', 'currency', 'created_at']
                )

                # Upsert from temp to main
                await conn.execute('''
                    INSERT INTO us_earnings_calendar SELECT * FROM temp_earnings_calendar
                    ON CONFLICT (symbol, reportdate) DO UPDATE SET
                        name = EXCLUDED.name,
                        fiscaldateending = EXCLUDED.fiscaldateending,
                        estimate = EXCLUDED.estimate,
                        currency = EXCLUDED.currency,
                        created_at = EXCLUDED.created_at
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(earnings_calendar_data)
        except Exception as e:
            logger.error(f"[EARNINGS_CAL] Error in save_earnings_calendar_data_optimized: {e}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            logger.info("[EARNINGS_CAL] Falling back to regular save")
            return await self.save_earnings_calendar_data(earnings_calendar_data)

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern - OPTIMIZED"""
        for i, symbol in enumerate(symbols, 1):
            try:
                if self.collection_logger.is_collected('us_earnings_calendar', symbol, []):
                    continue

                api_data = await self.get_earnings_calendar_data(symbol)
                if api_data:
                    transformed = self.transform_earnings_calendar_data(api_data, symbol)
                    if transformed:
                        await data_queue.put((symbol, transformed))
                        logger.info(f"[EARNINGS_CAL API] [{i}/{len(symbols)}] Queued {symbol}")

                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[EARNINGS_CAL API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 50):
        """DB worker for pipeline pattern - OPTIMIZED"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_earnings_calendar_data_optimized(batch)
                    total_saved += saved
                break
            symbol, data = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_earnings_calendar_data_optimized(batch)
                if saved > 0:
                    total_saved += saved
                    for record in batch:
                        self.collection_logger.mark_collected('us_earnings_calendar', record['symbol'], [], 1)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[EARNINGS_CAL DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern and all 9 optimizations"""
        logger.info(f"[EARNINGS_CAL OPTIMIZED] Starting collection")
        logger.info(f"[EARNINGS_CAL OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")

        await self.init_pool()

        all_symbols = await self.get_existing_symbols()
        if not all_symbols:
            logger.error("[EARNINGS_CAL] No symbols found")
            await self.close_pool()
            return

        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_earnings_calendar', s, [])]
        logger.info(f"[EARNINGS_CAL] Total: {len(all_symbols)}, To process: {len(symbols)}")

        if not symbols:
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=50))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[EARNINGS_CAL OPTIMIZED] Completed in {duration}, saved {total_saved} records")

    async def collect_earnings_calendar(self):

        # API     
        try:
            test_url = f"{self.base_url}?function=TIME_SERIES_DAILY&symbol=AAPL&outputsize=compact&apikey={self.api_key}"
            test_response = requests.get(test_url, timeout=10)
            
            if "rate limit" in test_response.text.lower():
                logger.error("API rate limit detected. Skipping Earnings Calendar collection for now.")
                return
            
            #      
            api_data = self.get_earnings_calendar_data()
            
            if api_data:
                transformed_data = self.transform_earnings_calendar_data(api_data)
                
                if transformed_data:
                    saved_count = await self.save_earnings_calendar_data(transformed_data)
                else:
                    logger.warning("No Earnings Calendar data to save")
            else:
                logger.warning("No Earnings Calendar data retrieved")
            
        except Exception as e:
            logger.error(f"Error during Earnings Calendar collection: {str(e)}")
        
    
    async def get_collection_status(self):
        try:
            conn = await self.get_connection()
            
            total_records_row = await conn.fetchrow('SELECT COUNT(*) FROM us_earnings_calendar')
            total_records = total_records_row[0]
            
            symbols_row = await conn.fetchrow('SELECT COUNT(DISTINCT symbol) FROM us_earnings_calendar')
            symbols_count = symbols_row[0]
            
            latest_row = await conn.fetchrow('''
                SELECT reportdate, fiscaldateending, symbol, name FROM us_earnings_calendar 
                ORDER BY reportdate DESC LIMIT 1
            ''')
            
            earliest_row = await conn.fetchrow('''
                SELECT reportdate, fiscaldateending, symbol, name FROM us_earnings_calendar 
                ORDER BY reportdate ASC LIMIT 1
            ''')
            
            await conn.close()
            
            if latest_row:
                pass
            if earliest_row:
                pass
            
            return total_records
            
        except Exception as e:
            logger.error(f"Failed to get collection status: {str(e)}")
            return 0


class InsiderTransactionsCollector:
    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2, target_date: date = None):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1
        self.call_interval = call_interval
        self.start_date = date(2024, 1, 1)

        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_insider_transactions_collected.json'
        else:
            log_file_path = 'log/us_insider_transactions_collected.json'

        self.collection_logger = CollectionLogger(log_file_path)
        self.pool = None
        self.session = None

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
        logger.info("[INSIDER_TRANS] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[INSIDER_TRANS] Database connection pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_existing_symbols(self) -> List[str]:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[INSIDER_TRANS] Found {len(symbols)} symbols")
            return symbols
        except Exception as e:
            logger.error(f"[INSIDER_TRANS] Failed to get existing symbols: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return []
    
    async def get_last_processed_date_per_symbol(self) -> Dict[str, date]:
        """Get last processed date for each symbol from us_insider_transactions table"""
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('''
                SELECT symbol, MAX(date) as last_date 
                FROM us_insider_transactions 
                GROUP BY symbol
            ''')
            await conn.close()
            return {row['symbol']: row['last_date'] for row in rows}
        except Exception as e:
            logger.error(f"Failed to get last processed dates: {str(e)}")
            return {}
    
    async def get_insider_transactions_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch Insider Transactions data with async aiohttp - OPTIMIZED"""
        try:
            params = {
                'function': 'INSIDER_TRANSACTIONS',
                'symbol': symbol,
                'apikey': self.api_key
            }
            async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'Error Message' in data:
                        logger.error(f"[INSIDER_TRANS] API error for {symbol}: {data['Error Message']}")
                        return None
                    elif 'Note' in data:
                        logger.warning(f"[INSIDER_TRANS] API limit reached: {data['Note']}")
                        return None
                    elif 'data' in data:
                        return data
                    else:
                        logger.warning(f"[INSIDER_TRANS] Unexpected response format for {symbol}")
                        return None
                else:
                    logger.error(f"[INSIDER_TRANS] API request failed for {symbol}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"[INSIDER_TRANS] Error fetching data for {symbol}: {e}")
            return None
    
    def transform_insider_transactions_data(self, api_data: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
        """Transform Insider Transactions data to match database schema"""
        try:
            transformed_records = []
            
            # Process data records
            data_records = api_data.get('data', [])
            
            for record in data_records:
                try:
                    # Get transaction date
                    date_str = record.get('transaction_date', '').strip()
                    
                    # Parse date
                    parsed_date = self.parse_date(date_str)
                    if not parsed_date:
                        logger.warning(f"Skipping record with invalid date: {date_str}")
                        continue
                    
                    # Filter for today only (  )
                    from datetime import date
                    today = date.today()
                    if parsed_date != today:
                        continue
                    
                    # Get other fields
                    ticker = record.get('ticker', symbol).strip()
                    executive = record.get('executive', '').strip()
                    executive_title = record.get('executive_title', '').strip()
                    security_type = record.get('security_type', '').strip()
                    acquisition_or_disposal = record.get('acquisition_or_disposal', '').strip()
                    
                    # Parse shares
                    shares_str = record.get('shares', '').strip()
                    parsed_shares = self.safe_decimal(shares_str)
                    
                    # Parse share price
                    share_price_str = record.get('share_price', '').strip()
                    parsed_share_price = self.safe_decimal(share_price_str)
                    
                    transformed = {
                        'date': parsed_date,
                        'symbol': ticker[:10] if ticker else symbol,
                        'executive': [executive] if executive else [],
                        'executive_title': [executive_title] if executive_title else [],
                        'security_type': security_type[:255] if security_type else None,
                        'acquisition_or_disposal': acquisition_or_disposal[:255] if acquisition_or_disposal else None,
                        'shares': parsed_shares,
                        'share_price': parsed_share_price,
                        'created_at': datetime.now()
                    }
                    
                    transformed_records.append(transformed)
                    
                except Exception as e:
                    logger.error(f"Error transforming record for {symbol}: {str(e)}, Record: {record}")
                    continue
            
            return transformed_records
            
        except Exception as e:
            logger.error(f"Error transforming Insider Transactions data for {symbol}: {str(e)}")
            return []
    
    def parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string to datetime object"""
        if not date_str or date_str.strip() == '':
            return None
        
        try:
            # Try different date formats
            for date_format in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                try:
                    return datetime.strptime(date_str.strip(), date_format).date()
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.error(f"Date parsing error: {str(e)}")
            return None
    
    def safe_decimal(self, value: str) -> Optional[float]:
        """Safely convert string to decimal"""
        if not value or value.strip() in ['', 'N/A', '-', 'null']:
            return None
        
        try:
            return float(value.strip())
        except ValueError:
            logger.warning(f"Could not convert to decimal: {value}")
            return None
    
    async def save_insider_transactions_data(self, insider_transactions_data: List[Dict[str, Any]]) -> int:
        """Save Insider Transactions data to database"""
        if not insider_transactions_data:
            return 0
        
        try:
            conn = await self.get_connection()
            saved_count = 0
            updated_count = 0
            
            for record in insider_transactions_data:
                try:
                    # Insert or update Insider Transactions record
                    result = await conn.execute('''
                        INSERT INTO us_insider_transactions 
                        (date, symbol, executive, executive_title, security_type, 
                         acquisition_or_disposal, shares, share_price, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (date, symbol, executive, executive_title) DO UPDATE SET
                        security_type = EXCLUDED.security_type,
                        acquisition_or_disposal = EXCLUDED.acquisition_or_disposal,
                        shares = EXCLUDED.shares,
                        share_price = EXCLUDED.share_price,
                        created_at = EXCLUDED.created_at
                        RETURNING (xmax = 0) AS inserted
                    ''', 
                    record['date'],
                    record['symbol'],
                    record['executive'],
                    record['executive_title'],
                    record['security_type'],
                    record['acquisition_or_disposal'],
                    record['shares'],
                    record['share_price'],
                    record['created_at']
                    )
                    
                    # Check if it was an insert or update
                    if result == "INSERT 0 1":
                        saved_count += 1
                    else:
                        updated_count += 1
                    
                    
                except Exception as e:
                    logger.error(f"Failed to save Insider Transaction record for {record.get('symbol', 'unknown')}: {str(e)}")

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return saved_count + updated_count

        except Exception as e:
            logger.error(f"[INSIDER_TRANS] Database error while saving Insider Transactions data: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return 0
    
    async def collect_insider_transactions(self):
        """Main method to collect Insider Transactions data for all symbols"""
        
        
        # Get all symbols from us_stock_basic table
        symbols = await self.get_existing_symbols()
        
        if not symbols:
            logger.warning("No symbols found in us_stock_basic table")
            return
        
        # UPSERT   - DELETE 
        from datetime import date
        today = date.today()

        symbols_to_process = symbols
        
        total_processed = 0
        total_saved = 0
        
        for i, symbol in enumerate(symbols_to_process, 1):
            try:
                
                # Fetch Insider Transactions data for this symbol
                api_data = self.get_insider_transactions_data(symbol)
                
                if api_data:
                    # Transform data (today's data only)
                    transformed_data = self.transform_insider_transactions_data(api_data, symbol)
                    
                    if transformed_data:
                        # Save to database
                        saved_count = await self.save_insider_transactions_data(transformed_data)
                        total_saved += saved_count
                    else:
                        logger.warning(f"No today's Insider Transactions data to save for {symbol}")
                else:
                    logger.warning(f"No Insider Transactions data retrieved for {symbol}")
                
                total_processed += 1

                # Add small delay between requests to respect API limits
                if i < len(symbols_to_process):  # Don't sleep after the last symbol
                    await asyncio.sleep(0.2)  # 300 requests per minute = ~0.2 seconds between requests

            except Exception as e:
                logger.error(f"Error processing symbol {symbol}: {str(e)}")
                continue
        
    
    async def get_collection_status(self):
        """Get current Insider Transactions collection status"""
        try:
            conn = await self.get_connection()
            
            total_records_row = await conn.fetchrow('SELECT COUNT(*) FROM us_insider_transactions')
            total_records = total_records_row[0]
            
            # Get unique symbols count
            symbols_row = await conn.fetchrow('SELECT COUNT(DISTINCT symbol) FROM us_insider_transactions')
            symbols_count = symbols_row[0]
            
            # Get latest date
            latest_row = await conn.fetchrow('''
                SELECT date, symbol FROM us_insider_transactions 
                ORDER BY date DESC LIMIT 1
            ''')
            
            # Get earliest date
            earliest_row = await conn.fetchrow('''
                SELECT date, symbol FROM us_insider_transactions 
                ORDER BY date ASC LIMIT 1
            ''')
            
            await conn.close()
            
            if latest_row:
                pass
            if earliest_row:
                pass
            
            return total_records
            
        except Exception as e:
            logger.error(f"Failed to get collection status: {str(e)}")
            return 0

    async def save_insider_transactions_data_optimized(self, insider_transactions_data: List[Dict[str, Any]]) -> int:
        """Save Insider Transactions data using COPY command - OPTIMIZED"""
        if not insider_transactions_data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                # Transaction optimization
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                # Create temp table
                await conn.execute('''
                    CREATE TEMP TABLE temp_insider_transactions (
                        date DATE,
                        symbol VARCHAR(10),
                        executive TEXT[],
                        executive_title TEXT[],
                        security_type VARCHAR(255),
                        acquisition_or_disposal VARCHAR(255),
                        shares NUMERIC,
                        share_price NUMERIC,
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')

                # Prepare data for COPY
                rows = [[
                    record['date'], record['symbol'], record['executive'],
                    record['executive_title'], record['security_type'],
                    record['acquisition_or_disposal'], record['shares'],
                    record['share_price'], record['created_at']
                ] for record in insider_transactions_data]

                # COPY to temp table
                await conn.copy_records_to_table(
                    'temp_insider_transactions', records=rows,
                    columns=['date', 'symbol', 'executive', 'executive_title',
                            'security_type', 'acquisition_or_disposal', 'shares',
                            'share_price', 'created_at']
                )

                # Upsert from temp to main
                await conn.execute('''
                    INSERT INTO us_insider_transactions SELECT * FROM temp_insider_transactions
                    ON CONFLICT (date, symbol, executive, executive_title) DO UPDATE SET
                        security_type = EXCLUDED.security_type,
                        acquisition_or_disposal = EXCLUDED.acquisition_or_disposal,
                        shares = EXCLUDED.shares,
                        share_price = EXCLUDED.share_price,
                        created_at = EXCLUDED.created_at
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(insider_transactions_data)
        except Exception as e:
            logger.error(f"[INSIDER_TRANS] Error in save_insider_transactions_data_optimized: {e}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            logger.info("[INSIDER_TRANS] Falling back to regular save")
            return await self.save_insider_transactions_data(insider_transactions_data)

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern - OPTIMIZED"""
        for i, symbol in enumerate(symbols, 1):
            try:
                if self.collection_logger.is_collected('us_insider_transactions', symbol, []):
                    continue

                api_data = await self.get_insider_transactions_data(symbol)
                if api_data:
                    transformed = self.transform_insider_transactions_data(api_data, symbol)
                    if transformed:
                        await data_queue.put((symbol, transformed))
                        logger.info(f"[INSIDER_TRANS API] [{i}/{len(symbols)}] Queued {symbol}")

                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[INSIDER_TRANS API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 50):
        """DB worker for pipeline pattern - OPTIMIZED"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_insider_transactions_data_optimized(batch)
                    total_saved += saved
                break
            symbol, data = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_insider_transactions_data_optimized(batch)
                if saved > 0:
                    total_saved += saved
                    for record in batch:
                        self.collection_logger.mark_collected('us_insider_transactions', record['symbol'], [], 1)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[INSIDER_TRANS DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern and all 9 optimizations"""
        logger.info(f"[INSIDER_TRANS OPTIMIZED] Starting collection")
        logger.info(f"[INSIDER_TRANS OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")

        await self.init_pool()

        all_symbols = await self.get_existing_symbols()
        if not all_symbols:
            logger.error("[INSIDER_TRANS] No symbols found")
            await self.close_pool()
            return

        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_insider_transactions', s, [])]
        logger.info(f"[INSIDER_TRANS] Total: {len(all_symbols)}, To process: {len(symbols)}")

        if not symbols:
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=50))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[INSIDER_TRANS OPTIMIZED] Completed in {duration}, saved {total_saved} records")



class DividendsCollector:
    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2, target_date: date = None):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1
        self.call_interval = call_interval
        self.start_date = date(2024, 1, 1)  # Dividends from 2024

        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_dividends_collected.json'
        else:
            log_file_path = 'log/us_dividends_collected.json'

        self.collection_logger = CollectionLogger(log_file_path)
        self.pool = None
        self.session = None

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
        logger.info("[DIVIDENDS] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[DIVIDENDS] Database connection pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_existing_symbols(self) -> List[str]:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[DIVIDENDS] Found {len(symbols)} symbols")
            return symbols
        except Exception as e:
            logger.error(f"[DIVIDENDS] Failed to get existing symbols: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return []
    
    async def get_symbols_with_recent_data(self) -> set:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('''
                SELECT DISTINCT symbol 
                FROM us_dividends 
                WHERE created_at >= NOW() - INTERVAL '7 days'
            ''')
            await conn.close()
            recent_symbols = {row['symbol'] for row in rows}
            return recent_symbols
        except Exception as e:
            logger.warning(f"Failed to get symbols with recent data: {str(e)}")
            return set()
    
    async def get_symbols_needing_update(self) -> set:
        try:
            all_symbols = await self.get_existing_symbols()
            recent_symbols = await self.get_symbols_with_recent_data()
            symbols_needing_update = all_symbols - recent_symbols
            
            
            return symbols_needing_update
            
        except Exception as e:
            logger.error(f"Failed to determine symbols needing update: {str(e)}")
            return await self.get_existing_symbols()
    
    async def get_dividends_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch Dividends data with async aiohttp - OPTIMIZED"""
        try:
            params = {
                'function': 'DIVIDENDS',
                'symbol': symbol,
                'apikey': self.api_key
            }
            async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'Error Message' in data:
                        logger.error(f"[DIVIDENDS] API error for {symbol}: {data['Error Message']}")
                        return None
                    elif 'Note' in data:
                        logger.warning(f"[DIVIDENDS] API limit reached: {data['Note']}")
                        return None
                    elif 'data' in data and data['data']:
                        return data
                    else:
                        logger.warning(f"[DIVIDENDS] No data found for {symbol}")
                        return None
                else:
                    logger.error(f"[DIVIDENDS] API request failed for {symbol}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"[DIVIDENDS] Error fetching data for {symbol}: {e}")
            return None
    
    def transform_dividends_data(self, api_data: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
        try:
            transformed_records = []
            
            dividends_data = api_data.get("data", [])
            
            for record in dividends_data:
                try:
                    # Parse date fields
                    ex_dividend_date_str = record.get("ex_dividend_date", "")
                    declaration_date_str = record.get("declaration_date", "")
                    record_date_str = record.get("record_date", "")
                    payment_date_str = record.get("payment_date", "")
                    
                    ex_dividend_date = None
                    declaration_date = None
                    record_date = None
                    payment_date = None
                    
                    if ex_dividend_date_str:
                        try:
                            ex_dividend_date = datetime.strptime(ex_dividend_date_str, "%Y-%m-%d").date()
                        except ValueError:
                            logger.warning(f"Could not parse ex_dividend_date: {ex_dividend_date_str}")
                    
                    if declaration_date_str and declaration_date_str not in ['None', 'null', '']:
                        try:
                            declaration_date = datetime.strptime(declaration_date_str, "%Y-%m-%d").date()
                        except ValueError:
                            logger.debug(f"Could not parse declaration_date: {declaration_date_str}")
                    
                    if record_date_str and record_date_str not in ['None', 'null', '']:
                        try:
                            record_date = datetime.strptime(record_date_str, "%Y-%m-%d").date()
                        except ValueError:
                            logger.debug(f"Could not parse record_date: {record_date_str}")
                    
                    if payment_date_str and payment_date_str not in ['None', 'null', '']:
                        try:
                            payment_date = datetime.strptime(payment_date_str, "%Y-%m-%d").date()
                        except ValueError:
                            logger.debug(f"Could not parse payment_date: {payment_date_str}")
                    
                    # At least ex_dividend_date should exist
                    if not ex_dividend_date:
                        continue
                    
                    # Filter data from 2020 onwards only
                    if ex_dividend_date.year < 2020:
                        continue
                    
                    transformed = {
                        'symbol': symbol[:10],  # VARCHAR(10) limit
                        'ex_dividend_date': ex_dividend_date,
                        'declaration_date': declaration_date,
                        'record_date': record_date,
                        'payment_date': payment_date,
                        'amount': self.safe_decimal(record.get("amount", ""), precision=18, scale=4),
                        'created_at': datetime.now()
                    }
                    
                    transformed_records.append(transformed)
                    
                except Exception as e:
                    logger.error(f"Error transforming record: {str(e)}, Record: {record}")
                    continue
            
            
            return transformed_records
            
        except Exception as e:
            logger.error(f"Error transforming Dividends data for {symbol}: {str(e)}")
            return []
    
    def safe_decimal(self, value: str, precision: int = 18, scale: int = 4) -> Optional[Decimal]:
        if not value or value.strip() in ['', 'N/A', '-', 'null', 'None']:
            return None
        
        try:
            # Remove any non-numeric characters except decimal point and minus sign
            cleaned_value = ''.join(c for c in str(value).strip() if c.isdigit() or c in '.-')
            if not cleaned_value or cleaned_value in ['-', '.', '-.']:
                return None
            return Decimal(cleaned_value)
        except (InvalidOperation, ValueError, TypeError):
            logger.warning(f"Could not convert to decimal: {value}")
            return None
    
    async def save_dividends_data(self, dividends_data: List[Dict[str, Any]]) -> int:
        if not dividends_data:
            return 0

        try:
            conn = await self.get_connection()
            saved_count = 0

            for record in dividends_data:
                try:
                    await conn.execute('''
                        INSERT INTO us_dividends
                        (symbol, ex_dividend_date, declaration_date, record_date, payment_date, amount, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (symbol, ex_dividend_date) DO UPDATE SET
                            declaration_date = EXCLUDED.declaration_date,
                            record_date = EXCLUDED.record_date,
                            payment_date = EXCLUDED.payment_date,
                            amount = EXCLUDED.amount,
                            created_at = EXCLUDED.created_at
                    ''',
                    record['symbol'],
                    record['ex_dividend_date'],
                    record['declaration_date'],
                    record['record_date'],
                    record['payment_date'],
                    record['amount'],
                    record['created_at']
                    )

                    saved_count += 1

                except Exception as e:
                    logger.error(f"Failed to save Dividends record for {record.get('symbol', 'unknown')}: {str(e)}")

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return saved_count

        except Exception as e:
            logger.error(f"[DIVIDENDS] Database error while saving Dividends data: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return 0

    async def save_dividends_data_optimized(self, dividends_data: List[Dict[str, Any]]) -> int:
        """Save Dividends data using COPY command - OPTIMIZED"""
        if not dividends_data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                # Transaction optimization
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                # Create temp table
                await conn.execute('''
                    CREATE TEMP TABLE temp_dividends (
                        symbol VARCHAR(10),
                        ex_dividend_date DATE,
                        declaration_date DATE,
                        record_date DATE,
                        payment_date DATE,
                        amount NUMERIC(18, 4),
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')

                # Prepare data for COPY
                rows = [[
                    record['symbol'], record['ex_dividend_date'], record['declaration_date'],
                    record['record_date'], record['payment_date'], record['amount'], record['created_at']
                ] for record in dividends_data]

                # COPY to temp table
                await conn.copy_records_to_table(
                    'temp_dividends', records=rows,
                    columns=['symbol', 'ex_dividend_date', 'declaration_date', 'record_date',
                            'payment_date', 'amount', 'created_at']
                )

                # Upsert from temp to main
                await conn.execute('''
                    INSERT INTO us_dividends SELECT * FROM temp_dividends
                    ON CONFLICT (symbol, ex_dividend_date) DO UPDATE SET
                        declaration_date = EXCLUDED.declaration_date,
                        record_date = EXCLUDED.record_date,
                        payment_date = EXCLUDED.payment_date,
                        amount = EXCLUDED.amount,
                        created_at = EXCLUDED.created_at
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(dividends_data)
        except Exception as e:
            logger.error(f"[DIVIDENDS] Error in save_dividends_data_optimized: {e}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            logger.info("[DIVIDENDS] Falling back to regular save")
            return await self.save_dividends_data(dividends_data)

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern - OPTIMIZED"""
        for i, symbol in enumerate(symbols, 1):
            try:
                if self.collection_logger.is_collected('us_dividends', symbol, []):
                    continue

                api_data = await self.get_dividends_data(symbol)
                if api_data:
                    transformed = self.transform_dividends_data(api_data, symbol)
                    if transformed:
                        await data_queue.put((symbol, transformed))
                        logger.info(f"[DIVIDENDS API] [{i}/{len(symbols)}] Queued {symbol}")

                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[DIVIDENDS API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 50):
        """DB worker for pipeline pattern - OPTIMIZED"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_dividends_data_optimized(batch)
                    total_saved += saved
                break
            symbol, data = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_dividends_data_optimized(batch)
                if saved > 0:
                    total_saved += saved
                    for record in batch:
                        self.collection_logger.mark_collected('us_dividends', record['symbol'], [], 1)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[DIVIDENDS DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern and all 9 optimizations"""
        logger.info(f"[DIVIDENDS OPTIMIZED] Starting collection")
        logger.info(f"[DIVIDENDS OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")

        await self.init_pool()

        all_symbols = await self.get_existing_symbols()
        if not all_symbols:
            logger.error("[DIVIDENDS] No symbols found")
            await self.close_pool()
            return

        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_dividends', s, [])]
        logger.info(f"[DIVIDENDS] Total: {len(all_symbols)}, To process: {len(symbols)}")

        if not symbols:
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=50))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[DIVIDENDS OPTIMIZED] Completed in {duration}, saved {total_saved} records")

    async def collect_dividends(self):
        
        try:
            symbols_to_process = await self.get_symbols_needing_update()
            
            if not symbols_to_process:
                return
            
            total_processed = 0
            total_saved = 0
            
            for i, symbol in enumerate(symbols_to_process, 1):
                
                api_data = self.get_dividends_data(symbol)
                
                if api_data:
                    transformed_data = self.transform_dividends_data(api_data, symbol)
                    
                    if transformed_data:
                        saved_count = await self.save_dividends_data(transformed_data, symbol)
                        total_saved += saved_count
                    else:
                        logger.warning(f"No valid Dividends data for {symbol}")
                else:
                    logger.warning(f"No Dividends data retrieved for {symbol}")
                
                total_processed += 1
                
                # Rate limiting: 5 calls per minute (12 second intervals)
                if i < len(symbols_to_process):
                    await asyncio.sleep(12)
            
            
        except Exception as e:
            logger.error(f"Error during Dividends collection: {str(e)}")
        
    
    async def get_collection_status(self):
        try:
            conn = await self.get_connection()
            
            total_records_row = await conn.fetchrow('SELECT COUNT(*) FROM us_dividends')
            total_records = total_records_row[0] if total_records_row else 0
            
            symbols_row = await conn.fetchrow('SELECT COUNT(DISTINCT symbol) FROM us_dividends')
            symbols_count = symbols_row[0] if symbols_row else 0
            
            latest_row = await conn.fetchrow('''
                SELECT ex_dividend_date, symbol, amount FROM us_dividends 
                ORDER BY ex_dividend_date DESC LIMIT 1
            ''')
            
            earliest_row = await conn.fetchrow('''
                SELECT ex_dividend_date, symbol, amount FROM us_dividends 
                ORDER BY ex_dividend_date ASC LIMIT 1
            ''')
            
            await conn.close()
            
            if latest_row:
                pass
            if earliest_row:
                pass
            
            return total_records
            
        except Exception as e:
            logger.error(f"Failed to get collection status: {str(e)}")
            return 0


class SplitsCollector:
    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2, target_date: date = None):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.base_url = "https://www.alphavantage.co/query"
        self.retry_count = 3
        self.retry_delay = 1
        self.call_interval = call_interval
        self.start_date = date(2024, 1, 1)

        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_splits_collected.json'
        else:
            log_file_path = 'log/us_splits_collected.json'

        self.collection_logger = CollectionLogger(log_file_path)
        self.pool = None
        self.session = None

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
        logger.info("[SPLITS] Database connection pool initialized (OPTIMIZED: min=10, max=50)")

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[SPLITS] Database connection pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_existing_symbols(self) -> List[str]:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[SPLITS] Found {len(symbols)} symbols")
            return symbols
        except Exception as e:
            logger.error(f"[SPLITS] Failed to get existing symbols: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return []

    async def get_symbols_with_recent_data(self) -> set:
        try:
            conn = await self.get_connection()
            rows = await conn.fetch('''
                SELECT DISTINCT symbol 
                FROM us_splits 
                WHERE created_at >= NOW() - INTERVAL '7 days'
            ''')
            await conn.close()
            recent_symbols = {row['symbol'] for row in rows}
            return recent_symbols
        except Exception as e:
            logger.warning(f"Failed to get symbols with recent data: {str(e)}")
            return set()
    
    async def get_symbols_needing_update(self) -> set:
        try:
            all_symbols = await self.get_existing_symbols()
            recent_symbols = await self.get_symbols_with_recent_data()
            symbols_needing_update = all_symbols - recent_symbols
            
            
            return symbols_needing_update
            
        except Exception as e:
            logger.error(f"Failed to determine symbols needing update: {str(e)}")
            return await self.get_existing_symbols()

    async def get_splits_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch Splits data with async aiohttp - OPTIMIZED"""
        try:
            params = {
                'function': 'SPLITS',
                'symbol': symbol,
                'apikey': self.api_key
            }
            async with self.session.get(self.base_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'Error Message' in data:
                        logger.error(f"[SPLITS] API error for {symbol}: {data['Error Message']}")
                        return None
                    elif 'Note' in data:
                        logger.warning(f"[SPLITS] API limit reached: {data['Note']}")
                        return None
                    elif 'data' in data:
                        return data
                    else:
                        logger.warning(f"[SPLITS] Unexpected response format for {symbol}")
                        return None
                else:
                    logger.error(f"[SPLITS] API request failed for {symbol}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"[SPLITS] Error fetching data for {symbol}: {e}")
            return None

    def transform_splits_data(self, api_data: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
        try:
            transformed_records = []
            
            splits_data = api_data.get("data", [])
            
            for record in splits_data:
                try:
                    # Parse effective_date
                    effective_date_str = record.get("effective_date", "")
                    effective_date = None
                    
                    if effective_date_str:
                        try:
                            effective_date = datetime.strptime(effective_date_str, "%Y-%m-%d").date()
                        except ValueError:
                            logger.warning(f"Could not parse effective_date: {effective_date_str}")
                            continue
                    else:
                        continue
                    
                    # Filter data from 2025 onwards only
                    if effective_date.year < 2025:
                        continue
                    
                    # Parse split_factor
                    split_factor = self.safe_decimal(record.get("split_factor", ""), precision=18, scale=4)
                    
                    if not split_factor:
                        logger.warning(f"Invalid split_factor for {symbol} on {effective_date}")
                        continue
                    
                    transformed = {
                        'symbol': symbol[:10],  # VARCHAR(10) limit
                        'effective_date': effective_date,
                        'split_factor': split_factor,
                        'created_at': datetime.now()
                    }
                    
                    transformed_records.append(transformed)
                    
                except Exception as e:
                    logger.error(f"Error transforming record: {str(e)}, Record: {record}")
                    continue
            
            
            return transformed_records
            
        except Exception as e:
            logger.error(f"Error transforming Splits data for {symbol}: {str(e)}")
            return []
    
    def safe_decimal(self, value: str, precision: int = 18, scale: int = 4) -> Optional[Decimal]:
        if not value or value.strip() in ['', 'N/A', '-', 'null', 'None']:
            return None
        
        try:
            # Remove any non-numeric characters except decimal point and minus sign
            cleaned_value = ''.join(c for c in str(value).strip() if c.isdigit() or c in '.-')
            if not cleaned_value or cleaned_value in ['-', '.', '-.']:
                return None
            return Decimal(cleaned_value)
        except (InvalidOperation, ValueError, TypeError):
            logger.warning(f"Could not convert to decimal: {value}")
            return None
    
    async def save_splits_data(self, splits_data: List[Dict[str, Any]]) -> int:
        if not splits_data:
            return 0

        conn = await self.get_connection()
        try:
            saved_count = 0

            for record in splits_data:
                try:
                    await conn.execute('''
                        INSERT INTO us_splits
                        (symbol, effective_date, split_factor, created_at)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (symbol, effective_date) DO UPDATE SET
                        split_factor = EXCLUDED.split_factor,
                        created_at = EXCLUDED.created_at
                    ''',
                    record['symbol'],
                    record['effective_date'],
                    record['split_factor'],
                    record['created_at']
                    )

                    saved_count += 1

                except Exception as e:
                    logger.error(f"Failed to save Splits record for {record.get('symbol', 'unknown')}: {str(e)}")

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return saved_count

        except Exception as e:
            logger.error(f"[SPLITS] Database error while saving Splits data: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return 0

    async def save_splits_data_optimized(self, splits_data: List[Dict[str, Any]]) -> int:
        """Save Splits data using COPY command - OPTIMIZED"""
        if not splits_data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                # Transaction optimization
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                # Create temp table
                await conn.execute('''
                    CREATE TEMP TABLE temp_splits (
                        symbol VARCHAR(10),
                        effective_date DATE,
                        split_factor DECIMAL(18, 4),
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')

                # Prepare data for COPY
                rows = [[
                    record['symbol'], record['effective_date'], record['split_factor'], record['created_at']
                ] for record in splits_data]

                # COPY to temp table
                await conn.copy_records_to_table(
                    'temp_splits', records=rows,
                    columns=['symbol', 'effective_date', 'split_factor', 'created_at']
                )

                # Upsert from temp to main
                await conn.execute('''
                    INSERT INTO us_splits SELECT * FROM temp_splits
                    ON CONFLICT (symbol, effective_date) DO UPDATE SET
                        split_factor = EXCLUDED.split_factor,
                        created_at = EXCLUDED.created_at
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(splits_data)
        except Exception as e:
            logger.error(f"[SPLITS] Error in save_splits_data_optimized: {e}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            logger.info("[SPLITS] Falling back to regular save")
            return await self.save_splits_data(splits_data)

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern - OPTIMIZED"""
        for i, symbol in enumerate(symbols, 1):
            try:
                if self.collection_logger.is_collected('us_splits', symbol, []):
                    continue

                api_data = await self.get_splits_data(symbol)
                if api_data:
                    transformed = self.transform_splits_data(api_data, symbol)
                    if transformed:
                        await data_queue.put((symbol, transformed))
                        logger.info(f"[SPLITS API] [{i}/{len(symbols)}] Queued {symbol}")

                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[SPLITS API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 50):
        """DB worker for pipeline pattern - OPTIMIZED"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_splits_data_optimized(batch)
                    total_saved += saved
                break
            symbol, data = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_splits_data_optimized(batch)
                if saved > 0:
                    total_saved += saved
                    for record in batch:
                        self.collection_logger.mark_collected('us_splits', record['symbol'], [], 1)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[SPLITS DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern and all 9 optimizations"""
        logger.info(f"[SPLITS OPTIMIZED] Starting collection")
        logger.info(f"[SPLITS OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")

        await self.init_pool()

        all_symbols = await self.get_existing_symbols()
        if not all_symbols:
            logger.error("[SPLITS] No symbols found")
            await self.close_pool()
            return

        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_splits', s, [])]
        logger.info(f"[SPLITS] Total: {len(all_symbols)}, To process: {len(symbols)}")

        if not symbols:
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=50))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[SPLITS OPTIMIZED] Completed in {duration}, saved {total_saved} records")

    async def collect_splits(self):
        
        try:
            symbols_to_process = await self.get_symbols_needing_update()
            
            if not symbols_to_process:
                return
            
            total_processed = 0
            total_saved = 0
            
            for i, symbol in enumerate(symbols_to_process, 1):
                
                api_data = self.get_splits_data(symbol)
                
                if api_data:
                    transformed_data = self.transform_splits_data(api_data, symbol)
                    
                    if transformed_data:
                        saved_count = await self.save_splits_data(transformed_data, symbol)
                        total_saved += saved_count
                    else:
                        logger.warning(f"No valid Splits data for {symbol}")
                else:
                    logger.warning(f"No Splits data retrieved for {symbol}")
                
                total_processed += 1
                
                # Rate limiting: 5 calls per minute (12 second intervals)
                if i < len(symbols_to_process):
                    await asyncio.sleep(12)
            
            
        except Exception as e:
            logger.error(f"Error during Splits collection: {str(e)}")
        
    
    async def get_collection_status(self):
        try:
            conn = await self.get_connection()
            
            total_records_row = await conn.fetchrow('SELECT COUNT(*) FROM us_splits')
            total_records = total_records_row[0] if total_records_row else 0
            
            symbols_row = await conn.fetchrow('SELECT COUNT(DISTINCT symbol) FROM us_splits')
            symbols_count = symbols_row[0] if symbols_row else 0
            
            latest_row = await conn.fetchrow('''
                SELECT effective_date, symbol, split_factor FROM us_splits 
                ORDER BY effective_date DESC LIMIT 1
            ''')
            
            earliest_row = await conn.fetchrow('''
                SELECT effective_date, symbol, split_factor FROM us_splits 
                ORDER BY effective_date ASC LIMIT 1
            ''')
            
            await conn.close()
            
            if latest_row:
                pass
            if earliest_row:
                pass
            
            return total_records
            
        except Exception as e:
            logger.error(f"Failed to get collection status: {str(e)}")
            return 0


class EconomicIndicatorCollector:
    """Base class for economic indicators with Pipeline pattern"""

    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.call_interval = call_interval
        self.base_url = "https://www.alphavantage.co/query"
        self.pool = None
        self.session = None
        self.start_date = date(2025, 1, 1)

    async def init_pool(self):
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
        logger.info(f"[{self.__class__.__name__}] Database pool initialized")

    async def close_pool(self):
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info(f"[{self.__class__.__name__}] Database pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    def safe_decimal(self, value: str, scale: int = 4) -> Optional[Decimal]:
        if not value or str(value).strip() in ['', 'N/A', '-', 'null', 'None', '.']:
            return None
        try:
            cleaned_value = ''.join(c for c in str(value).strip() if c.isdigit() or c in '.-')
            if not cleaned_value or cleaned_value in ['-', '.', '-.']:
                return None
            return Decimal(cleaned_value)
        except (InvalidOperation, ValueError, TypeError):
            return None


class FederalFundsRateCollector(EconomicIndicatorCollector):
    """Collects Federal Funds Rate data"""

    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2):
        super().__init__(api_key, database_url, call_interval)
        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_fed_funds_rate_collected.json'
        else:
            log_file_path = 'log/us_fed_funds_rate_collected.json'
        self.collection_logger = CollectionLogger(log_file_path)

    async def fetch_data(self) -> Optional[Dict]:
        try:
            url = f"{self.base_url}?function=FEDERAL_FUNDS_RATE&apikey={self.api_key}&datatype=json"
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if "data" in data:
                        return data
        except Exception as e:
            logger.error(f"[FED_FUNDS] Error fetching data: {e}")
        return None

    def transform_data(self, api_data: Dict) -> List[Dict]:
        transformed = []
        name = api_data.get("name")
        unit = api_data.get("unit")
        for record in api_data.get("data", []):
            try:
                record_date = datetime.strptime(record.get("date", ""), "%Y-%m-%d").date()
                if record_date >= self.start_date:
                    transformed.append({
                        'date': record_date,
                        'interval': 'daily',
                        'value': self.safe_decimal(record.get("value")),
                        'name': name,
                        'unit': unit,
                        'created_at': datetime.now()
                    })
            except Exception as e:
                logger.error(f"[FED_FUNDS] Transform error: {e}")
        return transformed

    async def save_batch(self, batch: List[Dict]) -> int:
        if not batch:
            return 0
        conn = await self.get_connection()
        try:
            async with conn.transaction():
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')
                await conn.execute('''
                    CREATE TEMP TABLE temp_fed_funds (
                        date DATE,
                        interval VARCHAR(20),
                        value DECIMAL(8,4),
                        name TEXT,
                        unit VARCHAR(50),
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')
                rows = [[r['date'], r['interval'], r['value'], r['name'], r['unit'], r['created_at']] for r in batch]
                await conn.copy_records_to_table('temp_fed_funds', records=rows,
                                                columns=['date', 'interval', 'value', 'name', 'unit', 'created_at'])
                await conn.execute('''
                    INSERT INTO us_fed_funds_rate (date, interval, value, name, unit, created_at)
                    SELECT * FROM temp_fed_funds
                    ON CONFLICT (date, interval) DO UPDATE SET
                        value = EXCLUDED.value,
                        name = EXCLUDED.name,
                        unit = EXCLUDED.unit,
                        created_at = EXCLUDED.created_at
                ''')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(batch)
        except Exception as e:
            logger.error(f"[FED_FUNDS] Save error: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return 0

    async def api_worker(self, data_queue: Queue):
        """API worker for pipeline pattern"""
        try:
            if self.collection_logger.is_collected('us_fed_funds_rate', 'fed_funds', []):
                logger.info("[FED_FUNDS API] Already collected, skipping")
                await data_queue.put(None)
                return

            api_data = await self.fetch_data()
            if api_data:
                transformed = self.transform_data(api_data)
                if transformed:
                    await data_queue.put(transformed)
                    logger.info(f"[FED_FUNDS API] Queued {len(transformed)} records")
        except Exception as e:
            logger.error(f"[FED_FUNDS API] Error: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 100):
        """DB worker for pipeline pattern"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_batch(batch)
                    total_saved += saved
                    if saved > 0:
                        self.collection_logger.mark_collected('us_fed_funds_rate', 'fed_funds', [], saved)
                        self.collection_logger.save_log()
                break
            batch.extend(item)
            if len(batch) >= batch_size:
                saved = await self.save_batch(batch)
                if saved > 0:
                    total_saved += saved
                    self.collection_logger.mark_collected('us_fed_funds_rate', 'fed_funds', [], saved)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[FED_FUNDS DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern"""
        logger.info(f"[FED_FUNDS OPTIMIZED] Starting collection")
        await self.init_pool()

        if self.collection_logger.is_collected('us_fed_funds_rate', 'fed_funds', []):
            logger.info("[FED_FUNDS] Already collected")
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=10)
        api_task = asyncio.create_task(self.api_worker(data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=100))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[FED_FUNDS OPTIMIZED] Completed in {duration}, saved {total_saved} records")


class TreasuryYieldCollector(EconomicIndicatorCollector):
    """Collects Treasury Yield data for multiple maturities"""

    MATURITIES = ['3month', '2year', '5year', '7year', '10year', '30year']

    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2):
        super().__init__(api_key, database_url, call_interval)
        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_treasury_yield_collected.json'
        else:
            log_file_path = 'log/us_treasury_yield_collected.json'
        self.collection_logger = CollectionLogger(log_file_path)

    async def fetch_data(self, maturity: str) -> Optional[Dict]:
        try:
            url = f"{self.base_url}?function=TREASURY_YIELD&interval=daily&maturity={maturity}&apikey={self.api_key}&datatype=json"
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if "data" in data:
                        return {"maturity": maturity, "data": data["data"], "name": data.get("name"), "unit": data.get("unit")}
        except Exception as e:
            logger.error(f"[TREASURY_{maturity}] Error fetching data: {e}")
        return None

    def transform_data(self, api_data: Dict) -> List[Dict]:
        transformed = []
        interval = api_data.get("maturity", "")  # maturity value goes into interval column
        name = api_data.get("name")
        unit = api_data.get("unit")
        for record in api_data.get("data", []):
            try:
                record_date = datetime.strptime(record.get("date", ""), "%Y-%m-%d").date()
                if record_date >= self.start_date:
                    transformed.append({
                        'date': record_date,
                        'interval': interval,
                        'value': self.safe_decimal(record.get("value")),
                        'name': name,
                        'unit': unit,
                        'created_at': datetime.now()
                    })
            except Exception as e:
                logger.error(f"[TREASURY_{interval}] Transform error: {e}")
        return transformed

    async def save_batch(self, batch: List[Dict]) -> int:
        if not batch:
            return 0
        conn = await self.get_connection()
        try:
            async with conn.transaction():
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')
                await conn.execute('''
                    CREATE TEMP TABLE temp_treasury (
                        date DATE,
                        interval VARCHAR(20),
                        value DECIMAL(8,4),
                        name TEXT,
                        unit VARCHAR(50),
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')
                rows = [[r['date'], r['interval'], r['value'], r['name'], r['unit'], r['created_at']] for r in batch]
                await conn.copy_records_to_table('temp_treasury', records=rows,
                                                columns=['date', 'interval', 'value', 'name', 'unit', 'created_at'])
                await conn.execute('''
                    INSERT INTO us_treasury_yield (date, interval, value, name, unit, created_at)
                    SELECT * FROM temp_treasury
                    ON CONFLICT (date, interval) DO UPDATE SET
                        value = EXCLUDED.value,
                        name = EXCLUDED.name,
                        unit = EXCLUDED.unit,
                        created_at = EXCLUDED.created_at
                ''')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(batch)
        except Exception as e:
            logger.error(f"[TREASURY] Save error: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return 0

    async def api_worker(self, data_queue: Queue):
        """API worker for pipeline pattern - loops through maturities"""
        for i, maturity in enumerate(self.MATURITIES, 1):
            try:
                if self.collection_logger.is_collected('us_treasury_yield', maturity, []):
                    logger.info(f"[TREASURY API] [{i}/{len(self.MATURITIES)}] {maturity} already collected, skipping")
                    continue

                api_data = await self.fetch_data(maturity)
                if api_data:
                    transformed = self.transform_data(api_data)
                    if transformed:
                        await data_queue.put((maturity, transformed))
                        logger.info(f"[TREASURY API] [{i}/{len(self.MATURITIES)}] Queued {maturity}: {len(transformed)} records")

                if i < len(self.MATURITIES):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[TREASURY API] Error {maturity}: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 100):
        """DB worker for pipeline pattern"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_batch(batch)
                    total_saved += saved
                break
            maturity, data = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_batch(batch)
                if saved > 0:
                    total_saved += saved
                    # Mark the maturity as collected
                    for record in batch:
                        if record['interval'] not in [r['interval'] for r in batch[:batch.index(record)]]:
                            self.collection_logger.mark_collected('us_treasury_yield', record['interval'], [], 1)
                    self.collection_logger.save_log()
                batch = []
        # Mark remaining maturities as collected
        if total_saved > 0:
            self.collection_logger.save_log()
        logger.info(f"[TREASURY DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern"""
        logger.info(f"[TREASURY OPTIMIZED] Starting collection")
        logger.info(f"[TREASURY OPTIMIZED] API rate: {60/self.call_interval:.0f} calls/min")
        await self.init_pool()

        uncollected_maturities = [m for m in self.MATURITIES if not self.collection_logger.is_collected('us_treasury_yield', m, [])]
        logger.info(f"[TREASURY] Total maturities: {len(self.MATURITIES)}, To process: {len(uncollected_maturities)}")

        if not uncollected_maturities:
            logger.info("[TREASURY] All maturities already collected")
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=10)
        api_task = asyncio.create_task(self.api_worker(data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=100))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[TREASURY OPTIMIZED] Completed in {duration}, saved {total_saved} records")


class CPICollector(EconomicIndicatorCollector):
    """Collects CPI data"""


    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2):
        super().__init__(api_key, database_url, call_interval)
        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_cpi_collected.json'
        else:
            log_file_path = 'log/us_cpi_collected.json'
        self.collection_logger = CollectionLogger(log_file_path)
    async def fetch_data(self) -> Optional[Dict]:
        try:
            url = f"{self.base_url}?function=CPI&interval=monthly&apikey={self.api_key}&datatype=json"
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if "data" in data:
                        return data
        except Exception as e:
            logger.error(f"[CPI] Error fetching data: {e}")
        return None

    def transform_data(self, api_data: Dict) -> List[Dict]:
        transformed = []
        name = api_data.get("name")
        unit = api_data.get("unit")
        for record in api_data.get("data", []):
            try:
                record_date = datetime.strptime(record.get("date", ""), "%Y-%m-%d").date()
                if record_date >= self.start_date:
                    transformed.append({
                        'date': record_date,
                        'interval': 'monthly',
                        'value': self.safe_decimal(record.get("value")),
                        'name': name,
                        'unit': unit,
                        'created_at': datetime.now()
                    })
            except Exception as e:
                logger.error(f"[CPI] Transform error: {e}")
        return transformed

    async def save_batch(self, batch: List[Dict]) -> int:
        if not batch:
            return 0
        conn = await self.get_connection()
        try:
            async with conn.transaction():
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')
                await conn.execute('''
                    CREATE TEMP TABLE temp_cpi (
                        date DATE,
                        interval VARCHAR(20),
                        value DECIMAL(8,4),
                        name TEXT,
                        unit VARCHAR(50),
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')
                rows = [[r['date'], r['interval'], r['value'], r['name'], r['unit'], r['created_at']] for r in batch]
                await conn.copy_records_to_table('temp_cpi', records=rows,
                                                columns=['date', 'interval', 'value', 'name', 'unit', 'created_at'])
                await conn.execute('''
                    INSERT INTO us_cpi (date, interval, value, name, unit, created_at)
                    SELECT * FROM temp_cpi
                    ON CONFLICT (date, interval) DO UPDATE SET
                        value = EXCLUDED.value,
                        name = EXCLUDED.name,
                        unit = EXCLUDED.unit,
                        created_at = EXCLUDED.created_at
                ''')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(batch)
        except Exception as e:
            logger.error(f"[CPI] Save error: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return 0

    async def api_worker(self, data_queue: Queue):
        """API worker for pipeline pattern"""
        try:
            if self.collection_logger.is_collected('us_cpi', 'cpi', []):
                logger.info("[CPI API] Already collected, skipping")
                await data_queue.put(None)
                return

            api_data = await self.fetch_data()
            if api_data:
                transformed = self.transform_data(api_data)
                if transformed:
                    await data_queue.put(transformed)
                    logger.info(f"[CPI API] Queued {len(transformed)} records")
        except Exception as e:
            logger.error(f"[CPI API] Error: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 100):
        """DB worker for pipeline pattern"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_batch(batch)
                    total_saved += saved
                    if saved > 0:
                        self.collection_logger.mark_collected('us_cpi', 'cpi', [], saved)
                        self.collection_logger.save_log()
                break
            batch.extend(item)
            if len(batch) >= batch_size:
                saved = await self.save_batch(batch)
                if saved > 0:
                    total_saved += saved
                    self.collection_logger.mark_collected('us_cpi', 'cpi', [], saved)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[CPI DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern"""
        logger.info(f"[CPI OPTIMIZED] Starting collection")
        await self.init_pool()

        if self.collection_logger.is_collected('us_cpi', 'cpi', []):
            logger.info("[CPI] Already collected")
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=10)
        api_task = asyncio.create_task(self.api_worker(data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=100))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[CPI OPTIMIZED] Completed in {duration}, saved {total_saved} records")



class UnemploymentRateCollector(EconomicIndicatorCollector):
    """Collects Unemployment Rate data"""


    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2):
        super().__init__(api_key, database_url, call_interval)
        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/us_unemployment_rate_collected.json'
        else:
            log_file_path = 'log/us_unemployment_rate_collected.json'
        self.collection_logger = CollectionLogger(log_file_path)
    async def fetch_data(self) -> Optional[Dict]:
        try:
            url = f"{self.base_url}?function=UNEMPLOYMENT&apikey={self.api_key}&datatype=json"
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if "data" in data:
                        return data
        except Exception as e:
            logger.error(f"[UNEMPLOYMENT] Error fetching data: {e}")
        return None

    def transform_data(self, api_data: Dict) -> List[Dict]:
        transformed = []
        name = api_data.get("name")
        unit = api_data.get("unit")
        for record in api_data.get("data", []):
            try:
                record_date = datetime.strptime(record.get("date", ""), "%Y-%m-%d").date()
                if record_date >= self.start_date:
                    transformed.append({
                        'date': record_date,
                        'interval': 'monthly',
                        'value': self.safe_decimal(record.get("value")),
                        'name': name,
                        'unit': unit,
                        'created_at': datetime.now()
                    })
            except Exception as e:
                logger.error(f"[UNEMPLOYMENT] Transform error: {e}")
        return transformed

    async def save_batch(self, batch: List[Dict]) -> int:
        if not batch:
            return 0
        conn = await self.get_connection()
        try:
            async with conn.transaction():
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')
                await conn.execute('''
                    CREATE TEMP TABLE temp_unemployment (
                        date DATE,
                        interval VARCHAR(20),
                        value DECIMAL(8,4),
                        name TEXT,
                        unit VARCHAR(50),
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')
                rows = [[r['date'], r['interval'], r['value'], r['name'], r['unit'], r['created_at']] for r in batch]
                await conn.copy_records_to_table('temp_unemployment', records=rows,
                                                columns=['date', 'interval', 'value', 'name', 'unit', 'created_at'])
                await conn.execute('''
                    INSERT INTO us_unemployment_rate (date, interval, value, name, unit, created_at)
                    SELECT * FROM temp_unemployment
                    ON CONFLICT (date, interval) DO UPDATE SET
                        value = EXCLUDED.value,
                        name = EXCLUDED.name,
                        unit = EXCLUDED.unit,
                        created_at = EXCLUDED.created_at
                ''')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(batch)
        except Exception as e:
            logger.error(f"[UNEMPLOYMENT] Save error: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return 0

    async def api_worker(self, data_queue: Queue):
        """API worker for pipeline pattern"""
        try:
            if self.collection_logger.is_collected('us_unemployment_rate', 'unemployment', []):
                logger.info("[UNEMPLOYMENT API] Already collected, skipping")
                await data_queue.put(None)
                return

            api_data = await self.fetch_data()
            if api_data:
                transformed = self.transform_data(api_data)
                if transformed:
                    await data_queue.put(transformed)
                    logger.info(f"[UNEMPLOYMENT API] Queued {len(transformed)} records")
        except Exception as e:
            logger.error(f"[UNEMPLOYMENT API] Error: {e}")
        await data_queue.put(None)

    async def db_worker(self, data_queue: Queue, batch_size: int = 100):
        """DB worker for pipeline pattern"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_batch(batch)
                    total_saved += saved
                    if saved > 0:
                        self.collection_logger.mark_collected('us_unemployment_rate', 'unemployment', [], saved)
                        self.collection_logger.save_log()
                break
            batch.extend(item)
            if len(batch) >= batch_size:
                saved = await self.save_batch(batch)
                if saved > 0:
                    total_saved += saved
                    self.collection_logger.mark_collected('us_unemployment_rate', 'unemployment', [], saved)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[UNEMPLOYMENT DB] Total saved: {total_saved}")
        return total_saved

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern"""
        logger.info(f"[UNEMPLOYMENT OPTIMIZED] Starting collection")
        await self.init_pool()

        if self.collection_logger.is_collected('us_unemployment_rate', 'unemployment', []):
            logger.info("[UNEMPLOYMENT] Already collected")
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=10)
        api_task = asyncio.create_task(self.api_worker(data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=100))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[UNEMPLOYMENT OPTIMIZED] Completed in {duration}, saved {total_saved} records")


class EarningsEstimatesCollectorOptimized:
    """Optimized Earnings Estimates collector with Pipeline pattern"""

    def __init__(self, api_key: str, database_url: str, call_interval: float = 0.2):
        self.api_key = api_key
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.call_interval = call_interval
        self.base_url = "https://www.alphavantage.co/query"
        self.pool = None
        self.session = None
        self.start_date = date(2021, 1, 1)

        # Railway Volume path support
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_file_path = '/app/log/earnings_estimates_optimized_collected.json'
        else:
            log_file_path = 'log/earnings_estimates_optimized_collected.json'

        self.collection_logger = CollectionLogger(log_file_path)

    async def init_pool(self):
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
        logger.info("[EARNINGS_EST] Database pool initialized")

    async def close_pool(self):
        if self.pool:
            await self.pool.close()
        if self.session:
            await self.session.close()
        logger.info("[EARNINGS_EST] Database pool closed")

    async def get_connection(self):
        if self.pool:
            return await self.pool.acquire()
        else:
            return await asyncpg.connect(self.database_url)

    async def get_active_symbols(self) -> List[str]:
        conn = await self.get_connection()
        try:
            rows = await conn.fetch('SELECT symbol FROM us_stock_basic WHERE is_active = true ORDER BY symbol')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            symbols = [row['symbol'] for row in rows]
            logger.info(f"[EARNINGS_EST] Found {len(symbols)} active symbols")
            return symbols
        except Exception as e:
            logger.error(f"[EARNINGS_EST] Error getting symbols: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return []

    async def fetch_data(self, symbol: str) -> Optional[Dict]:
        try:
            url = f"{self.base_url}?function=EARNINGS_ESTIMATES&symbol={symbol}&apikey={self.api_key}"
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if "estimates" in data:
                        return {"symbol": symbol, "data": data}
        except Exception as e:
            logger.error(f"[EARNINGS_EST] Error fetching {symbol}: {e}")
        return None

    def safe_decimal(self, value: str, scale: int = 4) -> Optional[Decimal]:
        if not value or str(value).strip() in ['', 'N/A', '-', 'null', 'None', '.']:
            return None
        try:
            cleaned_value = ''.join(c for c in str(value).strip() if c.isdigit() or c in '.-')
            if not cleaned_value or cleaned_value in ['-', '.', '-.']:
                return None
            return Decimal(cleaned_value)
        except (InvalidOperation, ValueError, TypeError):
            return None

    def safe_int(self, value: str) -> Optional[int]:
        if not value or str(value).strip() in ['', 'N/A', '-', 'null', 'None']:
            return None
        try:
            cleaned_value = ''.join(c for c in str(value).strip() if c.isdigit() or c == '-')
            if not cleaned_value or cleaned_value == '-':
                return None
            return int(cleaned_value)
        except (ValueError, TypeError):
            return None

    def transform_data(self, api_data: Dict) -> List[Dict]:
        transformed = []
        symbol = api_data.get("symbol", "")
        for estimate in api_data.get("data", {}).get("estimates", []):
            try:
                estimate_date = estimate.get("date", "")
                if not estimate_date:
                    continue
                try:
                    parsed_date = datetime.strptime(estimate_date, "%Y-%m-%d").date()
                except ValueError:
                    try:
                        parsed_date = datetime.strptime(estimate_date, "%Y-%m").date()
                    except ValueError:
                        continue
                if parsed_date < self.start_date:
                    continue
                transformed.append({
                    'symbol': symbol,
                    'estimate_date': parsed_date,
                    'horizon': estimate.get("horizon", "")[:50],
                    'eps_estimate_average': self.safe_decimal(estimate.get("eps_estimate_average")),
                    'eps_estimate_high': self.safe_decimal(estimate.get("eps_estimate_high")),
                    'eps_estimate_low': self.safe_decimal(estimate.get("eps_estimate_low")),
                    'eps_estimate_analyst_count': self.safe_int(estimate.get("eps_estimate_analyst_count")),
                    'eps_estimate_average_7_days_ago': self.safe_decimal(estimate.get("eps_estimate_average_7_days_ago")),
                    'eps_estimate_average_30_days_ago': self.safe_decimal(estimate.get("eps_estimate_average_30_days_ago")),
                    'eps_estimate_average_60_days_ago': self.safe_decimal(estimate.get("eps_estimate_average_60_days_ago")),
                    'eps_estimate_average_90_days_ago': self.safe_decimal(estimate.get("eps_estimate_average_90_days_ago")),
                    'eps_estimate_revision_up_trailing_7_days': self.safe_int(estimate.get("eps_estimate_revision_up_trailing_7_days")),
                    'eps_estimate_revision_down_trailing_7_days': self.safe_int(estimate.get("eps_estimate_revision_down_trailing_7_days")),
                    'eps_estimate_revision_up_trailing_30_days': self.safe_int(estimate.get("eps_estimate_revision_up_trailing_30_days")),
                    'eps_estimate_revision_down_trailing_30_days': self.safe_int(estimate.get("eps_estimate_revision_down_trailing_30_days")),
                    'revenue_estimate_average': self.safe_decimal(estimate.get("revenue_estimate_average"), scale=2),
                    'revenue_estimate_high': self.safe_decimal(estimate.get("revenue_estimate_high"), scale=2),
                    'revenue_estimate_low': self.safe_decimal(estimate.get("revenue_estimate_low"), scale=2),
                    'revenue_estimate_analyst_count': self.safe_int(estimate.get("revenue_estimate_analyst_count")),
                    'created_at': datetime.now()
                })
            except Exception as e:
                logger.error(f"[EARNINGS_EST] Transform error for {symbol}: {e}")
        return transformed

    async def api_worker(self, symbols: List[str], data_queue: Queue):
        """API worker for pipeline pattern"""
        for i, symbol in enumerate(symbols, 1):
            try:
                if self.collection_logger.is_collected('us_earnings_estimates', symbol, []):
                    continue

                api_data = await self.fetch_data(symbol)
                if api_data:
                    transformed = self.transform_data(api_data)
                    if transformed:
                        await data_queue.put((symbol, transformed))
                        logger.info(f"[EARNINGS_EST_OPT API] [{i}/{len(symbols)}] Queued {symbol}")

                if i < len(symbols):
                    await asyncio.sleep(self.call_interval)
            except Exception as e:
                logger.error(f"[EARNINGS_EST_OPT API] Error {symbol}: {e}")
        await data_queue.put(None)

    async def save_earnings_estimates_data(self, earnings_estimates_data: List[Dict]) -> int:
        """Regular save method (fallback for optimized version)"""
        if not earnings_estimates_data:
            return 0

        try:
            conn = await self.get_connection()
            saved_count = 0

            for record in earnings_estimates_data:
                try:
                    await conn.execute('''
                        INSERT INTO us_earnings_estimates
                        (symbol, estimate_date, horizon, eps_estimate_average, eps_estimate_high, eps_estimate_low,
                         eps_estimate_analyst_count, eps_estimate_average_7_days_ago, eps_estimate_average_30_days_ago,
                         eps_estimate_average_60_days_ago, eps_estimate_average_90_days_ago,
                         eps_estimate_revision_up_trailing_7_days, eps_estimate_revision_down_trailing_7_days,
                         eps_estimate_revision_up_trailing_30_days, eps_estimate_revision_down_trailing_30_days,
                         revenue_estimate_average, revenue_estimate_high, revenue_estimate_low,
                         revenue_estimate_analyst_count, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                        ON CONFLICT (symbol, estimate_date, horizon) DO UPDATE SET
                        eps_estimate_average = EXCLUDED.eps_estimate_average,
                        eps_estimate_high = EXCLUDED.eps_estimate_high,
                        eps_estimate_low = EXCLUDED.eps_estimate_low,
                        eps_estimate_analyst_count = EXCLUDED.eps_estimate_analyst_count,
                        eps_estimate_average_7_days_ago = EXCLUDED.eps_estimate_average_7_days_ago,
                        eps_estimate_average_30_days_ago = EXCLUDED.eps_estimate_average_30_days_ago,
                        eps_estimate_average_60_days_ago = EXCLUDED.eps_estimate_average_60_days_ago,
                        eps_estimate_average_90_days_ago = EXCLUDED.eps_estimate_average_90_days_ago,
                        eps_estimate_revision_up_trailing_7_days = EXCLUDED.eps_estimate_revision_up_trailing_7_days,
                        eps_estimate_revision_down_trailing_7_days = EXCLUDED.eps_estimate_revision_down_trailing_7_days,
                        eps_estimate_revision_up_trailing_30_days = EXCLUDED.eps_estimate_revision_up_trailing_30_days,
                        eps_estimate_revision_down_trailing_30_days = EXCLUDED.eps_estimate_revision_down_trailing_30_days,
                        revenue_estimate_average = EXCLUDED.revenue_estimate_average,
                        revenue_estimate_high = EXCLUDED.revenue_estimate_high,
                        revenue_estimate_low = EXCLUDED.revenue_estimate_low,
                        revenue_estimate_analyst_count = EXCLUDED.revenue_estimate_analyst_count,
                        created_at = EXCLUDED.created_at
                    ''',
                    record['symbol'],
                    record['estimate_date'],
                    record['horizon'],
                    record['eps_estimate_average'],
                    record['eps_estimate_high'],
                    record['eps_estimate_low'],
                    record['eps_estimate_analyst_count'],
                    record['eps_estimate_average_7_days_ago'],
                    record['eps_estimate_average_30_days_ago'],
                    record['eps_estimate_average_60_days_ago'],
                    record['eps_estimate_average_90_days_ago'],
                    record['eps_estimate_revision_up_trailing_7_days'],
                    record['eps_estimate_revision_down_trailing_7_days'],
                    record['eps_estimate_revision_up_trailing_30_days'],
                    record['eps_estimate_revision_down_trailing_30_days'],
                    record['revenue_estimate_average'],
                    record['revenue_estimate_high'],
                    record['revenue_estimate_low'],
                    record['revenue_estimate_analyst_count'],
                    record['created_at']
                    )

                    saved_count += 1

                except Exception as e:
                    logger.error(f"Failed to save record for {record.get('symbol', 'unknown')}: {str(e)}")

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return saved_count

        except Exception as e:
            logger.error(f"[EARNINGS_EST_OPT] Database error while saving data: {str(e)}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            return 0

    async def save_earnings_estimates_data_optimized(self, earnings_estimates_data: List[Dict]) -> int:
        """Save Earnings Estimates data using COPY command - OPTIMIZED"""
        if not earnings_estimates_data:
            return 0

        conn = await self.get_connection()
        try:
            async with conn.transaction():
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                await conn.execute('''
                    CREATE TEMP TABLE temp_earnings_estimates_opt (
                        symbol VARCHAR(20),
                        estimate_date DATE,
                        horizon VARCHAR(50),
                        eps_estimate_average NUMERIC(18,4),
                        eps_estimate_high NUMERIC(18,4),
                        eps_estimate_low NUMERIC(18,4),
                        eps_estimate_analyst_count INTEGER,
                        eps_estimate_average_7_days_ago NUMERIC(18,4),
                        eps_estimate_average_30_days_ago NUMERIC(18,4),
                        eps_estimate_average_60_days_ago NUMERIC(18,4),
                        eps_estimate_average_90_days_ago NUMERIC(18,4),
                        eps_estimate_revision_up_trailing_7_days INTEGER,
                        eps_estimate_revision_down_trailing_7_days INTEGER,
                        eps_estimate_revision_up_trailing_30_days INTEGER,
                        eps_estimate_revision_down_trailing_30_days INTEGER,
                        revenue_estimate_average NUMERIC(18,2),
                        revenue_estimate_high NUMERIC(18,2),
                        revenue_estimate_low NUMERIC(18,2),
                        revenue_estimate_analyst_count INTEGER,
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')

                rows = [[
                    record['symbol'], record['estimate_date'], record['horizon'],
                    record['eps_estimate_average'], record['eps_estimate_high'], record['eps_estimate_low'],
                    record['eps_estimate_analyst_count'], record['eps_estimate_average_7_days_ago'],
                    record['eps_estimate_average_30_days_ago'], record['eps_estimate_average_60_days_ago'],
                    record['eps_estimate_average_90_days_ago'], record['eps_estimate_revision_up_trailing_7_days'],
                    record['eps_estimate_revision_down_trailing_7_days'], record['eps_estimate_revision_up_trailing_30_days'],
                    record['eps_estimate_revision_down_trailing_30_days'], record['revenue_estimate_average'],
                    record['revenue_estimate_high'], record['revenue_estimate_low'],
                    record['revenue_estimate_analyst_count'], record['created_at']
                ] for record in earnings_estimates_data]

                await conn.copy_records_to_table(
                    'temp_earnings_estimates_opt', records=rows,
                    columns=['symbol', 'estimate_date', 'horizon', 'eps_estimate_average', 'eps_estimate_high',
                            'eps_estimate_low', 'eps_estimate_analyst_count', 'eps_estimate_average_7_days_ago',
                            'eps_estimate_average_30_days_ago', 'eps_estimate_average_60_days_ago',
                            'eps_estimate_average_90_days_ago', 'eps_estimate_revision_up_trailing_7_days',
                            'eps_estimate_revision_down_trailing_7_days', 'eps_estimate_revision_up_trailing_30_days',
                            'eps_estimate_revision_down_trailing_30_days', 'revenue_estimate_average',
                            'revenue_estimate_high', 'revenue_estimate_low', 'revenue_estimate_analyst_count',
                            'created_at']
                )

                await conn.execute('''
                    INSERT INTO us_earnings_estimates SELECT * FROM temp_earnings_estimates_opt
                    ON CONFLICT (symbol, estimate_date, horizon) DO UPDATE SET
                        eps_estimate_average = EXCLUDED.eps_estimate_average,
                        eps_estimate_high = EXCLUDED.eps_estimate_high,
                        eps_estimate_low = EXCLUDED.eps_estimate_low,
                        eps_estimate_analyst_count = EXCLUDED.eps_estimate_analyst_count,
                        eps_estimate_average_7_days_ago = EXCLUDED.eps_estimate_average_7_days_ago,
                        eps_estimate_average_30_days_ago = EXCLUDED.eps_estimate_average_30_days_ago,
                        eps_estimate_average_60_days_ago = EXCLUDED.eps_estimate_average_60_days_ago,
                        eps_estimate_average_90_days_ago = EXCLUDED.eps_estimate_average_90_days_ago,
                        eps_estimate_revision_up_trailing_7_days = EXCLUDED.eps_estimate_revision_up_trailing_7_days,
                        eps_estimate_revision_down_trailing_7_days = EXCLUDED.eps_estimate_revision_down_trailing_7_days,
                        eps_estimate_revision_up_trailing_30_days = EXCLUDED.eps_estimate_revision_up_trailing_30_days,
                        eps_estimate_revision_down_trailing_30_days = EXCLUDED.eps_estimate_revision_down_trailing_30_days,
                        revenue_estimate_average = EXCLUDED.revenue_estimate_average,
                        revenue_estimate_high = EXCLUDED.revenue_estimate_high,
                        revenue_estimate_low = EXCLUDED.revenue_estimate_low,
                        revenue_estimate_analyst_count = EXCLUDED.revenue_estimate_analyst_count,
                        created_at = EXCLUDED.created_at
                ''')

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(earnings_estimates_data)
        except Exception as e:
            logger.error(f"[EARNINGS_EST_OPT] Error in save_earnings_estimates_data_optimized: {e}")
            if self.pool:
                try:
                    await self.pool.release(conn)
                except:
                    pass
            logger.info("[EARNINGS_EST_OPT] Falling back to regular save")
            return await self.save_earnings_estimates_data(earnings_estimates_data)

    async def db_worker(self, data_queue: Queue, batch_size: int = 100):
        """DB worker for pipeline pattern - OPTIMIZED"""
        batch, total_saved = [], 0
        while True:
            item = await data_queue.get()
            if item is None:
                if batch:
                    saved = await self.save_earnings_estimates_data_optimized(batch)
                    if saved > 0:
                        total_saved += saved
                        for record in batch:
                            self.collection_logger.mark_collected('us_earnings_estimates', record['symbol'], [], 1)
                        self.collection_logger.save_log()
                break
            symbol, data = item
            batch.extend(data)
            if len(batch) >= batch_size:
                saved = await self.save_earnings_estimates_data_optimized(batch)
                if saved > 0:
                    total_saved += saved
                    for record in batch:
                        self.collection_logger.mark_collected('us_earnings_estimates', record['symbol'], [], 1)
                    self.collection_logger.save_log()
                batch = []
        logger.info(f"[EARNINGS_EST_OPT DB] Total saved: {total_saved}")
        return total_saved

    async def save_batch(self, batch: List[Dict]) -> int:
        if not batch:
            return 0
        conn = await self.get_connection()
        try:
            async with conn.transaction():
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')
                await conn.execute('''
                    CREATE TEMP TABLE temp_earnings_est (
                        symbol VARCHAR(20),
                        estimate_date DATE,
                        horizon VARCHAR(50),
                        eps_estimate_average DECIMAL(12,4),
                        eps_estimate_high DECIMAL(12,4),
                        eps_estimate_low DECIMAL(12,4),
                        eps_estimate_analyst_count INTEGER,
                        eps_estimate_average_7_days_ago DECIMAL(12,4),
                        eps_estimate_average_30_days_ago DECIMAL(12,4),
                        eps_estimate_average_60_days_ago DECIMAL(12,4),
                        eps_estimate_average_90_days_ago DECIMAL(12,4),
                        eps_estimate_revision_up_trailing_7_days INTEGER,
                        eps_estimate_revision_down_trailing_7_days INTEGER,
                        eps_estimate_revision_up_trailing_30_days INTEGER,
                        eps_estimate_revision_down_trailing_30_days INTEGER,
                        revenue_estimate_average DECIMAL(18,2),
                        revenue_estimate_high DECIMAL(18,2),
                        revenue_estimate_low DECIMAL(18,2),
                        revenue_estimate_analyst_count INTEGER,
                        created_at TIMESTAMP
                    ) ON COMMIT DROP
                ''')
                rows = [[
                    r['symbol'], r['estimate_date'], r['horizon'],
                    r['eps_estimate_average'], r['eps_estimate_high'], r['eps_estimate_low'],
                    r['eps_estimate_analyst_count'],
                    r['eps_estimate_average_7_days_ago'], r['eps_estimate_average_30_days_ago'],
                    r['eps_estimate_average_60_days_ago'], r['eps_estimate_average_90_days_ago'],
                    r['eps_estimate_revision_up_trailing_7_days'], r['eps_estimate_revision_down_trailing_7_days'],
                    r['eps_estimate_revision_up_trailing_30_days'], r['eps_estimate_revision_down_trailing_30_days'],
                    r['revenue_estimate_average'], r['revenue_estimate_high'], r['revenue_estimate_low'],
                    r['revenue_estimate_analyst_count'], r['created_at']
                ] for r in batch]
                await conn.copy_records_to_table('temp_earnings_est', records=rows,
                    columns=['symbol', 'estimate_date', 'horizon', 'eps_estimate_average', 'eps_estimate_high',
                            'eps_estimate_low', 'eps_estimate_analyst_count', 'eps_estimate_average_7_days_ago',
                            'eps_estimate_average_30_days_ago', 'eps_estimate_average_60_days_ago',
                            'eps_estimate_average_90_days_ago', 'eps_estimate_revision_up_trailing_7_days',
                            'eps_estimate_revision_down_trailing_7_days', 'eps_estimate_revision_up_trailing_30_days',
                            'eps_estimate_revision_down_trailing_30_days', 'revenue_estimate_average',
                            'revenue_estimate_high', 'revenue_estimate_low', 'revenue_estimate_analyst_count',
                            'created_at'])
                await conn.execute('''
                    INSERT INTO us_earnings_estimates (
                        symbol, estimate_date, horizon, eps_estimate_average, eps_estimate_high, eps_estimate_low,
                        eps_estimate_analyst_count, eps_estimate_average_7_days_ago, eps_estimate_average_30_days_ago,
                        eps_estimate_average_60_days_ago, eps_estimate_average_90_days_ago,
                        eps_estimate_revision_up_trailing_7_days, eps_estimate_revision_down_trailing_7_days,
                        eps_estimate_revision_up_trailing_30_days, eps_estimate_revision_down_trailing_30_days,
                        revenue_estimate_average, revenue_estimate_high, revenue_estimate_low,
                        revenue_estimate_analyst_count, created_at
                    )
                    SELECT * FROM temp_earnings_est
                    ON CONFLICT (symbol, estimate_date, horizon) DO UPDATE SET
                        eps_estimate_average = EXCLUDED.eps_estimate_average,
                        eps_estimate_high = EXCLUDED.eps_estimate_high,
                        eps_estimate_low = EXCLUDED.eps_estimate_low,
                        eps_estimate_analyst_count = EXCLUDED.eps_estimate_analyst_count,
                        eps_estimate_average_7_days_ago = EXCLUDED.eps_estimate_average_7_days_ago,
                        eps_estimate_average_30_days_ago = EXCLUDED.eps_estimate_average_30_days_ago,
                        eps_estimate_average_60_days_ago = EXCLUDED.eps_estimate_average_60_days_ago,
                        eps_estimate_average_90_days_ago = EXCLUDED.eps_estimate_average_90_days_ago,
                        eps_estimate_revision_up_trailing_7_days = EXCLUDED.eps_estimate_revision_up_trailing_7_days,
                        eps_estimate_revision_down_trailing_7_days = EXCLUDED.eps_estimate_revision_down_trailing_7_days,
                        eps_estimate_revision_up_trailing_30_days = EXCLUDED.eps_estimate_revision_up_trailing_30_days,
                        eps_estimate_revision_down_trailing_30_days = EXCLUDED.eps_estimate_revision_down_trailing_30_days,
                        revenue_estimate_average = EXCLUDED.revenue_estimate_average,
                        revenue_estimate_high = EXCLUDED.revenue_estimate_high,
                        revenue_estimate_low = EXCLUDED.revenue_estimate_low,
                        revenue_estimate_analyst_count = EXCLUDED.revenue_estimate_analyst_count,
                        created_at = EXCLUDED.created_at
                ''')
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return len(batch)
        except Exception as e:
            logger.error(f"[EARNINGS_EST] Save error: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return 0

    async def run_collection_optimized(self):
        """OPTIMIZED collection with pipeline pattern and CollectionLogger"""
        logger.info(f"[EARNINGS_EST_OPT] Starting collection")
        logger.info(f"[EARNINGS_EST_OPT] API rate: {60/self.call_interval:.0f} calls/min")

        await self.init_pool()

        all_symbols = await self.get_active_symbols()
        if not all_symbols:
            logger.error("[EARNINGS_EST_OPT] No symbols found")
            await self.close_pool()
            return

        symbols = [s for s in all_symbols if not self.collection_logger.is_collected('us_earnings_estimates', s, [])]
        logger.info(f"[EARNINGS_EST_OPT] Total: {len(all_symbols)}, To process: {len(symbols)}")

        if not symbols:
            await self.close_pool()
            return

        start_time = datetime.now()
        data_queue = Queue(maxsize=50)
        api_task = asyncio.create_task(self.api_worker(symbols, data_queue))
        db_task = asyncio.create_task(self.db_worker(data_queue, batch_size=100))
        await api_task
        total_saved = await db_task
        await self.close_pool()

        duration = datetime.now() - start_time
        logger.info(f"[EARNINGS_EST_OPT] Completed in {duration}, saved {total_saved} records")


if __name__ == "__main__":
    pass
