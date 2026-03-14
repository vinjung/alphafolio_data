"""
Naver Finance Research Report Crawler (Async Version)

Crawls research reports from finance.naver.com and stores in database.
Integrated with FastAPI async pattern.
"""

import aiohttp
import asyncpg
import asyncio
from bs4 import BeautifulSoup
import os
import logging
import re
from datetime import datetime, date
from typing import Dict, Optional, List
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NaverResearchCrawler:
    """Async Crawler for Naver Finance Research Reports"""

    def __init__(self, database_url: str):
        """
        Initialize crawler with database connection

        Args:
            database_url: PostgreSQL connection URL
        """
        self.base_url = "https://finance.naver.com/research"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://finance.naver.com/'
        }
        self.session = None
        self.db_pool = None

        # Database URL
        if database_url and database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url

        logger.info("NaverResearchCrawler initialized")

    async def initialize(self):
        """Initialize aiohttp session and database connection pool"""
        # Create aiohttp session
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)

        # Create asyncpg connection pool
        self.db_pool = await asyncpg.create_pool(
            self.database_url,
            min_size=1,
            max_size=5
        )

        logger.info("Session and database pool initialized")

    async def close(self):
        """Close aiohttp session and database connection pool"""
        if self.session:
            await self.session.close()

        if self.db_pool:
            await self.db_pool.close()

        logger.info("Session and database pool closed")

    async def get_report_list(self, page: int = 1) -> List[tuple]:
        """
        Get list of report IDs from list page

        Args:
            page: Page number (default 1)

        Returns:
            List of tuples: [(nid, date_str), ...]
        """
        url = f"{self.base_url}/company_list.naver?page={page}"

        try:
            logger.info(f"Crawling list page {page}...")
            await asyncio.sleep(1)  # Rate limiting

            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"Failed to get page {page}: Status {response.status}")
                    return []

                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

            # Find main table
            table = soup.find('table', {'class': 'type_1'})
            if not table:
                logger.warning(f"No table found on page {page}")
                return []

            report_list = []
            rows = table.find_all('tr')

            for row in rows:
                tds = row.find_all('td')
                if len(tds) < 6:
                    continue

                # Extract nid from link
                link = tds[1].find('a', href=True)
                if not link:
                    continue

                href = link.get('href', '')
                if 'nid=' not in href:
                    continue

                # Extract nid
                nid_str = href.split('nid=')[1].split('&')[0]
                try:
                    nid = int(nid_str)
                except ValueError:
                    continue

                # Extract date (Column 4: 25.10.31 format)
                date_str = tds[4].get_text(strip=True)

                report_list.append((nid, date_str))

            logger.info(f"Found {len(report_list)} reports on page {page}")
            return report_list

        except Exception as e:
            logger.error(f"Failed to get report list page {page}: {e}")
            return []

    async def get_today_reports(self, max_pages: int = 3) -> List[int]:
        """
        Get today's report IDs

        Args:
            max_pages: Maximum number of pages to check (default 3)

        Returns:
            List of report IDs (nid) published today
        """
        today = datetime.now().strftime('%y.%m.%d')  # Format: 25.10.31
        logger.info(f"Looking for reports published on {today}")

        today_reports = []

        for page in range(1, max_pages + 1):
            report_list = await self.get_report_list(page)

            if not report_list:
                break

            # Filter today's reports
            page_today = [nid for nid, date_str in report_list if date_str == today]
            today_reports.extend(page_today)

            # If no reports from today on this page, stop
            if not page_today:
                logger.info(f"No more reports from {today} found on page {page}")
                break

        logger.info(f"Total {len(today_reports)} reports found for {today}")
        return today_reports

    async def crawl_report_detail(self, nid: int) -> Optional[Dict]:
        """
        Crawl single report detail page

        Args:
            nid: Report ID from Naver Finance

        Returns:
            Dict with report data or None if failed
        """
        url = f"{self.base_url}/company_read.naver?nid={nid}&page=1"

        try:
            logger.info(f"Crawling report nid={nid}...")

            # Request with delay
            await asyncio.sleep(1)  # Rate limiting

            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"Failed to get report {nid}: Status {response.status}")
                    return None

                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

            # Parse HTML
            data = self._parse_report_page(soup, nid)

            if data:
                logger.info(f"Successfully parsed report nid={nid}: {data['title'][:30]}...")
                return data
            else:
                logger.warning(f"Failed to parse report nid={nid}")
                return None

        except aiohttp.ClientError as e:
            logger.error(f"Request failed for nid={nid}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for nid={nid}: {e}")
            return None

    def _parse_report_page(self, soup: BeautifulSoup, nid: int) -> Optional[Dict]:
        """
        Parse report detail page HTML

        Args:
            soup: BeautifulSoup object
            nid: Report ID

        Returns:
            Dict with extracted data
        """
        try:
            data = {'report_id': nid}

            # Find main table with class='type_1'
            table = soup.find('table', {'class': 'type_1'})
            if not table:
                logger.error("Cannot find table with class='type_1'")
                return None

            # Extract stock name from th.view_sbj > span > em
            view_sbj = table.find('th', {'class': 'view_sbj'})
            if view_sbj:
                # Stock name
                stock_em = view_sbj.find('em')
                if stock_em:
                    data['stock_name'] = stock_em.get_text(strip=True)

                # Title - get text after span, before p.source
                span_tag = view_sbj.find('span')
                source_tag = view_sbj.find('p', {'class': 'source'})

                if span_tag and source_tag:
                    # Get all text, remove stock name and source
                    full_text = view_sbj.get_text(strip=True)
                    stock_text = stock_em.get_text(strip=True) if stock_em else ''
                    source_text = source_tag.get_text(strip=True)

                    # Remove stock name and source to get title
                    title = full_text.replace(stock_text, '', 1).replace(source_text, '').strip()
                    data['title'] = title

                # Securities firm and date from p.source
                if source_tag:
                    source_text = source_tag.get_text(strip=True)
                    parts = source_text.split('|')
                    if len(parts) >= 2:
                        data['securities_firm'] = parts[0].strip()
                        data['publish_date'] = self._parse_date(parts[1].strip())

            # Extract target price and investment opinion from div.view_info_1
            view_info = table.find('div', {'class': 'view_info_1'})
            if view_info:
                # Target price
                money_em = view_info.find('em', {'class': 'money'})
                if money_em:
                    price_text = money_em.get_text(strip=True)
                    data['target_price'] = self._parse_price(price_text)

                # Investment opinion
                coment_em = view_info.find('em', {'class': 'coment'})
                if coment_em:
                    data['investment_opinion'] = coment_em.get_text(strip=True)

            # Extract summary from td.view_cnt
            view_cnt = table.find('td', {'class': 'view_cnt'})
            if view_cnt:
                # Get all text from the div inside
                content_div = view_cnt.find('div')
                if content_div:
                    summary_text = content_div.get_text(separator='\n', strip=True)
                    data['summary'] = summary_text[:5000]  # Limit length

            # Extract PDF URL if exists
            pdf_link = soup.find('a', href=re.compile(r'\.pdf$', re.IGNORECASE))
            if pdf_link:
                data['pdf_url'] = pdf_link.get('href', '')

            # Validate required fields
            if not data.get('title'):
                logger.warning("Title not found")
                return None

            return data

        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None

    def _parse_date(self, date_text: str) -> Optional[date]:
        """
        Parse date string to date object

        Args:
            date_text: Date string (e.g., "2025.10.31")

        Returns:
            datetime.date object or None
        """
        try:
            # Remove non-numeric except dots
            date_text = re.sub(r'[^\d\.]', '', date_text)

            # Parse various formats
            if '.' in date_text:
                parts = date_text.split('.')
                if len(parts) == 3:
                    year, month, day = parts
                    return date(int(year), int(month), int(day))

            return None

        except Exception as e:
            logger.warning(f"Date parse error: {date_text} - {e}")
            return None

    def _parse_price(self, price_text: str) -> Optional[float]:
        """
        Parse price string to float

        Args:
            price_text: Price string (e.g., "50,000원", "50000")

        Returns:
            Price as float or None
        """
        try:
            # Remove non-numeric except comma and dot
            price_text = re.sub(r'[^\d\,\.]', '', price_text)
            price_text = price_text.replace(',', '')

            if price_text:
                return float(price_text)

            return None

        except Exception as e:
            logger.warning(f"Price parse error: {price_text} - {e}")
            return None

    async def get_stock_code_from_name(self, stock_name: str) -> Optional[str]:
        """
        Get stock code from stock name using kr_stock_basic table

        Args:
            stock_name: Korean stock name

        Returns:
            6-digit stock code or None
        """
        try:
            async with self.db_pool.acquire() as conn:
                # Search for stock code
                row = await conn.fetchrow("""
                    SELECT symbol
                    FROM kr_stock_basic
                    WHERE stock_name = $1
                    LIMIT 1
                """, stock_name)

                if row:
                    return row['symbol']

                return None

        except Exception as e:
            logger.warning(f"Stock code lookup failed for '{stock_name}': {e}")
            return None

    async def save_to_database(self, data: Dict) -> bool:
        """
        Save report to database

        Args:
            data: Report data dict

        Returns:
            True if successful, False otherwise
        """
        try:
            async with self.db_pool.acquire() as conn:
                # Get stock code from name
                stock_code = None
                if data.get('stock_name'):
                    stock_code = await self.get_stock_code_from_name(data['stock_name'])

                # Insert or update
                query = """
                    INSERT INTO kr_research_reports (
                        report_id, stock_name, symbol, title, securities_firm,
                        date, target_price, investment_opinion, summary, pdf_url
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                    )
                    ON CONFLICT (report_id, securities_firm, date)
                    DO UPDATE SET
                        stock_name = EXCLUDED.stock_name,
                        symbol = EXCLUDED.symbol,
                        title = EXCLUDED.title,
                        target_price = EXCLUDED.target_price,
                        investment_opinion = EXCLUDED.investment_opinion,
                        summary = EXCLUDED.summary,
                        pdf_url = EXCLUDED.pdf_url,
                        updated_at = NOW()
                    RETURNING report_id;
                """

                report_id = await conn.fetchval(query,
                    data['report_id'],
                    data.get('stock_name'),
                    stock_code,
                    data.get('title'),
                    data.get('securities_firm'),
                    data.get('publish_date'),
                    data.get('target_price'),
                    data.get('investment_opinion'),
                    data.get('summary'),
                    data.get('pdf_url')
                )

                logger.info(f"[OK] Saved report {report_id} to database")
                return True

        except asyncpg.UniqueViolationError as e:
            logger.warning(f"Duplicate report: {e}")
            return False
        except Exception as e:
            logger.error(f"Database save error: {e}")
            return False

    async def run_daily_collection(self, max_pages: int = 3) -> Dict:
        """
        Run daily collection of research reports

        Args:
            max_pages: Maximum pages to check for today's reports

        Returns:
            Dict with collection results
        """
        logger.info("=" * 60)
        logger.info("Starting daily research report collection")
        logger.info("=" * 60)

        start_time = datetime.now()

        try:
            # Get today's reports
            logger.info("Step 1: Get today's report list...")
            today_nids = await self.get_today_reports(max_pages)

            if not today_nids:
                logger.info("No reports found for today")
                return {
                    "status": "success",
                    "message": "No reports found for today",
                    "total": 0,
                    "success": 0,
                    "failed": 0
                }

            logger.info(f"Found {len(today_nids)} reports published today")
            logger.info(f"Report IDs: {today_nids[:10]}{'...' if len(today_nids) > 10 else ''}")

            # Crawl each report
            logger.info("Step 2: Crawl and save reports...")
            success_count = 0
            fail_count = 0

            for i, nid in enumerate(today_nids, 1):
                logger.info(f"[{i}/{len(today_nids)}] Processing nid={nid}...")

                data = await self.crawl_report_detail(nid)
                if data:
                    success = await self.save_to_database(data)
                    if success:
                        logger.info(f"  OK - {data.get('stock_name', 'N/A')}")
                        success_count += 1
                    else:
                        logger.warning(f"  FAIL - Save failed")
                        fail_count += 1
                else:
                    logger.warning(f"  FAIL - Crawl failed")
                    fail_count += 1

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Summary
            logger.info("=" * 60)
            logger.info("SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Total reports: {len(today_nids)}")
            logger.info(f"Success: {success_count}")
            logger.info(f"Failed: {fail_count}")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info("=" * 60)

            return {
                "status": "success",
                "message": f"Collected {success_count}/{len(today_nids)} reports",
                "total": len(today_nids),
                "success": success_count,
                "failed": fail_count,
                "duration_seconds": duration
            }

        except Exception as e:
            logger.error(f"Collection failed: {e}", exc_info=True)
            raise


# Standalone execution for testing
if __name__ == '__main__':
    import asyncio

    async def main():
        """Test execution"""
        print("=" * 80)
        print("Naver Research Report Crawler - Today's Reports (Async)")
        print("=" * 80)
        print()

        # Get database URL from env
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            print("ERROR: DATABASE_URL not configured")
            return

        # Initialize crawler
        crawler = NaverResearchCrawler(database_url)
        await crawler.initialize()

        try:
            # Run collection
            result = await crawler.run_daily_collection(max_pages=3)

            print()
            print("=" * 80)
            print("RESULT")
            print("=" * 80)
            print(f"Status: {result['status']}")
            print(f"Message: {result['message']}")
            print(f"Total: {result['total']}")
            print(f"Success: {result['success']}")
            print(f"Failed: {result['failed']}")
            print("=" * 80)

        finally:
            await crawler.close()

    asyncio.run(main())
