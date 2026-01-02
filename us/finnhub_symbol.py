# alpha/data/us/finnhub_symbol.py
"""
Finnhub Symbol Collector
Collects NYSE and NASDAQ common stock symbols from Finnhub API
and stores them in PostgreSQL database
"""
import requests
import asyncpg
import logging
import os
from datetime import datetime
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# Setup logging - Use Railway Volume (/app/log) if available
if os.getenv('RAILWAY_ENVIRONMENT'):
    log_dir = Path('/app/log')
else:
    log_dir = Path(__file__).parent.parent / 'log'

log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'finnhub_symbol.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FinnhubSymbolCollector:
    def __init__(self, api_key: str, database_url: str):
        self.api_key = api_key
        self.database_url = database_url
        self.base_url = 'https://finnhub.io/api/v1'

    async def get_connection(self):
        """Get PostgreSQL database connection"""
        return await asyncpg.connect(self.database_url)

    async def create_table_if_not_exists(self, conn):
        """Create us_symbol table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS us_symbol (
            symbol VARCHAR(20) PRIMARY KEY,
            display_symbol VARCHAR(20),
            description VARCHAR(255),
            figi VARCHAR(20),
            mic VARCHAR(10),
            currency VARCHAR(10),
            type VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_us_symbol_mic ON us_symbol(mic);
        CREATE INDEX IF NOT EXISTS idx_us_symbol_type ON us_symbol(type);
        """

        await conn.execute(create_table_sql)
        logger.info("Table us_symbol created or already exists")

    def fetch_symbols(self, exchange: str = 'US', mic: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch stock symbols from Finnhub API"""
        url = f"{self.base_url}/stock/symbol"

        params = {
            'exchange': exchange,
            'token': self.api_key
        }

        if mic:
            params['mic'] = mic

        try:
            logger.info(f"Fetching symbols from Finnhub API (exchange={exchange}, mic={mic})")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            symbols = response.json()
            logger.info(f"Fetched {len(symbols)} symbols")
            return symbols

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch symbols: {str(e)}")
            return []

    def filter_common_stocks(self, symbols: List[Dict[str, Any]], target_mics: List[str]) -> List[Dict[str, Any]]:
        """Filter for common stocks from specified MIC codes"""
        filtered = []

        for symbol in symbols:
            mic = symbol.get('mic', '')
            symbol_type = symbol.get('type', '')

            # Filter: must be in target MICs and type must be 'Common Stock'
            if mic in target_mics and symbol_type == 'Common Stock':
                filtered.append(symbol)

        logger.info(f"Filtered to {len(filtered)} common stocks from {len(symbols)} total")
        return filtered

    async def upsert_symbols(self, conn, symbols: List[Dict[str, Any]]) -> int:
        """Insert or update symbols using executemany (fallback method)"""
        if not symbols:
            logger.warning("No symbols to insert")
            return 0

        upsert_sql = """
        INSERT INTO us_symbol (
            symbol, display_symbol, description, figi, mic, currency, type, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (symbol)
        DO UPDATE SET
            display_symbol = EXCLUDED.display_symbol,
            description = EXCLUDED.description,
            figi = EXCLUDED.figi,
            mic = EXCLUDED.mic,
            currency = EXCLUDED.currency,
            type = EXCLUDED.type,
            updated_at = CURRENT_TIMESTAMP
        """

        try:
            # Prepare batch values
            batch_values = [
                (
                    symbol_data.get('symbol', ''),
                    symbol_data.get('displaySymbol', ''),
                    symbol_data.get('description', ''),
                    symbol_data.get('figi', ''),
                    symbol_data.get('mic', ''),
                    symbol_data.get('currency', ''),
                    symbol_data.get('type', '')
                )
                for symbol_data in symbols
            ]

            async with conn.transaction():
                await conn.executemany(upsert_sql, batch_values)

            logger.info(f"Upserted {len(batch_values)} symbols using executemany")
            return len(batch_values)

        except Exception as e:
            logger.error(f"Failed to executemany insert symbols: {str(e)}")
            raise

    async def upsert_symbols_optimized(self, conn, symbols: List[Dict[str, Any]]) -> int:
        """Insert or update symbols using COPY command - OPTIMIZED"""
        if not symbols:
            logger.warning("No symbols to insert")
            return 0

        # Prepare data for bulk insert
        records = [
            (
                symbol_data.get('symbol', ''),
                symbol_data.get('displaySymbol', ''),
                symbol_data.get('description', ''),
                symbol_data.get('figi', ''),
                symbol_data.get('mic', ''),
                symbol_data.get('currency', ''),
                symbol_data.get('type', '')
            )
            for symbol_data in symbols
        ]

        # Use temporary table for bulk upsert
        try:
            async with conn.transaction():
                # Transaction optimization
                await conn.execute('SET LOCAL synchronous_commit = OFF')
                await conn.execute('SET LOCAL work_mem = "256MB"')

                # Create temporary table
                await conn.execute("""
                    CREATE TEMP TABLE temp_symbols (
                        symbol VARCHAR(20),
                        display_symbol VARCHAR(20),
                        description VARCHAR(255),
                        figi VARCHAR(20),
                        mic VARCHAR(10),
                        currency VARCHAR(10),
                        type VARCHAR(50)
                    ) ON COMMIT DROP
                """)

                # Bulk copy to temporary table
                await conn.copy_records_to_table(
                    'temp_symbols',
                    records=records,
                    columns=['symbol', 'display_symbol', 'description', 'figi', 'mic', 'currency', 'type']
                )

                # Upsert from temporary table
                await conn.execute("""
                    INSERT INTO us_symbol (
                        symbol, display_symbol, description, figi, mic, currency, type,
                        created_at, updated_at
                    )
                    SELECT
                        symbol, display_symbol, description, figi, mic, currency, type,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    FROM temp_symbols
                    ON CONFLICT (symbol)
                    DO UPDATE SET
                        display_symbol = EXCLUDED.display_symbol,
                        description = EXCLUDED.description,
                        figi = EXCLUDED.figi,
                        mic = EXCLUDED.mic,
                        currency = EXCLUDED.currency,
                        type = EXCLUDED.type,
                        updated_at = CURRENT_TIMESTAMP
                """)

            logger.info(f"Bulk upserted {len(records)} symbols using COPY (OPTIMIZED)")
            return len(records)

        except Exception as e:
            logger.error(f"Failed to COPY insert symbols: {str(e)}")
            logger.info("Falling back to executemany method")
            return await self.upsert_symbols(conn, symbols)

    async def get_statistics(self, conn) -> Dict[str, int]:
        """Get statistics from us_symbol table"""
        stats = {}

        # Total count
        total = await conn.fetchval("SELECT COUNT(*) FROM us_symbol")
        stats['total'] = total

        # Count by MIC
        mic_counts = await conn.fetch("""
            SELECT mic, COUNT(*) as count
            FROM us_symbol
            GROUP BY mic
            ORDER BY count DESC
        """)

        for row in mic_counts:
            stats[f"mic_{row['mic']}"] = row['count']

        # Count by type
        type_counts = await conn.fetch("""
            SELECT type, COUNT(*) as count
            FROM us_symbol
            GROUP BY type
            ORDER BY count DESC
        """)

        for row in type_counts:
            stats[f"type_{row['type']}"] = row['count']

        return stats

    async def run(self):
        """Main execution"""
        logger.info("=" * 60)
        logger.info("Starting Finnhub Symbol Collection")
        logger.info(f"Timestamp: {datetime.now()}")
        logger.info("=" * 60)

        # Target exchanges
        target_mics = ['XNYS', 'XNAS']  # NYSE and NASDAQ

        try:
            # Fetch all US symbols
            all_symbols = self.fetch_symbols(exchange='US')

            if not all_symbols:
                logger.error("No symbols fetched from API")
                return

            # Filter for NYSE and NASDAQ common stocks
            common_stocks = self.filter_common_stocks(all_symbols, target_mics)

            if not common_stocks:
                logger.error("No common stocks found after filtering")
                return

            # Display sample
            logger.info("\nSample symbols:")
            for symbol in common_stocks[:5]:
                logger.info(f"  {symbol['symbol']}: {symbol['description']} ({symbol['mic']})")

            # Connect to database
            conn = await self.get_connection()

            try:
                # Create table if not exists
                await self.create_table_if_not_exists(conn)

                # Insert/update symbols (OPTIMIZED with fallback)
                inserted = await self.upsert_symbols_optimized(conn, common_stocks)

                # Get statistics
                stats = await self.get_statistics(conn)

                logger.info("\n" + "=" * 60)
                logger.info("Collection Completed Successfully")
                logger.info("=" * 60)
                logger.info(f"Total symbols in database: {stats.get('total', 0)}")
                logger.info(f"NYSE (XNYS): {stats.get('mic_XNYS', 0)}")
                logger.info(f"NASDAQ (XNAS): {stats.get('mic_XNAS', 0)}")
                logger.info(f"Common Stock type: {stats.get('type_Common Stock', 0)}")

            finally:
                await conn.close()
                logger.info("Database connection closed")

        except Exception as e:
            logger.error(f"Error during execution: {str(e)}", exc_info=True)
            raise

async def main():
    """Entry point"""
    api_key = os.getenv('FINNHUB_API_KEY')
    database_url = os.getenv('DATABASE_URL')

    if not api_key:
        raise ValueError("FINNHUB_API_KEY environment variable is required")

    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    collector = FinnhubSymbolCollector(api_key, database_url)
    await collector.run()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
