"""
  ETF   
AlphaVantage TIME_SERIES_DAILY API 

 : 2025-01-01 ~ 
    skip
"""

import requests
import asyncpg
import asyncio
import os
import json
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from pathlib import Path

# .env  
load_dotenv()

#   
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL   .")

#    (Railway Volume path support)
if os.getenv('RAILWAY_ENVIRONMENT'):
    LOG_FILE = Path('/app/log/us_etf_collection.log')
else:
    LOG_FILE = Path('log/us_etf_collection.log')

# ETF   ( 11 +  38 =  49)
SECTOR_ETFS = {
    # ===  11  ETF ===
    'Technology': 'XLK',
    'Healthcare': 'XLV',
    'Financials': 'XLF',
    'Consumer Discretionary': 'XLY',
    'Industrials': 'XLI',
    'Consumer Staples': 'XLP',
    'Energy': 'XLE',
    'Utilities': 'XLU',
    'Materials': 'XLB',
    'Real Estate': 'XLRE',
    'Communication Services': 'XLC',

    # ===  38 ETF ===
    # Biotech (2)
    'Biotech - XBI': 'XBI',
    'Biotech - IBB': 'IBB',

    # Market Index (5)
    'Market Index - SPY': 'SPY',
    'Market Index - QQQ': 'QQQ',
    'Market Index - IWM': 'IWM',
    'Market Index - DIA': 'DIA',
    'Market Index - VTI': 'VTI',

    # Semiconductors (2)
    'Semiconductors - SMH': 'SMH',
    'Semiconductors - SOXX': 'SOXX',

    # Mid Cap (2)
    'Mid Cap - MDY': 'MDY',
    'Mid Cap - IJH': 'IJH',

    # Growth (2)
    'Growth - VUG': 'VUG',
    'Growth - IWF': 'IWF',

    # Value (2)
    'Value - VTV': 'VTV',
    'Value - IWD': 'IWD',

    # Volatility (2)
    'Volatility - VXX': 'VXX',
    'Volatility - UVXY': 'UVXY',

    # Bonds (5)
    'Bonds - TLT': 'TLT',
    'Bonds - SHY': 'SHY',
    'Bonds - HYG': 'HYG',
    'Bonds - LQD': 'LQD',
    'Bonds - AGG': 'AGG',

    # Commodity (5)
    'Commodity - GLD': 'GLD',
    'Commodity - SLV': 'SLV',
    'Commodity - USO': 'USO',
    'Commodity - DBA': 'DBA',
    'Commodity - UNG': 'UNG',

    # International (5)
    'International - EEM': 'EEM',
    'International - EFA': 'EFA',
    'International - VWO': 'VWO',
    'International - FXI': 'FXI',
    'International - EWJ': 'EWJ',

    # Financial Subsector (1)
    'Financial Subsector - KRE': 'KRE',

    # Thematic (1)
    'Thematic - ARKK': 'ARKK',

    # Software (1)
    'Software - IGV': 'IGV',

    # Healthcare Subsector (1)
    'Healthcare Subsector - IHI': 'IHI',

    # Leveraged (2)
    'Leveraged - TQQQ': 'TQQQ',
    'Inverse - SQQQ': 'SQQQ',
}

#  :    1  (get_latest_business_day() )
# COLLECTION_START_DATE, COLLECTION_END_DATE     (  )


class CollectionLogger:
    """   """

    def __init__(self, log_file: Path):
        self.log_file = log_file
        self.log_data = self._load_log()

    def _load_log(self) -> Dict:
        """  """
        if self.log_file.exists():
            try:
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"    : {e}")
                return {}
        return {}

    def _save_log(self):
        """  """
        try:
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(self.log_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"    : {e}")

    def is_collected(self, symbol: str) -> bool:
        """    (  )"""
        if symbol not in self.log_data:
            return False

        log_entry = self.log_data[symbol]
        if log_entry.get('status') != 'completed':
            return False

        #       
        last_collected_date = log_entry.get('last_date')
        if last_collected_date:
            try:
                last_date = datetime.fromisoformat(last_collected_date).date() if isinstance(last_collected_date, str) else last_collected_date
                today = date.today()

                #     
                if last_date != today:
                    return False
            except:
                return False

        return True

    def get_last_collection_date(self, symbol: str) -> Optional[datetime]:
        """   """
        if symbol in self.log_data and 'last_date' in self.log_data[symbol]:
            return datetime.fromisoformat(self.log_data[symbol]['last_date'])
        return None

    def mark_completed(self, symbol: str, record_count: int, last_date: str):
        """  """
        self.log_data[symbol] = {
            'status': 'completed',
            'collected_at': datetime.now().isoformat(),
            'record_count': record_count,
            'last_date': last_date
        }
        self._save_log()

    def mark_failed(self, symbol: str, error: str):
        """  """
        self.log_data[symbol] = {
            'status': 'failed',
            'failed_at': datetime.now().isoformat(),
            'error': error
        }
        self._save_log()

    def get_summary(self) -> Dict:
        """  """
        completed = sum(1 for v in self.log_data.values() if v.get('status') == 'completed')
        failed = sum(1 for v in self.log_data.values() if v.get('status') == 'failed')

        return {
            'total': len(self.log_data),
            'completed': completed,
            'failed': failed
        }


class USETFDataCollector:
    """  ETF  """

    def __init__(self, start_date: date = None, end_date: date = None):
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY   .")

        self.base_url = "https://www.alphavantage.co/query"
        self.connection_pool = None
        self.logger = CollectionLogger(LOG_FILE)

        #  None
        if start_date is None:
            self.start_date = self.get_latest_business_day()
        else:
            self.start_date = start_date

        if end_date is None:
            self.end_date = self.start_date
        else:
            self.end_date = end_date

    def get_latest_business_day(self) -> date:
        """    ( )"""
        today = date.today()
        #  3   
        for days_back in range(4):
            check_date = today - timedelta(days=days_back)
            if check_date.weekday() < 5:  # Monday to Friday
                return check_date
        # Fallback: return today
        return today

    async def initialize(self):
        """  """
        # asyncpg postgresql://  postgres://  
        db_url = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
        self.connection_pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3)
        print("OK -   ")

    async def close(self):
        """ """
        if self.connection_pool:
            await self.connection_pool.close()

    def fetch_etf_daily_data(self, symbol: str) -> Dict:
        """
        AlphaVantage TIME_SERIES_DAILY API 

        Args:
            symbol: ETF  (: XLK)

        Returns:
            JSON  
        """
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'outputsize': 'full',  # 20+  
            'apikey': self.api_key
        }

        try:
            print(f" [{symbol}] API  ...")
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # API  
            if 'Error Message' in data:
                error_msg = data['Error Message']
                print(f" [{symbol}] API : {error_msg}")
                return {}

            if 'Note' in data:
                note_msg = data['Note']
                print(f" [{symbol}] API : {note_msg}")
                return {}

            return data

        except Exception as e:
            print(f" [{symbol}] API  : {e}")
            return {}

    def filter_by_date(self, time_series_data: Dict) -> Dict:
        """
        API start_date ~ end_date

        Args:
            time_series_data: AlphaVantage  "Time Series (Daily)"

        Returns:

        """
        if not time_series_data:
            return {}

        filtered = {}

        for date_str, ohlcv in time_series_data.items():
            try:
                parsed_date = datetime.strptime(date_str, '%Y-%m-%d').date()

                # start_date ~ end_date
                if self.start_date <= parsed_date <= self.end_date:
                    filtered[date_str] = ohlcv
            except Exception as e:
                print(f"   : {date_str} - {e}")
                continue

        if filtered:
            dates = sorted(filtered.keys())
            print(f"   -  : {dates[0]} ~ {dates[-1]} ({len(filtered)})")

        return filtered

    async def save_to_database(self, symbol: str, time_series_data: Dict) -> Tuple[int, Optional[str]]:
        """
        ETF  us_daily_etf  

        Args:
            symbol: ETF 
            time_series_data: AlphaVantage  "Time Series (Daily)" 

        Returns:
            (  ,  )
        """
        if not time_series_data:
            return 0, None

        insert_count = 0
        update_count = 0
        last_date = None

        # UPSERT 
        query = """
        INSERT INTO us_daily_etf (symbol, date, open, high, low, close, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (symbol, date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            updated_at = CURRENT_TIMESTAMP
        RETURNING (xmax = 0) AS inserted
        """

        for date_str, ohlcv in time_series_data.items():
            try:
                date = datetime.strptime(date_str, '%Y-%m-%d').date()

                open_price = float(ohlcv['1. open'])
                high_price = float(ohlcv['2. high'])
                low_price = float(ohlcv['3. low'])
                close_price = float(ohlcv['4. close'])
                volume = int(ohlcv['5. volume'])

                #     
                async with self.connection_pool.acquire() as conn:
                    result = await conn.fetchrow(query, symbol, date, open_price, high_price,
                                                 low_price, close_price, volume)

                    if result['inserted']:
                        insert_count += 1
                    else:
                        update_count += 1

                #   
                if last_date is None or date > last_date:
                    last_date = date

            except Exception as e:
                print(f" [{symbol}] {date_str}  : {e}")
                continue

        total_count = insert_count + update_count
        print(f"OK - [{symbol}]   - : {insert_count}, : {update_count} ( {total_count})")

        return total_count, last_date.isoformat() if last_date else None

    async def collect_etf_data(self, symbol: str, sector: str, force: bool = False):
        """
         ETF    

        Args:
            symbol: ETF 
            sector:  
            force:     
        """
        print(f"\n{'='*60}")
        print(f" [{sector}] {symbol} ETF  ")
        print(f"{'='*60}")

        #    skip
        if not force and self.logger.is_collected(symbol):
            last_collection = self.logger.get_last_collection_date(symbol)
            print(f"OK - [{symbol}]    ( : {last_collection.strftime('%Y-%m-%d %H:%M:%S')})")
            return

        try:
            # API 
            data = self.fetch_etf_daily_data(symbol)

            if not data or 'Time Series (Daily)' not in data:
                error_msg = " "
                print(f" [{symbol}] {error_msg}")
                self.logger.mark_failed(symbol, error_msg)
                return

            meta_data = data.get('Meta Data', {})
            time_series = data['Time Series (Daily)']

            print(f" [{symbol}]  :")
            print(f"   - : {meta_data.get('2. Symbol')}")
            print(f"   -  : {meta_data.get('3. Last Refreshed')}")
            print(f"   -  : {len(time_series)}")

            # API    1 
            filtered_data = self.filter_by_date(time_series)
            print(f"   -  : {len(filtered_data)} ( )")

            if not filtered_data:
                error_msg = "   "
                print(f" [{symbol}] {error_msg}")
                self.logger.mark_failed(symbol, error_msg)
                return

            #  
            record_count, last_date = await self.save_to_database(symbol, filtered_data)

            #  
            self.logger.mark_completed(symbol, record_count, last_date)

        except Exception as e:
            error_msg = str(e)
            print(f" [{symbol}]  : {error_msg}")
            self.logger.mark_failed(symbol, error_msg)

    async def collect_all_sector_etfs(self, force: bool = False):
        """
          ETF   

        Args:
            force:     
        """
        print(f"   ETF    ({len(SECTOR_ETFS)})")
        print(f"  : {self.start_date.strftime('%Y-%m-%d')} (  1)")
        print(f"  : {LOG_FILE}")

        #  
        summary = self.logger.get_summary()
        if summary['total'] > 0:
            print(f"\n   :")
            print(f"   - : {summary['completed']}")
            print(f"   - : {summary['failed']}")
            print(f"   - : {summary['total']}")

        #  ETF 
        uncollected_etfs = [
            (sector, symbol) for sector, symbol in SECTOR_ETFS.items()
            if force or not self.logger.is_collected(symbol)
        ]

        skipped_count = len(SECTOR_ETFS) - len(uncollected_etfs)

        print(f"\n  ETF: {len(SECTOR_ETFS)}")
        print(f"OK -  : {skipped_count} ( skip)")
        print(f"  : {len(uncollected_etfs)}")

        #  ETF 
        for i, (sector, symbol) in enumerate(uncollected_etfs, 1):
            print(f"\n [{i}/{len(uncollected_etfs)}]")
            await self.collect_etf_data(symbol, sector, force)

            #    API Rate Limit 
            if i < len(uncollected_etfs):
                # AlphaVantage : 50 calls/min, 500 calls/day
                print("⏱ API Rate Limit  (1.2)...")
                await asyncio.sleep(1.2)

        print(f"\n{'='*60}")
        print(f"   ETF   !")
        print(f"{'='*60}")

        #  
        final_summary = self.logger.get_summary()
        print(f"\n  :")
        print(f"   - : {final_summary['completed']}")
        print(f"   - : {final_summary['failed']}")
        print(f"   - : {final_summary['total']}")

    async def get_latest_etf_prices(self) -> Dict[str, Dict]:
        """ ETF   """
        query = """
        SELECT DISTINCT ON (symbol)
            symbol,
            date,
            close
        FROM us_daily_etf
        ORDER BY symbol, date DESC
        """

        async with self.connection_pool.acquire() as conn:
            rows = await conn.fetch(query)

            result = {}
            for row in rows:
                result[row['symbol']] = {
                    'close': float(row['close']),
                    'date': row['date'].isoformat()
                }

            return result

    async def calculate_sector_performance(self, days: int = 20) -> Dict[str, Dict]:
        """
         ETF  

        Args:
            days:    ( 20)

        Returns:
               
        """
        performance = {}

        for sector, symbol in SECTOR_ETFS.items():
            query = f"""
            WITH price_data AS (
                SELECT
                    date,
                    close,
                    LAG(close, {days}) OVER (ORDER BY date) AS close_prev
                FROM us_daily_etf
                WHERE symbol = $1
                ORDER BY date DESC
                LIMIT 1
            )
            SELECT
                close,
                close_prev,
                CASE
                    WHEN close_prev IS NOT NULL AND close_prev > 0
                    THEN ((close - close_prev) / close_prev * 100)
                    ELSE NULL
                END AS return_pct
            FROM price_data
            """

            async with self.connection_pool.acquire() as conn:
                row = await conn.fetchrow(query, symbol)

                if row and row['return_pct'] is not None:
                    performance[sector] = {
                        'symbol': symbol,
                        'current_price': float(row['close']),
                        'return_pct': float(row['return_pct']),
                        'period_days': days
                    }

        return performance


async def main():
    """  """
    collector = USETFDataCollector()

    try:
        await collector.initialize()

        #   ETF   (force=False:    skip)
        await collector.collect_all_sector_etfs(force=False)

        #   
        print("\n" + "="*60)
        print("  ETF   ")
        print("="*60)

        latest_prices = await collector.get_latest_etf_prices()
        for symbol, info in sorted(latest_prices.items()):
            print(f"{symbol}: ${info['close']:.2f} ({info['date']})")

        #    (20 )
        print("\n" + "="*60)
        print("   ( 20)")
        print("="*60)

        performance = await collector.calculate_sector_performance(days=20)
        for sector, data in sorted(performance.items(), key=lambda x: x[1]['return_pct'], reverse=True):
            print(f"{sector:25s} ({data['symbol']}): {data['return_pct']:+6.2f}%")

    except Exception as e:
        print(f"  : {e}")
        import traceback
        traceback.print_exc()

    finally:
        await collector.close()


if __name__ == "__main__":
    asyncio.run(main())
