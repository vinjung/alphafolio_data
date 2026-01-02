# alpha/data/kr/kr_calculator.py
import asyncpg
import pandas as pd
import numpy as np
import logging
import os
import asyncio
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging path (Railway Volume support)
if os.getenv('RAILWAY_ENVIRONMENT'):
    log_path = '/app/log/kr_calculator.log'
else:
    from pathlib import Path
    log_path = str(Path(__file__).parent.parent / 'log' / 'kr_calculator.log')

# Configure logging - Optimized (WARNING level for performance)
logging.basicConfig(
    level=logging.WARNING,  # Changed from INFO to WARNING for 5-10% performance gain
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Enable INFO logs for batch progress only
batch_logger = logging.getLogger(__name__ + '.batch')
batch_logger.setLevel(logging.INFO)

class KoreanTechnicalIndicatorCalculator:
    def __init__(self, database_url: str):
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        self.execution_date = date.today()  # Current execution date
        self.pool = None
        # Memory cache for frequently accessed data
        self.ohlcv_cache = {}  # Cache for historical OHLCV data
        self.cache_max_size = 1000  # Limit cache size to prevent memory issues
        # Semaphore for symbol-level concurrency control
        self.symbol_semaphore = None  # Initialize in init_pool

    async def init_pool(self):
        """Initialize connection pool with optimized settings"""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=20,  # Increased from 10 for better connection availability
            max_size=100,  # Maximum connection pool size
            command_timeout=120,  # Command timeout
            max_inactive_connection_lifetime=300
        )
        # Semaphore removed for smart batch approach - let DB pool handle concurrency
        self.symbol_semaphore = None
        logger.info(f"Database pool initialized: min_size=20, max_size=100, semaphore=disabled")

    async def get_connection(self):
        if self.pool:
            # Add retry logic for connection acquisition
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    return await self.pool.acquire()
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Failed to acquire connection after {max_retries} attempts: {e}")
                        raise
                    await asyncio.sleep(1)  # Wait 1 second before retry
        else:
            return await asyncpg.connect(self.database_url)

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()

    # Progress file management functions
    def get_progress_file_path(self, execution_date: date) -> str:
        """Get progress file path for given execution date"""
        date_str = execution_date.strftime('%Y%m%d')

        # Use Railway Volume if available
        if os.getenv('RAILWAY_ENVIRONMENT'):
            log_dir = '/app/log'
        else:
            from pathlib import Path
            log_dir = str(Path(__file__).parent.parent / 'log')

        return f"{log_dir}/kr_indicators_progress_{date_str}.txt"

    def load_completed_symbols(self, execution_date: date) -> set:
        """Load completed symbols from progress file"""
        progress_file = self.get_progress_file_path(execution_date)
        completed_symbols = set()

        try:
            if os.path.exists(progress_file):
                with open(progress_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        symbol = line.strip()
                        if symbol:
                            completed_symbols.add(symbol)
                logger.info(f"Loaded {len(completed_symbols)} completed symbols from {progress_file}")
            else:
                logger.info(f"Progress file not found: {progress_file}")
        except Exception as e:
            logger.warning(f"Failed to load progress file {progress_file}: {e}")

        return completed_symbols

    def save_progress(self, execution_date: date, symbol: str):
        """Save progress for a completed symbol"""
        progress_file = self.get_progress_file_path(execution_date)

        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(progress_file), exist_ok=True)

            # Append symbol to progress file
            with open(progress_file, 'a', encoding='utf-8') as f:
                f.write(f"{symbol}\n")
        except Exception as e:
            logger.warning(f"Failed to save progress for {symbol}: {e}")

    # Historical data query functions for incremental calculations
    async def get_previous_vwap_data(self, symbol: str, target_date: date) -> Optional[Dict]:
        """Get previous VWAP data for incremental calculation"""
        conn = None
        try:
            conn = await self.get_connection()
            # Get VWAP data from regular table
            query = """
                SELECT vwap, eod_vwap, avg_vwap, price_vs_vwap
                FROM kr_indicators
                WHERE symbol = $1 AND date = $2
            """
            row = await conn.fetchrow(query, symbol, target_date)

            if row:
                return {
                    'vwap': row['vwap'],
                    'eod_vwap': row['eod_vwap'],
                    'avg_vwap': row['avg_vwap'],
                    'price_vs_vwap': row['price_vs_vwap']
                }
            return None

        except Exception as e:
            logger.warning(f"Failed to get previous VWAP data for {symbol} on {target_date}: {e}")
            return None
        finally:
            if conn:
                await conn.close()

    async def get_previous_obv(self, symbol: str, target_date: date) -> Optional[float]:
        """Get previous OBV for incremental calculation"""
        conn = None
        try:
            conn = await self.get_connection()
            # Get OBV data from regular table
            query = "SELECT obv FROM kr_indicators WHERE symbol = $1 AND date = $2"
            obv_value = await conn.fetchval(query, symbol, target_date)
            return obv_value

        except Exception as e:
            logger.warning(f"Failed to get previous OBV for {symbol} on {target_date}: {e}")
            return None
        finally:
            if conn:
                await conn.close()

    async def get_historical_closes(self, symbol: str, target_date: date, days: int) -> List[float]:
        """Get historical close prices for rolling calculations"""
        try:
            conn = await self.get_connection()

            # Get data from kr_intraday_total table (non-partitioned)
            query = """
                SELECT close, date
                FROM kr_intraday_total
                WHERE symbol = $1 AND date < $2
                ORDER BY date DESC
                LIMIT $3
            """
            rows = await conn.fetch(query, symbol, target_date, days)

            await conn.close()

            if not rows:
                logger.warning(f"No historical close prices found for {symbol} before {target_date}")
                return []

            # Extract closes and return in chronological order (oldest first)
            closes = [float(row['close']) for row in rows if row['close'] is not None]
            return list(reversed(closes))

        except Exception as e:
            logger.warning(f"Failed to get historical closes for {symbol}: {e}")
            return []

    async def get_today_intraday_data(self, symbol: str, target_date: date) -> Optional[Dict]:
        """Get today's OHLCV data for calculations"""
        try:
            conn = await self.get_connection()
            # Get today's OHLCV data from regular table
            query = """
                SELECT date, open, high, low, close, volume, stock_name
                FROM kr_intraday_total
                WHERE symbol = $1 AND date = $2
            """
            row = await conn.fetchrow(query, symbol, target_date)
            await conn.close()

            if row:
                return {
                    'date': row['date'],
                    'open': float(row['open']) if row['open'] is not None else None,
                    'high': float(row['high']) if row['high'] is not None else None,
                    'low': float(row['low']) if row['low'] is not None else None,
                    'close': float(row['close']) if row['close'] is not None else None,
                    'volume': float(row['volume']) if row['volume'] is not None else None,
                    'stock_name': row['stock_name']
                }
            return None

        except Exception as e:
            logger.warning(f"Failed to get today's data for {symbol} on {target_date}: {e}")
            return None

    async def get_historical_ohlcv_data(self, symbol: str, target_date: date, days: int) -> List[Dict]:
        """Get historical OHLCV data with caching for performance"""
        try:
            # Check cache first
            cache_key = f"{symbol}_{target_date}_{days}"
            if cache_key in self.ohlcv_cache:
                return self.ohlcv_cache[cache_key]

            conn = await self.get_connection()

            # Get historical data from kr_intraday_total table
            query = """
                SELECT date, open, high, low, close, volume, stock_name
                FROM kr_intraday_total
                WHERE symbol = $1 AND date < $2
                ORDER BY date DESC
                LIMIT $3
            """
            rows = await conn.fetch(query, symbol, target_date, days)

            await conn.close()

            if not rows:
                logger.warning(f"No historical OHLCV data found for {symbol} before {target_date}")
                return []

            all_data = []
            for row in rows:
                if all(val is not None for val in [row['open'], row['high'], row['low'], row['close'], row['volume']]):
                    all_data.append({
                        'date': row['date'],
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'volume': float(row['volume']),
                        'stock_name': row['stock_name']
                    })

            # Return in chronological order (oldest first)
            result = list(reversed(all_data))

            # Cache the result (with size limit)
            if len(self.ohlcv_cache) < self.cache_max_size:
                self.ohlcv_cache[cache_key] = result
            elif cache_key not in self.ohlcv_cache:
                # Remove oldest cache entry to make space
                oldest_key = next(iter(self.ohlcv_cache))
                del self.ohlcv_cache[oldest_key]
                self.ohlcv_cache[cache_key] = result

            return result

        except Exception as e:
            logger.warning(f"Failed to get historical OHLCV data for {symbol}: {e}")
            return []

    async def check_missing_historical_data(self, symbol: str, target_date: date) -> List[str]:
        """Check what historical indicator data is missing"""
        missing_indicators = []
        from datetime import timedelta
        yesterday = target_date - timedelta(days=1)

        try:
            # Check VWAP data
            prev_vwap_data = await self.get_previous_vwap_data(symbol, yesterday)
            if not prev_vwap_data or prev_vwap_data.get('vwap') is None:
                missing_indicators.append('vwap')

            # Check OBV data
            prev_obv = await self.get_previous_obv(symbol, yesterday)
            if prev_obv is None:
                missing_indicators.append('obv')

            # Check if we have enough historical indicators for RSI, MACD, etc.
            historical_closes = await self.get_historical_closes(symbol, target_date, 50)
            if len(historical_closes) < 14:
                missing_indicators.append('basic_indicators')

        except Exception as e:
            logger.warning(f"Error checking missing historical data for {symbol}: {e}")
            missing_indicators = ['all']  # Force full calculation

        return missing_indicators

    async def calculate_missing_historical_indicators(self, symbol: str, target_date: date) -> bool:
        """Calculate missing historical indicators and save to database"""
        try:
            logger.info(f"Calculating missing historical indicators for {symbol}")

            # Get comprehensive historical data (100 days should be enough for most indicators)
            historical_data = await self.get_historical_ohlcv_data(symbol, target_date, 100)

            if len(historical_data) < 20:  # Need at least 20 days for meaningful calculations
                logger.warning(f"Insufficient historical data for {symbol}: {len(historical_data)} days")
                return False

            # Calculate indicators for each historical day
            indicators_calculated = 0

            for i in range(14, len(historical_data)):  # Start from day 14 to have enough data for RSI
                current_date = historical_data[i]['date']
                current_data = historical_data[i]

                # Check if indicators already exist for this date
                if await self.check_daily_indicators_exist(symbol, current_date):
                    continue

                # Get previous days for calculations
                previous_data = historical_data[:i]  # All previous days

                # Calculate indicators for this historical day
                indicators_data = {}

                # 1. Basic indicators (RSI, MACD, Bollinger Bands, etc.)
                if len(previous_data) >= 14:
                    basic_indicators = self.calculate_single_date_indicators(previous_data, current_data)
                    indicators_data.update(basic_indicators)

                # 2. VWAP calculation
                if i > 0:  # Need at least one previous day
                    prev_day_data = historical_data[i-1]

                    # For first day, initialize VWAP
                    if i == 1:
                        vwap = (current_data['high'] + current_data['low'] + current_data['close']) / 3
                        indicators_data.update({
                            'vwap': vwap,
                            'eod_vwap': current_data['close'],
                            'avg_vwap': vwap,
                            'price_vs_vwap': 0.0
                        })
                    else:
                        # Get previous VWAP from database
                        from datetime import timedelta
                        prev_date = current_date - timedelta(days=1)
                        prev_vwap_data = await self.get_previous_vwap_data(symbol, prev_date)

                        if prev_vwap_data and prev_vwap_data.get('vwap'):
                            vwap_result = self.calculate_incremental_vwap(
                                prev_vwap=prev_vwap_data['vwap'],
                                prev_cumulative_volume=1000000,  # Simplified
                                prev_cumulative_value=prev_vwap_data['vwap'] * 1000000,
                                today_high=current_data['high'],
                                today_low=current_data['low'],
                                today_close=current_data['close'],
                                today_volume=current_data['volume']
                            )
                            indicators_data.update({
                                'vwap': vwap_result['vwap'],
                                'eod_vwap': vwap_result['eod_vwap'],
                                'avg_vwap': vwap_result['avg_vwap'],
                                'price_vs_vwap': vwap_result['price_vs_vwap']
                            })

                # 3. OBV calculation
                if i > 0:
                    prev_day_data = historical_data[i-1]

                    # For first day, OBV = volume
                    if i == 1:
                        indicators_data['obv'] = current_data['volume']
                    else:
                        # Get previous OBV from database
                        from datetime import timedelta
                        prev_date = current_date - timedelta(days=1)
                        prev_obv = await self.get_previous_obv(symbol, prev_date)

                        if prev_obv is not None:
                            new_obv = self.calculate_incremental_obv(
                                prev_obv=prev_obv,
                                today_close=current_data['close'],
                                yesterday_close=prev_day_data['close'],
                                today_volume=current_data['volume']
                            )
                            indicators_data['obv'] = new_obv

                # Save indicators if any were calculated
                if indicators_data:
                    await self.save_single_date_indicators(
                        symbol=symbol,
                        stock_name=current_data['stock_name'],
                        date=current_date,
                        indicators=indicators_data
                    )
                    indicators_calculated += 1

            logger.info(f"Calculated {indicators_calculated} historical indicator records for {symbol}")
            return indicators_calculated > 0

        except Exception as e:
            logger.error(f"Failed to calculate missing historical indicators for {symbol}: {e}")
            return False

    async def get_symbols_from_intraday(self, target_date: date = None) -> List[str]:
        """Get all symbols from kr_intraday_total for the specified date"""
        if target_date is None:
            target_date = self.execution_date

        conn = None
        try:
            conn = await self.get_connection()

            # Get symbols from regular table
            query = 'SELECT DISTINCT symbol FROM kr_intraday_total WHERE date = $1 ORDER BY symbol'
            rows = await conn.fetch(query, target_date)

            symbols = [row['symbol'] for row in rows]
            logger.info(f"Found {len(symbols)} symbols in kr_intraday_total for {target_date}")
            return symbols

        except Exception as e:
            logger.error(f"Failed to get symbols for {target_date}: {e}")
            return []
        finally:
            if conn:
                if self.pool:
                    await self.pool.release(conn)
                else:
                    await conn.close()

    async def get_trading_dates(self, start_date: date, end_date: date) -> List[date]:
        """Get all trading dates with data between start_date and end_date"""
        try:
            conn = await self.get_connection()
            query = """
                SELECT DISTINCT date
                FROM kr_intraday_total
                WHERE date >= $1 AND date <= $2
                ORDER BY date
            """
            rows = await conn.fetch(query, start_date, end_date)
            await conn.close()

            trading_dates = [row['date'] for row in rows]
            logger.info(f"Found {len(trading_dates)} trading dates from {start_date} to {end_date}")
            return trading_dates

        except Exception as e:
            logger.error(f"Failed to get trading dates: {e}")
            return []

    async def get_intraday_data(self, symbol: str, limit: int = 1000) -> pd.DataFrame:
        """Get OHLCV data for a symbol from kr_intraday_total partition tables"""
        try:
            if self.pool:
                async with self.pool.acquire() as conn:
                    return await self._get_intraday_data_with_conn(conn, symbol, limit)
            else:
                conn = await asyncpg.connect(self.database_url)
                try:
                    return await self._get_intraday_data_with_conn(conn, symbol, limit)
                finally:
                    await conn.close()
        except Exception as e:
            logger.error(f"Failed to get intraday data for {symbol}: {e}")
            return pd.DataFrame()

    async def _get_intraday_data_with_conn(self, conn, symbol: str, limit: int = 1000) -> pd.DataFrame:
        """Helper method to get intraday data with provided connection"""
        try:
            # Get data from regular table
            query = '''
                SELECT date, open, high, low, close, volume, stock_name
                FROM kr_intraday_total
                WHERE symbol = $1 AND date <= $2
                ORDER BY date DESC
                LIMIT $3
            '''
            rows = await conn.fetch(query, symbol, self.execution_date, limit)

            if not rows:
                logger.warning(f"No data found for {symbol} up to {self.execution_date}")
                return pd.DataFrame()

            all_data = [dict(row) for row in rows]

            df = pd.DataFrame(all_data)

            # Remove duplicates and sort
            df = df.drop_duplicates(subset=['date']).sort_values('date')
            df.reset_index(drop=True, inplace=True)

            # Limit the data
            if len(df) > limit:
                df = df.tail(limit).reset_index(drop=True)

            # Handle null values in OHLCV data - fill with valid methods
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in df.columns:
                    # Forward fill first, then backward fill if needed
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].ffill().bfill()

            # Remove rows where critical OHLCV data is still null
            initial_len = len(df)
            df = df.dropna(subset=['close'])  # Close price is essential
            final_len = len(df)

            if initial_len != final_len:
                logger.info(f"Removed {initial_len - final_len} rows with null close prices for {symbol}")

            if len(df) == 0:
                logger.warning(f"No valid OHLCV data for {symbol}")
                return pd.DataFrame()

            logger.info(f"Retrieved {len(df)} valid records for {symbol} up to {self.execution_date}")
            return df

        except Exception as e:
            logger.error(f"Failed to get intraday data for {symbol}: {e}")
            return pd.DataFrame()

    # Technical Indicator Calculation Functions (same as us_calculator.py)

    def calculate_sma(self, prices: pd.Series, period: int) -> pd.Series:
        """Simple Moving Average with null handling"""
        try:
            if prices.empty or len(prices) < period:
                return pd.Series(index=prices.index, dtype=float)
            return prices.rolling(window=period, min_periods=1).mean()
        except Exception as e:
            logger.warning(f"SMA calculation failed: {e}")
            return pd.Series(index=prices.index, dtype=float)

    def calculate_ema(self, prices: pd.Series, period: int) -> pd.Series:
        """Exponential Moving Average with null handling"""
        try:
            if prices.empty or len(prices) < 2:
                return pd.Series(index=prices.index, dtype=float)
            return prices.ewm(span=period, min_periods=1).mean()
        except Exception as e:
            logger.warning(f"EMA calculation failed: {e}")
            return pd.Series(index=prices.index, dtype=float)

    def calculate_wma(self, prices: pd.Series, period: int) -> pd.Series:
        """Weighted Moving Average with null handling"""
        try:
            if prices.empty or len(prices) < period:
                return pd.Series(index=prices.index, dtype=float)

            def wma_single(x):
                try:
                    valid_data = x[~np.isnan(x)]
                    if len(valid_data) == 0:
                        return np.nan
                    weights = np.arange(1, len(valid_data) + 1)
                    return np.sum(weights * valid_data) / np.sum(weights)
                except:
                    return np.nan

            return prices.rolling(window=period, min_periods=1).apply(wma_single, raw=True)
        except Exception as e:
            logger.warning(f"WMA calculation failed: {e}")
            return pd.Series(index=prices.index, dtype=float)

    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Relative Strength Index with null handling"""
        try:
            if prices.empty or len(prices) < period + 1:
                return pd.Series(index=prices.index, dtype=float)

            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()

            # Avoid division by zero
            rs = gain / loss.replace(0, np.nan)
            rsi = 100 - (100 / (1 + rs))

            # Clamp RSI values to valid range [0, 100]
            rsi = rsi.clip(0, 100)
            return rsi
        except Exception as e:
            logger.warning(f"RSI calculation failed: {e}")
            return pd.Series(index=prices.index, dtype=float)

    def calculate_macd(self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """MACD Line, Signal Line, and Histogram with null handling"""
        try:
            if prices.empty or len(prices) < slow:
                empty_series = pd.Series(index=prices.index, dtype=float)
                return empty_series, empty_series, empty_series

            ema_fast = self.calculate_ema(prices, fast)
            ema_slow = self.calculate_ema(prices, slow)
            macd_line = ema_fast - ema_slow
            macd_signal = self.calculate_ema(macd_line, signal)
            macd_hist = macd_line - macd_signal
            return macd_line, macd_signal, macd_hist
        except Exception as e:
            logger.warning(f"MACD calculation failed: {e}")
            empty_series = pd.Series(index=prices.index, dtype=float)
            return empty_series, empty_series, empty_series

    def calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Bollinger Bands: Upper, Middle, Lower with null handling"""
        try:
            if prices.empty or len(prices) < period:
                empty_series = pd.Series(index=prices.index, dtype=float)
                return empty_series, empty_series, empty_series

            middle_band = self.calculate_sma(prices, period)
            std = prices.rolling(window=period, min_periods=1).std()
            upper_band = middle_band + (std * std_dev)
            lower_band = middle_band - (std * std_dev)
            return upper_band, middle_band, lower_band
        except Exception as e:
            logger.warning(f"Bollinger Bands calculation failed: {e}")
            empty_series = pd.Series(index=prices.index, dtype=float)
            return empty_series, empty_series, empty_series

    def calculate_atr(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Average True Range with null handling"""
        try:
            if any(series.empty for series in [high, low, close]) or len(close) < period:
                return pd.Series(index=close.index, dtype=float)

            high_low = high - low
            high_close = np.abs(high - close.shift())
            low_close = np.abs(low - close.shift())

            # Handle potential NaN values from the shift operation
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1, skipna=True)
            atr = true_range.rolling(window=period, min_periods=1).mean()
            return atr
        except Exception as e:
            logger.warning(f"ATR calculation failed: {e}")
            return pd.Series(index=close.index, dtype=float)

    def calculate_stochastic(self, high: pd.Series, low: pd.Series, close: pd.Series,
                           k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
        """Stochastic %K and %D with null handling"""
        try:
            if any(series.empty for series in [high, low, close]) or len(close) < k_period:
                empty_series = pd.Series(index=close.index, dtype=float)
                return empty_series, empty_series

            lowest_low = low.rolling(window=k_period, min_periods=1).min()
            highest_high = high.rolling(window=k_period, min_periods=1).max()

            # Avoid division by zero
            denominator = highest_high - lowest_low
            denominator = denominator.replace(0, np.nan)

            k_percent = 100 * ((close - lowest_low) / denominator)
            k_percent = k_percent.clip(0, 100)  # Clamp to valid range

            d_percent = k_percent.rolling(window=d_period, min_periods=1).mean()
            return k_percent, d_percent
        except Exception as e:
            logger.warning(f"Stochastic calculation failed: {e}")
            empty_series = pd.Series(index=close.index, dtype=float)
            return empty_series, empty_series

    def calculate_mfi(self, high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int = 14) -> pd.Series:
        """Money Flow Index"""
        typical_price = (high + low + close) / 3
        money_flow = typical_price * volume

        positive_flow = money_flow.where(typical_price > typical_price.shift(), 0).rolling(window=period).sum()
        negative_flow = money_flow.where(typical_price < typical_price.shift(), 0).rolling(window=period).sum()

        money_flow_ratio = positive_flow / negative_flow
        mfi = 100 - (100 / (1 + money_flow_ratio))
        return mfi

    def calculate_roc(self, prices: pd.Series, period: int = 10) -> pd.Series:
        """Rate of Change"""
        roc = ((prices - prices.shift(period)) / prices.shift(period)) * 100
        return roc

    def calculate_vwap(self, high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> Dict[str, pd.Series]:
        """Volume Weighted Average Price calculations"""
        typical_price = (high + low + close) / 3
        vwap = (typical_price * volume).cumsum() / volume.cumsum()

        # Calculate daily VWAP (reset each day)
        eod_vwap = vwap.copy()  # End of day VWAP
        avg_vwap = vwap.rolling(window=20).mean()  # 20-day average VWAP
        price_vs_vwap = ((close - vwap) / vwap) * 100  # Price vs VWAP percentage

        return {
            'vwap': vwap,
            'eod_vwap': eod_vwap,
            'avg_vwap': avg_vwap,
            'price_vs_vwap': price_vs_vwap
        }

    def calculate_adx(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Average Directional Index"""
        # Calculate True Range
        tr = self.calculate_atr(high, low, close, 1) * 1  # Single period TR

        # Calculate Directional Movement
        plus_dm = high.diff()
        minus_dm = low.diff() * -1

        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)

        # Smooth the values
        tr_smooth = tr.rolling(window=period).mean()
        plus_dm_smooth = plus_dm.rolling(window=period).mean()
        minus_dm_smooth = minus_dm.rolling(window=period).mean()

        # Calculate DI
        plus_di = 100 * (plus_dm_smooth / tr_smooth)
        minus_di = 100 * (minus_dm_smooth / tr_smooth)

        # Calculate DX
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)

        # Calculate ADX
        adx = dx.rolling(window=period).mean()
        return adx

    def calculate_aroon(self, high: pd.Series, low: pd.Series, period: int = 14) -> pd.Series:
        """AROON Oscillator (Up - Down)"""
        def aroon_up(x):
            return ((period - x.argmax()) / period) * 100

        def aroon_down(x):
            return ((period - x.argmin()) / period) * 100

        aroon_up_series = high.rolling(window=period + 1).apply(aroon_up, raw=True)
        aroon_down_series = low.rolling(window=period + 1).apply(aroon_down, raw=True)

        # Return AROON Oscillator (Up - Down)
        return aroon_up_series - aroon_down_series

    def calculate_cci(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20) -> pd.Series:
        """Commodity Channel Index"""
        typical_price = (high + low + close) / 3
        sma = typical_price.rolling(window=period).mean()
        mean_deviation = typical_price.rolling(window=period).apply(lambda x: np.abs(x - x.mean()).mean())
        cci = (typical_price - sma) / (0.015 * mean_deviation)
        return cci

    def calculate_obv(self, close: pd.Series, volume: pd.Series) -> pd.Series:
        """On-Balance Volume"""
        obv = pd.Series(index=close.index, dtype=float)
        obv.iloc[0] = volume.iloc[0]

        for i in range(1, len(close)):
            if close.iloc[i] > close.iloc[i-1]:
                obv.iloc[i] = obv.iloc[i-1] + volume.iloc[i]
            elif close.iloc[i] < close.iloc[i-1]:
                obv.iloc[i] = obv.iloc[i-1] - volume.iloc[i]
            else:
                obv.iloc[i] = obv.iloc[i-1]

        return obv

    # Incremental calculation methods for daily processing
    def calculate_incremental_vwap(self, prev_vwap: float, prev_cumulative_volume: float,
                                 prev_cumulative_value: float, today_high: float,
                                 today_low: float, today_close: float, today_volume: float) -> Dict[str, float]:
        """Calculate incremental VWAP based on previous day's data"""
        try:
            # Convert Decimal to float to avoid type mismatch
            prev_vwap = float(prev_vwap) if prev_vwap is not None else 0.0
            prev_cumulative_volume = float(prev_cumulative_volume) if prev_cumulative_volume is not None else 0.0
            prev_cumulative_value = float(prev_cumulative_value) if prev_cumulative_value is not None else 0.0
            today_high = float(today_high) if today_high is not None else 0.0
            today_low = float(today_low) if today_low is not None else 0.0
            today_close = float(today_close) if today_close is not None else 0.0
            today_volume = float(today_volume) if today_volume is not None else 0.0

            # Calculate today's typical price
            typical_price = (today_high + today_low + today_close) / 3

            # Calculate new cumulative values
            new_cumulative_volume = prev_cumulative_volume + today_volume
            new_cumulative_value = prev_cumulative_value + (typical_price * today_volume)

            # Calculate new VWAP
            if new_cumulative_volume > 0:
                new_vwap = new_cumulative_value / new_cumulative_volume
            else:
                new_vwap = prev_vwap

            # Calculate price vs VWAP percentage
            price_vs_vwap = ((today_close - new_vwap) / new_vwap) * 100 if new_vwap != 0 else 0

            return {
                'vwap': new_vwap,
                'eod_vwap': new_vwap,  # End of day VWAP is current VWAP
                'avg_vwap': new_vwap,  # For daily calculation, use current VWAP
                'price_vs_vwap': price_vs_vwap,
                'cumulative_volume': new_cumulative_volume,
                'cumulative_value': new_cumulative_value
            }

        except Exception as e:
            logger.warning(f"Incremental VWAP calculation failed: {e}")
            return {
                'vwap': prev_vwap,
                'eod_vwap': prev_vwap,
                'avg_vwap': prev_vwap,
                'price_vs_vwap': 0,
                'cumulative_volume': prev_cumulative_volume,
                'cumulative_value': prev_cumulative_value
            }

    def calculate_incremental_obv(self, prev_obv: float, today_close: float,
                                yesterday_close: float, today_volume: float) -> float:
        """Calculate incremental OBV based on previous day's OBV"""
        try:
            # Convert Decimal to float to avoid type mismatch
            prev_obv = float(prev_obv) if prev_obv is not None else 0.0
            today_close = float(today_close) if today_close is not None else 0.0
            yesterday_close = float(yesterday_close) if yesterday_close is not None else 0.0
            today_volume = float(today_volume) if today_volume is not None else 0.0

            if today_close > yesterday_close:
                return prev_obv + today_volume
            elif today_close < yesterday_close:
                return prev_obv - today_volume
            else:
                return prev_obv

        except Exception as e:
            logger.warning(f"Incremental OBV calculation failed: {e}")
            return float(prev_obv) if prev_obv is not None else 0.0

    def calculate_single_date_indicators(self, historical_ohlcv: List[Dict], today_data: Dict) -> Dict[str, float]:
        """Calculate single date indicators using historical OHLCV data"""
        try:
            if not historical_ohlcv or not today_data:
                return {}

            # Combine historical data with today's data
            all_data = historical_ohlcv + [today_data]

            # Extract OHLCV series from the combined data
            all_closes = [d['close'] for d in all_data]
            all_highs = [d['high'] for d in all_data]
            all_lows = [d['low'] for d in all_data]
            all_volumes = [d['volume'] for d in all_data]

            # Convert to pandas Series
            close_series = pd.Series(all_closes)
            high_series = pd.Series(all_highs)
            low_series = pd.Series(all_lows)
            volume_series = pd.Series(all_volumes)

            indicators = {}

            # Calculate indicators and get only the last value (today's)
            rsi_series = self.calculate_rsi(close_series)
            if not rsi_series.empty and not pd.isna(rsi_series.iloc[-1]):
                indicators['rsi'] = round(float(rsi_series.iloc[-1]), 4)

            macd, macd_signal, macd_hist = self.calculate_macd(close_series)
            if not macd.empty and not pd.isna(macd.iloc[-1]):
                indicators['macd'] = round(float(macd.iloc[-1]), 4)
                if not pd.isna(macd_signal.iloc[-1]):
                    indicators['macd_signal'] = round(float(macd_signal.iloc[-1]), 4)
                if not pd.isna(macd_hist.iloc[-1]):
                    indicators['macd_hist'] = round(float(macd_hist.iloc[-1]), 4)

            bb_upper, bb_middle, bb_lower = self.calculate_bollinger_bands(close_series)
            if not bb_upper.empty and not pd.isna(bb_upper.iloc[-1]):
                indicators['real_upper_band'] = round(float(bb_upper.iloc[-1]), 4)
                indicators['real_middle_band'] = round(float(bb_middle.iloc[-1]), 4)
                indicators['real_lower_band'] = round(float(bb_lower.iloc[-1]), 4)

            atr_series = self.calculate_atr(high_series, low_series, close_series)
            if not atr_series.empty and not pd.isna(atr_series.iloc[-1]):
                indicators['atr'] = round(float(atr_series.iloc[-1]), 4)

            slowk, slowd = self.calculate_stochastic(high_series, low_series, close_series)
            if not slowk.empty and not pd.isna(slowk.iloc[-1]):
                indicators['slowk'] = round(float(slowk.iloc[-1]), 4)
                if not pd.isna(slowd.iloc[-1]):
                    indicators['slowd'] = round(float(slowd.iloc[-1]), 4)

            mfi_series = self.calculate_mfi(high_series, low_series, close_series, volume_series)
            if not mfi_series.empty and not pd.isna(mfi_series.iloc[-1]):
                indicators['mfi'] = round(float(mfi_series.iloc[-1]), 4)

            roc_series = self.calculate_roc(close_series)
            if not roc_series.empty and not pd.isna(roc_series.iloc[-1]):
                indicators['roc'] = round(float(roc_series.iloc[-1]), 4)

            sma_series = self.calculate_sma(close_series, 20)
            if not sma_series.empty and not pd.isna(sma_series.iloc[-1]):
                indicators['sma'] = round(float(sma_series.iloc[-1]), 4)

            ema_series = self.calculate_ema(close_series, 20)
            if not ema_series.empty and not pd.isna(ema_series.iloc[-1]):
                indicators['ema'] = round(float(ema_series.iloc[-1]), 4)

            wma_series = self.calculate_wma(close_series, 20)
            if not wma_series.empty and not pd.isna(wma_series.iloc[-1]):
                indicators['wma'] = round(float(wma_series.iloc[-1]), 4)

            adx_series = self.calculate_adx(high_series, low_series, close_series)
            if not adx_series.empty and not pd.isna(adx_series.iloc[-1]):
                indicators['adx'] = round(float(adx_series.iloc[-1]), 4)

            aroon_series = self.calculate_aroon(high_series, low_series)
            if not aroon_series.empty and not pd.isna(aroon_series.iloc[-1]):
                indicators['aroon'] = round(float(aroon_series.iloc[-1]), 4)

            cci_series = self.calculate_cci(high_series, low_series, close_series)
            if not cci_series.empty and not pd.isna(cci_series.iloc[-1]):
                indicators['cci'] = round(float(cci_series.iloc[-1]), 4)

            # Round all indicators to 4 decimal places
            rounded_indicators = {}
            for key, value in indicators.items():
                if value is not None and not pd.isna(value) and not np.isinf(value):
                    rounded_indicators[key] = round(float(value), 4)
                else:
                    rounded_indicators[key] = None

            return rounded_indicators

        except Exception as e:
            logger.warning(f"Single date indicators calculation failed: {e}")
            return {}

    # Daily indicator calculation function
    async def calculate_daily_indicators(self, symbol: str, execution_date: date) -> bool:
        """Calculate indicators for a single symbol on a specific date"""
        # Semaphore removed - DB pool handles concurrency now
        try:
            # 1. Check if indicators already exist for this date
            if await self.check_daily_indicators_exist(symbol, execution_date):
                logger.info(f"Indicators already exist for {symbol} on {execution_date}, skipping")
                return True

            # 2. Get today's OHLCV data
            today_data = await self.get_today_intraday_data(symbol, execution_date)
            if not today_data or not all(today_data[key] is not None for key in ['high', 'low', 'close', 'volume']):
                logger.warning(f"Missing or invalid OHLCV data for {symbol} on {execution_date}, skipping")
                return False

            logger.info(f"Calculating daily indicators for {symbol} on {execution_date}")

            # 3. Skip checking for missing historical data to avoid timeouts
            # Just calculate today's indicators directly

            # 4. Calculate today's indicators
            indicators_data = {}

            # A) Get historical OHLCV data (not just closes)
            historical_ohlcv = await self.get_historical_ohlcv_data(symbol, execution_date, 50)
            if len(historical_ohlcv) >= 14:  # Minimum for RSI (14 days)
                simple_indicators = self.calculate_single_date_indicators(historical_ohlcv, today_data)
                indicators_data.update(simple_indicators)
                logger.info(f"Calculated {len(simple_indicators)} basic indicators for {symbol}")
            elif len(historical_ohlcv) < 14:
                logger.warning(f"Insufficient historical data for {symbol}: only {len(historical_ohlcv)} days available (need 14+), skipping basic indicators")
                # Still calculate VWAP and OBV which don't need as much history

            # B) VWAP calculation - use ONLY today's data
            logger.info(f"Calculating daily VWAP for {symbol}")

            # Daily VWAP = (H + L + C) / 3
            daily_vwap = (today_data['high'] + today_data['low'] + today_data['close']) / 3
            price_vs_vwap = ((today_data['close'] - daily_vwap) / daily_vwap) * 100 if daily_vwap != 0 else 0

            indicators_data.update({
                'vwap': round(daily_vwap, 4),
                'eod_vwap': round(today_data['close'], 4),
                'avg_vwap': round(daily_vwap, 4),  # Same as daily for daily calculation
                'price_vs_vwap': round(price_vs_vwap, 4)
            })

            # C) OBV calculation - use last 7 valid data points
            logger.info(f"Calculating OBV from last 7 data points for {symbol}")

            # Get last 7 valid data points
            obv_history = await self.get_historical_ohlcv_data(symbol, execution_date, 7)

            if obv_history and len(obv_history) >= 1:  # At least 1 historical point
                # Calculate OBV from available data points
                obv = obv_history[0]['volume']  # Start with first day's volume

                for i in range(1, len(obv_history)):
                    curr_close = obv_history[i]['close']
                    prev_close = obv_history[i-1]['close']

                    if curr_close > prev_close:
                        obv += obv_history[i]['volume']
                    elif curr_close < prev_close:
                        obv -= obv_history[i]['volume']
                    # If equal, OBV stays the same

                # Add today's change
                yesterday_close = obv_history[-1]['close']
                if today_data['close'] > yesterday_close:
                    obv += today_data['volume']
                elif today_data['close'] < yesterday_close:
                    obv -= today_data['volume']

                indicators_data['obv'] = round(obv, 4)
                logger.info(f"Calculated OBV using {len(obv_history)} historical data points")
            else:
                # No historical data, use today's volume as initial OBV
                logger.warning(f"No OBV history for {symbol}, initializing with today's volume")
                indicators_data['obv'] = round(today_data['volume'], 4)

            # 4. Save indicators to database
            if indicators_data:
                await self.save_single_date_indicators(
                    symbol=symbol,
                    stock_name=today_data['stock_name'],
                    date=execution_date,
                    indicators=indicators_data
                )
                logger.info(f"Saved {len(indicators_data)} indicators for {symbol} on {execution_date}")
                return True
            else:
                logger.warning(f"No indicators calculated for {symbol} on {execution_date}, skipping")
                return False  # Return False but don't treat as error - just skip

        except Exception as e:
            logger.error(f"Failed to calculate daily indicators for {symbol} on {execution_date}: {e}")
            return False

    async def check_daily_indicators_exist(self, symbol: str, target_date: date) -> bool:
        """Check if indicators already exist for this symbol and date"""
        try:
            conn = await self.get_connection()

            # Check if indicators exist for this symbol and date in single table
            query = "SELECT COUNT(*) FROM kr_indicators WHERE symbol = $1 AND date = $2"
            count = await conn.fetchval(query, symbol, target_date)
            await conn.close()

            return count > 0

        except Exception as e:
            logger.warning(f"Failed to check existing indicators for {symbol} on {target_date}: {e}")
            return False

    async def save_single_date_indicators(self, symbol: str, stock_name: str, date: date, indicators: Dict):
        """Save indicators for a single date to kr_indicators regular table"""
        try:
            conn = await self.get_connection()

            # Prepare record data
            record_data = {
                'symbol': symbol,
                'stock_name': stock_name,
                'date': date,
                'created_at': datetime.now()
            }

            # Add indicator values
            indicator_columns = [
                'rsi', 'macd', 'macd_signal', 'macd_hist', 'real_upper_band', 'real_middle_band', 'real_lower_band',
                'vwap', 'eod_vwap', 'avg_vwap', 'price_vs_vwap', 'atr', 'slowk', 'slowd', 'mfi', 'roc',
                'sma', 'ema', 'adx', 'wma', 'aroon', 'cci', 'obv'
            ]

            for col in indicator_columns:
                if col in indicators and indicators[col] is not None:
                    try:
                        # Ensure 4 decimal places for all numeric indicators
                        value = float(indicators[col])
                        if not pd.isna(value) and not np.isinf(value):
                            record_data[col] = round(value, 4)
                        else:
                            record_data[col] = None
                    except (ValueError, TypeError):
                        record_data[col] = None
                else:
                    record_data[col] = None

            # Prepare insert query with ROUND for numeric columns
            columns = list(record_data.keys())
            column_names = ', '.join(columns)

            # Create rounded placeholders for VALUES
            non_numeric_columns = ['symbol', 'stock_name', 'date', 'created_at', 'updated_at']
            rounded_placeholders = []
            for i, col in enumerate(columns):
                if col in non_numeric_columns:
                    rounded_placeholders.append(f'${i+1}')
                else:
                    rounded_placeholders.append(f'ROUND(${i+1}::numeric, 4)')

            rounded_values_str = ', '.join(rounded_placeholders)

            # Create rounded update clauses for ON CONFLICT
            rounded_update_clauses = []
            for col in columns:
                if col not in ['symbol', 'date']:
                    if col in ['stock_name', 'created_at', 'updated_at']:
                        rounded_update_clauses.append(f'{col} = EXCLUDED.{col}')
                    else:
                        rounded_update_clauses.append(f'{col} = ROUND(EXCLUDED.{col}::numeric, 4)')

            rounded_update_str = ', '.join(rounded_update_clauses)

            query = f'''
                INSERT INTO kr_indicators ({column_names})
                VALUES ({rounded_values_str})
                ON CONFLICT (symbol, date) DO UPDATE SET
                {rounded_update_str}
            '''

            # Execute insert
            values = [record_data[col] for col in columns]
            await conn.execute(query, *values)
            await conn.close()

        except Exception as e:
            logger.error(f"Failed to save indicators for {symbol} on {date}: {e}")
            if 'conn' in locals():
                await conn.close()

    # Database Save Functions for kr_indicators table

    async def save_all_indicators_to_kr_table(self, symbol: str, stock_name: str, dates: pd.Series, indicators_data: Dict):
        """Save all indicators to kr_indicators partition tables using batch operations"""
        try:
            if self.pool:
                async with self.pool.acquire() as conn:
                    await self._save_all_indicators_with_conn(conn, symbol, stock_name, dates, indicators_data)
            else:
                conn = await asyncpg.connect(self.database_url)
                try:
                    await self._save_all_indicators_with_conn(conn, symbol, stock_name, dates, indicators_data)
                finally:
                    await conn.close()
        except Exception as e:
            logger.error(f"Failed to batch save indicators for {symbol}: {e}")

    async def _save_all_indicators_with_conn(self, conn, symbol: str, stock_name: str, dates: pd.Series, indicators_data: Dict):
        """Helper method to save indicators with provided connection"""
        try:
            # Prepare all records for regular table
            all_records = []

            for i, date_val in enumerate(dates):

                # Collect all indicator values for this date
                record_data = {
                    'symbol': symbol,
                    'stock_name': stock_name,
                    'date': date_val,
                    'created_at': datetime.now()
                }

                # Add each indicator value (handle NaN values)
                indicators_map = {
                    'rsi': 'rsi',
                    'macd': 'macd',
                    'macd_signal': 'macd_signal',
                    'macd_hist': 'macd_hist',
                    'bb_upper': 'real_upper_band',
                    'bb_middle': 'real_middle_band',
                    'bb_lower': 'real_lower_band',
                    'vwap': 'vwap',
                    'eod_vwap': 'eod_vwap',
                    'avg_vwap': 'avg_vwap',
                    'price_vs_vwap': 'price_vs_vwap',
                    'atr': 'atr',
                    'slowk': 'slowk',
                    'slowd': 'slowd',
                    'mfi': 'mfi',
                    'roc': 'roc',
                    'sma': 'sma',
                    'ema': 'ema',
                    'adx': 'adx',
                    'wma': 'wma',
                    'aroon': 'aroon',
                    'cci': 'cci',
                    'obv': 'obv'
                }

                # Check if any indicator has valid data for this date
                has_valid_data = False
                for key, col_name in indicators_map.items():
                    if key in indicators_data and i < len(indicators_data[key]):
                        value = indicators_data[key].iloc[i]
                        # More robust null checking
                        if not pd.isna(value) and value is not None and not np.isinf(value):
                            try:
                                record_data[col_name] = float(value)
                                has_valid_data = True
                            except (ValueError, TypeError):
                                record_data[col_name] = None
                        else:
                            record_data[col_name] = None
                    else:
                        record_data[col_name] = None

                # Only add record if it has at least some valid indicator data
                if has_valid_data:
                    all_records.append(record_data)

            # Save to regular table
            if not all_records:
                logger.warning(f"No valid indicator data to save for {symbol}")
                return

            # Prepare batch insert for regular table
            columns = list(all_records[0].keys())
            placeholders = ', '.join([f'${i+1}' for i in range(len(columns))])
            column_names = ', '.join(columns)

            # Create update clause for ON CONFLICT
            update_clauses = ', '.join([f'{col} = EXCLUDED.{col}' for col in columns if col not in ['symbol', 'date']])

            query = f'''
                INSERT INTO kr_indicators ({column_names})
                VALUES ({placeholders})
                ON CONFLICT (symbol, date) DO UPDATE SET
                {update_clauses}
            '''

            # Prepare values for batch insert
            values_list = []
            for record in all_records:
                values = [record[col] for col in columns]
                values_list.append(values)

            # Execute batch insert
            await conn.executemany(query, values_list)
            logger.info(f"Saved {len(all_records)} indicator records to kr_indicators for {symbol}")

            logger.info(f"Successfully batch saved indicator records for {symbol}")

        except Exception as e:
            logger.error(f"Failed to batch save indicators for {symbol}: {e}")
            raise

    async def check_indicators_exist(self, symbol: str) -> bool:
        """Check if indicators already exist for this symbol and execution date with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if self.pool:
                    async with self.pool.acquire() as conn:
                        # Check if we have recent indicators for this symbol
                        year_month = self.execution_date.strftime('%Y_%m')
                        indicator_table = f"kr_indicators_{year_month}"

                        # Check if indicator table exists first
                        check_query = """
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables
                                WHERE table_name = $1
                            );
                        """
                        table_exists = await conn.fetchval(check_query, indicator_table)

                        if not table_exists:
                            return False

                        # Check if we have indicators for this symbol on execution date
                        query = f"SELECT COUNT(*) FROM {indicator_table} WHERE symbol = $1 AND date = $2"
                        count = await conn.fetchval(query, symbol, self.execution_date)
                        return count > 0
                else:
                    conn = await asyncpg.connect(self.database_url)
                    try:
                        # Check if we have recent indicators for this symbol
                        year_month = self.execution_date.strftime('%Y_%m')
                        indicator_table = f"kr_indicators_{year_month}"

                        # Check if indicator table exists first
                        check_query = """
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables
                                WHERE table_name = $1
                            );
                        """
                        table_exists = await conn.fetchval(check_query, indicator_table)

                        if not table_exists:
                            return False

                        # Check if we have indicators for this symbol on execution date
                        query = f"SELECT COUNT(*) FROM {indicator_table} WHERE symbol = $1 AND date = $2"
                        count = await conn.fetchval(query, symbol, self.execution_date)
                        return count > 0
                    finally:
                        await conn.close()

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Attempt {attempt + 1} failed to check existing indicators for {symbol}: {e}, retrying...")
                    await asyncio.sleep(0.5)  # Brief delay before retry
                    continue
                else:
                    logger.warning(f"Failed to check existing indicators for {symbol} after {max_retries} attempts: {e}")
                    return False

        return False

    async def calculate_and_save_indicators_batch(self, symbol: str, force_recalculate: bool = False):
        """Calculate and batch save all technical indicators for a Korean symbol"""
        try:
            # Skip if already processed (unless force recalculate)
            if not force_recalculate and await self.check_indicators_exist(symbol):
                return

            # Get intraday data
            df = await self.get_intraday_data(symbol)
            if df.empty:
                logger.warning(f"No data found for {symbol}")
                return

            # Extract price series and stock name
            dates = df['date']
            high = df['high']
            low = df['low']
            close = df['close']
            volume = df['volume']
            stock_name = df['stock_name'].iloc[0] if not df['stock_name'].empty else ''

            # Calculate all indicators
            logger.info(f"Calculating all indicators for {symbol}")

            indicators_data = {}

            # Basic indicators
            indicators_data['rsi'] = self.calculate_rsi(close)
            macd, macd_signal, macd_hist = self.calculate_macd(close)
            indicators_data['macd'] = macd
            indicators_data['macd_signal'] = macd_signal
            indicators_data['macd_hist'] = macd_hist

            bb_upper, bb_middle, bb_lower = self.calculate_bollinger_bands(close)
            indicators_data['bb_upper'] = bb_upper
            indicators_data['bb_middle'] = bb_middle
            indicators_data['bb_lower'] = bb_lower

            indicators_data['atr'] = self.calculate_atr(high, low, close)
            slowk, slowd = self.calculate_stochastic(high, low, close)
            indicators_data['slowk'] = slowk
            indicators_data['slowd'] = slowd

            indicators_data['mfi'] = self.calculate_mfi(high, low, close, volume)
            indicators_data['roc'] = self.calculate_roc(close)
            indicators_data['sma'] = self.calculate_sma(close, 20)
            indicators_data['ema'] = self.calculate_ema(close, 20)
            indicators_data['wma'] = self.calculate_wma(close, 20)

            vwap_dict = self.calculate_vwap(high, low, close, volume)
            indicators_data['vwap'] = vwap_dict['vwap']
            indicators_data['eod_vwap'] = vwap_dict['eod_vwap']
            indicators_data['avg_vwap'] = vwap_dict['avg_vwap']
            indicators_data['price_vs_vwap'] = vwap_dict['price_vs_vwap']

            indicators_data['adx'] = self.calculate_adx(high, low, close)
            indicators_data['aroon'] = self.calculate_aroon(high, low)
            indicators_data['cci'] = self.calculate_cci(high, low, close)
            indicators_data['obv'] = self.calculate_obv(close, volume)

            # Batch save all indicators to kr_indicators table
            await self.save_all_indicators_to_kr_table(symbol, stock_name, dates, indicators_data)

            logger.info(f"Completed processing Korean indicators for {symbol}")

        except Exception as e:
            logger.error(f"Error processing Korean indicators for {symbol}: {e}")
            # Don't re-raise, just continue to next symbol

    async def process_symbol_batch(self, symbols: List[str], batch_id: int, force_recalculate: bool = False):
        """Process a batch of symbols"""
        try:
            logger.info(f"Korean Batch {batch_id}: Starting processing {len(symbols)} symbols (force_recalculate={force_recalculate})")

            processed_count = 0
            skipped_count = 0

            for i, symbol in enumerate(symbols):
                try:
                    # Check if we should skip this symbol
                    if not force_recalculate and await self.check_indicators_exist(symbol):
                        skipped_count += 1
                        continue

                    await self.calculate_and_save_indicators_batch(symbol, force_recalculate)
                    processed_count += 1

                except Exception as e:
                    logger.error(f"Korean Batch {batch_id}: Failed to process {symbol}: {e}")
                    continue

            logger.info(f"Korean Batch {batch_id}: Completed - Processed: {processed_count}, Skipped: {skipped_count}, Total: {len(symbols)}")

        except Exception as e:
            logger.error(f"Korean Batch {batch_id}: Batch processing failed: {e}")

    # Daily processing functions
    async def process_daily_symbol_batch(self, symbols: List[str], batch_id: int, execution_date: date):
        """Process a batch of symbols for daily calculation with parallel processing"""
        try:
            batch_logger.info(f"Daily Batch {batch_id}: Starting processing {len(symbols)} symbols for {execution_date}")

            # Load completed symbols from progress file
            completed_symbols = self.load_completed_symbols(execution_date)

            # Filter out already completed symbols
            symbols_to_process = [s for s in symbols if s not in completed_symbols]
            skipped_count = len(symbols) - len(symbols_to_process)

            if not symbols_to_process:
                batch_logger.info(f"Daily Batch {batch_id}: All symbols already completed")
                return

            # Process all symbols in parallel (concurrency controlled by symbol_semaphore)
            tasks = [self.calculate_daily_indicators(symbol, execution_date) for symbol in symbols_to_process]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Count successes and save progress
            processed_count = 0
            failed_count = 0

            for symbol, result in zip(symbols_to_process, results):
                if isinstance(result, Exception):
                    logger.error(f"Daily Batch {batch_id}: Failed to process {symbol}: {result}")
                    failed_count += 1
                elif result:
                    processed_count += 1
                    self.save_progress(execution_date, symbol)
                else:
                    failed_count += 1

            batch_logger.info(f"Daily Batch {batch_id}: Completed - Processed: {processed_count}, Skipped: {skipped_count}, Failed: {failed_count}")

        except Exception as e:
            logger.error(f"Daily Batch {batch_id}: Daily batch processing failed: {e}")

    async def run_daily_calculator(self, execution_date: date = None):
        """Main function for daily indicator calculation with parallel processing"""
        try:
            if execution_date is None:
                execution_date = self.execution_date
            else:
                # Update instance execution_date for save_progress
                self.execution_date = execution_date

            batch_logger.info(f"Starting Daily Korean Technical Indicator Calculator for {execution_date}")

            # Initialize connection pool
            await self.init_pool()

            # Get symbols for the execution date
            symbols = await self.get_symbols_from_intraday(execution_date)
            if not symbols:
                batch_logger.warning(f"No symbols found for {execution_date}")
                await self.close_pool()
                return

            batch_logger.info(f"Found {len(symbols)} Korean symbols to process for {execution_date}")

            # Load completed symbols from progress file
            completed_symbols = self.load_completed_symbols(execution_date)
            remaining_symbols = [s for s in symbols if s not in completed_symbols]

            if not remaining_symbols:
                batch_logger.info(f"All symbols already processed for {execution_date}")
                await self.close_pool()
                return

            batch_logger.info(f"Processing {len(remaining_symbols)} remaining symbols (completed: {len(completed_symbols)})")

            # Split remaining symbols into batches - optimized dynamic batch sizing
            if len(remaining_symbols) <= 50:
                batch_size = 10  # Small datasets: smaller batches for faster initial results
            elif len(remaining_symbols) <= 100:
                batch_size = 25
            elif len(remaining_symbols) <= 500:
                batch_size = 50
            else:
                batch_size = 100  # Large datasets: larger batches for efficiency

            symbol_batches = []
            for i in range(0, len(remaining_symbols), batch_size):
                batch = remaining_symbols[i:i + batch_size]
                symbol_batches.append(batch)

            batch_logger.info(f"Split {len(remaining_symbols)} symbols into {len(symbol_batches)} batches of max {batch_size} symbols each")
            for i, batch in enumerate(symbol_batches, 1):
                batch_logger.info(f"Daily Batch {i}: {len(batch)} symbols")

            # Create parallel tasks
            tasks = []
            for i, batch in enumerate(symbol_batches):
                if batch:  # Only create task if batch is not empty
                    task = asyncio.create_task(self.process_daily_symbol_batch(batch, i+1, execution_date))
                    tasks.append(task)

            # Wait for all batches to complete
            if tasks:
                batch_logger.info("Starting daily parallel processing...")
                await asyncio.gather(*tasks, return_exceptions=True)

            # Close connection pool
            await self.close_pool()

            # Final summary
            final_completed = self.load_completed_symbols(execution_date)
            batch_logger.info(f"Daily calculation completed for {execution_date}: {len(final_completed)}/{len(symbols)} symbols processed")

        except Exception as e:
            logger.error(f"Daily calculator failed: {e}")
            if self.pool:
                await self.close_pool()
            raise

    async def run_calculator_parallel(self):
        """Main function to run the calculator with parallel processing (5 workers)"""
        try:
            logger.info(f"Starting Korean Technical Indicator Calculator (Parallel) for execution date: {self.execution_date}")

            # Initialize connection pool
            await self.init_pool()

            symbols = await self.get_symbols_from_intraday()
            logger.info(f"Found {len(symbols)} Korean symbols to process")

            # Split symbols into batches - optimized dynamic batch sizing
            if len(symbols) <= 50:
                batch_size = 10  # Small datasets: smaller batches for faster initial results
            elif len(symbols) <= 100:
                batch_size = 25
            elif len(symbols) <= 500:
                batch_size = 50
            else:
                batch_size = 100  # Large datasets: larger batches for efficiency

            symbol_batches = []
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                symbol_batches.append(batch)

            logger.info(f"Split {len(symbols)} symbols into {len(symbol_batches)} batches of max {batch_size} symbols each")
            for i, batch in enumerate(symbol_batches, 1):
                logger.info(f"Korean Batch {i}: {len(batch)} symbols")

            # Create parallel tasks
            tasks = []
            # Check environment variable for force recalculate option
            force_recalculate = os.getenv('FORCE_RECALCULATE', 'false').lower() == 'true'
            logger.info(f"Force recalculate mode: {force_recalculate}")

            for i, batch in enumerate(symbol_batches):
                task = asyncio.create_task(self.process_symbol_batch(batch, i+1, force_recalculate))
                tasks.append(task)

            # Wait for all batches to complete
            logger.info("Starting Korean parallel processing...")
            await asyncio.gather(*tasks, return_exceptions=True)

            # Close connection pool
            await self.close_pool()


        except Exception as e:
            logger.error(f"Korean parallel calculator failed: {e}")
            if self.pool:
                await self.close_pool()
            raise

    async def run_period_calculator(self, start_date: date, end_date: date):
        """Calculate indicators for all trading dates in the specified period"""
        try:
            logger.info(f"Starting Period Korean Technical Indicator Calculator from {start_date} to {end_date}")

            # Get all trading dates in the period
            trading_dates = await self.get_trading_dates(start_date, end_date)

            if not trading_dates:
                logger.warning(f"No trading dates found between {start_date} and {end_date}")
                return

            # Initialize connection pool
            await self.init_pool()

            total_processed = 0
            total_skipped = 0

            # Process each trading date
            for i, current_date in enumerate(trading_dates, 1):
                # Update execution_date for each date
                self.execution_date = current_date

                logger.info(f"Processing date {i}/{len(trading_dates)}: {current_date}")

                # Get symbols for this date
                symbols = await self.get_symbols_from_intraday(current_date)

                if not symbols:
                    logger.warning(f"No symbols found for {current_date}, skipping")
                    continue

                logger.info(f"Found {len(symbols)} symbols for {current_date}")

                # Process all symbols for this date with parallel processing
                processed_count = 0
                skipped_count = 0

                # Split symbols into batches for parallel processing
                batch_size = 20  # Process 20 symbols concurrently
                symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

                for batch_idx, symbol_batch in enumerate(symbol_batches, 1):
                    logger.info(f"Processing batch {batch_idx}/{len(symbol_batches)} for {current_date} ({len(symbol_batch)} symbols)")

                    # Create tasks for parallel processing
                    tasks = []
                    for symbol in symbol_batch:
                        task = asyncio.create_task(self.calculate_daily_indicators(symbol, current_date))
                        tasks.append((symbol, task))

                    # Wait for all tasks in this batch to complete
                    results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)

                    # Process results
                    for (symbol, _), result in zip(tasks, results):
                        if isinstance(result, Exception):
                            logger.error(f"Error processing {symbol} on {current_date}: {result}")
                            skipped_count += 1
                        elif result:
                            processed_count += 1
                        else:
                            skipped_count += 1

                total_processed += processed_count
                total_skipped += skipped_count

                logger.info(f"Date {current_date}: Processed {processed_count}, Skipped {skipped_count}")

            await self.close_pool()

            logger.info(f"Period calculation completed: Total Processed {total_processed}, Total Skipped {total_skipped}")

        except Exception as e:
            logger.error(f"Period calculator failed: {e}")
            if self.pool:
                await self.close_pool()
            raise

    async def process_single_date(self, current_date: date) -> tuple:
        """Process all symbols for a single date with parallel processing"""
        try:
            # Get symbols for this date
            symbols = await self.get_symbols_from_intraday(current_date)

            if not symbols:
                logger.warning(f"No symbols found for {current_date}, skipping")
                return 0, 0

            logger.info(f"Found {len(symbols)} symbols for {current_date}")

            # Process all symbols for this date with parallel processing
            processed_count = 0
            skipped_count = 0

            # Split symbols into batches for parallel processing
            batch_size = 20  # Process 20 symbols concurrently
            symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

            for batch_idx, symbol_batch in enumerate(symbol_batches, 1):
                logger.debug(f"Processing batch {batch_idx}/{len(symbol_batches)} for {current_date} ({len(symbol_batch)} symbols)")

                # Create tasks for parallel processing
                tasks = []
                for symbol in symbol_batch:
                    task = asyncio.create_task(self.calculate_daily_indicators(symbol, current_date))
                    tasks.append((symbol, task))

                # Wait for all tasks in this batch to complete
                results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)

                # Process results
                for (symbol, _), result in zip(tasks, results):
                    if isinstance(result, Exception):
                        logger.error(f"Error processing {symbol} on {current_date}: {result}")
                        skipped_count += 1
                    elif result:
                        processed_count += 1
                    else:
                        skipped_count += 1

            return processed_count, skipped_count

        except Exception as e:
            logger.error(f"Error processing date {current_date}: {e}")
            return 0, 0

    async def get_missing_dates(self, start_date: date, end_date: date) -> List[date]:
        """Get dates that have data but missing indicators"""
        try:
            conn = await self.get_connection()

            # Get dates with data but no indicators
            query = """
                SELECT DISTINCT i.date
                FROM kr_intraday_total i
                LEFT JOIN (
                    SELECT date, COUNT(DISTINCT symbol) as indicator_count
                    FROM kr_indicators
                    WHERE date >= $1 AND date <= $2
                    GROUP BY date
                ) ind ON i.date = ind.date
                WHERE i.date >= $1 AND i.date <= $2
                AND (ind.indicator_count IS NULL OR ind.indicator_count < (
                    SELECT COUNT(DISTINCT symbol) * 0.9  -- 90% threshold
                    FROM kr_intraday_total
                    WHERE date = i.date
                ))
                ORDER BY i.date
            """

            rows = await conn.fetch(query, start_date, end_date)
            await conn.close()

            missing_dates = [row['date'] for row in rows]
            logger.info(f"Found {len(missing_dates)} dates with missing indicators")
            return missing_dates

        except Exception as e:
            logger.error(f"Failed to get missing dates: {e}")
            return []

    async def run_smart_calculator(self, daily_mode: bool = True, execution_date: date = None):
        """Smart calculator that adapts based on mode and data status"""
        try:
            if daily_mode:
                # DAILY MODE: Check if today has data, if not check missing dates
                if execution_date is None:
                    execution_date = self.execution_date
                else:
                    # Update instance execution_date for save_progress
                    self.execution_date = execution_date

                logger.info(f"Smart Daily Mode: Checking {execution_date}")

                # Check if today has data
                symbols = await self.get_symbols_from_intraday(execution_date)

                if symbols:
                    # Today has data, process normally
                    logger.info(f"Processing today's data: {execution_date} ({len(symbols)} symbols)")
                    return await self.run_daily_calculator(execution_date)
                else:
                    # No data for today, check for missing historical dates
                    logger.info(f"No data for {execution_date}, checking for missing historical indicators")

                    from datetime import date, timedelta
                    start_date = date(2025, 8, 18)
                    end_date = execution_date - timedelta(days=1)

                    missing_dates = await self.get_missing_dates(start_date, end_date)

                    if missing_dates:
                        # Process missing dates (limited to recent ones for daily mode)
                        recent_missing = missing_dates[-5:]  # Last 5 missing dates
                        logger.info(f"Processing {len(recent_missing)} recent missing dates in daily mode")

                        await self.init_pool()

                        for missing_date in recent_missing:
                            logger.info(f"Processing missing date: {missing_date}")

                            date_symbols = await self.get_symbols_from_intraday(missing_date)
                            if date_symbols:
                                processed = 0
                                for symbol in date_symbols:
                                    try:
                                        success = await self.calculate_daily_indicators(symbol, missing_date)
                                        if success:
                                            processed += 1
                                    except Exception as e:
                                        logger.error(f"Error processing {symbol} on {missing_date}: {e}")
                                        continue

                                logger.info(f"Processed {processed} symbols for {missing_date}")

                        await self.close_pool()
                        return
                    else:
                        logger.info("All historical data is up to date")
                        return
            else:
                # BATCH MODE: Full period processing
                from datetime import date, timedelta
                start_date = date(2025, 8, 18)
                end_date = date.today() - timedelta(days=1)
                return await self.run_period_calculator(start_date, end_date)

        except Exception as e:
            logger.error(f"Smart calculator failed: {e}")
            raise

    async def run_calculator(self, daily_mode: bool = False, execution_date: date = None):
        """Main function - supports both batch and daily processing with smart logic"""
        return await self.run_smart_calculator(daily_mode, execution_date)

async def main():
    """Main execution function"""
    # Get database URL from environment
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    # Create calculator instance
    calculator = KoreanTechnicalIndicatorCalculator(database_url)

    # Check for daily mode
    daily_mode = os.getenv('DAILY_MODE', 'false').lower() == 'true'

    if daily_mode:
        # Run in daily mode for today's date
        logger.info("Running in daily mode")
        await calculator.run_calculator(daily_mode=True)
    else:
        # Run in batch mode (original behavior)
        logger.info("Running in batch mode")
        await calculator.run_calculator()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())