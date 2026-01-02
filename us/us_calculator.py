# alpha/data/us/us_calculator.py
import asyncpg
import pandas as pd
import numpy as np
import logging
import os
import asyncio
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
import pytz
import pandas_market_calendars as mcal

# Load environment variables
load_dotenv()

# ===========================
#  :      
# ===========================

def get_latest_us_market_date() -> date:
    """
          

    -   (ET)  
    -   (16:00 ET) :  
    -   (16:00 ET) :  ( )
    -   NYSE   

    Returns:
        date:      
    """
    try:
        # 1.   (ET) 
        et_tz = pytz.timezone('America/New_York')
        now_et = datetime.now(et_tz)

        # 2.    (16:00 ET)
        market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)

        # 3.   
        if now_et < market_close:
            #   :  
            target_date = now_et.date() - timedelta(days=1)
        else:
            #   :  
            target_date = now_et.date()

        # 4. NYSE     
        nyse = mcal.get_calendar('NYSE')

        #  10   (   )
        schedule = nyse.schedule(
            start_date=target_date - timedelta(days=10),
            end_date=target_date
        )

        if not schedule.empty:
            #    
            latest_trading_day = schedule.index[-1].date()
            return latest_trading_day
        else:
            # Fallback:   (    )
            while target_date.weekday() >= 5:  # (5), (6)
                target_date -= timedelta(days=1)
            return target_date

    except Exception as e:
        #       
        print(f"     : {e}")
        now = datetime.now()
        target_date = now.date() - timedelta(days=1)

        #  
        while target_date.weekday() >= 5:
            target_date -= timedelta(days=1)

        return target_date

# Configure logging - Optimized (WARNING level for performance)
logging.basicConfig(
    level=logging.WARNING,  # Changed from INFO to WARNING for 5-10% performance gain
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('log/us_calculator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Enable INFO logs for batch progress only
batch_logger = logging.getLogger(__name__ + '.batch')
batch_logger.setLevel(logging.INFO)

# List of all 15 indicators
ALL_INDICATORS = ['macd', 'bbands', 'vwap', 'atr', 'stoch', 'mfi', 'cci',
                  'rsi', 'roc', 'sma', 'ema', 'adx', 'wma', 'aroon', 'obv']

class USTechnicalIndicatorCalculator:
    def __init__(self, database_url: str, max_concurrent_batches: int = 40):
        if database_url.startswith('postgresql+asyncpg://'):
            database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')
        self.database_url = database_url
        #       (, ,    )
        self.execution_date = get_latest_us_market_date()
        self.pool = None
        # Memory cache for frequently accessed data
        self.ohlcv_cache = {}
        self.cache_max_size = 1000
        # Semaphore control - Symbol-level
        self.max_concurrent_batches = max_concurrent_batches
        self.symbol_semaphore = None  # Controls concurrent symbol processing (80)

    async def init_pool(self):
        """Initialize connection pool"""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=10,
            max_size=100,
            command_timeout=300,
            max_inactive_connection_lifetime=1800
        )
        # Initialize semaphore for symbol-level concurrency control
        self.symbol_semaphore = asyncio.Semaphore(80)

    async def get_connection(self):
        if self.pool:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    conn = await self.pool.acquire()
                    # Test if connection is alive
                    try:
                        await conn.fetchval('SELECT 1')
                        return conn
                    except:
                        # Connection is dead, release and try again
                        try:
                            await self.pool.release(conn)
                        except:
                            pass
                        if attempt < max_retries - 1:
                            continue
                        else:
                            raise
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Failed to acquire connection after {max_retries} attempts: {e}")
                        raise
                    await asyncio.sleep(1)
        else:
            return await asyncpg.connect(self.database_url)

    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()

    # Data retrieval functions
    async def get_symbols_from_daily(self, target_date: date = None) -> List[str]:
        """Get all symbols from us_daily for the specified date"""
        if target_date is None:
            target_date = self.execution_date

        conn = await self.get_connection()
        try:
            query = 'SELECT DISTINCT symbol FROM us_daily WHERE date = $1 ORDER BY symbol'
            rows = await conn.fetch(query, target_date)

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

            symbols = [row['symbol'] for row in rows]
            logger.info(f"Found {len(symbols)} symbols in us_daily for {target_date}")
            return symbols

        except Exception as e:
            logger.error(f"Failed to get symbols for {target_date}: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return []

    async def get_weekly_data_from_table(self, symbol: str, limit: int = 200) -> pd.DataFrame:
        """Get weekly OHLCV data directly from us_weekly table"""
        conn = await self.get_connection()
        try:
            query = '''
                SELECT date, open, high, low, close, volume
                FROM us_weekly
                WHERE symbol = $1 AND date <= $2
                ORDER BY date DESC
                LIMIT $3
            '''
            rows = await conn.fetch(query, symbol, self.execution_date, limit)

            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()

            if not rows:
                logger.info(f"No weekly data found for {symbol} in us_weekly table")
                return pd.DataFrame()

            all_data = [dict(row) for row in rows]
            df = pd.DataFrame(all_data)

            # Remove duplicates and sort
            df = df.drop_duplicates(subset=['date']).sort_values('date')
            df.reset_index(drop=True, inplace=True)

            # Handle null values in OHLCV data
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].ffill().bfill()

            # Remove rows where critical OHLCV data is still null
            df = df.dropna(subset=['close'])

            if len(df) == 0:
                logger.info(f"No valid weekly OHLCV data for {symbol}")
                return pd.DataFrame()

            logger.info(f"Retrieved {len(df)} weekly records from us_weekly for {symbol}")
            return df

        except Exception as e:
            logger.error(f"Failed to get weekly data from table for {symbol}: {e}")
            if self.pool:
                await self.pool.release(conn)
            else:
                await conn.close()
            return pd.DataFrame()

    async def get_daily_data(self, symbol: str, limit: int = 500) -> pd.DataFrame:
        """Get OHLCV data for a symbol from us_daily table"""
        conn = None
        try:
            conn = await self.get_connection()

            query = '''
                SELECT date, open, high, low, close, volume
                FROM us_daily
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

            # Handle null values in OHLCV data
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].ffill().bfill()

            # Remove rows where critical OHLCV data is still null
            initial_len = len(df)
            df = df.dropna(subset=['close'])
            final_len = len(df)

            if initial_len != final_len:
                logger.info(f"Removed {initial_len - final_len} rows with null close prices for {symbol}")

            if len(df) == 0:
                logger.warning(f"No valid OHLCV data for {symbol}")
                return pd.DataFrame()

            logger.info(f"Retrieved {len(df)} valid records for {symbol} up to {self.execution_date}")
            return df

        except Exception as e:
            logger.error(f"Failed to get daily data for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    def get_latest_friday(self, execution_date: date) -> date:
        """Get the most recent Friday from execution date (inclusive)"""
        # 0=Monday, 4=Friday
        days_since_friday = (execution_date.weekday() - 4) % 7
        if days_since_friday == 0:
            # Today is Friday
            return execution_date
        else:
            # Go back to most recent Friday
            return execution_date - timedelta(days=days_since_friday)

    # Technical Indicator Calculation Functions (unchanged - keeping them compact)

    def calculate_sma(self, prices: pd.Series, period: int) -> pd.Series:
        """Simple Moving Average"""
        try:
            if prices.empty or len(prices) < period:
                return pd.Series(index=prices.index, dtype=float)
            return prices.rolling(window=period, min_periods=1).mean()
        except Exception as e:
            logger.warning(f"SMA calculation failed: {e}")
            return pd.Series(index=prices.index, dtype=float)

    def calculate_ema(self, prices: pd.Series, period: int) -> pd.Series:
        """Exponential Moving Average"""
        try:
            if prices.empty or len(prices) < 2:
                return pd.Series(index=prices.index, dtype=float)
            return prices.ewm(span=period, min_periods=1, adjust=False).mean()
        except Exception as e:
            logger.warning(f"EMA calculation failed: {e}")
            return pd.Series(index=prices.index, dtype=float)

    def calculate_wma(self, prices: pd.Series, period: int) -> pd.Series:
        """Weighted Moving Average"""
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
        """Relative Strength Index"""
        try:
            if prices.empty or len(prices) < period + 1:
                return pd.Series(index=prices.index, dtype=float)

            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()

            rs = gain / loss.replace(0, np.nan)
            rsi = 100 - (100 / (1 + rs))

            rsi = rsi.clip(0, 100)
            return rsi
        except Exception as e:
            logger.warning(f"RSI calculation failed: {e}")
            return pd.Series(index=prices.index, dtype=float)

    def calculate_macd(self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """MACD Line, Signal Line, and Histogram"""
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
        """Bollinger Bands: Upper, Middle, Lower"""
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
        """Average True Range"""
        try:
            if any(series.empty for series in [high, low, close]) or len(close) < period:
                return pd.Series(index=close.index, dtype=float)

            high_low = high - low
            high_close = np.abs(high - close.shift())
            low_close = np.abs(low - close.shift())

            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1, skipna=True)
            atr = true_range.rolling(window=period, min_periods=1).mean()
            return atr
        except Exception as e:
            logger.warning(f"ATR calculation failed: {e}")
            return pd.Series(index=close.index, dtype=float)

    def calculate_stochastic(self, high: pd.Series, low: pd.Series, close: pd.Series,
                           k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
        """Stochastic %K and %D"""
        try:
            if any(series.empty for series in [high, low, close]) or len(close) < k_period:
                empty_series = pd.Series(index=close.index, dtype=float)
                return empty_series, empty_series

            lowest_low = low.rolling(window=k_period, min_periods=1).min()
            highest_high = high.rolling(window=k_period, min_periods=1).max()

            denominator = highest_high - lowest_low
            denominator = denominator.replace(0, np.nan)

            k_percent = 100 * ((close - lowest_low) / denominator)
            k_percent = k_percent.clip(0, 100)

            d_percent = k_percent.rolling(window=d_period, min_periods=1).mean()
            return k_percent, d_percent
        except Exception as e:
            logger.warning(f"Stochastic calculation failed: {e}")
            empty_series = pd.Series(index=close.index, dtype=float)
            return empty_series, empty_series

    def calculate_mfi(self, high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int = 14) -> pd.Series:
        """Money Flow Index"""
        try:
            if any(series.empty for series in [high, low, close, volume]) or len(close) < period:
                return pd.Series(index=close.index, dtype=float)

            typical_price = (high + low + close) / 3
            money_flow = typical_price * volume

            positive_flow = money_flow.where(typical_price > typical_price.shift(), 0).rolling(window=period, min_periods=1).sum()
            negative_flow = money_flow.where(typical_price < typical_price.shift(), 0).rolling(window=period, min_periods=1).sum()

            money_flow_ratio = positive_flow / negative_flow.replace(0, np.nan)
            mfi = 100 - (100 / (1 + money_flow_ratio))
            mfi = mfi.clip(0, 100)
            return mfi
        except Exception as e:
            logger.warning(f"MFI calculation failed: {e}")
            return pd.Series(index=close.index, dtype=float)

    def calculate_roc(self, prices: pd.Series, period: int = 10) -> pd.Series:
        """Rate of Change"""
        try:
            if prices.empty or len(prices) < period + 1:
                return pd.Series(index=prices.index, dtype=float)

            # Replace zero and very small values to prevent division by zero
            shifted_prices = prices.shift(period).replace(0, np.nan)
            shifted_prices = shifted_prices.where(shifted_prices.abs() >= 0.0001, np.nan)

            roc = ((prices - shifted_prices) / shifted_prices) * 100

            # Filter out infinite values
            roc = roc.replace([np.inf, -np.inf], np.nan)

            return roc
        except Exception as e:
            logger.warning(f"ROC calculation failed: {e}")
            return pd.Series(index=prices.index, dtype=float)

    def calculate_vwap(self, high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> Dict[str, pd.Series]:
        """Volume Weighted Average Price calculations (Daily approximation)"""
        try:
            if any(series.empty for series in [high, low, close, volume]):
                empty_series = pd.Series(index=close.index, dtype=float)
                return {
                    'eod_vwap': empty_series,
                    'avg_vwap': empty_series,
                    'price_vs_vwap': empty_series
                }

            typical_price = (high + low + close) / 3

            # Daily VWAP (simplified as typical price for daily data)
            eod_vwap = typical_price

            # 20-day average VWAP
            avg_vwap = eod_vwap.rolling(window=20, min_periods=1).mean()

            # Price vs VWAP percentage
            price_vs_vwap = ((close - eod_vwap) / eod_vwap.replace(0, np.nan)) * 100

            return {
                'eod_vwap': eod_vwap,
                'avg_vwap': avg_vwap,
                'price_vs_vwap': price_vs_vwap
            }
        except Exception as e:
            logger.warning(f"VWAP calculation failed: {e}")
            empty_series = pd.Series(index=close.index, dtype=float)
            return {
                'eod_vwap': empty_series,
                'avg_vwap': empty_series,
                'price_vs_vwap': empty_series
            }

    def calculate_adx(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Average Directional Index"""
        try:
            if any(series.empty for series in [high, low, close]) or len(close) < period * 2:
                return pd.Series(index=close.index, dtype=float)

            # Calculate True Range
            high_low = high - low
            high_close = np.abs(high - close.shift())
            low_close = np.abs(low - close.shift())
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1, skipna=True)

            # Calculate Directional Movement
            plus_dm = high.diff()
            minus_dm = -low.diff()

            plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
            minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)

            # Smooth the values
            tr_smooth = tr.rolling(window=period, min_periods=1).mean()
            plus_dm_smooth = plus_dm.rolling(window=period, min_periods=1).mean()
            minus_dm_smooth = minus_dm.rolling(window=period, min_periods=1).mean()

            # Calculate DI
            plus_di = 100 * (plus_dm_smooth / tr_smooth.replace(0, np.nan))
            minus_di = 100 * (minus_dm_smooth / tr_smooth.replace(0, np.nan))

            # Calculate DX
            dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)

            # Calculate ADX
            adx = dx.rolling(window=period, min_periods=1).mean()
            return adx
        except Exception as e:
            logger.warning(f"ADX calculation failed: {e}")
            return pd.Series(index=close.index, dtype=float)

    def calculate_aroon(self, high: pd.Series, low: pd.Series, period: int = 14) -> pd.Series:
        """AROON Oscillator (Up - Down)"""
        try:
            if any(series.empty for series in [high, low]) or len(high) < period + 1:
                return pd.Series(index=high.index, dtype=float)

            def aroon_up(x):
                try:
                    return ((period - x.argmax()) / period) * 100
                except:
                    return np.nan

            def aroon_down(x):
                try:
                    return ((period - x.argmin()) / period) * 100
                except:
                    return np.nan

            aroon_up_series = high.rolling(window=period + 1, min_periods=period + 1).apply(aroon_up, raw=True)
            aroon_down_series = low.rolling(window=period + 1, min_periods=period + 1).apply(aroon_down, raw=True)

            # Return AROON Oscillator (Up - Down)
            return aroon_up_series - aroon_down_series
        except Exception as e:
            logger.warning(f"AROON calculation failed: {e}")
            return pd.Series(index=high.index, dtype=float)

    def calculate_cci(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20) -> pd.Series:
        """Commodity Channel Index"""
        try:
            if any(series.empty for series in [high, low, close]) or len(close) < period:
                return pd.Series(index=close.index, dtype=float)

            typical_price = (high + low + close) / 3
            sma = typical_price.rolling(window=period, min_periods=1).mean()
            mean_deviation = typical_price.rolling(window=period, min_periods=1).apply(lambda x: np.abs(x - x.mean()).mean())
            cci = (typical_price - sma) / (0.015 * mean_deviation.replace(0, np.nan))
            return cci
        except Exception as e:
            logger.warning(f"CCI calculation failed: {e}")
            return pd.Series(index=close.index, dtype=float)

    def calculate_obv(self, close: pd.Series, volume: pd.Series) -> pd.Series:
        """On-Balance Volume"""
        try:
            if close.empty or volume.empty or len(close) < 2:
                return pd.Series(index=close.index, dtype=float)

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
        except Exception as e:
            logger.warning(f"OBV calculation failed: {e}")
            return pd.Series(index=close.index, dtype=float)

    # Database save functions - MODIFIED to return bool

    async def save_rsi(self, symbol: str, dates: pd.Series, rsi: pd.Series, interval: str = 'weekly', time_period: int = 40) -> bool:
        """Save RSI data to us_rsi table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for i, (date_val, rsi_val) in enumerate(zip(dates, rsi)):
                if pd.notna(rsi_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'RSI',
                        'last_refreshed': None,
                        'interval': interval,
                        'time_period': time_period,
                        'series_type': 'close',
                        'time_zone': 'US/Eastern',
                        'date': date_val,
                        'rsi': round(float(rsi_val), 4),
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_rsi (symbol, indicator, last_refreshed, interval, time_period,
                                       series_type, time_zone, date, rsi, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    rsi = EXCLUDED.rsi, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['time_period'], r['series_type'], r['time_zone'], r['date'],
                     r['rsi'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} RSI records for {symbol}")
                return True
            else:
                logger.warning(f"No RSI records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save RSI for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_macd(self, symbol: str, dates: pd.Series, macd: pd.Series, macd_signal: pd.Series,
                       macd_hist: pd.Series, interval: str = 'daily') -> bool:
        """Save MACD data to us_macd table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for i, (date_val, macd_val, signal_val, hist_val) in enumerate(zip(dates, macd, macd_signal, macd_hist)):
                if pd.notna(macd_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Moving Average Convergence/Divergence (MACD)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'fast_period': 12,
                        'slow_period': 26,
                        'signal_period': 9,
                        'series_type': 'close',
                        'date': date_val,
                        'macd': round(float(macd_val), 4) if pd.notna(macd_val) else None,
                        'macd_signal': round(float(signal_val), 4) if pd.notna(signal_val) else None,
                        'macd_hist': round(float(hist_val), 4) if pd.notna(hist_val) else None,
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_macd (symbol, indicator, last_refreshed, interval, fast_period,
                                        slow_period, signal_period, series_type, date, macd,
                                        macd_signal, macd_hist, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    macd = EXCLUDED.macd, macd_signal = EXCLUDED.macd_signal,
                    macd_hist = EXCLUDED.macd_hist, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['fast_period'], r['slow_period'], r['signal_period'], r['series_type'],
                     r['date'], r['macd'], r['macd_signal'], r['macd_hist'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} MACD records for {symbol}")
                return True
            else:
                logger.warning(f"No MACD records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save MACD for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_bbands(self, symbol: str, dates: pd.Series, upper: pd.Series, middle: pd.Series,
                         lower: pd.Series, interval: str = 'daily') -> bool:
        """Save Bollinger Bands data to us_bbands table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for i, (date_val, upper_val, middle_val, lower_val) in enumerate(zip(dates, upper, middle, lower)):
                if pd.notna(upper_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Bollinger Bands (BBANDS)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'time_period': 20,
                        'deviation_multiplier_upper': 2,
                        'deviation_multiplier_lower': 2,
                        'ma_type': 0,
                        'series_type': 'close',
                        'time_zone': 'US/Eastern Time',
                        'date': date_val,
                        'real_upper_band': round(float(upper_val), 4) if pd.notna(upper_val) else None,
                        'real_middle_band': round(float(middle_val), 4) if pd.notna(middle_val) else None,
                        'real_lower_band': round(float(lower_val), 4) if pd.notna(lower_val) else None,
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_bbands (symbol, indicator, last_refreshed, interval, time_period,
                                          deviation_multiplier_upper, deviation_multiplier_lower,
                                          ma_type, series_type, time_zone, date, real_upper_band,
                                          real_middle_band, real_lower_band, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    real_upper_band = EXCLUDED.real_upper_band, real_middle_band = EXCLUDED.real_middle_band,
                    real_lower_band = EXCLUDED.real_lower_band, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'], r['time_period'],
                     r['deviation_multiplier_upper'], r['deviation_multiplier_lower'], r['ma_type'],
                     r['series_type'], r['time_zone'], r['date'], r['real_upper_band'],
                     r['real_middle_band'], r['real_lower_band'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} BBANDS records for {symbol}")
                return True
            else:
                logger.warning(f"No BBANDS records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save BBANDS for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_vwap(self, symbol: str, dates: pd.Series, close: pd.Series, volume: pd.Series,
                       eod_vwap: pd.Series, avg_vwap: pd.Series, price_vs_vwap: pd.Series) -> bool:
        """Save VWAP data to us_vwap table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for i, (date_val, close_val, vol_val, eod_val, avg_val, pvv_val) in enumerate(
                zip(dates, close, volume, eod_vwap, avg_vwap, price_vs_vwap)):
                if pd.notna(eod_val):
                    records.append({
                        'symbol': symbol,
                        'close': round(float(close_val), 4) if pd.notna(close_val) else None,
                        'volume': int(vol_val) if pd.notna(vol_val) else None,
                        'eod_vwap': round(float(eod_val), 4) if pd.notna(eod_val) else None,
                        'avg_vwap': round(float(avg_val), 4) if pd.notna(avg_val) else None,
                        'price_vs_vwap': round(float(pvv_val), 4) if pd.notna(pvv_val) else None,
                        'date': date_val,
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_vwap (symbol, close, volume, eod_vwap, avg_vwap,
                                        price_vs_vwap, date, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    close = EXCLUDED.close, volume = EXCLUDED.volume,
                    eod_vwap = EXCLUDED.eod_vwap, avg_vwap = EXCLUDED.avg_vwap,
                    price_vs_vwap = EXCLUDED.price_vs_vwap, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['close'], r['volume'], r['eod_vwap'],
                     r['avg_vwap'], r['price_vs_vwap'], r['date'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} VWAP records for {symbol}")
                return True
            else:
                logger.warning(f"No VWAP records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save VWAP for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_atr(self, symbol: str, dates: pd.Series, atr: pd.Series,
                      interval: str = 'daily', time_period: int = 14) -> bool:
        """Save ATR data to us_atr table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for date_val, atr_val in zip(dates, atr):
                if pd.notna(atr_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Average True Range (ATR)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'time_period': time_period,
                        'time_zone': 'US/Eastern Time',
                        'date': date_val,
                        'atr': round(float(atr_val), 6),
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_atr (symbol, indicator, last_refreshed, interval, time_period,
                                       time_zone, date, atr, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    atr = EXCLUDED.atr, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['time_period'], r['time_zone'], r['date'], r['atr'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} ATR records for {symbol}")
                return True
            else:
                logger.warning(f"No ATR records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save ATR for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_stoch(self, symbol: str, dates: pd.Series, slowk: pd.Series, slowd: pd.Series,
                        interval: str = 'daily') -> bool:
        """Save Stochastic data to us_stoch table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for date_val, slowk_val, slowd_val in zip(dates, slowk, slowd):
                if pd.notna(slowk_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Stochastic (STOCH)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'fastk_period': 14,
                        'slowk_period': 3,
                        'slowk_ma_type': 0,
                        'slowd_period': 3,
                        'slowd_ma_type': 0,
                        'time_zone': 'US/Eastern Time',
                        'date': date_val,
                        'slowk': round(float(slowk_val), 4) if pd.notna(slowk_val) else None,
                        'slowd': round(float(slowd_val), 4) if pd.notna(slowd_val) else None,
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_stoch (symbol, indicator, last_refreshed, interval, fastk_period,
                                         slowk_period, slowk_ma_type, slowd_period, slowd_ma_type,
                                         time_zone, date, slowk, slowd, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    slowk = EXCLUDED.slowk, slowd = EXCLUDED.slowd, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['fastk_period'], r['slowk_period'], r['slowk_ma_type'],
                     r['slowd_period'], r['slowd_ma_type'], r['time_zone'],
                     r['date'], r['slowk'], r['slowd'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} STOCH records for {symbol}")
                return True
            else:
                logger.warning(f"No STOCH records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save STOCH for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_mfi(self, symbol: str, dates: pd.Series, mfi: pd.Series,
                      interval: str = 'daily', time_period: int = 14) -> bool:
        """Save MFI data to us_mfi table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for date_val, mfi_val in zip(dates, mfi):
                if pd.notna(mfi_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Money Flow Index (MFI)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'time_period': time_period,
                        'time_zone': 'US/Eastern Time',
                        'date': date_val,
                        'mfi': round(float(mfi_val), 4),
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_mfi (symbol, indicator, last_refreshed, interval, time_period,
                                       time_zone, date, mfi, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    mfi = EXCLUDED.mfi, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['time_period'], r['time_zone'], r['date'], r['mfi'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} MFI records for {symbol}")
                return True
            else:
                logger.warning(f"No MFI records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save MFI for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_roc(self, symbol: str, dates: pd.Series, roc: pd.Series,
                      interval: str = 'weekly', time_period: int = 8) -> bool:
        """Save ROC data to us_roc table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for date_val, roc_val in zip(dates, roc):
                if pd.notna(roc_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Rate of change : ((price/prevPrice)-1)*100',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'time_period': time_period,
                        'series_type': 'close',
                        'time_zone': 'US/Eastern Time',
                        'date': date_val,
                        'roc': round(float(roc_val), 4),
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_roc (symbol, indicator, last_refreshed, interval, time_period,
                                       series_type, time_zone, date, roc, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    roc = EXCLUDED.roc, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['time_period'], r['series_type'], r['time_zone'], r['date'],
                     r['roc'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} ROC records for {symbol}")
                return True
            else:
                logger.warning(f"No ROC records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save ROC for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_sma(self, symbol: str, dates: pd.Series, sma: pd.Series,
                      interval: str = 'weekly', time_period: int = 13) -> bool:
        """Save SMA data to us_sma table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for date_val, sma_val in zip(dates, sma):
                if pd.notna(sma_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Simple Moving Average (SMA)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'time_period': time_period,
                        'series_type': 'close',
                        'time_zone': 'US/Eastern',
                        'date': date_val,
                        'sma': round(float(sma_val), 4),
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_sma (symbol, indicator, last_refreshed, interval, time_period,
                                       series_type, time_zone, date, sma, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    sma = EXCLUDED.sma, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['time_period'], r['series_type'], r['time_zone'], r['date'],
                     r['sma'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} SMA records for {symbol}")
                return True
            else:
                logger.warning(f"No SMA records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save SMA for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_ema(self, symbol: str, dates: pd.Series, ema: pd.Series,
                      interval: str = 'weekly', time_period: int = 10) -> bool:
        """Save EMA data to us_ema table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for date_val, ema_val in zip(dates, ema):
                if pd.notna(ema_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Exponential Moving Average (EMA)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'time_period': time_period,
                        'series_type': 'close',
                        'time_zone': 'US/Eastern',
                        'date': date_val,
                        'ema': round(float(ema_val), 4),
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_ema (symbol, indicator, last_refreshed, interval, time_period,
                                       series_type, time_zone, date, ema, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    ema = EXCLUDED.ema, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['time_period'], r['series_type'], r['time_zone'], r['date'],
                     r['ema'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} EMA records for {symbol}")
                return True
            else:
                logger.warning(f"No EMA records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save EMA for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_adx(self, symbol: str, dates: pd.Series, adx: pd.Series,
                      interval: str = 'weekly', time_period: int = 7) -> bool:
        """Save ADX data to us_adx table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for date_val, adx_val in zip(dates, adx):
                if pd.notna(adx_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Average Directional Movement Index (ADX)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'time_period': time_period,
                        'time_zone': 'US/Eastern Time',
                        'date': date_val,
                        'adx': round(float(adx_val), 4),
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_adx (symbol, indicator, last_refreshed, interval, time_period,
                                       time_zone, date, adx, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    adx = EXCLUDED.adx, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['time_period'], r['time_zone'], r['date'], r['adx'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} ADX records for {symbol}")
                return True
            else:
                logger.warning(f"No ADX records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save ADX for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_wma(self, symbol: str, dates: pd.Series, wma: pd.Series,
                      interval: str = 'weekly', time_period: int = 10) -> bool:
        """Save WMA data to us_wma table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for date_val, wma_val in zip(dates, wma):
                if pd.notna(wma_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Weighted Moving Average (WMA)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'time_period': time_period,
                        'series_type': 'close',
                        'time_zone': 'US/Eastern',
                        'date': date_val,
                        'wma': round(float(wma_val), 4),
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_wma (symbol, indicator, last_refreshed, interval, time_period,
                                       series_type, time_zone, date, wma, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    wma = EXCLUDED.wma, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['time_period'], r['series_type'], r['time_zone'], r['date'],
                     r['wma'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} WMA records for {symbol}")
                return True
            else:
                logger.warning(f"No WMA records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save WMA for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_aroon(self, symbol: str, dates: pd.Series, aroon: pd.Series,
                        interval: str = 'weekly', time_period: int = 5) -> bool:
        """Save AROON data to us_aroon table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for date_val, aroon_val in zip(dates, aroon):
                if pd.notna(aroon_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Aroon (AROON)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'time_period': time_period,
                        'time_zone': 'US/Eastern Time',
                        'date': date_val,
                        'aroon': round(float(aroon_val), 4),
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_aroon (symbol, indicator, last_refreshed, interval, time_period,
                                         time_zone, date, aroon, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    aroon = EXCLUDED.aroon, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['time_period'], r['time_zone'], r['date'], r['aroon'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} AROON records for {symbol}")
                return True
            else:
                logger.warning(f"No AROON records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save AROON for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_cci(self, symbol: str, dates: pd.Series, cci: pd.Series,
                      interval: str = 'daily', time_period: int = 20) -> bool:
        """Save CCI data to us_cci table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for date_val, cci_val in zip(dates, cci):
                if pd.notna(cci_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'Commodity Channel Index (CCI)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'time_period': time_period,
                        'time_zone': 'US/Eastern Time',
                        'date': date_val,
                        'cci': round(float(cci_val), 4),
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_cci (symbol, indicator, last_refreshed, interval, time_period,
                                       time_zone, date, cci, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    cci = EXCLUDED.cci, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['time_period'], r['time_zone'], r['date'], r['cci'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} CCI records for {symbol}")
                return True
            else:
                logger.warning(f"No CCI records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save CCI for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    async def save_obv(self, symbol: str, dates: pd.Series, obv: pd.Series, interval: str = 'weekly') -> bool:
        """Save OBV data to us_obv table. Returns True if successful, False otherwise."""
        conn = None
        try:
            conn = await self.get_connection()

            records = []
            for date_val, obv_val in zip(dates, obv):
                if pd.notna(obv_val):
                    records.append({
                        'symbol': symbol,
                        'indicator': 'On Balance Volume (OBV)',
                        'last_refreshed': date_val,
                        'interval': interval,
                        'time_zone': 'US/Eastern Time',
                        'date': date_val,
                        'obv': round(float(obv_val), 4),
                        'created_at': datetime.now()
                    })

            if records:
                query = '''
                    INSERT INTO us_obv (symbol, indicator, last_refreshed, interval, time_zone,
                                       date, obv, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                    obv = EXCLUDED.obv, created_at = EXCLUDED.created_at
                '''

                values_list = [
                    (r['symbol'], r['indicator'], r['last_refreshed'], r['interval'],
                     r['time_zone'], r['date'], r['obv'], r['created_at'])
                    for r in records
                ]

                await conn.executemany(query, values_list)
                logger.info(f"Saved {len(records)} OBV records for {symbol}")
                return True
            else:
                logger.warning(f"No OBV records to save for {symbol}")
                return False

        except Exception as e:
            logger.error(f"Failed to save OBV for {symbol}: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    # Unified table save function

    async def save_to_unified_table(self, symbol: str, indicator_data: Dict) -> bool:
        """Save all indicators to unified table us_indicators

        Args:
            symbol: Stock symbol
            indicator_data: Dictionary containing all indicator values

        Returns:
            bool: True if successful, False otherwise
        """
        conn = None
        try:
            conn = await self.get_connection()

            # 1. Get stock_name from us_stock_basic
            stock_name_query = "SELECT stock_name FROM us_stock_basic WHERE symbol = $1"
            name_result = await conn.fetchval(stock_name_query, symbol)
            stock_name = name_result if name_result else None

            # 2. Get vwap from us_vwap_base (EOD: End of Day VWAP)
            vwap_query = """
                SELECT vwap
                FROM us_vwap_base
                WHERE symbol = $1 AND date = $2
                ORDER BY datetime DESC
                LIMIT 1
            """
            vwap_result = await conn.fetchrow(vwap_query, symbol, self.execution_date)
            vwap_value = float(vwap_result['vwap']) if vwap_result and vwap_result['vwap'] else None

            # 3. UPSERT to us_indicators
            query = """
            INSERT INTO us_indicators (
                symbol, stock_name, date,
                rsi, macd, macd_signal, macd_hist,
                real_upper_band, real_middle_band, real_lower_band,
                vwap, eod_vwap, avg_vwap, price_vs_vwap,
                atr, slowk, slowd, mfi, roc, sma, ema, adx, wma, aroon, cci, obv,
                created_at
            ) VALUES (
                $1, $2, $3,
                $4, $5, $6, $7,
                $8, $9, $10,
                $11, $12, $13, $14,
                $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
                $27
            )
            ON CONFLICT (date, symbol) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                rsi = EXCLUDED.rsi,
                macd = EXCLUDED.macd,
                macd_signal = EXCLUDED.macd_signal,
                macd_hist = EXCLUDED.macd_hist,
                real_upper_band = EXCLUDED.real_upper_band,
                real_middle_band = EXCLUDED.real_middle_band,
                real_lower_band = EXCLUDED.real_lower_band,
                vwap = EXCLUDED.vwap,
                eod_vwap = EXCLUDED.eod_vwap,
                avg_vwap = EXCLUDED.avg_vwap,
                price_vs_vwap = EXCLUDED.price_vs_vwap,
                atr = EXCLUDED.atr,
                slowk = EXCLUDED.slowk,
                slowd = EXCLUDED.slowd,
                mfi = EXCLUDED.mfi,
                roc = EXCLUDED.roc,
                sma = EXCLUDED.sma,
                ema = EXCLUDED.ema,
                adx = EXCLUDED.adx,
                wma = EXCLUDED.wma,
                aroon = EXCLUDED.aroon,
                cci = EXCLUDED.cci,
                obv = EXCLUDED.obv,
                created_at = EXCLUDED.created_at
            """

            await conn.execute(
                query,
                symbol,
                stock_name,
                self.execution_date,
                indicator_data.get('rsi'),
                indicator_data.get('macd'),
                indicator_data.get('macd_signal'),
                indicator_data.get('macd_hist'),
                indicator_data.get('real_upper_band'),
                indicator_data.get('real_middle_band'),
                indicator_data.get('real_lower_band'),
                vwap_value,  # from us_vwap_base
                indicator_data.get('eod_vwap'),
                indicator_data.get('avg_vwap'),
                indicator_data.get('price_vs_vwap'),
                indicator_data.get('atr'),
                indicator_data.get('slowk'),
                indicator_data.get('slowd'),
                indicator_data.get('mfi'),
                indicator_data.get('roc'),
                indicator_data.get('sma'),
                indicator_data.get('ema'),
                indicator_data.get('adx'),
                indicator_data.get('wma'),
                indicator_data.get('aroon'),
                indicator_data.get('cci'),
                indicator_data.get('obv'),
                datetime.now()
            )

            logger.info(f"[{symbol}] Saved to unified table us_indicators")
            return True

        except Exception as e:
            logger.error(f"[{symbol}] Failed to save to unified table: {e}")
            return False
        finally:
            if conn:
                try:
                    if self.pool:
                        await self.pool.release(conn)
                    else:
                        await conn.close()
                except Exception as close_error:
                    logger.debug(f"Error closing connection: {close_error}")

    # Main calculation function - MODIFIED to return Dict[str, bool] and support partial retry

    async def calculate_and_save_all_indicators(self, symbol: str, indicators_to_process: List[str] = None) -> Dict[str, bool]:
        """Calculate and save technical indicators for a US symbol

        Args:
            symbol: Stock symbol
            indicators_to_process: List of specific indicators to process. If None, process all.

        Returns:
            Dict with indicator names as keys and success status as values
            Example: {'macd': True, 'rsi': False, 'obv': True, ...}
        """
        # If no specific indicators requested, process all
        if indicators_to_process is None:
            indicators_to_process = ALL_INDICATORS

        results = {}
        indicator_data = {}  # Collect all indicator values for unified table

        async with self.symbol_semaphore:
            try:
                logger.info(f"[{symbol}] Starting calculation for {len(indicators_to_process)} indicators")

                # Get daily data
                df_daily = await self.get_daily_data(symbol, limit=500)
                if df_daily.empty:
                    logger.warning(f"[{symbol}] No daily data found")
                    return {ind: False for ind in indicators_to_process}

                # Get weekly data
                latest_friday = self.get_latest_friday(self.execution_date)
                df_weekly = await self.get_weekly_data_from_table(symbol, limit=200)

                if df_weekly.empty:
                    logger.warning(f"[{symbol}] No weekly data available")
                    return {ind: False for ind in indicators_to_process}

                # Filter weekly data
                df_weekly = df_weekly[df_weekly['date'] <= latest_friday]
                if df_weekly.empty:
                    logger.warning(f"[{symbol}] No weekly data up to {latest_friday}")
                    return {ind: False for ind in indicators_to_process}

                df_weekly['date'] = pd.to_datetime(df_weekly['date']).dt.date

                # Extract OHLCV data
                dates_daily = df_daily['date']
                high_daily = df_daily['high']
                low_daily = df_daily['low']
                close_daily = df_daily['close']
                volume_daily = df_daily['volume']

                dates_weekly = df_weekly['date']
                high_weekly = df_weekly['high']
                low_weekly = df_weekly['low']
                close_weekly = df_weekly['close']
                volume_weekly = df_weekly['volume']

                # Process each requested indicator

                # MACD (daily)
                if 'macd' in indicators_to_process:
                    try:
                        macd, macd_signal, macd_hist = self.calculate_macd(close_daily)
                        if len(macd) > 0 and pd.notna(macd.iloc[-1]):
                            results['macd'] = await self.save_macd(symbol,
                                                                   pd.Series([self.execution_date]),
                                                                   pd.Series([macd.iloc[-1]]),
                                                                   pd.Series([macd_signal.iloc[-1]]),
                                                                   pd.Series([macd_hist.iloc[-1]]),
                                                                   interval='daily')
                            # Collect data for unified table
                            if results['macd']:
                                indicator_data['macd'] = float(macd.iloc[-1])
                                indicator_data['macd_signal'] = float(macd_signal.iloc[-1])
                                indicator_data['macd_hist'] = float(macd_hist.iloc[-1])
                        else:
                            results['macd'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] MACD failed: {e}")
                        results['macd'] = False

                # BBANDS (daily)
                if 'bbands' in indicators_to_process:
                    try:
                        bb_upper, bb_middle, bb_lower = self.calculate_bollinger_bands(close_daily, period=20)
                        if len(bb_upper) > 0 and pd.notna(bb_upper.iloc[-1]):
                            results['bbands'] = await self.save_bbands(symbol,
                                                                       pd.Series([self.execution_date]),
                                                                       pd.Series([bb_upper.iloc[-1]]),
                                                                       pd.Series([bb_middle.iloc[-1]]),
                                                                       pd.Series([bb_lower.iloc[-1]]),
                                                                       interval='daily')
                            # Collect data for unified table
                            if results['bbands']:
                                indicator_data['real_upper_band'] = float(bb_upper.iloc[-1])
                                indicator_data['real_middle_band'] = float(bb_middle.iloc[-1])
                                indicator_data['real_lower_band'] = float(bb_lower.iloc[-1])
                        else:
                            results['bbands'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] BBANDS failed: {e}")
                        results['bbands'] = False

                # VWAP (daily)
                if 'vwap' in indicators_to_process:
                    try:
                        vwap_dict = self.calculate_vwap(high_daily, low_daily, close_daily, volume_daily)
                        if len(vwap_dict['eod_vwap']) > 0 and pd.notna(vwap_dict['eod_vwap'].iloc[-1]):
                            results['vwap'] = await self.save_vwap(symbol,
                                                                   pd.Series([self.execution_date]),
                                                                   pd.Series([close_daily.iloc[-1]]),
                                                                   pd.Series([volume_daily.iloc[-1]]),
                                                                   pd.Series([vwap_dict['eod_vwap'].iloc[-1]]),
                                                                   pd.Series([vwap_dict['avg_vwap'].iloc[-1]]),
                                                                   pd.Series([vwap_dict['price_vs_vwap'].iloc[-1]]))
                            # Collect data for unified table
                            if results['vwap']:
                                indicator_data['eod_vwap'] = float(vwap_dict['eod_vwap'].iloc[-1])
                                indicator_data['avg_vwap'] = float(vwap_dict['avg_vwap'].iloc[-1])
                                indicator_data['price_vs_vwap'] = float(vwap_dict['price_vs_vwap'].iloc[-1])
                        else:
                            results['vwap'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] VWAP failed: {e}")
                        results['vwap'] = False

                # ATR (daily)
                if 'atr' in indicators_to_process:
                    try:
                        atr = self.calculate_atr(high_daily, low_daily, close_daily, period=14)
                        if len(atr) > 0 and pd.notna(atr.iloc[-1]):
                            results['atr'] = await self.save_atr(symbol,
                                                                 pd.Series([self.execution_date]),
                                                                 pd.Series([atr.iloc[-1]]),
                                                                 interval='daily', time_period=14)
                            # Collect data for unified table
                            if results['atr']:
                                indicator_data['atr'] = float(atr.iloc[-1])
                        else:
                            results['atr'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] ATR failed: {e}")
                        results['atr'] = False

                # STOCH (daily)
                if 'stoch' in indicators_to_process:
                    try:
                        slowk, slowd = self.calculate_stochastic(high_daily, low_daily, close_daily, k_period=14, d_period=3)
                        if len(slowk) > 0 and pd.notna(slowk.iloc[-1]):
                            results['stoch'] = await self.save_stoch(symbol,
                                                                     pd.Series([self.execution_date]),
                                                                     pd.Series([slowk.iloc[-1]]),
                                                                     pd.Series([slowd.iloc[-1]]),
                                                                     interval='daily')
                            # Collect data for unified table
                            if results['stoch']:
                                indicator_data['slowk'] = float(slowk.iloc[-1])
                                indicator_data['slowd'] = float(slowd.iloc[-1])
                        else:
                            results['stoch'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] STOCH failed: {e}")
                        results['stoch'] = False

                # MFI (daily)
                if 'mfi' in indicators_to_process:
                    try:
                        mfi = self.calculate_mfi(high_daily, low_daily, close_daily, volume_daily, period=14)
                        if len(mfi) > 0 and pd.notna(mfi.iloc[-1]):
                            results['mfi'] = await self.save_mfi(symbol,
                                                                 pd.Series([self.execution_date]),
                                                                 pd.Series([mfi.iloc[-1]]),
                                                                 interval='daily', time_period=14)
                            # Collect data for unified table
                            if results['mfi']:
                                indicator_data['mfi'] = float(mfi.iloc[-1])
                        else:
                            results['mfi'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] MFI failed: {e}")
                        results['mfi'] = False

                # CCI (daily)
                if 'cci' in indicators_to_process:
                    try:
                        cci = self.calculate_cci(high_daily, low_daily, close_daily, period=20)
                        if len(cci) > 0 and pd.notna(cci.iloc[-1]):
                            results['cci'] = await self.save_cci(symbol,
                                                                 pd.Series([self.execution_date]),
                                                                 pd.Series([cci.iloc[-1]]),
                                                                 interval='daily', time_period=20)
                            # Collect data for unified table
                            if results['cci']:
                                indicator_data['cci'] = float(cci.iloc[-1])
                        else:
                            results['cci'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] CCI failed: {e}")
                        results['cci'] = False

                # RSI (weekly)
                if 'rsi' in indicators_to_process:
                    try:
                        rsi = self.calculate_rsi(close_weekly, period=40)
                        if len(rsi) > 0 and pd.notna(rsi.iloc[-1]):
                            results['rsi'] = await self.save_rsi(symbol,
                                                                 pd.Series([self.execution_date]),
                                                                 pd.Series([rsi.iloc[-1]]),
                                                                 interval='weekly', time_period=40)
                            # Collect data for unified table
                            if results['rsi']:
                                indicator_data['rsi'] = float(rsi.iloc[-1])
                        else:
                            results['rsi'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] RSI failed: {e}")
                        results['rsi'] = False

                # ROC (weekly)
                if 'roc' in indicators_to_process:
                    try:
                        roc = self.calculate_roc(close_weekly, period=8)
                        if len(roc) > 0 and pd.notna(roc.iloc[-1]):
                            results['roc'] = await self.save_roc(symbol,
                                                                 pd.Series([self.execution_date]),
                                                                 pd.Series([roc.iloc[-1]]),
                                                                 interval='weekly', time_period=8)
                            # Collect data for unified table
                            if results['roc']:
                                indicator_data['roc'] = float(roc.iloc[-1])
                        else:
                            results['roc'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] ROC failed: {e}")
                        results['roc'] = False

                # SMA (weekly)
                if 'sma' in indicators_to_process:
                    try:
                        sma = self.calculate_sma(close_weekly, period=13)
                        if len(sma) > 0 and pd.notna(sma.iloc[-1]):
                            results['sma'] = await self.save_sma(symbol,
                                                                 pd.Series([self.execution_date]),
                                                                 pd.Series([sma.iloc[-1]]),
                                                                 interval='weekly', time_period=13)
                            # Collect data for unified table
                            if results['sma']:
                                indicator_data['sma'] = float(sma.iloc[-1])
                        else:
                            results['sma'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] SMA failed: {e}")
                        results['sma'] = False

                # EMA (weekly)
                if 'ema' in indicators_to_process:
                    try:
                        ema = self.calculate_ema(close_weekly, period=10)
                        if len(ema) > 0 and pd.notna(ema.iloc[-1]):
                            results['ema'] = await self.save_ema(symbol,
                                                                 pd.Series([self.execution_date]),
                                                                 pd.Series([ema.iloc[-1]]),
                                                                 interval='weekly', time_period=10)
                            # Collect data for unified table
                            if results['ema']:
                                indicator_data['ema'] = float(ema.iloc[-1])
                        else:
                            results['ema'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] EMA failed: {e}")
                        results['ema'] = False

                # ADX (weekly)
                if 'adx' in indicators_to_process:
                    try:
                        adx = self.calculate_adx(high_weekly, low_weekly, close_weekly, period=7)
                        if len(adx) > 0 and pd.notna(adx.iloc[-1]):
                            results['adx'] = await self.save_adx(symbol,
                                                                 pd.Series([self.execution_date]),
                                                                 pd.Series([adx.iloc[-1]]),
                                                                 interval='weekly', time_period=7)
                            # Collect data for unified table
                            if results['adx']:
                                indicator_data['adx'] = float(adx.iloc[-1])
                        else:
                            results['adx'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] ADX failed: {e}")
                        results['adx'] = False

                # WMA (weekly)
                if 'wma' in indicators_to_process:
                    try:
                        wma = self.calculate_wma(close_weekly, period=10)
                        if len(wma) > 0 and pd.notna(wma.iloc[-1]):
                            results['wma'] = await self.save_wma(symbol,
                                                                 pd.Series([self.execution_date]),
                                                                 pd.Series([wma.iloc[-1]]),
                                                                 interval='weekly', time_period=10)
                            # Collect data for unified table
                            if results['wma']:
                                indicator_data['wma'] = float(wma.iloc[-1])
                        else:
                            results['wma'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] WMA failed: {e}")
                        results['wma'] = False

                # AROON (weekly)
                if 'aroon' in indicators_to_process:
                    try:
                        aroon = self.calculate_aroon(high_weekly, low_weekly, period=5)
                        if len(aroon) > 0 and pd.notna(aroon.iloc[-1]):
                            results['aroon'] = await self.save_aroon(symbol,
                                                                     pd.Series([self.execution_date]),
                                                                     pd.Series([aroon.iloc[-1]]),
                                                                     interval='weekly', time_period=5)
                            # Collect data for unified table
                            if results['aroon']:
                                indicator_data['aroon'] = float(aroon.iloc[-1])
                        else:
                            results['aroon'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] AROON failed: {e}")
                        results['aroon'] = False

                # OBV (weekly)
                if 'obv' in indicators_to_process:
                    try:
                        obv = self.calculate_obv(close_weekly, volume_weekly)
                        if len(obv) > 0 and pd.notna(obv.iloc[-1]):
                            results['obv'] = await self.save_obv(symbol,
                                                                 pd.Series([self.execution_date]),
                                                                 pd.Series([obv.iloc[-1]]),
                                                                 interval='weekly')
                            # Collect data for unified table
                            if results['obv']:
                                indicator_data['obv'] = float(obv.iloc[-1])
                        else:
                            results['obv'] = False
                    except Exception as e:
                        logger.error(f"[{symbol}] OBV failed: {e}")
                        results['obv'] = False

                # Save to unified table us_indicators
                try:
                    await self.save_to_unified_table(symbol, indicator_data)
                except Exception as e:
                    logger.error(f"[{symbol}] Failed to save to unified table: {e}")

                success_count = sum(1 for v in results.values() if v)
                total_count = len(results)
                logger.info(f"[{symbol}] Completed: {success_count}/{total_count} indicators successful")

                return results

            except Exception as e:
                logger.error(f"[{symbol}] Critical error: {e}", exc_info=True)
                return {ind: False for ind in indicators_to_process}

    async def process_symbol_batch(self, symbols: List[str], batch_id: int):
        """Process a batch of symbols in parallel"""
        try:
            batch_logger.info(f"Batch {batch_id}: Starting {len(symbols)} symbols")

            # Create tasks for all symbols
            tasks = []
            for symbol in symbols:
                tasks.append((symbol, self.calculate_and_save_all_indicators(symbol, None)))

            # Wait for all tasks
            if not tasks:
                batch_logger.info(f"Batch {batch_id}: No symbols to process")
                return

            symbols_processing = [s for s, _ in tasks]
            results = await asyncio.gather(*[t for _, t in tasks], return_exceptions=True)

            # Analyze results
            full_success = 0
            partial_success = 0
            total_fail = 0

            for symbol, result in zip(symbols_processing, results):
                if isinstance(result, Exception):
                    logger.error(f"Batch {batch_id}: {symbol} exception: {result}")
                    total_fail += 1
                elif isinstance(result, dict):
                    # Count results
                    successful_indicators = sum(1 for v in result.values() if v)
                    total_indicators = len(result)

                    if successful_indicators == total_indicators:
                        full_success += 1
                    elif successful_indicators > 0:
                        partial_success += 1
                        failed_list = [k for k, v in result.items() if not v]
                        logger.warning(f"Batch {batch_id}: {symbol} partial ({successful_indicators}/{total_indicators}), failed: {failed_list}")
                    else:
                        total_fail += 1
                else:
                    total_fail += 1

            batch_logger.info(f"Batch {batch_id}: Complete={full_success}, Partial={partial_success}, Failed={total_fail}")

        except Exception as e:
            logger.error(f"Batch {batch_id}: Batch processing failed: {e}")

    async def run_calculator(self, execution_date: date = None):
        """Main function to run the calculator"""
        try:
            if execution_date is None:
                execution_date = self.execution_date
            else:
                self.execution_date = execution_date

            batch_logger.info(f"Starting US Technical Indicator Calculator for {execution_date}")

            # Initialize connection pool
            await self.init_pool()

            # Get symbols for the execution date
            symbols = await self.get_symbols_from_daily(execution_date)
            if not symbols:
                logger.warning(f"No symbols found for {execution_date}")
                await self.close_pool()
                return

            batch_logger.info(f"Found {len(symbols)} US symbols to process for {execution_date}")

            # Split into batches
            if len(symbols) <= 100:
                batch_size = 25
            elif len(symbols) <= 500:
                batch_size = 50
            else:
                batch_size = 100

            symbol_batches = []
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                symbol_batches.append(batch)

            batch_logger.info(f"Split into {len(symbol_batches)} batches of max {batch_size} symbols each")

            # Create parallel tasks
            tasks = []
            for i, batch in enumerate(symbol_batches):
                if batch:
                    task = asyncio.create_task(self.process_symbol_batch(batch, i+1))
                    tasks.append(task)

            # Wait for all batches to complete
            if tasks:
                batch_logger.info(f"Starting parallel processing of {len(tasks)} batches...")
                await asyncio.gather(*tasks, return_exceptions=True)

            # Close connection pool
            await self.close_pool()

            batch_logger.info(f"Calculation completed for {execution_date}: {len(symbols)} symbols processed")

        except Exception as e:
            logger.error(f"Calculator failed: {e}")
            if self.pool:
                await self.close_pool()
            raise

async def main():
    """Main execution function"""
    # Get database URL from environment
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    # Get max concurrent batches from environment (default: 40)
    max_concurrent_batches = int(os.getenv('MAX_CONCURRENT_BATCHES', '40'))

    batch_logger.info(f"Starting US Calculator with max_concurrent_batches={max_concurrent_batches}")

    # Create calculator instance
    calculator = USTechnicalIndicatorCalculator(
        database_url=database_url,
        max_concurrent_batches=max_concurrent_batches
    )

    # Run calculator (execution_date is already set to latest US market trading day in __init__)
    batch_logger.info(f"\n{'='*80}")
    batch_logger.info(f"Processing date: {calculator.execution_date} (Latest US Market Trading Day)")
    batch_logger.info(f"{'='*80}\n")

    await calculator.run_calculator()

    batch_logger.info(f"\n{'='*80}")
    batch_logger.info(f"Calculation completed: {calculator.execution_date}")
    batch_logger.info(f"{'='*80}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
