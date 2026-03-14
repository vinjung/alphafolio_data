"""
Populate us_option_daily_summary table from us_option table
Aggregates millions of option records into daily summaries per symbol

Includes:
- Volume aggregation (call/put)
- Implied Volatility stats (avg/min/max)
- GEX (Gamma Exposure) calculation: Net GEX = Call GEX - Put GEX
  - Call GEX = Σ(gamma × OI × 100 × spot)
  - Put GEX = Σ(gamma × OI × 100 × spot)
  - Positive Net GEX: Dealers hedge opposite → Market stabilizing
  - Negative Net GEX: Dealers hedge same direction → Volatility amplifying

Period: 2024-01-02 to 2025-11-21
Processing: 5 dates at a time (batch size = 5)
Connection pool: max 40 connections

Usage:
    python populate_option_summary.py              # Full population
    python populate_option_summary.py --gex-only   # Update GEX columns only
"""
import asyncio
import asyncpg
import argparse
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env file")

# Remove +asyncpg suffix if present
if DATABASE_URL.startswith('postgresql+asyncpg://'):
    DATABASE_URL = DATABASE_URL.replace('postgresql+asyncpg://', 'postgresql://')


async def get_date_range(conn: asyncpg.Connection):
    """Get available date range from us_option table (optimized - no COUNT)"""
    query = """
    SELECT
        MIN(date) as start_date,
        MAX(date) as end_date
    FROM us_option
    WHERE date >= '2024-01-02' AND date <= '2025-11-21'
    """
    result = await conn.fetchrow(query)
    return result


def generate_date_range(start_date_str: str, end_date_str: str):
    """
    Generate all dates in range (Python-based, no DB query needed)
    Much faster than SELECT DISTINCT on 228M rows
    """
    from datetime import date

    start_date = date.fromisoformat(start_date_str)
    end_date = date.fromisoformat(end_date_str)

    dates = []
    current_date = start_date

    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)

    return dates


async def process_date_batch(pool: asyncpg.Pool, dates: list, gex_only: bool = False):
    """
    Process a batch of dates in parallel

    Args:
        pool: asyncpg connection pool
        dates: list of dates to process
        gex_only: if True, only update GEX columns (skip volume/IV)
    """
    async def process_single_date(date):
        async with pool.acquire() as conn:
            try:
                if not gex_only:
                    # Full aggregation: Volume, IV, GEX, and gamma_flip_distance
                    query = """
                    WITH strike_gex AS (
                        -- Calculate Net GEX per strike price for each symbol
                        SELECT
                            o.symbol,
                            o.date,
                            o.strike,
                            d.close as spot_price,
                            SUM(CASE WHEN o.type = 'call' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                ELSE 0 END) -
                            SUM(CASE WHEN o.type = 'put' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                ELSE 0 END) as strike_net_gex
                        FROM us_option o
                        LEFT JOIN us_daily d ON o.symbol = d.symbol AND o.date = d.date
                        WHERE o.date = $1
                          AND o.strike IS NOT NULL
                          AND o.gamma IS NOT NULL
                        GROUP BY o.symbol, o.date, o.strike, d.close
                    ),
                    gex_with_sign AS (
                        -- Add sign and lag for flip detection
                        SELECT
                            symbol,
                            date,
                            strike,
                            spot_price,
                            strike_net_gex,
                            SIGN(strike_net_gex) as gex_sign,
                            LAG(SIGN(strike_net_gex)) OVER (PARTITION BY symbol, date ORDER BY strike) as prev_gex_sign,
                            LAG(strike) OVER (PARTITION BY symbol, date ORDER BY strike) as prev_strike,
                            LAG(strike_net_gex) OVER (PARTITION BY symbol, date ORDER BY strike) as prev_strike_gex
                        FROM strike_gex
                        WHERE strike_net_gex != 0
                    ),
                    flip_points AS (
                        -- Find gamma flip points (where sign changes)
                        SELECT
                            symbol,
                            date,
                            spot_price,
                            -- Linear interpolation to find exact flip point
                            CASE
                                WHEN prev_strike_gex IS NOT NULL AND strike_net_gex != 0
                                     AND prev_strike_gex != strike_net_gex
                                THEN prev_strike + (strike - prev_strike) *
                                     ABS(prev_strike_gex) / (ABS(prev_strike_gex) + ABS(strike_net_gex))
                                ELSE strike
                            END as flip_strike,
                            ABS(strike - spot_price) as distance_from_spot
                        FROM gex_with_sign
                        WHERE gex_sign != prev_gex_sign
                          AND prev_gex_sign IS NOT NULL
                          AND gex_sign != 0
                          AND prev_gex_sign != 0
                    ),
                    nearest_flip AS (
                        -- Get the nearest flip point to current price for each symbol
                        SELECT DISTINCT ON (symbol, date)
                            symbol,
                            date,
                            spot_price,
                            flip_strike,
                            CASE WHEN spot_price > 0
                                THEN ((flip_strike - spot_price) / spot_price) * 100
                                ELSE NULL
                            END as gamma_flip_distance
                        FROM flip_points
                        WHERE spot_price IS NOT NULL AND spot_price > 0
                        ORDER BY symbol, date, distance_from_spot ASC
                    )
                    INSERT INTO us_option_daily_summary
                        (symbol, date, total_call_volume, total_put_volume,
                         avg_implied_volatility, min_implied_volatility, max_implied_volatility,
                         call_gex, put_gex, net_gex, gex_ratio, gamma_flip_distance)
                    SELECT
                        o.symbol,
                        o.date,
                        SUM(CASE WHEN o.type = 'call' THEN o.volume ELSE 0 END) as total_call_volume,
                        SUM(CASE WHEN o.type = 'put' THEN o.volume ELSE 0 END) as total_put_volume,
                        AVG(o.implied_volatility) as avg_implied_volatility,
                        MIN(o.implied_volatility) as min_implied_volatility,
                        MAX(o.implied_volatility) as max_implied_volatility,
                        -- GEX Calculation: gamma x OI x 100 x spot
                        SUM(CASE WHEN o.type = 'call' AND o.gamma IS NOT NULL AND o.open_interest > 0
                            THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                            ELSE 0 END) as call_gex,
                        SUM(CASE WHEN o.type = 'put' AND o.gamma IS NOT NULL AND o.open_interest > 0
                            THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                            ELSE 0 END) as put_gex,
                        -- Net GEX = Call GEX - Put GEX
                        SUM(CASE WHEN o.type = 'call' AND o.gamma IS NOT NULL AND o.open_interest > 0
                            THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                            ELSE 0 END) -
                        SUM(CASE WHEN o.type = 'put' AND o.gamma IS NOT NULL AND o.open_interest > 0
                            THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                            ELSE 0 END) as net_gex,
                        -- GEX Ratio: Net GEX / (spot x total OI x 100)
                        CASE WHEN SUM(o.open_interest) > 0 AND MAX(d.close) > 0
                            THEN (
                                SUM(CASE WHEN o.type = 'call' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                    THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                    ELSE 0 END) -
                                SUM(CASE WHEN o.type = 'put' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                    THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                    ELSE 0 END)
                            ) / (MAX(d.close) * SUM(o.open_interest) * 100)
                            ELSE 0 END as gex_ratio,
                        -- Gamma Flip Distance from CTE
                        nf.gamma_flip_distance
                    FROM us_option o
                    LEFT JOIN us_daily d ON o.symbol = d.symbol AND o.date = d.date
                    LEFT JOIN nearest_flip nf ON o.symbol = nf.symbol AND o.date = nf.date
                    WHERE o.date = $1
                    GROUP BY o.symbol, o.date, nf.gamma_flip_distance
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        total_call_volume = EXCLUDED.total_call_volume,
                        total_put_volume = EXCLUDED.total_put_volume,
                        avg_implied_volatility = EXCLUDED.avg_implied_volatility,
                        min_implied_volatility = EXCLUDED.min_implied_volatility,
                        max_implied_volatility = EXCLUDED.max_implied_volatility,
                        call_gex = EXCLUDED.call_gex,
                        put_gex = EXCLUDED.put_gex,
                        net_gex = EXCLUDED.net_gex,
                        gex_ratio = EXCLUDED.gex_ratio,
                        gamma_flip_distance = EXCLUDED.gamma_flip_distance
                    """
                else:
                    # GEX only update (for existing records) - includes gamma_flip_distance
                    query = """
                    WITH strike_gex AS (
                        -- Calculate Net GEX per strike price for each symbol
                        SELECT
                            o.symbol,
                            o.date,
                            o.strike,
                            d.close as spot_price,
                            SUM(CASE WHEN o.type = 'call' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                ELSE 0 END) -
                            SUM(CASE WHEN o.type = 'put' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                ELSE 0 END) as strike_net_gex
                        FROM us_option o
                        LEFT JOIN us_daily d ON o.symbol = d.symbol AND o.date = d.date
                        WHERE o.date = $1
                          AND o.strike IS NOT NULL
                          AND o.gamma IS NOT NULL
                        GROUP BY o.symbol, o.date, o.strike, d.close
                    ),
                    gex_with_sign AS (
                        -- Add sign and lag for flip detection
                        SELECT
                            symbol,
                            date,
                            strike,
                            spot_price,
                            strike_net_gex,
                            SIGN(strike_net_gex) as gex_sign,
                            LAG(SIGN(strike_net_gex)) OVER (PARTITION BY symbol, date ORDER BY strike) as prev_gex_sign,
                            LAG(strike) OVER (PARTITION BY symbol, date ORDER BY strike) as prev_strike,
                            LAG(strike_net_gex) OVER (PARTITION BY symbol, date ORDER BY strike) as prev_strike_gex
                        FROM strike_gex
                        WHERE strike_net_gex != 0
                    ),
                    flip_points AS (
                        -- Find gamma flip points (where sign changes)
                        SELECT
                            symbol,
                            date,
                            spot_price,
                            -- Linear interpolation to find exact flip point
                            CASE
                                WHEN prev_strike_gex IS NOT NULL AND strike_net_gex != 0
                                     AND prev_strike_gex != strike_net_gex
                                THEN prev_strike + (strike - prev_strike) *
                                     ABS(prev_strike_gex) / (ABS(prev_strike_gex) + ABS(strike_net_gex))
                                ELSE strike
                            END as flip_strike,
                            ABS(strike - spot_price) as distance_from_spot
                        FROM gex_with_sign
                        WHERE gex_sign != prev_gex_sign
                          AND prev_gex_sign IS NOT NULL
                          AND gex_sign != 0
                          AND prev_gex_sign != 0
                    ),
                    nearest_flip AS (
                        -- Get the nearest flip point to current price for each symbol
                        SELECT DISTINCT ON (symbol, date)
                            symbol,
                            date,
                            spot_price,
                            flip_strike,
                            CASE WHEN spot_price > 0
                                THEN ((flip_strike - spot_price) / spot_price) * 100
                                ELSE NULL
                            END as gamma_flip_distance
                        FROM flip_points
                        WHERE spot_price IS NOT NULL AND spot_price > 0
                        ORDER BY symbol, date, distance_from_spot ASC
                    ),
                    gex_calc AS (
                        SELECT
                            o.symbol,
                            o.date,
                            SUM(CASE WHEN o.type = 'call' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                ELSE 0 END) as call_gex,
                            SUM(CASE WHEN o.type = 'put' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                ELSE 0 END) as put_gex,
                            SUM(CASE WHEN o.type = 'call' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                ELSE 0 END) -
                            SUM(CASE WHEN o.type = 'put' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                ELSE 0 END) as net_gex,
                            CASE WHEN SUM(o.open_interest) > 0 AND MAX(d.close) > 0
                                THEN (
                                    SUM(CASE WHEN o.type = 'call' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                        THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                        ELSE 0 END) -
                                    SUM(CASE WHEN o.type = 'put' AND o.gamma IS NOT NULL AND o.open_interest > 0
                                        THEN o.gamma * o.open_interest * 100 * COALESCE(d.close, 0)
                                        ELSE 0 END)
                                ) / (MAX(d.close) * SUM(o.open_interest) * 100)
                                ELSE 0 END as gex_ratio
                        FROM us_option o
                        LEFT JOIN us_daily d ON o.symbol = d.symbol AND o.date = d.date
                        WHERE o.date = $1
                        GROUP BY o.symbol, o.date
                    )
                    UPDATE us_option_daily_summary s
                    SET
                        call_gex = g.call_gex,
                        put_gex = g.put_gex,
                        net_gex = g.net_gex,
                        gex_ratio = g.gex_ratio,
                        gamma_flip_distance = nf.gamma_flip_distance
                    FROM gex_calc g
                    LEFT JOIN nearest_flip nf ON g.symbol = nf.symbol AND g.date = nf.date
                    WHERE s.symbol = g.symbol AND s.date = g.date
                    """

                await conn.execute(query, date)

                # Get count of symbols processed
                count_query = """
                SELECT COUNT(*) as symbol_count
                FROM us_option_daily_summary
                WHERE date = $1
                """
                result = await conn.fetchrow(count_query, date)
                symbol_count = result['symbol_count']

                return (date, symbol_count, None)

            except Exception as e:
                logger.error(f"Error processing date {date}: {e}")
                return (date, 0, str(e))

    # Process all dates in this batch concurrently
    tasks = [process_single_date(date) for date in dates]
    results = await asyncio.gather(*tasks)

    return results


async def main():
    """Main execution function"""

    # Parse arguments
    parser = argparse.ArgumentParser(description='Populate us_option_daily_summary with GEX')
    parser.add_argument('--gex-only', action='store_true',
                        help='Only update GEX columns (skip volume/IV)')
    parser.add_argument('--start', type=str, default='2024-01-02',
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default='2025-11-21',
                        help='End date (YYYY-MM-DD)')
    args = parser.parse_args()

    mode = "GEX Only" if args.gex_only else "Full (Volume + IV + GEX)"

    print("="*80)
    print("us_option_daily_summary Population")
    print("="*80)
    print(f"Mode: {mode}")
    print(f"Period: {args.start} to {args.end}")
    print(f"Batch size: 5 dates at a time")
    print(f"Connection pool: max 40 connections")
    print("="*80)
    print()

    # Create connection pool
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=10,
        max_size=40,
        command_timeout=300  # 5 minutes timeout per query
    )

    try:
        # Generate all dates in range (Python-based, instant)
        print("Generating date range...")
        all_dates = generate_date_range(args.start, args.end)
        total_dates = len(all_dates)
        print(f"Generated {total_dates} dates to process ({args.start} to {args.end})")
        print()

        # Process in batches of 5
        BATCH_SIZE = 5
        total_batches = (total_dates + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"Processing {total_batches} batches...")
        print()

        start_time = datetime.now()
        total_symbols_processed = 0
        total_errors = 0

        for batch_idx in range(0, total_dates, BATCH_SIZE):
            batch_num = batch_idx // BATCH_SIZE + 1
            date_batch = all_dates[batch_idx:batch_idx + BATCH_SIZE]

            batch_start = datetime.now()
            timestamp = batch_start.strftime('%Y-%m-%d %H:%M:%S')

            print(f"[{timestamp}] [배치 {batch_num}/{total_batches}] 시작: {date_batch[0]} ~ {date_batch[-1]} ({len(date_batch)}개 날짜)")

            results = await process_date_batch(pool, date_batch, gex_only=args.gex_only)
            batch_duration = (datetime.now() - batch_start).total_seconds()

            # Analyze results
            batch_symbols = 0
            batch_errors = 0
            success_dates = []
            error_dates = []

            for date, symbol_count, error in results:
                if error:
                    batch_errors += 1
                    error_dates.append(date)
                    print(f"  [{date}] ERROR: {error}")
                else:
                    batch_symbols += symbol_count
                    success_dates.append((date, symbol_count))

            total_symbols_processed += batch_symbols
            total_errors += batch_errors

            # Print detailed results
            timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            success_count = len(date_batch) - batch_errors
            print(f"[{timestamp_end}] [배치 {batch_num}/{total_batches}] 완료: {success_count}개 | 실패: {batch_errors}개 | 심볼: {batch_symbols:,}개 | 소요: {batch_duration:.1f}초")

            # Show sample of processed dates
            if success_dates:
                sample_size = min(3, len(success_dates))
                print(f"  처리된 날짜 샘플 ({sample_size}개):")
                for date, symbol_count in success_dates[:sample_size]:
                    print(f"    - {date}: {symbol_count:,} symbols")

            print()

        total_duration = (datetime.now() - start_time).total_seconds()

        print("="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Total dates processed: {total_dates}")
        print(f"Total symbols processed: {total_symbols_processed:,}")
        print(f"Total errors: {total_errors}")
        print(f"Total duration: {total_duration:.1f}s ({total_duration/60:.1f} minutes)")
        print(f"Average speed: {total_duration/total_dates:.2f}s per date")
        print()

        # Verify final count
        async with pool.acquire() as conn:
            verify_query = """
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT date) as unique_dates,
                MIN(date) as min_date,
                MAX(date) as max_date,
                COUNT(CASE WHEN net_gex IS NOT NULL THEN 1 END) as gex_populated
            FROM us_option_daily_summary
            WHERE date >= $1 AND date <= $2
            """
            verify_result = await conn.fetchrow(
                verify_query,
                datetime.strptime(args.start, '%Y-%m-%d').date(),
                datetime.strptime(args.end, '%Y-%m-%d').date()
            )

            print("Final verification:")
            print(f"  Total rows: {verify_result['total_rows']:,}")
            print(f"  GEX populated: {verify_result['gex_populated']:,}")
            print(f"  Unique symbols: {verify_result['unique_symbols']:,}")
            print(f"  Unique dates: {verify_result['unique_dates']:,}")
            print(f"  Date range: {verify_result['min_date']} to {verify_result['max_date']}")
            print()

        print("Population completed successfully!")

    finally:
        await pool.close()


if __name__ == '__main__':
    asyncio.run(main())
