"""
kr_individual_investor_daily_trading 테이블 데이터 복구 스크립트

문제점:
- buy_volume / sell_volume: 서로 뒤바뀌어 저장됨
- sell_value: 실제로는 net_value가 저장되어 있음
- net_value: 항상 0으로 저장됨

복구 로직:
1. buy_volume <-> sell_volume 스왑
2. net_value = 현재 sell_value (실제 순매수 데이터)
3. sell_value = buy_value - net_value (계산으로 복구)
"""

import os
import asyncio
import asyncpg
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def analyze_current_data(conn):
    """현재 데이터 상태 분석"""
    logger.info("=== 현재 데이터 상태 분석 ===")

    # 전체 행 수
    total = await conn.fetchval("SELECT COUNT(*) FROM kr_individual_investor_daily_trading")
    logger.info(f"전체 행 수: {total:,}")

    # net_value가 0이 아닌 행 수
    non_zero_net = await conn.fetchval("""
        SELECT COUNT(*) FROM kr_individual_investor_daily_trading
        WHERE inst_net_value != 0 OR retail_net_value != 0 OR foreign_net_value != 0
    """)
    logger.info(f"net_value != 0 행 수: {non_zero_net:,}")

    # 샘플 데이터 (삼성전자 최근 5일)
    sample = await conn.fetch("""
        SELECT date, symbol,
               inst_buy_volume, inst_sell_volume, inst_net_volume,
               inst_buy_value, inst_sell_value, inst_net_value
        FROM kr_individual_investor_daily_trading
        WHERE symbol = '005930'
        ORDER BY date DESC
        LIMIT 5
    """)

    logger.info("\n삼성전자(005930) 현재 저장된 데이터 (수정 전):")
    for row in sample:
        logger.info(f"  {row['date']}: buy_vol={row['inst_buy_volume']:,}, sell_vol={row['inst_sell_volume']:,}, "
                   f"net_vol={row['inst_net_volume']:,}")
        logger.info(f"           buy_val={row['inst_buy_value']:,}, sell_val={row['inst_sell_value']:,}, "
                   f"net_val={row['inst_net_value']:,}")

    return total


async def fix_data(conn, dry_run=True):
    """데이터 복구 실행

    복구 로직:
    - 각 투자자 유형(inst, retail, foreign)에 대해:
      1. new_buy_volume = 현재 sell_volume (스왑)
      2. new_sell_volume = 현재 buy_volume (스왑)
      3. new_net_value = 현재 sell_value (실제 순매수 데이터가 여기 저장됨)
      4. new_sell_value = buy_value - new_net_value (계산)

    - total의 경우:
      1. volume은 스왑
      2. net_value = 현재 sell_value
      3. sell_value = buy_value - net_value
    """

    if dry_run:
        logger.info("\n=== DRY RUN 모드 (실제 변경 없음) ===")
    else:
        logger.info("\n=== 실제 데이터 수정 실행 ===")

    # 복구 쿼리
    update_query = """
    UPDATE kr_individual_investor_daily_trading
    SET
        -- Institution: volume 스왑
        inst_buy_volume = inst_sell_volume,
        inst_sell_volume = inst_buy_volume,
        -- Institution: value 복구 (현재 sell_value에 net_value가 있음)
        inst_net_value = inst_sell_value,
        inst_sell_value = inst_buy_value - inst_sell_value,

        -- Retail: volume 스왑
        retail_buy_volume = retail_sell_volume,
        retail_sell_volume = retail_buy_volume,
        -- Retail: value 복구
        retail_net_value = retail_sell_value,
        retail_sell_value = retail_buy_value - retail_sell_value,

        -- Foreign: volume 스왑
        foreign_buy_volume = foreign_sell_volume,
        foreign_sell_volume = foreign_buy_volume,
        -- Foreign: value 복구
        foreign_net_value = foreign_sell_value,
        foreign_sell_value = foreign_buy_value - foreign_sell_value,

        -- Total: volume 스왑
        total_buy_volume = total_sell_volume,
        total_sell_volume = total_buy_volume,
        -- Total: value 복구
        total_net_value = total_sell_value,
        total_sell_value = total_buy_value - total_sell_value
    """

    if dry_run:
        # 예상 결과 미리보기
        preview_query = """
        SELECT date, symbol,
               -- 현재 값
               inst_buy_volume as curr_buy_vol,
               inst_sell_volume as curr_sell_vol,
               inst_buy_value as curr_buy_val,
               inst_sell_value as curr_sell_val,
               inst_net_value as curr_net_val,
               -- 수정 후 예상 값
               inst_sell_volume as new_buy_vol,
               inst_buy_volume as new_sell_vol,
               inst_sell_value as new_net_val,
               inst_buy_value - inst_sell_value as new_sell_val
        FROM kr_individual_investor_daily_trading
        WHERE symbol = '005930'
        ORDER BY date DESC
        LIMIT 3
        """

        preview = await conn.fetch(preview_query)
        logger.info("\n삼성전자(005930) 수정 예상 결과:")
        for row in preview:
            logger.info(f"\n{row['date']}:")
            logger.info(f"  현재: buy_vol={row['curr_buy_vol']:,}, sell_vol={row['curr_sell_vol']:,}")
            logger.info(f"  수정: buy_vol={row['new_buy_vol']:,}, sell_vol={row['new_sell_vol']:,}")
            logger.info(f"  현재: buy_val={row['curr_buy_val']:,}, sell_val={row['curr_sell_val']:,}, net_val={row['curr_net_val']:,}")
            logger.info(f"  수정: buy_val={row['curr_buy_val']:,}, sell_val={row['new_sell_val']:,}, net_val={row['new_net_val']:,}")

        return 0
    else:
        # 실제 업데이트 실행
        result = await conn.execute(update_query)
        affected = int(result.split()[-1])
        logger.info(f"업데이트된 행 수: {affected:,}")
        return affected


async def verify_fix(conn):
    """수정 결과 검증"""
    logger.info("\n=== 수정 결과 검증 ===")

    # net_value가 0이 아닌 행 수
    non_zero_net = await conn.fetchval("""
        SELECT COUNT(*) FROM kr_individual_investor_daily_trading
        WHERE inst_net_value != 0 OR retail_net_value != 0 OR foreign_net_value != 0
    """)
    logger.info(f"net_value != 0 행 수: {non_zero_net:,}")

    # 데이터 정합성 검증: net_value = buy_value - sell_value 인지 확인
    invalid_count = await conn.fetchval("""
        SELECT COUNT(*) FROM kr_individual_investor_daily_trading
        WHERE inst_net_value != inst_buy_value - inst_sell_value
           OR retail_net_value != retail_buy_value - retail_sell_value
           OR foreign_net_value != foreign_buy_value - foreign_sell_value
    """)
    logger.info(f"정합성 오류 행 수: {invalid_count:,}")

    # 샘플 데이터 확인
    sample = await conn.fetch("""
        SELECT date, symbol,
               inst_buy_volume, inst_sell_volume, inst_net_volume,
               inst_buy_value, inst_sell_value, inst_net_value
        FROM kr_individual_investor_daily_trading
        WHERE symbol = '005930'
        ORDER BY date DESC
        LIMIT 5
    """)

    logger.info("\n삼성전자(005930) 수정 후 데이터:")
    for row in sample:
        logger.info(f"  {row['date']}: buy_vol={row['inst_buy_volume']:,}, sell_vol={row['inst_sell_volume']:,}, "
                   f"net_vol={row['inst_net_volume']:,}")
        logger.info(f"           buy_val={row['inst_buy_value']:,}, sell_val={row['inst_sell_value']:,}, "
                   f"net_val={row['inst_net_value']:,}")
        # 검증
        calc_net = row['inst_buy_value'] - row['inst_sell_value']
        status = "OK" if calc_net == row['inst_net_value'] else "MISMATCH"
        logger.info(f"           검증: buy-sell={calc_net:,}, net={row['inst_net_value']:,} -> {status}")


async def main():
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    if database_url.startswith('postgresql+asyncpg://'):
        database_url = database_url.replace('postgresql+asyncpg://', 'postgresql://')

    conn = await asyncpg.connect(database_url)

    try:
        # 1. 현재 상태 분석
        total = await analyze_current_data(conn)

        # 2. DRY RUN (미리보기)
        await fix_data(conn, dry_run=True)

        # 3. 사용자 확인
        print("\n" + "="*60)
        print(f"총 {total:,}개 행이 수정됩니다.")
        print("="*60)
        confirm = input("실제로 데이터를 수정하시겠습니까? (yes/no): ").strip().lower()

        if confirm == 'yes':
            # 4. 실제 수정 실행
            affected = await fix_data(conn, dry_run=False)

            # 5. 결과 검증
            await verify_fix(conn)

            logger.info(f"\n완료! {affected:,}개 행이 수정되었습니다.")
        else:
            logger.info("작업이 취소되었습니다.")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
