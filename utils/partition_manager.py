"""
Partition Management Utilities
파티션 자동 생성 및 관리
"""
import asyncpg
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class PartitionManager:
    """PostgreSQL 파티션 자동 생성 및 관리"""

    # 파티션이 필요한 테이블 목록
    PARTITIONED_TABLES = [
        'kr_program_daily_trading',
        'kr_blocktrades',
        'kr_investor_daily_trading'
    ]

    def __init__(self, database_url: str):
        self.database_url = database_url

    async def create_future_partitions(
        self,
        months_ahead: int = 2
    ) -> Dict[str, List[str]]:
        """
        미래 N개월치 파티션 자동 생성

        Args:
            months_ahead: 현재월 기준으로 몇 개월 앞까지 생성할지 (기본 2개월)

        Returns:
            {
                "created": ["partition1", "partition2", ...],
                "skipped": ["partition3", "partition4", ...],
                "attached": ["partition5", ...],
                "errors": ["error_msg1", ...]
            }
        """
        conn = await asyncpg.connect(self.database_url)

        results = {
            "created": [],
            "skipped": [],
            "attached": [],
            "errors": []
        }

        try:
            # 현재 월 기준으로 +1월, +2월, ... 계산
            now = datetime.now()
            months_to_create = []

            for i in range(1, months_ahead + 1):
                future_month = now + relativedelta(months=i)
                months_to_create.append((future_month.year, future_month.month))

            logger.info(f"Creating partitions for: {months_to_create}")

            for year, month in months_to_create:
                # 파티션 시작일과 종료일 계산
                start_date = datetime(year, month, 1)
                end_date = start_date + relativedelta(months=1)

                start_str = start_date.strftime('%Y-%m-%d')
                end_str = end_date.strftime('%Y-%m-%d')

                for table in self.PARTITIONED_TABLES:
                    partition_name = f"{table}_{year}_{month:02d}"

                    try:
                        # 파티션 테이블이 존재하는지 확인
                        exists = await conn.fetchval("""
                            SELECT EXISTS (
                                SELECT 1 FROM pg_tables
                                WHERE tablename = $1
                            )
                        """, partition_name)

                        if exists:
                            # 이미 파티션으로 연결되어 있는지 확인
                            is_attached = await conn.fetchval("""
                                SELECT EXISTS (
                                    SELECT 1 FROM pg_class c
                                    JOIN pg_namespace n ON n.oid = c.relnamespace
                                    WHERE c.relname = $1
                                    AND c.relispartition = true
                                )
                            """, partition_name)

                            if is_attached:
                                logger.info(f"Skipped: {partition_name} (already exists)")
                                results["skipped"].append(partition_name)
                                continue
                            else:
                                # 테이블은 있지만 파티션으로 연결 안됨 -> 연결 시도
                                await conn.execute(f"""
                                    ALTER TABLE {table}
                                    ATTACH PARTITION {partition_name}
                                    FOR VALUES FROM ('{start_str}') TO ('{end_str}')
                                """)
                                logger.info(f"Attached: {partition_name}")
                                results["attached"].append(partition_name)
                        else:
                            # 파티션 테이블 자체가 없음 -> 생성
                            await conn.execute(f"""
                                CREATE TABLE {partition_name}
                                PARTITION OF {table}
                                FOR VALUES FROM ('{start_str}') TO ('{end_str}')
                            """)
                            logger.info(f"Created: {partition_name}")
                            results["created"].append(partition_name)

                    except Exception as e:
                        error_msg = f"{partition_name}: {str(e)}"
                        logger.error(f"Failed to create/attach partition: {error_msg}")
                        results["errors"].append(error_msg)

        finally:
            await conn.close()

        return results

    async def get_partition_info(self) -> List[Dict]:
        """
        현재 파티션 정보 조회

        Returns:
            [
                {
                    "table": "kr_program_daily_trading_2025_10",
                    "parent": "kr_program_daily_trading",
                    "bounds": "FOR VALUES FROM ('2025-10-01') TO ('2025-11-01')"
                },
                ...
            ]
        """
        conn = await asyncpg.connect(self.database_url)

        try:
            query = """
            SELECT
                c.relname AS partition_name,
                p.relname AS parent_table,
                pg_get_expr(c.relpartbound, c.oid) AS partition_bounds
            FROM pg_class c
            JOIN pg_inherits i ON i.inhrelid = c.oid
            JOIN pg_class p ON p.oid = i.inhparent
            WHERE c.relispartition = true
            AND p.relname = ANY($1)
            ORDER BY p.relname, c.relname;
            """

            rows = await conn.fetch(query, self.PARTITIONED_TABLES)

            results = []
            for row in rows:
                results.append({
                    "table": row["partition_name"],
                    "parent": row["parent_table"],
                    "bounds": row["partition_bounds"]
                })

            return results

        finally:
            await conn.close()
