# utils/schedule_helper.py
"""
Schedule checking utilities
"""
import asyncpg
from datetime import datetime, date, time
from typing import Tuple, List

class ScheduleChecker:
    """스케줄 판단 시스템"""

    def __init__(self):
        self.last_executions = {}
        self._trading_day_cache = {}  # {(date, market): (bool, str)} 일별 캐시

    async def is_trading_day(self, database_url: str, market: str) -> Tuple[bool, str]:
        """
        trading_calendar 테이블 조회하여 오늘이 거래일인지 판단

        Args:
            database_url: PostgreSQL 접속 URL
            market: "KR" 또는 "US"

        Returns:
            (거래일여부, 이유)
            - (True, "Trading day: 2026-02-16 (Mon)") - 거래일
            - (False, "KR holiday: 설날 연휴 (2026-02-16)") - 공휴일
            - (False, "Weekend: 2026-02-14 (Sat)") - 주말
            - (True, "No calendar data for 2026-02-16, proceeding") - 데이터 없으면 fail-open
        """
        today = date.today()
        cache_key = (today, market)

        # 캐시 히트
        if cache_key in self._trading_day_cache:
            return self._trading_day_cache[cache_key]

        try:
            conn = await asyncpg.connect(database_url)
            try:
                row = await conn.fetchrow(
                    "SELECT is_kr_holiday, kr_holiday_name, is_us_holiday, us_holiday_name, day_of_week "
                    "FROM trading_calendar WHERE date = $1",
                    today
                )
            finally:
                await conn.close()

            if row is None:
                result = (True, f"No calendar data for {today}, proceeding")
                self._trading_day_cache[cache_key] = result
                return result

            day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            day_name = day_names[row['day_of_week']] if row['day_of_week'] is not None else '?'

            if market == "KR":
                is_holiday = row['is_kr_holiday']
                holiday_name = row['kr_holiday_name']
            else:  # US
                is_holiday = row['is_us_holiday']
                holiday_name = row['us_holiday_name']

            if is_holiday:
                if holiday_name:
                    reason = f"{market} holiday: {holiday_name} ({today} {day_name})"
                else:
                    reason = f"Weekend: {today} ({day_name})"
                result = (False, reason)
            else:
                result = (True, f"Trading day: {today} ({day_name})")

            self._trading_day_cache[cache_key] = result
            return result

        except Exception as e:
            # Fail-open: DB 에러 시 수집 진행
            result = (True, f"Calendar check failed ({e}), proceeding")
            # 에러 시에는 캐시하지 않음 (다음 호출에서 재시도)
            return result

    def should_execute_intraday(
        self,
        exclude_times: List[Tuple[int, int]] = [(9, 0), (16, 30)]
    ) -> Tuple[bool, str]:
        """
        장중 데이터 수집 실행 여부 판단 (kr_intraday, kr_intraday_detail)

        Args:
            exclude_times: 제외할 시간 목록 [(시, 분), ...]

        Returns:
            (실행여부, 이유)
        """
        now = datetime.now()

        # 제외 시간 체크
        for hour, minute in exclude_times:
            if now.hour == hour and now.minute == minute:
                return False, f"Excluded time: {hour:02d}:{minute:02d}"

        return True, f"Execute at {now.strftime('%H:%M')}"

    def should_execute_in_months(
        self,
        months: List[int]
    ) -> Tuple[bool, str]:
        """
        특정 월에만 실행 여부 판단

        Args:
            months: 실행할 월 목록 (예: [1, 2, 4, 5, 7, 8, 10, 11])

        Returns:
            (실행여부, 이유)
        """
        now = datetime.now()

        if now.month not in months:
            return False, f"Not in target months: {now.month} not in {months}"

        return True, f"Execute in month {now.month}"

    def should_execute_nth_week(
        self,
        weeks: List[int],
        months: List[int] = None
    ) -> Tuple[bool, str]:
        """
        N째주 실행 여부 판단

        Args:
            weeks: 실행할 주차 목록 (예: [2, 4] = 둘째주, 넷째주)
            months: 특정 월만 실행 (선택사항)

        Returns:
            (실행여부, 이유)
        """
        now = datetime.now()

        # 특정 월 체크 (옵션)
        if months and now.month not in months:
            return False, f"Not in target months: {now.month} not in {months}"

        # 현재 주차 계산 (1~5)
        week_of_month = (now.day - 1) // 7 + 1

        if week_of_month not in weeks:
            return False, f"Not target week: {week_of_month} not in {weeks}"

        return True, f"Execute on week {week_of_month} of month {now.month}"

    def should_execute_on_weekday(
        self,
        weekdays: List[int]
    ) -> Tuple[bool, str]:
        """
        특정 요일 실행 여부 판단

        Args:
            weekdays: 요일 목록 (0=월, 1=화, ..., 6=일)

        Returns:
            (실행여부, 이유)
        """
        now = datetime.now()

        if now.weekday() not in weekdays:
            weekday_names = ['월', '화', '수', '목', '금', '토', '일']
            return False, f"Not target weekday: {weekday_names[now.weekday()]}"

        return True, f"Execute on {['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][now.weekday()]}"

    def should_execute_first_week(self) -> Tuple[bool, str]:
        """
        첫째주 실행 여부 판단 (1~7일)

        Returns:
            (실행여부, 이유)
        """
        now = datetime.now()

        if now.day > 7:
            return False, f"Not first week: day {now.day}"

        return True, f"Execute on first week: day {now.day}"


# 글로벌 인스턴스 (싱글톤)
schedule_checker = ScheduleChecker()
