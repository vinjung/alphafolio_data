# utils/schedule_helper.py
"""
Schedule checking utilities
"""
from datetime import datetime, time
from typing import Tuple, List

class ScheduleChecker:
    """스케줄 판단 시스템"""

    def __init__(self):
        self.last_executions = {}

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
