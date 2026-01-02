"""
한국은행 경제지표 데이터 수집 모듈
9개 주요 경제지표를 통합 테이블에 수집

Author: Claude Code Integration
"""
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, List, Optional


# 수집 대상 경제지표 정의
DATA_SPECS = [
    {
        'name': '한국은행 기준금리',
        'stat_code': '722Y001',
        'cycle': 'D',
        'items': ['0101000'],
        'full_start_date': '20200101',
        'overlap_days': 7
    },
    {
        'name': '국내총생산(실질성장률)',
        'stat_code': '200Y101',
        'cycle': 'A',
        'items': ['2010101', '2010102', '2010103', '2010104', '2010105', '2010106', '2010107'],
        'full_start_date': '2020',
        'overlap_years': 1
    },
    {
        'name': '환율',
        'stat_code': '731Y001',
        'cycle': 'D',
        'items': ['0000001', '0000053', '0000002', '0000003'],
        'full_start_date': '20200101',
        'overlap_days': 7
    },
    {
        'name': '생산자물가지수',
        'stat_code': '404Y014',
        'cycle': 'M',
        'items': ['*AA'],
        'full_start_date': '202001',
        'overlap_months': 2
    },
    {
        'name': '소비자물가지수',
        'stat_code': '901Y009',
        'cycle': 'M',
        'items': ['0'],
        'full_start_date': '202001',
        'overlap_months': 2
    },
    {
        'name': '수출물가지수',
        'stat_code': '402Y014',
        'cycle': 'M',
        'items': ['*AA'],
        'full_start_date': '202001',
        'overlap_months': 2
    },
    {
        'name': '수입물가지수',
        'stat_code': '401Y015',
        'cycle': 'M',
        'items': ['*AA'],
        'full_start_date': '202001',
        'overlap_months': 2
    },
    {
        'name': '경제심리지수',
        'stat_code': '513Y001',
        'cycle': 'M',
        'items': ['E1000', 'E2000'],
        'full_start_date': '202001',
        'overlap_months': 2
    },
    {
        'name': '뉴스심리지수',
        'stat_code': '521Y001',
        'cycle': 'D',
        'items': ['A001'],
        'full_start_date': '20200101',
        'overlap_days': 7
    },
    {
        'name': '국내총생산(200Y102)',
        'stat_code': '200Y102',
        'cycle': 'Q',
        'items': ['10111'],
        'full_start_date': '2020Q1',
        'overlap_quarters': 2
    },
]


class BOKDataCollector:
    """한국은행 경제지표 데이터 수집기"""

    def __init__(self, api_key: str, database_url: str, mode: str = 'incremental'):
        """
        Args:
            api_key: BOK API 키
            database_url: PostgreSQL 연결 URL
            mode: 'full' (전체 수집) 또는 'incremental' (증분 수집, 기본)
        """
        self.api_key = api_key
        self.database_url = database_url
        self.base_url = 'https://ecos.bok.or.kr/api'
        self.conn = None
        self.mode = mode
        self.results = {
            'total_saved': 0,
            'indicators': {},
            'errors': []
        }

    def connect_db(self):
        """PostgreSQL 연결"""
        try:
            self.conn = psycopg2.connect(self.database_url)
            return True
        except Exception as e:
            self.results['errors'].append(f"DB 연결 실패: {str(e)}")
            return False

    def get_latest_date(self, stat_code: str) -> Optional[datetime]:
        """DB에서 특정 통계표의 최신 데이터 시점 조회"""
        try:
            # 환율 데이터는 exchange_rate 테이블에서 조회
            if stat_code == '731Y001':
                table_name = 'exchange_rate'
            else:
                table_name = 'bok_economic_indicators'

            with self.conn.cursor() as cur:
                cur.execute(f"""
                    SELECT MAX(time_value)
                    FROM {table_name}
                    WHERE stat_code = %s
                """, (stat_code,))
                result = cur.fetchone()
                return result[0] if result and result[0] else None
        except Exception as e:
            self.results['errors'].append(f"최신 데이터 조회 실패 ({stat_code}): {str(e)}")
            return None

    def calculate_start_date(self, spec: Dict) -> str:
        """수집 모드에 따라 시작일 계산"""
        cycle = spec['cycle']
        stat_code = spec['stat_code']

        # 전체 수집 모드
        if self.mode == 'full':
            return spec['full_start_date']

        # 증분 수집 모드: DB에서 최신일 조회
        latest_date = self.get_latest_date(stat_code)

        if not latest_date:
            # DB에 데이터가 없으면 에러 (전체 수집은 mode='full'로 명시적 실행 필요)
            raise ValueError(f"DB에 {stat_code} 데이터가 없습니다. 최초 수집은 mode='full'로 실행하세요.")

        # 오버랩 기간을 빼서 시작일 계산
        if cycle == 'D':
            overlap_days = spec.get('overlap_days', 7)
            start_date = latest_date - timedelta(days=overlap_days)
            return start_date.strftime('%Y%m%d')

        elif cycle == 'M':
            overlap_months = spec.get('overlap_months', 2)
            start_date = latest_date - relativedelta(months=overlap_months)
            return start_date.strftime('%Y%m')

        elif cycle == 'A':
            overlap_years = spec.get('overlap_years', 1)
            start_date = latest_date - relativedelta(years=overlap_years)
            return start_date.strftime('%Y')

        elif cycle == 'Q':
            # 분기는 2분기 오버랩
            start_date = latest_date - relativedelta(months=6)
            year = start_date.year
            quarter = (start_date.month - 1) // 3 + 1
            return f"{year}Q{quarter}"

        return spec['full_start_date']

    def calculate_end_date(self, cycle: str) -> str:
        """종료일 계산 (현재 + 약간의 여유)"""
        today = datetime.now()

        if cycle == 'D':
            end_date = today + timedelta(days=1)
            return end_date.strftime('%Y%m%d')
        elif cycle == 'M':
            end_date = today + relativedelta(months=1)
            return end_date.strftime('%Y%m')
        elif cycle == 'Q':
            year = today.year
            quarter = (today.month - 1) // 3 + 2
            if quarter > 4:
                year += 1
                quarter = 1
            return f"{year}Q{quarter}"
        elif cycle == 'A':
            return str(today.year + 1)

        return '20991231'

    def parse_time_to_date(self, time_str: str, cycle: str) -> Optional[datetime]:
        """다양한 시점 형식을 DATE로 변환"""
        try:
            time_str = str(time_str).strip()

            if cycle == 'D':
                if len(time_str) == 8:
                    return datetime.strptime(time_str, '%Y%m%d').date()
            elif cycle == 'M':
                if len(time_str) == 6:
                    return datetime.strptime(time_str + '01', '%Y%m%d').date()
            elif cycle == 'Q':
                if 'Q' in time_str:
                    year = int(time_str[:4])
                    quarter = int(time_str[-1])
                    month = (quarter - 1) * 3 + 1
                    return datetime(year, month, 1).date()
            elif cycle == 'A':
                if len(time_str) == 4:
                    return datetime.strptime(time_str + '0101', '%Y%m%d').date()

            return None
        except Exception as e:
            self.results['errors'].append(f"날짜 변환 실패: {time_str} ({cycle}): {str(e)}")
            return None

    def fetch_data(self, spec: Dict, item_code: str, start_date: str, end_date: str) -> List[Dict]:
        """특정 통계의 특정 항목 데이터 조회"""
        stat_code = spec['stat_code']
        cycle = spec['cycle']

        endpoint = f"{self.base_url}/StatisticSearch/{self.api_key}/json/kr/1/100000/{stat_code}/{cycle}/{start_date}/{end_date}/{item_code}"

        try:
            response = requests.get(endpoint, timeout=60)
            response.raise_for_status()
            data = response.json()

            # 에러 체크
            if 'RESULT' in data:
                result = data['RESULT']
                if result.get('CODE') != 'INFO-000':
                    error_msg = result.get('MESSAGE', '알 수 없는 오류')
                    if result.get('CODE') == 'INFO-200':
                        # 데이터 없음은 정상 (신규 데이터 없을 수 있음)
                        return []
                    else:
                        self.results['errors'].append(f"API 경고 ({stat_code}/{item_code}): {error_msg}")
                        return []

            # 데이터 추출
            if 'StatisticSearch' in data and 'row' in data['StatisticSearch']:
                return data['StatisticSearch']['row']
            else:
                return []

        except requests.exceptions.Timeout:
            self.results['errors'].append(f"API 타임아웃: {stat_code}/{item_code}")
            return []
        except requests.exceptions.RequestException as e:
            self.results['errors'].append(f"API 요청 실패 ({stat_code}/{item_code}): {str(e)}")
            return []
        except Exception as e:
            self.results['errors'].append(f"데이터 조회 실패 ({stat_code}/{item_code}): {str(e)}")
            return []

    def save_to_db(self, spec: Dict, all_rows: List[Dict]) -> int:
        """데이터를 PostgreSQL에 저장 (UPSERT)"""
        if not all_rows:
            return 0

        cycle = spec['cycle']
        stat_code = spec['stat_code']

        # 환율 데이터는 exchange_rate 테이블에 저장 (item_code1만 사용)
        is_exchange_rate = (stat_code == '731Y001')

        if is_exchange_rate:
            upsert_query = """
            INSERT INTO exchange_rate (
                stat_code, stat_name, item_code1, item_name1,
                unit_name, cycle, time_value, time_original, data_value,
                created_at, updated_at
            ) VALUES %s
            ON CONFLICT (stat_code, time_value, item_code1)
            DO UPDATE SET
                stat_name = EXCLUDED.stat_name,
                item_name1 = EXCLUDED.item_name1,
                unit_name = EXCLUDED.unit_name,
                cycle = EXCLUDED.cycle,
                data_value = EXCLUDED.data_value,
                updated_at = CURRENT_TIMESTAMP
            """
        else:
            upsert_query = """
            INSERT INTO bok_economic_indicators (
                stat_code, stat_name, item_code1, item_name1,
                item_code2, item_name2, item_code3, item_name3, item_code4, item_name4,
                unit_name, wgt, cycle, time_value, time_original, data_value,
                created_at, updated_at
            ) VALUES %s
            ON CONFLICT (stat_code, time_value, item_code1, item_code2, item_code3, item_code4)
            DO UPDATE SET
                stat_name = EXCLUDED.stat_name,
                item_name1 = EXCLUDED.item_name1,
                item_name2 = EXCLUDED.item_name2,
                item_name3 = EXCLUDED.item_name3,
                item_name4 = EXCLUDED.item_name4,
                unit_name = EXCLUDED.unit_name,
                wgt = EXCLUDED.wgt,
                data_value = EXCLUDED.data_value,
                updated_at = CURRENT_TIMESTAMP
            """

        # 데이터 변환
        values = []
        for row in all_rows:
            time_original = row.get('TIME', '')
            time_value = self.parse_time_to_date(time_original, cycle)

            if not time_value:
                continue

            # 데이터값 파싱
            data_value = row.get('DATA_VALUE')
            if data_value:
                try:
                    data_value = float(data_value)
                except:
                    data_value = None

            # 환율 테이블용 데이터 (item_code1만 사용, wgt 제외)
            if is_exchange_rate:
                values.append((
                    row.get('STAT_CODE', ''),
                    row.get('STAT_NAME', ''),
                    row.get('ITEM_CODE1', ''),
                    row.get('ITEM_NAME1', ''),
                    row.get('UNIT_NAME', ''),
                    cycle,
                    time_value,
                    time_original,
                    data_value,
                    datetime.now(),
                    datetime.now()
                ))
            else:
                # 가중치 파싱 (bok_economic_indicators만 사용)
                wgt = row.get('WGT')
                if wgt:
                    try:
                        wgt = float(wgt)
                    except:
                        wgt = None

                values.append((
                    row.get('STAT_CODE', ''),
                    row.get('STAT_NAME', ''),
                    row.get('ITEM_CODE1', ''),
                    row.get('ITEM_NAME1', ''),
                    row.get('ITEM_CODE2', ''),
                    row.get('ITEM_NAME2', ''),
                    row.get('ITEM_CODE3', ''),
                    row.get('ITEM_NAME3', ''),
                    row.get('ITEM_CODE4', ''),
                    row.get('ITEM_NAME4', ''),
                    row.get('UNIT_NAME', ''),
                    wgt,
                    cycle,
                    time_value,
                    time_original,
                    data_value,
                    datetime.now(),
                    datetime.now()
                ))

        if not values:
            return 0

        try:
            with self.conn.cursor() as cur:
                execute_values(cur, upsert_query, values)
                self.conn.commit()
                return len(values)

        except Exception as e:
            self.conn.rollback()
            self.results['errors'].append(f"데이터 저장 실패 ({spec['name']}): {str(e)}")
            return 0

    def collect_indicator(self, spec: Dict) -> int:
        """특정 경제지표의 모든 항목 수집"""
        # 수집 기간 계산
        start_date = self.calculate_start_date(spec)
        end_date = self.calculate_end_date(spec['cycle'])

        indicator_saved = 0

        for item_code in spec['items']:
            # 데이터 조회
            rows = self.fetch_data(spec, item_code, start_date, end_date)

            if rows:
                # DB 저장
                saved = self.save_to_db(spec, rows)
                indicator_saved += saved

        # 결과 기록
        self.results['indicators'][spec['stat_code']] = {
            'name': spec['name'],
            'saved': indicator_saved,
            'period': f"{start_date} ~ {end_date}"
        }

        return indicator_saved

    def collect(self) -> Dict:
        """
        전체 수집 프로세스 실행

        Returns:
            Dict: 수집 결과
            {
                'success': bool,
                'total_saved': int,
                'indicators': {...},
                'errors': [...]
            }
        """
        try:
            # DB 연결
            if not self.connect_db():
                return {
                    'success': False,
                    'message': 'DB 연결 실패',
                    'total_saved': 0,
                    'indicators': {},
                    'errors': self.results['errors']
                }

            # 각 경제지표 수집
            for spec in DATA_SPECS:
                saved = self.collect_indicator(spec)
                self.results['total_saved'] += saved

            return {
                'success': True,
                'message': f'{len(DATA_SPECS)}개 경제지표 수집 완료',
                'mode': self.mode,
                'total_saved': self.results['total_saved'],
                'indicators': self.results['indicators'],
                'errors': self.results['errors']
            }

        except Exception as e:
            self.results['errors'].append(f"수집 프로세스 오류: {str(e)}")
            return {
                'success': False,
                'message': str(e),
                'total_saved': self.results['total_saved'],
                'indicators': self.results['indicators'],
                'errors': self.results['errors']
            }

        finally:
            if self.conn:
                self.conn.close()


def collect_bok_indicators(api_key: str, database_url: str, mode: str = 'incremental') -> Dict:
    """
    한국은행 경제지표 수집 (진입점 함수)

    Args:
        api_key: BOK API 키
        database_url: PostgreSQL 연결 URL
        mode: 'full' 또는 'incremental' (기본)

    Returns:
        Dict: 수집 결과
    """
    collector = BOKDataCollector(api_key, database_url, mode)
    return collector.collect()
