# alpha/data/main.py
"""
Alpha Data Collector API
FastAPI 서버 - Cloud Scheduler에서 호출하는 엔드포인트 제공
"""
import os
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import JSONResponse

# Load environment variables
load_dotenv()

# Add paths
import sys
sys.path.append(str(Path(__file__).parent))

# Import utilities
from utils.auth import verify_api_key
from utils.schedule_helper import schedule_checker

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith('postgresql+asyncpg://'):
    DATABASE_URL = DATABASE_URL.replace('postgresql+asyncpg://', 'postgresql://')

DART_API_KEY = os.getenv('DART_API_KEY')
API_SECRET_KEY = os.getenv('API_SECRET_KEY')

# FastAPI app
app = FastAPI(
    title="Alpha Data Collector API",
    description="주식 데이터 수집 자동화 시스템 (Cloud Scheduler 연동)",
    version="1.0.0"
)

# Startup event
@app.on_event("startup")
async def startup_event():
    """서버 시작 시 실행"""
    logger.info("=" * 60)
    logger.info("Alpha Data Collector API Starting")
    logger.info("=" * 60)
    logger.info(f"Environment: {'Railway' if os.getenv('RAILWAY_ENVIRONMENT') else 'Local'}")
    logger.info(f"Database URL: {DATABASE_URL[:50]}...")
    logger.info(f"API Key configured: {bool(API_SECRET_KEY)}")
    logger.info("=" * 60)


# ========================================
# Health Check Endpoints
# ========================================

@app.get("/")
async def root():
    """루트 엔드포인트 - 서버 상태 확인"""
    return {
        "service": "Alpha Data Collector API",
        "status": "running",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health")
async def health_check():
    """헬스체크 - Railway 및 모니터링용"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "database": "connected" if DATABASE_URL else "not configured"
    }


# ========================================
# Phase 2: 한국 실시간 수집 (장중)
# ========================================

@app.post("/collect/kr/intraday")
async def collect_kr_intraday(api_key: str = Depends(verify_api_key)):
    """
    kr_intraday 수집 (30분마다 - 09:00, 16:30 제외)
    Cloud Scheduler: 0,30 9-16 * * 1-5
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/intraday - Started")
    logger.info("=" * 60)

    # 스케줄 체크 (09:00, 16:30 제외)
    should_run, reason = schedule_checker.should_execute_intraday(
        exclude_times=[(9, 0), (16, 30)]
    )

    if not should_run:
        logger.info(f"Skipped: {reason}")
        return {
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    # 실제 수집 실행
    try:
        from kr.krx import KrxCompleteCollector

        collector = KrxCompleteCollector(DATABASE_URL)
        await collector.run_intraday_collection()

        logger.info("kr_intraday collection completed successfully")
        return {
            "status": "success",
            "message": "kr_intraday collection completed",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"kr_intraday collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/kr/intraday-detail")
async def collect_kr_intraday_detail(api_key: str = Depends(verify_api_key)):
    """
    kr_intraday_detail 수집 (30분마다 - 09:00, 16:30 제외)
    Cloud Scheduler: 0,30 9-16 * * 1-5
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/intraday-detail - Started")
    logger.info("=" * 60)

    # 스케줄 체크 (09:00, 16:30 제외)
    should_run, reason = schedule_checker.should_execute_intraday(
        exclude_times=[(9, 0), (16, 30)]
    )

    if not should_run:
        logger.info(f"Skipped: {reason}")
        return {
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    # 실제 수집 실행
    try:
        from kr.krx import KrxCompleteCollector

        collector = KrxCompleteCollector(DATABASE_URL)
        await collector.run_intraday_detail_collection()

        logger.info("kr_intraday_detail collection completed successfully")
        return {
            "status": "success",
            "message": "kr_intraday_detail collection completed",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"kr_intraday_detail collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


# ========================================
# Phase 3: 한국 일일 작업 (장마감 후)
# ========================================

@app.post("/collect/kr/daily-complete")
async def collect_kr_daily_complete(api_key: str = Depends(verify_api_key)):
    """
    한국 주식 일일 데이터 수집 (13개 작업 순차 실행)
    Cloud Scheduler: 5 16 * * 1-5 (월~금 16:05)

    순서:
    1-8: KRX 데이터 (8개 테이블)
    9: krx_index.py (KRX 벤치마크 지수)
    10: index.py (시장지수)
    11: kr_calculator.py (기술적 지표)
    12: bok.py (한국은행 경제지표)
    13: research_crawler.py (네이버 리서치 리포트)
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/daily-complete - Started")
    logger.info("=" * 60)

    results = []
    start_time = datetime.now()

    try:
        from kr.krx import KrxCompleteCollector
        from kr.krx_index import KrxIndexCollector
        from index.index import MarketIndexCollector
        from kr.kr_calculator import KoreanTechnicalIndicatorCalculator
        from kr.bok import collect_bok_indicators
        from kr.research_crawler import NaverResearchCrawler

        collector = KrxCompleteCollector(DATABASE_URL)

        # 1. kr_program_daily_trading
        logger.info("[1/13] Starting kr_program_daily_trading...")
        await collector.run_program_daily_trading_collection()
        results.append({"step": 1, "task": "kr_program_daily_trading", "status": "success"})
        logger.info("[1/13]  kr_program_daily_trading completed")

        # 2. kr_blocktrades
        logger.info("[2/13] Starting kr_blocktrades...")
        await collector.run_blocktrades_collection()
        results.append({"step": 2, "task": "kr_blocktrades", "status": "success"})
        logger.info("[2/13]  kr_blocktrades completed")

        # 3. kr_foreign_ownership
        logger.info("[3/13] Starting kr_foreign_ownership...")
        await collector.run_foreign_ownership_collection()
        results.append({"step": 3, "task": "kr_foreign_ownership", "status": "success"})
        logger.info("[3/13]  kr_foreign_ownership completed")

        # 4. kr_stock_basic
        logger.info("[4/13] Starting kr_stock_basic...")
        await collector.run_stock_basic_collection()
        results.append({"step": 4, "task": "kr_stock_basic", "status": "success"})
        logger.info("[4/13]  kr_stock_basic completed")

        # 5. kr_stock_detail
        logger.info("[5/13] Starting kr_stock_detail...")
        await collector.run_stock_detail_collection()
        results.append({"step": 5, "task": "kr_stock_detail", "status": "success"})
        logger.info("[5/13]  kr_stock_detail completed")

        # 6. kr_investor_daily_trading
        logger.info("[6/13] Starting kr_investor_daily_trading...")
        await collector.run_investor_daily_trading_collection()
        results.append({"step": 6, "task": "kr_investor_daily_trading", "status": "success"})
        logger.info("[6/13]  kr_investor_daily_trading completed")

        # 7. kr_intraday_total
        logger.info("[7/13] Starting kr_intraday_total...")
        await collector.run_intraday_total_collection()
        results.append({"step": 7, "task": "kr_intraday_total", "status": "success"})
        logger.info("[7/13]  kr_intraday_total completed")

        # 8. kr_individual_investor_daily_trading
        logger.info("[8/13] Starting kr_individual_investor_daily_trading...")
        await collector.run_individual_investor_daily_trading_collection()
        results.append({"step": 8, "task": "kr_individual_investor_daily_trading", "status": "success"})
        logger.info("[8/13]  kr_individual_investor_daily_trading completed")

        # 9. krx_index.py (KRX 벤치마크 지수)
        logger.info("[9/13] Starting kr_benchmark_index...")
        today = datetime.now().strftime('%Y%m%d')
        krx_index_collector = KrxIndexCollector(DATABASE_URL)
        await krx_index_collector.run_collection(today, today)
        results.append({"step": 9, "task": "kr_benchmark_index", "status": "success"})
        logger.info("[9/13]  kr_benchmark_index completed")

        # 10. index.py (시장지수)
        logger.info("[10/13] Starting index collection...")
        index_collector = MarketIndexCollector(DATABASE_URL)
        await index_collector.run_collection()
        results.append({"step": 10, "task": "market_index", "status": "success"})
        logger.info("[10/13]  Market index collection completed")

        # 11. kr_calculator.py (기술적 지표)
        logger.info("[11/13] Starting kr_calculator...")
        kr_calc = KoreanTechnicalIndicatorCalculator(DATABASE_URL)
        await kr_calc.run_calculator(daily_mode=True)
        results.append({"step": 11, "task": "kr_indicators", "status": "success"})
        logger.info("[11/13]  kr_calculator completed")

        # 12. bok.py (한국은행 경제지표)
        logger.info("[12/13] Starting bok indicators...")
        bok_api_key = os.getenv('BOK_API_KEY')
        if not bok_api_key:
            logger.warning("[12/13] BOK_API_KEY not found, skipping bok collection")
            results.append({"step": 12, "task": "bok_indicators", "status": "skipped", "reason": "BOK_API_KEY not found"})
        else:
            bok_result = collect_bok_indicators(
                api_key=bok_api_key,
                database_url=DATABASE_URL,
                mode='incremental'
            )
            if bok_result['success']:
                results.append({"step": 12, "task": "bok_indicators", "status": "success", "saved": bok_result['total_saved']})
                logger.info(f"[12/13]  bok indicators completed: {bok_result['total_saved']} records saved")
            else:
                logger.error(f"[12/13]  bok indicators failed: {bok_result.get('message', 'Unknown error')}")
                results.append({"step": 12, "task": "bok_indicators", "status": "failed", "error": bok_result.get('message')})

        # 13. research_crawler.py (네이버 리서치 리포트)
        logger.info("[13/13] Starting research crawler...")
        crawler = NaverResearchCrawler(DATABASE_URL)
        await crawler.initialize()
        try:
            research_result = await crawler.run_daily_collection(max_pages=5)
            results.append({"step": 13, "task": "research_crawler", "status": "success",
                          "total": research_result.get('total', 0),
                          "success": research_result.get('success', 0),
                          "failed": research_result.get('failed', 0)})
            logger.info(f"[13/13]  research crawler completed: {research_result.get('success', 0)} reports collected")
        finally:
            await crawler.close()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"kr_daily_complete: All 13 tasks completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "All 13 tasks completed successfully",
            "duration_seconds": duration,
            "results": results,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"kr_daily_complete failed at step {len(results) + 1}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "completed_tasks": results,
                "failed_at_step": len(results) + 1
            }
        )


@app.post("/admin/kr/truncate-intraday")
async def truncate_kr_intraday(api_key: str = Depends(verify_api_key)):
    """
    kr_intraday, kr_intraday_detail 테이블 TRUNCATE
    Cloud Scheduler: 0 0 * * 0 (일요일 00:00)
    """
    logger.info("=" * 60)
    logger.info("POST /admin/kr/truncate-intraday - Started")
    logger.info("=" * 60)

    try:
        import asyncpg

        conn = await asyncpg.connect(DATABASE_URL)

        try:
            # TRUNCATE kr_intraday
            logger.info("Truncating kr_intraday...")
            await conn.execute("TRUNCATE TABLE kr_intraday")
            logger.info(" kr_intraday truncated")

            # TRUNCATE kr_intraday_detail
            logger.info("Truncating kr_intraday_detail...")
            await conn.execute("TRUNCATE TABLE kr_intraday_detail")
            logger.info(" kr_intraday_detail truncated")

            logger.info("=" * 60)
            logger.info("TRUNCATE completed successfully")
            logger.info("=" * 60)

            return {
                "status": "success",
                "message": "kr_intraday and kr_intraday_detail truncated",
                "timestamp": datetime.now().isoformat()
            }

        finally:
            await conn.close()

    except Exception as e:
        logger.error(f"TRUNCATE failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"TRUNCATE failed: {str(e)}")


# ========================================
# Phase 4: 한국 DART 재무제표
# ========================================

@app.post("/collect/kr/dart/company-info")
async def collect_dart_company_info(api_key: str = Depends(verify_api_key)):
    """
    dart_company_info 수집 (매월 1일 00:00)
    Cloud Scheduler: 0 0 1 * *
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/dart/company-info - Started")
    logger.info("=" * 60)

    try:
        from kr.dart import collect_company_info

        await collect_company_info(DATABASE_URL, DART_API_KEY)

        logger.info("dart_company_info collection completed successfully")
        return {
            "status": "success",
            "message": "dart_company_info collection completed",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"dart_company_info collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/kr/dart/financial-position")
async def collect_dart_financial_position(api_key: str = Depends(verify_api_key)):
    """
    kr_financial_position 수집 (특정월 토요일 00:10)
    Cloud Scheduler: 10 0 * 1,2,4,5,7,8,10,11 6
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/dart/financial-position - Started")
    logger.info("=" * 60)

    # 스케줄 체크 (특정월만)
    should_run, reason = schedule_checker.should_execute_in_months([1, 2, 4, 5, 7, 8, 10, 11])

    if not should_run:
        logger.info(f"Skipped: {reason}")
        return {
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    try:
        from kr.dart import collect_financial_data

        await collect_financial_data(DATABASE_URL, DART_API_KEY)

        logger.info("kr_financial_position collection completed successfully")
        return {
            "status": "success",
            "message": "kr_financial_position collection completed",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"kr_financial_position collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/kr/dart/audit")
async def collect_dart_audit(api_key: str = Depends(verify_api_key)):
    """
    kr_audit 수집 (특정월 월요일 00:10)
    Cloud Scheduler: 10 0 * 1,2,4,5,7,8,10,11 1
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/dart/audit - Started")
    logger.info("=" * 60)

    # 스케줄 체크 (특정월만)
    should_run, reason = schedule_checker.should_execute_in_months([1, 2, 4, 5, 7, 8, 10, 11])

    if not should_run:
        logger.info(f"Skipped: {reason}")
        return {
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    try:
        from kr.dart import collect_audit_data

        await collect_audit_data(DATABASE_URL, DART_API_KEY)

        logger.info("kr_audit collection completed successfully")
        return {
            "status": "success",
            "message": "kr_audit collection completed",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"kr_audit collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/kr/dart/dividends")
async def collect_dart_dividends(api_key: str = Depends(verify_api_key)):
    """
    kr_dividends 수집 (특정월 화요일 00:10)
    Cloud Scheduler: 10 0 * 1,2,4,5,7,8,10,11 2
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/dart/dividends - Started")
    logger.info("=" * 60)

    # 스케줄 체크 (특정월만)
    should_run, reason = schedule_checker.should_execute_in_months([1, 2, 4, 5, 7, 8, 10, 11])

    if not should_run:
        logger.info(f"Skipped: {reason}")
        return {
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    try:
        from kr.dart import collect_dividends_data

        await collect_dividends_data(DATABASE_URL, DART_API_KEY)

        logger.info("kr_dividends collection completed successfully")
        return {
            "status": "success",
            "message": "kr_dividends collection completed",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"kr_dividends collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/kr/dart/largest-shareholder")
async def collect_dart_largest_shareholder(api_key: str = Depends(verify_api_key)):
    """
    kr_largest_shareholder 수집 (특정월 수요일 00:10)
    Cloud Scheduler: 10 0 * 1,2,4,5,7,8,10,11 3
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/dart/largest-shareholder - Started")
    logger.info("=" * 60)

    # 스케줄 체크 (특정월만)
    should_run, reason = schedule_checker.should_execute_in_months([1, 2, 4, 5, 7, 8, 10, 11])

    if not should_run:
        logger.info(f"Skipped: {reason}")
        return {
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    try:
        from kr.dart import collect_shareholder_data

        await collect_shareholder_data(DATABASE_URL, DART_API_KEY)

        logger.info("kr_largest_shareholder collection completed successfully")
        return {
            "status": "success",
            "message": "kr_largest_shareholder collection completed",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"kr_largest_shareholder collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/kr/dart/stock-acquisition")
async def collect_dart_stock_acquisition(api_key: str = Depends(verify_api_key)):
    """
    kr_stockacquisitiondisposal 수집 (특정월 목요일 00:10)
    Cloud Scheduler: 10 0 * 1,2,4,5,7,8,10,11 4
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/dart/stock-acquisition - Started")
    logger.info("=" * 60)

    # 스케줄 체크 (특정월만)
    should_run, reason = schedule_checker.should_execute_in_months([1, 2, 4, 5, 7, 8, 10, 11])

    if not should_run:
        logger.info(f"Skipped: {reason}")
        return {
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    try:
        from kr.dart import collect_stock_acquisition_data

        # Note: is_initial_run=False for scheduled runs
        await collect_stock_acquisition_data(DATABASE_URL, DART_API_KEY, is_initial_run=False)

        logger.info("kr_stockacquisitiondisposal collection completed successfully")
        return {
            "status": "success",
            "message": "kr_stockacquisitiondisposal collection completed",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"kr_stockacquisitiondisposal collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/kr/dart/executive")
async def collect_dart_executive(api_key: str = Depends(verify_api_key)):
    """
    kr_executive 수집 (특정월 금요일 00:10)
    Cloud Scheduler: 10 0 * 1,2,4,5,7,8,10,11 5
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/dart/executive - Started")
    logger.info("=" * 60)

    # 스케줄 체크 (특정월만)
    should_run, reason = schedule_checker.should_execute_in_months([1, 2, 4, 5, 7, 8, 10, 11])

    if not should_run:
        logger.info(f"Skipped: {reason}")
        return {
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    try:
        from kr.dart import collect_executive_data

        await collect_executive_data(DATABASE_URL, DART_API_KEY)

        logger.info("kr_executive collection completed successfully")
        return {
            "status": "success",
            "message": "kr_executive collection completed",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"kr_executive collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/kr/bok-indicators")
async def collect_bok_indicators(api_key: str = Depends(verify_api_key)):
    """
    한국은행 경제지표 수집 (주 1회 - 매주 월요일)
    9개 경제지표 증분 수집 (기준금리, GDP, 환율, 물가지수, 경제심리지수 등)
    Cloud Scheduler: 0 9 * * 1
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/bok-indicators - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from kr.bok import collect_bok_indicators as bok_collect

        bok_api_key = os.getenv('BOK_API_KEY')
        if not bok_api_key:
            raise HTTPException(status_code=500, detail="BOK_API_KEY not configured")

        # 증분 수집 (기본 모드)
        result = bok_collect(
            api_key=bok_api_key,
            database_url=DATABASE_URL,
            mode='incremental'
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"bok-indicators: Completed in {duration:.2f}s")
        logger.info(f"Total saved: {result.get('total_saved', 0)} records")
        logger.info("=" * 60)

        return {
            "status": "success" if result.get('success') else "failed",
            "message": result.get('message', ''),
            "mode": result.get('mode', 'incremental'),
            "total_saved": result.get('total_saved', 0),
            "indicators": result.get('indicators', {}),
            "errors": result.get('errors', []),
            "duration_seconds": duration,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"bok-indicators collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/kr/research-reports")
async def collect_kr_research_reports(api_key: str = Depends(verify_api_key)):
    """
    네이버 증권 리서치 리포트 수집 (매일 - 평일 17:00)
    당일 발행된 증권사 리서치 리포트 수집
    Cloud Scheduler: 0 17 * * 1-5
    """
    logger.info("=" * 60)
    logger.info("POST /collect/kr/research-reports - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from kr.research_crawler import NaverResearchCrawler

        # Initialize crawler
        crawler = NaverResearchCrawler(DATABASE_URL)
        await crawler.initialize()

        try:
            # Run daily collection
            result = await crawler.run_daily_collection(max_pages=5)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info("=" * 60)
            logger.info(f"kr-research-reports: Completed in {duration:.2f}s")
            logger.info(f"Total: {result.get('total', 0)}, Success: {result.get('success', 0)}, Failed: {result.get('failed', 0)}")
            logger.info("=" * 60)

            return {
                "status": result.get('status', 'failed'),
                "message": result.get('message', ''),
                "total_reports": result.get('total', 0),
                "success_count": result.get('success', 0),
                "failed_count": result.get('failed', 0),
                "duration_seconds": duration,
                "timestamp": end_time.isoformat()
            }

        finally:
            await crawler.close()

    except Exception as e:
        logger.error(f"kr-research-reports collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


# ========================================
# Phase 5: 미국 주식
# ========================================

@app.post("/collect/us/daily")
async def collect_us_daily(api_key: str = Depends(verify_api_key)):
    """
    미국 일일 데이터 수집 (화~금 06:05)
    us_daily -> us_vwap -> us_news -> us_etf -> index -> calculator -> indicator_recovery -> us_option -> populate_option_summary -> us_move_index -> us_fred_macro -> us_earnings_calendar (순차)
    Cloud Scheduler: 5 6 * * 2-5
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/daily - Started")
    logger.info("=" * 60)

    results = []
    start_time = datetime.now()

    try:
        from us.alphavantage import DailyCollector, VWAPCollector, get_latest_available_date
        from us.us_news import collect_all_us_stocks
        from us.us_etf import USETFDataCollector
        from index.index import MarketIndexCollector
        from us.us_calculator import USTechnicalIndicatorCalculator
        from us.indicator_recovery import IndicatorRecoveryCollector
        from us.us_option import USOptionCollector
        from us.populate_option_summary import process_date_batch
        from us.us_move_index_collector import DatabaseManager as MoveDbManager, MoveIndexCollector
        from us.us_fred_macro_collector import DatabaseManager as FredDbManager, FredMacroCollector
        from us.finance_data import EarningsCalendarCollector
        import asyncpg

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')
        target_date = await get_latest_available_date(api_key_us)
        call_interval = 0.2  # 300 calls/min

        # 1. us_daily
        logger.info("[1/12] Starting us_daily...")
        daily_collector = DailyCollector(api_key_us, DATABASE_URL, call_interval, target_date)
        await daily_collector.run_collection_optimized()
        results.append({"step": 1, "task": "us_daily", "status": "success"})
        logger.info("[1/12]  us_daily completed")

        # 2. us_vwap
        logger.info("[2/12] Starting us_vwap...")
        vwap_collector = VWAPCollector(api_key_us, DATABASE_URL, call_interval, target_date)
        await vwap_collector.run_collection_optimized()
        results.append({"step": 2, "task": "us_vwap", "status": "success"})
        logger.info("[2/12]  us_vwap completed")

        # 3. us_news
        logger.info("[3/12] Starting us_news...")
        await collect_all_us_stocks(news_delay=0.2)
        results.append({"step": 3, "task": "us_news", "status": "success"})
        logger.info("[3/12]  us_news completed")

        # 4. us_etf
        logger.info("[4/12] Starting us_daily_etf...")
        etf_collector = USETFDataCollector()
        await etf_collector.initialize()
        await etf_collector.collect_all_sector_etfs(force=False)
        await etf_collector.close()
        results.append({"step": 4, "task": "us_daily_etf", "status": "success"})
        logger.info("[4/12]  us_daily_etf completed")

        # 5. index (시장지수)
        logger.info("[5/12] Starting index collection...")
        index_collector = MarketIndexCollector(DATABASE_URL)
        await index_collector.run_collection()
        results.append({"step": 5, "task": "market_index", "status": "success"})
        logger.info("[5/12]  Market index collection completed")

        # 6. us_calculator (기술적 지표)
        logger.info("[6/12] Starting us_calculator...")
        calculator = USTechnicalIndicatorCalculator(database_url=DATABASE_URL, max_concurrent_batches=40)
        await calculator.run_calculator()
        results.append({"step": 6, "task": "us_calculator", "status": "success"})
        logger.info("[6/12]  us_calculator completed")

        # 7. indicator_recovery (누락 지표 복구)
        logger.info("[7/12] Starting indicator_recovery...")
        recovery_collector = IndicatorRecoveryCollector(api_key=api_key_us, database_url=DATABASE_URL, execution_date=target_date)
        await recovery_collector.run_collection()
        results.append({"step": 7, "task": "indicator_recovery", "status": "success"})
        logger.info("[7/12]  indicator_recovery completed")

        # 8. us_option (옵션 데이터)
        logger.info("[8/12] Starting us_option...")
        option_collector = USOptionCollector(api_key_us, DATABASE_URL, call_interval, target_date)
        await option_collector.init_pool()
        await option_collector.run_collection_optimized()
        await option_collector.close_pool()
        results.append({"step": 8, "task": "us_option", "status": "success"})
        logger.info("[8/12]  us_option completed")

        # 9. populate_option_summary (옵션 요약 데이터)
        logger.info("[9/12] Starting populate_option_summary...")
        date_str = target_date.strftime('%Y-%m-%d')
        option_pool = await asyncpg.create_pool(DATABASE_URL, min_size=10, max_size=40, command_timeout=300)
        try:
            await process_date_batch(option_pool, [date_str], gex_only=False)
        finally:
            await option_pool.close()
        results.append({"step": 9, "task": "populate_option_summary", "status": "success"})
        logger.info("[9/12]  populate_option_summary completed")

        # 10. us_move_index (MOVE Index)
        logger.info("[10/12] Starting us_move_index...")
        move_db = MoveDbManager(DATABASE_URL)
        await move_db.initialize()
        try:
            move_collector = MoveIndexCollector(move_db)
            await move_collector.collect(start_date=None, full_refresh=False)
        finally:
            await move_db.close()
        results.append({"step": 10, "task": "us_move_index", "status": "success"})
        logger.info("[10/12]  us_move_index completed")

        # 11. us_fred_macro (FRED 거시경제 데이터)
        logger.info("[11/12] Starting us_fred_macro...")
        fred_api_key = os.getenv('FRED_API_KEY')
        if not fred_api_key:
            logger.warning("[11/12] FRED_API_KEY not found, skipping us_fred_macro")
            results.append({"step": 11, "task": "us_fred_macro", "status": "skipped", "reason": "FRED_API_KEY not found"})
        else:
            fred_db = FredDbManager(DATABASE_URL)
            await fred_db.initialize()
            try:
                fred_collector = FredMacroCollector(fred_api_key, fred_db)
                await fred_collector.collect_all(start_date=None, full_refresh=False)
            finally:
                await fred_db.close()
            results.append({"step": 11, "task": "us_fred_macro", "status": "success"})
        logger.info("[11/12]  us_fred_macro completed")

        # 12. us_earnings_calendar (어닝 캘린더)
        logger.info("[12/12] Starting us_earnings_calendar...")
        earnings_collector = EarningsCalendarCollector(api_key_us, DATABASE_URL, call_interval=0.2, horizon='3month')
        await earnings_collector.init_pool()
        await earnings_collector.run_collection_optimized()
        await earnings_collector.close_pool()
        results.append({"step": 12, "task": "us_earnings_calendar", "status": "success"})
        logger.info("[12/12]  us_earnings_calendar completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_daily: All 12 tasks completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "All 12 tasks completed successfully",
            "duration_seconds": duration,
            "results": results,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_daily failed at step {len(results) + 1}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "completed_tasks": results,
                "failed_at_step": len(results) + 1
            }
        )


@app.post("/collect/us/saturday-complete")
async def collect_us_saturday_complete(api_key: str = Depends(verify_api_key)):
    """
    미국 토요일 완전 수집 (토요일 06:05)
    us_daily -> us_vwap -> us_news -> us_etf -> us_weekly -> index -> calculator -> indicator_recovery -> us_option -> populate_option_summary -> us_move_index -> us_fred_macro -> us_earnings_calendar (순차)
    Cloud Scheduler: 5 6 * * 6
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/saturday-complete - Started")
    logger.info("=" * 60)

    results = []
    start_time = datetime.now()

    try:
        from us.alphavantage import DailyCollector, VWAPCollector, WeeklyCollector, get_latest_available_date
        from us.us_news import collect_all_us_stocks
        from us.us_etf import USETFDataCollector
        from index.index import MarketIndexCollector
        from us.us_calculator import USTechnicalIndicatorCalculator
        from us.indicator_recovery import IndicatorRecoveryCollector
        from us.us_option import USOptionCollector
        from us.populate_option_summary import process_date_batch
        from us.us_move_index_collector import DatabaseManager as MoveDbManager, MoveIndexCollector
        from us.us_fred_macro_collector import DatabaseManager as FredDbManager, FredMacroCollector
        from us.finance_data import EarningsCalendarCollector
        import asyncpg

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')
        target_date = await get_latest_available_date(api_key_us)
        call_interval = 0.2  # 300 calls/min

        # 1. us_daily
        logger.info("[1/13] Starting us_daily...")
        daily_collector = DailyCollector(api_key_us, DATABASE_URL, call_interval, target_date)
        await daily_collector.run_collection_optimized()
        results.append({"step": 1, "task": "us_daily", "status": "success"})
        logger.info("[1/13]  us_daily completed")

        # 2. us_vwap
        logger.info("[2/13] Starting us_vwap...")
        vwap_collector = VWAPCollector(api_key_us, DATABASE_URL, call_interval, target_date)
        await vwap_collector.run_collection_optimized()
        results.append({"step": 2, "task": "us_vwap", "status": "success"})
        logger.info("[2/13]  us_vwap completed")

        # 3. us_news
        logger.info("[3/13] Starting us_news...")
        await collect_all_us_stocks(news_delay=0.2)
        results.append({"step": 3, "task": "us_news", "status": "success"})
        logger.info("[3/13]  us_news completed")

        # 4. us_etf
        logger.info("[4/13] Starting us_daily_etf...")
        etf_collector = USETFDataCollector()
        await etf_collector.initialize()
        await etf_collector.collect_all_sector_etfs(force=False)
        await etf_collector.close()
        results.append({"step": 4, "task": "us_daily_etf", "status": "success"})
        logger.info("[4/13]  us_daily_etf completed")

        # 5. us_weekly
        logger.info("[5/13] Starting us_weekly...")
        weekly_collector = WeeklyCollector(api_key_us, DATABASE_URL, max_concurrent=20)
        await weekly_collector.run_collection()
        results.append({"step": 5, "task": "us_weekly", "status": "success"})
        logger.info("[5/13]  us_weekly completed")

        # 6. index (시장지수)
        logger.info("[6/13] Starting index collection...")
        index_collector = MarketIndexCollector(DATABASE_URL)
        await index_collector.run_collection()
        results.append({"step": 6, "task": "market_index", "status": "success"})
        logger.info("[6/13]  Market index collection completed")

        # 7. us_calculator (기술적 지표)
        logger.info("[7/13] Starting us_calculator...")
        calculator = USTechnicalIndicatorCalculator(database_url=DATABASE_URL, max_concurrent_batches=40)
        await calculator.run_calculator()
        results.append({"step": 7, "task": "us_calculator", "status": "success"})
        logger.info("[7/13]  us_calculator completed")

        # 8. indicator_recovery (누락 지표 복구)
        logger.info("[8/13] Starting indicator_recovery...")
        recovery_collector = IndicatorRecoveryCollector(api_key=api_key_us, database_url=DATABASE_URL, execution_date=target_date)
        await recovery_collector.run_collection()
        results.append({"step": 8, "task": "indicator_recovery", "status": "success"})
        logger.info("[8/13]  indicator_recovery completed")

        # 9. us_option (옵션 데이터)
        logger.info("[9/13] Starting us_option...")
        option_collector = USOptionCollector(api_key_us, DATABASE_URL, call_interval, target_date)
        await option_collector.init_pool()
        await option_collector.run_collection_optimized()
        await option_collector.close_pool()
        results.append({"step": 9, "task": "us_option", "status": "success"})
        logger.info("[9/13]  us_option completed")

        # 10. populate_option_summary (옵션 요약 데이터)
        logger.info("[10/13] Starting populate_option_summary...")
        date_str = target_date.strftime('%Y-%m-%d')
        option_pool = await asyncpg.create_pool(DATABASE_URL, min_size=10, max_size=40, command_timeout=300)
        try:
            await process_date_batch(option_pool, [date_str], gex_only=False)
        finally:
            await option_pool.close()
        results.append({"step": 10, "task": "populate_option_summary", "status": "success"})
        logger.info("[10/13]  populate_option_summary completed")

        # 11. us_move_index (MOVE Index)
        logger.info("[11/13] Starting us_move_index...")
        move_db = MoveDbManager(DATABASE_URL)
        await move_db.initialize()
        try:
            move_collector = MoveIndexCollector(move_db)
            await move_collector.collect(start_date=None, full_refresh=False)
        finally:
            await move_db.close()
        results.append({"step": 11, "task": "us_move_index", "status": "success"})
        logger.info("[11/13]  us_move_index completed")

        # 12. us_fred_macro (FRED 거시경제 데이터)
        logger.info("[12/13] Starting us_fred_macro...")
        fred_api_key = os.getenv('FRED_API_KEY')
        if not fred_api_key:
            logger.warning("[12/13] FRED_API_KEY not found, skipping us_fred_macro")
            results.append({"step": 12, "task": "us_fred_macro", "status": "skipped", "reason": "FRED_API_KEY not found"})
        else:
            fred_db = FredDbManager(DATABASE_URL)
            await fred_db.initialize()
            try:
                fred_collector = FredMacroCollector(fred_api_key, fred_db)
                await fred_collector.collect_all(start_date=None, full_refresh=False)
            finally:
                await fred_db.close()
            results.append({"step": 12, "task": "us_fred_macro", "status": "success"})
        logger.info("[12/13]  us_fred_macro completed")

        # 13. us_earnings_calendar (어닝 캘린더)
        logger.info("[13/13] Starting us_earnings_calendar...")
        earnings_collector = EarningsCalendarCollector(api_key_us, DATABASE_URL, call_interval=0.2, horizon='3month')
        await earnings_collector.init_pool()
        await earnings_collector.run_collection_optimized()
        await earnings_collector.close_pool()
        results.append({"step": 13, "task": "us_earnings_calendar", "status": "success"})
        logger.info("[13/13]  us_earnings_calendar completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_saturday_complete: All 13 tasks completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "All 13 tasks completed successfully",
            "duration_seconds": duration,
            "results": results,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_saturday_complete failed at step {len(results) + 1}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "completed_tasks": results,
                "failed_at_step": len(results) + 1
            }
        )


@app.post("/collect/us/monthly")
async def collect_us_monthly(api_key: str = Depends(verify_api_key)):
    """
    미국 월간 작업 (첫째주 일요일 00:00)
    us_calculator -> us_stock_grade (순차)
    Cloud Scheduler: 0 0 * * 0
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/monthly - Started")
    logger.info("=" * 60)

    # 스케줄 체크 (첫째주만)
    should_run, reason = schedule_checker.should_execute_first_week()

    if not should_run:
        logger.info(f"Skipped: {reason}")
        return {
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    results = []
    start_time = datetime.now()

    try:
        from us.us_calculator import USTechnicalIndicatorCalculator
        from us.us_stock_grade import USStockAnalysisService

        # 1. us_calculator
        logger.info("[1/2] Starting us_calculator...")
        calculator = USTechnicalIndicatorCalculator(database_url=DATABASE_URL, max_concurrent_batches=40)
        await calculator.run_calculator()
        results.append({"step": 1, "task": "us_calculator", "status": "success"})
        logger.info("[1/2]  us_calculator completed")

        # 2. us_stock_grade
        logger.info("[2/2] Starting us_stock_grade...")
        grader = USStockAnalysisService()
        await grader.initialize()
        await grader.analyze_all_us_stocks()
        await grader.close()
        results.append({"step": 2, "task": "us_stock_grade", "status": "success"})
        logger.info("[2/2]  us_stock_grade completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_monthly: All 2 tasks completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "All 2 tasks completed successfully",
            "duration_seconds": duration,
            "results": results,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_monthly failed at step {len(results) + 1}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "completed_tasks": results,
                "failed_at_step": len(results) + 1
            }
        )


@app.post("/collect/us/stock-listing")
async def collect_us_stock_listing(api_key: str = Depends(verify_api_key)):
    """
    US Stock Listing 다운로드 (매주 월요일 00:00)
    NASDAQ + NYSE 상장 종목 리스트 수집 및 GCS 업로드
    Cloud Scheduler: 0 0 * * 1
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/stock-listing - Started")
    logger.info("=" * 60)

    try:
        from us.stock_listing_downloader import StockListingDownloader

        downloader = StockListingDownloader()
        nasdaq_stocks, nyse_stocks, combined_stocks = downloader.run()

        # Count results
        nasdaq_count = len(nasdaq_stocks) if nasdaq_stocks is not None else 0
        nyse_count = len(nyse_stocks) if nyse_stocks is not None else 0
        total_count = len(combined_stocks) if combined_stocks is not None else 0

        logger.info("=" * 60)
        logger.info(f"Stock listing download completed: {total_count} total symbols")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "Stock listing download completed",
            "nasdaq_count": nasdaq_count,
            "nyse_count": nyse_count,
            "total_count": total_count,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Stock listing download failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@app.post("/collect/us/finnhub-symbol")
async def collect_us_finnhub_symbol(api_key: str = Depends(verify_api_key)):
    """
    Finnhub 심볼 수집 (매주 월요일 00:10)
    US 거래소 심볼 정보 수집 (NASDAQ, NYSE, AMEX 등)
    Cloud Scheduler: 10 0 * * 1
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/finnhub-symbol - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from us.finnhub_symbol import FinnhubSymbolCollector

        finnhub_api_key = os.getenv('FINNHUB_API_KEY')
        if not finnhub_api_key:
            raise HTTPException(status_code=500, detail="FINNHUB_API_KEY not found")

        collector = FinnhubSymbolCollector(finnhub_api_key, DATABASE_URL)
        await collector.run()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"Finnhub symbol collection completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "Finnhub symbol collection completed",
            "duration_seconds": duration,
            "timestamp": end_time.isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Finnhub symbol collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/us/stock-basic")
async def collect_us_stock_basic(api_key: str = Depends(verify_api_key)):
    """
    US Stock Basic 정보 수집 (매주 월요일 00:30)
    Alphavantage OVERVIEW API로 종목별 기본 정보 수집
    - 회사명, 섹터, 산업, 시가총액, PER, PEG, 배당수익률, EPS, Beta, 52주 고가/저가 등
    Cloud Scheduler: 30 0 * * 1
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/stock-basic - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from us.alphavantage import AlphaVantageCollector

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')

        # CSV 파일 경로 (stock_listing_downloader가 Railway Volume에 생성한 파일)
        # Railway: /app/log/us_common_stocks_*.csv
        # Local: log/us_common_stocks_*.csv
        if os.getenv('RAILWAY_ENVIRONMENT'):
            csv_file_path = '/app/log/us_common_stocks_*.csv'
        else:
            csv_file_path = 'log/us_common_stocks_*.csv'

        collector = AlphaVantageCollector(api_key_us, DATABASE_URL)

        # CSV에서 심볼 로드 및 수집 실행
        await collector.collect_stock_data(csv_file_path=csv_file_path)

        # 수집 상태 확인
        status_counts, total_records = await collector.get_collection_status()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_stock_basic collection completed in {duration:.2f}s")
        logger.info(f"Total records in us_stock_basic: {total_records}")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "us_stock_basic collection completed",
            "duration_seconds": duration,
            "total_records": total_records,
            "status_counts": status_counts,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_stock_basic collection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/us/financials")
async def collect_us_financials(api_key: str = Depends(verify_api_key)):
    """
    미국 재무제표 수집 (둘째주/넷째주 일요일 00:00, 특정월만)
    Cloud Scheduler: 0 0 * 1,2,4,5,7,8,10,11 0
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/financials - Started")
    logger.info("=" * 60)

    # 스케줄 체크 (둘째주/넷째주 + 특정월)
    should_run, reason = schedule_checker.should_execute_nth_week([2, 4], months=[1, 2, 4, 5, 7, 8, 10, 11])

    if not should_run:
        logger.info(f"Skipped: {reason}")
        return {
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    results = []
    start_time = datetime.now()

    try:
        from us.finance_data import (
            IncomeStatementCollector,
            BalanceSheetCollector,
            CashFlowCollector,
            EarningsEstimatesCollector,
            EarningsCalendarCollector,
            InsiderTransactionsCollector,
            DividendsCollector,
            SplitsCollector
        )

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')

        # 1. Income Statement
        logger.info("[1/8] Starting income_statement...")
        income_collector = IncomeStatementCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await income_collector.run_collection_optimized()
        results.append({"step": 1, "task": "income_statement", "status": "success"})
        logger.info("[1/8]  income_statement completed")

        # 2. Balance Sheet
        logger.info("[2/8] Starting balance_sheet...")
        balance_collector = BalanceSheetCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await balance_collector.run_collection_optimized()
        results.append({"step": 2, "task": "balance_sheet", "status": "success"})
        logger.info("[2/8]  balance_sheet completed")

        # 3. Cash Flow
        logger.info("[3/8] Starting cash_flow...")
        cashflow_collector = CashFlowCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await cashflow_collector.run_collection_optimized()
        results.append({"step": 3, "task": "cash_flow", "status": "success"})
        logger.info("[3/8]  cash_flow completed")

        # 4. Earnings Estimates
        logger.info("[4/8] Starting earnings_estimates...")
        estimates_collector = EarningsEstimatesCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await estimates_collector.run_collection_optimized()
        results.append({"step": 4, "task": "earnings_estimates", "status": "success"})
        logger.info("[4/8]  earnings_estimates completed")

        # 5. Earnings Calendar
        logger.info("[5/8] Starting earnings_calendar...")
        calendar_collector = EarningsCalendarCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await calendar_collector.run_collection_optimized()
        results.append({"step": 5, "task": "earnings_calendar", "status": "success"})
        logger.info("[5/8]  earnings_calendar completed")

        # 6. Insider Transactions
        logger.info("[6/8] Starting insider_transactions...")
        insider_collector = InsiderTransactionsCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await insider_collector.run_collection_optimized()
        results.append({"step": 6, "task": "insider_transactions", "status": "success"})
        logger.info("[6/8]  insider_transactions completed")

        # 7. Dividends
        logger.info("[7/8] Starting dividends...")
        dividends_collector = DividendsCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await dividends_collector.run_collection_optimized()
        results.append({"step": 7, "task": "dividends", "status": "success"})
        logger.info("[7/8]  dividends completed")

        # 8. Splits
        logger.info("[8/8] Starting splits...")
        splits_collector = SplitsCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await splits_collector.run_collection_optimized()
        results.append({"step": 8, "task": "splits", "status": "success"})
        logger.info("[8/8]  splits completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_financials: All 8 tasks completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "All 8 financial tasks completed successfully",
            "duration_seconds": duration,
            "results": results,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_financials failed at step {len(results) + 1}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "completed_tasks": results,
                "failed_at_step": len(results) + 1
            }
        )


@app.post("/admin/create-partitions")
async def create_partitions(api_key: str = Depends(verify_api_key)):
    """
    파티션 자동 생성 (매월 1일 00:00)
    현재월+1, 현재월+2 파티션 생성 (이미 존재하면 스킵)
    Cloud Scheduler: 0 0 1 * *
    """
    logger.info("=" * 60)
    logger.info("POST /admin/create-partitions - Started")
    logger.info("=" * 60)

    try:
        from utils.partition_manager import PartitionManager

        manager = PartitionManager(DATABASE_URL)
        results = await manager.create_future_partitions(months_ahead=2)

        logger.info(f"Partition creation completed")
        logger.info(f"  Created: {len(results['created'])} partitions")
        logger.info(f"  Skipped: {len(results['skipped'])} partitions")
        logger.info(f"  Attached: {len(results['attached'])} partitions")
        logger.info(f"  Errors: {len(results['errors'])} partitions")

        logger.info("=" * 60)
        logger.info("Partition creation completed")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": f"Created {len(results['created'])}, Skipped {len(results['skipped'])}, Attached {len(results['attached'])}, Errors {len(results['errors'])}",
            "results": results,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Partition creation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Partition creation failed: {str(e)}")


# ========================================
# Phase 6: 미국 경제 지표
# ========================================

@app.post("/collect/us/fed-funds-rate")
async def collect_us_fed_funds_rate(api_key: str = Depends(verify_api_key)):
    """
    연방 기금 금리 수집 (매주 목요일 03:00)
    Cloud Scheduler: 0 3 * * 4
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/fed-funds-rate - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from us.finance_data import FederalFundsRateCollector

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')

        logger.info("Starting Federal Funds Rate collection...")
        collector = FederalFundsRateCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await collector.run_collection_optimized()
        logger.info(" Federal Funds Rate collection completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_fed_funds_rate completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "Federal Funds Rate collection completed",
            "duration_seconds": duration,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_fed_funds_rate failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/us/treasury-yield")
async def collect_us_treasury_yield(api_key: str = Depends(verify_api_key)):
    """
    국채 수익률 수집 (매주 토요일 03:00)
    Cloud Scheduler: 0 3 * * 6
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/treasury-yield - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from us.finance_data import TreasuryYieldCollector

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')

        logger.info("Starting Treasury Yield collection...")
        collector = TreasuryYieldCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await collector.run_collection_optimized()
        logger.info(" Treasury Yield collection completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_treasury_yield completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "Treasury Yield collection completed",
            "duration_seconds": duration,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_treasury_yield failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/us/cpi")
async def collect_us_cpi(api_key: str = Depends(verify_api_key)):
    """
    소비자물가지수(CPI) 수집 (매월 2일 22:00)
    Cloud Scheduler: 0 22 2 * *
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/cpi - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from us.finance_data import CPICollector

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')

        logger.info("Starting CPI collection...")
        collector = CPICollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await collector.run_collection_optimized()
        logger.info(" CPI collection completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_cpi completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "CPI collection completed",
            "duration_seconds": duration,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_cpi failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/us/unemployment-rate")
async def collect_us_unemployment_rate(api_key: str = Depends(verify_api_key)):
    """
    실업률 수집 (매월 3일 22:00)
    Cloud Scheduler: 0 22 3 * *
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/unemployment-rate - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from us.finance_data import UnemploymentRateCollector

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')

        logger.info("Starting Unemployment Rate collection...")
        collector = UnemploymentRateCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await collector.run_collection_optimized()
        logger.info(" Unemployment Rate collection completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_unemployment_rate completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "Unemployment Rate collection completed",
            "duration_seconds": duration,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_unemployment_rate failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/us/splits-daily")
async def collect_us_splits_daily(api_key: str = Depends(verify_api_key)):
    """
    주식 분할(Splits) 수집 (화~토 13:00)
    Cloud Scheduler: 0 13 * * 2-6
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/splits-daily - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from us.finance_data import SplitsCollector

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')

        logger.info("Starting Splits collection...")
        collector = SplitsCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await collector.run_collection_optimized()
        logger.info(" Splits collection completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_splits completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "Splits collection completed",
            "duration_seconds": duration,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_splits failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/us/insider-transactions-daily")
async def collect_us_insider_transactions_daily(api_key: str = Depends(verify_api_key)):
    """
    내부자 거래(Insider Transactions) 수집 (화~토 14:00)
    Cloud Scheduler: 0 14 * * 2-6
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/insider-transactions-daily - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from us.finance_data import InsiderTransactionsCollector

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')

        logger.info("Starting Insider Transactions collection...")
        collector = InsiderTransactionsCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await collector.run_collection_optimized()
        logger.info(" Insider Transactions collection completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_insider_transactions completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "Insider Transactions collection completed",
            "duration_seconds": duration,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_insider_transactions failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/us/dividends-quarterly")
async def collect_us_dividends_quarterly(api_key: str = Depends(verify_api_key)):
    """
    배당금(Dividends) 수집 (3,6,9,12월 매주 금요일 16:00)
    Cloud Scheduler: 0 16 * 3,6,9,12 5
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/dividends-quarterly - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from us.finance_data import DividendsCollector

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')

        logger.info("Starting Dividends collection...")
        collector = DividendsCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await collector.run_collection_optimized()
        logger.info(" Dividends collection completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_dividends completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "Dividends collection completed",
            "duration_seconds": duration,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_dividends failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/us/earnings-estimates-quarterly")
async def collect_us_earnings_estimates_quarterly(api_key: str = Depends(verify_api_key)):
    """
    실적 추정(Earnings Estimates) 수집 (분기별 1일, 2일 19:00)
    Cloud Scheduler: 0 19 1,2 1,4,7,10 *
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/earnings-estimates-quarterly - Started")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        from us.finance_data import EarningsEstimatesCollector

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')

        logger.info("Starting Earnings Estimates collection...")
        collector = EarningsEstimatesCollector(api_key_us, DATABASE_URL, call_interval=0.2)
        await collector.run_collection_optimized()
        logger.info(" Earnings Estimates collection completed")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_earnings_estimates completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "Earnings Estimates collection completed",
            "duration_seconds": duration,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_earnings_estimates failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/collect/us/financials-core")
async def collect_us_financials_core(api_key: str = Depends(verify_api_key)):
    """
    핵심 재무제표 3종 동시 병렬 수집 (둘째주/넷째주 일요일 00:00, 특정월만)
    Income Statement, Balance Sheet, Cash Flow를 100회/분 속도로 병렬 수집
    Cloud Scheduler: 0 0 * 1,2,4,5,7,8,10,11 0
    """
    logger.info("=" * 60)
    logger.info("POST /collect/us/financials-core - Started")
    logger.info("=" * 60)

    # 스케줄 체크 (둘째주/넷째주 + 특정월)
    should_run, reason = schedule_checker.should_execute_nth_week([2, 4], months=[1, 2, 4, 5, 7, 8, 10, 11])

    if not should_run:
        logger.info(f"Skipped: {reason}")
        return {
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    start_time = datetime.now()

    try:
        from us.finance_data import (
            IncomeStatementCollector,
            BalanceSheetCollector,
            CashFlowCollector
        )
        import asyncio

        api_key_us = os.getenv('ALPHAVANTAGE_API_KEY')
        # API 속도 100회/분 = 0.6초 간격
        call_interval = 0.6

        logger.info("Starting parallel collection of 3 core financial statements...")
        logger.info("API call rate: 100 calls/min (0.6s interval)")

        # 3개 Collector 초기화
        income_collector = IncomeStatementCollector(api_key_us, DATABASE_URL, call_interval=call_interval)
        balance_collector = BalanceSheetCollector(api_key_us, DATABASE_URL, call_interval=call_interval)
        cashflow_collector = CashFlowCollector(api_key_us, DATABASE_URL, call_interval=call_interval)

        # 동시 병렬 실행
        logger.info("[PARALLEL] Starting Income Statement, Balance Sheet, Cash Flow...")
        results = await asyncio.gather(
            income_collector.run_collection_optimized(),
            balance_collector.run_collection_optimized(),
            cashflow_collector.run_collection_optimized(),
            return_exceptions=True
        )

        # 결과 확인
        tasks_info = [
            {"task": "income_statement", "status": "success" if not isinstance(results[0], Exception) else "failed"},
            {"task": "balance_sheet", "status": "success" if not isinstance(results[1], Exception) else "failed"},
            {"task": "cash_flow", "status": "success" if not isinstance(results[2], Exception) else "failed"}
        ]

        # 에러 로깅
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i+1} failed: {result}")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"us_financials_core: All 3 tasks completed in {duration:.2f}s")
        logger.info("=" * 60)

        return {
            "status": "success",
            "message": "All 3 core financial statements collected in parallel",
            "duration_seconds": duration,
            "results": tasks_info,
            "timestamp": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"us_financials_core failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


# ========================================
# Phase 7 예정: 최종 배포
# ========================================


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"Starting server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
