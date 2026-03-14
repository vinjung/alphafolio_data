"""
Test script for get_individual_investor_data function in krx.py
Tests KRX login session and individual investor data collection
"""
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kr.krx import KrxCompleteCollector, KrxApiClient


def test_individual_investor_data():
    """Test get_individual_investor_data with a single symbol"""

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("ERROR: DATABASE_URL not found in environment")
        return

    # Test symbol: Samsung Electronics
    test_symbol_info = {
        'symbol': '005930',
        'stock_name': 'Samsung Electronics',
        'standard_symbol': 'KR7005930003'
    }

    print("=" * 60)
    print("Testing get_individual_investor_data")
    print("=" * 60)
    print(f"Test symbol: {test_symbol_info['symbol']} ({test_symbol_info['stock_name']})")
    print()

    # Create collector instance
    collector = KrxCompleteCollector(database_url)

    # Step 1: Test KRX login session
    print("[Step 1] Testing KRX login session...")
    session = collector.krx_client.get_session()

    if session:
        print("  SUCCESS: KRX login session acquired")
        print(f"  Session cookies: {len(session.cookies)} cookies")
    else:
        print("  FAILED: Could not acquire KRX login session")
        return

    print()

    # Step 2: Test get_individual_investor_data
    print("[Step 2] Testing get_individual_investor_data...")

    try:
        csv_content = collector.get_individual_investor_data(test_symbol_info)

        if csv_content:
            print("  SUCCESS: CSV data received")
            print(f"  Content length: {len(csv_content)} characters")
            print()
            print("  CSV Preview (first 500 chars):")
            print("-" * 40)
            print(csv_content[:500])
            print("-" * 40)
        else:
            print("  FAILED: No data returned (csv_content is None or empty)")

    except Exception as e:
        print(f"  FAILED: Exception occurred - {e}")

    print()

    # Step 3: Test CSV parsing if data was received
    if csv_content:
        print("[Step 3] Testing parse_individual_investor_csv...")
        try:
            from datetime import datetime
            trade_date = datetime.now().strftime('%Y%m%d')
            parsed_data = collector.parse_individual_investor_csv(csv_content, test_symbol_info, trade_date)

            if parsed_data:
                print("  SUCCESS: CSV parsed successfully")
                print()
                print("  Parsed data:")
                for key, value in parsed_data.items():
                    print(f"    {key}: {value}")
            else:
                print("  FAILED: parse_individual_investor_csv returned None")

        except Exception as e:
            print(f"  FAILED: Exception during parsing - {e}")

    print()
    print("=" * 60)
    print("Test completed")
    print("=" * 60)


if __name__ == "__main__":
    test_individual_investor_data()
