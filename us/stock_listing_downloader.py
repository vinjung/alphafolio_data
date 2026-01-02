# alpha/data/us/stock_listing_downloader.py
"""
US Stock Listing Downloader
Downloads NASDAQ and NYSE listings from datahub.io
"""

import requests
import pandas as pd
import os
import logging
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from pathlib import Path
from google.cloud import storage
from google.oauth2 import service_account
import json
from dotenv import load_dotenv
import asyncpg
import asyncio

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('log/stock_listing_downloader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StockListingDownloader:
    def __init__(self, database_url: Optional[str] = None):
        """Initialize the downloader with datahub.io endpoints"""

        # Database URL
        self.database_url = database_url or os.getenv('DATABASE_URL')

        # Direct download URLs from datahub.io
        self.download_urls = {
            'nasdaq': 'https://datahub.io/core/nasdaq-listings/r/nasdaq-listed.csv',
            'nyse': 'https://datahub.io/core/nyse-other-listings/r/nyse-listed.csv'
        }

        # Headers for requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        # Local output directory - Use Railway Volume (/app/log) if available
        if os.getenv('RAILWAY_ENVIRONMENT'):
            # Railway environment: use Volume mount path
            self.output_dir = Path('/app/log')
        else:
            # Local environment: use log folder
            self.output_dir = Path(__file__).parent.parent / 'log'

        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {self.output_dir}")

        # Google Cloud Storage settings
        self.gcs_bucket_name = 'dducksang-kr-data'
        self.gcs_client = None

        # Initialize GCS client if credentials are available
        self.init_gcs_client()
    
    def init_gcs_client(self):
        """Initialize Google Cloud Storage client using GCP_SA_KEY (same as KRX)"""
        try:
            # Use same environment variable as KRX
            gcp_sa_key_str = os.getenv('GCP_SA_KEY')
            if not gcp_sa_key_str:
                logger.warning("GCP_SA_KEY environment variable not found. GCS upload will be skipped.")
                return
            
            # Parse JSON credentials
            credentials_info = json.loads(gcp_sa_key_str)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            self.gcs_client = storage.Client(credentials=credentials)
            logger.info("Google Cloud Storage client initialized successfully")
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse GCP_SA_KEY JSON: {str(e)}")
            self.gcs_client = None
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {str(e)}")
            self.gcs_client = None
    
    def download_csv(self, exchange: str) -> Optional[pd.DataFrame]:
        """Download CSV file from datahub.io"""
        try:
            url = self.download_urls.get(exchange.lower())
            if not url:
                logger.error(f"Unknown exchange: {exchange}")
                return None
            
            logger.info(f"Downloading {exchange.upper()} listings from {url}")
            
            # Download CSV directly
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # Save raw CSV first
            date_str = datetime.now().strftime('%Y%m%d')
            filename = f'{exchange.lower()}-listed_{date_str}.csv'
            local_path = self.output_dir / filename
            
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded {exchange.upper()} listings to {local_path}")
            
            # Parse CSV into DataFrame
            df = pd.read_csv(local_path)
            logger.info(f"Loaded {len(df)} records from {exchange.upper()}")
            
            # Display columns for analysis
            logger.info(f"Columns in {exchange.upper()} data: {df.columns.tolist()}")
            
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {exchange.upper()} listings: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error processing {exchange.upper()} data: {str(e)}")
            return None
    
    def upload_to_gcs(self, local_path: Path) -> bool:
        """Upload file to Google Cloud Storage"""
        if not self.gcs_client:
            logger.warning("GCS client not available. Skipping upload.")
            return False
        
        try:
            bucket = self.gcs_client.bucket(self.gcs_bucket_name)
            blob_name = f"us/{local_path.name}"
            blob = bucket.blob(blob_name)
            
            blob.upload_from_filename(str(local_path))
            logger.info(f"Uploaded {local_path.name} to gs://{self.gcs_bucket_name}/{blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload to GCS: {str(e)}")
            return False
    
    def filter_common_stocks(self, df: pd.DataFrame, exchange: str) -> pd.DataFrame:
        """Filter for common stocks only"""
        original_count = len(df)
        
        # Check available columns
        logger.info(f"\n{exchange.upper()} DataFrame Info:")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"Total records: {original_count}")
        
        # Different filtering strategies based on exchange and available columns
        filtered_df = df.copy()
        
        if exchange.lower() == 'nasdaq':
            # NASDAQ specific filtering
            if 'ETF' in df.columns:
                # Exclude ETFs
                filtered_df = filtered_df[filtered_df['ETF'] != 'Y']
                logger.info(f"Excluded ETFs: {original_count - len(filtered_df)} records removed")
            
            if 'Test Issue' in df.columns:
                # Exclude test issues
                before = len(filtered_df)
                filtered_df = filtered_df[filtered_df['Test Issue'] != 'Y']
                logger.info(f"Excluded test issues: {before - len(filtered_df)} records removed")
        
        elif exchange.lower() == 'nyse':
            # NYSE specific filtering
            if 'ETF' in df.columns:
                # Exclude ETFs
                filtered_df = filtered_df[filtered_df['ETF'] != 'Y']
                logger.info(f"Excluded ETFs: {original_count - len(filtered_df)} records removed")
            
            if 'Asset Type' in df.columns:
                # Filter for stocks only
                before = len(filtered_df)
                filtered_df = filtered_df[filtered_df['Asset Type'] == 'Stock']
                logger.info(f"Filtered for stocks: {before - len(filtered_df)} records removed")
        
        # Remove any rows with empty symbols
        # Handle different column names for different exchanges
        symbol_col = 'Symbol' if 'Symbol' in filtered_df.columns else 'ACT Symbol'

        if symbol_col in filtered_df.columns:
            before = len(filtered_df)
            filtered_df = filtered_df[filtered_df[symbol_col].notna()]
            filtered_df = filtered_df[filtered_df[symbol_col].str.strip() != '']
            logger.info(f"Removed empty symbols: {before - len(filtered_df)} records removed")

        logger.info(f"{exchange.upper()}: Filtered to {len(filtered_df)} common stocks from {original_count} total")

        # Show sample of filtered data
        if len(filtered_df) > 0:
            logger.info(f"\nSample of filtered {exchange.upper()} stocks:")
            # Use appropriate column names based on what's available
            if 'Symbol' in filtered_df.columns and 'Security Name' in filtered_df.columns:
                sample_cols = ['Symbol', 'Security Name']
            elif 'ACT Symbol' in filtered_df.columns and 'Company Name' in filtered_df.columns:
                sample_cols = ['ACT Symbol', 'Company Name']
            else:
                sample_cols = filtered_df.columns[:2].tolist()
            logger.info(f"{filtered_df[sample_cols].head()}")
        
        return filtered_df
    
    def combine_and_deduplicate(self, nasdaq_df: pd.DataFrame, nyse_df: pd.DataFrame) -> pd.DataFrame:
        """Combine NASDAQ and NYSE listings and remove duplicates"""
        # Standardize column names
        nasdaq_df = nasdaq_df.rename(columns={'Symbol': 'symbol', 'Security Name': 'name'})
        nyse_df = nyse_df.rename(columns={'ACT Symbol': 'symbol', 'Company Name': 'name'})
        
        # Add exchange column
        nasdaq_df['exchange'] = 'NASDAQ'
        nyse_df['exchange'] = 'NYSE'
        
        # Combine
        combined_df = pd.concat([nasdaq_df, nyse_df], ignore_index=True)
        
        # Remove duplicates (keep first occurrence)
        before = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['symbol'], keep='first')
        logger.info(f"Removed {before - len(combined_df)} duplicate symbols")
        
        # Sort by symbol
        combined_df = combined_df.sort_values('symbol')
        
        logger.info(f"Combined total: {len(combined_df)} unique symbols")
        
        return combined_df
    
    def save_combined_list(self, combined_df: pd.DataFrame) -> Path:
        """Save combined and filtered stock list"""
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f'us_common_stocks_{date_str}.csv'
        output_path = self.output_dir / filename
        
        # Select relevant columns
        columns_to_save = ['symbol', 'name', 'exchange']
        # Add other columns if they exist
        for col in ['Market Cap', 'IPO Year', 'Sector', 'Industry']:
            if col in combined_df.columns:
                columns_to_save.append(col)
        
        save_df = combined_df[columns_to_save] if all(col in combined_df.columns for col in columns_to_save) else combined_df
        save_df.to_csv(output_path, index=False)
        
        logger.info(f"Saved combined stock list to {output_path}")

        # Upload to GCS
        self.upload_to_gcs(output_path)

        return output_path

    async def upsert_symbols_to_db(self, symbols: List[str]) -> Dict[str, int]:
        """
        Upsert symbols to us_symbol table
        - New symbols: INSERT with symbol, created_at, updated_at
        - Existing symbols: UPDATE only updated_at
        """
        if not self.database_url:
            logger.warning("DATABASE_URL not configured. Skipping DB upsert.")
            return {"inserted": 0, "updated": 0}

        if not symbols:
            logger.warning("No symbols to upsert.")
            return {"inserted": 0, "updated": 0}

        try:
            conn = await asyncpg.connect(self.database_url)

            # Upsert query
            upsert_query = """
                INSERT INTO us_symbol (symbol, created_at, updated_at)
                VALUES ($1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (symbol) DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP
            """

            # Check existing symbols before upsert
            existing_symbols_result = await conn.fetch(
                "SELECT symbol FROM us_symbol WHERE symbol = ANY($1::text[])",
                symbols
            )
            existing_symbols = {row['symbol'] for row in existing_symbols_result}

            # Execute batch upsert
            async with conn.transaction():
                await conn.executemany(upsert_query, [(symbol,) for symbol in symbols])

            await conn.close()

            # Calculate stats
            inserted_count = len(symbols) - len(existing_symbols)
            updated_count = len(existing_symbols)

            logger.info(f"DB Upsert completed: {inserted_count} inserted, {updated_count} updated")

            return {
                "inserted": inserted_count,
                "updated": updated_count,
                "total": len(symbols)
            }

        except Exception as e:
            logger.error(f"Failed to upsert symbols to DB: {str(e)}")
            return {"inserted": 0, "updated": 0, "error": str(e)}

    async def run(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Main execution"""
        logger.info("="*60)
        logger.info("Starting US Stock Listing Download Process")
        logger.info(f"Timestamp: {datetime.now()}")
        logger.info("="*60)

        # Download NASDAQ listings
        nasdaq_df = self.download_csv('nasdaq')
        nasdaq_filtered = None
        if nasdaq_df is not None:
            nasdaq_filtered = self.filter_common_stocks(nasdaq_df, 'nasdaq')

            # Upload to GCS
            date_str = datetime.now().strftime('%Y%m%d')
            nasdaq_path = self.output_dir / f'nasdaq-listed_{date_str}.csv'
            if nasdaq_path.exists():
                self.upload_to_gcs(nasdaq_path)

        # Download NYSE listings
        nyse_df = self.download_csv('nyse')
        nyse_filtered = None
        if nyse_df is not None:
            nyse_filtered = self.filter_common_stocks(nyse_df, 'nyse')

            # Upload to GCS
            date_str = datetime.now().strftime('%Y%m%d')
            nyse_path = self.output_dir / f'nyse-listed_{date_str}.csv'
            if nyse_path.exists():
                self.upload_to_gcs(nyse_path)

        # Combine and deduplicate
        combined_df = None
        if nasdaq_filtered is not None and nyse_filtered is not None:
            combined_df = self.combine_and_deduplicate(nasdaq_filtered, nyse_filtered)
            self.save_combined_list(combined_df)

            # Upsert symbols to us_symbol table
            logger.info("\n" + "="*60)
            logger.info("Upserting symbols to us_symbol table")
            logger.info("="*60)

            symbols = combined_df['symbol'].tolist()
            upsert_result = await self.upsert_symbols_to_db(symbols)

            logger.info(f"Upsert Result: {upsert_result}")

        logger.info("\n" + "="*60)
        logger.info("Download Process Completed Successfully")
        logger.info("="*60)

        # Summary
        if nasdaq_filtered is not None:
            logger.info(f"NASDAQ common stocks: {len(nasdaq_filtered)}")
        if nyse_filtered is not None:
            logger.info(f"NYSE common stocks: {len(nyse_filtered)}")
        if combined_df is not None:
            logger.info(f"Total unique symbols: {len(combined_df)}")

        return nasdaq_filtered, nyse_filtered, combined_df


async def main():
    """Entry point for async execution"""
    downloader = StockListingDownloader()
    nasdaq_stocks, nyse_stocks, combined_stocks = await downloader.run()

    if combined_stocks is not None:
        print(f"\nTotal common stocks ready for API calls: {len(combined_stocks)}")

if __name__ == "__main__":
    asyncio.run(main())