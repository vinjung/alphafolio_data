# alpha/data/kr/gcs_handler.py
import os
import json
from typing import Optional, List, Tuple
from datetime import datetime, timedelta
import logging

from google.cloud import storage
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET_NAME = "dducksang-kr-data"

class GCSHandler:
    def __init__(self):
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """GCS 클라이언트 초기화"""
        gcp_sa_key_str = os.getenv('GCP_SA_KEY')
        if not gcp_sa_key_str:
            logger.error("GCP_SA_KEY environment variable not found")
            return
        
        try:
            credentials_info = json.loads(gcp_sa_key_str)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            self.client = storage.Client(credentials=credentials)
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
    
    def upload_file(self, file_name: str, data: str, folder: str = "") -> bool:
        """GCS에 파일 업로드"""
        if not self.client or not data:
            logger.error("No GCS client or data to upload")
            return False
        
        try:
            bucket = self.client.bucket(BUCKET_NAME)
            blob_path = f"{folder}/{file_name}" if folder else file_name
            blob = bucket.blob(blob_path)
            
            blob.upload_from_string(data, content_type='text/csv')
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload to GCS: {e}")
            return False
    
    def get_latest_file(self, prefix: str) -> Tuple[Optional[str], Optional[str]]:
        """지정된 prefix로 가장 최신 파일 가져오기"""
        if not self.client:
            return None, None
        
        try:
            bucket = self.client.bucket(BUCKET_NAME)
            blobs = list(bucket.list_blobs(prefix=prefix))
            
            if not blobs:
                logger.warning(f"No files found with prefix {prefix}")
                return None, None
            
            # 파일명 기준으로 정렬 (타임스탬프 포함)
            latest_blob = max(blobs, key=lambda x: x.name)
            
            logger.info(f"Found latest file: {latest_blob.name}")
            data = latest_blob.download_as_text()
            
            return latest_blob.name, data
            
        except Exception as e:
            logger.error(f"Failed to get latest file from GCS: {e}")
            return None, None
    
    def list_files(self, prefix: str = "") -> List[str]:
        """지정된 prefix의 모든 파일 목록 반환"""
        if not self.client:
            return []
        
        try:
            bucket = self.client.bucket(BUCKET_NAME)
            blobs = bucket.list_blobs(prefix=prefix)
            file_list = [blob.name for blob in blobs]
            
            logger.info(f"Found {len(file_list)} files with prefix '{prefix}'")
            return file_list
            
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            return []
    
    def delete_file(self, file_path: str) -> bool:
        """특정 파일 삭제"""
        if not self.client:
            return False
        
        try:
            bucket = self.client.bucket(BUCKET_NAME)
            blob = bucket.blob(file_path)
            blob.delete()
            
            logger.info(f"Deleted file: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete file {file_path}: {e}")
            return False
    
    def delete_old_files(self, prefix: str, days_old: int = 30) -> int:
        """지정된 일수보다 오래된 파일들 삭제"""
        if not self.client:
            return 0
        
        try:
            bucket = self.client.bucket(BUCKET_NAME)
            blobs = bucket.list_blobs(prefix=prefix)
            
            cutoff_date = datetime.now() - timedelta(days=days_old)
            deleted_count = 0
            
            for blob in blobs:
                # blob의 생성 시간 확인
                if blob.time_created.replace(tzinfo=None) < cutoff_date:
                    try:
                        blob.delete()
                        logger.info(f"Deleted old file: {blob.name} (created: {blob.time_created})")
                        deleted_count += 1
                    except Exception as e:
                        logger.error(f"Failed to delete {blob.name}: {e}")
            
            logger.info(f"Deleted {deleted_count} old files (older than {days_old} days)")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to delete old files: {e}")
            return 0
    
    def delete_all_files_in_folder(self, folder: str) -> int:
        """특정 폴더의 모든 파일 삭제"""
        if not self.client:
            return 0
        
        try:
            bucket = self.client.bucket(BUCKET_NAME)
            blobs = bucket.list_blobs(prefix=f"{folder}/")
            
            deleted_count = 0
            for blob in blobs:
                try:
                    blob.delete()
                    logger.info(f"Deleted file: {blob.name}")
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Failed to delete {blob.name}: {e}")
            
            logger.info(f"Deleted {deleted_count} files from folder '{folder}'")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to delete files in folder {folder}: {e}")
            return 0
    
    def cleanup_files_by_pattern(self, prefix: str, keep_latest: int = 5) -> int:
        """패턴별로 최신 N개 파일만 유지하고 나머지 삭제"""
        if not self.client:
            return 0
        
        try:
            bucket = self.client.bucket(BUCKET_NAME)
            blobs = list(bucket.list_blobs(prefix=prefix))
            
            if len(blobs) <= keep_latest:
                logger.info(f"Only {len(blobs)} files found, no cleanup needed")
                return 0
            
            # 생성 시간 기준으로 정렬 (최신순)
            blobs_sorted = sorted(blobs, key=lambda x: x.time_created, reverse=True)
            files_to_delete = blobs_sorted[keep_latest:]
            
            deleted_count = 0
            for blob in files_to_delete:
                try:
                    blob.delete()
                    logger.info(f"Cleaned up old file: {blob.name}")
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Failed to delete {blob.name}: {e}")
            
            logger.info(f"Cleanup completed: kept {keep_latest} latest files, deleted {deleted_count} files")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup files: {e}")
            return 0

# 전역 인스턴스 (싱글톤 패턴)
_gcs_handler = None

def get_gcs_handler() -> GCSHandler:
    """GCS 핸들러 인스턴스 반환"""
    global _gcs_handler
    if _gcs_handler is None:
        _gcs_handler = GCSHandler()
    return _gcs_handler