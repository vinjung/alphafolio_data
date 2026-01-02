import json
import os
import logging
from datetime import datetime, date
from typing import Dict, List, Set, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class CollectionLogger:
    def __init__(self, log_file_path: str):
        self.log_file = Path(log_file_path)
        self.collection_log = self.load_log()

        # Ensure log directory exists
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def load_log(self) -> Dict:
        """Load existing log file or create new structure"""
        try:
            if self.log_file.exists():
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded collection log from {self.log_file}")
                    return data
            else:
                logger.info(f"Creating new collection log at {self.log_file}")
                return {
                    "metadata": {
                        "created_at": datetime.now().isoformat(),
                        "last_updated": None,
                        "version": "1.0"
                    },
                    "collection_log": {}
                }
        except Exception as e:
            logger.error(f"Error loading log file: {str(e)}")
            return {
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "last_updated": None,
                    "version": "1.0"
                },
                "collection_log": {}
            }

    def save_log(self):
        """Save log to file"""
        try:
            # Ensure metadata exists
            if "metadata" not in self.collection_log:
                self.collection_log["metadata"] = {
                    "created_at": datetime.now().isoformat(),
                    "last_updated": None,
                    "version": "1.0"
                }

            self.collection_log["metadata"]["last_updated"] = datetime.now().isoformat()

            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(self.collection_log, f, indent=2, ensure_ascii=False)

            logger.debug(f"Collection log saved to {self.log_file}")
        except Exception as e:
            logger.error(f"Error saving log file: {str(e)}")

    def is_collected(self, indicator: str, symbol: str, target_dates: List[str]) -> bool:
        """Check if data for given indicator/symbol/dates is already collected"""
        try:
            collection_data = self.collection_log.get("collection_log", {})

            if indicator not in collection_data:
                return False

            if symbol not in collection_data[indicator]:
                return False

            collected_dates = set(collection_data[indicator][symbol].get("dates", []))
            target_dates_set = set(target_dates)

            # Check if all target dates are already collected
            return target_dates_set.issubset(collected_dates)

        except Exception as e:
            logger.error(f"Error checking collection status: {str(e)}")
            return False

    def mark_collected(self, indicator: str, symbol: str, dates: List[str],
                      records_count: int = 0, collection_time: Optional[str] = None):
        """Mark data as collected and update log"""
        try:
            if "collection_log" not in self.collection_log:
                self.collection_log["collection_log"] = {}

            collection_data = self.collection_log["collection_log"]

            if indicator not in collection_data:
                collection_data[indicator] = {}

            if symbol not in collection_data[indicator]:
                collection_data[indicator][symbol] = {
                    "dates": [],
                    "collection_history": []
                }

            # Add new dates (avoid duplicates)
            existing_dates = set(collection_data[indicator][symbol]["dates"])
            new_dates = [d for d in dates if d not in existing_dates]
            collection_data[indicator][symbol]["dates"].extend(new_dates)

            # Sort dates
            collection_data[indicator][symbol]["dates"].sort()

            # Add collection history entry
            history_entry = {
                "collected_at": collection_time or datetime.now().isoformat(),
                "dates": dates,
                "records_count": records_count,
                "new_dates_count": len(new_dates)
            }

            collection_data[indicator][symbol]["collection_history"].append(history_entry)

            # Keep only last 10 history entries per symbol
            if len(collection_data[indicator][symbol]["collection_history"]) > 10:
                collection_data[indicator][symbol]["collection_history"] = \
                    collection_data[indicator][symbol]["collection_history"][-10:]

            logger.debug(f"Marked {indicator}/{symbol} as collected for {len(dates)} dates, "
                        f"{records_count} records")

        except Exception as e:
            logger.error(f"Error marking collection: {str(e)}")

    def get_collection_summary(self) -> Dict:
        """Get summary of collection status"""
        try:
            collection_data = self.collection_log.get("collection_log", {})
            summary = {
                "total_indicators": len(collection_data),
                "indicators": {}
            }

            for indicator, symbols_data in collection_data.items():
                summary["indicators"][indicator] = {
                    "total_symbols": len(symbols_data),
                    "total_dates": sum(len(symbol_info.get("dates", []))
                                     for symbol_info in symbols_data.values()),
                    "symbols": list(symbols_data.keys())
                }

            return summary

        except Exception as e:
            logger.error(f"Error getting collection summary: {str(e)}")
            return {}

    def get_collected_dates_for_symbol(self, indicator: str, symbol: str) -> List[str]:
        """Get list of dates already collected for specific indicator/symbol"""
        try:
            collection_data = self.collection_log.get("collection_log", {})

            if indicator in collection_data and symbol in collection_data[indicator]:
                return collection_data[indicator][symbol].get("dates", [])

            return []

        except Exception as e:
            logger.error(f"Error getting collected dates: {str(e)}")
            return []

    def cleanup_old_entries(self, days_to_keep: int = 30):
        """Remove collection history entries older than specified days"""
        try:
            cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)
            collection_data = self.collection_log.get("collection_log", {})

            for indicator in collection_data:
                for symbol in collection_data[indicator]:
                    history = collection_data[indicator][symbol].get("collection_history", [])
                    filtered_history = []

                    for entry in history:
                        try:
                            entry_time = datetime.fromisoformat(entry["collected_at"]).timestamp()
                            if entry_time >= cutoff_date:
                                filtered_history.append(entry)
                        except:
                            # Keep entries with invalid timestamps
                            filtered_history.append(entry)

                    collection_data[indicator][symbol]["collection_history"] = filtered_history

            logger.info(f"Cleaned up collection history older than {days_to_keep} days")

        except Exception as e:
            logger.error(f"Error cleaning up old entries: {str(e)}")
