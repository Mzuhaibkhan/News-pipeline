"""
clear_old_news.py — News Pipeline
Clears news articles older than 15 days from the MongoDB collection.
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from db import get_collection

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

MONGO_URI: str = os.getenv("MONGO_URI", "")
MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "news_pipeline")
MONGO_COLLECTION: str = os.getenv("MONGO_COLLECTION", "articles")

# MongoDB collection is provided by the shared singleton in db.py

def run_cleanup(days_old: int = 15):
    log.info("=" * 60)
    log.info("Starting cleanup script...")
    collection = get_collection()
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
    log.info(f"Removing articles dated before {cutoff_date.isoformat()}")
    
    query = {
        "$or": [
            {"published_at": {"$lt": cutoff_date}},
            {"published_at": None, "fetched_at": {"$lt": cutoff_date}}
        ]
    }
    
    try:
        result = collection.delete_many(query)
        log.info(f"Cleanup complete. Deleted {result.deleted_count} old articles.")
    except Exception as e:
        log.error(f"Error during deletion: {e}")
        
    log.info("=" * 60)

if __name__ == "__main__":
    run_cleanup()
