from fastapi import FastAPI, BackgroundTasks, Query
from typing import Dict, Any, List
import logging

import fetch
import clear_old_news

app = FastAPI(
    title="News Pipeline API",
    description="API for fetching and storing news articles from various sources.",
    version="1.0.0"
)

log = logging.getLogger(__name__)

@app.get("/")
def read_root() -> Dict[str, str]:
    """Healthcheck endpoint."""
    return {"status": "ok", "message": "News Pipeline Service is running"}

@app.post("/api/fetch")
@app.get("/api/fetch")
def trigger_fetch(company: str = Query(None, description="Optional company name or ticker to search for")) -> Dict[str, Any]:
    """
    Triggers the fetch pipeline, upserts to MongoDB, and returns the fetched articles.
    If 'company' is provided, it specifically fetches articles related to that company.
    """
    try:
        articles = fetch.run_pipeline(return_data=True, query=company)
        return {
            "status": "success",
            "fetched_count": len(articles),
            "data": articles
        }
    except Exception as e:
        log.error(f"Error during fetch: {e}")
        return {
            "status": "error",
            "message": str(e)
        }

@app.post("/api/cleanup")
def trigger_cleanup(days_old: int = Query(15, description="Number of days old before deleting")) -> Dict[str, str]:
    """
    Triggers cleanup of old articles from MongoDB.
    """
    try:
        clear_old_news.run_cleanup(days_old=days_old)
        return {"status": "success", "message": f"Cleared articles older than {days_old} days"}
    except Exception as e:
        log.error(f"Error during cleanup: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=10000, reload=True)
