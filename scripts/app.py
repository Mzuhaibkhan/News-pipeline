from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from pydantic import BaseModel, EmailStr
from typing import Dict, Any, List, Optional
import logging
import time
import threading

import os
import json
import redis

import fetch
import clear_old_news
import newsletter

app = FastAPI(
    title="News Pipeline API",
    description="API for fetching, storing, and emailing daily news newsletters from various sources.",
    version="1.1.0"
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Caching Layer (Redis Distributed Cache with InMemory Fallback)
# ---------------------------------------------------------------------------
class InMemoryCache:
    def __init__(self):
        self._cache = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._cache:
                return None
            val, expiry = self._cache[key]
            if time.time() > expiry:
                del self._cache[key]
                return None
            return val

    def set(self, key: str, value: Any, ttl: int = 60):
        with self._lock:
            self._cache[key] = (value, time.time() + ttl)

    def clear(self):
        with self._lock:
            self._cache.clear()
            log.info("In-memory cache cleared.")


class RedisCache:
    def __init__(self, redis_url: str):
        self._url = redis_url
        self._client = redis.Redis.from_url(
            redis_url, 
            socket_timeout=2.0, 
            socket_connect_timeout=2.0
        )
        # Ping check to fail fast if connection cannot be established
        self._client.ping()

    def get(self, key: str) -> Optional[Any]:
        try:
            val = self._client.get(key)
            if val is not None:
                return json.loads(val)
        except Exception as e:
            log.error("Redis cache error on get: %s", e)
        return None

    def set(self, key: str, value: Any, ttl: int = 60):
        try:
            self._client.setex(key, ttl, json.dumps(value))
        except Exception as e:
            log.error("Redis cache error on set: %s", e)

    def clear(self):
        try:
            self._client.flushdb()
            log.info("Redis cache cleared (flushdb).")
        except Exception as e:
            log.error("Redis cache error on clear: %s", e)


def get_cache():
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        try:
            log.info("Attempting to connect to Redis cache at %s...", redis_url)
            redis_cache = RedisCache(redis_url)
            log.info("Successfully connected to Redis cache.")
            return redis_cache
        except Exception as e:
            log.warning("Failed to connect to Redis. Falling back to InMemoryCache. Error: %s", e)
    else:
        log.info("REDIS_URL not configured. Using InMemoryCache.")
    return InMemoryCache()


cache = get_cache()


# ---------------------------------------------------------------------------
# Background Task Tracking and Locking
# ---------------------------------------------------------------------------
_active_fetches = set()
_fetch_lock = threading.Lock()

def _run_pipeline_background(company: Optional[str]):
    """Background task function to execute the pipeline fetch and clear cache on success."""
    key = company if company is not None else "__all__"
    try:
        fetch.run_pipeline(return_data=False, query=company)
    except Exception as e:
        log.error(f"Background fetch error for {company or 'all'}: {e}")
    finally:
        with _fetch_lock:
            _active_fetches.discard(key)
        cache.clear()


# Pydantic Schemas for Requests
class SendEmailRequest(BaseModel):
    email: EmailStr
    limit: Optional[int] = 10
    company: Optional[str] = None


class SubscriberRequest(BaseModel):
    email: EmailStr


class BroadcastRequest(BaseModel):
    limit: Optional[int] = 10
    company: Optional[str] = None


@app.get("/")
def read_root() -> Dict[str, str]:
    """Healthcheck endpoint."""
    return {"status": "ok", "message": "News Pipeline Service is running"}

@app.post("/api/fetch")
@app.get("/api/fetch")
def trigger_fetch(background_tasks: BackgroundTasks, company: str = Query(None, description="Optional company name or ticker to search for")) -> Dict[str, Any]:
    """
    Triggers the fetch pipeline in the background and returns immediately.
    If 'company' is provided, it specifically fetches articles related to that company.
    """
    key = company if company is not None else "__all__"
    
    with _fetch_lock:
        if key in _active_fetches:
            return {
                "status": "accepted",
                "message": f"Fetch pipeline is already running for: {company or 'all'}. Please wait."
            }
        _active_fetches.add(key)
        
    background_tasks.add_task(_run_pipeline_background, company)
    return {
        "status": "accepted",
        "message": f"Fetch pipeline triggered in background for: {company or 'all'}."
    }

@app.get("/api/articles")
def get_articles(company: str = Query(None, description="Optional company name or ticker to filter by"), limit: int = Query(None, description="Max number of articles to return")) -> Dict[str, Any]:
    """
    Retrieves already fetched articles directly from the database without querying external APIs.
    This naturally skips any broken/revoked external APIs.
    """
    cache_key = f"articles:{company or 'all'}:{limit or 'default'}"
    cached_val = cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    try:
        articles = fetch.get_saved_articles(query=company, limit=limit)
        response_data = {
            "status": "success",
            "count": len(articles),
            "data": articles
        }
        # Cache for 60 seconds
        cache.set(cache_key, response_data, ttl=60)
        return response_data
    except Exception as e:
        log.error(f"Error fetching saved articles: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------------------------------------------------------
# Newsletter & Email Endpoints
# ---------------------------------------------------------------------------

@app.post("/api/newsletter/send")
def send_newsletter_to_email(req: SendEmailRequest, background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """
    Emails today's fetched news digest directly to the specified user email address.
    """
    try:
        result = newsletter.send_todays_news_email(
            to_email=req.email,
            limit=req.limit or 10,
            company=req.company
        )
        return result
    except Exception as e:
        log.error(f"Error sending email digest to {req.email}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/newsletter/subscribe")
def subscribe(req: SubscriberRequest) -> Dict[str, Any]:
    """
    Subscribes a user email address to receive daily news updates.
    """
    try:
        result = newsletter.subscribe_email(req.email)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        log.error(f"Error subscribing email {req.email}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/newsletter/unsubscribe")
def unsubscribe(req: SubscriberRequest) -> Dict[str, Any]:
    """
    Unsubscribes a user email address from daily news updates.
    """
    try:
        result = newsletter.unsubscribe_email(req.email)
        return result
    except Exception as e:
        log.error(f"Error unsubscribing email {req.email}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/newsletter/subscribers")
def list_subscribers() -> Dict[str, Any]:
    """
    Retrieves list of active subscriber emails.
    """
    try:
        subscribers = newsletter.get_active_subscribers()
        return {
            "status": "success",
            "count": len(subscribers),
            "subscribers": subscribers
        }
    except Exception as e:
        log.error(f"Error listing subscribers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/newsletter/broadcast")
def broadcast_newsletter_to_all(req: Optional[BroadcastRequest] = None) -> Dict[str, Any]:
    """
    Broadcasts today's news digest to all active subscribers.
    """
    try:
        limit = req.limit if req and req.limit else 10
        company = req.company if req else None
        result = newsletter.broadcast_newsletter(limit=limit, company=company)
        return result
    except Exception as e:
        log.error(f"Error broadcasting newsletter: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=10000, reload=True)

