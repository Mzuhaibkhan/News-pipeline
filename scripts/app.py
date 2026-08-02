from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from starlette.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, EmailStr
from typing import Dict, Any, List, Optional
import logging
import time
import threading

import os
import redis

import fetch
import clear_old_news
import newsletter

# ---------------------------------------------------------------------------
# Use orjson for fast JSON serialization (5-10x faster, handles datetime)
# Falls back to stdlib json if orjson is not installed.
# ---------------------------------------------------------------------------
try:
    import orjson

    def _json_dumps(obj: Any) -> bytes:
        return orjson.dumps(obj, default=str)

    def _json_loads(data: bytes) -> Any:
        return orjson.loads(data)

except ImportError:
    import json

    def _json_dumps(obj: Any) -> bytes:
        return json.dumps(obj, default=str).encode()

    def _json_loads(data: bytes) -> Any:
        return json.loads(data)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Caching Layer (Redis Distributed Cache with InMemory Fallback)
# ---------------------------------------------------------------------------

# Cache key prefix for safe invalidation (avoids flushdb on shared Redis)
_CACHE_PREFIX = "newspipe:"


class InMemoryCache:
    """
    Lock-free read path: reads check expiry without holding the lock.
    Only writes and clears acquire the lock. Under high concurrency this
    eliminates the serialization bottleneck on cache hits.
    """

    def __init__(self):
        self._cache: dict[str, tuple[Any, float]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        entry = self._cache.get(key)
        if entry is None:
            return None
        val, expiry = entry
        if time.time() > expiry:
            # Expired — lazy cleanup, no lock needed for correctness
            self._cache.pop(key, None)
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
            socket_connect_timeout=2.0,
            decode_responses=False,  # we handle bytes ourselves via orjson
        )
        # Ping check to fail fast if connection cannot be established
        self._client.ping()

    def get(self, key: str) -> Optional[Any]:
        try:
            val = self._client.get(_CACHE_PREFIX + key)
            if val is not None:
                return _json_loads(val)
        except Exception as e:
            log.error("Redis cache error on get: %s", e)
        return None

    def set(self, key: str, value: Any, ttl: int = 60):
        try:
            self._client.setex(_CACHE_PREFIX + key, ttl, _json_dumps(value))
        except Exception as e:
            log.error("Redis cache error on set: %s", e)

    def clear(self):
        """Delete only our prefixed keys instead of flushing the entire DB."""
        try:
            cursor = 0
            while True:
                cursor, keys = self._client.scan(cursor, match=f"{_CACHE_PREFIX}*", count=100)
                if keys:
                    self._client.delete(*keys)
                if cursor == 0:
                    break
            log.info("Redis cache cleared (prefix-scanned deletion).")
        except Exception as e:
            log.error("Redis cache error on clear: %s", e)


def _init_cache():
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


cache = _init_cache()


# ---------------------------------------------------------------------------
# Lifespan: pre-warm DB + indexes at startup, increase threadpool
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app):
    """
    Startup: pre-warm MongoDB connection, ensure indexes, expand threadpool.
    Shutdown: nothing special needed (PyMongo handles cleanup).
    """
    # Increase the default anyio/starlette threadpool from 40 to 200
    # so that synchronous `def` endpoints can serve 200 concurrent requests
    # per worker process without queuing.
    import anyio
    limiter = anyio.to_thread.current_default_thread_limiter()
    limiter.total_tokens = 200
    log.info("Threadpool limiter expanded to %d tokens.", limiter.total_tokens)

    # Pre-warm database and indexes so the first request pays no setup cost
    from db import ensure_all_indexes
    ensure_all_indexes()

    yield  # app is running
    log.info("Shutting down News Pipeline API.")


app = FastAPI(
    title="News Pipeline API",
    description="API for fetching, storing, and emailing daily news newsletters from various sources.",
    version="2.0.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Middleware — GZip compress responses > 500 bytes (70-85% size reduction)
# ---------------------------------------------------------------------------
app.add_middleware(GZipMiddleware, minimum_size=500)


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


def _send_email_background(email: str, limit: int, company: Optional[str]):
    """Background task function to send a newsletter email without blocking the request."""
    try:
        newsletter.send_todays_news_email(to_email=email, limit=limit, company=company)
        log.info("Background email sent successfully to %s.", email)
    except Exception as e:
        log.error("Background email to %s failed: %s", email, e)


def _broadcast_background(limit: int, company: Optional[str]):
    """Background task function to broadcast newsletter to all subscribers."""
    try:
        result = newsletter.broadcast_newsletter(limit=limit, company=company)
        log.info("Background broadcast complete: sent=%s, failed=%s",
                 result.get("sent_count"), result.get("failed_count"))
    except Exception as e:
        log.error("Background broadcast failed: %s", e)


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
def get_articles(
    company: str = Query(None, description="Optional company name or ticker to filter by"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    per_page: int = Query(20, ge=1, le=100, description="Articles per page (max 100)"),
) -> Dict[str, Any]:
    """
    Retrieves already fetched articles directly from the database with pagination.
    This naturally skips any broken/revoked external APIs.
    """
    cache_key = f"articles:{company or 'all'}:p{page}:pp{per_page}"
    cached_val = cache.get(cache_key)
    if cached_val is not None:
        return cached_val

    try:
        skip = (page - 1) * per_page
        articles, total = fetch.get_saved_articles(
            query=company, limit=per_page, skip=skip
        )
        total_pages = max(1, -(-total // per_page))  # ceiling division
        response_data = {
            "status": "success",
            "count": len(articles),
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "data": articles,
        }
        # Cache for 5 minutes — articles don't change frequently;
        # cache is invalidated automatically when the fetch pipeline completes.
        cache.set(cache_key, response_data, ttl=300)
        return response_data
    except Exception as e:
        log.error(f"Error fetching saved articles: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def _run_cleanup_background(days_old: int):
    """Background task function for article cleanup."""
    try:
        clear_old_news.run_cleanup(days_old=days_old)
        log.info("Background cleanup completed for articles older than %d days.", days_old)
    except Exception as e:
        log.error("Background cleanup error: %s", e)
    finally:
        cache.clear()


@app.post("/api/cleanup", status_code=202)
def trigger_cleanup(
    background_tasks: BackgroundTasks,
    days_old: int = Query(15, description="Number of days old before deleting"),
) -> Dict[str, str]:
    """
    Triggers cleanup of old articles from MongoDB in the background.
    Returns immediately with 202 Accepted.
    """
    background_tasks.add_task(_run_cleanup_background, days_old)
    return {
        "status": "accepted",
        "message": f"Cleanup triggered in background for articles older than {days_old} days.",
    }

# ---------------------------------------------------------------------------
# Newsletter & Email Endpoints
# ---------------------------------------------------------------------------

@app.post("/api/newsletter/send", status_code=202)
def send_newsletter_to_email(req: SendEmailRequest, background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """
    Queues today's news digest to be emailed to the specified user address.
    Returns immediately with 202 Accepted — the email is sent in the background.
    """
    background_tasks.add_task(_send_email_background, req.email, req.limit or 10, req.company)
    return {
        "status": "accepted",
        "message": f"Newsletter email queued for delivery to {req.email}."
    }


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


@app.post("/api/newsletter/broadcast", status_code=202)
def broadcast_newsletter_to_all(req: Optional[BroadcastRequest] = None, background_tasks: BackgroundTasks = None) -> Dict[str, Any]:
    """
    Broadcasts today's news digest to all active subscribers in the background.
    Returns immediately with 202 Accepted.
    """
    limit = req.limit if req and req.limit else 10
    company = req.company if req else None
    background_tasks.add_task(_broadcast_background, limit, company)
    return {
        "status": "accepted",
        "message": "Newsletter broadcast queued for all active subscribers."
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=10000, reload=True)
