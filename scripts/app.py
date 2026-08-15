from contextlib import asynccontextmanager
import asyncio
import hmac
import uuid
from fastapi import FastAPI, BackgroundTasks, Depends, Header, Query, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, EmailStr
from typing import Dict, Any, List, Optional
import logging
import time
import threading
from datetime import datetime, timedelta

import os
import redis

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

import fetch
import clear_old_news
import newsletter
from db import get_async_client, get_async_collection, ensure_all_indexes

# SSE streaming support (optional — graceful degradation if not installed)
try:
    from sse_starlette.sse import EventSourceResponse
    _SSE_AVAILABLE = True
except ImportError:
    _SSE_AVAILABLE = False

# Prometheus metrics (optional — graceful degradation if not installed)
try:
    from prometheus_fastapi_instrumentator import Instrumentator as PrometheusInstrumentator
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

# Signed unsubscribe token validation
try:
    from tokens import validate_unsubscribe_token
    _TOKENS_AVAILABLE = True
except ImportError:
    validate_unsubscribe_token = None
    _TOKENS_AVAILABLE = False

# ---------------------------------------------------------------------------
# Use orjson for fast JSON serialization (5-10x faster, handles datetime)
# Falls back to stdlib json if orjson is not installed.
# ---------------------------------------------------------------------------
try:
    import orjson
    from fastapi.responses import ORJSONResponse

    def _json_dumps(obj: Any) -> bytes:
        return orjson.dumps(obj, default=str)

    def _json_loads(data: bytes) -> Any:
        return orjson.loads(data)


except ImportError:
    import json
    from fastapi.responses import JSONResponse as ORJSONResponse

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
            # Expired — thread-safe cleanup
            with self._lock:
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
            decode_responses=False,
            max_connections=50,  # explicit pool cap to prevent connection storms
        )
        # Ping check to fail fast if connection cannot be established
        self._client.ping()

    def get(self, key: str) -> Optional[bytes]:
        """Return raw cached bytes (caller is responsible for deserialization)."""
        try:
            return self._client.get(_CACHE_PREFIX + key)
        except Exception as e:
            log.error("Redis cache error on get: %s", e)
        return None

    def set(self, key: str, value: bytes, ttl: int = 60):
        """Store pre-serialized bytes directly (no double-encoding)."""
        try:
            self._client.setex(_CACHE_PREFIX + key, ttl, value)
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
        if not redis_url.startswith("rediss://"):
            log.warning(
                "REDIS_URL does not use TLS (rediss://). "
                "This is insecure for production deployments."
            )
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
    Startup: pre-warm both sync and async MongoDB clients, ensure indexes,
    expand threadpool for any remaining sync endpoints.
    Shutdown: close the async Motor client.
    """
    # Increase the default anyio/starlette threadpool from 40 to 200
    # so that synchronous `def` endpoints can serve 200 concurrent requests
    # per worker process without queuing.
    import anyio
    thread_limiter = anyio.to_thread.current_default_thread_limiter()
    thread_limiter.total_tokens = 200
    log.info("Threadpool limiter expanded to %d tokens.", thread_limiter.total_tokens)

    # Pre-warm PyMongo connection and ensure indexes (sync)
    ensure_all_indexes()

    # Pre-warm async Motor client so the first async request pays no setup cost
    async_client = get_async_client()
    await async_client.admin.command("ping")
    log.info("Async Motor client pre-warmed and connected.")

    yield  # app is running

    async_client.close()
    log.info("Shutting down News Pipeline API.")


app = FastAPI(
    title="News Pipeline API",
    description="API for fetching, storing, and emailing daily news newsletters from various sources.",
    version="2.0.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Middleware — GZip compress responses > 500 bytes (70-85% size reduction)
# ---------------------------------------------------------------------------
app.add_middleware(GZipMiddleware, minimum_size=500)

# ---------------------------------------------------------------------------
# Prometheus Metrics — exposes /metrics endpoint for monitoring systems
# Tracks request latency, counts, in-progress gauges per endpoint.
# Install: pip install prometheus-fastapi-instrumentator
# ---------------------------------------------------------------------------
if _PROMETHEUS_AVAILABLE:
    PrometheusInstrumentator().instrument(app).expose(app, endpoint="/metrics")
    log.info("Prometheus metrics enabled at /metrics")

# ---------------------------------------------------------------------------
# CORS — restrict cross-origin access to explicitly allowed frontend domains.
# Set CORS_ALLOWED_ORIGINS in .env as a comma-separated list of origins.
# ---------------------------------------------------------------------------
_cors_origins = os.getenv("CORS_ALLOWED_ORIGINS", "").split(",")
_cors_origins = [o.strip() for o in _cors_origins if o.strip()]

if _cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_methods=["GET", "POST"],
        allow_headers=["X-Api-Key", "Content-Type"],
        allow_credentials=False,
    )
    log.info("CORS middleware enabled for origins: %s", _cors_origins)


# ---------------------------------------------------------------------------
# Security Headers — industry-standard response hardening
# ---------------------------------------------------------------------------
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers.pop("server", None)
    return response


# ---------------------------------------------------------------------------
# Request ID Tracing — correlate logs across services during incidents
# ---------------------------------------------------------------------------
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


def get_real_ip(request: Request) -> str:
    """
    Extract real client IP address, supporting Cloudflare (CF-Connecting-IP)
    and standard reverse proxies (X-Forwarded-For).
    """
    cf_ip = request.headers.get("cf-connecting-ip")
    if cf_ip:
        return cf_ip.strip()
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()
    return get_remote_address(request)


# ---------------------------------------------------------------------------
# Rate Limiting — protect against single-IP abuse (uses Redis if available)
# ---------------------------------------------------------------------------
rate_limiter = Limiter(key_func=get_real_ip)
app.state.limiter = rate_limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# ---------------------------------------------------------------------------
# API Key Authentication — protects admin/destructive endpoints
# ---------------------------------------------------------------------------

_API_SECRET_KEY = os.getenv("API_SECRET_KEY", "")

if not _API_SECRET_KEY:
    log.warning("API_SECRET_KEY not set — admin endpoints are UNPROTECTED (dev mode).")


async def verify_api_key(x_api_key: str = Header(None)):
    """Validate the X-API-Key header against the API_SECRET_KEY env var.

    If API_SECRET_KEY is not configured, all requests are allowed (dev mode).

    Uses hmac.compare_digest() for constant-time comparison to prevent
    timing attacks that could brute-force the key one character at a time.

    Future evolution path:
    - Soon: Multiple named API keys stored in DB with scopes
    - Later: JWT/OAuth2 with expiry, refresh tokens, and role-based access
    """
    if not _API_SECRET_KEY:
        return  # No key configured — allow through (development mode)
    if x_api_key is None or not hmac.compare_digest(x_api_key, _API_SECRET_KEY):
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key. Provide a valid X-API-Key header.",
        )


# ---------------------------------------------------------------------------
# Background Task Tracking and Locking
# ---------------------------------------------------------------------------
_active_fetches = set()
_fetch_results: Dict[str, Dict[str, Any]] = {}
_fetch_lock = threading.Lock()

def _run_pipeline_background(company: Optional[str]):
    """Background task function to execute the pipeline fetch and clear cache on success."""
    key = company if company is not None else "__all__"
    started = time.time()
    error_msg = None
    try:
        fetch.run_pipeline(return_data=False, query=company)
    except Exception as e:
        error_msg = str(e)
        log.error(f"Background fetch error for {company or 'all'}: {e}")
    finally:
        duration = round(time.time() - started, 2)
        _fetch_results[key] = {
            "company": company or "all",
            "duration_seconds": duration,
            "error": error_msg,
        }
        with _fetch_lock:
            _active_fetches.discard(key)
        cache.clear()
        log.info("Pipeline for '%s' finished in %.2fs.", company or "all", duration)


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
def trigger_fetch(background_tasks: BackgroundTasks, company: Optional[str] = Query(None, min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9\s\-\.]+$", description="Company name or ticker (alphanumeric, max 100 chars)"), _api_key: str = Depends(verify_api_key)) -> Dict[str, Any]:
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


@app.get("/api/fetch/status")
def fetch_status(
    company: Optional[str] = Query(None, min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9\s\-\.]+$", description="Company name or ticker (alphanumeric, max 100 chars)"),
) -> Dict[str, Any]:
    """
    Check the current status of a fetch pipeline run.
    Returns 'running', 'completed' (with duration), or 'idle'.
    """
    key = company if company is not None else "__all__"
    with _fetch_lock:
        is_running = key in _active_fetches
    if is_running:
        return {"status": "running", "company": company or "all"}
    result = _fetch_results.get(key)
    if result:
        return {"status": "completed", **result}
    return {"status": "idle", "company": company or "all"}


@app.get("/api/articles")
@rate_limiter.limit("60/minute")
async def get_articles(
    request: Request,
    company: Optional[str] = Query(None, min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9\s\-\.]+$", description="Company name or ticker (alphanumeric, max 100 chars)"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    per_page: int = Query(20, ge=1, le=100, description="Articles per page (max 100)"),
):
    """
    Retrieves already fetched articles directly from the database with pagination.

    This endpoint is **fully async** (Motor) so it never blocks the threadpool,
    allowing a single Uvicorn worker to handle thousands of concurrent reads.
    Responses are pre-serialized to bytes and cached, eliminating redundant
    JSON encoding on cache hits.
    """
    headers = {"Cache-Control": "public, max-age=60, s-maxage=300, stale-while-revalidate=60"}
    cache_key = f"articles:{company or 'all'}:p{page}:pp{per_page}"

    # Fast path: return pre-serialized bytes from cache
    cached_bytes = cache.get(cache_key)
    if cached_bytes is not None:
        return Response(content=cached_bytes, media_type="application/json", headers=headers)

    try:
        skip = (page - 1) * per_page
        collection = get_async_collection()

        # Lean projection — exclude heavy fields the list view doesn't need
        projection = {
            "_id": 0,
            "content": 0,
            "url_hash": 0,
            "created_at": 0,
            "fetched_at": 0,
        }

        if company:
            # Filtered: single round-trip via $facet (text-search + count + page)
            pipeline = [
                {"$match": {"$text": {"$search": company}}},
                {"$sort": {"published_at": -1}},
                {"$facet": {
                    "data": [
                        {"$skip": skip},
                        {"$limit": per_page},
                        {"$project": projection},
                    ],
                    "total": [{"$count": "count"}],
                }},
            ]
            result = await collection.aggregate(pipeline).to_list(length=1)
            facet = result[0] if result else {"data": [], "total": []}
            articles = facet.get("data", [])
            total = facet["total"][0]["count"] if facet.get("total") else 0
        else:
            # Unfiltered: run count (O(1) metadata) and find concurrently
            total, articles = await asyncio.gather(
                collection.estimated_document_count(),
                collection.find({}, projection)
                    .sort("published_at", -1)
                    .skip(skip)
                    .limit(per_page)
                    .to_list(length=per_page),
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

        # Pre-serialize to bytes and cache — avoids redundant JSON encoding on hits
        response_bytes = _json_dumps(response_data)
        cache.set(cache_key, response_bytes, ttl=300)
        return Response(content=response_bytes, media_type="application/json", headers=headers)
    except Exception as e:
        log.error("Error fetching saved articles: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred.")

@app.get("/api/articles/all")
@rate_limiter.limit("10/minute")
async def get_all_articles(
    request: Request,
    company: Optional[str] = Query(None, min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9\s\-\.]+$", description="Company name or ticker (alphanumeric, max 100 chars)"),
    _api_key: str = Depends(verify_api_key),
):
    """
    Retrieves ALL fetched articles directly from the database without pagination.
    WARNING: Can return a very large payload.
    """
    headers = {"Cache-Control": "public, max-age=60, s-maxage=300, stale-while-revalidate=60"}
    cache_key = f"articles:{company or 'all'}:all"

    # Fast path: return pre-serialized bytes from cache
    cached_bytes = cache.get(cache_key)
    if cached_bytes is not None:
        return Response(content=cached_bytes, media_type="application/json", headers=headers)

    try:
        collection = get_async_collection()

        # Lean projection
        projection = {
            "_id": 0,
            "content": 0,
            "url_hash": 0,
            "created_at": 0,
            "fetched_at": 0,
        }

        if company:
            pipeline = [
                {"$match": {"$text": {"$search": company}}},
                {"$sort": {"published_at": -1}},
                {"$project": projection},
            ]
            articles = await collection.aggregate(pipeline).to_list(length=None)
        else:
            articles = await collection.find({}, projection).sort("published_at", -1).to_list(length=None)

        response_data = {
            "status": "success",
            "count": len(articles),
            "data": articles,
        }

        response_bytes = _json_dumps(response_data)
        cache.set(cache_key, response_bytes, ttl=300)
        return Response(content=response_bytes, media_type="application/json", headers=headers)
    except Exception as e:
        log.error("Error fetching all saved articles: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred.")

@app.get("/api/articles/new")
@rate_limiter.limit("60/minute")
async def get_newest_articles(
    request: Request,
    company: Optional[str] = Query(None, min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9\s\-\.]+$", description="Company name or ticker (alphanumeric, max 100 chars)"),
):
    """
    Retrieves only the articles fetched during the most recent fetch pipeline run.
    """
    headers = {"Cache-Control": "public, max-age=60, s-maxage=300, stale-while-revalidate=60"}
    cache_key = f"articles:{company or 'all'}:new"

    cached_bytes = cache.get(cache_key)
    if cached_bytes is not None:
        return Response(content=cached_bytes, media_type="application/json", headers=headers)

    try:
        collection = get_async_collection()

        # Find the absolute latest fetched_at timestamp in the collection
        latest_article = await collection.find_one({}, sort=[("fetched_at", -1)])
        if not latest_article or "fetched_at" not in latest_article:
            return Response(content=_json_dumps({"status": "success", "count": 0, "data": []}), media_type="application/json")

        latest_time = latest_article["fetched_at"]
        
        # A single fetch pipeline run usually takes a few seconds to a minute.
        # We grab everything fetched within 10 minutes of the absolute latest article
        # to ensure we capture the entire batch.
        batch_threshold = latest_time - timedelta(minutes=10)

        projection = {
            "_id": 0,
            "content": 0,
            "url_hash": 0,
            "created_at": 0,
            # We keep fetched_at out of the response to match other endpoints, 
            # though it's used for the query.
            "fetched_at": 0, 
        }

        query = {"fetched_at": {"$gte": batch_threshold}}

        if company:
            pipeline = [
                {"$match": {"$text": {"$search": company}, "fetched_at": {"$gte": batch_threshold}}},
                {"$sort": {"published_at": -1}},
                {"$project": projection},
            ]
            articles = await collection.aggregate(pipeline).to_list(length=None)
        else:
            articles = await collection.find(query, projection).sort("published_at", -1).to_list(length=None)

        response_data = {
            "status": "success",
            "count": len(articles),
            "data": articles,
        }

        response_bytes = _json_dumps(response_data)
        cache.set(cache_key, response_bytes, ttl=300)
        return Response(content=response_bytes, media_type="application/json", headers=headers)
    except Exception as e:
        log.error("Error fetching newest articles: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred.")

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
    _api_key: str = Depends(verify_api_key),
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
def send_newsletter_to_email(req: SendEmailRequest, background_tasks: BackgroundTasks, _api_key: str = Depends(verify_api_key)) -> Dict[str, Any]:
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
@rate_limiter.limit("5/minute")
def subscribe(request: Request, req: SubscriberRequest) -> Dict[str, Any]:
    """
    Subscribes a user email address to receive daily news updates.
    """
    try:
        result = newsletter.subscribe_email(req.email)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        log.error("Error subscribing email %s: %s", req.email, e, exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred.")


@app.post("/api/newsletter/unsubscribe")
@rate_limiter.limit("5/minute")
def unsubscribe(request: Request, req: SubscriberRequest) -> Dict[str, Any]:
    """
    Unsubscribes a user email address from daily news updates.
    """
    try:
        result = newsletter.unsubscribe_email(req.email)
        return result
    except Exception as e:
        log.error("Error unsubscribing email %s: %s", req.email, e, exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred.")


@app.get("/api/newsletter/subscribers")
def list_subscribers(_api_key: str = Depends(verify_api_key)) -> Dict[str, Any]:
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
        log.error("Error listing subscribers: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred.")


@app.post("/api/newsletter/broadcast", status_code=202)
def broadcast_newsletter_to_all(req: Optional[BroadcastRequest] = None, background_tasks: BackgroundTasks = None, _api_key: str = Depends(verify_api_key)) -> Dict[str, Any]:
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


# ---------------------------------------------------------------------------
# Token-Based 1-Click Unsubscribe (GET with signed HMAC token)
# ---------------------------------------------------------------------------

@app.get("/api/newsletter/unsubscribe")
@rate_limiter.limit("10/minute")
def unsubscribe_via_token(
    request: Request,
    token: str = Query(..., description="Signed HMAC unsubscribe token from email"),
) -> Dict[str, Any]:
    """1-click unsubscribe endpoint triggered from email footer links.

    Validates the cryptographically signed token to extract the email
    address, then performs unsubscription. Invalid or expired tokens
    are rejected with HTTP 400.
    """
    if not _TOKENS_AVAILABLE or validate_unsubscribe_token is None:
        raise HTTPException(
            status_code=501,
            detail="Token-based unsubscribe is not configured.",
        )

    email = validate_unsubscribe_token(token)
    if not email:
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired unsubscribe token.",
        )

    try:
        result = newsletter.unsubscribe_email(email)
        return {
            "status": "success",
            "email": email,
            "message": f"Successfully unsubscribed {email} from newsletter.",
        }
    except Exception as e:
        log.error("Error during token unsubscribe for %s: %s", email, e, exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred.")


# ---------------------------------------------------------------------------
# Real-Time News Streaming via Server-Sent Events (SSE)
# ---------------------------------------------------------------------------

# In-memory event bus for SSE subscribers (lightweight pub/sub)
_sse_subscribers: list[asyncio.Queue] = []
_sse_lock = threading.Lock()


def publish_sse_event(event_data: Dict[str, Any]) -> None:
    """Push a news event to all connected SSE clients.

    Called from background pipeline tasks when new articles are ingested.
    Thread-safe: acquires lock before iterating subscriber queues.
    """
    with _sse_lock:
        for queue in _sse_subscribers:
            try:
                queue.put_nowait(event_data)
            except asyncio.QueueFull:
                pass  # Drop events for slow consumers


if _SSE_AVAILABLE:
    @app.get("/api/stream/news")
    async def stream_news(request: Request):
        """Real-time Server-Sent Events (SSE) stream of incoming news articles.

        Connect with: `curl -N http://localhost:10000/api/stream/news`
        or use EventSource in JavaScript.

        Events are pushed when the fetch pipeline ingests new articles.
        The stream stays open until the client disconnects.
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=100)

        with _sse_lock:
            _sse_subscribers.append(queue)

        async def event_generator():
            try:
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        data = await asyncio.wait_for(queue.get(), timeout=30.0)
                        yield {
                            "event": "article",
                            "data": _json_dumps(data).decode("utf-8"),
                        }
                    except asyncio.TimeoutError:
                        # Send keepalive comment to prevent proxy/CDN timeout
                        yield {"comment": "keepalive"}
            finally:
                with _sse_lock:
                    if queue in _sse_subscribers:
                        _sse_subscribers.remove(queue)

        return EventSourceResponse(event_generator())


# ---------------------------------------------------------------------------
# Readiness Health Check — reports whether the DB is actually reachable.
# Orchestrators (Docker, K8s, Render) use this to route traffic only to
# healthy instances, preventing users from hitting cold/broken pods.
# ---------------------------------------------------------------------------

@app.get("/health/ready")
async def readiness_check():
    """Deep health check: verifies MongoDB connectivity.

    NOTE: Intentionally unauthenticated — this endpoint is consumed by
    orchestrator probes (Docker, K8s, Render) and must remain publicly
    accessible. It only returns {"status": "ready"} or HTTP 503,
    exposing no sensitive data.
    """
    try:
        client = get_async_client()
        await client.admin.command("ping")
        return {"status": "ready"}
    except Exception:
        raise HTTPException(status_code=503, detail="Database unavailable")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=10000, reload=True)
