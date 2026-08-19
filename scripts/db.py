"""
db.py — Shared MongoDB connection module (singleton client).

Provides a lazily-initialized MongoClient that is reused across the entire
application lifetime, avoiding the overhead of creating a new connection,
pinging the server, and recreating indexes on every API call.
"""

from __future__ import annotations

import logging
import os
import threading

import certifi
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient, ReadPreference
from pymongo.errors import ConnectionFailure, OperationFailure

load_dotenv()

log = logging.getLogger(__name__)

MONGO_URI: str = os.getenv("MONGO_URI", "")
MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "news_pipeline")
MONGO_COLLECTION: str = os.getenv("MONGO_COLLECTION", "articles")
MONGO_SUBSCRIBERS_COLLECTION: str = os.getenv("MONGO_SUBSCRIBERS_COLLECTION", "subscribers")

# ---------------------------------------------------------------------------
# Singleton state (thread-safe)
# ---------------------------------------------------------------------------

_client: MongoClient | None = None
_client_lock = threading.Lock()
_indexes_ensured: set[str] = set()
_indexes_lock = threading.Lock()


def _safe_create_index(collection, keys, **kwargs):
    """Safely create an index without allowing non-fatal index conflicts to crash startup."""
    try:
        collection.create_index(keys, **kwargs)
    except OperationFailure as exc:
        log.warning("Index creation notice for %s on '%s': %s", keys, collection.name, exc)
    except Exception as exc:
        log.warning("Unexpected error creating index for %s on '%s': %s", keys, collection.name, exc)


def get_client() -> MongoClient:
    """Return a lazily-initialized, reusable MongoClient singleton."""
    global _client
    if _client is not None:
        return _client

    with _client_lock:
        # Double-checked locking
        if _client is not None:
            return _client

        if not MONGO_URI:
            log.critical("MONGO_URI is not set in .env — cannot connect to MongoDB.")
            raise RuntimeError("MONGO_URI is not set in .env")

        try:
            client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=10_000,
                tlsCAFile=certifi.where(),
                # --- Connection pool tuning for low idle bandwidth ---
                maxPoolSize=10,         # support 10 concurrent DB ops per worker
                minPoolSize=0,          # drop idle connections when inactive to save bandwidth
                maxIdleTimeMS=15_000,   # reclaim idle connections after 15s
            )
            client.admin.command("ping")  # fail-fast connectivity check
            log.info("Connected to MongoDB cluster (singleton client).")
        except ConnectionFailure as exc:
            log.critical("MongoDB connection failed: %s", exc)
            raise RuntimeError(f"MongoDB connection failed: {exc}")

        _client = client
        return _client


def get_db():
    """Return the application database."""
    return get_client()[MONGO_DB_NAME]


def get_collection():
    """
    Return the *articles* collection, ensuring indexes exist
    (only on the first call).
    """
    db = get_db()
    collection = db[MONGO_COLLECTION]

    # Fast path: skip lock entirely after first initialization
    if "articles" in _indexes_ensured:
        return collection

    with _indexes_lock:
        if "articles" not in _indexes_ensured:
            _safe_create_index(collection, "url_hash", unique=True, background=True)
            _safe_create_index(collection, [("published_at", -1)], background=True)
            _safe_create_index(collection, "source", background=True)
            _safe_create_index(collection, "category", background=True)
            _safe_create_index(collection, [("category", 1), ("published_at", -1)], background=True)
            _safe_create_index(collection, [("source", 1), ("published_at", -1)], background=True)
            _safe_create_index(
                collection,
                [("title", "text"), ("description", "text"), ("keywords", "text")],
                background=True,
            )

            # --- TTL Index: Automatically disabled by default (0 = keep forever) ---
            ttl_days = int(os.getenv("ARTICLE_TTL_DAYS", "0"))
            if ttl_days > 0:
                try:
                    collection.create_index(
                        "published_at",
                        expireAfterSeconds=ttl_days * 24 * 60 * 60,
                        name="ttl_published_at",
                        background=True,
                    )
                    log.info("TTL index set: articles auto-expire after %d days.", ttl_days)
                except OperationFailure as exc:
                    # Handle IndexOptionsConflict (code 85) with legacy 'published_at_1' index
                    if exc.code == 85 or "IndexOptionsConflict" in str(exc) or "already exists" in str(exc):
                        log.info("Index conflict on published_at (code 85). Dropping legacy 'published_at_1' index...")
                        try:
                            collection.drop_index("published_at_1")
                            collection.create_index(
                                "published_at",
                                expireAfterSeconds=ttl_days * 24 * 60 * 60,
                                name="ttl_published_at",
                                background=True,
                            )
                            log.info("Successfully dropped legacy index and created TTL index.")
                        except Exception as drop_err:
                            log.warning("Could not replace legacy published_at index with TTL index: %s", drop_err)
                    else:
                        log.warning("Could not create TTL index: %s", exc)
                except Exception as exc:
                    log.warning("Unexpected error creating TTL index: %s", exc)
            else:
                try:
                    collection.drop_index("ttl_published_at")
                    log.info("TTL index disabled (dropped 'ttl_published_at'). Articles will persist indefinitely.")
                except Exception:
                    pass

            # --- Compound indexes for NLP-enriched field queries ---
            _safe_create_index(
                collection,
                [("entities.tickers", 1), ("published_at", -1)],
                background=True,
            )
            _safe_create_index(
                collection,
                [("sentiment.label", 1), ("published_at", -1)],
                background=True,
            )
            _safe_create_index(collection, "content_hash", background=True, sparse=True)
            _safe_create_index(collection, [("fetched_at", -1)], background=True)

            log.info("Indexes ensured on collection '%s'.", MONGO_COLLECTION)
            _indexes_ensured.add("articles")

    return collection


def get_subscribers_collection():
    """
    Return the *subscribers* collection, ensuring the email unique index
    exists (only on the first call).
    """
    db = get_db()
    collection = db[MONGO_SUBSCRIBERS_COLLECTION]

    # Fast path: skip lock entirely after first initialization
    if "subscribers" in _indexes_ensured:
        return collection

    with _indexes_lock:
        if "subscribers" not in _indexes_ensured:
            _safe_create_index(collection, "email", unique=True)
            log.info("Indexes ensured on collection '%s'.", MONGO_SUBSCRIBERS_COLLECTION)
            _indexes_ensured.add("subscribers")

    return collection


def ensure_all_indexes():
    """
    Pre-warm the database connection and ensure all indexes exist.
    Call this once at application startup so that no request ever pays
    the index-creation cost or contends on the indexes lock.
    """
    log.info("Pre-warming MongoDB connection and ensuring indexes...")
    get_collection()
    get_subscribers_collection()
    log.info("All indexes ensured and connection pre-warmed.")


# ---------------------------------------------------------------------------
# Async Motor client (for high-concurrency read endpoints)
# ---------------------------------------------------------------------------

_async_client: AsyncIOMotorClient | None = None
_async_client_lock = threading.Lock()


def get_async_client() -> AsyncIOMotorClient:
    """Return a lazily-initialized, reusable async Motor client singleton.

    Motor wraps PyMongo with an async API, so every `await collection.find()`
    yields control back to the event-loop instead of blocking a threadpool
    slot.  This lets a single Uvicorn worker handle thousands of in-flight
    MongoDB reads concurrently.
    """
    global _async_client
    if _async_client is not None:
        return _async_client

    with _async_client_lock:
        if _async_client is not None:
            return _async_client

        if not MONGO_URI:
            raise RuntimeError("MONGO_URI is not set in .env")

        _async_client = AsyncIOMotorClient(
            MONGO_URI,
            serverSelectionTimeoutMS=10_000,
            tlsCAFile=certifi.where(),
            maxPoolSize=10,
            minPoolSize=0,
            maxIdleTimeMS=15_000,
        )
        log.info("Async Motor client initialized.")
        return _async_client


def get_async_db():
    """Return the application database via the async Motor client."""
    return get_async_client()[MONGO_DB_NAME]


def get_async_collection(read_pref=ReadPreference.SECONDARY_PREFERRED):
    """Return the *articles* collection via the async Motor client.

    Defaults to SECONDARY_PREFERRED read-preference so reads are
    distributed across replica-set members instead of hammering
    the primary node.
    """
    db = get_async_db()
    return db.get_collection(MONGO_COLLECTION, read_preference=read_pref)


def get_async_subscribers_collection(read_pref=ReadPreference.PRIMARY):
    """Return the *subscribers* collection via the async Motor client.

    Uses PRIMARY read-preference by default since subscriber operations
    often involve immediate-read-after-write consistency.
    """
    db = get_async_db()
    return db.get_collection(MONGO_SUBSCRIBERS_COLLECTION, read_preference=read_pref)
