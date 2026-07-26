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
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

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

    with _indexes_lock:
        if "articles" not in _indexes_ensured:
            collection.create_index("url_hash", unique=True, background=True)
            collection.create_index("published_at", background=True)
            collection.create_index("source", background=True)
            collection.create_index("category", background=True)
            collection.create_index(
                [("title", "text"), ("description", "text"), ("keywords", "text")],
                background=True,
            )
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

    with _indexes_lock:
        if "subscribers" not in _indexes_ensured:
            collection.create_index("email", unique=True)
            log.info("Indexes ensured on collection '%s'.", MONGO_SUBSCRIBERS_COLLECTION)
            _indexes_ensured.add("subscribers")

    return collection
