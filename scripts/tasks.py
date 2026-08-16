"""
tasks.py — Distributed ARQ Worker

This module defines the ARQ worker configuration and the background tasks
that are pulled from the Redis queue and executed out-of-band from the API.

To run the worker:
    arq tasks.WorkerSettings
"""

import asyncio
import logging
import os
import time
from typing import Any, Dict, Optional

from arq.connections import RedisSettings
from dotenv import load_dotenv

import fetch
import clear_old_news
import newsletter

load_dotenv()

log = logging.getLogger("arq.worker")


# ---------------------------------------------------------------------------
# Task Functions (executed by the ARQ worker process)
# ---------------------------------------------------------------------------

async def run_pipeline_task(ctx: Dict[Any, Any], company: Optional[str] = None) -> str:
    """
    Background job to run the fetch pipeline.
    Uses Redis-based distributed locking to prevent multiple workers from
    running the fetch pipeline concurrently for the same company/query.
    """
    redis = ctx["redis"]
    key_suffix = company if company else "__all__"
    lock_key = f"newspipe:lock:fetch:{key_suffix}"
    
    # Simple distributed lock via SETNX (expires in 10 minutes to prevent deadlocks)
    acquired = await redis.set(lock_key, "1", nx=True, ex=600)
    if not acquired:
        msg = f"Fetch pipeline for '{key_suffix}' is already running. Job aborted."
        log.warning(msg)
        return msg

    started = time.time()
    try:
        # fetch.run_pipeline is fully synchronous, so we run it in a thread
        await asyncio.to_thread(fetch.run_pipeline, return_data=False, query=company)
        
        # Clear the API cache prefix so fresh results are served
        await _clear_cache(redis)
        
        duration = round(time.time() - started, 2)
        return f"Pipeline for '{key_suffix}' finished in {duration}s."
    except Exception as e:
        log.error(f"Fetch pipeline error: {e}", exc_info=True)
        raise
    finally:
        # Release the lock
        await redis.delete(lock_key)


async def send_email_task(ctx: Dict[Any, Any], email: str, limit: int = 10, company: Optional[str] = None) -> str:
    """Background job to send an individual newsletter email."""
    try:
        await asyncio.to_thread(
            newsletter.send_todays_news_email,
            to_email=email,
            limit=limit,
            company=company
        )
        return f"Successfully sent newsletter to {email}."
    except Exception as e:
        log.error(f"Failed to send newsletter to {email}: {e}", exc_info=True)
        raise


async def broadcast_task(ctx: Dict[Any, Any], limit: int = 10, company: Optional[str] = None) -> str:
    """Background job to broadcast the newsletter to all active subscribers."""
    try:
        result = await asyncio.to_thread(
            newsletter.broadcast_newsletter,
            limit=limit,
            company=company
        )
        sent = result.get("sent_count", 0)
        failed = result.get("failed_count", 0)
        return f"Broadcast complete: {sent} sent, {failed} failed."
    except Exception as e:
        log.error(f"Broadcast error: {e}", exc_info=True)
        raise


async def cleanup_task(ctx: Dict[Any, Any], days_old: int = 15) -> str:
    """Background job to execute the manual old article cleanup script."""
    try:
        await asyncio.to_thread(clear_old_news.run_cleanup, days_old=days_old)
        
        # Clear the API cache prefix so deleted articles are removed from cache
        redis = ctx["redis"]
        await _clear_cache(redis)
        
        return f"Cleanup complete for articles older than {days_old} days."
    except Exception as e:
        log.error(f"Cleanup error: {e}", exc_info=True)
        raise


async def _clear_cache(redis) -> None:
    """Helper to scan and delete all 'newspipe:' cached API responses."""
    try:
        cursor = b"0"
        while cursor:
            cursor, keys = await redis.scan(cursor, match="newspipe:articles:*", count=100)
            if keys:
                await redis.delete(*keys)
        log.info("API cache cleared successfully.")
    except Exception as e:
        log.error(f"Failed to clear cache: {e}")


# ---------------------------------------------------------------------------
# ARQ Worker Configuration
# ---------------------------------------------------------------------------

async def startup(ctx: Dict[Any, Any]) -> None:
    log.info("ARQ Worker starting...")
    # Pre-warm MongoDB connections inside the worker process
    import db
    await asyncio.to_thread(db.ensure_all_indexes)


async def shutdown(ctx: Dict[Any, Any]) -> None:
    log.info("ARQ Worker shutting down...")


# Default REDIS_URL or fallback to localhost
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

class WorkerSettings:
    """Configuration for the ARQ worker."""
    redis_settings = RedisSettings.from_dsn(REDIS_URL)
    functions = [
        run_pipeline_task,
        send_email_task,
        broadcast_task,
        cleanup_task,
    ]
    on_startup = startup
    on_shutdown = shutdown
    # Automatically retry failed jobs (e.g. SMTP transient failure)
    max_tries = 3
