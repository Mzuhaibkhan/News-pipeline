from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from pydantic import BaseModel, EmailStr
from typing import Dict, Any, List, Optional
import logging

import fetch
import clear_old_news
import newsletter

app = FastAPI(
    title="News Pipeline API",
    description="API for fetching, storing, and emailing daily news newsletters from various sources.",
    version="1.1.0"
)

log = logging.getLogger(__name__)


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

@app.get("/api/articles")
def get_articles(company: str = Query(None, description="Optional company name or ticker to filter by"), limit: int = Query(None, description="Max number of articles to return")) -> Dict[str, Any]:
    """
    Retrieves already fetched articles directly from the database without querying external APIs.
    This naturally skips any broken/revoked external APIs.
    """
    try:
        articles = fetch.get_saved_articles(query=company, limit=limit)
        return {
            "status": "success",
            "count": len(articles),
            "data": articles
        }
    except Exception as e:
        log.error(f"Error fetching saved articles: {e}")
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

