"""
newsletter.py — Daily News Newsletter & Subscriber Manager

Handles:
1. Subscribing / Unsubscribing user email addresses in MongoDB.
2. Querying today's fetched articles from MongoDB.
3. Generating responsive, modern HTML & text email newsletters.
4. Dispatching newsletters via SMTP (Gmail, Mailgun, custom SMTP, etc.).
"""

from __future__ import annotations

import argparse
import logging
import os
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# ---------------------------------------------------------------------------
# Environment Setup & Configuration
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
MONGO_ARTICLES_COLLECTION: str = os.getenv("MONGO_COLLECTION", "articles")
MONGO_SUBSCRIBERS_COLLECTION: str = os.getenv("MONGO_SUBSCRIBERS_COLLECTION", "subscribers")

# SMTP Configuration
SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER: str = os.getenv("SMTP_USER", "")
SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
EMAIL_FROM: str = os.getenv("EMAIL_FROM", SMTP_USER or "News Pipeline Digest <noreply@newspipeline.com>")

# ---------------------------------------------------------------------------
# MongoDB Connections & Operations
# ---------------------------------------------------------------------------

def get_db():
    """Connect to MongoDB cluster and return database object."""
    if not MONGO_URI:
        log.critical("MONGO_URI is not set in environment.")
        raise RuntimeError("MONGO_URI is not set in environment.")

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000, tlsCAFile=certifi.where())
        client.admin.command("ping")
        return client[MONGO_DB_NAME]
    except ConnectionFailure as exc:
        log.critical("MongoDB connection failed: %s", exc)
        raise RuntimeError(f"MongoDB connection failed: {exc}")


def get_subscribers_collection():
    """Get the subscribers collection with an index on email."""
    db = get_db()
    collection = db[MONGO_SUBSCRIBERS_COLLECTION]
    collection.create_index("email", unique=True)
    return collection


def get_articles_collection():
    """Get the articles collection."""
    db = get_db()
    return db[MONGO_ARTICLES_COLLECTION]


def subscribe_email(email: str) -> Dict[str, Any]:
    """
    Subscribe an email address to the newsletter.
    Upserts entry in MongoDB subscribers collection.
    """
    email_clean = email.strip().lower()
    if not email_clean or "@" not in email_clean:
        raise ValueError(f"Invalid email address: '{email}'")

    collection = get_subscribers_collection()
    now = datetime.now(timezone.utc)
    
    result = collection.update_one(
        {"email": email_clean},
        {
            "$set": {
                "active": True,
                "updated_at": now,
            },
            "$setOnInsert": {
                "created_at": now,
            }
        },
        upsert=True
    )
    
    action = "re-activated" if result.matched_count > 0 else "subscribed"
    log.info("Email '%s' successfully %s.", email_clean, action)
    return {
        "status": "success",
        "email": email_clean,
        "action": action,
        "active": True
    }


def unsubscribe_email(email: str) -> Dict[str, Any]:
    """
    Unsubscribe an email address from the newsletter (soft delete: sets active=False).
    """
    email_clean = email.strip().lower()
    collection = get_subscribers_collection()
    now = datetime.now(timezone.utc)

    result = collection.update_one(
        {"email": email_clean},
        {
            "$set": {
                "active": False,
                "updated_at": now,
            }
        }
    )

    if result.matched_count == 0:
        log.info("Unsubscribe requested for non-existent email '%s'.", email_clean)
        return {"status": "not_found", "message": f"Email '{email_clean}' not found in subscribers list."}

    log.info("Email '%s' unsubscribed successfully.", email_clean)
    return {"status": "success", "email": email_clean, "active": False}


def get_active_subscribers() -> List[str]:
    """Retrieve list of all active subscriber email addresses."""
    collection = get_subscribers_collection()
    cursor = collection.find({"active": True}, {"_id": 0, "email": 1})
    return [doc["email"] for doc in cursor if "email" in doc]

# ---------------------------------------------------------------------------
# News Fetching for Digest
# ---------------------------------------------------------------------------

def fetch_todays_news(limit: int = 10, company: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch articles from MongoDB published or fetched within the last 24 hours.
    Falls back to latest articles if no recent news is found.
    """
    collection = get_articles_collection()
    
    # Define past 24 hours window
    now = datetime.now(timezone.utc)
    start_of_day = now - timedelta(hours=24)
    
    # Query for recent articles
    query: Dict[str, Any] = {
        "$or": [
            {"published_at": {"$gte": start_of_day.isoformat()}},
            {"fetched_at": {"$gte": start_of_day.isoformat()}}
        ]
    }
    
    if company:
        query_lower = company.lower()
        company_filter = {
            "$or": [
                {"title": {"$regex": query_lower, "$options": "i"}},
                {"description": {"$regex": query_lower, "$options": "i"}},
                {"keywords": {"$regex": query_lower, "$options": "i"}}
            ]
        }
        query = {"$and": [query, company_filter]}

    cursor = collection.find(query, {"_id": 0}).sort([("published_at", -1)]).limit(limit)
    articles = list(cursor)

    # Fallback to absolute latest articles if past 24 hours has no items
    if not articles:
        log.info("No articles found in past 24h, falling back to latest stored articles.")
        fallback_query = {}
        if company:
            fallback_query = {
                "$or": [
                    {"title": {"$regex": company, "$options": "i"}},
                    {"description": {"$regex": company, "$options": "i"}},
                    {"keywords": {"$regex": company, "$options": "i"}}
                ]
            }
        cursor = collection.find(fallback_query, {"_id": 0}).sort([("published_at", -1)]).limit(limit)
        articles = list(cursor)

    return articles

# ---------------------------------------------------------------------------
# Newsletter HTML & Text Template Generators
# ---------------------------------------------------------------------------

def render_html_newsletter(articles: List[Dict[str, Any]], recipient_email: Optional[str] = None) -> str:
    """
    Renders a responsive, modern HTML email digest template.
    """
    date_str = datetime.now(timezone.utc).strftime("%A, %B %d, %Y")
    
    articles_html = ""
    for idx, item in enumerate(articles, 1):
        title = item.get("title") or "Untitled Article"
        url = item.get("url") or "#"
        source = item.get("source") or "News Pipeline"
        category = (item.get("category") or "General").capitalize()
        description = item.get("description") or "No description available."
        published_at = item.get("published_at") or ""
        image_url = item.get("image_url")
        keywords = item.get("keywords") or []
        sentiment = item.get("sentiment", {})
        polarity = sentiment.get("polarity", 0.0)

        # Sentiment badge design
        if polarity > 0.1:
            sentiment_badge = '<span style="background-color: #d1fae5; color: #065f46; font-size: 11px; font-weight: 600; padding: 3px 8px; border-radius: 12px; margin-left: 6px;">Positive</span>'
        elif polarity < -0.1:
            sentiment_badge = '<span style="background-color: #fee2e2; color: #991b1b; font-size: 11px; font-weight: 600; padding: 3px 8px; border-radius: 12px; margin-left: 6px;">Negative</span>'
        else:
            sentiment_badge = '<span style="background-color: #f3f4f6; color: #374151; font-size: 11px; font-weight: 600; padding: 3px 8px; border-radius: 12px; margin-left: 6px;">Neutral</span>'

        # Formatted keywords pills
        kw_html = ""
        if keywords:
            tags = "".join(
                f'<span style="display: inline-block; background-color: #eff6ff; color: #1e40af; font-size: 11px; font-weight: 500; padding: 2px 6px; border-radius: 4px; margin-right: 4px; margin-bottom: 4px;">#{kw}</span>'
                for kw in keywords[:4]
            )
            kw_html = f'<div style="margin-top: 10px;">{tags}</div>'

        # Optional image block
        img_html = ""
        if image_url and image_url.startswith("http"):
            img_html = f'<img src="{image_url}" alt="Article Image" style="width: 100%; max-height: 200px; object-fit: cover; border-radius: 8px; margin-bottom: 12px;" />'

        articles_html += f"""
        <div style="background-color: #ffffff; border: 1px solid #e5e7eb; border-radius: 10px; padding: 20px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.05);">
            {img_html}
            <div style="margin-bottom: 8px;">
                <span style="background-color: #2563eb; color: #ffffff; font-size: 11px; font-weight: 700; padding: 3px 8px; border-radius: 4px; text-transform: uppercase;">{category}</span>
                <span style="color: #6b7280; font-size: 12px; margin-left: 8px;">via <strong>{source}</strong></span>
                {sentiment_badge}
            </div>
            <h2 style="margin: 8px 0; font-size: 18px; font-weight: 700; line-height: 1.3;">
                <a href="{url}" target="_blank" style="color: #111827; text-decoration: none;">{title}</a>
            </h2>
            <p style="color: #4b5563; font-size: 14px; line-height: 1.5; margin: 8px 0 12px 0;">{description}</p>
            {kw_html}
            <div style="margin-top: 12px; padding-top: 10px; border-top: 1px solid #f3f4f6; text-align: right;">
                <a href="{url}" target="_blank" style="display: inline-block; color: #2563eb; font-size: 13px; font-weight: 600; text-decoration: none;">Read full story &rarr;</a>
            </div>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Daily News Digest</title>
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f3f4f6; margin: 0; padding: 20px 0; color: #1f2937;">
    <div style="max-width: 640px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.08);">
        <!-- Header -->
        <div style="background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); color: #ffffff; padding: 32px 24px; text-align: center;">
            <span style="background-color: rgba(255,255,255,0.15); font-size: 11px; font-weight: 700; letter-spacing: 1px; padding: 4px 12px; border-radius: 20px; text-transform: uppercase;">Daily News Pipeline Digest</span>
            <h1 style="margin: 12px 0 4px 0; font-size: 26px; font-weight: 800;">Today's Headlines</h1>
            <p style="margin: 0; color: #94a3b8; font-size: 14px;">{date_str} &bull; {len(articles)} Stories Curated For You</p>
        </div>

        <!-- Main Content Area -->
        <div style="padding: 24px; background-color: #f9fafb;">
            {articles_html}
        </div>

        <!-- Footer -->
        <div style="background-color: #1e293b; color: #94a3b8; padding: 24px; text-align: center; font-size: 12px; line-height: 1.6;">
            <p style="margin: 0 0 8px 0; font-weight: 600; color: #cbd5e1;">News Pipeline Automated Digest</p>
            <p style="margin: 0 0 12px 0;">You are receiving this email because you subscribed to daily news updates.</p>
            <p style="margin: 0;">
                Powered by FastAPI &amp; MongoDB Atlas
            </p>
        </div>
    </div>
</body>
</html>
"""
    return html


def render_text_newsletter(articles: List[Dict[str, Any]]) -> str:
    """
    Renders a clean plain-text newsletter fallback.
    """
    date_str = datetime.now(timezone.utc).strftime("%A, %B %d, %Y")
    lines = [
        "============================================================",
        f"DAILY NEWS PIPELINE DIGEST — {date_str}",
        "============================================================",
        "",
    ]
    for idx, item in enumerate(articles, 1):
        lines.append(f"{idx}. {item.get('title', 'Untitled')}")
        lines.append(f"   Source: {item.get('source', 'Unknown')} | Category: {item.get('category', 'General')}")
        lines.append(f"   Link: {item.get('url', '')}")
        lines.append(f"   Summary: {item.get('description', '')}")
        lines.append("")

    lines.append("============================================================")
    lines.append("Powered by News Pipeline Service")
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Email Dispatching via SMTP
# ---------------------------------------------------------------------------

def send_email(to_email: str, subject: str, html_content: str, text_content: str) -> None:
    """
    Sends an email using standard SMTP.
    """
    if not SMTP_USER or not SMTP_PASSWORD:
        log.critical("SMTP_USER or SMTP_PASSWORD is not configured in environment.")
        raise RuntimeError("SMTP credentials (SMTP_USER / SMTP_PASSWORD) are missing in environment variables.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = to_email

    msg.attach(MIMEText(text_content, "plain"))
    msg.attach(MIMEText(html_content, "html"))

    log.info("Connecting to SMTP server %s:%d ...", SMTP_HOST, SMTP_PORT)
    
    if SMTP_PORT == 465:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(EMAIL_FROM, [to_email], msg.as_string())
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(EMAIL_FROM, [to_email], msg.as_string())

    log.info("Email successfully sent to '%s'.", to_email)


def send_todays_news_email(
    to_email: str,
    limit: int = 10,
    company: Optional[str] = None,
    subject: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetches today's news and emails the digest to a specified email address.
    """
    articles = fetch_todays_news(limit=limit, company=company)
    if not articles:
        return {
            "status": "warning",
            "message": "No news articles found to send.",
            "articles_count": 0
        }

    date_str = datetime.now(timezone.utc).strftime("%b %d, %Y")
    email_subject = subject or f"📰 Today's News Digest - {date_str}"

    html_content = render_html_newsletter(articles, recipient_email=to_email)
    text_content = render_text_newsletter(articles)

    send_email(
        to_email=to_email,
        subject=email_subject,
        html_content=html_content,
        text_content=text_content,
    )

    return {
        "status": "success",
        "recipient": to_email,
        "articles_sent": len(articles),
        "subject": email_subject
    }


def broadcast_newsletter(limit: int = 10, company: Optional[str] = None) -> Dict[str, Any]:
    """
    Dispatches today's news newsletter to all active subscribers in MongoDB.
    """
    subscribers = get_active_subscribers()
    if not subscribers:
        log.warning("No active subscribers found in database.")
        return {
            "status": "warning",
            "message": "No active subscribers found in database.",
            "sent_count": 0
        }

    articles = fetch_todays_news(limit=limit, company=company)
    if not articles:
        return {
            "status": "warning",
            "message": "No news articles found to broadcast.",
            "sent_count": 0
        }

    date_str = datetime.now(timezone.utc).strftime("%b %d, %Y")
    subject = f"📰 Today's News Digest - {date_str}"

    sent_count = 0
    failed_count = 0
    errors = []

    for email in subscribers:
        try:
            html_content = render_html_newsletter(articles, recipient_email=email)
            text_content = render_text_newsletter(articles)
            send_email(to_email=email, subject=subject, html_content=html_content, text_content=text_content)
            sent_count += 1
        except Exception as exc:
            log.error("Failed to send newsletter to %s: %s", email, exc)
            failed_count += 1
            errors.append({"email": email, "error": str(exc)})

    return {
        "status": "success",
        "sent_count": sent_count,
        "failed_count": failed_count,
        "total_subscribers": len(subscribers),
        "errors": errors
    }

# ---------------------------------------------------------------------------
# CLI Command Line Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="News Pipeline Newsletter & Subscriber Tool")
    parser.add_argument("--send-to", type=str, help="Email address to send today's news digest directly")
    parser.add_argument("--subscribe", type=str, help="Subscribe an email address to daily updates")
    parser.add_argument("--unsubscribe", type=str, help="Unsubscribe an email address from daily updates")
    parser.add_argument("--broadcast", action="store_true", help="Broadcast today's news digest to all active subscribers")
    parser.add_argument("--limit", type=int, default=10, help="Max number of news articles to include in newsletter")
    parser.add_argument("--company", type=str, default=None, help="Filter articles by company name/ticker")

    args = parser.parse_args()

    if args.subscribe:
        res = subscribe_email(args.subscribe)
        print("Subscribe result:", res)
    elif args.unsubscribe:
        res = unsubscribe_email(args.unsubscribe)
        print("Unsubscribe result:", res)
    elif args.send_to:
        res = send_todays_news_email(to_email=args.send_to, limit=args.limit, company=args.company)
        print("Send result:", res)
    elif args.broadcast:
        res = broadcast_newsletter(limit=args.limit, company=args.company)
        print("Broadcast result:", res)
    else:
        parser.print_help()
