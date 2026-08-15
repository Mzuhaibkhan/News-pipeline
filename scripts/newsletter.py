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
import html as html_mod
import logging
import os
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape

from db import get_db, get_collection as get_articles_collection, get_subscribers_collection

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

# MongoDB connections are provided by the shared singleton in db.py


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
    time_filter: Dict[str, Any] = {
        "$or": [
            {"published_at": {"$gte": start_of_day}},
            {"fetched_at": {"$gte": start_of_day}}
        ]
    }
    
    if company:
        # Use $text search (backed by text index on title, description, keywords)
        # instead of $regex which cannot use indexes and triggers collection scans.
        company_filter: Dict[str, Any] = {"$text": {"$search": company}}
        query: Dict[str, Any] = {"$and": [time_filter, company_filter]}
    else:
        query = time_filter

    cursor = collection.find(query, {"_id": 0}).sort([("published_at", -1)]).limit(limit)
    articles = list(cursor)

    # Fallback to absolute latest articles if past 24 hours has no items
    if not articles:
        log.info("No articles found in past 24h, falling back to latest stored articles.")
        if company:
            fallback_query: Dict[str, Any] = {"$text": {"$search": company}}
        else:
            fallback_query = {}
        cursor = collection.find(fallback_query, {"_id": 0}).sort([("published_at", -1)]).limit(limit)
        articles = list(cursor)

    return articles

# ---------------------------------------------------------------------------
# Newsletter HTML & Text Template Generators (Jinja2)
# ---------------------------------------------------------------------------

# Jinja2 environment — loads templates from the templates/ directory
_TEMPLATE_DIR = Path(__file__).parent / "templates"
_jinja_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATE_DIR)),
    autoescape=select_autoescape(["html"]),
    trim_blocks=True,
    lstrip_blocks=True,
)

# Import signed token generator (soft import — works without API_SECRET_KEY set)
try:
    from tokens import generate_unsubscribe_url as _generate_unsub_url
except Exception:
    _generate_unsub_url = None
    log.warning("tokens.py unavailable — signed unsubscribe links disabled.")


def _prepare_template_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Flatten article dicts into a template-friendly format with escaped fields."""
    prepared = []
    for item in articles:
        sentiment = item.get("sentiment", {})
        entities = item.get("entities", {})
        prepared.append({
            "title": html_mod.escape(item.get("title") or "Untitled Article"),
            "url": html_mod.escape(item.get("url") or "#"),
            "source": html_mod.escape(item.get("source") or "News Pipeline"),
            "category": html_mod.escape((item.get("category") or "General").capitalize()),
            "description": html_mod.escape(item.get("description") or "No description available."),
            "published_at": item.get("published_at") or "",
            "image_url": item.get("image_url"),
            "keywords": [html_mod.escape(kw) for kw in (item.get("keywords") or [])],
            "tickers": [html_mod.escape(t) for t in entities.get("tickers", [])],
            "sentiment_label": sentiment.get("label", "neutral"),
            "polarity": sentiment.get("polarity", 0.0),
        })
    return prepared


def render_html_newsletter(
    articles: List[Dict[str, Any]],
    recipient_email: Optional[str] = None,
) -> str:
    """
    Renders a responsive HTML email digest using the Jinja2 template.
    Includes sentiment badges, ticker pills, and a signed unsubscribe link.
    """
    date_str = datetime.now(timezone.utc).strftime("%A, %B %d, %Y")
    prepared = _prepare_template_articles(articles)

    # Generate signed unsubscribe URL per recipient
    unsubscribe_url = ""
    if recipient_email and _generate_unsub_url:
        try:
            unsubscribe_url = _generate_unsub_url(recipient_email)
        except Exception:
            pass

    # Compute summary stats for the header bar
    sentiment_counts = {"positive": 0, "negative": 0, "neutral": 0}
    for a in prepared:
        label = a.get("sentiment_label", "neutral")
        sentiment_counts[label] = sentiment_counts.get(label, 0) + 1

    stats = [
        {"value": len(prepared), "label": "Articles", "color": "#2563eb"},
        {"value": sentiment_counts["positive"], "label": "Positive", "color": "#059669"},
        {"value": sentiment_counts["negative"], "label": "Negative", "color": "#dc2626"},
    ]

    template = _jinja_env.get_template("digest.html")
    return template.render(
        subject=f"📰 News Pipeline Digest — {date_str}",
        date_str=date_str,
        article_count=len(prepared),
        articles=prepared,
        stats=stats,
        unsubscribe_url=unsubscribe_url,
    )


def render_text_newsletter(
    articles: List[Dict[str, Any]],
    recipient_email: Optional[str] = None,
) -> str:
    """
    Renders a plain-text newsletter fallback using the Jinja2 text template.
    """
    date_str = datetime.now(timezone.utc).strftime("%A, %B %d, %Y")
    prepared = _prepare_template_articles(articles)

    unsubscribe_url = ""
    if recipient_email and _generate_unsub_url:
        try:
            unsubscribe_url = _generate_unsub_url(recipient_email)
        except Exception:
            pass

    template = _jinja_env.get_template("digest.txt")
    return template.render(
        date_str=date_str,
        article_count=len(prepared),
        articles=prepared,
        unsubscribe_url=unsubscribe_url,
    )

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
    Reuses a single SMTP connection for all recipients to avoid per-email
    TCP + TLS handshake overhead.
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
    subject = f"\U0001f4f0 Today's News Digest - {date_str}"

    if not SMTP_USER or not SMTP_PASSWORD:
        log.critical("SMTP_USER or SMTP_PASSWORD is not configured — cannot broadcast.")
        return {
            "status": "error",
            "message": "SMTP credentials missing.",
            "sent_count": 0,
        }

    sent_count = 0
    failed_count = 0
    errors = []

    # Open ONE SMTP connection and reuse it for every subscriber.
    try:
        if SMTP_PORT == 465:
            server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15)
        else:
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)
            server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
    except Exception as exc:
        log.error("Failed to open SMTP connection for broadcast: %s", exc)
        return {
            "status": "error",
            "message": f"SMTP connection failed: {exc}",
            "sent_count": 0,
        }

    try:
        for email in subscribers:
            try:
                html_content = render_html_newsletter(articles, recipient_email=email)
                text_content = render_text_newsletter(articles, recipient_email=email)

                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = EMAIL_FROM
                msg["To"] = email
                msg.attach(MIMEText(text_content, "plain"))
                msg.attach(MIMEText(html_content, "html"))

                server.sendmail(EMAIL_FROM, [email], msg.as_string())
                sent_count += 1
            except Exception as exc:
                log.error("Failed to send newsletter to %s: %s", email, exc)
                failed_count += 1
                errors.append({"email": email, "error": str(exc)})
    finally:
        try:
            server.quit()
        except Exception:
            pass

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
