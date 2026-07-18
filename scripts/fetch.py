"""
fetch.py — News Pipeline
Fetches articles from NewsAPI, The Guardian API, and RSS feeds.
Performs full enrichment (language detection, sentiment, keyword extraction)
and upserts into a MongoDB Atlas cluster.

Requirements (install via pip):
    pymongo[srv]
    python-dotenv
    requests
    feedparser
    langdetect
    textblob
    rake-nltk
    nltk

Environment variables (in .env file):
    MONGO_URI          — MongoDB connection string (e.g. mongodb+srv://...)
    MONGO_DB_NAME      — Database name (default: news_pipeline)
    MONGO_COLLECTION   — Collection name (default: articles)
    NEWSAPI_KEY        — API key from https://newsapi.org  (optional)
    GUARDIAN_KEY       — API key from https://open-platform.theguardian.com (optional)
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Generator

import feedparser
import nltk
import certifi
import requests
from dotenv import load_dotenv
from langdetect import LangDetectException, detect
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, ConnectionFailure
from rake_nltk import Rake
from textblob import TextBlob

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Download required NLTK data silently
for _pkg in ("stopwords", "punkt", "punkt_tab"):
    try:
        nltk.download(_pkg, quiet=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MONGO_URI: str = os.getenv("MONGO_URI", "")
MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "news_pipeline")
MONGO_COLLECTION: str = os.getenv("MONGO_COLLECTION", "articles")

NEWSAPI_KEY: str = os.getenv("NEWSAPI_KEY", "")
GUARDIAN_KEY: str = os.getenv("GUARDIAN_KEY", "")

TIINGO_KEY: str = os.getenv("TIINGO_KEY", "")
MARKETAUX_KEY: str = os.getenv("MARKETAUX_KEY", "")
STOCK_NEWS_KEY: str = os.getenv("STOCK_NEWS_KEY", "")
APITUBE_KEY: str = os.getenv("APITUBE_KEY", "")
GNEWS_KEY: str = os.getenv("GNEWS_KEY", "")
FINNHUB_KEY: str = os.getenv("FINNHUB_KEY", "")
INDIANAPI_KEY: str = os.getenv("INDIANAPI_KEY", "")
NEWSDATA_KEY: str = os.getenv("NEWSDATA_KEY", "")

CUSTOM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# NewsAPI — top headlines config
NEWSAPI_CATEGORIES: list[str] = [
    "business", "entertainment", "general",
    "health", "science", "sports", "technology",
]
NEWSAPI_COUNTRIES: list[str] = ["us", "in"]
NEWSAPI_PAGE_SIZE: int = 100  # max allowed

# The Guardian — sections to query
GUARDIAN_SECTIONS: list[str] = [
    "world", "technology", "science", "business",
    "sport", "culture", "politics", "environment",
]
GUARDIAN_PAGE_SIZE: int = 30  # max per request

# RSS feeds — add/remove freely
RSS_FEEDS: dict[str, str] = {
    "BBC News": "http://feeds.bbci.co.uk/news/rss.xml",
    "BBC Technology": "http://feeds.bbci.co.uk/news/technology/rss.xml",
    "Reuters Top News": "https://feeds.reuters.com/reuters/topNews",
    "Reuters Business": "https://feeds.reuters.com/reuters/businessNews",
    "Reuters Technology": "https://feeds.reuters.com/reuters/technologyNews",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "NPR News": "https://feeds.npr.org/1001/rss.xml",
    "TechCrunch": "https://techcrunch.com/feed/",
    "Hacker News": "https://hnrss.org/frontpage",
    "NASA Breaking News": "https://www.nasa.gov/rss/dyn/breaking_news.rss",
    "The Verge": "https://www.theverge.com/rss/index.xml",
    "Wired": "https://www.wired.com/feed/rss",
    "Ars Technica": "http://feeds.arstechnica.com/arstechnica/index",
    "MIT Tech Review": "https://www.technologyreview.com/feed/",
}

REQUEST_TIMEOUT: int = 15  # seconds
REQUEST_DELAY: float = 0.3  # polite delay between API calls


# ---------------------------------------------------------------------------
# Enrichment helpers
# ---------------------------------------------------------------------------

def _detect_language(text: str) -> str:
    """Return ISO 639-1 language code, or 'unknown' on failure."""
    try:
        return detect(text[:500]) if text.strip() else "unknown"
    except LangDetectException:
        return "unknown"


def _sentiment(text: str) -> dict[str, float]:
    """Return polarity [-1, 1] and subjectivity [0, 1] via TextBlob."""
    if not text.strip():
        return {"polarity": 0.0, "subjectivity": 0.0}
    blob = TextBlob(text[:2000])
    return {
        "polarity": round(blob.sentiment.polarity, 4),
        "subjectivity": round(blob.sentiment.subjectivity, 4),
    }


def _keywords(text: str, max_keywords: int = 10) -> list[str]:
    """Extract top keywords using RAKE."""
    if not text.strip():
        return []
    r = Rake()
    r.extract_keywords_from_text(text[:2000])
    ranked = r.get_ranked_phrases()
    return ranked[:max_keywords]


def _url_hash(url: str) -> str:
    """Stable deduplication key from article URL."""
    return hashlib.sha256(url.encode()).hexdigest()


def enrich(article: dict[str, Any]) -> dict[str, Any]:
    """Add language, sentiment, and keywords to an article dict in-place."""
    combined_text = " ".join(filter(None, [
        article.get("title", ""),
        article.get("description", ""),
        article.get("content", ""),
    ]))
    article["language"] = _detect_language(combined_text)
    article["sentiment"] = _sentiment(combined_text)
    article["keywords"] = _keywords(combined_text)
    return article


# ---------------------------------------------------------------------------
# Source adapters
# ---------------------------------------------------------------------------

def _parse_dt(value: str | None) -> datetime | None:
    """Try several common ISO-8601 / RFC-2822 datetime formats."""
    if not value:
        return None
    formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def fetch_newsapi() -> Generator[dict[str, Any], None, None]:
    """Yield articles from NewsAPI top-headlines (all configured categories/countries)."""
    if not NEWSAPI_KEY:
        log.warning("NEWSAPI_KEY not set — skipping NewsAPI source.")
        return

    base_url = "https://newsapi.org/v2/top-headlines"
    headers = {"X-Api-Key": NEWSAPI_KEY}

    for country in NEWSAPI_COUNTRIES:
        for category in NEWSAPI_CATEGORIES:
            params = {
                "country": country,
                "category": category,
                "pageSize": NEWSAPI_PAGE_SIZE,
            }
            try:
                resp = requests.get(base_url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as exc:
                log.error("NewsAPI request failed for country=%s category=%s: %s", country, category, exc)
                time.sleep(REQUEST_DELAY)
                continue

            articles = data.get("articles", [])
            log.info("NewsAPI [%s-%s] → %d articles", country, category, len(articles))

            for raw in articles:
                url = (raw.get("url") or "").strip()
                if not url or url == "https://removed.com":
                    continue

                source_name = (raw.get("source") or {}).get("name", "NewsAPI")
                yield {
                    "url": url,
                    "url_hash": _url_hash(url),
                    "source": source_name,
                    "source_type": "newsapi",
                    "category": category,
                    "title": (raw.get("title") or "").strip(),
                    "description": (raw.get("description") or "").strip(),
                    "content": (raw.get("content") or "").strip(),
                    "author": (raw.get("author") or "").strip(),
                    "image_url": raw.get("urlToImage"),
                    "published_at": _parse_dt(raw.get("publishedAt")),
                    "fetched_at": datetime.now(timezone.utc),
                }

            time.sleep(REQUEST_DELAY)



def fetch_guardian() -> Generator[dict[str, Any], None, None]:
    """Yield articles from The Guardian Open Platform API."""
    if not GUARDIAN_KEY:
        log.warning("GUARDIAN_KEY not set — skipping Guardian source.")
        return

    base_url = "https://content.guardianapis.com/search"

    for section in GUARDIAN_SECTIONS:
        params = {
            "section": section,
            "api-key": GUARDIAN_KEY,
            "page-size": GUARDIAN_PAGE_SIZE,
            "show-fields": "trailText,bodyText,byline,thumbnail",
            "order-by": "newest",
        }
        try:
            resp = requests.get(base_url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            log.error("Guardian request failed for section=%s: %s", section, exc)
            time.sleep(REQUEST_DELAY)
            continue

        results = data.get("response", {}).get("results", [])
        log.info("Guardian [%s] → %d articles", section, len(results))

        for raw in results:
            url = (raw.get("webUrl") or "").strip()
            if not url:
                continue

            fields = raw.get("fields") or {}
            yield {
                "url": url,
                "url_hash": _url_hash(url),
                "source": "The Guardian",
                "source_type": "guardian",
                "category": section,
                "title": (raw.get("webTitle") or "").strip(),
                "description": (fields.get("trailText") or "").strip(),
                "content": (fields.get("bodyText") or "")[:3000].strip(),
                "author": (fields.get("byline") or "").strip(),
                "image_url": fields.get("thumbnail"),
                "published_at": _parse_dt(raw.get("webPublicationDate")),
                "fetched_at": datetime.now(timezone.utc),
            }

        time.sleep(REQUEST_DELAY)


def fetch_rss() -> Generator[dict[str, Any], None, None]:
    """Yield articles from all configured RSS feeds."""
    for feed_name, feed_url in RSS_FEEDS.items():
        try:
            parsed = feedparser.parse(feed_url)
        except Exception as exc:
            log.error("RSS parse error for %s: %s", feed_name, exc)
            continue

        entries = parsed.get("entries", [])
        log.info("RSS [%s] → %d entries", feed_name, len(entries))

        for entry in entries:
            url = (
                entry.get("link")
                or entry.get("id")
                or ""
            ).strip()
            if not url:
                continue

            # Published date — try multiple keys
            pub_raw = (
                entry.get("published")
                or entry.get("updated")
                or entry.get("created")
            )
            published_at = _parse_dt(pub_raw)

            # Try to get a struct_time fallback
            if published_at is None:
                for key in ("published_parsed", "updated_parsed"):
                    ts = entry.get(key)
                    if ts:
                        try:
                            published_at = datetime(*ts[:6], tzinfo=timezone.utc)
                        except Exception:
                            pass
                        break

            # Description / summary
            summary = ""
            if entry.get("summary"):
                summary = entry["summary"]
            elif entry.get("description"):
                summary = entry["description"]

            # Content
            content = ""
            if entry.get("content"):
                for c in entry["content"]:
                    if c.get("value"):
                        content = c["value"]
                        break

            # Author
            author = (
                entry.get("author")
                or entry.get("dc_creator")
                or ""
            ).strip()

            # Image
            image_url = None
            if entry.get("media_thumbnail"):
                image_url = entry["media_thumbnail"][0].get("url")
            elif entry.get("media_content"):
                image_url = entry["media_content"][0].get("url")

            # Category / tag
            category = ""
            if entry.get("tags"):
                category = entry["tags"][0].get("term", "")

            yield {
                "url": url,
                "url_hash": _url_hash(url),
                "source": feed_name,
                "source_type": "rss",
                "category": category,
                "title": (entry.get("title") or "").strip(),
                "description": summary.strip(),
                "content": content.strip(),
                "author": author,
                "image_url": image_url,
                "published_at": published_at,
                "fetched_at": datetime.now(timezone.utc),
            }

        time.sleep(REQUEST_DELAY)



def fetch_tiingo() -> Generator[dict[str, Any], None, None]:
    if not TIINGO_KEY: return
    url = "https://api.tiingo.com/tiingo/news?limit=10"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Token {TIINGO_KEY}"
    }
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        for item in data:
            url_str = (item.get("url") or "").strip()
            if not url_str: continue
            yield {
                "url": url_str,
                "url_hash": _url_hash(url_str),
                "source": f"Tiingo - {item.get('source')}",
                "source_type": "tiingo",
                "category": "general",
                "title": (item.get("title") or "").strip(),
                "description": (item.get("description") or "").strip(),
                "published_at": _parse_dt(item.get("publishedDate")),
                "fetched_at": datetime.now(timezone.utc),
            }
    except Exception as e:
        log.error(f"Error fetching Tiingo: {e}")



def fetch_marketaux() -> Generator[dict[str, Any], None, None]:
    if not MARKETAUX_KEY: return
    url = f"https://api.marketaux.com/v1/news/all?api_token={MARKETAUX_KEY}&language=en&limit=10"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json().get("data", [])
        for item in data:
            url_str = (item.get("url") or "").strip()
            if not url_str: continue
            yield {
                "url": url_str,
                "url_hash": _url_hash(url_str),
                "source": f"Marketaux - {item.get('source')}",
                "source_type": "marketaux",
                "category": "general",
                "title": (item.get("title") or "").strip(),
                "description": (item.get("description") or "").strip(),
                "published_at": _parse_dt(item.get("published_at")),
                "fetched_at": datetime.now(timezone.utc),
            }
    except Exception as e:
        log.error(f"Error fetching Marketaux: {e}")


def fetch_stock_news_api() -> Generator[dict[str, Any], None, None]:
    if not STOCK_NEWS_KEY: return
    url = f"https://stocknewsapi.com/api/v1/category?section=general&items=10&token={STOCK_NEWS_KEY}"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json().get("data", [])
        for item in data:
            url_str = (item.get("news_url") or "").strip()
            if not url_str: continue
            yield {
                "url": url_str,
                "url_hash": _url_hash(url_str),
                "source": f"StockNewsAPI - {item.get('source_name')}",
                "source_type": "stocknewsapi",
                "category": "general",
                "title": (item.get("title") or "").strip(),
                "description": (item.get("text") or "").strip(),
                "published_at": _parse_dt(item.get("date")),
                "fetched_at": datetime.now(timezone.utc),
            }
    except Exception as e:
        log.error(f"Error fetching Stock News API: {e}")


def fetch_apitube() -> Generator[dict[str, Any], None, None]:
    if not APITUBE_KEY: return
    url = f"https://api.apitube.io/v1/news/everything?api_key={APITUBE_KEY}&limit=10"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json().get("results", [])
        for item in data:
            url_str = (item.get("url") or "").strip()
            if not url_str: continue
            yield {
                "url": url_str,
                "url_hash": _url_hash(url_str),
                "source": f"APITube - {item.get('source', {}).get('name', 'Unknown')}",
                "source_type": "apitube",
                "category": "general",
                "title": (item.get("title") or "").strip(),
                "description": (item.get("summary") or item.get("content") or "").strip(),
                "published_at": _parse_dt(item.get("published_at")),
                "fetched_at": datetime.now(timezone.utc),
            }
    except Exception as e:
        log.error(f"Error fetching APITube: {e}")


def fetch_gnews() -> Generator[dict[str, Any], None, None]:
    if not GNEWS_KEY: return
    url = f"https://gnews.io/api/v4/search?q=stocks&lang=en&max=10&apikey={GNEWS_KEY}"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json().get("articles", [])
        for item in data:
            url_str = (item.get("url") or "").strip()
            if not url_str: continue
            yield {
                "url": url_str,
                "url_hash": _url_hash(url_str),
                "source": f"GNews - {item.get('source', {}).get('name', 'Unknown')}",
                "source_type": "gnews",
                "category": "general",
                "title": (item.get("title") or "").strip(),
                "description": (item.get("description") or item.get("content") or "").strip(),
                "published_at": _parse_dt(item.get("publishedAt")),
                "fetched_at": datetime.now(timezone.utc),
            }
    except Exception as e:
        log.error(f"Error fetching GNews: {e}")


def fetch_finnhub() -> Generator[dict[str, Any], None, None]:
    if not FINNHUB_KEY: return
    url = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_KEY}"
    try:
        response = requests.get(url, headers=CUSTOM_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        for item in data[:10]:
            url_str = (item.get("url") or "").strip()
            if not url_str: continue
            dt = datetime.fromtimestamp(item["datetime"], tz=timezone.utc)
            yield {
                "url": url_str,
                "url_hash": _url_hash(url_str),
                "source": f"Finnhub - {item.get('source', 'General')}",
                "source_type": "finnhub",
                "category": "general",
                "title": (item.get("headline") or "").strip(),
                "description": (item.get("summary") or "").strip(),
                "published_at": dt,
                "fetched_at": datetime.now(timezone.utc),
            }
    except Exception as e:
        log.error(f"Error fetching Finnhub: {e}")


def fetch_sec_edgar() -> Generator[dict[str, Any], None, None]:
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom"
    try:
        # SEC EDGAR strictly requires a specifically formatted User-Agent to avoid 403 Forbidden
        sec_headers = {"User-Agent": "NewsPipelineBot admin@example.com"}
        response = requests.get(url, headers=sec_headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        for entry in feed.entries[:10]:
            url_str = (entry.get("link") or "").strip()
            if not url_str: continue
            yield {
                "url": url_str,
                "url_hash": _url_hash(url_str),
                "source": "SEC EDGAR",
                "source_type": "sec_edgar",
                "category": "regulatory",
                "title": (entry.get("title") or "").strip(),
                "description": (entry.get("summary") or "Corporate Regulatory Filing Submission.").strip(),
                "published_at": _parse_dt(entry.get("updated")) or datetime.now(timezone.utc),
                "fetched_at": datetime.now(timezone.utc),
            }
    except Exception as e:
        log.error(f"Error fetching SEC EDGAR: {e}")


def fetch_reddit_rss() -> Generator[dict[str, Any], None, None]:
    url = "https://www.reddit.com/r/stocks/.rss"
    try:
        response = requests.get(url, headers=CUSTOM_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        for entry in feed.entries[:10]:
            url_str = (entry.get("link") or "").strip()
            if not url_str: continue
            desc = "Social sentiment post."
            content_list = entry.get("content", [])
            if content_list and len(content_list) > 0:
                desc = content_list[0].get("value", desc)
            yield {
                "url": url_str,
                "url_hash": _url_hash(url_str),
                "source": "Reddit /r/stocks",
                "source_type": "reddit_rss",
                "category": "social",
                "title": (entry.get("title") or "").strip(),
                "description": desc.strip(),
                "published_at": _parse_dt(entry.get("updated")) or datetime.now(timezone.utc),
                "fetched_at": datetime.now(timezone.utc),
            }
    except Exception as e:
        log.error(f"Error fetching Reddit RSS: {e}")


def fetch_indianapi() -> Generator[dict[str, Any], None, None]:
    """Yield articles from IndianAPI (dedicated Indian market news)."""
    if not INDIANAPI_KEY:
        return
    url = "https://analyst.indianapi.in/get/market_news"
    headers = {"X-API-Key": INDIANAPI_KEY}
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        # IndianAPI typically returns a list of news items
        for item in data:
            url_str = (item.get("link") or "").strip()
            if not url_str:
                continue
            
            pub_raw = item.get("pubDate") or item.get("date") or item.get("published_at")
            published_at = _parse_dt(pub_raw) if pub_raw else datetime.now(timezone.utc)
            
            yield {
                "url": url_str,
                "url_hash": _url_hash(url_str),
                "source": "IndianAPI",
                "source_type": "indianapi",
                "category": "business",
                "title": (item.get("title") or "").strip(),
                "description": (item.get("description") or "").strip(),
                "published_at": published_at,
                "fetched_at": datetime.now(timezone.utc),
            }
    except Exception as e:
        log.error(f"Error fetching IndianAPI: {e}")


def fetch_newsdata() -> Generator[dict[str, Any], None, None]:
    """Yield business articles filtered for India from NewsData.io."""
    if not NEWSDATA_KEY:
        return
    url = f"https://newsdata.io/api/1/news?apikey={NEWSDATA_KEY}&country=in&category=business"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success":
            results = data.get("results", [])
            for article in results:
                url_str = (article.get("link") or "").strip()
                if not url_str:
                    continue
                
                source_id = article.get("source_id") or "NewsData"
                pub_raw = article.get("pubDate") or article.get("published_at")
                published_at = _parse_dt(pub_raw) if pub_raw else datetime.now(timezone.utc)
                
                yield {
                    "url": url_str,
                    "url_hash": _url_hash(url_str),
                    "source": f"NewsData - {source_id.upper()}",
                    "source_type": "newsdata",
                    "category": "business",
                    "title": (article.get("title") or "").strip(),
                    "description": (article.get("description") or "").strip(),
                    "content": (article.get("content") or "").strip(),
                    "author": (article.get("creator") or [""])[0] if isinstance(article.get("creator"), list) and article.get("creator") else "",
                    "image_url": article.get("image_url"),
                    "published_at": published_at,
                    "fetched_at": datetime.now(timezone.utc),
                }
        else:
            log.warning("NewsData API status was not 'success': %s", data.get("status"))
    except Exception as e:
        log.error(f"Error fetching NewsData.io: {e}")


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------

def get_collection():
    """Return the MongoDB collection, raising on connection failure."""
    if not MONGO_URI:
        log.critical("MONGO_URI is not set in .env — cannot connect to MongoDB.")
        sys.exit(1)

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000, tlsCAFile=certifi.where())
        client.admin.command("ping")  # fail-fast connectivity check
        log.info("Connected to MongoDB cluster.")
    except ConnectionFailure as exc:
        log.critical("MongoDB connection failed: %s", exc)
        sys.exit(1)

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION]

    # Ensure indexes
    collection.create_index("url_hash", unique=True, background=True)
    collection.create_index("published_at", background=True)
    collection.create_index("source", background=True)
    collection.create_index("category", background=True)
    collection.create_index([("title", "text"), ("description", "text"), ("keywords", "text")],
                            background=True)
    log.info("Indexes ensured on collection '%s'.", MONGO_COLLECTION)
    return collection


def upsert_articles(collection, articles: list[dict[str, Any]]) -> tuple[int, int, int]:
    """
    Bulk-upsert articles into MongoDB.
    Returns (inserted, updated, errors).
    """
    if not articles:
        return 0, 0, 0

    ops = [
        UpdateOne(
            {"url_hash": article["url_hash"]},
            {"$set": article, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
        for article in articles
    ]

    try:
        result = collection.bulk_write(ops, ordered=False)
        inserted = result.upserted_count
        updated = result.modified_count
        return inserted, updated, 0
    except BulkWriteError as exc:
        errors = len(exc.details.get("writeErrors", []))
        inserted = exc.details.get("nUpserted", 0)
        updated = exc.details.get("nModified", 0)
        log.warning("Bulk write partial error — %d write errors.", errors)
        return inserted, updated, errors


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(return_data: bool = False, query: str | None = None) -> list[dict[str, Any]] | None:
    """Main entry point: fetch → enrich → upsert."""
    log.info("=" * 60)
    log.info("News Pipeline starting at %s", datetime.now(timezone.utc).isoformat())
    log.info("=" * 60)

    collection = get_collection()

    total_inserted = total_updated = total_errors = 0
    batch: list[dict[str, Any]] = []
    all_fetched: list[dict[str, Any]] = []
    BATCH_SIZE = 50

    def flush_batch() -> None:
        nonlocal total_inserted, total_updated, total_errors, batch, all_fetched
        if not batch:
            return
        if return_data:
            all_fetched.extend(batch)
        ins, upd, err = upsert_articles(collection, batch)
        total_inserted += ins
        total_updated += upd
        total_errors += err
        log.info(
            "Flushed batch of %d — inserted: %d, updated: %d, errors: %d",
            len(batch), ins, upd, err,
        )
        batch = []

    # Chain all sources
    sources = [
        fetch_newsapi(),
        #fetch_guardian(),
        fetch_rss(),
        #fetch_tiingo(),
        fetch_marketaux(),
        #fetch_stock_news_api(),
        fetch_apitube(),
        fetch_gnews(),
        fetch_finnhub(),
        fetch_sec_edgar(),
        fetch_reddit_rss(),
        fetch_indianapi(),
        fetch_newsdata(),
    ]

    for source_gen in sources:
        for raw_article in source_gen:
            try:
                enriched = enrich(raw_article)
                batch.append(enriched)
                if len(batch) >= BATCH_SIZE:
                    flush_batch()
            except Exception as exc:
                log.warning("Enrichment error for %s: %s", raw_article.get("url"), exc)

    flush_batch()  # final flush

    log.info("=" * 60)
    log.info(
        "Pipeline complete — total inserted: %d | updated: %d | errors: %d",
        total_inserted, total_updated, total_errors,
    )
    log.info("=" * 60)

    if return_data:
        if query:
            query_lower = query.lower()
            return [
                a for a in all_fetched
                if query_lower in str(a.get("title", "")).lower()
                or query_lower in str(a.get("description", "")).lower()
                or query_lower in str(a.get("content", "")).lower()
            ]
        return all_fetched


def get_saved_articles(query: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    """Retrieve already fetched articles from MongoDB."""
    collection = get_collection()
    
    # Exclude _id to make it easily JSON serializable
    projection = {"_id": 0}
    
    if query:
        # Use MongoDB text search if possible, else fallback to regex
        # We already have a text index on title, description, keywords
        db_filter = {"$text": {"$search": query}}
        cursor = collection.find(db_filter, projection).sort([("published_at", -1)])
    else:
        cursor = collection.find({}, projection).sort([("published_at", -1)])
        
    if limit is not None and limit > 0:
        cursor = cursor.limit(limit)
        
    articles = list(cursor)
    
    # If text search returned nothing, fallback to regex search for safety
    if query and not articles:
        regex_filter = {
            "$or": [
                {"title": {"$regex": query, "$options": "i"}},
                {"description": {"$regex": query, "$options": "i"}},
                {"content": {"$regex": query, "$options": "i"}},
                {"category": {"$regex": query, "$options": "i"}},
                {"keywords": {"$regex": query, "$options": "i"}}
            ]
        }
        cursor = collection.find(regex_filter, projection).sort([("published_at", -1)])
        if limit is not None and limit > 0:
            cursor = cursor.limit(limit)
        articles = list(cursor)
        
    return articles


if __name__ == "__main__":
    run_pipeline()

    try:
        import clear_old_news
        clear_old_news.run_cleanup(days_old=15)
    except Exception as e:
        log.error(f"Failed to run automatic cleanup: {e}")
