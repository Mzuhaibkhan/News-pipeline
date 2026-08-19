# News Pipeline — Comprehensive Architecture & Improvement Analysis

## Executive Summary

This document provides a deep, critical architectural review of the **News Pipeline** platform. Following the initial security and stability hardening, this analysis outlines concrete strategies, design patterns, and an actionable roadmap to evolve the service into a high-performance, enterprise-grade news intelligence engine.

---

## 1. Architecture Overview: Current vs. Target State

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   CURRENT vs TARGET ARCHITECTURE                                 │
├──────────────────────────────────────────────────┬───────────────────────────────────────────────┤
│ Current State                                    │ Target Architecture (Next-Gen)                │
├──────────────────────────────────────────────────┼───────────────────────────────────────────────┤
│ • Monolithic flat directory in /scripts          │ • Modular domain-driven architecture (src/)   │
│ • Sync requests in ThreadPoolExecutor            │ • Async HTTP (httpx/aiohttp) + Circuit breaker│
│ • Lexicon NLP (TextBlob, RAKE, langdetect)       │ • Financial NLP (FinBERT, GLiNER NER, Embeds) │
│ • Text regex/keyword matching in MongoDB         │ • Hybrid Search (Full-text + Atlas Vector RAG)│
│ • In-process ephemeral BackgroundTasks           │ • Distributed persistent task queue (ARQ/Celery)│
│ • Python f-string inline email templates         │ • MJML + Jinja2 responsive templates          │
│ • Manual script-based database cleanup           │ • Native MongoDB Engine TTL Indexes (Zero-ops)│
└──────────────────────────────────────────────────┴───────────────────────────────────────────────┘
```

---

## 2. Detailed Critical Analysis & Recommendations

### 2.1 Project Structure & Code Organization

#### Critical Observations
1. **Flat `scripts/` Directory**: All logic (`app.py`, `fetch.py`, `db.py`, `newsletter.py`, `clear_old_news.py`, `Dockerfile`) lives in a single folder without separation of concerns. This limits modularity, makes dependency injection difficult, and complicates unit/integration testing.
2. **Broken GitHub Actions Workflow**: `.github/workflows/daily-fetch.yml.md` contains a `.md` extension. GitHub Actions only executes files ending in `.yml` or `.yaml`. Consequently, the scheduled cron pipeline (`0 2 * * *`) **never triggers automatically**.

#### Recommended Clean Architecture (`src/` Layout)
```text
news-pipeline/
├── src/
│   ├── api/                 # FastAPI routes, dependencies, schemas
│   │   ├── v1/
│   │   │   ├── articles.py
│   │   │   ├── fetch.py
│   │   │   └── newsletter.py
│   │   └── router.py
│   ├── core/                # Config (pydantic-settings), security, logging
│   │   ├── config.py
│   │   └── security.py
│   ├── db/                  # PyMongo/Motor singletons, repositories, indexes
│   │   ├── client.py
│   │   └── models.py
│   ├── ingestion/           # Source adapters (NewsAPI, RSS, SEC Edgar, etc.)
│   │   ├── base.py          # Abstract BaseNewsAdapter
│   │   ├── registry.py      # Adapter registry & dynamic loader
│   │   └── adapters/
│   ├── nlp/                 # Sentiment, NER, Vector Embeddings, Language
│   │   ├── sentiment.py
│   │   ├── entities.py
│   │   └── embeddings.py
│   ├── tasks/               # Distributed background jobs (ARQ / Celery)
│   │   ├── worker.py
│   │   └── cron.py
│   └── templates/           # Jinja2 / MJML email templates
├── tests/                   # Pytest suite with mock responses
├── .github/workflows/
│   └── daily-fetch.yml      # (Renamed from .yml.md)
├── Dockerfile               # Root-level multi-stage container
├── pyproject.toml           # Modern package configuration
└── env.example
```

---

### 2.2 Ingestion Engine & Source Adapters

#### Critical Observations
1. **Sync `requests` in `ThreadPoolExecutor`**: Blocking OS threads on external I/O incurs heavy memory and context-switching overhead. As feeds scale from 14 to 100+, thread-based execution degrades rapidly.
2. **Missing Circuit Breaker & Adaptive Rate Limiting**: If an external API returns `429 Too Many Requests` (or exhausts its daily quota), the pipeline continues attempting requests on every run rather than backing off gracefully.
3. **URL-Only Deduplication**: `sha256(url)` fails to catch syndicated wire stories where multiple outlets (e.g., Reuters, AP, Yahoo Finance) republish the exact same article with distinct URLs.
4. **Snippet-Only Ingestion**: Many external APIs return only a truncated 200-character description. Articles lack full-text body content for deep keyword analysis or semantic search.

#### Solution: Standardized Async Adapter Pattern & Content Deduplication
```python
from abc import ABC, abstractmethod
from typing import AsyncGenerator
import httpx

class BaseNewsAdapter(ABC):
    name: str
    source_type: str

    @abstractmethod
    async def fetch(
        self,
        client: httpx.AsyncClient,
        query: str | None = None
    ) -> AsyncGenerator[dict, None]:
        """Fetch raw articles asynchronously."""
        pass
```

* **Circuit Breakers**: Implement per-provider circuit breakers (via Redis flags or `pybreaker`) that trip to `OPEN` for 1 hour when quota limits are reached.
* **Content Hashing (MinHash / SimHash)**: Compute a locality-sensitive hash over normalized titles and summaries to eliminate syndicated duplicates across outlets.
* **Full-Text Article Extraction**: Integrate lightweight extraction libraries (such as `trafilatura` or `newspaper4k`) to fetch full article bodies on-demand when snippets are insufficient.

---

### 2.3 Advanced NLP & AI Intelligence Layer

#### Critical Observations
1. **TextBlob Sentiment Limitations**: TextBlob uses a 2013-era rule-based lexicon. It struggles with financial domain vocabulary (e.g., *"shares plunge on margin compression"* or *"revenue headwinds"*).
2. **RAKE Keyword Noise**: RAKE extracts multi-word noun clusters that often yield unnatural phrases (e.g., `"sample apple news article"`).
3. **`langdetect` Latency**: `langdetect` is non-deterministic and computationally slow across high-throughput loops.

```
┌─────────────────┐     ┌────────────────────────────────────────────────────────┐
│  Raw Article    │ ──> │ NLP Enrichment Pipeline                                │
└─────────────────┘     │  1. Fast Language Filtering (fasttext / py3langid)     │
                        │  2. Named Entity Recognition (GLiNER / spaCy)          │
                        │     → Entities: [ORG: Apple, TICKER: AAPL, PERSON: Cook]│
                        │  3. Financial Sentiment (FinBERT / ONNX quantized)     │
                        │  4. Dense Vector Embedding (all-MiniLM-L6-v2)          │
                        └────────────────────────────────────────────────────────┘
```

#### Upgrades
1. **Quantized FinBERT Sentiment**: Use ONNX Runtime to execute a quantized **FinBERT** model in sub-millisecond latency per headline, delivering domain-accurate financial sentiment (`positive`, `negative`, `neutral` with confidence scores).
2. **Structured Named Entity Recognition (NER)**: Use **GLiNER** or **spaCy** to extract precise financial entities:
   * **Tickers**: `["$AAPL", "$NVDA"]`
   * **Organizations**: `["Apple Inc.", "NVIDIA"]`
   * **Key People**: `["Tim Cook", "Jensen Huang"]`
3. **Dense Vector Embeddings**: Generate 384-dimensional embeddings via `sentence-transformers/all-MiniLM-L6-v2` or `FastEmbed` for semantic search.

---

### 2.4 Database Supercharging (MongoDB)

#### Critical Observations
1. **Manual Cleanup Overhead**: Running a separate Python script (`clear_old_news.py`) with explicit `delete_many` queries adds maintenance complexity and consumes database compute.
2. **Text-Only Search Limitations**: MongoDB `$text` search cannot handle typo tolerance, synonyms, or contextual similarity (e.g., searching for *"EV manufacturing"* will miss articles mentioning only *"Tesla gigafactory"*).

#### Upgrades
* **Native MongoDB TTL Indexes (Zero-Maintenance Cleanup)**:
  Replace the cleanup script with a self-managing engine TTL index on `published_at`:
  ```python
  # Documents automatically expire and delete after 15 days (1,296,000 seconds)
  collection.create_index(
      "published_at",
      expireAfterSeconds=15 * 24 * 60 * 60,
      background=True
  )
  ```
* **Atlas Vector Search (Hybrid RAG Search)**:
  Combine keyword matching with vector cosine distance:
  ```python
  pipeline = [
      {
          "$vectorSearch": {
              "index": "articles_vector_index",
              "path": "embedding",
              "queryVector": query_embedding,
              "numCandidates": 100,
              "limit": 20
          }
      }
  ]
  ```

---

### 2.5 Distributed Task Queues & Scalability

#### Critical Observations
1. **In-Process `BackgroundTasks` Vulnerability**: FastAPI's built-in background tasks run on local worker threads. If the container restarts or Render cycles workers, active ingestion pipelines are abruptly killed without retry capability.
2. **Multi-Worker In-Memory State**: The in-memory lock `_active_fetches = set()` is local to each Gunicorn process. When running 2+ workers without Redis, multiple workers can trigger duplicate fetch jobs simultaneously.

#### Solution: Distributed Task Queue (ARQ / Celery)
* Introduce **ARQ** (lightweight async Redis queue) for background execution:
  * Persistent task execution across worker restarts
  * Built-in exponential retry logic
  * Global distributed locking via Redis `Redlock`
* **Real-Time Streaming**: Implement Server-Sent Events (SSE) via `/api/stream/news` to stream live incoming articles directly to frontend dashboards without polling.

---

### 2.6 Newsletter & Subscriber Experience

#### Critical Observations
1. **Python String Email Templates**: HTML emails are constructed via string concatenation in `newsletter.py`, making responsive design updates and cross-client testing difficult.
2. **Unprotected Unsubscribe Endpoint**: Anyone can unsubscribe arbitrary email addresses by sending a raw email string in POST `/api/newsletter/unsubscribe`.

#### Upgrades
* **MJML + Jinja2 Template System**: Generate responsive, cross-client HTML email digests that render consistently across Apple Mail, Gmail, and Outlook (including dark mode).
* **Cryptographically Signed 1-Click Unsubscribe**:
  ```python
  import itsdangerous
  serializer = itsdangerous.URLSafeTimedSerializer(os.getenv("API_SECRET_KEY"))

  def get_unsubscribe_link(email: str) -> str:
      token = serializer.dumps(email, salt="email-unsubscribe")
      return f"https://api.yourdomain.com/api/newsletter/unsubscribe?token={token}"
  ```
* **Personalized Ticker & Topic Subscriptions**: Allow users to store custom watchlists and preferences (e.g., `"tickers": ["AAPL", "TSLA"]`, `"categories": ["technology"]`).

---

### 2.7 Observability & DevOps

1. **Prometheus Metrics**: Instrument FastAPI with custom telemetry (`/metrics`):
   * `news_articles_ingested_total{source="newsapi", status="success"}`
   * `news_pipeline_duration_seconds`
   * `newsletter_email_delivery_latency`
2. **Automated CI Test Suite with VCR.py**: Mock external API responses using `vcrpy` / `respx` to test ingestion pipelines reliably in GitHub Actions without consuming live API rate limits.
3. **Restore CI/CD Workflow**: Rename `.github/workflows/daily-fetch.yml.md` to `.github/workflows/daily-fetch.yml`.

---

## 3. Prioritized Implementation Roadmap

```
Phase 1: Quick Wins & Cleanup
├── Rename .github/workflows/daily-fetch.yml.md -> .yml
├── Add native MongoDB TTL index on published_at
└── Move Dockerfile to repository root

Phase 2: Modular Architecture & Async Ingestion
├── Reorganize codebase into src/ layout
├── Refactor source fetchers to unified async httpx adapters
└── Implement provider circuit breakers & MinHash deduplication

Phase 3: AI & Financial NLP Enrichment
├── Integrate quantized FinBERT sentiment (ONNX)
├── Implement GLiNER / spaCy Named Entity Recognition (tickers, orgs)
└── Generate dense vector embeddings for semantic search

Phase 4: Distributed Tasks, Scalability & Delivery
├── Migrate background execution to ARQ (Redis task queue)
├── Upgrade newsletter system to MJML templates + signed unsubscribe links
└── Add Prometheus /metrics endpoint and comprehensive test suite
```
