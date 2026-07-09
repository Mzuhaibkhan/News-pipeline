# News Pipeline API

This is a FastAPI-based web service that aggregates news from multiple sources (NewsAPI, The Guardian, Tiingo, Marketaux, RSS feeds, etc.), enriches them with language detection, sentiment analysis (via TextBlob), and keyword extraction (via RAKE), and then upserts the data into a MongoDB Atlas cluster.

## Running the API

1. Ensure your `.env` file is configured with the necessary API keys and MongoDB connection strings (see `env.example`).
2. Install dependencies:
   ```bash
   pip install -r scripts/requirements.txt
   ```
3. Run the application:
   ```bash
   cd scripts
   python app.py
   ```
   Or using Uvicorn directly:
   ```bash
   uvicorn app:app --host 0.0.0.0 --port 10000 --reload
   ```

## Endpoints

### 1. Healthcheck

**Endpoint:** `GET /`

Returns a simple healthcheck status to ensure the service is running.

**Example Response:**
```json
{
  "status": "ok",
  "message": "News Pipeline Service is running"
}
```

### 2. Fetch News

**Endpoint:** `GET /api/fetch` or `POST /api/fetch`

Triggers the fetch pipeline. It fetches articles from all configured sources, enriches them, upserts them to MongoDB, and returns the fetched articles. 

**Query Parameters:**
- `company` (Optional string): Filters the returned fetched articles for a specific company name or ticker (e.g., `Apple` or `AAPL`).

**Example Response:**
```json
{
  "status": "success",
  "fetched_count": 2,
  "data": [
    {
      "url": "https://example.com/news-article",
      "url_hash": "2a34...a8f2",
      "source": "NewsAPI",
      "source_type": "newsapi",
      "category": "business",
      "title": "Sample Apple News Article",
      "description": "A short summary of the news.",
      "content": "Full content of the news article here...",
      "author": "Jane Doe",
      "image_url": "https://example.com/image.png",
      "published_at": "2026-07-09T10:00:00Z",
      "fetched_at": "2026-07-09T12:00:00Z",
      "language": "en",
      "sentiment": {
        "polarity": 0.25,
        "subjectivity": 0.4
      },
      "keywords": [
        "apple news",
        "sample"
      ]
    },
    {
       "...": "..."
    }
  ]
}
```

### 3. Cleanup Old News

**Endpoint:** `POST /api/cleanup`

Triggers a cleanup operation to delete old articles from the MongoDB database.

**Query Parameters:**
- `days_old` (Optional integer, default: `15`): The number of days old an article must be before it is deleted.

**Example Response:**
```json
{
  "status": "success",
  "message": "Cleared articles older than 15 days"
}
```
