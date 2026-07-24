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

## Deployment to Render (with MongoDB Atlas)

Since this app connects to a cloud-based MongoDB Atlas cluster, deploying it to Render is incredibly straightforward. You don't need to host a separate database on Render; the app will just connect to Atlas.

1. **Push your code to GitHub.**
2. **Log into Render** (https://render.com) and click **New > Web Service**.
3. Connect your GitHub account and select this `News-pipeline` repository.
4. **Configuration**:
   - **Name:** `news-pipeline-api` (or similar)
   - **Root Directory:** `News-pipeline/scripts` (if your app is inside the scripts folder, otherwise leave blank or match your exact repo structure)
   - **Environment:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn app:app --host 0.0.0.0 --port 10000`
5. **Environment Variables**:
   Add all the keys from your `.env` file under "Environment > Add Environment Variable". Crucially, make sure `MONGO_URI` is set to your MongoDB Atlas connection string.
6. Click **Create Web Service**. Your API will now be live on Render!

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
       "...": "..."
    }
  ]
}
```

### 3. Get Saved Articles

**Endpoint:** `GET /api/articles`

Retrieves already-fetched articles directly from the database without querying external APIs. This naturally skips any broken/revoked external APIs and is much faster.

**Query Parameters:**
- `company` (Optional string): Filters the returned articles for a specific company name or ticker.
- `limit` (Optional integer, default: `100`): Maximum number of articles to return.

**Example Response:**
```json
{
  "status": "success",
  "count": 1,
  "data": [
    {
       "title": "Saved Article Example",
       "...": "..."
    }
  ]
}
```

### 4. Cleanup Old News

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

---

## 📬 Newsletter & Email Feature

This feature formats today's fetched news into a responsive HTML newsletter and emails it to users or subscribers.

### SMTP Setup

Configure your SMTP credentials in `.env`:
```env
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password_here
EMAIL_FROM=News Pipeline Digest <your_email@gmail.com>
```
> **Note for Gmail users:** Generate an **App Password** from your Google Account security settings (`Security -> 2-Step Verification -> App Passwords`).

### Newsletter Endpoints

#### 1. Send Digest to Email (On-Demand)
**Endpoint:** `POST /api/newsletter/send`

**Request Body:**
```json
{
  "email": "user@example.com",
  "limit": 10,
  "company": "Apple"
}
```

#### 2. Subscribe to Daily Newsletter
**Endpoint:** `POST /api/newsletter/subscribe`

**Request Body:**
```json
{
  "email": "user@example.com"
}
```

#### 3. Unsubscribe
**Endpoint:** `POST /api/newsletter/unsubscribe`

**Request Body:**
```json
{
  "email": "user@example.com"
}
```

#### 4. List Active Subscribers
**Endpoint:** `GET /api/newsletter/subscribers`

#### 5. Broadcast to All Active Subscribers
**Endpoint:** `POST /api/newsletter/broadcast`

**Request Body (Optional):**
```json
{
  "limit": 10
}
```

### Command Line Interface (CLI)

You can also manage subscribers and send newsletters directly from the command line using `scripts/newsletter.py`:

```bash
cd scripts

# Send today's news digest directly to an email
python newsletter.py --send-to user@example.com --limit 10

# Subscribe an email
python newsletter.py --subscribe user@example.com

# Unsubscribe an email
python newsletter.py --unsubscribe user@example.com

# Broadcast today's news to all active subscribers
python newsletter.py --broadcast
```

