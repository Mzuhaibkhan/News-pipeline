# Cloudflare Integration & Bandwidth Optimization Guide for Render

This guide explains how to connect **Cloudflare CDN & Web Application Firewall (WAF)** in front of your Render deployment to slash bandwidth usage, cache API responses at edge servers worldwide, and protect your service from malicious bots.

---

## 1. Domain Setup & DNS Configuration

1. Log into your [Cloudflare Dashboard](https://dash.cloudflare.com/) and add your custom domain.
2. Go to **DNS** $\rightarrow$ **Records** and add a `CNAME` record:
   - **Type**: `CNAME`
   - **Name**: `api` (or `@` / your preferred subdomain)
   - **Target**: `<your-app-name>.onrender.com` (your Render service URL)
   - **Proxy status**: **Proxied** (Orange Cloud icon enabled)

3. In **Render Dashboard** $\rightarrow$ **Settings** $\rightarrow$ **Custom Domains**:
   - Add your custom domain (e.g. `api.yourdomain.com`).
   - Render will verify the Cloudflare CNAME record and issue a managed TLS certificate.

---

## 2. SSL/TLS Encryption Settings

In Cloudflare Dashboard:
1. Navigate to **SSL/TLS** $\rightarrow$ **Overview**.
2. Set the encryption mode to **Full (Strict)**.
   * This ensures encrypted HTTPS communication both between the client and Cloudflare, and between Cloudflare and Render.

---

## 3. Configure Cloudflare Edge Cache Rules (Slash Render Bandwidth)

By default, Cloudflare does not cache JSON API responses. Creating a Cache Rule caches your public news endpoints at Cloudflare's 300+ global edge locations, serving read traffic without hitting your Render server at all!

1. In Cloudflare Dashboard, go to **Caching** $\rightarrow$ **Cache Rules**.
2. Click **Create rule**:
   - **Rule Name**: `Cache News API Endpoints`
   - **If incoming requests match**:
     - Expression: `(http.request.uri.path wildcard "/api/articles*")`
   - **Cache status**: **Eligible for cache**
   - **Edge TTL**: **Use Cache-Control header if present, otherwise 5 minutes**
3. Save and deploy.

> **Effect:** When users or bots request `/api/articles`, Cloudflare serves the response directly from edge cache. Render bandwidth consumption drops by up to **90%+**.

---

## 4. Bot Management & WAF Protection

Cloudflare automatically filters out bad bots, vulnerability scanners, and scraper scripts that drain server bandwidth.

1. Go to **Security** $\rightarrow$ **Bots**.
2. Turn ON **Bot Fight Mode** (or **Super Bot Fight Mode** if on Pro plan).
3. Go to **Security** $\rightarrow$ **WAF** $\rightarrow$ **Rate Limiting Rules**:
   - Create a rule to challenge or block IPs making more than 100 requests per minute to `/api/*`.

---

## 5. Render Health Check Path Configuration

To prevent Render internal checks from consuming bandwidth:
1. Go to **Render Dashboard** $\rightarrow$ **Your Service** $\rightarrow$ **Settings** $\rightarrow$ **Health Check Path**.
2. Set it to:
   ```text
   /health/ready
   ```
   *(Returns `{"status": "ready"}` (~20 bytes)).*

---

## 6. How Real Client IP Detection Works in Code

Your FastAPI application automatically detects real client IPs behind Cloudflare:
- Cloudflare appends the original visitor IP in the `CF-Connecting-IP` header.
- [`scripts/app.py`](file:///c:/Github/News-pipeline/News-pipeline/scripts/app.py#L200) reads `CF-Connecting-IP` for rate-limiting, so rate limits apply per visitor rather than blocking Cloudflare proxy IPs.
