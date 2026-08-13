"""
entities.py — Named Entity & Ticker Extraction

Extracts structured financial metadata from article text:
- Stock tickers (e.g., $AAPL, TSLA, RELIANCE.NS)
- Organization names (e.g., Apple Inc., Federal Reserve)
- Key financial events (e.g., earnings, merger, IPO, dividend)
- Sector classification hints

Uses lightweight regex-based extraction (zero ML dependencies) for
production-grade speed. Designed as a drop-in upgrade path: replace
regex extractors with spaCy/GLiNER models when compute budget allows.
"""

from __future__ import annotations

import re
from typing import Any


# ---------------------------------------------------------------------------
# US Stock Ticker Detection
# ---------------------------------------------------------------------------

# Pattern: $AAPL, $TSLA, or standalone 1-5 letter uppercase words that look
# like tickers when surrounded by financial context.
_CASHTAG_RE = re.compile(r"\$([A-Z]{1,5})\b")

# Standalone uppercase tokens (2-5 chars) that might be tickers.
# Only matched when accompanied by financial context keywords.
_BARE_TICKER_RE = re.compile(r"\b([A-Z]{2,5})\b")

# Indian stock exchange tickers (e.g., RELIANCE.NS, TCS.BO)
_INDIAN_TICKER_RE = re.compile(r"\b([A-Z]{2,20})\.(NS|BO|BSE|NSE)\b")

# Words that look like tickers but aren't (common false positives)
_TICKER_BLACKLIST = frozenset({
    "CEO", "CFO", "CTO", "COO", "IPO", "ETF", "SEC", "GDP", "FBI", "CIA",
    "NASA", "NYSE", "USA", "UK", "EU", "UN", "WHO", "NATO", "IMF", "BBC",
    "CNN", "NBC", "CBS", "ABC", "FOX", "AP", "AI", "IT", "HR", "PR",
    "VP", "MD", "PhD", "AM", "PM", "EST", "PST", "UTC", "GMT", "IST",
    "USD", "EUR", "GBP", "JPY", "INR", "BTC", "ETH", "NFT", "API",
    "RSS", "URL", "HTML", "CSS", "SQL", "AWS", "GCP", "NEW", "THE",
    "AND", "FOR", "ARE", "NOT", "HAS", "HAD", "WAS", "ALL", "CAN",
    "HER", "HIS", "HOW", "ITS", "MAY", "OLD", "OUR", "OWN", "SAY",
    "SHE", "TOO", "USE", "WAY", "WHO", "DID", "GET", "HIM", "LET",
    "RAN", "SAT", "SET", "TOP", "TRY", "WIN", "WON", "BIG", "END",
    "FAR", "FEW", "GOT", "MAN", "PUT", "RUN", "TWO", "AGO", "DAY",
    "ANY", "ADD", "AGE", "AID", "AIM", "AIR", "ARM", "ART", "ASK",
    "BAD", "BED", "BIT", "BOX", "BOY", "BUS", "BUY", "CUT", "DOG",
    "EAR", "EAT", "EYE", "FIT", "FLY", "GAS", "GUN", "GUY", "HIT",
    "HOT", "ICE", "ILL", "JOB", "KEY", "KID", "LAW", "LAY", "LED",
    "LEG", "LIE", "LOT", "LOW", "MAP", "MEN", "MIX", "NET", "NOR",
    "OIL", "PAY", "RED", "RID", "ROW", "SEA", "SIT", "SIX", "SKY",
    "SON", "TAX", "TEN", "TIE", "WAR", "WET", "YES", "YET",
})

# Financial context keywords — if these appear in the text, standalone
# uppercase words are more likely to be tickers.
_FINANCIAL_CONTEXT = frozenset({
    "stock", "stocks", "share", "shares", "trading", "traded", "trade",
    "market", "markets", "investor", "investors", "earnings", "revenue",
    "profit", "loss", "quarter", "quarterly", "annual", "dividend",
    "ipo", "merger", "acquisition", "bull", "bear", "rally", "crash",
    "portfolio", "fund", "index", "nasdaq", "dow", "s&p", "sensex",
    "nifty", "bse", "nse", "wall street", "equity", "bond", "yield",
    "valuation", "capitalization", "cap", "sector", "analyst", "upgrade",
    "downgrade", "target", "forecast", "guidance", "buyback", "split",
    "listing", "delisting", "sec filing", "10-k", "10-q", "hedge",
})


def extract_tickers(text: str) -> list[str]:
    """
    Extract stock ticker symbols from article text.

    Strategy:
    1. Always extract explicit cashtags ($AAPL, $TSLA).
    2. Extract Indian exchange tickers (RELIANCE.NS, TCS.BO).
    3. If financial context keywords are present, also extract
       bare uppercase tokens that look like tickers.

    Returns a deduplicated, sorted list of ticker symbols.
    """
    tickers: set[str] = set()

    # 1. Explicit cashtags (highest confidence)
    for match in _CASHTAG_RE.finditer(text):
        ticker = match.group(1)
        if ticker not in _TICKER_BLACKLIST:
            tickers.add(ticker)

    # 2. Indian exchange tickers
    for match in _INDIAN_TICKER_RE.finditer(text):
        tickers.add(f"{match.group(1)}.{match.group(2)}")

    # 3. Bare tickers only if financial context is present
    text_lower = text.lower()
    has_financial_context = any(kw in text_lower for kw in _FINANCIAL_CONTEXT)

    if has_financial_context:
        for match in _BARE_TICKER_RE.finditer(text):
            ticker = match.group(1)
            if (
                ticker not in _TICKER_BLACKLIST
                and len(ticker) >= 2
                and len(ticker) <= 5
            ):
                tickers.add(ticker)

    return sorted(tickers)


# ---------------------------------------------------------------------------
# Organization Name Extraction
# ---------------------------------------------------------------------------

# Common organization suffixes that indicate a company name
_ORG_SUFFIXES = (
    "Inc.", "Inc", "Corp.", "Corp", "Corporation", "Ltd.", "Ltd",
    "Limited", "LLC", "LLP", "PLC", "Plc", "Group", "Holdings",
    "Technologies", "Technology", "Therapeutics", "Pharmaceuticals",
    "Semiconductor", "Semiconductors", "Systems", "Solutions",
    "Partners", "Capital", "Ventures", "Entertainment",
    "Communications", "Industries", "International", "Global",
    "Financial", "Bancorp", "Bank", "Motors", "Airlines",
    "Energy", "Petroleum", "Resources", "Services",
)

_ORG_PATTERN = re.compile(
    r"\b([A-Z][a-zA-Z&\-']+(?:\s+[A-Z][a-zA-Z&\-']+)*\s+"
    + "|".join(re.escape(s) for s in _ORG_SUFFIXES)
    + r")\b"
)

# Well-known organizations for direct matching
_KNOWN_ORGS = {
    "Federal Reserve", "Fed", "European Central Bank", "ECB",
    "Bank of England", "Bank of Japan", "Reserve Bank of India", "RBI",
    "Securities and Exchange Commission", "SEC",
    "World Bank", "International Monetary Fund", "IMF",
    "Wall Street", "Silicon Valley",
    "Apple", "Google", "Microsoft", "Amazon", "Meta", "Tesla",
    "NVIDIA", "Netflix", "Alphabet", "Samsung", "Intel", "AMD",
    "Qualcomm", "Broadcom", "Oracle", "Salesforce", "Adobe",
    "PayPal", "Visa", "Mastercard", "JPMorgan", "Goldman Sachs",
    "Morgan Stanley", "Berkshire Hathaway", "BlackRock",
    "Tata", "Reliance", "Infosys", "Wipro", "HCL", "TCS",
    "Adani", "HDFC", "ICICI", "Bajaj", "Mahindra",
}


def extract_organizations(text: str) -> list[str]:
    """Extract organization/company names from text."""
    orgs: set[str] = set()

    # Match known organizations
    for org in _KNOWN_ORGS:
        if org in text:
            orgs.add(org)

    # Match pattern-based organizations (e.g., "Apple Inc.", "Tesla Corp.")
    for match in _ORG_PATTERN.finditer(text):
        org = match.group(0).strip()
        if len(org) > 3:  # Skip very short matches
            orgs.add(org)

    return sorted(orgs)


# ---------------------------------------------------------------------------
# Financial Event Detection
# ---------------------------------------------------------------------------

_EVENT_PATTERNS: dict[str, re.Pattern] = {
    "earnings": re.compile(
        r"\b(earnings|quarterly results|profit report|revenue report|"
        r"financial results|beat expectations|missed estimates)\b", re.I
    ),
    "merger_acquisition": re.compile(
        r"\b(merger|acquisition|acquire[ds]?|takeover|buyout|"
        r"deal|purchase[ds]?|bid for)\b", re.I
    ),
    "ipo": re.compile(
        r"\b(ipo|initial public offering|public listing|"
        r"goes public|went public|direct listing|spac)\b", re.I
    ),
    "dividend": re.compile(
        r"\b(dividend|payout|distribution|yield|ex-dividend)\b", re.I
    ),
    "stock_split": re.compile(
        r"\b(stock split|share split|reverse split)\b", re.I
    ),
    "layoffs": re.compile(
        r"\b(layoff|layoffs|laid off|job cuts|workforce reduction|"
        r"downsizing|restructuring|let go)\b", re.I
    ),
    "regulatory": re.compile(
        r"\b(sec filing|regulatory|regulation|compliance|"
        r"antitrust|investigation|lawsuit|fine[ds]?|penalty)\b", re.I
    ),
    "product_launch": re.compile(
        r"\b(launch|launches|launched|unveiled|announces|"
        r"new product|release[ds]?|rollout)\b", re.I
    ),
    "partnership": re.compile(
        r"\b(partnership|collaboration|joint venture|"
        r"strategic alliance|teamed up|partnered)\b", re.I
    ),
}


def extract_events(text: str) -> list[str]:
    """Detect financial event types mentioned in the article text."""
    events = []
    for event_type, pattern in _EVENT_PATTERNS.items():
        if pattern.search(text):
            events.append(event_type)
    return events


# ---------------------------------------------------------------------------
# Sector Classification (Keyword-Based)
# ---------------------------------------------------------------------------

_SECTOR_KEYWORDS: dict[str, list[str]] = {
    "technology": ["software", "hardware", "ai", "artificial intelligence",
                    "cloud", "saas", "semiconductor", "chip", "data center",
                    "cybersecurity", "tech", "computing", "digital"],
    "finance": ["bank", "banking", "fintech", "insurance", "lending",
                "credit", "payment", "wealth management", "hedge fund"],
    "healthcare": ["pharma", "pharmaceutical", "biotech", "drug", "fda",
                    "clinical trial", "vaccine", "hospital", "health"],
    "energy": ["oil", "gas", "petroleum", "renewable", "solar", "wind",
               "nuclear", "ev", "electric vehicle", "battery", "lithium"],
    "consumer": ["retail", "e-commerce", "consumer", "brand", "fashion",
                 "food", "beverage", "restaurant", "hospitality"],
    "automotive": ["automobile", "automotive", "car", "vehicle", "ev",
                   "self-driving", "autonomous", "electric car"],
    "real_estate": ["real estate", "property", "reit", "housing",
                    "mortgage", "commercial property", "construction"],
    "media": ["media", "streaming", "entertainment", "gaming", "content",
              "advertising", "social media", "metaverse"],
}


def classify_sectors(text: str) -> list[str]:
    """Classify an article into one or more financial sectors."""
    text_lower = text.lower()
    sectors = []
    for sector, keywords in _SECTOR_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            sectors.append(sector)
    return sectors


# ---------------------------------------------------------------------------
# Master Entity Extraction Function
# ---------------------------------------------------------------------------

def extract_entities(text: str) -> dict[str, Any]:
    """
    Run all entity extraction passes on article text and return
    a structured metadata dictionary.

    Returns:
        {
            "tickers": ["AAPL", "TSLA"],
            "organizations": ["Apple", "Tesla"],
            "events": ["earnings", "product_launch"],
            "sectors": ["technology", "automotive"],
        }
    """
    return {
        "tickers": extract_tickers(text),
        "organizations": extract_organizations(text),
        "events": extract_events(text),
        "sectors": classify_sectors(text),
    }
