"""
deduplication.py — Content-Based Duplicate Detection

Generates lightweight 64-bit content fingerprints (SimHash) from normalized
article text to detect syndicated duplicate news stories across different
publishers (e.g., the same Reuters wire story republished on Yahoo Finance,
MarketWatch, and CNBC under different URLs).

This complements URL-based deduplication (`url_hash`) by catching content
duplicates that have distinct URLs.

Usage:
    from deduplication import content_hash, is_near_duplicate

    hash_a = content_hash("Apple reports record Q3 earnings")
    hash_b = content_hash("Apple Reports Record Q3 Earnings Today")
    print(is_near_duplicate(hash_a, hash_b))  # True (hamming distance <= 3)
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Optional


# ---------------------------------------------------------------------------
# Text Normalization
# ---------------------------------------------------------------------------

# Common stopwords to remove for fingerprinting (kept minimal for speed)
_STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "it", "as", "are", "was", "were",
    "be", "been", "being", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "shall", "can",
    "this", "that", "these", "those", "not", "no", "nor", "so", "if",
    "than", "too", "very", "just", "about", "also", "into", "over",
    "after", "before", "between", "through", "during", "up", "down",
    "out", "off", "then", "once", "here", "there", "when", "where",
    "why", "how", "all", "each", "every", "both", "few", "more", "most",
    "other", "some", "such", "only", "own", "same", "its", "he", "she",
    "they", "we", "you", "i", "me", "him", "her", "us", "them", "my",
    "your", "his", "our", "their", "what", "which", "who", "whom",
})


def _normalize(text: str) -> list[str]:
    """Normalize text to lowercase tokens, remove punctuation and stopwords."""
    # Unicode normalize and lowercase
    text = unicodedata.normalize("NFKD", text.lower())
    # Remove non-alphanumeric except spaces
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    # Split and filter
    tokens = [w for w in text.split() if w and w not in _STOP_WORDS and len(w) > 1]
    return tokens


# ---------------------------------------------------------------------------
# SimHash Implementation (64-bit)
# ---------------------------------------------------------------------------

def _token_hash(token: str) -> int:
    """Generate a 64-bit hash for a single token using MD5 (fast, non-crypto)."""
    digest = hashlib.md5(token.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big")


def simhash(tokens: list[str], hashbits: int = 64) -> int:
    """
    Compute a 64-bit SimHash fingerprint from a list of tokens.

    SimHash is a locality-sensitive hash: similar documents produce
    similar hashes (small Hamming distance), enabling O(1) duplicate
    detection without pairwise comparison.
    """
    if not tokens:
        return 0

    v = [0] * hashbits
    for token in tokens:
        h = _token_hash(token)
        for i in range(hashbits):
            bitmask = 1 << (hashbits - 1 - i)
            if h & bitmask:
                v[i] += 1
            else:
                v[i] -= 1

    fingerprint = 0
    for i in range(hashbits):
        if v[i] > 0:
            fingerprint |= 1 << (hashbits - 1 - i)
    return fingerprint


def hamming_distance(hash_a: int, hash_b: int) -> int:
    """Count the number of differing bits between two hashes."""
    return bin(hash_a ^ hash_b).count("1")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def content_hash(text: str) -> str:
    """
    Generate a hex content fingerprint for an article's combined text.

    Returns a 16-character hex string (64-bit SimHash).
    Use this value to detect near-duplicate articles across different sources.
    """
    tokens = _normalize(text)
    if not tokens:
        return "0" * 16
    h = simhash(tokens)
    return f"{h:016x}"


def is_near_duplicate(
    hash_a: str,
    hash_b: str,
    threshold: int = 3,
) -> bool:
    """
    Check if two content hashes represent near-duplicate articles.

    Args:
        hash_a: Hex content hash of article A.
        hash_b: Hex content hash of article B.
        threshold: Maximum Hamming distance to consider as duplicate (default: 3).
                   A threshold of 3 means up to 3 out of 64 bits can differ.

    Returns:
        True if the articles are near-duplicates.
    """
    try:
        int_a = int(hash_a, 16)
        int_b = int(hash_b, 16)
    except (ValueError, TypeError):
        return False
    return hamming_distance(int_a, int_b) <= threshold


def deduplicate_batch(
    articles: list[dict],
    text_field: str = "_combined_text",
    threshold: int = 3,
) -> list[dict]:
    """
    Remove near-duplicate articles from a batch using SimHash fingerprints.

    Each article gets a 'content_hash' field. If two articles have a Hamming
    distance <= threshold, only the first one (by order) is kept.

    Args:
        articles: List of article dicts (must have title/description).
        text_field: Internal key used for combined text (not stored).
        threshold: Hamming distance threshold for dedup.

    Returns:
        Deduplicated list with 'content_hash' field added to each article.
    """
    seen_hashes: list[int] = []
    unique: list[dict] = []

    for article in articles:
        combined = " ".join(filter(None, [
            article.get("title", ""),
            article.get("description", ""),
        ]))
        h_str = content_hash(combined)
        h_int = int(h_str, 16)

        # Check against all seen hashes
        is_dup = False
        for seen in seen_hashes:
            if hamming_distance(h_int, seen) <= threshold:
                is_dup = True
                break

        if not is_dup:
            article["content_hash"] = h_str
            seen_hashes.append(h_int)
            unique.append(article)

    return unique
