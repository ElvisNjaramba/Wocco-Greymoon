# services/fuzzy_title.py

import re
import hashlib
from difflib import SequenceMatcher

def _normalize_title(title: str) -> str:
    if not title:
        return ""
    t = title.lower().strip()
    t = re.sub(r'[\W_]+', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def _get_trigrams(text: str) -> set:
    """Convert text into character trigrams for similarity comparison."""
    text = f"  {text}  "  # pad for edge trigrams
    return {text[i:i+3] for i in range(len(text) - 2)}

def title_similarity(title_a: str, title_b: str) -> float:
    """
    Returns similarity ratio between 0.0 and 1.0.
    Uses trigram overlap (Jaccard similarity) — fast and effective.
    """
    a = _normalize_title(title_a)
    b = _normalize_title(title_b)

    if not a or not b:
        return 0.0

    # Fast path: exact match
    if a == b:
        return 1.0

    # Fast path: one is substring of the other
    if a in b or b in a:
        return 0.85

    trigrams_a = _get_trigrams(a)
    trigrams_b = _get_trigrams(b)

    if not trigrams_a or not trigrams_b:
        return 0.0

    intersection = len(trigrams_a & trigrams_b)
    union = len(trigrams_a | trigrams_b)

    return intersection / union if union else 0.0

def make_title_bucket_hash(title: str) -> str:
    """
    Creates a coarse 'bucket' hash from the first ~4 significant words.
    Leads in the same bucket are candidates for fuzzy comparison.
    This avoids comparing every new lead against all existing leads.
    """
    normalized = _normalize_title(title)
    words = normalized.split()

    # Remove very common service words that appear in nearly every title
    STOP_WORDS = {
        'cleaning', 'service', 'services', 'professional', 'affordable',
        'cheap', 'best', 'great', 'quality', 'reliable', 'licensed',
        'insured', 'free', 'estimate', 'call', 'now', 'today', 'available',
        'experienced', 'local', 'residential', 'commercial', 'and', 'the',
        'for', 'your', 'with', 'we', 'our', 'all', 'any', 'get', 'need',
        'looking', 'repair', 'installation', 'removal', 'junk', 'house',
        'home', 'yard', 'lawn',
    }

    significant = [w for w in words if w not in STOP_WORDS and len(w) > 2]

    # Use first 3 significant words as the bucket key
    bucket_key = " ".join(significant[:3])

    if not bucket_key:
        # Fall back to first 3 words of anything
        bucket_key = " ".join(words[:3])

    return hashlib.md5(bucket_key.encode()).hexdigest()[:16]