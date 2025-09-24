#!/usr/bin/env python3
"""
Fuzzy VIN Matcher - Robust driver name extraction and VIN suggestion for Telegram groups
"""

import re
import logging
from typing import List, Tuple, Set
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """
    Normalize a name for fuzzy matching
    - Lowercase
    - Strip punctuation except hyphens and apostrophes
    - Collapse multiple whitespace
    """
    # Keep only letters, spaces, hyphens, and apostrophes
    normalized = re.sub(r"[^\w\s\-']", " ", name, flags=re.UNICODE)
    # Collapse multiple spaces
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.lower().strip()


def extract_names_from_title(title: str) -> List[str]:
    """
    Extract probable driver names from messy group titles

    Examples:
    >>> extract_names_from_title("198 - S* - Abdul Rashid Bigzad - (C) - Truck_041. Phone: (770) 912-5431")
    ['Abdul Rashid Bigzad']

    >>> extract_names_from_title("090 - N* - Sherzod Pirmetov / Jamoliddin Sodikov - (O) - Truck_7062. Phone: ...")
    ['Sherzod Pirmetov', 'Jamoliddin Sodikov']

    >>> extract_names_from_title("111 - B* - Rafael Suarez / Gretzin Sanchez - (C) - Truck_3834. Phone: ...")
    ['Rafael Suarez', 'Gretzin Sanchez']
    """
    if not title:
        return []

    # Remove phone numbers first (they mess up parsing)
    title = re.sub(r"Phone:\s*[\(\d\)\-\s]+", "", title, flags=re.IGNORECASE)

    # Remove truck identifiers
    title = re.sub(r"Truck_\w+", "", title, flags=re.IGNORECASE)

    # Split on various delimiters but preserve the parts
    # First split on major separators: -, /, (, ), Phone:
    parts = re.split(r"\s*[-/\(\)]\s*", title)

    names = []
    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Skip parts that look like codes, numbers, or single letters
        if (len(part) <= 3 or
            re.match(r"^\d+$", part) or  # Just numbers
            # Single letter codes like "S*", "B*"
            re.match(r"^[A-Z]\*?$", part) or
            re.match(r"^\([A-Z]\)$", part) or  # Status codes like "(C)", "(O)"
                part.lower() in ["phone", "truck"]):
            continue

        # Check if part looks like a name (at least 2 words with letters)
        words = part.split()
        valid_words = []

        for word in words:
            # Skip short words unless they're common name parts
            if len(word) < 2 and word.lower() not in ["de", "da", "le", "la"]:
                continue
            # Must contain at least some letters
            if not re.search(r"[a-zA-Z]", word):
                continue
            # Skip obvious non-name tokens
            if (word.lower() in ["truck", "phone"] or
                re.match(r"^\d+$", word) or
                    word.startswith("Truck_")):
                continue
            valid_words.append(word)

        # Must have at least 2 valid words to be considered a name
        if len(valid_words) >= 2:
            name = " ".join(valid_words)
            # Final cleanup and validation
            if len(name) >= 4 and not re.match(r"^\d+", name):
                names.append(name)

    # Dedupe while preserving order
    seen = set()
    unique_names = []
    for name in names:
        normalized = normalize_name(name)
        if normalized not in seen and len(normalized.split()) >= 2:
            seen.add(normalized)
            unique_names.append(name)

    return unique_names


def build_assets_index(
        rows: List[List[str]], driver_col: int, vin_col: int) -> List[Tuple[str, str]]:
    """
    Build (driver_name, vin) pairs from assets sheet data

    Args:
        rows: Raw sheet data including headers
        driver_col: Column index for driver names (typically 3 for column D)
        vin_col: Column index for VINs (typically 4 for column E)

    Returns:
        List of (driver_name, vin) tuples, filtered and normalized
    """
    assets = []

    # Skip header row
    data_rows = rows[1:] if len(rows) > 1 else []

    for row_idx, row in enumerate(data_rows):
        try:
            # Ensure row has enough columns
            if len(row) <= max(driver_col, vin_col):
                continue

            driver_name = str(
                row[driver_col]).strip() if row[driver_col] else ""
            vin = str(row[vin_col]).strip().upper() if row[vin_col] else ""

            # Validate driver name (must have at least 2 words and 4 chars)
            if not driver_name or len(driver_name) < 4:
                continue
            if len(driver_name.split()) < 2:
                continue

            # Validate VIN (must be exactly 17 alphanumeric characters)
            if not vin or len(vin) != 17 or not re.match(
                    r"^[A-Z0-9]{17}$", vin):
                continue

            assets.append((driver_name, vin))

        except (IndexError, ValueError, AttributeError) as e:
            logger.debug(f"Skipping assets row {row_idx}: {e}")
            continue

    logger.debug(
        f"Built assets index with {len(assets)} valid driver/VIN pairs")
    return assets


def top_matches_for_name(
        name: str, assets: List[Tuple[str, str]], k: int = 5) -> List[Tuple[str, str, int]]:
    """
    Find top fuzzy matches for a name against assets driver names

    Returns:
        List of (driver_name, vin, score) tuples, sorted by score descending

    Examples:
    >>> assets = [("Rafael Suarez", "1XKWD49X5NR476547"), ("Gretzin Sanchez", "3AKJHHDR3KSHU6562")]
    >>> matches = top_matches_for_name("Rafael", assets, k=2)
    >>> matches[0][2] > 80  # Should have high score
    True
    """
    if not name or not assets:
        return []

    normalized_query = normalize_name(name)
    if not normalized_query:
        return []

    scored_matches = []

    for driver_name, vin in assets:
        normalized_driver = normalize_name(driver_name)

        # Use token_set_ratio for better partial matching
        score = fuzz.token_set_ratio(normalized_query, normalized_driver)

        if score > 0:  # Include all non-zero scores for ranking
            scored_matches.append((driver_name, vin, score))

    # Sort by score descending and return top k
    scored_matches.sort(key=lambda x: x[2], reverse=True)
    return scored_matches[:k]


def shortlist_for_group_title(
        title: str, assets: List[Tuple[str, str]], k_each: int = 3) -> List[Tuple[str, str, int]]:
    """
    Generate a shortlist of VIN candidates for a group title

    Args:
        title: Group title to parse
        assets: List of (driver_name, vin) pairs
        k_each: Max matches per extracted name

    Returns:
        Deduplicated list of (driver_name, vin, score) sorted by score desc, max 6-8 items
    """
    extracted_names = extract_names_from_title(title)
    logger.info(f"Extracted names from '{title}': {extracted_names}")

    if not extracted_names:
        logger.warning(f"No names extracted from group title: {title}")
        return []

    all_matches = []
    seen_vins = set()

    for name in extracted_names:
        matches = top_matches_for_name(name, assets, k_each)
        logger.debug(
            f"Top matches for '{name}': {[(d, v, s) for d, v, s in matches[:3]]}")

        for driver_name, vin, score in matches:
            # Dedupe by VIN while preserving highest score
            if vin not in seen_vins:
                seen_vins.add(vin)
                all_matches.append((driver_name, vin, score))

    # Sort by score descending and limit to top 8
    all_matches.sort(key=lambda x: x[2], reverse=True)
    shortlist = all_matches[:8]

    logger.info(
        f"Generated shortlist with {len(shortlist)} candidates, top score: {shortlist[0][2] if shortlist else 0}")
    return shortlist


def redact_phone(text: str) -> str:
    """Redact middle digits of phone numbers for logging"""
    return re.sub(r"(\(\d{3}\)\s*)\d{3}(\-\d{4})", r"\1XXX\2", text)


# Test examples for validation
if __name__ == "__main__":
    # Test name extraction
    test_titles = [
        "198 - S* - Abdul Rashid Bigzad - (C) - Truck_041. Phone: (770) 912-5431",
        "090 - N* - Sherzod Pirmetov / Jamoliddin Sodikov - (O) - Truck_7062. Phone: ...",
        "111 - B* - Rafael Suarez / Gretzin Sanchez - (C) - Truck_3834. Phone: ...",
        "Just Some Random Text Without Names",
        "123 - Numbers Only - 456",
        "Carlos Martinez - Driver",
        ""]

    print("Testing name extraction:")
    for title in test_titles:
        names = extract_names_from_title(title)
        print(f"'{title}' -> {names}")

    # Test matching
    test_assets = [
        ("Rafael Suarez", "1XKWD49X5NR476547"),
        ("Gretzin Sanchez", "3AKJHHDR3KSHU6562"),
        ("Abdul Rashid Bigzad", "3AKJHHFG3SSVR2041"),
        ("Sherzod Pirmetov", "4V4NC9EH6GN946170"),
        ("Carlos Martinez", "3AKJGLDR3HSJJ9954")
    ]

    print("\nTesting fuzzy matching:")
    test_queries = ["Rafael", "Gretzin", "Abdul Rashid", "Martinez"]
    for query in test_queries:
        matches = top_matches_for_name(query, test_assets, k=3)
        print(f"'{query}' -> {[(d, v[-4:], s) for d, v, s in matches]}")
