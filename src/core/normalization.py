import re
from functools import lru_cache
from typing import Any, Dict

from ..config import CANONICAL_PRODUCT_KEYS, CATEGORY_LABELS, PRODUCT_TYPE_ALIASES


_RE_SLASH = re.compile(r"\s*/\s*")
_RE_HYPHEN = re.compile(r"\s*-\s*")
_RE_WS = re.compile(r"\s+")
_RE_CSV = re.compile(r"\s*,\s*")
_RE_LPAREN = re.compile(r"\s*\(\s*")
_RE_RPAREN = re.compile(r"\s*\)\s*")
_RE_PAREN_SP = re.compile(r"(?<!\s)\(")

_CATEGORY_TEXT_ALIASES = {
    "healthcare": "Health",
    "health care": "Health",
    "data centre": "Datacentre",
    "data center": "Datacentre",
}


def normalize_text_key(value: Any) -> str:
    if value is None:
        return ""

    normalized = str(value).strip().lower()
    if not normalized:
        return ""

    normalized = _RE_SLASH.sub("/", normalized)
    normalized = _RE_HYPHEN.sub("-", normalized)
    normalized = _RE_LPAREN.sub("(", normalized)
    normalized = _RE_RPAREN.sub(")", normalized)
    normalized = _RE_PAREN_SP.sub(" (", normalized)
    normalized = _RE_WS.sub(" ", normalized)
    return normalized


@lru_cache(maxsize=1)
def get_product_alias_map() -> Dict[str, str]:
    alias_map: Dict[str, str] = {}
    for raw_key, raw_value in PRODUCT_TYPE_ALIASES.items():
        normalized_key = normalize_text_key(raw_key)
        normalized_value = normalize_text_key(raw_value)
        if not normalized_key or normalized_value not in CANONICAL_PRODUCT_KEYS:
            continue
        alias_map[normalized_key] = normalized_value
    return alias_map


@lru_cache(maxsize=1)
def get_category_alias_map() -> Dict[str, str]:
    alias_map: Dict[str, str] = {}
    valid_labels = set(CATEGORY_LABELS.values())

    for label in valid_labels:
        normalized = normalize_text_key(label)
        if normalized:
            alias_map[normalized] = label

    for raw_key, canonical in _CATEGORY_TEXT_ALIASES.items():
        if canonical not in valid_labels:
            continue
        normalized = normalize_text_key(raw_key)
        if normalized:
            alias_map[normalized] = canonical

    return alias_map


def compute_product_key(product_type_raw: Any) -> str:
    if product_type_raw is None:
        return ""

    raw = str(product_type_raw).strip()
    if not raw:
        return ""

    alias_map = get_product_alias_map()
    for part in _RE_CSV.split(raw):
        if not part:
            continue
        token = normalize_text_key(part)
        if not token:
            continue
        if token in CANONICAL_PRODUCT_KEYS:
            return token
        canonical = alias_map.get(token)
        if canonical:
            return canonical

    return "unknown"


def normalize_category_value(category_value: Any) -> str:
    if category_value is None:
        return ""

    raw = str(category_value).strip()
    if not raw:
        return ""

    alias_map = get_category_alias_map()
    for part in _RE_CSV.split(raw):
        token = str(part).strip()
        if not token:
            continue

        if token.isdigit():
            mapped = CATEGORY_LABELS.get(token)
            if mapped:
                return mapped

        mapped = alias_map.get(normalize_text_key(token))
        if mapped:
            return mapped

    return raw