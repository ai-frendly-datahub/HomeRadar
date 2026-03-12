from __future__ import annotations

"""
Entity extractor for HomeRadar.

Extracts real estate-related entities from text:
- Apartment complexes (래미안, 힐스테이트, etc.)
- Districts and regions (강남구, 분당구, etc.)
- Development projects (GTX, 재개발, etc.)
- Keywords (급등, 전세, etc.)
"""

import re
from typing import Any, Optional

from analyzers.realestate_entities_data import (
    COMPLEX_BRANDS,
    DEVELOPMENT_PROJECTS,
    ENTITY_TYPES,
    KEYWORD_NORMALIZATION,
    KEYWORDS_ALL,
    REGION_NORMALIZATION,
    REGIONS_ALL,
)


_keyword_pattern_cache: dict[str, Optional[re.Pattern[str]]] = {}


def _is_ascii_only(keyword: str) -> bool:
    return all(ord(char) < 128 for char in keyword)


def _get_keyword_pattern(keyword: str) -> Optional[re.Pattern[str]]:
    cached = _keyword_pattern_cache.get(keyword)
    if keyword in _keyword_pattern_cache:
        return cached

    pattern = (
        re.compile(r"\b" + re.escape(keyword) + r"\b", re.IGNORECASE)
        if _is_ascii_only(keyword)
        else None
    )
    _keyword_pattern_cache[keyword] = pattern
    return pattern


class EntityExtractor:
    """
    Extract real estate entities from text.

    Uses dictionary-based matching with normalization.
    """

    def __init__(self):
        """Initialize entity extractor with dictionaries."""
        self.entity_types = ENTITY_TYPES
        self.region_normalization = REGION_NORMALIZATION
        self.keyword_normalization = KEYWORD_NORMALIZATION

    def extract(self, text: str) -> dict[str, list[str]]:
        """
        Extract all entity types from text.

        Args:
            text: Text to extract entities from

        Returns:
            Dictionary mapping entity types to lists of found entities
            Example: {
                "complex": ["래미안", "힐스테이트"],
                "district": ["강남구"],
                "project": ["GTX"],
                "keyword": ["급등", "전세"]
            }
        """
        if not text:
            return {}

        # Normalize text
        text_lower = text.lower()

        results: dict[str, list[str]] = {}

        # Extract each entity type
        for entity_type, entity_dict in self.entity_types.items():
            found = self._extract_entities(text, text_lower, entity_dict)

            # Apply normalization
            if entity_type == "district":
                found = self._normalize_regions(found)
            elif entity_type == "keyword":
                found = self._normalize_keywords(found)

            if found:
                results[entity_type] = list(set(found))  # Remove duplicates

        return results

    def _extract_entities(self, text: str, text_lower: str, entity_dict: set[str]) -> list[str]:
        """
        Extract entities from text using dictionary.

        Args:
            text: Original text
            text_lower: Lowercased text for case-insensitive matching
            entity_dict: Set of entity strings to search for

        Returns:
            List of found entities
        """
        found = []

        for entity in entity_dict:
            normalized = entity.lower()
            if not normalized:
                continue

            pattern = _get_keyword_pattern(normalized)
            matched = pattern.search(text) if pattern is not None else normalized in text_lower
            if matched or entity in text:
                found.append(entity)

        return found

    def _normalize_regions(self, regions: list[str]) -> list[str]:
        """
        Normalize region names.

        Args:
            regions: List of region names

        Returns:
            Normalized region names
        """
        normalized = []

        for region in regions:
            # Apply normalization map
            normalized_region = self.region_normalization.get(region.lower(), region)
            normalized.append(normalized_region)

        return normalized

    def _normalize_keywords(self, keywords: list[str]) -> list[str]:
        """
        Normalize keywords.

        Args:
            keywords: List of keywords

        Returns:
            Normalized keywords
        """
        normalized = []

        for keyword in keywords:
            # Apply normalization map
            normalized_keyword = self.keyword_normalization.get(keyword.lower(), keyword)
            normalized.append(normalized_keyword)

        return normalized

    def extract_from_item(self, item: dict[str, Any]) -> dict[str, list[str]]:
        """
        Extract entities from a news item (title + summary).

        Args:
            item: Dictionary with 'title' and 'summary' fields

        Returns:
            Dictionary of extracted entities
        """
        # Combine title and summary for extraction
        text = f"{item.get('title', '')} {item.get('summary', '')}"

        return self.extract(text)

    def get_entity_count(self, entities: dict[str, list[str]]) -> int:
        """
        Get total count of entities.

        Args:
            entities: Entity dictionary from extract()

        Returns:
            Total number of entities found
        """
        return sum(len(values) for values in entities.values())

    def has_entities(self, entities: dict[str, list[str]]) -> bool:
        """
        Check if any entities were found.

        Args:
            entities: Entity dictionary from extract()

        Returns:
            True if any entities found, False otherwise
        """
        return self.get_entity_count(entities) > 0


# Convenience function
def extract_entities(text: str) -> dict[str, list[str]]:
    """
    Extract entities from text (convenience function).

    Args:
        text: Text to extract entities from

    Returns:
        Dictionary of extracted entities
    """
    extractor = EntityExtractor()
    return extractor.extract(text)
