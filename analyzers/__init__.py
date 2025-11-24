"""
HomeRadar analyzers module.

This module analyzes real estate data:
- Entity extraction (complex names, districts, development zones)
- Price trend analysis (surge/drop detection)
- Market signal scoring (rent-to-price gap, transaction volume)
"""

from analyzers.entity_extractor import EntityExtractor, extract_entities

__all__ = ["EntityExtractor", "extract_entities"]
