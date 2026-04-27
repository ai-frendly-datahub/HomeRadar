from __future__ import annotations

import sys
from importlib import import_module


_MODULE_ALIASES = {
    "analyzer": "homeradar.analyzer",
    "collector": "homeradar.collector",
    "exceptions": "homeradar.exceptions",
    "models": "homeradar.models",
    "nl_query": "homeradar.nl_query",
    "reporter": "homeradar.reporter",
    "search_index": "homeradar.search_index",
    "storage": "homeradar.storage",
}

for _module_name, _target in _MODULE_ALIASES.items():
    sys.modules[f"{__name__}.{_module_name}"] = import_module(_target)


RadarStorage = import_module("homeradar.storage").RadarStorage


__all__ = ["RadarStorage"]
