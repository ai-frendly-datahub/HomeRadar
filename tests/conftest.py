"""Pytest configuration for HomeRadar tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import structlog

# Ensure the project root is in the path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


@pytest.fixture(autouse=True)
def reset_structlog() -> object:
    structlog.reset_defaults()
    yield
    structlog.reset_defaults()
