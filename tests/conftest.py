"""Pytest configuration for HomeRadar tests."""

import sys
from pathlib import Path

# Ensure the project root is in the path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
