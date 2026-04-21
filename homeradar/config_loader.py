from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml

from .models import RadarSettings


def _read_yaml_dict(path: Path) -> dict[str, Any]:
    loaded = cast(object, yaml.safe_load(path.read_text(encoding="utf-8")))
    if isinstance(loaded, dict):
        return {str(k): v for k, v in cast(dict[object, Any], loaded).items()}
    return {}


def _resolve_path(path_value: str, *, project_root: Path) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return (project_root / path).resolve()


def load_settings(config_path: Path | None = None) -> RadarSettings:
    project_root = Path(__file__).resolve().parents[1]
    config_file = config_path or project_root / "config" / "config.yaml"

    raw: dict[str, object] = {}
    if config_file.exists():
        raw = _read_yaml_dict(config_file)

    return RadarSettings(
        database_path=_resolve_path(
            str(raw.get("database_path", "data/homeradar.duckdb")),
            project_root=project_root,
        ),
        report_dir=_resolve_path(
            str(raw.get("report_dir", "reports")),
            project_root=project_root,
        ),
        raw_data_dir=_resolve_path(
            str(raw.get("raw_data_dir", "data/raw")),
            project_root=project_root,
        ),
        search_db_path=_resolve_path(
            str(raw.get("search_db_path", "data/search_index.db")),
            project_root=project_root,
        ),
    )


def load_source_quality_config(sources_path: Path | None = None) -> dict[str, Any]:
    """Load HomeRadar source quality metadata from config/sources.yaml."""
    project_root = Path(__file__).resolve().parents[1]
    config_file = sources_path or project_root / "config" / "sources.yaml"

    if not config_file.exists():
        raise FileNotFoundError(f"Source config not found: {config_file}")

    raw = _read_yaml_dict(config_file)
    return {
        "data_quality": raw.get("data_quality", {}),
        "source_backlog": raw.get("source_backlog", {}),
        "sources": raw.get("sources", []),
    }
