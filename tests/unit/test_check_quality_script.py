from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path

import yaml

from collectors.base import RawItem
from graph.graph_store import GraphStore


def _load_script_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "check_quality.py"
    spec = importlib.util.spec_from_file_location("homeradar_check_quality_script", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_generate_quality_artifacts_writes_home_quality_report(
    tmp_path: Path,
    capsys,
) -> None:
    project_root = tmp_path
    (project_root / "config").mkdir(parents=True)

    (project_root / "config" / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "database_path": "data/homeradar.duckdb",
                "report_dir": "reports",
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (project_root / "config" / "sources.yaml").write_text(
        yaml.safe_dump(
            {
                "data_quality": {
                    "quality_outputs": {
                        "tracked_source_ids": ["molit_apt_transaction"],
                    }
                },
                "sources": [
                    {
                        "id": "molit_apt_transaction",
                        "name": "MOLIT Transaction",
                        "type": "rss",
                        "enabled": True,
                        "freshness_sla_days": 7,
                        "event_model": "transaction_price",
                        "verification_role": "official_primary_transaction",
                    }
                ],
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    store = GraphStore(project_root / "data" / "homeradar.duckdb")
    store.add_items(
        [
            RawItem(
                url="https://example.com/molit/transaction/1",
                title="Official transaction update",
                summary="MOLIT transaction record for apartment market monitoring.",
                source_id="molit_apt_transaction",
                published_at=datetime.now(UTC),
                region="서울",
                raw_data={
                    "home_quality": {
                        "verification_state": "official_primary",
                        "verification_role": "official_primary_transaction",
                        "merge_policy": "authoritative_source",
                        "event_model": "transaction_price",
                    }
                },
            )
        ]
    )

    module = _load_script_module()
    paths, report = module.generate_quality_artifacts(project_root)

    assert Path(paths["latest"]).exists()
    assert Path(paths["dated"]).exists()
    assert report["summary"]["tracked_sources"] == 1
    assert report["summary"]["fresh_sources"] == 1
    assert report["verification_states"] == {"official_primary": 1}

    module.PROJECT_ROOT = project_root
    module.main()
    captured = capsys.readouterr()
    assert "quality_report=" in captured.out
    assert "tracked_sources=1" in captured.out
