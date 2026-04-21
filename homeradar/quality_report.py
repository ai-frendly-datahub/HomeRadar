from __future__ import annotations

import json
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def build_quality_report(
    *,
    sources: Sequence[Mapping[str, Any]],
    store: Any,
    quality_config: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _as_utc(generated_at or datetime.now(UTC))
    quality = _dict(quality_config or {}, "data_quality")
    source_metrics = _load_source_metrics(store)
    verification_states = _load_verification_state_counts(store)

    source_rows = [
        _build_source_row(
            source=source,
            metrics=source_metrics.get(str(source.get("id", ""))),
            quality=quality,
            generated_at=generated,
        )
        for source in sources
    ]
    status_counts = Counter(str(row["status"]) for row in source_rows)
    official_summary = _official_primary_summary(source_rows, verification_states)

    summary = {
        "total_sources": len(source_rows),
        "enabled_sources": sum(1 for row in source_rows if row["enabled"]),
        "tracked_sources": sum(1 for row in source_rows if row["tracked"]),
        "fresh_sources": status_counts.get("fresh", 0),
        "stale_sources": status_counts.get("stale", 0),
        "stale_source_ids": _source_ids_by_status(source_rows, "stale"),
        "missing_sources": status_counts.get("missing", 0),
        "missing_source_ids": _source_ids_by_status(source_rows, "missing"),
        "unknown_event_date_sources": status_counts.get("unknown_event_date", 0),
        "unknown_event_date_source_ids": _source_ids_by_status(source_rows, "unknown_event_date"),
        "skipped_sources": (
            status_counts.get("skipped_disabled", 0)
            + status_counts.get("skipped_missing_env", 0)
        ),
        "skipped_disabled_sources": status_counts.get("skipped_disabled", 0),
        "skipped_disabled_source_ids": _source_ids_by_status(source_rows, "skipped_disabled"),
        "skipped_missing_env_sources": status_counts.get("skipped_missing_env", 0),
        "skipped_missing_env_source_ids": _source_ids_by_status(source_rows, "skipped_missing_env"),
        "not_tracked_sources": status_counts.get("not_tracked", 0),
        "verification_state_count": sum(verification_states.values()),
    }
    summary.update(official_summary)

    return {
        "category": "home",
        "generated_at": generated.isoformat(),
        "summary": summary,
        "verification_states": dict(sorted(verification_states.items())),
        "sources": source_rows,
    }


def write_quality_report(
    report: Mapping[str, Any],
    *,
    output_dir: Path,
    category_name: str = "home",
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = _parse_datetime(str(report.get("generated_at") or "")) or datetime.now(UTC)
    date_stamp = _as_utc(generated_at).strftime("%Y%m%d")
    latest_path = output_dir / f"{category_name}_quality.json"
    dated_path = output_dir / f"{category_name}_{date_stamp}_quality.json"
    encoded = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    latest_path.write_text(encoded + "\n", encoding="utf-8")
    dated_path.write_text(encoded + "\n", encoding="utf-8")
    return {"latest": latest_path, "dated": dated_path}


def _build_source_row(
    *,
    source: Mapping[str, Any],
    metrics: Mapping[str, Any] | None,
    quality: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    source_id = str(source.get("id", "")).strip()
    freshness_sla_days = _source_sla_days(source, quality)
    latest_event_at = _parse_datetime(str((metrics or {}).get("latest_event_at") or ""))
    latest_observed_at = _parse_datetime(str((metrics or {}).get("latest_observed_at") or ""))
    age_days = _age_days(generated_at, latest_event_at) if latest_event_at else None
    required_env = _required_env_var(source)
    item_count = _as_int((metrics or {}).get("item_count")) or 0
    tracked = freshness_sla_days is not None
    status = _source_status(
        source=source,
        tracked=tracked,
        required_env=required_env,
        item_count=item_count,
        latest_event_at=latest_event_at,
        freshness_sla_days=freshness_sla_days,
        age_days=age_days,
    )

    return {
        "source_id": source_id,
        "name": str(source.get("name", source_id)),
        "enabled": bool(source.get("enabled", False)),
        "tracked": tracked,
        "status": status,
        "skip_reason": _skip_reason(required_env, status),
        "required_env": required_env or "",
        "event_model": str(source.get("event_model", "")),
        "verification_role": str(source.get("verification_role", "")),
        "merge_policy": str(source.get("merge_policy", "")),
        "official_primary": _is_official_primary_source(source, quality),
        "freshness_sla_days": freshness_sla_days,
        "event_date_field": str(source.get("event_date_field", "")),
        "observed_date_field": str(source.get("observed_date_field", "")),
        "item_count": item_count,
        "latest_event_at": latest_event_at.isoformat() if latest_event_at else None,
        "latest_observed_at": latest_observed_at.isoformat() if latest_observed_at else None,
        "age_days": round(age_days, 2) if age_days is not None else None,
        "latest_title": str((metrics or {}).get("latest_title", "")),
        "latest_url": str((metrics or {}).get("latest_url", "")),
    }


def _official_primary_summary(
    source_rows: Sequence[Mapping[str, Any]],
    verification_states: Mapping[str, int],
) -> dict[str, Any]:
    official_rows = [row for row in source_rows if bool(row.get("official_primary"))]
    official_status_counts = Counter(str(row.get("status", "")) for row in official_rows)
    blocked_rows = [
        row for row in official_rows if str(row.get("status", "")) == "skipped_missing_env"
    ]
    blocked_source_ids = [str(row.get("source_id", "")) for row in blocked_rows]
    required_env = sorted(
        {
            str(row.get("required_env", ""))
            for row in blocked_rows
            if str(row.get("required_env", "")).strip()
        }
    )

    official_count = len(official_rows)
    fresh_count = official_status_counts.get("fresh", 0)
    blocked_count = len(blocked_rows)
    credentialed_count = sum(
        1
        for row in official_rows
        if bool(row.get("enabled"))
        and str(row.get("status", "")) not in {"skipped_missing_env", "skipped_disabled"}
    )

    if official_count == 0:
        official_status = "not_configured"
    elif blocked_count == official_count:
        official_status = "blocked_missing_env"
    elif fresh_count == official_count:
        official_status = "fresh"
    elif fresh_count > 0:
        official_status = "partial_fresh"
    elif blocked_count > 0:
        official_status = "partial_blocked"
    else:
        official_status = "no_fresh_official_primary"

    return {
        "official_primary_status": official_status,
        "official_primary_sources": official_count,
        "official_primary_fresh_sources": fresh_count,
        "official_primary_credentialed_sources": credentialed_count,
        "official_primary_blocked_sources": blocked_count,
        "official_primary_blocked_source_ids": blocked_source_ids,
        "official_primary_required_env": required_env,
        "corroboration_only_items": int(
            verification_states.get("market_corroboration_requires_official_source", 0)
        ),
        "official_primary_items": int(verification_states.get("official_primary", 0)),
    }


def _source_ids_by_status(
    source_rows: Sequence[Mapping[str, Any]],
    status: str,
) -> list[str]:
    return [
        str(row.get("source_id", ""))
        for row in source_rows
        if str(row.get("status", "")) == status and str(row.get("source_id", "")).strip()
    ]


def _is_official_primary_source(
    source: Mapping[str, Any],
    quality: Mapping[str, Any],
) -> bool:
    source_id = str(source.get("id", "")).strip()
    tracked_ids = _tracked_source_ids(quality)
    if tracked_ids:
        return source_id in tracked_ids

    verification_role = str(source.get("verification_role", ""))
    merge_policy = str(source.get("merge_policy", ""))
    return verification_role.startswith("official_primary") or merge_policy == "authoritative_source"


def _tracked_source_ids(quality: Mapping[str, Any]) -> set[str]:
    quality_outputs = _dict(quality, "quality_outputs")
    raw_ids = quality_outputs.get("tracked_source_ids")
    if not isinstance(raw_ids, Sequence) or isinstance(raw_ids, str):
        return set()
    return {str(source_id).strip() for source_id in raw_ids if str(source_id).strip()}


def _source_status(
    *,
    source: Mapping[str, Any],
    tracked: bool,
    required_env: str | None,
    item_count: int,
    latest_event_at: datetime | None,
    freshness_sla_days: int | None,
    age_days: float | None,
) -> str:
    if not bool(source.get("enabled", False)):
        return "skipped_disabled"
    if required_env:
        return "skipped_missing_env"
    if not tracked:
        return "not_tracked"
    if item_count == 0:
        return "missing"
    if latest_event_at is None or age_days is None:
        return "unknown_event_date"
    if freshness_sla_days is not None and age_days > freshness_sla_days:
        return "stale"
    return "fresh"


def _load_source_metrics(store: Any) -> dict[str, dict[str, Any]]:
    try:
        with store._connection() as conn:
            rows = conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        source_id,
                        url,
                        title,
                        published_at,
                        COALESCE(last_seen_at, updated_at, created_at) AS observed_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY source_id
                            ORDER BY COALESCE(published_at, last_seen_at, updated_at, created_at) DESC NULLS LAST
                        ) AS rn
                    FROM urls
                ),
                counts AS (
                    SELECT
                        source_id,
                        COUNT(*) AS item_count,
                        MAX(published_at) AS latest_event_at,
                        MAX(COALESCE(last_seen_at, updated_at, created_at)) AS latest_observed_at
                    FROM urls
                    GROUP BY source_id
                )
                SELECT
                    counts.source_id,
                    counts.item_count,
                    counts.latest_event_at,
                    counts.latest_observed_at,
                    ranked.title,
                    ranked.url
                FROM counts
                LEFT JOIN ranked
                  ON ranked.source_id = counts.source_id AND ranked.rn = 1
                """
            ).fetchall()
    except Exception:
        return {}

    metrics: dict[str, dict[str, Any]] = {}
    for row in rows:
        source_id = str(row[0])
        metrics[source_id] = {
            "item_count": int(row[1]),
            "latest_event_at": row[2],
            "latest_observed_at": row[3],
            "latest_title": row[4] or "",
            "latest_url": row[5] or "",
        }
    return metrics


def _load_verification_state_counts(store: Any) -> dict[str, int]:
    try:
        with store._connection() as conn:
            rows = conn.execute(
                """
                SELECT COALESCE(NULLIF(TRIM(verification_state), ''), 'unclassified') AS state,
                       COUNT(*) AS count
                FROM urls
                GROUP BY 1
                ORDER BY count DESC, state ASC
                """
            ).fetchall()
    except Exception:
        return {}
    return {str(row[0]): int(row[1]) for row in rows}


def _source_sla_days(
    source: Mapping[str, Any],
    quality: Mapping[str, Any],
) -> int | None:
    source_sla = _as_int(source.get("freshness_sla_days"))
    if source_sla is not None:
        return source_sla

    source_id = str(source.get("id", "")).strip()
    freshness = _dict(quality, "freshness_sla")
    raw_model = freshness.get(source_id)
    if isinstance(raw_model, Mapping):
        return _as_int(raw_model.get("max_age_days"))
    return None


def _required_env_var(source: Mapping[str, Any]) -> str | None:
    source_id = str(source.get("id", "")).strip()
    source_type = str(source.get("type", "")).strip().lower()
    if source_type == "api" and source_id.startswith("molit"):
        if source.get("service_key") or os.environ.get("MOLIT_SERVICE_KEY"):
            return None
        return "MOLIT_SERVICE_KEY"
    if source_type == "subscription":
        if source.get("api_key") or os.environ.get("SUBSCRIPTION_API_KEY"):
            return None
        return "SUBSCRIPTION_API_KEY"
    if source_type == "onbid":
        if source.get("api_key") or os.environ.get("ONBID_API_KEY"):
            return None
        return "ONBID_API_KEY"
    return None


def _skip_reason(required_env: str | None, status: str) -> str:
    if status == "skipped_missing_env" and required_env:
        return f"{required_env} not set"
    if status == "skipped_disabled":
        return "source disabled"
    return ""


def _dict(mapping: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = mapping.get(key)
    return value if isinstance(value, Mapping) else {}


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _age_days(generated_at: datetime, event_at: datetime) -> float:
    return max(0.0, (_as_utc(generated_at) - _as_utc(event_at)).total_seconds() / 86400)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None
