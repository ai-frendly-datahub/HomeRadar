from __future__ import annotations
from typing import Optional

import asyncio
import os
from pathlib import Path

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from mcp_server.tools import (
    handle_price_watch,
    handle_recent_updates,
    handle_search,
    handle_sql,
    handle_top_trends,
)

app = Server("homeradar")


def _db_path() -> Path:
    return Path(os.getenv("HOMERADAR_DB_PATH", "data/homeradar.duckdb"))


def _search_db_path() -> Path:
    return Path(os.getenv("HOMERADAR_SEARCH_DB_PATH", "data/search_index.db"))


def _as_int(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _as_float(value: object) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="search",
            description="Search real-estate listings and articles using NL parsing + FTS5.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1},
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="recent_updates",
            description="Fetch recent HomeRadar items from DuckDB urls table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1},
                    "limit": {"type": "integer", "minimum": 1},
                },
            },
        ),
        Tool(
            name="sql",
            description="Execute read-only SQL (SELECT/WITH/EXPLAIN only) against HomeRadar DuckDB.",
            inputSchema={
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
        ),
        Tool(
            name="price_watch",
            description="Query transactions by region and price range from HomeRadar transaction data.",
            inputSchema={
                "type": "object",
                "properties": {
                    "region": {"type": "string"},
                    "min_price": {"type": "number"},
                    "max_price": {"type": "number"},
                    "limit": {"type": "integer", "minimum": 1},
                },
            },
        ),
        Tool(
            name="top_trends",
            description="Show top trending entities (district, complex, project).",
            inputSchema={
                "type": "object",
                "properties": {
                    "entity_type": {"type": "string"},
                    "days": {"type": "integer", "minimum": 1},
                    "limit": {"type": "integer", "minimum": 1},
                },
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict[str, object] | None) -> list[TextContent]:
    args: dict[str, object] = arguments or {}

    if name == "search":
        result = handle_search(
            search_db_path=_search_db_path(),
            db_path=_db_path(),
            query=str(args.get("query", "")),
            limit=_as_int(args.get("limit"), 20),
        )
    elif name == "recent_updates":
        result = handle_recent_updates(
            db_path=_db_path(),
            days=_as_int(args.get("days"), 7),
            limit=_as_int(args.get("limit"), 20),
        )
    elif name == "sql":
        result = handle_sql(db_path=_db_path(), query=str(args.get("query", "")))
    elif name == "price_watch":
        result = handle_price_watch(
            db_path=_db_path(),
            region=str(args["region"]) if "region" in args and args.get("region") is not None else None,
            min_price=_as_float(args.get("min_price")),
            max_price=_as_float(args.get("max_price")),
            limit=_as_int(args.get("limit"), 20),
        )
    elif name == "top_trends":
        result = handle_top_trends(
            db_path=_db_path(),
            entity_type=str(args.get("entity_type", "district")),
            days=_as_int(args.get("days"), 7),
            limit=_as_int(args.get("limit"), 10),
        )
    else:
        result = f"{{\"ok\": false, \"error\": \"Unknown tool: {name}\"}}"

    return [TextContent(type="text", text=result)]


async def main() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
