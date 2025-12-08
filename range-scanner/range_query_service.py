#!/usr/bin/env python3
"""
Lightweight HTTP service that exposes range queries backed by the DuckDB fact table.

Every response mirrors the old JSON structure so the viewer/tests can consume it:
- `all`: summary for every combo matching the filters
- `by_pot_size`, `by_bb_size`, `by_stack_bucket`, `by_tournament_stage`: bucketed views

Example:
    python3 range_query_service.py serve --db range_analysis.duckdb --port 8080
    curl "http://localhost:8080/ranges?position=BTN&stage=preflop&action=raise"
"""

from __future__ import annotations

import argparse
import json
import duckdb
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse


HAND_RANK_ORDER = "AKQJT98765432"


def hand_rank_key(hand: str) -> Tuple[int, int]:
    """Sort pocket pairs ahead of suited/off-suit combos."""
    if len(hand) == 2:
        return (-100, HAND_RANK_ORDER.index(hand[0]))
    first = HAND_RANK_ORDER.index(hand[0])
    second = HAND_RANK_ORDER.index(hand[1]) if len(hand) > 1 else first
    return (-10, first * 2 + second)


def build_summary(rows: List[Tuple[str, int]]) -> Dict:
    """Convert (hand, count) rows into frequency stats."""
    total = sum(count for _, count in rows)
    summary = {}
    for hand, count in sorted(rows, key=lambda item: hand_rank_key(item[0])):
        freq = (count / total * 100) if total else 0
        summary[hand] = {
            "count": count,
            "frequency_pct": round(freq, 2),
        }
    counts = [count for _, count in rows]
    median_pct = (median(counts) / total * 100) if total and counts else 0
    return {
        "hands": summary,
        "total_instances": total,
        "unique_combos": len(rows),
        "median_frequency_pct": round(median_pct, 2),
    }


@dataclass
class RangeQueryFilters:
    """Filter payload used by the HTTP API and CLI."""

    position: Optional[str] = None
    stage: Optional[str] = None
    action: Optional[str] = None
    tournament_stage: Optional[str] = None
    pot_bucket: Optional[str] = None
    bb_bucket: Optional[str] = None
    stack_bucket: Optional[str] = None
    player: Optional[str] = None
    tournament_id: Optional[str] = None
    stack_bb_min: Optional[float] = None
    stack_bb_max: Optional[float] = None
    cards: Optional[str] = None
    limit: Optional[int] = None


class RangeQueryService:
    """Executes aggregate range queries against the DuckDB warehouse."""

    BASE_FROM = """
        FROM range_occurrences ro
    """

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database {self.db_path} not found.")

    def query_ranges(self, filters: RangeQueryFilters) -> Dict:
        if not (filters.position and filters.stage and filters.action):
            raise ValueError("position, stage, and action filters are required")

        with duckdb.connect(self.db_path.as_posix()) as conn:
            where_clause, params = self._build_where(filters)

            all_rows = self._query_all(conn, where_clause, params, filters.limit)
            pot_rows = self._query_bucket(
                conn, where_clause, params, "COALESCE(ro.pot_bucket, 'N/A')"
            )
            bb_rows = self._query_bucket(
                conn, where_clause, params, "COALESCE(ro.bb_bucket, 'N/A')"
            )
            stack_rows = self._query_bucket(
                conn, where_clause, params, "COALESCE(ro.stack_bucket, 'UNKNOWN')"
            )
            tournament_rows = self._query_bucket(
                conn, where_clause, params, "COALESCE(ro.tournament_stage, 'UNKNOWN')"
            )

        return {
            "filters": filters.__dict__,
            "all": build_summary(all_rows),
            "by_pot_size": self._group_bucket_rows(pot_rows),
            "by_bb_size": self._group_bucket_rows(bb_rows),
            "by_stack_bucket": self._group_bucket_rows(stack_rows),
            "by_tournament_stage": self._group_bucket_rows(tournament_rows),
        }

    def _build_where(self, filters: RangeQueryFilters) -> Tuple[str, List]:
        clauses: List[str] = []
        params: List = []
        mapping = {
            "position": ("ro.position = ?", filters.position),
            "stage": ("ro.stage = ?", filters.stage),
            "action": ("ro.action = ?", filters.action),
            "tournament_stage": ("ro.tournament_stage = ?", filters.tournament_stage),
            "pot_bucket": ("ro.pot_bucket = ?", filters.pot_bucket),
            "bb_bucket": ("ro.bb_bucket = ?", filters.bb_bucket),
            "stack_bucket": ("ro.stack_bucket = ?", filters.stack_bucket),
            "player": ("ro.player = ?", filters.player),
            "tournament_id": ("ro.tournament_id = ?", filters.tournament_id),
            "cards": ("ro.cards = ?", filters.cards),
        }
        for _, (clause, value) in mapping.items():
            if value:
                clauses.append(clause)
                params.append(value)

        if filters.stack_bb_min is not None:
            clauses.append("ro.stack_size_bb >= ?")
            params.append(filters.stack_bb_min)
        if filters.stack_bb_max is not None:
            clauses.append("ro.stack_size_bb <= ?")
            params.append(filters.stack_bb_max)

        where_clause = "WHERE " + " AND ".join(clauses) if clauses else ""
        return where_clause, params

    def _query_all(
        self,
        conn: duckdb.DuckDBPyConnection,
        where_clause: str,
        params: List,
        limit: Optional[int],
    ) -> List[Tuple[str, int]]:
        query = f"""
            SELECT ro.cards AS hand, COUNT(*) AS count
            {self.BASE_FROM}
            {where_clause}
            GROUP BY ro.cards
            ORDER BY count DESC
        """
        if limit:
            query += " LIMIT ?"
            params = params + [limit]
        cursor = conn.execute(query, params)
        return cursor.fetchall()

    def _query_bucket(
        self,
        conn: duckdb.DuckDBPyConnection,
        where_clause: str,
        params: List,
        bucket_expr: str,
    ) -> List[Tuple[str, str, int]]:
        query = f"""
            SELECT {bucket_expr} AS bucket, ro.cards AS hand, COUNT(*) AS count
            {self.BASE_FROM}
            {where_clause}
            GROUP BY bucket, ro.cards
            ORDER BY bucket, count DESC
        """
        cursor = conn.execute(query, params)
        return cursor.fetchall()

    def _group_bucket_rows(self, rows: List[Tuple[str, str, int]]) -> Dict[str, Dict]:
        grouped: Dict[str, List[Tuple[str, int]]] = {}
        for bucket, hand, count in rows:
            grouped.setdefault(bucket, []).append((hand, count))
        return {
            bucket: build_summary(bucket_rows)
            for bucket, bucket_rows in grouped.items()
        }


class _APIRequestHandler(BaseHTTPRequestHandler):
    """HTTP handler that serves /ranges and /health endpoints."""

    def __init__(self, service: RangeQueryService, *args, **kwargs):
        self.service = service
        super().__init__(*args, **kwargs)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_response(200, {"status": "ok"})
            return
        if parsed.path != "/ranges":
            self._send_response(404, {"error": "not found"})
            return

        try:
            filters = self._parse_filters(parse_qs(parsed.query))
            result = self.service.query_ranges(filters)
            self._send_response(200, result)
        except ValueError as exc:
            self._send_response(400, {"error": str(exc)})
        except Exception as exc:  # pylint: disable=broad-except
            self._send_response(500, {"error": str(exc)})

    def _parse_filters(self, query: Dict[str, List[str]]) -> RangeQueryFilters:
        def get(name: str) -> Optional[str]:
            return query.get(name, [None])[0]

        def get_float(name: str) -> Optional[float]:
            value = get(name)
            if value is None:
                return None
            try:
                return float(value)
            except ValueError:
                raise ValueError(f"Invalid float for {name}: {value}") from None

        def get_int(name: str) -> Optional[int]:
            value = get(name)
            if value is None:
                return None
            try:
                return int(value)
            except ValueError:
                raise ValueError(f"Invalid integer for {name}: {value}") from None

        return RangeQueryFilters(
            position=get("position"),
            stage=get("stage"),
            action=get("action"),
            tournament_stage=get("tournament_stage"),
            pot_bucket=get("pot_bucket"),
            bb_bucket=get("bb_bucket"),
            stack_bucket=get("stack_bucket"),
            player=get("player"),
            tournament_id=get("tournament_id"),
            stack_bb_min=get_float("stack_bb_min"),
            stack_bb_max=get_float("stack_bb_max"),
            cards=get("cards"),
            limit=get_int("limit"),
        )

    def _send_response(self, status_code: int, payload: Dict):
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: A003
        """Silence noisy default logging."""
        return


def make_handler(service: RangeQueryService):
    def handler(*args, **kwargs):
        _APIRequestHandler(service, *args, **kwargs)

    return handler


def run_server(db_path: Path, host: str, port: int):
    service = RangeQueryService(db_path)
    handler = make_handler(service)
    httpd = ThreadingHTTPServer((host, port), handler)
    print(f"Range query service listening on http://{host}:{port} (db={db_path})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        httpd.server_close()


def run_cli_query(db_path: Path, filters: RangeQueryFilters):
    service = RangeQueryService(db_path)
    result = service.query_ranges(filters)
    print(json.dumps(result, indent=2))


def parse_args():
    parser = argparse.ArgumentParser(description="Range Query Service")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("range_analysis.duckdb"),
        help="DuckDB database path",
    )
    subparsers = parser.add_subparsers(dest="command")

    serve_parser = subparsers.add_parser("serve", help="Start HTTP server (default)")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8080)

    query_parser = subparsers.add_parser("query", help="Run a single query via CLI")
    query_parser.add_argument("--position", required=True)
    query_parser.add_argument("--stage", required=True)
    query_parser.add_argument("--action", required=True)
    query_parser.add_argument("--tournament_stage")
    query_parser.add_argument("--pot_bucket")
    query_parser.add_argument("--bb_bucket")
    query_parser.add_argument("--stack_bucket")
    query_parser.add_argument("--player")
    query_parser.add_argument("--tournament_id")
    query_parser.add_argument("--stack_bb_min", type=float)
    query_parser.add_argument("--stack_bb_max", type=float)
    query_parser.add_argument("--cards")
    query_parser.add_argument("--limit", type=int)

    return parser.parse_args()


def main():
    args = parse_args()
    filters = RangeQueryFilters()
    if args.command == "query":
        filters = RangeQueryFilters(
            position=args.position,
            stage=args.stage,
            action=args.action,
            tournament_stage=args.tournament_stage,
            pot_bucket=args.pot_bucket,
            bb_bucket=args.bb_bucket,
            stack_bucket=args.stack_bucket,
            player=args.player,
            tournament_id=args.tournament_id,
            stack_bb_min=args.stack_bb_min,
            stack_bb_max=args.stack_bb_max,
            cards=args.cards,
            limit=args.limit,
        )
        run_cli_query(args.db, filters)
    else:
        host = getattr(args, "host", "127.0.0.1")
        port = getattr(args, "port", 8080)
        run_server(args.db, host, port)


if __name__ == "__main__":
    main()
