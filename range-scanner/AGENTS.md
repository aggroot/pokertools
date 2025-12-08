# Repository Guidelines

## Project Structure & Module Organization
Core logic resides in `poker_range_analyzer.py`, which houses the `HandHistoryParser`, `RangeAnalyzer`, and CLI `main`. Raw PokerStars hand histories stay in `hands/`; keep the original per-tournament folder names so position heuristics infer table sizes correctly. Regenerate `range_analysis_report.txt` plus the DuckDB warehouse `range_analysis.duckdb` whenever parsing rules change and treat the `test_*` outputs as comparison fixtures.

## Build, Test & Development Commands
- `python3 extract_zips.py` — recursively expands `.zip` files in `hands/`, deleting each archive afterward.
- `python3 test_analyzer.py` — smoke-test mode that scans the first 100 files, writes `test_range_analysis_report.txt`/`test_range_analysis.duckdb`, and logs a quick regression summary.
- `python3 poker_range_analyzer.py` — production run that uses all CPU cores, parses every file, and overwrites the main report and DuckDB warehouse.
- `python3 range_query_service.py serve --db range_analysis.duckdb` — lightweight HTTP API for querying the DuckDB warehouse; use `query` subcommand for ad-hoc CLI filtering.

## Coding Style & Naming Conventions
Use Python 3.10+ with 4-space indentation, `snake_case` for functions and variables, and `CapWords` for dataclasses such as `HandAction`. Keep regex patterns, position maps, and other constants at module scope; add a brief comment whenever betting or position logic is non-obvious. Favor `pathlib.Path`, `Counter`, and `defaultdict` for filesystem and aggregation tasks, and run `python -m black poker_range_analyzer.py test_analyzer.py` before committing for consistent formatting.

## Testing Guidelines
Treat `python3 test_analyzer.py` as the gating check; verify its console summary and diff its outputs against the committed `test_range_*` files before pushing. For parser changes, temporarily point the test script at a minimal `hands/<tmp>` subset and note any manual verification commands in the PR. Failing tests should include the smallest reproducible hand history snippet.

## Commit & Pull Request Guidelines
Upstream pokertools history uses short, imperative subjects prefixed with the touched area (for example, `parser: normalize blind detection`). Keep body text under 72 columns, list datasets touched, and mention any new or replaced output files. Pull requests should include motivation, the exact commands run, runtime or sample counts, and a sanitized excerpt (10–20 lines) from the regenerated report or JSON. Link to the tracking issue and call out anything reviewers need to download separately.

## Data & Configuration Tips
Hand histories often contain sensitive player names; scrub or alias them before sharing logs in tickets. Large deliveries (e.g., `2025-12-04_SNGMTT_ZXBBQ271.zip`) should live in `hands/` only long enough to extract; avoid committing them or the gigabyte-scale outputs they produce. Full runs saturate CPU via `multiprocessing.Pool`, so reduce concurrency by editing the `cpu_count()` usage on shared hardware and delete unneeded reports when finished.
