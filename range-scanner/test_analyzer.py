#!/usr/bin/env python3
"""
Test version - analyzes only first N files to verify output
"""

import sys

sys.path.insert(0, ".")

from pathlib import Path
from collections import defaultdict
from multiprocessing import cpu_count

from poker_range_analyzer import (
    HandHistoryParser,
    RangeDatabaseExporter,
    RangeReportBuilder,
    process_tournaments,
)


def main():
    """Test on a subset of files"""
    print("Poker Range Analyzer - TEST MODE")
    print("=" * 80)

    hands_dir = Path("hands")
    if not hands_dir.exists():
        print(f"Error: {hands_dir} directory not found")
        return

    # Find all hand history files
    txt_files = list(hands_dir.rglob("*.txt"))

    # Test with first 100 files
    TEST_SIZE = 100
    test_files = txt_files[:TEST_SIZE]

    print(f"Total files available: {len(txt_files)}")
    print(f"Testing with: {TEST_SIZE} files")
    print()

    grouped_files = defaultdict(list)
    for file_path in test_files:
        tournament_id, chunk = HandHistoryParser.tournament_info_from_path(
            str(file_path)
        )
        grouped_files[tournament_id].append((chunk, file_path))

    tasks = []
    for tournament_id, files in grouped_files.items():
        sorted_entries = [
            (chunk, str(path)) for chunk, path in sorted(files, key=lambda x: x[0])
        ]
        tasks.append((tournament_id, sorted_entries))

    worker_count = min(cpu_count(), len(tasks), 4) or 1
    occurrences, hands_parsed, shown_hands, total_actions = process_tournaments(
        tasks, worker_count
    )

    print("\nParsing complete!")
    print(f"  Total hands parsed: {hands_parsed}")
    print(f"  Shown hands found: {shown_hands}")
    print(f"  Total actions tracked: {total_actions}")

    # Generate report
    print("\nGenerating analysis report...")
    db_file = "test_range_analysis.duckdb"
    exporter = RangeDatabaseExporter(db_file)
    exporter.export(occurrences)

    report_builder = RangeReportBuilder(db_file)
    report = report_builder.generate_report()

    # Save report
    report_file = "test_range_analysis_report.txt"
    with open(report_file, "w") as f:
        f.write(report)
    print(f"Report saved to {report_file}")

    # Print sample from report
    print("\n" + "=" * 80)
    print("SAMPLE OUTPUT (first 100 lines of report)")
    print("=" * 80)
    lines = report.split("\n")
    for line in lines[:100]:
        print(line)

    # Print quick summary
    print("\n" + "=" * 80)
    print("QUICK SUMMARY")
    print("=" * 80)

    # Count unique hands per position
    from collections import Counter

    for position, unique_combos, total in report_builder.preflop_open_summary():
        print(
            f"{position} Preflop Opening Range: {unique_combos} unique combos, {total} instances"
        )

    # Show storage summary
    print("\n" + "=" * 80)
    print("RANGE DATA WAREHOUSE")
    print("=" * 80)
    print(f"Range occurrences stored in {db_file}")
    print("Each row represents a single shown combo/action with filterable metadata.")

    print("\nDone! Check the output files for detailed analysis.")
    print("\nIf this looks good, run the full analyzer with:")
    print("  python3 poker_range_analyzer.py")


if __name__ == "__main__":
    main()
