#!/usr/bin/env python3
"""
Test Individual Analytical Queries

Simple script to test individual queries against specific partitioning strategies
for debugging and development purposes.
"""

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.analytics.analytical_queries import AnalyticalQueryRunner


def main():
    parser = argparse.ArgumentParser(description="Test individual analytical queries")
    parser.add_argument(
        "--strategy",
        choices=["no_partition", "attribute_state", "spatial_h3_l3", "hybrid_state_h3"],
        required=True,
        help="Partitioning strategy to test",
    )
    parser.add_argument(
        "--query",
        choices=["1", "2", "3", "4", "5", "all"],
        default="all",
        help="Query number to run (default: all)",
    )

    args = parser.parse_args()

    try:
        runner = AnalyticalQueryRunner()

        if args.query == "all":
            results = runner.run_all_queries(args.strategy)
            print(f"\n📊 Results for {args.strategy}:")

            if "summary" in results:
                summary = results["summary"]
                print(
                    f"   ✅ {summary['successful_queries']}/{summary['total_queries']} queries succeeded"
                )
                print(
                    f"   ⏱️  Total execution time: {summary['total_execution_time']:.2f}s"
                )
                print(f"   💾 Peak memory usage: {summary['max_memory_usage']:.1f}MB")
        else:
            # Run single query
            dataset_path = runner._get_dataset_path(args.strategy)
            query_methods = {
                "1": runner.query_1_state_aggregation,
                "2": runner.query_2_spatial_filtering,
                "3": runner.query_3_multi_state_analysis,
                "4": runner.query_4_full_table_scan,
                "5": runner.query_5_complex_spatial_attribute,
            }

            print(f"🔍 Testing Query {args.query} against {args.strategy}")
            result = query_methods[args.query](dataset_path)

            if result["success"]:
                print(f"✅ Query completed in {result['execution_time_seconds']:.2f}s")
                print(f"   📊 Returned {result['result_rows']} rows")
                print(f"   💾 Memory usage: {result['memory_peak_mb']:.1f}MB")
                if result["result_sample"]:
                    print(f"   📋 Sample results: {result['result_sample']}")
            else:
                print(f"❌ Query failed: {result['error']}")

        return 0

    except Exception as e:
        print(f"❌ Test failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
