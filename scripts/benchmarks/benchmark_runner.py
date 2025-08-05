#!/usr/bin/env python3
"""
Comprehensive Benchmark Runner

Executes analytical queries against all partitioning strategies and collects
performance metrics for comparison.
"""

import argparse
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.analytics.analytical_queries import (
    AnalyticalQueryRunner,
    save_benchmark_results,
)


def check_dataset_availability(runner: AnalyticalQueryRunner) -> dict:
    """Check which datasets are available for testing"""
    strategies = ["no_partition", "attribute_state", "spatial_h3_l6", "hybrid_state_h3"]
    available = {}

    print("🔍 Checking dataset availability...")

    for strategy in strategies:
        try:
            dataset_path = runner._get_dataset_path(strategy)

            # Try a simple count query to verify the dataset exists and is readable
            test_query = f"SELECT COUNT(*) FROM '{dataset_path}' LIMIT 1;"
            result = runner.conn.execute(test_query).fetchone()

            if result and result[0] > 0:
                available[strategy] = {
                    "status": "available",
                    "path": dataset_path,
                    "record_count": result[0],
                }
                print(f"   ✅ {strategy}: {result[0]:,} records")
            else:
                available[strategy] = {
                    "status": "empty",
                    "path": dataset_path,
                    "record_count": 0,
                }
                print(f"   ⚠️  {strategy}: dataset empty")

        except Exception as e:
            available[strategy] = {
                "status": "unavailable",
                "path": runner._get_dataset_path(strategy),
                "error": str(e),
            }
            print(f"   ❌ {strategy}: {str(e)[:100]}")

    return available


def run_lightweight_benchmark(
    runner: AnalyticalQueryRunner, available_strategies: list
) -> dict:
    """Run a lightweight benchmark with only fast queries"""
    print(
        f"\n🚀 Running Lightweight Benchmark on {len(available_strategies)} strategies"
    )
    print("=" * 60)

    results = {
        "benchmark_type": "lightweight",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "strategies_tested": available_strategies,
        "results": {},
    }

    for strategy in available_strategies:
        print(f"\n📊 Testing {strategy.upper()}")

        try:
            dataset_path = runner._get_dataset_path(strategy)

            # Run only the fast, non-spatial queries
            queries = [
                ("query_1_state_aggregation", runner.query_1_state_aggregation),
                ("query_3_multi_state_analysis", runner.query_3_multi_state_analysis),
            ]

            strategy_results = {
                "strategy": strategy,
                "dataset_path": dataset_path,
                "queries": {},
            }

            for query_name, query_func in queries:
                print(f"   🔍 {query_name}...")

                try:
                    query_result = query_func(dataset_path)
                    strategy_results["queries"][query_name] = query_result

                    if query_result["success"]:
                        print(
                            f"      ✅ {query_result['execution_time_seconds']:.2f}s "
                            f"({query_result['result_rows']} rows)"
                        )
                    else:
                        print(f"      ❌ {query_result['error']}")

                except Exception as e:
                    print(f"      ❌ Exception: {e}")
                    strategy_results["queries"][query_name] = {
                        "success": False,
                        "error": str(e),
                    }

            # Calculate summary
            successful_queries = [
                q for q in strategy_results["queries"].values() if q["success"]
            ]
            if successful_queries:
                strategy_results["summary"] = {
                    "successful_queries": len(successful_queries),
                    "total_execution_time": sum(
                        q["execution_time_seconds"] for q in successful_queries
                    ),
                    "avg_execution_time": sum(
                        q["execution_time_seconds"] for q in successful_queries
                    )
                    / len(successful_queries),
                }

            results["results"][strategy] = strategy_results

        except Exception as e:
            print(f"   ❌ Strategy failed: {e}")
            results["results"][strategy] = {"success": False, "error": str(e)}

    return results


def run_full_benchmark(
    runner: AnalyticalQueryRunner, available_strategies: list
) -> dict:
    """Run the complete benchmark with all queries"""
    print(f"\n🎯 Running Full Benchmark on {len(available_strategies)} strategies")
    print("=" * 60)

    results = {
        "benchmark_type": "full",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "strategies_tested": available_strategies,
        "results": {},
    }

    for strategy in available_strategies:
        print(f"\n📊 Strategy: {strategy.upper()}")

        try:
            strategy_results = runner.run_all_queries(strategy)
            results["results"][strategy] = strategy_results

            if "summary" in strategy_results:
                summary = strategy_results["summary"]
                print(
                    f"   📈 Summary: {summary['successful_queries']}/{summary['total_queries']} queries"
                )
                print(f"   ⏱️  Total time: {summary['total_execution_time']:.2f}s")
                print(f"   💾 Peak memory: {summary['max_memory_usage']:.1f}MB")

        except Exception as e:
            print(f"   ❌ Strategy failed: {e}")
            results["results"][strategy] = {"success": False, "error": str(e)}

    return results


def print_comparative_summary(results: dict):
    """Print a comparative summary of benchmark results"""
    print(f"\n📈 Benchmark Summary ({results['benchmark_type'].title()})")
    print("=" * 60)

    strategies_data = []

    for strategy, strategy_results in results["results"].items():
        if "summary" in strategy_results:
            summary = strategy_results["summary"]
            strategies_data.append(
                {
                    "strategy": strategy,
                    "total_time": summary.get("total_execution_time", 0),
                    "avg_time": summary.get("avg_execution_time", 0),
                    "successful_queries": summary.get("successful_queries", 0),
                    "peak_memory": summary.get("max_memory_usage", 0),
                }
            )

    if strategies_data:
        # Sort by total execution time
        strategies_data.sort(key=lambda x: x["total_time"])

        print(
            f"{'Strategy':<20} {'Total Time':<12} {'Avg Time':<10} {'Success':<8} {'Peak Memory':<12}"
        )
        print("-" * 70)

        for data in strategies_data:
            print(
                f"{data['strategy']:<20} {data['total_time']:>8.2f}s    "
                f"{data['avg_time']:>6.2f}s   {data['successful_queries']:>3}      "
                f"{data['peak_memory']:>8.1f}MB"
            )

        # Find best performing strategy
        if strategies_data:
            fastest = strategies_data[0]
            print(
                f"\n🏆 Fastest Strategy: {fastest['strategy']} ({fastest['total_time']:.2f}s total)"
            )


def main():
    """Main benchmark execution"""
    parser = argparse.ArgumentParser(description="Run analytical query benchmarks")
    parser.add_argument(
        "--mode",
        choices=["check", "lightweight", "full"],
        default="lightweight",
        help="Benchmark mode (default: lightweight)",
    )
    parser.add_argument(
        "--strategies",
        nargs="+",
        choices=["no_partition", "attribute_state", "spatial_h3_l6", "hybrid_state_h3"],
        help="Specific strategies to test (default: all available)",
    )

    args = parser.parse_args()

    try:
        print("🚀 Parquet Performance Testing - Analytical Benchmark")
        print("=" * 60)

        # Initialize runner
        runner = AnalyticalQueryRunner()

        # Check dataset availability
        availability = check_dataset_availability(runner)
        available_strategies = [
            strategy
            for strategy, info in availability.items()
            if info["status"] == "available"
        ]

        if not available_strategies:
            print("\n❌ No datasets available for testing!")
            return 1

        # Filter by user-specified strategies if provided
        if args.strategies:
            available_strategies = [
                s for s in available_strategies if s in args.strategies
            ]
            if not available_strategies:
                print("\n❌ None of the specified strategies are available!")
                return 1

        print(f"\n✅ Found {len(available_strategies)} available datasets")

        # Run benchmark based on mode
        if args.mode == "check":
            print("\n📋 Dataset availability check complete")
            return 0
        elif args.mode == "lightweight":
            results = run_lightweight_benchmark(runner, available_strategies)
        else:  # full
            results = run_full_benchmark(runner, available_strategies)

        # Save results
        output_file = save_benchmark_results(results)

        # Print summary
        print_comparative_summary(results)

        print("\n✅ Benchmark Complete!")
        print(f"   📊 Tested {len(available_strategies)} strategies")
        print(f"   📁 Results saved to: {output_file}")

        return 0

    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
