#!/usr/bin/env python3
"""
Analytical Queries for Parquet Performance Testing

This module defines analytical queries that will be executed against different
partitioning strategies to measure performance characteristics.

Each query represents a common geospatial analytics pattern:
1. State-level aggregation (benefits from state partitioning)
2. Spatial filtering (benefits from H3 partitioning)
3. Cross-partition joins (tests partition pruning)
4. Full table scan (baseline performance)
5. Complex spatial-attribute filtering (benefits from hybrid partitioning)
"""

import duckdb
import time
import psutil
import os
import json
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv
import yaml
import multiprocessing


def _execute_query_worker(query: str, result_queue: "multiprocessing.Queue") -> None:
    """Execute a DuckDB query in an isolated subprocess and return results via queue.

    Sends a small payload to avoid large IPC transfers:
    - success: bool
    - rows: list (entire result set) on success
    - error: str on failure
    """
    try:
        # Local imports inside subprocess context
        import duckdb  # noqa: WPS433 (local import by design for subprocess)
        import os as _os

        conn = duckdb.connect()

        # Configure DuckDB and S3 in the subprocess
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
        conn.execute("SET enable_progress_bar = true;")

        endpoint_url = _os.getenv("AWS_ENDPOINT_URL")
        if endpoint_url:
            if endpoint_url.startswith("https://"):
                endpoint_host = endpoint_url.replace("https://", "")
            elif endpoint_url.startswith("http://"):
                endpoint_host = endpoint_url.replace("http://", "")
            else:
                endpoint_host = endpoint_url

            conn.execute(f"SET s3_endpoint='{endpoint_host}';")
            conn.execute("SET s3_use_ssl=true;")
            conn.execute("SET s3_url_style='path';")

        rows = conn.execute(query).fetchall()
        result_queue.put({"success": True, "rows": rows})
    except Exception as exc:  # pragma: no cover - defensive
        result_queue.put({"success": False, "error": str(exc)})
    finally:
        try:
            conn.close()
        except Exception:  # pragma: no cover - best effort
            pass


# Load environment variables
load_dotenv()


class AnalyticalQueryRunner:
    """Executes analytical queries against partitioned datasets"""

    def __init__(
        self,
        config_path: str = "s3_config/s3_config.yaml",
        query_timeout_seconds: int = 300,
    ):
        self.config = self._load_config(config_path)
        self.conn = duckdb.connect()
        self._setup_duckdb()
        self.query_timeout_seconds = query_timeout_seconds

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    def _setup_duckdb(self):
        """Configure DuckDB for S3 access"""
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")
        self.conn.execute("INSTALL spatial;")
        self.conn.execute("LOAD spatial;")

        # Enable progress bar for long-running operations
        self.conn.execute("SET enable_progress_bar = true;")

        # Configure S3 endpoint for Ceph
        endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        if endpoint_url:
            if endpoint_url.startswith("https://"):
                endpoint_host = endpoint_url.replace("https://", "")
            elif endpoint_url.startswith("http://"):
                endpoint_host = endpoint_url.replace("http://", "")
            else:
                endpoint_host = endpoint_url

            self.conn.execute(f"SET s3_endpoint='{endpoint_host}';")
            self.conn.execute("SET s3_use_ssl=true;")
            self.conn.execute("SET s3_url_style='path';")

    def _get_dataset_path(self, strategy: str) -> str:
        """Get the S3 path for a partitioning strategy"""
        bucket = self.config["test_bucket"]["bucket"]
        prefix = self.config["test_bucket"]["prefix"]
        strategy_config = self.config["partitioning_strategies"][strategy]

        if strategy == "no_partition":
            return f"s3://{bucket}/{prefix}{strategy_config['path']}hazus_CensusTract.parquet"
        else:
            # For partitioned datasets, use wildcard to read all partitions
            return f"s3://{bucket}/{prefix}{strategy_config['path']}**/*.parquet"

    def _measure_performance(self, query: str, description: str) -> Dict[str, Any]:
        """Execute query with a timeout and measure performance metrics."""
        process_info = psutil.Process()
        initial_memory_mb = process_info.memory_info().rss / 1024 / 1024
        start_time = time.time()

        # Execute the DuckDB query in a separate process for hard timeout enforcement
        result_queue: multiprocessing.Queue = multiprocessing.Queue()
        worker = multiprocessing.Process(
            target=_execute_query_worker, args=(query, result_queue)
        )
        worker.start()
        worker.join(self.query_timeout_seconds)

        # Handle timeout
        if worker.is_alive():
            worker.terminate()
            worker.join(2)
            return {
                "success": False,
                "description": description,
                "error": f"Query exceeded timeout of {self.query_timeout_seconds}s",
                "timed_out": True,
                "execution_time_seconds": time.time() - start_time,
            }

        # Collect worker result
        try:
            payload = result_queue.get_nowait()
        except Exception as exc:  # pragma: no cover - defensive
            payload = {"success": False, "error": f"No result from worker: {exc}"}

        end_time = time.time()
        final_memory_mb = process_info.memory_info().rss / 1024 / 1024
        cpu_percent_avg = process_info.cpu_percent()

        if payload.get("success"):
            rows = payload.get("rows", [])
            return {
                "success": True,
                "description": description,
                "execution_time_seconds": end_time - start_time,
                "memory_peak_mb": final_memory_mb,
                "memory_delta_mb": final_memory_mb - initial_memory_mb,
                "cpu_percent_avg": cpu_percent_avg,
                "result_rows": len(rows),
                "result_sample": rows[:3] if rows else [],
            }
        else:
            return {
                "success": False,
                "description": description,
                "error": payload.get("error", "Unknown error"),
                "execution_time_seconds": end_time - start_time,
            }

    def query_1_state_aggregation(self, dataset_path: str) -> Dict[str, Any]:
        """Query 1: State-level aggregation (benefits from state partitioning)"""
        query = f"""
        SELECT
            StateAbbr,
            COUNT(*) as tract_count,
            COUNT(*) * 100.0 / (SELECT COUNT(*) FROM '{dataset_path}') as percentage
        FROM '{dataset_path}'
        GROUP BY StateAbbr
        ORDER BY tract_count DESC
        LIMIT 10;
        """

        return self._measure_performance(
            query, "State-level aggregation - count tracts by state"
        )

    def query_2_spatial_filtering(self, dataset_path: str) -> Dict[str, Any]:
        """Query 2: Simple spatial filtering (benefits from spatial partitioning)"""
        # Simple area-based filtering instead of complex spatial operations
        query = f"""
        SELECT
            StateAbbr,
            Tract,
            ST_Area(ST_GeomFromWKB(geometry)) as area
        FROM '{dataset_path}'
        WHERE StateAbbr = 'CA'
        AND ST_Area(ST_GeomFromWKB(geometry)) > 0.001
        ORDER BY area DESC
        LIMIT 100;
        """

        return self._measure_performance(
            query, "Simple spatial filtering - large California tracts"
        )

    def query_3_multi_state_analysis(self, dataset_path: str) -> Dict[str, Any]:
        """Query 3: Multi-state analysis (tests partition pruning)"""
        query = f"""
        SELECT
            StateAbbr,
            COUNT(*) as tract_count
        FROM '{dataset_path}'
        WHERE StateAbbr IN ('CA', 'TX', 'FL', 'NY', 'PA')
        GROUP BY StateAbbr
        ORDER BY tract_count DESC;
        """

        return self._measure_performance(
            query, "Multi-state analysis - top 5 populous states"
        )

    def query_4_full_table_scan(self, dataset_path: str) -> Dict[str, Any]:
        """Query 4: Full table scan (baseline performance test)"""
        query = f"""
        SELECT
            COUNT(*) as total_tracts,
            COUNT(DISTINCT StateAbbr) as unique_states
        FROM '{dataset_path}';
        """

        return self._measure_performance(query, "Full table scan - basic counts")

    def query_5_complex_spatial_attribute(self, dataset_path: str) -> Dict[str, Any]:
        """Query 5: Multi-condition filtering (benefits from hybrid partitioning)"""
        query = f"""
        SELECT
            StateAbbr,
            COUNT(*) as tract_count,
            AVG(ST_Area(ST_GeomFromWKB(geometry))) as avg_area
        FROM '{dataset_path}'
        WHERE StateAbbr IN ('CA', 'NV', 'OR', 'WA')
        AND ST_Area(ST_GeomFromWKB(geometry)) > 0.001
        GROUP BY StateAbbr
        ORDER BY avg_area DESC;
        """

        return self._measure_performance(
            query, "Multi-condition filtering - West Coast states with large tracts"
        )

    def run_all_queries(self, strategy: str) -> Dict[str, Any]:
        """Run all analytical queries against a specific partitioning strategy"""
        print(f"🔍 Running analytical queries against {strategy.upper()} strategy...")

        dataset_path = self._get_dataset_path(strategy)
        print(f"   📁 Dataset path: {dataset_path}")

        queries = [
            ("query_1_state_aggregation", self.query_1_state_aggregation),
            ("query_2_spatial_filtering", self.query_2_spatial_filtering),
            ("query_3_multi_state_analysis", self.query_3_multi_state_analysis),
            ("query_4_full_table_scan", self.query_4_full_table_scan),
            (
                "query_5_complex_spatial_attribute",
                self.query_5_complex_spatial_attribute,
            ),
        ]

        results = {
            "strategy": strategy,
            "dataset_path": dataset_path,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "queries": {},
        }

        for query_name, query_func in queries:
            print(f"   🚀 Executing {query_name}...")

            try:
                query_result = query_func(dataset_path)
                results["queries"][query_name] = query_result

                if query_result["success"]:
                    print(
                        f"      ✅ Completed in {query_result['execution_time_seconds']:.2f}s "
                        f"({query_result['result_rows']} rows)"
                    )
                else:
                    print(f"      ❌ Failed: {query_result['error']}")

            except Exception as e:
                print(f"      ❌ Exception: {e}")
                results["queries"][query_name] = {
                    "success": False,
                    "error": str(e),
                    "description": f"Failed to execute {query_name}",
                }

        # Calculate summary statistics
        successful_queries = [q for q in results["queries"].values() if q["success"]]
        if successful_queries:
            results["summary"] = {
                "total_queries": len(queries),
                "successful_queries": len(successful_queries),
                "total_execution_time": sum(
                    q["execution_time_seconds"] for q in successful_queries
                ),
                "avg_execution_time": sum(
                    q["execution_time_seconds"] for q in successful_queries
                )
                / len(successful_queries),
                "max_memory_usage": max(
                    q.get("memory_peak_mb", 0) for q in successful_queries
                ),
                "total_result_rows": sum(
                    q.get("result_rows", 0) for q in successful_queries
                ),
            }

        return results

    def benchmark_all_strategies(self) -> Dict[str, Any]:
        """Run analytical queries against all partitioning strategies"""
        print("🎯 Starting Analytical Query Benchmark")
        print("=" * 50)

        strategies = [
            "no_partition",
            "attribute_state",
            "spatial_h3_l3",
            "hybrid_state_h3",
        ]
        all_results = {
            "benchmark_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "strategies": {},
        }

        for strategy in strategies:
            print(f"\n📊 Strategy: {strategy.upper()}")
            try:
                strategy_results = self.run_all_queries(strategy)
                all_results["strategies"][strategy] = strategy_results

                if "summary" in strategy_results:
                    summary = strategy_results["summary"]
                    print(
                        f"   📈 Summary: {summary['successful_queries']}/{summary['total_queries']} queries succeeded"
                    )
                    print(f"   ⏱️  Total time: {summary['total_execution_time']:.2f}s")
                    print(f"   💾 Peak memory: {summary['max_memory_usage']:.1f}MB")

            except Exception as e:
                print(f"   ❌ Strategy failed: {e}")
                all_results["strategies"][strategy] = {
                    "success": False,
                    "error": str(e),
                }

        return all_results


def save_benchmark_results(results: Dict[str, Any], output_dir: str = "results"):
    """Save benchmark results to JSON file"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filename = f"analytical_benchmark_{timestamp}.json"
    filepath = output_path / filename

    with open(filepath, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\n💾 Benchmark results saved to: {filepath}")
    return filepath


def main():
    """Main analytical query benchmark"""
    print("🚀 Parquet Performance Testing - Analytical Queries")
    print("=" * 60)

    try:
        # Create query runner
        runner = AnalyticalQueryRunner()

        # Run benchmark
        results = runner.benchmark_all_strategies()

        # Save results
        output_file = save_benchmark_results(results)

        # Print summary
        print("\n✅ Analytical Query Benchmark Complete!")
        print(f"   📊 Tested {len(results['strategies'])} partitioning strategies")
        print(f"   📁 Results saved to: {output_file}")

        # Print comparative summary
        print("\n📈 Performance Summary:")
        for strategy, strategy_results in results["strategies"].items():
            if "summary" in strategy_results:
                summary = strategy_results["summary"]
                print(
                    f"   {strategy:20s}: {summary['total_execution_time']:6.2f}s total, "
                    f"{summary['max_memory_usage']:6.1f}MB peak"
                )

        return 0

    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
