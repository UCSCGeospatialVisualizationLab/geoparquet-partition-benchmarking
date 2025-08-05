#!/usr/bin/env python3
"""
Create Partitioned Datasets Script
Reads Census tract data from S3 source and creates partitioned datasets for performance testing
"""

import duckdb
import yaml
import pandas as pd
import boto3
import h3
import time
import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

# Load environment variables from .env file
load_dotenv()


class PartitionCreator:
    def __init__(self, config, max_workers=16):
        self.config = config
        self.s3_client = self._get_s3_client()
        self.source_url = config["source"]["url"]
        self.target_bucket = config["test_bucket"]["bucket"]
        self.target_prefix = config["test_bucket"]["prefix"]
        self.max_workers = max_workers

        # Initialize DuckDB with S3 support
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")
        self.conn.execute("INSTALL spatial;")
        self.conn.execute("LOAD spatial;")

        # Configure DuckDB for Ceph S3
        self._configure_duckdb_s3()

        # Thread lock for DuckDB operations (DuckDB is not thread-safe)
        self.db_lock = threading.Lock()

    def _get_s3_client(self):
        """Get S3 client with proper endpoint configuration"""
        endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        if endpoint_url:
            return boto3.client("s3", endpoint_url=endpoint_url)
        else:
            return boto3.client("s3")

    def _configure_duckdb_s3(self):
        """Configure DuckDB for Ceph S3 access"""
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

    def _create_partition_worker(self, partition_data, query_template, target_url):
        """Worker function for creating individual partitions in parallel"""
        try:
            # Create a new DuckDB connection for this thread
            thread_conn = duckdb.connect()
            thread_conn.execute("INSTALL httpfs;")
            thread_conn.execute("LOAD httpfs;")
            thread_conn.execute("INSTALL spatial;")
            thread_conn.execute("LOAD spatial;")

            # Configure S3 for this connection
            endpoint_url = os.getenv("AWS_ENDPOINT_URL")
            if endpoint_url:
                if endpoint_url.startswith("https://"):
                    endpoint_host = endpoint_url.replace("https://", "")
                elif endpoint_url.startswith("http://"):
                    endpoint_host = endpoint_url.replace("http://", "")
                else:
                    endpoint_host = endpoint_url

                thread_conn.execute(f"SET s3_endpoint='{endpoint_host}';")
                thread_conn.execute("SET s3_use_ssl=true;")
                thread_conn.execute("SET s3_url_style='path';")

            # Execute the partition query
            if isinstance(partition_data, pd.DataFrame):
                # For DataFrame-based partitions (H3)
                thread_conn.register("partition_data", partition_data)
                thread_conn.execute(query_template)
            else:
                # For direct SQL queries (state-based)
                query = query_template.format(**partition_data)
                thread_conn.execute(query)

            thread_conn.close()
            return {
                "success": True,
                "target_url": target_url,
                "rows": (
                    len(partition_data)
                    if isinstance(partition_data, pd.DataFrame)
                    else None
                ),
            }

        except Exception as e:
            return {"success": False, "target_url": target_url, "error": str(e)}

    def create_no_partition_dataset(self):
        """Strategy 1: No partitioning - copy source file as-is"""
        print("📄 Creating No Partition Dataset...")

        strategy_path = self.config["partitioning_strategies"]["no_partition"]["path"]
        target_key = f"{self.target_prefix}{strategy_path}hazus_CensusTract.parquet"
        target_url = f"s3://{self.target_bucket}/{target_key}"

        try:
            start_time = time.time()

            # Use DuckDB to copy and potentially reoptimize the file
            copy_query = f"""
            COPY (
                SELECT StateAbbr, Tract, geometry 
                FROM '{self.source_url}'
            ) TO '{target_url}' (FORMAT PARQUET, COMPRESSION 'snappy');
            """

            print(f"   📁 Writing to: {target_url}")
            self.conn.execute(copy_query)

            duration = time.time() - start_time
            print(f"   ✅ Completed in {duration:.2f}s")

            return {
                "strategy": "no_partition",
                "files_created": 1,
                "target_path": target_url,
                "duration_seconds": duration,
            }

        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return None

    def create_attribute_state_dataset(self):
        """Strategy 2: Partition by StateAbbr (Parallelized)"""
        print("🗺️  Creating Attribute State Dataset...")

        strategy_path = self.config["partitioning_strategies"]["attribute_state"][
            "path"
        ]

        try:
            start_time = time.time()

            # Get list of unique states
            states_query = (
                f"SELECT DISTINCT StateAbbr FROM '{self.source_url}' ORDER BY StateAbbr"
            )
            states_df = self.conn.execute(states_query).df()
            states = states_df["StateAbbr"].tolist()

            print(
                f"   📊 Partitioning into {len(states)} state files using {self.max_workers} parallel workers..."
            )

            # Prepare partition tasks
            partition_tasks = []
            for state in states:
                target_key = f"{self.target_prefix}{strategy_path}StateAbbr={state}/hazus_tracts.parquet"
                target_url = f"s3://{self.target_bucket}/{target_key}"

                query_template = f"""
                COPY (
                    SELECT StateAbbr, Tract, geometry 
                    FROM '{self.source_url}' 
                    WHERE StateAbbr = '{state}'
                ) TO '{target_url}' (FORMAT PARQUET, COMPRESSION 'snappy');
                """

                partition_tasks.append(
                    {
                        "partition_data": {"state": state},
                        "query_template": query_template,
                        "target_url": target_url,
                        "state": state,
                    }
                )

            # Execute partitions in parallel
            created_files = []
            failed_files = []

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_task = {
                    executor.submit(
                        self._create_partition_worker,
                        task["partition_data"],
                        task["query_template"],
                        task["target_url"],
                    ): task
                    for task in partition_tasks
                }

                # Process completed tasks
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    result = future.result()

                    if result["success"]:
                        created_files.append(result["target_url"])
                        if len(created_files) <= 5:  # Show first few
                            print(f"      ✅ {task['state']}: {result['target_url']}")
                        elif len(created_files) == 6:
                            print(
                                f"      ... (showing first 5, completing {len(states)} total)"
                            )
                    else:
                        failed_files.append(result)
                        print(f"      ❌ {task['state']}: {result['error']}")

            duration = time.time() - start_time

            if failed_files:
                print(
                    f"   ⚠️  Completed {len(created_files)} files, {len(failed_files)} failed in {duration:.2f}s"
                )
            else:
                print(f"   ✅ Completed {len(created_files)} files in {duration:.2f}s")

            return {
                "strategy": "attribute_state",
                "files_created": len(created_files),
                "files_failed": len(failed_files),
                "target_paths": created_files[:5],  # First 5 for brevity
                "duration_seconds": duration,
            }

        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return None

    def _extract_centroid_coordinates(self):
        """Extract centroid coordinates from geometry for H3 indexing"""
        print("   🎯 Extracting centroid coordinates for H3 indexing...")

        # Create a temporary view with extracted coordinates
        coord_query = f"""
        CREATE OR REPLACE VIEW tract_centroids AS
        SELECT 
            StateAbbr,
            Tract,
            geometry,
            ST_X(ST_Centroid(ST_GeomFromWKB(geometry))) as longitude,
            ST_Y(ST_Centroid(ST_GeomFromWKB(geometry))) as latitude
        FROM '{self.source_url}'
        WHERE geometry IS NOT NULL
        """

        self.conn.execute(coord_query)

        # Get sample to verify coordinates
        sample_query = "SELECT longitude, latitude FROM tract_centroids LIMIT 5"
        sample_df = self.conn.execute(sample_query).df()
        print(
            f"      📍 Sample coordinates: {sample_df[['longitude', 'latitude']].to_dict('records')}"
        )

        return True

    def create_spatial_h3_dataset(self):
        """Strategy 3: Partition by H3 level 6 hexagons"""
        print("🏬 Creating Spatial H3 Dataset...")

        strategy_config = self.config["partitioning_strategies"]["spatial_h3_l6"]
        strategy_path = strategy_config["path"]
        h3_level = strategy_config["h3_level"]

        try:
            start_time = time.time()

            # Extract coordinates for H3 indexing
            if not self._extract_centroid_coordinates():
                return None

            print(f"   🔢 Computing H3 level {h3_level} hexagon indices...")

            # Add H3 column using Python UDF since DuckDB doesn't have native H3
            # First get all data with coordinates
            data_query = """
            SELECT StateAbbr, Tract, geometry, longitude, latitude 
            FROM tract_centroids
            """
            df = self.conn.execute(data_query).df()

            # Add H3 indices
            df["h3_level6"] = df.apply(
                lambda row: (
                    h3.geo_to_h3(row["latitude"], row["longitude"], h3_level)
                    if pd.notna(row["latitude"]) and pd.notna(row["longitude"])
                    else None
                ),
                axis=1,
            )

            # Filter out any records without valid H3 indices
            df = df.dropna(subset=["h3_level6"])

            # Get unique H3 cells
            h3_cells = df["h3_level6"].unique()
            print(f"   📊 Partitioning into {len(h3_cells)} H3 hexagon files...")

            # Prepare partition tasks
            partition_tasks = []
            for h3_cell in h3_cells:
                partition_df = df[df["h3_level6"] == h3_cell][
                    ["StateAbbr", "Tract", "geometry"]
                ]

                if len(partition_df) > 0:
                    target_key = f"{self.target_prefix}{strategy_path}h3_level6={h3_cell}/hazus_tracts.parquet"
                    target_url = f"s3://{self.target_bucket}/{target_key}"

                    query_template = "COPY partition_data TO '{target_url}' (FORMAT PARQUET, COMPRESSION 'snappy');"

                    partition_tasks.append(
                        {
                            "partition_data": partition_df,
                            "query_template": query_template.format(
                                target_url=target_url
                            ),
                            "target_url": target_url,
                            "h3_cell": h3_cell,
                            "row_count": len(partition_df),
                        }
                    )

            print(
                f"   📊 Uploading {len(partition_tasks)} H3 partitions using {self.max_workers} parallel workers..."
            )

            # Execute partitions in parallel
            created_files = []
            failed_files = []

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_task = {
                    executor.submit(
                        self._create_partition_worker,
                        task["partition_data"],
                        task["query_template"],
                        task["target_url"],
                    ): task
                    for task in partition_tasks
                }

                # Process completed tasks
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    result = future.result()

                    if result["success"]:
                        created_files.append(result["target_url"])
                        if len(created_files) <= 5:  # Show first few
                            print(
                                f"      ✅ {task['h3_cell']}: {task['row_count']} tracts -> {result['target_url']}"
                            )
                        elif len(created_files) == 6:
                            print(
                                f"      ... (showing first 5, completing {len(partition_tasks)} total)"
                            )
                    else:
                        failed_files.append(result)
                        print(f"      ❌ {task['h3_cell']}: {result['error']}")

            duration = time.time() - start_time

            if failed_files:
                print(
                    f"   ⚠️  Completed {len(created_files)} files, {len(failed_files)} failed in {duration:.2f}s"
                )
            else:
                print(f"   ✅ Completed {len(created_files)} files in {duration:.2f}s")

            return {
                "strategy": "spatial_h3_l6",
                "files_created": len(created_files),
                "files_failed": len(failed_files),
                "h3_level": h3_level,
                "target_paths": created_files[:5],  # First 5 for brevity
                "duration_seconds": duration,
            }

        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return None

    def create_hybrid_state_h3_dataset(self):
        """Strategy 4: Hybrid partitioning by StateAbbr then H3"""
        print("🔄 Creating Hybrid State+H3 Dataset...")

        strategy_config = self.config["partitioning_strategies"]["hybrid_state_h3"]
        strategy_path = strategy_config["path"]
        h3_level = strategy_config["h3_level"]

        try:
            start_time = time.time()

            # Extract coordinates for H3 indexing
            if not self._extract_centroid_coordinates():
                return None

            print(f"   🔢 Computing H3 level {h3_level} indices by state...")

            # Get all data with coordinates and state info
            data_query = """
            SELECT StateAbbr, Tract, geometry, longitude, latitude 
            FROM tract_centroids
            """
            df = self.conn.execute(data_query).df()

            # Add H3 indices
            df["h3_level6"] = df.apply(
                lambda row: (
                    h3.geo_to_h3(row["latitude"], row["longitude"], h3_level)
                    if pd.notna(row["latitude"]) and pd.notna(row["longitude"])
                    else None
                ),
                axis=1,
            )

            # Filter out any records without valid H3 indices
            df = df.dropna(subset=["h3_level6"])

            # Group by state and H3 cell
            partition_groups = df.groupby(["StateAbbr", "h3_level6"])
            print(f"   📊 Creating {len(partition_groups)} hybrid partitions...")

            # Prepare partition tasks
            partition_tasks = []
            for (state, h3_cell), group_df in partition_groups:
                partition_df = group_df[["StateAbbr", "Tract", "geometry"]]

                target_key = f"{self.target_prefix}{strategy_path}StateAbbr={state}/h3_level6={h3_cell}/hazus_tracts.parquet"
                target_url = f"s3://{self.target_bucket}/{target_key}"

                query_template = "COPY partition_data TO '{target_url}' (FORMAT PARQUET, COMPRESSION 'snappy');"

                partition_tasks.append(
                    {
                        "partition_data": partition_df,
                        "query_template": query_template.format(target_url=target_url),
                        "target_url": target_url,
                        "state": state,
                        "h3_cell": h3_cell,
                        "row_count": len(partition_df),
                    }
                )

            print(
                f"   📊 Uploading {len(partition_tasks)} hybrid partitions using {self.max_workers} parallel workers..."
            )

            # Execute partitions in parallel
            created_files = []
            failed_files = []

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_task = {
                    executor.submit(
                        self._create_partition_worker,
                        task["partition_data"],
                        task["query_template"],
                        task["target_url"],
                    ): task
                    for task in partition_tasks
                }

                # Process completed tasks
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    result = future.result()

                    if result["success"]:
                        created_files.append(result["target_url"])
                        if len(created_files) <= 5:  # Show first few
                            print(
                                f"      ✅ {task['state']}/{task['h3_cell']}: {task['row_count']} tracts -> {result['target_url']}"
                            )
                        elif len(created_files) == 6:
                            print(
                                f"      ... (showing first 5, completing {len(partition_tasks)} total)"
                            )
                    else:
                        failed_files.append(result)
                        print(
                            f"      ❌ {task['state']}/{task['h3_cell']}: {result['error']}"
                        )

            duration = time.time() - start_time

            if failed_files:
                print(
                    f"   ⚠️  Completed {len(created_files)} files, {len(failed_files)} failed in {duration:.2f}s"
                )
            else:
                print(f"   ✅ Completed {len(created_files)} files in {duration:.2f}s")

            return {
                "strategy": "hybrid_state_h3",
                "files_created": len(created_files),
                "files_failed": len(failed_files),
                "h3_level": h3_level,
                "target_paths": created_files[:5],  # First 5 for brevity
                "duration_seconds": duration,
            }

        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return None

    def run_all_strategies(self):
        """Run all partitioning strategies"""
        print("🚀 Creating All Partitioned Datasets")
        print("=" * 50)

        results = []

        strategies = [
            ("no_partition", self.create_no_partition_dataset),
            ("attribute_state", self.create_attribute_state_dataset),
            ("spatial_h3_l6", self.create_spatial_h3_dataset),
            ("hybrid_state_h3", self.create_hybrid_state_h3_dataset),
        ]

        for strategy_name, strategy_func in strategies:
            print(f"\n📋 Strategy: {strategy_name.upper()}")
            result = strategy_func()
            if result:
                results.append(result)
            else:
                print(f"   ⚠️  Strategy {strategy_name} failed")

        return results

    def run_single_strategy(self, strategy_name):
        """Run a single partitioning strategy"""
        print(f"🚀 Creating {strategy_name.upper()} Dataset")
        print("=" * 50)

        strategy_methods = {
            "no_partition": self.create_no_partition_dataset,
            "attribute_state": self.create_attribute_state_dataset,
            "spatial_h3_l6": self.create_spatial_h3_dataset,
            "hybrid_state_h3": self.create_hybrid_state_h3_dataset,
        }

        if strategy_name not in strategy_methods:
            print(f"❌ Unknown strategy: {strategy_name}")
            return None

        return strategy_methods[strategy_name]()


def load_config():
    """Load S3 configuration"""
    config_path = Path(__file__).parent.parent.parent / "s3_config" / "s3_config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def save_partitioning_results(results):
    """Save partitioning results for reference"""
    results_dir = Path(__file__).parent.parent.parent / "results"
    results_dir.mkdir(exist_ok=True)

    partition_data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_strategies": len(results),
        "results": results,
        "total_duration": sum(r.get("duration_seconds", 0) for r in results),
        "total_files_created": sum(r.get("files_created", 0) for r in results),
    }

    output_file = results_dir / "partitioning_results.json"
    with open(output_file, "w") as f:
        json.dump(partition_data, f, indent=2, default=str)

    print(f"\n💾 Results saved to: {output_file}")
    return partition_data


def main():
    """Main partitioning workflow"""
    parser = argparse.ArgumentParser(
        description="Create partitioned datasets for performance testing"
    )
    parser.add_argument(
        "--strategy",
        choices=["no_partition", "attribute_state", "spatial_h3_l6", "hybrid_state_h3"],
        help="Run single strategy (default: run all)",
    )
    parser.add_argument(
        "--all-strategies", action="store_true", help="Run all partitioning strategies"
    )
    parser.add_argument(
        "--workers", type=int, default=8, help="Number of parallel workers (default: 8)"
    )

    args = parser.parse_args()

    # Load configuration
    config = load_config()

    # Create partitioner
    partitioner = PartitionCreator(config, max_workers=args.workers)

    # Run partitioning
    if args.strategy:
        results = [partitioner.run_single_strategy(args.strategy)]
        results = [r for r in results if r is not None]  # Filter out None results
    else:
        results = partitioner.run_all_strategies()

    if results:
        # Save results
        summary = save_partitioning_results(results)

        print("\n✅ Partitioning Complete!")
        print(f"   📊 Strategies completed: {len(results)}")
        print(f"   📁 Total files created: {summary['total_files_created']}")
        print(f"   ⏱️  Total duration: {summary['total_duration']:.2f}s")
        print("\nNext steps:")
        print("1. Validate partitioned datasets")
        print("2. Proceed to Step 5: Analytical Queries")

        return 0
    else:
        print("❌ No partitioning strategies completed successfully")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
