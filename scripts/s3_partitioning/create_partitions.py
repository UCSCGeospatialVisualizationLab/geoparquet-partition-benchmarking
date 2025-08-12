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


# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

# Load environment variables from .env file
load_dotenv()


class PartitionCreator:
    def __init__(self, config):
        self.config = config
        self.s3_client = self._get_s3_client()
        self.source_url = config["source"]["url"]
        self.target_bucket = config["test_bucket"]["bucket"]
        self.target_prefix = config["test_bucket"]["prefix"]

        # Initialize DuckDB with S3 support
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")
        self.conn.execute("INSTALL spatial;")
        self.conn.execute("LOAD spatial;")

        # Enable progress bar for long-running operations
        self.conn.execute("SET enable_progress_bar = true;")

        # Configure DuckDB for Ceph S3
        self._configure_duckdb_s3()

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
        """Strategy 2: Partition by StateAbbr using DuckDB Hive partitioning"""
        print("🗺️  Creating Attribute State Dataset with Hive Partitioning...")

        strategy_path = self.config["partitioning_strategies"]["attribute_state"][
            "path"
        ]
        target_path = f"s3://{self.target_bucket}/{self.target_prefix}{strategy_path}"

        try:
            start_time = time.time()

            print("   📊 Using DuckDB PARTITION_BY for Hive partitioning...")
            print(f"   📁 Target path: {target_path}")

            # Use DuckDB's native Hive partitioning with PARTITION_BY
            copy_query = f"""
            COPY (
                SELECT StateAbbr, Tract, geometry 
                FROM '{self.source_url}'
            ) TO '{target_path}' (
                FORMAT PARQUET,
                COMPRESSION 'snappy',
                PARTITION_BY (StateAbbr),
                OVERWRITE_OR_IGNORE
            );
            """

            print("   🚀 Executing Hive partitioned write...")
            self.conn.execute(copy_query)

            duration = time.time() - start_time
            print(f"   ✅ Completed Hive partitioning in {duration:.2f}s")

            # Count partitions created (estimate based on unique states)
            count_query = f"SELECT COUNT(DISTINCT StateAbbr) as state_count FROM '{self.source_url}'"
            state_count = self.conn.execute(count_query).fetchone()[0]

            return {
                "strategy": "attribute_state",
                "partitioning_type": "hive",
                "partition_column": "StateAbbr",
                "estimated_partitions": state_count,
                "target_path": target_path,
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
        """Strategy 3: Partition by H3 hexagons using DuckDB Hive partitioning"""
        print("🏬 Creating Spatial H3 Dataset with Hive Partitioning...")

        strategy_config = self.config["partitioning_strategies"]["spatial_h3_l3"]
        strategy_path = strategy_config["path"]
        h3_level = strategy_config["h3_level"]
        target_path = f"s3://{self.target_bucket}/{self.target_prefix}{strategy_path}"

        try:
            start_time = time.time()

            # Extract coordinates for H3 indexing
            if not self._extract_centroid_coordinates():
                return None

            print(f"   🔢 Computing H3 level {h3_level} hexagon indices...")

            # Get all data with coordinates
            data_query = """
            SELECT StateAbbr, Tract, geometry, longitude, latitude 
            FROM tract_centroids
            """
            df = self.conn.execute(data_query).df()

            # Add H3 indices
            h3_column = f"h3_level{h3_level}"
            print(f"   🔢 Computing {len(df):,} H3 level {h3_level} indices...")

            df[h3_column] = df.apply(
                lambda row: (
                    h3.geo_to_h3(row["latitude"], row["longitude"], h3_level)
                    if pd.notna(row["latitude"]) and pd.notna(row["longitude"])
                    else None
                ),
                axis=1,
            )

            # Filter out any records without valid H3 indices
            df = df.dropna(subset=[h3_column])
            print(f"   ✅ Generated {len(df):,} valid H3 indices")

            # Get unique H3 cells for reporting
            h3_cells = df[h3_column].unique()
            print(
                f"   📊 Will create {len(h3_cells)} H3 hexagon partitions using Hive partitioning..."
            )

            # Register the DataFrame with DuckDB for partitioned write
            self.conn.register(
                "h3_partitioned_data", df[["StateAbbr", "Tract", "geometry", h3_column]]
            )

            print(f"   📁 Target path: {target_path}")
            print("   🚀 Executing Hive partitioned write...")

            # Use DuckDB's native Hive partitioning with PARTITION_BY
            copy_query = f"""
            COPY (
                SELECT StateAbbr, Tract, geometry, {h3_column}
                FROM h3_partitioned_data
            ) TO '{target_path}' (
                FORMAT PARQUET,
                COMPRESSION 'snappy',
                PARTITION_BY ({h3_column}),
                OVERWRITE_OR_IGNORE
            );
            """

            self.conn.execute(copy_query)

            duration = time.time() - start_time
            print(f"   ✅ Completed Hive partitioning in {duration:.2f}s")

            return {
                "strategy": "spatial_h3_l3",
                "partitioning_type": "hive",
                "partition_column": h3_column,
                "h3_level": h3_level,
                "estimated_partitions": len(h3_cells),
                "target_path": target_path,
                "duration_seconds": duration,
            }

        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return None

    def create_hybrid_state_h3_dataset(self):
        """Strategy 4: Hybrid partitioning by StateAbbr then H3 using DuckDB Hive partitioning"""
        print("🔄 Creating Hybrid State+H3 Dataset with Hive Partitioning...")

        strategy_config = self.config["partitioning_strategies"]["hybrid_state_h3"]
        strategy_path = strategy_config["path"]
        h3_level = strategy_config["h3_level"]
        target_path = f"s3://{self.target_bucket}/{self.target_prefix}{strategy_path}"

        try:
            start_time = time.time()

            # Extract coordinates for H3 indexing
            if not self._extract_centroid_coordinates():
                return None

            print(
                f"   🔢 Computing H3 level {h3_level} indices for hybrid partitioning..."
            )

            # Get all data with coordinates and state info
            data_query = """
            SELECT StateAbbr, Tract, geometry, longitude, latitude 
            FROM tract_centroids
            """
            df = self.conn.execute(data_query).df()

            # Add H3 indices
            h3_column = f"h3_level{h3_level}"
            print(f"   🔢 Computing {len(df):,} H3 level {h3_level} indices...")

            df[h3_column] = df.apply(
                lambda row: (
                    h3.geo_to_h3(row["latitude"], row["longitude"], h3_level)
                    if pd.notna(row["latitude"]) and pd.notna(row["longitude"])
                    else None
                ),
                axis=1,
            )

            # Filter out any records without valid H3 indices
            df = df.dropna(subset=[h3_column])
            print(f"   ✅ Generated {len(df):,} valid H3 indices")

            # Get unique combinations for reporting
            partition_groups = df.groupby(["StateAbbr", h3_column]).size()
            print(
                f"   📊 Will create {len(partition_groups)} hybrid partitions using Hive partitioning..."
            )

            # Register the DataFrame with DuckDB for partitioned write
            self.conn.register(
                "hybrid_partitioned_data",
                df[["StateAbbr", "Tract", "geometry", h3_column]],
            )

            print(f"   📁 Target path: {target_path}")
            print("   🚀 Executing Hive partitioned write...")

            # Use DuckDB's native Hive partitioning with PARTITION_BY (multiple columns)
            copy_query = f"""
            COPY (
                SELECT StateAbbr, Tract, geometry, {h3_column}
                FROM hybrid_partitioned_data
            ) TO '{target_path}' (
                FORMAT PARQUET,
                COMPRESSION 'snappy',
                PARTITION_BY (StateAbbr, {h3_column}),
                OVERWRITE_OR_IGNORE
            );
            """

            self.conn.execute(copy_query)

            duration = time.time() - start_time
            print(f"   ✅ Completed Hive partitioning in {duration:.2f}s")

            return {
                "strategy": "hybrid_state_h3",
                "partitioning_type": "hive",
                "partition_columns": ["StateAbbr", h3_column],
                "h3_level": h3_level,
                "estimated_partitions": len(partition_groups),
                "target_path": target_path,
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
            ("spatial_h3_l3", self.create_spatial_h3_dataset),
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
            "spatial_h3_l3": self.create_spatial_h3_dataset,
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
        choices=["no_partition", "attribute_state", "spatial_h3_l3", "hybrid_state_h3"],
        help="Run single strategy (default: run all)",
    )
    parser.add_argument(
        "--all-strategies", action="store_true", help="Run all partitioning strategies"
    )

    args = parser.parse_args()

    # Load configuration
    config = load_config()

    # Create partitioner
    partitioner = PartitionCreator(config)

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
