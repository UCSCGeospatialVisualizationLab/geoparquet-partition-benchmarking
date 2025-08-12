#!/usr/bin/env python3
"""
S3 Data Exploration Script
Analyzes Census tract data structure from S3 to inform partitioning strategies
"""

import duckdb
import yaml
import boto3
import time
from pathlib import Path
import json
import sys
import os
from dotenv import load_dotenv

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

# Load environment variables from .env file
load_dotenv()


def load_config():
    """Load S3 configuration"""
    config_path = Path(__file__).parent.parent.parent / "s3_config" / "s3_config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def test_s3_access(config):
    """Test basic S3 connectivity and access"""
    print("🔍 Testing S3 Access...")

    try:
        # Get endpoint URL from environment variable
        endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        if endpoint_url:
            print(f"   Using Ceph S3 endpoint: {endpoint_url}")
            s3_client = boto3.client("s3", endpoint_url=endpoint_url)
        else:
            print("   Using default AWS S3")
            s3_client = boto3.client("s3")

        # Try to head the object
        source_bucket = config["source"]["bucket"]
        source_key = f"{config['source']['prefix']}{config['source']['file']}"

        print(f"   Checking: s3://{source_bucket}/{source_key}")

        response = s3_client.head_object(Bucket=source_bucket, Key=source_key)
        file_size_bytes = response["ContentLength"]
        file_size_gb = file_size_bytes / (1024**3)

        print("✅ S3 Access OK")
        print(f"   File size: {file_size_gb:.2f} GB ({file_size_bytes:,} bytes)")
        print(f"   Last modified: {response['LastModified']}")

        return True, file_size_gb

    except Exception as e:
        print(f"❌ S3 Access failed: {e}")
        print("   Troubleshooting:")
        print("   - Check AWS_ENDPOINT_URL environment variable")
        print("   - Verify AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
        print("   - Confirm bucket and file path exist")
        return False, 0


def analyze_schema_and_data(config):
    """Analyze parquet schema and data distribution using DuckDB"""
    print("\n📊 Analyzing Data Schema and Distribution...")

    source_url = config["source"]["url"]

    try:
        # Initialize DuckDB with S3 support
        conn = duckdb.connect()
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")

        # Configure DuckDB for Ceph S3 if endpoint is specified
        endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        if endpoint_url:
            print(f"🔗 Configuring DuckDB for Ceph S3 endpoint: {endpoint_url}")
            # Extract hostname from URL for DuckDB (it doesn't want the full URL)
            if endpoint_url.startswith("https://"):
                endpoint_host = endpoint_url.replace("https://", "")
            elif endpoint_url.startswith("http://"):
                endpoint_host = endpoint_url.replace("http://", "")
            else:
                endpoint_host = endpoint_url
                
            # Set S3 endpoint for DuckDB
            conn.execute(f"SET s3_endpoint='{endpoint_host}';")
            conn.execute("SET s3_use_ssl=true;")
            conn.execute("SET s3_url_style='path';")  # Ceph often uses path-style URLs

        print(f"🔗 Connecting to: {source_url}")

        # Test basic connectivity with LIMIT
        print("   Testing connection...")
        test_query = f"SELECT COUNT(*) as total_records FROM '{source_url}'"
        start_time = time.time()
        result = conn.execute(test_query).fetchone()
        query_time = time.time() - start_time

        total_records = result[0]
        print(f"   ✅ Connection successful ({query_time:.2f}s)")
        print(f"   📊 Total records: {total_records:,}")

        # Get schema information
        print("\n📋 Schema Analysis:")
        schema_query = f"DESCRIBE SELECT * FROM '{source_url}' LIMIT 1"
        schema_df = conn.execute(schema_query).df()

        print(f"   Columns: {len(schema_df)}")
        for _, row in schema_df.iterrows():
            print(f"   - {row['column_name']}: {row['column_type']}")

        # Sample a few records to understand data structure
        print("\n🔬 Data Sample:")
        sample_query = f"SELECT * FROM '{source_url}' LIMIT 3"
        sample_df = conn.execute(sample_query).df()
        print(sample_df.head())

        # Analyze StateAbbr distribution (key for partitioning)
        print("\n🗺️  State Code Distribution:")
        state_query = f"""
        SELECT 
            StateAbbr,
            COUNT(*) as tract_count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
        FROM '{source_url}' 
        GROUP BY StateAbbr 
        ORDER BY tract_count DESC
        LIMIT 10
        """

        state_df = conn.execute(state_query).df()
        print(state_df.to_string(index=False))

        # Get total state count for partitioning planning
        total_states_query = (
            f"SELECT COUNT(DISTINCT StateAbbr) as state_count FROM '{source_url}'"
        )
        total_states = conn.execute(total_states_query).fetchone()[0]
        print(f"\n   📍 Total unique states/territories: {total_states}")

        # Estimate records per state (for partition sizing)
        avg_per_state = total_records / total_states
        print(f"   📏 Average records per state: {avg_per_state:,.0f}")

        # Try to analyze spatial bounds (if geometry column exists)
        print("\n🌍 Spatial Analysis:")
        try:
            # Check if we have geometry/spatial columns
            spatial_columns = []
            for _, row in schema_df.iterrows():
                col_name = row["column_name"].lower()
                if any(
                    spatial_term in col_name
                    for spatial_term in [
                        "geom",
                        "shape",
                        "coord",
                        "lat",
                        "lon",
                        "point",
                    ]
                ):
                    spatial_columns.append(row["column_name"])

            if spatial_columns:
                print(f"   🗺️  Found spatial columns: {spatial_columns}")

                # If we have lat/lon columns, analyze spatial distribution
                if any("lat" in col.lower() for col in spatial_columns) and any(
                    "lon" in col.lower() for col in spatial_columns
                ):
                    bounds_query = f"""
                    SELECT 
                        MIN(longitude) as min_lon, MAX(longitude) as max_lon,
                        MIN(latitude) as min_lat, MAX(latitude) as max_lat
                    FROM '{source_url}'
                    """
                    try:
                        bounds_df = conn.execute(bounds_query).df()
                        print("   📐 Spatial bounds:")
                        print(
                            f"      Longitude: {bounds_df['min_lon'].iloc[0]:.3f} to {bounds_df['max_lon'].iloc[0]:.3f}"
                        )
                        print(
                            f"      Latitude: {bounds_df['min_lat'].iloc[0]:.3f} to {bounds_df['max_lat'].iloc[0]:.3f}"
                        )
                    except Exception as spatial_e:
                        print(f"   ⚠️  Could not determine spatial bounds: {spatial_e}")
            else:
                print("   ℹ️  No obvious spatial columns found")

        except Exception as e:
            print(f"   ⚠️  Spatial analysis failed: {e}")

        return {
            "total_records": total_records,
            "total_states": total_states,
            "avg_records_per_state": avg_per_state,
            "schema": schema_df.to_dict("records"),
            "state_distribution": state_df.to_dict("records"),
            "query_time": query_time,
        }

    except Exception as e:
        print(f"❌ Data analysis failed: {e}")
        return None


def test_http_performance(config):
    """Test HTTP range request performance"""
    print("\n🚀 Testing HTTP Performance...")

    source_url = config["source"]["url"]

    try:
        conn = duckdb.connect()
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")

        # Configure DuckDB for Ceph S3 if endpoint is specified
        endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        if endpoint_url:
            print(f"   Configuring DuckDB for Ceph S3: {endpoint_url}")
            # Extract hostname from URL for DuckDB (it doesn't want the full URL)
            if endpoint_url.startswith("https://"):
                endpoint_host = endpoint_url.replace("https://", "")
            elif endpoint_url.startswith("http://"):
                endpoint_host = endpoint_url.replace("http://", "")
            else:
                endpoint_host = endpoint_url
                
            conn.execute(f"SET s3_endpoint='{endpoint_host}';")
            conn.execute("SET s3_use_ssl=true;")
            conn.execute("SET s3_url_style='path';")

        # Test different query patterns to understand HTTP behavior
        tests = [
            (
                "Small selective query",
                f"SELECT StateAbbr, COUNT(*) FROM '{source_url}' WHERE StateAbbr = 'CA' GROUP BY StateAbbr",
            ),
            ("Metadata query", f"SELECT COUNT(*) FROM '{source_url}'"),
            ("Limited scan", f"SELECT * FROM '{source_url}' LIMIT 1000"),
        ]

        results = []
        for test_name, query in tests:
            print(f"   🧪 {test_name}...")
            start_time = time.time()
            try:
                result = conn.execute(query).fetchdf()
                duration = time.time() - start_time
                results.append(
                    {
                        "test": test_name,
                        "duration": duration,
                        "rows_returned": len(result),
                    }
                )
                print(f"      ✅ {duration:.2f}s ({len(result)} rows)")
            except Exception as e:
                print(f"      ❌ Failed: {e}")
                results.append({"test": test_name, "duration": None, "error": str(e)})

        return results

    except Exception as e:
        print(f"❌ HTTP performance testing failed: {e}")
        return []


def calculate_partition_estimates(analysis_results, config):
    """Calculate estimated partition sizes for different strategies"""
    print("\n📏 Partition Size Estimates...")

    if not analysis_results:
        print("❌ No analysis data available")
        return

    total_records = analysis_results["total_records"]
    total_states = analysis_results["total_states"]

    # Assume average record size (rough estimate for census tract data)
    estimated_record_size_bytes = 2048  # 2KB per record estimate
    total_size_bytes = total_records * estimated_record_size_bytes
    total_size_mb = total_size_bytes / (1024**2)

    print("   📊 Dataset size estimates:")
    print(f"      Total records: {total_records:,}")
    print(f"      Estimated record size: {estimated_record_size_bytes:,} bytes")
    print(f"      Estimated total size: {total_size_mb:.1f} MB")

    strategies = config["partitioning_strategies"]

    for strategy_name, strategy_config in strategies.items():
        print(f"\n   📋 {strategy_name.upper()} Strategy:")
        target_size_mb = strategy_config["target_file_size_mb"]

        if strategy_name == "no_partition":
            print("      Files: 1")
            print(f"      Size per file: ~{total_size_mb:.1f} MB")

        elif strategy_name == "attribute_state":
            avg_records_per_state = total_records / total_states
            estimated_size_per_state = (
                avg_records_per_state * estimated_record_size_bytes
            ) / (1024**2)
            print(f"      Files: ~{total_states}")
            print(f"      Size per file: ~{estimated_size_per_state:.1f} MB")
            print(f"      Target size: {target_size_mb} MB")

        elif strategy_name == "spatial_h3_l3":
            # H3 Level 6 has ~4,992,676 hexagons globally, but we'll estimate based on target size
            estimated_files = int(total_size_mb / target_size_mb)
            records_per_file = total_records / estimated_files
            print(f"      Estimated files: ~{estimated_files}")
            print(f"      Records per file: ~{records_per_file:.0f}")
            print(f"      Target size: {target_size_mb} MB per file")

        elif strategy_name == "hybrid_state_h3":
            # Estimate sub-partitions per state
            estimated_size_per_state = (
                total_records / total_states * estimated_record_size_bytes
            ) / (1024**2)
            sub_partitions_per_state = max(
                1, int(estimated_size_per_state / target_size_mb)
            )
            total_files = total_states * sub_partitions_per_state
            print(f"      Files: ~{total_files} ({sub_partitions_per_state} per state)")
            print(f"      Size per file: ~{target_size_mb} MB")


def save_exploration_results(analysis_results, performance_results, file_size_gb):
    """Save exploration results to JSON for later reference"""

    results_dir = Path(__file__).parent.parent.parent / "results"
    results_dir.mkdir(exist_ok=True)

    exploration_data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source_file_size_gb": file_size_gb,
        "analysis": analysis_results,
        "performance_tests": performance_results,
    }

    output_file = results_dir / "s3_data_exploration.json"
    with open(output_file, "w") as f:
        json.dump(exploration_data, f, indent=2, default=str)

    print(f"\n💾 Results saved to: {output_file}")


def main():
    """Main exploration workflow"""
    print("🚀 S3 Data Exploration - Census Tract Dataset")
    print("=" * 50)

    # Load configuration
    config = load_config()
    print(f"📁 Source: {config['source']['url']}")

    # Test S3 access
    s3_ok, file_size_gb = test_s3_access(config)
    if not s3_ok:
        print("❌ Cannot proceed without S3 access. Check your AWS credentials.")
        return 1

    # Analyze data structure
    analysis_results = analyze_schema_and_data(config)
    if not analysis_results:
        print("❌ Data analysis failed")
        return 1

    # Test HTTP performance
    performance_results = test_http_performance(config)

    # Calculate partition estimates
    calculate_partition_estimates(analysis_results, config)

    # Save results
    save_exploration_results(analysis_results, performance_results, file_size_gb)

    print("\n✅ S3 Data Exploration Complete!")
    print("\nNext steps:")
    print("1. Review the analysis results")
    print("2. Adjust partitioning parameters in s3_config/s3_config.yaml if needed")
    print("3. Proceed to Step 3: S3 Partitioning Setup")

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
