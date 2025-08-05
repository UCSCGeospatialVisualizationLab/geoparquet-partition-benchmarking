# Scripts Directory

This directory contains all the scripts for the parquet partitioning performance testing framework.

## Directory Structure

### `s3_partitioning/`
Scripts to create partitioned datasets in S3 from the source Census tract data.

**Key Files:**
- `create_partitions.py` - Main partitioning script
- `h3_spatial_partitioner.py` - H3 hexagon-based spatial partitioning
- `attribute_partitioner.py` - State-based attribute partitioning  
- `hybrid_partitioner.py` - Combined state + H3 partitioning
- `partition_validator.py` - Validates created partitions

### `analytics/`
DuckDB analytical queries that access S3-hosted partitioned data via HTTP.

**Key Files:**
- `query_runner.py` - Main query execution framework
- `spatial_queries.py` - Point-in-polygon, spatial joins, range queries
- `aggregation_queries.py` - Population aggregations by state/region
- `complex_queries.py` - Multi-condition analytical queries
- `performance_profiler.py` - Query performance measurement

### `visualization/`
HTTP-based client implementations for accessing and visualizing S3 parquet data.

**Key Files:**
- `arrow_client/` - Full HTTP download + Apache Arrow JS/WASM
- `hyparquet_client/` - Incremental columnar streaming client
- `duckdb_wasm_client/` - DuckDB-WASM with HTTP range requests
- `performance_monitor.js` - Client-side performance measurement

### `benchmarks/`
Performance measurement infrastructure with network metrics collection.

**Key Files:**
- `benchmark_runner.py` - Main benchmark execution framework
- `network_profiler.py` - HTTP request patterns and bandwidth monitoring
- `resource_monitor.py` - CPU, memory, and I/O measurement
- `results_collector.py` - Performance data aggregation and storage

## Usage

1. **Set up environment**: Copy `env.template` to `.env` and configure your S3 credentials
2. **Install dependencies**: `pip install -r requirements.txt && npm install`
3. **Create partitions**: Run scripts in `s3_partitioning/` to generate test datasets
4. **Run analytics**: Execute queries via scripts in `analytics/`
5. **Test visualization**: Use clients in `visualization/` for browser-based testing
6. **Measure performance**: Run comprehensive benchmarks via `benchmarks/`

## Next Steps

See `PERFORMANCE_TEST_PLAN.md` for detailed implementation steps and progress tracking.