# Visualization Clients Performance Testing

This directory contains the implementation for **all three visualization clients** as part of the parquet partitioning performance testing framework:

1. **Client 1**: Arrow.js + parquet-wasm (Full HTTP download + real Parquet parsing)
2. **Client 2**: Hyparquet (Incremental streaming)
3. **Client 3**: DuckDB-WASM (SQL queries with HTTP range requests)

## 🎉 All Clients Fully Functional

## Overview

All three clients test selective spatial loading across all 4 partitioning strategies, each with different data access approaches:

### Client 1: Arrow.js + parquet-wasm
- **NO_PARTITION**: Gracefully skipped due to 1.6GB file size limitations
- **ATTRIBUTE_STATE**: Downloads CA partition + parquet-wasm parsing + Arrow processing
- **SPATIAL_H3_L3**: Downloads H3 hexagons + parallel processing + memory monitoring  
- **HYBRID_STATE_H3**: Downloads CA/H3 sub-partitions + efficient combining

### Client 2: Hyparquet Streaming
- **NO_PARTITION**: Progressive streaming with early termination
- **ATTRIBUTE_STATE**: Single file streaming with row group processing
- **SPATIAL_H3_L3**: Parallel hexagon streaming + concurrent batch processing
- **HYBRID_STATE_H3**: Hierarchical streaming with state/spatial optimization

### Client 3: DuckDB-WASM SQL
- **NO_PARTITION**: Direct SQL on large file with HTTP range requests
- **ATTRIBUTE_STATE**: Optimized single-file SQL with spatial WHERE clauses
- **SPATIAL_H3_L3**: UNION queries across multiple H3 partitions
- **HYBRID_STATE_H3**: Hierarchical SQL with state and spatial filtering

## Test Region

**California Bay Area**: `west: -122.6, south: 37.2, east: -121.8, north: 38.0`

## Parquet-WASM Integration

This client now uses **actual Parquet file parsing** via the `parquet-wasm` library:

- ✅ **Real Parquet Parsing**: Downloads and parses actual Parquet files from S3
- ✅ **WASM Performance**: Native-speed Parquet decoding in the browser  
- ✅ **Arrow Integration**: Converts parsed data to Arrow format for processing
- ✅ **Fallback Support**: Falls back to mock data if parsing fails
- ✅ **Performance Metrics**: Tracks both Parquet parsing time and Arrow processing time

### Key Performance Measurements:
- **HTTP Download Time**: Time to download Parquet file from S3
- **Parquet Parse Time**: Time for parquet-wasm to decode the file
- **Parquet → Arrow Convert Time**: Time to convert to Arrow format
- **Arrow Processing Time**: Time for spatial filtering and analysis
- **Memory Usage**: Peak memory during parsing and processing
- **File Size vs Rows**: Efficiency of different partition strategies

## Files

### Client 1: Arrow.js + parquet-wasm
- `arrow_client.html` - Browser-based test client with real Parquet parsing
- `js/arrow_loader.js` - Core Arrow.js + parquet-wasm integration logic
- `js/partition_strategies.js` - Strategy-specific data loading with graceful skipping
- `js/performance_metrics.js` - HTTP, memory, and processing metrics

### Client 2: Hyparquet Streaming  
- `hyparquet_client.html` - Browser-based streaming test client
- `js/hyparquet_loader.js` - Progressive streaming and early termination logic
- `js/hyparquet_strategies.js` - Strategy-optimized streaming configurations
- `js/streaming_metrics.js` - Streaming efficiency and batch processing metrics

### Client 3: DuckDB-WASM SQL
- `duckdb_client.html` - Browser-based SQL query interface (✅ **Now Working!**)
- `js/duckdb_loader.js` - SQL execution and connection management
- `js/duckdb_strategies.js` - SQL query strategies with spatial predicates  
- `js/range_metrics.js` - HTTP range request and query performance metrics
- `js/duckdb-wasm/` - Local WASM files to avoid CORS issues

### Automation & Testing
- `*_cli_runner.js` - CLI scripts for automated testing of each client

## Browser Usage

1. **Install dependencies**:
   ```bash
   cd ../../
   npm install  # Installs Arrow.js, Hyparquet, DuckDB-WASM, and parquet-wasm
   ```

2. **Serve files over HTTP** (required for ES modules):
   ```bash
   npx serve scripts/visualization -p 3000
   ```

3. **Open clients in browser**:
   ```bash
   # Client 1: Arrow.js + parquet-wasm
   open http://localhost:3000/arrow_client
   
   # Client 2: Hyparquet Streaming  
   open http://localhost:3000/hyparquet_client
   
   # Client 3: DuckDB-WASM SQL ✅
   open http://localhost:3000/duckdb_client
   ```

4. **Run tests**:
   - Click individual strategy buttons for targeted tests
   - Click "Run All Tests" for complete performance suite  
   - Use "Export Results" to save JSON results for analysis
   - Monitor browser console for detailed performance logs

## Performance Metrics

The client measures:

### HTTP Metrics
- Request count and patterns
- Total bytes downloaded  
- Average Time to First Byte (TTFB)
- HTTP request duration

### Arrow Processing Metrics
- Arrow table parsing time
- Row processing rates
- Memory usage throughout processing
- Filter operation efficiency

### Spatial Filtering Metrics
- Input vs output row counts
- Spatial filter efficiency
- Bbox intersection performance

## Results Format

Results are saved as JSON with:

```json
{
  "metadata": {
    "testType": "arrow_client_performance",
    "timestamp": "2024-01-XX...",
    "config": { ... },
    "bbox": { "west": -122.6, ... }
  },
  "summary": {
    "no_partition": {
      "avgTime": 1234.5,
      "avgBytes": 12345678,
      "avgRows": 1500,
      "efficiency": 0.12
    }
  },
  "results": [ ... ]
}
```

## Implementation Notes

### Partition Strategy Details

1. **NO_PARTITION**: Uses streaming fetch to avoid downloading full 1.6GB file, applies Arrow filtering for Bay Area bbox
2. **ATTRIBUTE_STATE**: Downloads only `StateAbbr=CA` partition (~3MB), filters to bbox in Arrow
3. **SPATIAL_H3_L3**: Downloads H3 hexagons covering Bay Area (estimated 4 hexagons)
4. **HYBRID_STATE_H3**: Downloads CA state partitions with H3 sub-directories

