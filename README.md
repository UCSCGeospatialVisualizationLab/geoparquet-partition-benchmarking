# Parquet Partitioning Performance Testing Framework

A comprehensive performance testing framework for evaluating different parquet partitioning strategies using S3-native storage and HTTP access patterns.

## 🎯 Project Overview

This project evaluates the performance trade-offs of different parquet partitioning strategies for large geospatial datasets, specifically using the Census tract data from the HAZUS dataset (FEMA). The testing framework uses realistic cloud-native patterns with S3 storage and HTTP access.

**Dataset**: `s3://vizlab-geodatalake/hazus/vector/hazus_CensusTract.parquet` (~1.6GB)  
**Environment**: 4-core Docker container with network monitoring  
**Objective**: Optimize partitioning strategies for both analytics and visualization workloads

## 🏗️ Partitioning Strategies Under Test

1. **Pure Spatial Grid** (H3 Level 6, ~5 MB files)
2. **Attribute-Based** (state_code, ~120 MB files)  
3. **Hybrid Spatial + Attribute** (state_code ➜ H3 subfolders, ~40 MB)
4. **No Partitioning** (single ~1.6 GB file, control case)

## 🖥️ Visualization Clients

1. **Full Parquet Download** → Apache Arrow JS/WASM
2. **Hyparquet** (incremental columnar streaming)
3. **DuckDB-WASM** (HTTP range requests)

## 📁 Project Structure

```
parquet-performance-testing/
├── scripts/                    # Core implementation scripts
│   ├── s3_partitioning/       # Dataset partitioning scripts
│   ├── analytics/             # DuckDB analytical queries  
│   ├── visualization/         # HTTP-based client implementations
│   └── benchmarks/           # Performance measurement framework
├── docker/                    # Docker environment configuration
├── results/                   # Performance test results storage
├── analysis/                  # Results analysis and visualization
├── s3_config/                # S3 bucket layouts and configurations
├── PERFORMANCE_TEST_PLAN.md  # Detailed implementation plan
└── requirements.txt          # Python dependencies
```

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- AWS credentials with S3 access
- Node.js 18+ (for visualization clients)
- Make (for simplified command execution)

### ⚡ Simple Usage

```bash
make setup    # Complete setup (dependencies, .env, Docker)  
make test     # Run full performance test suite
make report   # Generate analysis report
```

### 📋 All Commands

```bash
make help     # Show available commands  
make setup    # Initial setup (dependencies, .env, Docker)
make test     # Run complete performance test suite
make report   # Generate analysis report and dashboard  
make clean    # Clean up temporary files
make stop     # Stop Docker environment
```

### 🔧 Manual Setup (if preferred)

1. **Clone and Navigate**:
   ```bash
   git clone <repository-url>
   cd parquet-performance-testing
   ```

2. **Configure Environment**:
   ```bash
   cp env.template .env
   # Edit .env with your AWS credentials:
   #   AWS_ACCESS_KEY_ID=your_key
   #   AWS_SECRET_ACCESS_KEY=your_secret  
   #   TEST_S3_BUCKET=geoparquest-performance-test-bucket
   ```

3. **Run Setup**:
   ```bash
   make setup
   # This installs dependencies and starts Docker environment
   ```

## 📊 Performance Metrics

### Analytics Metrics
- Query execution time (cold start & warm cache)
- CPU utilization and memory usage
- S3 request patterns and data transfer efficiency
- Network latency impact

### Visualization Metrics  
- Time to first byte (TTFB)
- Progressive loading milestones
- Total bytes transferred
- Browser memory usage
- Frame rate during interaction

### Network Metrics
- HTTP request count and patterns
- Range request efficiency
- Bandwidth utilization
- Connection reuse and caching effectiveness

## 🎛️ Configuration

### S3 Configuration
Edit `s3_config/s3_config.yaml` to customize:
- Bucket names and regions
- Partitioning parameters (H3 levels, target file sizes)
- Query test cases
- Network simulation conditions

### Environment Variables
Key settings in `.env`:
- `AWS_*`: AWS credentials and region
- `TEST_S3_BUCKET`: Your test bucket name (e.g., `geoparquest-performance-test-bucket`)
- `TEST_RUNS`: Number of test iterations
- `DOCKER_CPU_LIMIT`: Resource constraints

## 📈 Expected Insights

- **Partition Size Sweet Spot**: Optimal file sizes for HTTP range requests
- **Network vs. Processing Trade-offs**: Stream vs. batch download performance
- **Client Performance Comparison**: Arrow vs. Hyparquet vs. DuckDB-WASM
- **Query Pattern Impact**: How different analytical patterns benefit from different partitioning
- **Scalability Projections**: Performance expectations for larger datasets

## 🔧 Development

### Project Status
Track implementation progress in `PERFORMANCE_TEST_PLAN.md` - each step includes:
- Detailed objectives and tasks
- Implementation notes sections
- Status tracking and checkboxes

### Contributing
1. Follow the step-by-step plan in `PERFORMANCE_TEST_PLAN.md`
2. Update progress and notes in each step
3. Run tests and validate results
4. Document findings and insights

## 📋 Implementation Progress

See `PERFORMANCE_TEST_PLAN.md` for detailed step-by-step progress tracking.

**Current Status**: 
- ✅ Step 1: Project Structure Setup
- ⏳ Step 2: S3 Data Exploration  
- ⏳ Step 3: S3 Partitioning Setup
- ⏳ Step 4: Partitioning Implementation
- ... (see plan for full details)

## 🤝 Support

For questions or issues:
1. Check the detailed plan in `PERFORMANCE_TEST_PLAN.md`
2. Review directory-specific README files
3. Examine configuration files in `s3_config/`

## 📄 License

MIT License - see LICENSE file for details.

---

**Next Steps**: Proceed with Step 2 (S3 Data Exploration) as outlined in `PERFORMANCE_TEST_PLAN.md`.