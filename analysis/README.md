# Analysis Directory

This directory contains scripts and notebooks for analyzing performance test results and generating insights.

## Key Files

### `generate_report.py`
Creates comprehensive performance comparison reports with:
- Comparative tables of metrics by partition + client combination
- Statistical analysis of performance differences
- Recommendations for optimal strategies

### `create_visualizations.py` 
Generates performance visualization charts:
- Partition size vs. query runtime scatter plots
- Bytes transferred vs. rendering time analysis
- Memory usage patterns over time
- Network efficiency comparisons

### `statistical_analysis.py`
Performs statistical analysis on performance data:
- Significance testing between partitioning strategies
- Confidence intervals for performance metrics
- Outlier detection and data quality checks

### `export_utilities.py`
Data export utilities for external analysis:
- CSV exports for spreadsheet analysis
- JSON exports for web dashboards
- Parquet exports for further big data analysis

## Jupyter Notebooks

### `performance_exploration.ipynb`
Interactive exploration of performance results:
- Data loading and initial exploration
- Visual analysis of key metrics  
- Hypothesis testing and insights

### `network_analysis.ipynb`
Deep dive into network performance patterns:
- HTTP request pattern analysis
- Bandwidth utilization efficiency
- Cache hit rates and optimization opportunities

### `strategy_comparison.ipynb`
Comparative analysis across partitioning strategies:
- Performance trade-offs by use case
- Scalability projections
- Cost-benefit analysis

## Generated Outputs

### Performance Dashboard
- `dashboard.html` - Interactive web dashboard with all results
- Filterable by partition strategy, client type, query type
- Real-time metric comparisons

### Static Reports  
- `performance_summary.pdf` - Executive summary with key findings
- `technical_report.pdf` - Detailed technical analysis
- `recommendations.md` - Strategy recommendations by use case

## Usage

1. **Run Analysis Pipeline**:
   ```bash
   python generate_report.py
   python create_visualizations.py
   ```

2. **Interactive Analysis**:
   ```bash
   jupyter notebook performance_exploration.ipynb
   ```

3. **Generate Dashboard**:
   ```bash
   python -m http.server 8000
   # Visit http://localhost:8000/dashboard.html
   ```

## Key Insights Expected

- **Partition Size Sweet Spot**: Optimal file sizes for HTTP range requests
- **Network vs. Processing Trade-offs**: When to stream vs. batch download  
- **Client Performance**: Arrow vs. Hyparquet vs. DuckDB-WASM comparison
- **Query Pattern Impact**: How different queries benefit from different partitioning
- **Scalability Projections**: Performance expectations for larger datasets