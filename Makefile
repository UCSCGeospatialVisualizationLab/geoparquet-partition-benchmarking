# Parquet Performance Testing Framework - Makefile
# Essential commands for setup and testing

.PHONY: help setup test report clean serve

# Default target
help: ## Show available commands
	@echo "Parquet Performance Testing Framework"
	@echo "===================================="
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Quick start: make setup && make test"

setup: ## Initial setup (dependencies, .env, Docker)
	@echo "🚀 Setting up project..."
	./setup.sh
	pip install -r requirements.txt
	npm install
	cd docker && docker-compose up -d
	@echo "✅ Setup complete!"

test: ## Run complete performance test suite
	@echo "📊 Creating partitioned datasets..."
	python scripts/s3_partitioning/create_partitions.py
	@echo "🧪 Running performance tests..."
	python scripts/benchmarks/benchmark_runner.py
	@echo "✅ Tests completed!"

report: ## Generate analysis report and dashboard
	@echo "📈 Generating performance analysis..."
	python analysis/generate_report.py
	@echo "✅ Report generated in results/ directory"

clean: ## Clean up temporary files
	@echo "🧹 Cleaning up..."
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete
	rm -rf results/temp/
	@echo "✅ Cleanup complete"

stop: ## Stop Docker environment
	cd docker && docker-compose down

serve: ## Serve the visualization directory
	npx serve scripts/visualization -p 3000