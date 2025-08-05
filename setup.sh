#!/bin/bash
# Parquet Performance Testing - Quick Setup Script

echo "🚀 Parquet Performance Testing Framework Setup"
echo "=============================================="

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "📝 Creating .env file from template..."
    cp env.template .env
    echo "✅ .env file created. Please edit it with your AWS credentials:"
    echo "   - AWS_ACCESS_KEY_ID"
    echo "   - AWS_SECRET_ACCESS_KEY" 
    echo "   - TEST_S3_BUCKET (your test bucket name)"
    echo ""
else
    echo "✅ .env file already exists"
fi

# Check if Python dependencies are installed
echo "🐍 Checking Python dependencies..."
if ! python -c "import duckdb, pandas, geopandas" 2>/dev/null; then
    echo "📦 Installing Python dependencies..."
    pip install -r requirements.txt
else
    echo "✅ Python dependencies already installed"
fi

# Check if Node.js dependencies are installed
echo "📦 Checking Node.js dependencies..."
if [ ! -d "node_modules" ]; then
    echo "📦 Installing Node.js dependencies..."
    npm install
else
    echo "✅ Node.js dependencies already installed"
fi

# Check Docker
echo "🐳 Checking Docker..."
if command -v docker &> /dev/null; then
    echo "✅ Docker is available"
    echo "🔧 To start the test environment:"
    echo "   cd docker && docker-compose up -d"
else
    echo "⚠️  Docker not found. Please install Docker to use the containerized environment."
fi

echo ""
echo "🎯 Next Steps:"
echo "1. Edit .env file with your AWS credentials"
echo "2. Configure your test S3 bucket in s3_config/s3_config.yaml"
echo "3. Run: cd docker && docker-compose up -d"
echo "4. Proceed with Step 2 in PERFORMANCE_TEST_PLAN.md"
echo ""
echo "📚 Documentation:"
echo "   - Main plan: PERFORMANCE_TEST_PLAN.md"
echo "   - Project overview: README.md"
echo "   - Directory guides: */README.md"