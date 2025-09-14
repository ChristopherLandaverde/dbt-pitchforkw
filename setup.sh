#!/bin/bash

# Setup script for Pitchfork Analytics dbt project

echo "🎵 Setting up Pitchfork Analytics dbt project..."

# Check if dbt is installed
if ! command -v dbt &> /dev/null; then
    echo "❌ dbt is not installed. Please install dbt first:"
    echo "   pip install dbt-core dbt-sqlite"
    exit 1
fi

# Install Python dependencies
echo "📦 Installing Python dependencies..."
pip install -r requirements.txt

# Install dbt packages
echo "📦 Installing dbt packages..."
dbt deps

# Test database connection
echo "🔍 Testing database connection..."
dbt debug

# Parse project files
echo "🔍 Parsing project files..."
dbt parse

# Run initial models (if data exists)
echo "🚀 Running initial models..."
dbt run --select stg_pitchfork_reviews

# Run tests
echo "🧪 Running tests..."
dbt test

# Generate documentation
echo "📚 Generating documentation..."
dbt docs generate

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Check your database connection: dbt debug"
echo "2. Run all models: dbt run"
echo "3. Run tests: dbt test"
echo "4. View documentation: dbt docs serve"
echo "5. Check the PRD: docs/PRD.md"
