#!/bin/bash

# Quick activation script for the virtual environment
echo "🐍 Activating virtual environment..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run: python3 -m venv venv"
    exit 1
fi

# Activate the virtual environment
source venv/bin/activate

echo "✅ Virtual environment activated!"
echo "📦 Installed packages:"
pip list | grep -E "(dbt|pytest)"

echo ""
echo "🚀 Ready to work on your dbt project!"
echo "   - Run dbt commands: dbt run, dbt test, etc."
echo "   - Deactivate when done: deactivate"
