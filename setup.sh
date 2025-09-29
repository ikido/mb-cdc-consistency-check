#!/bin/bash

# CDC Consistency Check - Setup Script
# This script sets up the virtual environment and installs dependencies using uv

set -e  # Exit on any error

echo "🚀 Setting up CDC Consistency Check environment..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ uv is not installed. Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    echo "✅ uv installed successfully"
fi

# Create virtual environment with uv
echo "📦 Creating virtual environment..."
uv venv

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source .venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies with uv..."
uv pip install -r requirements.txt

# Create .env from template if it doesn't exist
if [ ! -f ".env" ]; then
    if [ -f "env.template" ]; then
        echo "📝 Creating .env file from template..."
        cp env.template .env
        echo "⚠️  Please edit .env file with your database connection details"
    else
        echo "📝 Creating .env template..."
        cat > .env << EOF
# Database Connection URLs
MONEYBALL_DATABASE_URL=postgresql://username:password@localhost:5432/moneyball
SNOWFLAKE_DATABASE_URL=snowflake://mb_cdc_validator:USER_PASSWORD@RIB63479/MONEYBALL/public?warehouse=OPENFLOW_WAREHOUSE&role=mb_cdc_validator_role

# Target Tables (comma-separated)
TARGET_TABLES=companies,transactions,funds,notes,users

# Output Configuration
OUTPUT_DIR=./tmp
EOF
        echo "⚠️  Please edit .env file with your database connection details"
    fi
else
    echo "✅ .env file already exists"
fi

# Make scripts executable
chmod +x schema_dump.py

# Create tmp directory for schemas
mkdir -p tmp

echo ""
echo "🎉 Setup completed successfully!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your database connection details"
echo "2. Activate the virtual environment: source .venv/bin/activate"
echo "3. Run schema dump: python schema_dump.py"
echo ""
echo "For Snowflake setup instructions, see README.md"
