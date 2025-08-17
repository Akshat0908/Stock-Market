#!/bin/bash

# Stock Market Data Pipeline Setup Script
# This script helps set up the environment and start the pipeline

set -e

echo "🚀 Stock Market Data Pipeline Setup"
echo "=================================="
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    echo "   Visit: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    echo "   Visit: https://docs.docker.com/compose/install/"
    exit 1
fi

echo "✅ Docker and Docker Compose are installed"
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    if [ -f env.example ]; then
        cp env.example .env
        echo "✅ .env file created from env.example"
    else
        echo "❌ env.example file not found. Please create .env manually."
        exit 1
    fi
else
    echo "✅ .env file already exists"
fi

echo ""

# Generate Fernet key if not set
if ! grep -q "FERNET_KEY=" .env || grep -q "FERNET_KEY=your_fernet_key_here" .env; then
    echo "🔑 Generating Fernet key for Airflow..."
    FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || echo "your_fernet_key_here")
    
    if [ "$FERNET_KEY" != "your_fernet_key_here" ]; then
        # Update .env file with generated key
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            sed -i '' "s/FERNET_KEY=your_fernet_key_here/FERNET_KEY=$FERNET_KEY/" .env
        else
            # Linux
            sed -i "s/FERNET_KEY=your_fernet_key_here/FERNET_KEY=$FERNET_KEY/" .env
        fi
        echo "✅ Fernet key generated and added to .env"
    else
        echo "⚠️  Could not generate Fernet key automatically. Please set it manually in .env"
    fi
else
    echo "✅ Fernet key already configured"
fi

echo ""

# Check if API key is configured
if grep -q "ALPHA_VANTAGE_API_KEY=your_api_key_here" .env; then
    echo "⚠️  IMPORTANT: You need to set your Alpha Vantage API key in .env"
    echo "   1. Get a free API key from: https://www.alphavantage.co/support/#api-key"
    echo "   2. Edit .env and replace 'your_api_key_here' with your actual API key"
    echo ""
    read -p "Press Enter after you've updated the API key in .env..."
    
    # Check again
    if grep -q "ALPHA_VANTAGE_API_KEY=your_api_key_here" .env; then
        echo "❌ API key still not configured. Please update .env and run setup again."
        exit 1
    fi
else
    echo "✅ Alpha Vantage API key is configured"
fi

echo ""

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p logs plugins
echo "✅ Directories created"

echo ""

# Check if user wants to start the pipeline now
echo "🚀 Setup complete! You can now start the pipeline."
echo ""
read -p "Would you like to start the pipeline now? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Starting the pipeline..."
    echo ""
    
    # Start the services
    docker-compose up -d
    
    echo ""
    echo "✅ Pipeline started successfully!"
    echo ""
    echo "📊 Access Airflow UI at: http://localhost:8080"
    echo "   Username: admin"
    echo "   Password: admin"
    echo ""
    echo "📝 View logs with: docker-compose logs -f"
    echo "🛑 Stop pipeline with: docker-compose down"
    echo ""
    echo "🔍 Monitor the DAG 'stock_market_pipeline' in the Airflow UI"
    echo "   It will run daily at 6 PM on weekdays"
    
else
    echo ""
    echo "📋 To start the pipeline later, run:"
    echo "   docker-compose up -d"
    echo ""
    echo "📊 Then access Airflow UI at: http://localhost:8080"
    echo "   Username: admin"
    echo "   Password: admin"
fi

echo ""
echo "🎉 Setup complete! Happy data pipelining! 🚀📊"
