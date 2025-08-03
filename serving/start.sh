#!/bin/bash

# Serving Layer Startup Script
echo "🚀 Starting Lakehouse Analytics Serving Layer..."

# Check if running in development or production
if [ "$1" = "dev" ]; then
    echo "📱 Starting in development mode..."
    
    # Install dependencies if needed
    if [ ! -d "venv" ]; then
        echo "Creating virtual environment..."
        python -m venv venv
        source venv/bin/activate
        pip install -r requirements.txt
    else
        source venv/bin/activate
    fi
    
    # Start API and Dashboard concurrently
    echo "🌐 Starting API server on port 8000..."
    uvicorn api:app --host 0.0.0.0 --port 8000 --reload &
    API_PID=$!
    
    echo "📊 Starting Dashboard on port 8501..."
    streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0 &
    DASHBOARD_PID=$!
    
    echo "✅ Services started!"
    echo "📱 API: http://localhost:8000"
    echo "📱 API Docs: http://localhost:8000/docs"
    echo "📊 Dashboard: http://localhost:8501"
    
    # Wait for services
    wait $API_PID $DASHBOARD_PID
    
elif [ "$1" = "api" ]; then
    echo "🌐 Starting API server only..."
    uvicorn api:app --host 0.0.0.0 --port 8000 --reload
    
elif [ "$1" = "dashboard" ]; then
    echo "📊 Starting Dashboard only..."
    streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
    
else
    echo "Usage: $0 {dev|api|dashboard}"
    echo "  dev       - Start both API and Dashboard in development mode"
    echo "  api       - Start API server only"
    echo "  dashboard - Start Dashboard only"
    exit 1
fi
