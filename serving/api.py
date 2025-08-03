from fastapi import FastAPI, HTTPException, Depends, Query, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import List, Optional
from datetime import datetime, timedelta
import pandas as pd
import logging
from config import settings
from database import DatabaseService
from models import (
    TripResponse, ZoneMetrics, WeatherImpact, DemandPrediction,
    RealTimeActivity, APIResponse, PaginatedResponse, DashboardStats
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize database service
db_service = None
if settings:
    db_service = DatabaseService(settings.database_url)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Lakehouse Analytics API starting up...")
    yield
    # Shutdown
    logger.info("Lakehouse Analytics API shutting down...")

# Initialize FastAPI app with lifespan
app = FastAPI(
    title="Lakehouse Analytics API",
    description="REST API for real-time and historical taxi analytics",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# API Key authentication
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def get_api_key(api_key: str = Security(api_key_header)):
    if settings and api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow()}

# Dashboard endpoints
@app.get("/api/v1/dashboard/stats", response_model=DashboardStats)
async def get_dashboard_stats(api_key: str = Depends(get_api_key)):
    """Get key statistics for dashboard"""
    try:
        if not db_service:
            raise HTTPException(status_code=503, detail="Database service not available")
        
        stats = db_service.get_dashboard_stats()
        return DashboardStats(**stats)
    except Exception as e:
        logger.error(f"Error getting dashboard stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Trip endpoints
@app.get("/api/v1/trips/recent")
async def get_recent_trips(
    limit: int = Query(100, ge=1, le=1000),
    hours_back: int = Query(24, ge=1, le=168),
    api_key: str = Depends(get_api_key)
):
    """Get recent trips"""
    try:
        if not db_service:
            # Return mock data for development
            return {
                "success": True,
                "message": "Recent trips retrieved successfully",
                "data": [
                    {
                        "trip_id": "demo_001",
                        "pickup_datetime": datetime.utcnow(),
                        "dropoff_datetime": datetime.utcnow() + timedelta(minutes=15),
                        "fare_amount": 25.50,
                        "pickup_zone": "Manhattan",
                        "dropoff_zone": "Brooklyn"
                    }
                ]
            }
        
        trips_df = db_service.get_recent_trips(limit, hours_back)
        trips = trips_df.to_dict('records')
        
        return APIResponse(
            success=True,
            message=f"Retrieved {len(trips)} recent trips",
            data=trips
        )
    except Exception as e:
        logger.error(f"Error getting recent trips: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Analytics endpoints
@app.get("/api/v1/analytics/zones")
async def get_zone_metrics(
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    limit: int = Query(50, ge=1, le=500),
    api_key: str = Depends(get_api_key)
):
    """Get zone performance metrics"""
    try:
        if not db_service:
            # Return mock data
            return {
                "success": True,
                "message": "Zone metrics retrieved successfully",
                "data": [
                    {
                        "zone_id": 1,
                        "zone_name": "Times Square",
                        "borough": "Manhattan",
                        "total_trips": 1250,
                        "avg_fare": 18.75,
                        "total_revenue": 23437.50,
                        "peak_hour_factor": 35.2
                    }
                ]
            }
        
        metrics_df = db_service.get_zone_metrics(start_date, end_date)
        metrics = metrics_df.head(limit).to_dict('records')
        
        return APIResponse(
            success=True,
            message=f"Retrieved metrics for {len(metrics)} zones",
            data=metrics
        )
    except Exception as e:
        logger.error(f"Error getting zone metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/analytics/weather-impact")
async def get_weather_impact(
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    api_key: str = Depends(get_api_key)
):
    """Get weather impact analysis"""
    try:
        if not db_service:
            # Return mock data
            return {
                "success": True,
                "message": "Weather impact data retrieved successfully",
                "data": [
                    {
                        "date": datetime.utcnow().date(),
                        "weather_condition": "Clear",
                        "temperature_celsius": 22.5,
                        "total_trips": 2847,
                        "avg_fare_amount": 16.80,
                        "weather_impact_score": 75.3
                    }
                ]
            }
        
        impact_df = db_service.get_weather_impact(start_date, end_date)
        impact_data = impact_df.to_dict('records')
        
        return APIResponse(
            success=True,
            message=f"Retrieved weather impact for {len(impact_data)} days",
            data=impact_data
        )
    except Exception as e:
        logger.error(f"Error getting weather impact: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/analytics/time-series")
async def get_time_series(
    metric: str = Query("trip_count", regex="^(trip_count|revenue|avg_fare)$"),
    days_back: int = Query(7, ge=1, le=30),
    api_key: str = Depends(get_api_key)
):
    """Get time series data for charts"""
    try:
        if not db_service:
            # Return mock time series data
            base_time = datetime.utcnow() - timedelta(days=days_back)
            mock_data = []
            for i in range(days_back * 24):  # Hourly data
                timestamp = base_time + timedelta(hours=i)
                value = 100 + (i % 24) * 5 + (i % 7) * 10  # Simulate daily/weekly patterns
                mock_data.append({
                    "timestamp": timestamp.isoformat(),
                    "value": value
                })
            
            return {
                "success": True,
                "message": f"Time series data for {metric}",
                "data": {
                    "metric_name": metric,
                    "unit": "count" if metric == "trip_count" else "currency",
                    "series": mock_data
                }
            }
        
        ts_df = db_service.get_hourly_trip_counts(days_back)
        
        metric_mapping = {
            "trip_count": "trip_count",
            "revenue": "total_revenue", 
            "avg_fare": "avg_fare"
        }
        
        if metric not in metric_mapping:
            raise HTTPException(status_code=400, detail="Invalid metric")
        
        series_data = []
        for _, row in ts_df.iterrows():
            series_data.append({
                "timestamp": row['hour'].isoformat(),
                "value": float(row[metric_mapping[metric]])
            })
        
        return APIResponse(
            success=True,
            message=f"Time series data for {metric}",
            data={
                "metric_name": metric,
                "unit": "count" if metric == "trip_count" else "currency",
                "series": series_data
            }
        )
    except Exception as e:
        logger.error(f"Error getting time series: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Prediction endpoints
@app.get("/api/v1/predictions/demand")
async def get_demand_predictions(
    hours_ahead: int = Query(24, ge=1, le=168),
    top_zones: int = Query(20, ge=1, le=100),
    api_key: str = Depends(get_api_key)
):
    """Get demand predictions by zone"""
    try:
        if not db_service:
            # Return mock predictions
            mock_predictions = []
            for i in range(top_zones):
                for hour in range(1, min(hours_ahead + 1, 25)):
                    mock_predictions.append({
                        "location_id": i + 1,
                        "zone_name": f"Zone {i + 1}",
                        "prediction_hour": (datetime.utcnow() + timedelta(hours=hour)).isoformat(),
                        "predicted_demand": 50 + (i * 5) + (hour % 12) * 3,
                        "confidence_score": 0.75 + (i % 5) * 0.05,
                        "weather_impact_factor": 1.0 + (i % 3) * 0.1
                    })
            
            return {
                "success": True,
                "message": f"Demand predictions for next {hours_ahead} hours",
                "data": mock_predictions[:100]  # Limit response size
            }
        
        predictions_df = db_service.get_demand_prediction(hours_ahead)
        predictions = predictions_df.head(top_zones * min(hours_ahead, 24)).to_dict('records')
        
        return APIResponse(
            success=True,
            message=f"Demand predictions for next {hours_ahead} hours",
            data=predictions
        )
    except Exception as e:
        logger.error(f"Error getting demand predictions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Real-time endpoints
@app.get("/api/v1/realtime/activity")
async def get_real_time_activity(
    minutes_back: int = Query(60, ge=5, le=240),
    api_key: str = Depends(get_api_key)
):
    """Get real-time zone activity"""
    try:
        if not db_service:
            # Return mock real-time data
            return {
                "success": True,
                "message": f"Real-time activity for last {minutes_back} minutes",
                "data": [
                    {
                        "zone_id": 1,
                        "zone_name": "Times Square",
                        "activity_timestamp": datetime.utcnow().isoformat(),
                        "activity_score": 85.7,
                        "pickup_count": 42,
                        "revenue_last_hour": 1250.75
                    }
                ]
            }
        
        activity_df = db_service.get_real_time_activity(minutes_back)
        activity_data = activity_df.to_dict('records')
        
        return APIResponse(
            success=True,
            message=f"Real-time activity for last {minutes_back} minutes",
            data=activity_data
        )
    except Exception as e:
        logger.error(f"Error getting real-time activity: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Data export endpoints
@app.get("/api/v1/export/trips")
async def export_trips(
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    format: str = Query("json", regex="^(json|csv)$"),
    api_key: str = Depends(get_api_key)
):
    """Export trip data in various formats"""
    try:
        # This would implement data export functionality
        # For now, return a simple response
        return APIResponse(
            success=True,
            message=f"Export functionality for {format} format",
            data={"export_url": f"/downloads/trips_{start_date.date()}_{end_date.date()}.{format}"}
        )
    except Exception as e:
        logger.error(f"Error exporting trips: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
