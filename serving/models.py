from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field

# Trip Models
class TripBase(BaseModel):
    pickup_location_id: Optional[int] = None
    dropoff_location_id: Optional[int] = None
    pickup_datetime: datetime
    dropoff_datetime: Optional[datetime] = None
    passenger_count: Optional[int] = None
    trip_distance: Optional[float] = None
    fare_amount: Optional[float] = None
    tip_amount: Optional[float] = None
    total_amount: Optional[float] = None

class TripResponse(TripBase):
    trip_id: str
    vendor_id: Optional[int] = None
    payment_type: Optional[str] = None
    pickup_zone: Optional[str] = None
    dropoff_zone: Optional[str] = None

# Analytics Models
class ZoneMetrics(BaseModel):
    zone_id: int
    zone_name: str
    borough: str
    total_pickups: int
    total_dropoffs: int
    avg_fare: float
    total_revenue: float
    avg_trip_distance: float
    peak_hour_factor: float

class WeatherImpact(BaseModel):
    date: datetime
    weather_condition: str
    temperature_celsius: float
    humidity: float
    total_trips: int
    avg_fare_amount: float
    weather_impact_score: float

class DemandPrediction(BaseModel):
    location_id: int
    zone_name: str
    prediction_hour: datetime
    predicted_demand: float
    confidence_score: float
    weather_impact_factor: float
    historical_avg: float

class RealTimeActivity(BaseModel):
    zone_id: int
    zone_name: str
    activity_timestamp: datetime
    activity_score: float
    pickup_count: int
    revenue_last_hour: float
    avg_wait_time: Optional[float] = None

# API Response Models
class APIResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class PaginatedResponse(BaseModel):
    success: bool
    message: str
    data: List[Any]
    total_count: int
    page: int
    page_size: int
    total_pages: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)

# Dashboard Models
class DashboardStats(BaseModel):
    total_trips_today: int
    total_revenue_today: float
    avg_fare_today: float
    active_zones: int
    peak_hour: str
    weather_condition: str
    top_zones: List[Dict[str, Any]]

class TimeSeriesData(BaseModel):
    timestamps: List[datetime]
    values: List[float]
    metric_name: str
    unit: str

# Filter Models
class DateRangeFilter(BaseModel):
    start_date: datetime
    end_date: datetime

class ZoneFilter(BaseModel):
    zone_ids: Optional[List[int]] = None
    borough: Optional[str] = None

class AnalyticsFilter(BaseModel):
    date_range: DateRangeFilter
    zones: Optional[ZoneFilter] = None
    weather_conditions: Optional[List[str]] = None
    min_fare: Optional[float] = None
    max_fare: Optional[float] = None
