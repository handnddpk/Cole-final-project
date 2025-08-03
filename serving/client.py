import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import json

class LakehouseClient:
    """Client library for Lakehouse Analytics API"""
    
    def __init__(self, base_url: str = "http://localhost:8000/api/v1", api_key: str = "demo-api-key-2024"):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key})
    
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make HTTP request to API"""
        try:
            response = self.session.get(f"{self.base_url}/{endpoint}", params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"API request failed: {e}")
    
    def health_check(self) -> Dict[str, Any]:
        """Check API health"""
        return self._make_request("../health")
    
    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get dashboard statistics"""
        return self._make_request("dashboard/stats")
    
    def get_recent_trips(self, limit: int = 100, hours_back: int = 24) -> pd.DataFrame:
        """Get recent trips as DataFrame"""
        params = {"limit": limit, "hours_back": hours_back}
        response = self._make_request("trips/recent", params)
        
        if response.get('success'):
            return pd.DataFrame(response['data'])
        else:
            raise Exception(f"Failed to get trips: {response.get('message')}")
    
    def get_zone_metrics(self, start_date: datetime, end_date: datetime, limit: int = 50) -> pd.DataFrame:
        """Get zone performance metrics"""
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "limit": limit
        }
        response = self._make_request("analytics/zones", params)
        
        if response.get('success'):
            return pd.DataFrame(response['data'])
        else:
            raise Exception(f"Failed to get zone metrics: {response.get('message')}")
    
    def get_weather_impact(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get weather impact analysis"""
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }
        response = self._make_request("analytics/weather-impact", params)
        
        if response.get('success'):
            return pd.DataFrame(response['data'])
        else:
            raise Exception(f"Failed to get weather impact: {response.get('message')}")
    
    def get_time_series(self, metric: str = "trip_count", days_back: int = 7) -> pd.DataFrame:
        """Get time series data"""
        params = {"metric": metric, "days_back": days_back}
        response = self._make_request("analytics/time-series", params)
        
        if response.get('success'):
            series_data = response['data']['series']
            df = pd.DataFrame(series_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        else:
            raise Exception(f"Failed to get time series: {response.get('message')}")
    
    def get_demand_predictions(self, hours_ahead: int = 24, top_zones: int = 20) -> pd.DataFrame:
        """Get demand predictions"""
        params = {"hours_ahead": hours_ahead, "top_zones": top_zones}
        response = self._make_request("predictions/demand", params)
        
        if response.get('success'):
            df = pd.DataFrame(response['data'])
            if 'prediction_hour' in df.columns:
                df['prediction_hour'] = pd.to_datetime(df['prediction_hour'])
            return df
        else:
            raise Exception(f"Failed to get predictions: {response.get('message')}")
    
    def get_real_time_activity(self, minutes_back: int = 60) -> pd.DataFrame:
        """Get real-time zone activity"""
        params = {"minutes_back": minutes_back}
        response = self._make_request("realtime/activity", params)
        
        if response.get('success'):
            df = pd.DataFrame(response['data'])
            if 'activity_timestamp' in df.columns:
                df['activity_timestamp'] = pd.to_datetime(df['activity_timestamp'])
            return df
        else:
            raise Exception(f"Failed to get real-time activity: {response.get('message')}")
    
    def export_trips(self, start_date: datetime, end_date: datetime, format: str = "json") -> Dict[str, Any]:
        """Export trip data"""
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "format": format
        }
        return self._make_request("export/trips", params)

# Example usage and helper functions
def example_usage():
    """Example usage of the client library"""
    client = LakehouseClient()
    
    # Health check
    print("API Health:", client.health_check())
    
    # Get dashboard stats
    stats = client.get_dashboard_stats()
    print("Dashboard Stats:", stats)
    
    # Get recent trips
    trips = client.get_recent_trips(limit=10)
    print(f"Recent trips shape: {trips.shape}")
    print(trips.head())
    
    # Get zone metrics for last week
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    zones = client.get_zone_metrics(start_date, end_date, limit=10)
    print(f"Zone metrics shape: {zones.shape}")
    print(zones.head())
    
    # Get time series data
    ts_data = client.get_time_series("trip_count", days_back=3)
    print(f"Time series shape: {ts_data.shape}")
    print(ts_data.head())
    
    # Get predictions
    predictions = client.get_demand_predictions(hours_ahead=12, top_zones=5)
    print(f"Predictions shape: {predictions.shape}")
    print(predictions.head())

def create_sample_analysis():
    """Create a sample analysis using the client"""
    client = LakehouseClient()
    
    try:
        # Get data for analysis
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        print("📊 Lakehouse Analytics - Sample Analysis")
        print("=" * 50)
        
        # Dashboard overview
        stats = client.get_dashboard_stats()
        if stats.get('success'):
            data = stats['data']
            print(f"Total Trips Today: {data['total_trips_today']:,}")
            print(f"Total Revenue Today: ${data['total_revenue_today']:,.2f}")
            print(f"Average Fare: ${data['avg_fare_today']:.2f}")
            print(f"Peak Hour: {data['peak_hour']}")
            print()
        
        # Zone performance
        zones = client.get_zone_metrics(start_date, end_date, limit=5)
        print("🏆 Top 5 Zones by Revenue:")
        print("-" * 30)
        for _, zone in zones.iterrows():
            print(f"{zone['zone_name']}: ${zone['total_revenue']:,.2f} ({zone['total_trips']} trips)")
        print()
        
        # Time series analysis
        ts_data = client.get_time_series("trip_count", days_back=7)
        if not ts_data.empty:
            daily_avg = ts_data.groupby(ts_data['timestamp'].dt.date)['value'].mean()
            print("📈 Daily Average Trips (Last 7 Days):")
            print("-" * 35)
            for date, avg_trips in daily_avg.tail(5).items():
                print(f"{date}: {avg_trips:.0f} trips")
            print()
        
        # Real-time activity
        activity = client.get_real_time_activity(minutes_back=60)
        if not activity.empty:
            print("⚡ Most Active Zones (Last Hour):")
            print("-" * 32)
            for _, zone in activity.head(3).iterrows():
                print(f"{zone['zone_name']}: {zone['pickup_count']} pickups, ${zone['revenue_last_hour']:.2f}")
        
    except Exception as e:
        print(f"Analysis failed: {e}")

if __name__ == "__main__":
    # Run example usage
    create_sample_analysis()
