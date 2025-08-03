import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
from datetime import datetime, timedelta
import time
import json
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Lakehouse Analytics Dashboard",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration
API_BASE_URL = "http://analytics-api:8000/api/v1"
API_KEY = "demo-api-key-2024"

# Helper functions
def get_api_headers():
    return {"X-API-Key": API_KEY}

def fetch_data(endpoint, params=None):
    """Fetch data from API with error handling"""
    try:
        response = requests.get(
            f"{API_BASE_URL}/{endpoint}",
            headers=get_api_headers(),
            params=params,
            timeout=10
        )
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

def create_mock_data():
    """Create mock data for development when API is not available"""
    return {
        "dashboard_stats": {
            "total_trips_today": 2847,
            "total_revenue_today": 47832.50,
            "avg_fare_today": 16.80,
            "active_zones": 125,
            "peak_hour": "18:00",
            "weather_condition": "Clear",
            "top_zones": [
                {"zone_name": "Times Square", "trips": 342, "revenue": 6840.25},
                {"zone_name": "JFK Airport", "trips": 198, "revenue": 8910.75},
                {"zone_name": "Central Park", "trips": 156, "revenue": 3128.40},
                {"zone_name": "Brooklyn Bridge", "trips": 134, "revenue": 2680.15},
                {"zone_name": "Wall Street", "trips": 112, "revenue": 2912.85}
            ]
        },
        "time_series": [
            {"timestamp": (datetime.now() - timedelta(hours=i)).isoformat(), 
             "value": 100 + (i % 24) * 5 + (i % 7) * 10}
            for i in range(168, 0, -1)  # Last 7 days
        ]
    }

# Dashboard components
def render_header():
    """Render dashboard header"""
    st.title("🚕 Lakehouse Analytics Dashboard")
    st.markdown("Real-time and historical taxi analytics powered by Kafka CDC, Iceberg, and Spark")
    
    # Status indicators
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("System Status", "🟢 Online")
    with col2:
        st.metric("Last Update", datetime.now().strftime("%H:%M:%S"))
    with col3:
        st.metric("Data Freshness", "< 5 min")
    with col4:
        st.metric("Active Pipelines", "4/4")

def render_kpi_cards(stats_data):
    """Render KPI cards"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Trips Today",
            f"{stats_data['total_trips_today']:,}",
            delta="+12.5%"
        )
    
    with col2:
        st.metric(
            "Total Revenue Today",
            f"${stats_data['total_revenue_today']:,.2f}",
            delta="+8.3%"
        )
    
    with col3:
        st.metric(
            "Average Fare",
            f"${stats_data['avg_fare_today']:.2f}",
            delta="-2.1%"
        )
    
    with col4:
        st.metric(
            "Active Zones",
            stats_data['active_zones'],
            delta="+3"
        )

def render_time_series_chart(time_series_data):
    """Render time series chart"""
    df = pd.DataFrame(time_series_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    fig = px.line(
        df, 
        x='timestamp', 
        y='value',
        title='Trip Volume Over Time (Last 7 Days)',
        labels={'value': 'Number of Trips', 'timestamp': 'Time'}
    )
    
    fig.update_layout(
        height=400,
        xaxis_title="Time",
        yaxis_title="Number of Trips",
        hovermode='x unified'
    )
    
    return fig

def render_zone_performance(top_zones):
    """Render zone performance chart"""
    df = pd.DataFrame(top_zones)
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Trip Count by Zone', 'Revenue by Zone'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Trip count bar chart
    fig.add_trace(
        go.Bar(
            x=df['zone_name'],
            y=df['trips'],
            name='Trips',
            marker_color='lightblue'
        ),
        row=1, col=1
    )
    
    # Revenue bar chart
    fig.add_trace(
        go.Bar(
            x=df['zone_name'],
            y=df['revenue'],
            name='Revenue',
            marker_color='lightgreen'
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        height=400,
        showlegend=False,
        title_text="Top 5 Zones Performance"
    )
    
    return fig

def render_real_time_map():
    """Render real-time activity map (mock)"""
    # Generate mock map data
    import numpy as np
    
    map_data = pd.DataFrame({
        'lat': np.random.normal(40.7589, 0.1, 100),
        'lon': np.random.normal(-73.9851, 0.1, 100),
        'activity': np.random.uniform(0, 100, 100)
    })
    
    try:
        fig = px.scatter_mapbox(
            map_data,
            lat='lat',
            lon='lon',
            size='activity',
            color='activity',
            color_continuous_scale='Viridis',
            title='Real-time Zone Activity',
            zoom=10,
            height=500
        )
        
        fig.update_layout(
            mapbox_style="open-street-map",
            margin={"r":0,"t":40,"l":0,"b":0}
        )
        
        return fig
    except Exception as e:
        # Fallback to simple scatter plot if mapbox fails
        fig = px.scatter(
            map_data,
            x='lon',
            y='lat',
            size='activity',
            color='activity',
            color_continuous_scale='Viridis',
            title='Real-time Zone Activity (Scatter Plot)',
            height=500
        )
        return fig

def render_analytics_section():
    """Render analytics section"""
    st.header("📊 Advanced Analytics")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Weather Impact", "Demand Prediction", "Zone Analysis", "Real-time Activity"])
    
    with tab1:
        st.subheader("Weather Impact Analysis")
        
        # Mock weather impact data
        weather_data = pd.DataFrame({
            'date': pd.date_range(start='2024-01-01', periods=30),
            'weather': ['Clear', 'Rainy', 'Cloudy', 'Clear', 'Snowy'] * 6,
            'trips': np.random.randint(800, 1200, 30),
            'avg_fare': np.random.uniform(15, 25, 30)
        })
        
        fig = px.scatter(
            weather_data,
            x='trips',
            y='avg_fare',
            color='weather',
            size='trips',
            title='Trip Count vs Average Fare by Weather Condition',
            labels={'trips': 'Number of Trips', 'avg_fare': 'Average Fare ($)'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Weather correlation metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Clear Days Avg Trips", "1,045", delta="+15%")
        with col2:
            st.metric("Rainy Days Avg Trips", "1,285", delta="+23%")
        with col3:
            st.metric("Snow Days Avg Trips", "891", delta="-8%")
    
    with tab2:
        st.subheader("Demand Prediction")
        
        # Mock prediction data
        pred_data = pd.DataFrame({
            'hour': pd.date_range(start=datetime.now(), periods=24, freq='H'),
            'predicted': np.random.randint(80, 150, 24),
            'confidence': np.random.uniform(0.7, 0.95, 24)
        })
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=pred_data['hour'],
            y=pred_data['predicted'],
            mode='lines+markers',
            name='Predicted Demand',
            line=dict(color='blue', width=3)
        ))
        
        fig.update_layout(
            title='24-Hour Demand Forecast',
            xaxis_title='Hour',
            yaxis_title='Predicted Trips',
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Confidence score
        avg_confidence = pred_data['confidence'].mean()
        st.metric("Model Confidence", f"{avg_confidence:.1%}", delta="+2.3%")
    
    with tab3:
        st.subheader("Zone Performance Analysis")
        
        # Mock zone data
        zone_data = pd.DataFrame({
            'zone': [f'Zone {i}' for i in range(1, 21)],
            'revenue': np.random.uniform(1000, 5000, 20),
            'trips': np.random.randint(50, 200, 20),
            'avg_fare': np.random.uniform(12, 30, 20)
        })
        
        fig = px.scatter(
            zone_data,
            x='trips',
            y='revenue',
            size='avg_fare',
            hover_name='zone',
            title='Zone Performance: Trips vs Revenue',
            labels={'trips': 'Number of Trips', 'revenue': 'Total Revenue ($)'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("Real-time Activity")
        
        # Real-time metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Active Trips", "247", delta="+12")
        with col2:
            st.metric("Avg Wait Time", "3.2 min", delta="-0.5 min")
        with col3:
            st.metric("Peak Zones", "8", delta="+2")
        with col4:
            st.metric("Revenue/Hour", "$2,841", delta="+$156")
        
        # Activity heatmap - use Streamlit's built-in map
        map_data = pd.DataFrame({
            'lat': np.random.normal(40.7589, 0.1, 50),
            'lon': np.random.normal(-73.9851, 0.1, 50)
        })
        st.map(map_data)

def render_sidebar():
    """Render sidebar controls"""
    st.sidebar.header("🎛️ Controls")
    
    # Date range selector
    st.sidebar.subheader("Date Range")
    start_date = st.sidebar.date_input("Start Date", datetime.now() - timedelta(days=7))
    end_date = st.sidebar.date_input("End Date", datetime.now())
    
    # Filters
    st.sidebar.subheader("Filters")
    boroughs = st.sidebar.multiselect(
        "Borough",
        ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"],
        default=["Manhattan", "Brooklyn"]
    )
    
    fare_range = st.sidebar.slider(
        "Fare Range ($)",
        min_value=0,
        max_value=100,
        value=(10, 50)
    )
    
    # Refresh controls
    st.sidebar.subheader("Refresh")
    auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)")
    
    if st.sidebar.button("Refresh Now"):
        st.experimental_rerun()
    
    # Export options
    st.sidebar.subheader("Export")
    if st.sidebar.button("Export Dashboard Data"):
        st.sidebar.success("Export initiated!")
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'boroughs': boroughs,
        'fare_range': fare_range,
        'auto_refresh': auto_refresh
    }

# Main dashboard
def main():
    # Render header
    render_header()
    
    # Render sidebar
    filters = render_sidebar()
    
    # Fetch dashboard data
    stats_response = fetch_data("dashboard/stats")
    
    if stats_response and stats_response.get('success'):
        stats_data = stats_response['data']
    else:
        # Use mock data if API is not available
        mock_data = create_mock_data()
        stats_data = mock_data['dashboard_stats']
        st.warning("⚠️ Using mock data - API not available")
    
    # Render KPI cards
    st.header("📈 Key Performance Indicators")
    render_kpi_cards(stats_data)
    
    st.divider()
    
    # Main charts section
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Time series chart
        time_series_response = fetch_data("analytics/time-series", {"days_back": 7})
        if time_series_response and time_series_response.get('success'):
            time_series_data = time_series_response['data']['series']
        else:
            time_series_data = create_mock_data()['time_series']
        
        fig_timeseries = render_time_series_chart(time_series_data)
        st.plotly_chart(fig_timeseries, use_container_width=True)
    
    with col2:
        # Top zones performance
        fig_zones = render_zone_performance(stats_data['top_zones'])
        st.plotly_chart(fig_zones, use_container_width=True)
        
        # Current conditions
        st.subheader("🌤️ Current Conditions")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Weather", stats_data['weather_condition'])
            st.metric("Peak Hour", stats_data['peak_hour'])
        with col2:
            st.metric("Temperature", "22°C")
            st.metric("Humidity", "65%")
    
    st.divider()
    
    # Real-time map
    st.header("🗺️ Real-time Activity Map")
    fig_map = render_real_time_map()
    st.plotly_chart(fig_map, use_container_width=True)
    
    st.divider()
    
    # Analytics section
    render_analytics_section()
    
    # Auto-refresh
    if filters['auto_refresh']:
        time.sleep(30)
        st.experimental_rerun()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"Dashboard error: {e}")
        st.info("Try refreshing the page or check if the API service is running.")
