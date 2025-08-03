# Lakehouse Analytics Serving Layer

This directory contains the serving layer for the Lakehouse Analytics platform, providing REST APIs and interactive dashboards for data access and visualization.

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │───▶│   FastAPI       │───▶│   Streamlit     │
│   (Data Store)  │    │   (REST API)    │    │   (Dashboard)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                        │                        │
        ▼                        ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Redis       │    │   API Clients   │    │   Web Browser   │
│   (Caching)     │    │   (Libraries)   │    │   (Users)       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Features

### REST API (FastAPI)
- **Trip Analytics**: Recent trips, zone metrics, time series
- **Weather Impact**: Correlation analysis between weather and demand
- **Demand Prediction**: ML-powered forecasting
- **Real-time Activity**: Live zone activity monitoring
- **Data Export**: JSON/CSV export capabilities
- **Authentication**: API key-based security
- **Documentation**: Auto-generated OpenAPI docs

### Interactive Dashboard (Streamlit)
- **Real-time KPIs**: Live metrics and performance indicators
- **Time Series Charts**: Trip volume, revenue, and fare trends
- **Zone Performance**: Geographic and performance analysis
- **Weather Impact**: Visual correlation analysis
- **Demand Forecasting**: Predictive analytics visualization
- **Activity Map**: Real-time zone activity heatmap
- **Filters & Controls**: Interactive data exploration

### Client Library
- **Python SDK**: Easy integration with other applications
- **DataFrame Support**: Pandas integration for data analysis
- **Error Handling**: Robust error handling and retries
- **Examples**: Sample analysis and usage patterns

## 🛠️ Setup and Installation

### Prerequisites
- Python 3.11+
- Docker & Docker Compose (optional)
- PostgreSQL database (from main pipeline)

### Local Development

1. **Create virtual environment:**
```bash
cd serving
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

3. **Set environment variables:**
```bash
cp .env.example .env
# Edit .env with your database credentials
```

4. **Start services:**
```bash
# Start both API and Dashboard
./start.sh dev

# Or start individually
./start.sh api      # API only on port 8000
./start.sh dashboard # Dashboard only on port 8501
```

### Docker Deployment

```bash
# From the main project directory
docker-compose up -d analytics-api analytics-dashboard

# Check logs
docker logs analytics-api
docker logs analytics-dashboard
```

## 📊 API Endpoints

### Health & Status
- `GET /health` - Health check
- `GET /api/v1/dashboard/stats` - Dashboard statistics

### Trip Data
- `GET /api/v1/trips/recent` - Recent trips
  - Parameters: `limit`, `hours_back`

### Analytics
- `GET /api/v1/analytics/zones` - Zone performance metrics
  - Parameters: `start_date`, `end_date`, `limit`
- `GET /api/v1/analytics/weather-impact` - Weather correlation
  - Parameters: `start_date`, `end_date`
- `GET /api/v1/analytics/time-series` - Time series data
  - Parameters: `metric`, `days_back`

### Predictions
- `GET /api/v1/predictions/demand` - Demand forecasting
  - Parameters: `hours_ahead`, `top_zones`

### Real-time
- `GET /api/v1/realtime/activity` - Live zone activity
  - Parameters: `minutes_back`

### Export
- `GET /api/v1/export/trips` - Export trip data
  - Parameters: `start_date`, `end_date`, `format`

## 🎯 Usage Examples

### Using the API directly

```bash
# Get dashboard stats
curl -H "X-API-Key: demo-api-key-2024" \
  http://localhost:8000/api/v1/dashboard/stats

# Get recent trips
curl -H "X-API-Key: demo-api-key-2024" \
  "http://localhost:8000/api/v1/trips/recent?limit=10"

# Get zone metrics
curl -H "X-API-Key: demo-api-key-2024" \
  "http://localhost:8000/api/v1/analytics/zones?start_date=2024-01-01T00:00:00&end_date=2024-01-07T23:59:59"
```

### Using the Python client

```python
from client import LakehouseClient
from datetime import datetime, timedelta

# Initialize client
client = LakehouseClient()

# Get dashboard stats
stats = client.get_dashboard_stats()
print(f"Total trips today: {stats['data']['total_trips_today']}")

# Get recent trips as DataFrame
trips_df = client.get_recent_trips(limit=100)
print(trips_df.head())

# Get zone performance
end_date = datetime.now()
start_date = end_date - timedelta(days=7)
zones_df = client.get_zone_metrics(start_date, end_date)

# Analyze top performing zones
top_zones = zones_df.nlargest(5, 'total_revenue')
print(top_zones[['zone_name', 'total_revenue', 'total_trips']])
```

### Dashboard Usage

1. **Access Dashboard**: http://localhost:8501
2. **Set Filters**: Use sidebar to filter by date range, borough, fare range
3. **Explore Data**: Navigate through different tabs and sections
4. **Export Data**: Use export buttons to download analysis results
5. **Auto-refresh**: Enable auto-refresh for real-time monitoring

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `POSTGRES_HOST` | PostgreSQL host | localhost |
| `POSTGRES_PORT` | PostgreSQL port | 5432 |
| `POSTGRES_DB` | Database name | taxi_db |
| `POSTGRES_USER` | Database user | postgres |
| `POSTGRES_PASSWORD` | Database password | postgres |
| `API_KEY` | API authentication key | demo-api-key-2024 |
| `REDIS_URL` | Redis connection URL | redis://localhost:6379 |
| `CACHE_TTL` | Cache time-to-live (seconds) | 300 |

### API Configuration

The API supports various configuration options:

```python
# config.py
class Settings(BaseSettings):
    api_title: str = "Lakehouse Analytics API"
    api_version: str = "1.0.0"
    api_port: int = 8000
    cache_ttl: int = 300
    # ... more settings
```

## 📈 Performance & Scaling

### Caching Strategy
- **Redis caching** for expensive queries
- **Query result caching** with configurable TTL
- **Connection pooling** for database efficiency

### Scaling Options
1. **Horizontal scaling**: Multiple API instances behind load balancer
2. **Database optimization**: Read replicas, indexing, partitioning
3. **Caching layers**: Redis cluster, CDN for static assets
4. **Container orchestration**: Kubernetes deployment

## 🔒 Security

### Authentication
- **API Key authentication** for all endpoints
- **CORS configuration** for web access
- **Rate limiting** (can be added)

### Best Practices
- Use environment variables for sensitive data
- Implement proper error handling
- Validate input parameters
- Monitor API usage and performance

## 🧪 Testing

### Run Tests
```bash
# Install test dependencies
pip install pytest pytest-asyncio httpx

# Run API tests
pytest tests/test_api.py

# Run client tests
pytest tests/test_client.py
```

### Manual Testing
1. **API Documentation**: http://localhost:8000/docs
2. **Interactive testing**: Use Swagger UI
3. **Dashboard testing**: Navigate through all features
4. **Load testing**: Use tools like `ab` or `wrk`

## 🚨 Monitoring & Logging

### Logging
- **Structured logging** with timestamps and levels
- **API request/response logging**
- **Error tracking** and alerts
- **Performance metrics**

### Health Checks
- **API health endpoint**: `/health`
- **Database connectivity checks**
- **Cache system status**
- **External service dependencies**

## 🔧 Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check PostgreSQL is running
   - Verify connection parameters
   - Check network connectivity

2. **API Key Authentication Error**
   - Verify API key in headers
   - Check environment variables
   - Confirm key format

3. **Dashboard Not Loading**
   - Check API service is running
   - Verify API URL configuration
   - Check browser console for errors

4. **Slow Query Performance**
   - Check database indexes
   - Review query execution plans
   - Consider query optimization

### Debug Commands
```bash
# Check API health
curl http://localhost:8000/health

# View API logs
docker logs analytics-api -f

# View dashboard logs
docker logs analytics-dashboard -f

# Test database connection
python -c "from database import DatabaseService; db = DatabaseService('postgresql://...'); print(db.execute_query('SELECT 1'))"
```

## 🚀 Production Deployment

### Docker Compose Production
```yaml
version: '3.8'
services:
  analytics-api:
    build: ./serving
    environment:
      - ENVIRONMENT=production
      - LOG_LEVEL=INFO
    deploy:
      replicas: 2
      resources:
        limits:
          memory: 512M
        reservations:
          memory: 256M
```

### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: analytics-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: analytics-api
  template:
    metadata:
      labels:
        app: analytics-api
    spec:
      containers:
      - name: api
        image: lakehouse/analytics-api:latest
        ports:
        - containerPort: 8000
        env:
        - name: POSTGRES_HOST
          value: "postgres-service"
```

## 📚 Additional Resources

- **API Documentation**: http://localhost:8000/docs
- **Dashboard Demo**: http://localhost:8501
- **Main Pipeline Documentation**: ../PIPELINE_DOCUMENTATION.md
- **Client Examples**: `client.py`
- **Docker Configuration**: `Dockerfile`

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/new-feature`
3. Make changes and test
4. Submit pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.
