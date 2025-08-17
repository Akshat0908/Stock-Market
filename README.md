# Stock Market Data Pipeline

A production-ready, Dockerized data pipeline for fetching and storing stock market data using Apache Airflow and PostgreSQL.

## 🎯 Overview

This pipeline automatically fetches daily stock market data from Alpha Vantage API, processes the JSON responses, and stores structured data in PostgreSQL with comprehensive error handling and retry logic.

## ✨ Features

- **Automated Data Collection**: Scheduled daily data fetching after market close
- **Robust Error Handling**: Comprehensive retry logic with exponential backoff
- **Data Quality Validation**: Built-in verification of fetched data
- **Scalable Architecture**: Support for multiple stock symbols
- **Production Ready**: Dockerized with proper logging and monitoring
- **Upsert Logic**: Prevents duplicate data with intelligent conflict resolution

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Alpha        │    │   Apache        │    │   PostgreSQL    │
│   Vantage API  │───▶│   Airflow       │───▶│   Database      │
│                 │    │   (Orchestrator)│    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   Redis         │
                       │   (Message      │
                       │    Broker)      │
                       └─────────────────┘
```

## 📋 Prerequisites

- Docker and Docker Compose
- Alpha Vantage API key (free tier available)
- At least 4GB RAM available for containers

## 🚀 Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd stock-market-pipeline

# Copy environment file
cp env.example .env
```

### 2. Configure Environment

Edit the `.env` file with your configuration:

```bash
# Required: Get your free API key from https://www.alphavantage.co/support/#api-key
ALPHA_VANTAGE_API_KEY=your_actual_api_key_here

# Optional: Customize stock symbols
STOCK_SYMBOLS=AAPL,MSFT,GOOGL,AMZN,TSLA

# Generate a Fernet key for Airflow
FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
```

### 3. Start the Pipeline

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f airflow-webserver
```

### 4. Access Airflow UI

- **URL**: http://localhost:8080
- **Username**: admin
- **Password**: admin

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `ALPHA_VANTAGE_API_KEY` | Alpha Vantage API key | - | ✅ |
| `STOCK_SYMBOLS` | Comma-separated stock symbols | AAPL,MSFT,GOOGL,AMZN,TSLA | ❌ |
| `POSTGRES_HOST` | PostgreSQL host | postgres | ❌ |
| `POSTGRES_PORT` | PostgreSQL port | 5432 | ❌ |
| `POSTGRES_DB` | Database name | stockmarket | ❌ |
| `POSTGRES_USER` | Database user | airflow | ❌ |
| `POSTGRES_PASSWORD` | Database password | airflow | ❌ |
| `AIRFLOW_USERNAME` | Airflow admin username | admin | ❌ |
| `AIRFLOW_PASSWORD` | Airflow admin password | admin | ❌ |
| `FERNET_KEY` | Airflow encryption key | - | ✅ |

### Stock Symbols

Configure which stocks to track by setting the `STOCK_SYMBOLS` environment variable:

```bash
# Example: Track major tech companies
STOCK_SYMBOLS=AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA,META

# Example: Track financial companies
STOCK_SYMBOLS=JPM,BAC,WFC,GS,MS
```

## 📊 Database Schema

The pipeline creates and manages the following table structure:

```sql
CREATE TABLE stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open NUMERIC(10,4),
    high NUMERIC(10,4),
    low NUMERIC(10,4),
    close NUMERIC(10,4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_symbol_time UNIQUE (symbol, timestamp)
);
```

### Key Features:
- **Unique Constraint**: Prevents duplicate data for same symbol/timestamp
- **Automatic Timestamps**: Tracks when records are created/updated
- **Indexed Fields**: Optimized for common query patterns

## 🔄 Pipeline Workflow

### Daily Execution Flow

1. **Environment Validation** ✅
   - Checks required environment variables
   - Validates database connectivity

2. **Data Fetching** 📡
   - Retrieves data from Alpha Vantage API
   - Implements retry logic with exponential backoff
   - Handles API rate limiting gracefully

3. **Data Processing** 🔄
   - Parses JSON responses
   - Extracts OHLCV data
   - Validates data integrity

4. **Data Storage** 💾
   - Performs upsert operations
   - Updates existing records
   - Inserts new records

5. **Quality Verification** 🔍
   - Checks data completeness
   - Validates against expected symbols
   - Reports any anomalies

### Schedule

- **Frequency**: Daily at 6:00 PM (18:00)
- **Days**: Monday through Friday (weekdays only)
- **Timing**: After market close for complete daily data

## 📈 Monitoring and Logging

### Airflow UI

- **DAG Status**: Monitor pipeline execution
- **Task Logs**: View detailed execution logs
- **Task History**: Track success/failure rates
- **Scheduling**: Manage pipeline timing

### Logs

All pipeline activities are logged with structured logging:

```json
{
  "timestamp": "2024-01-15T18:00:00Z",
  "level": "info",
  "symbol": "AAPL",
  "data_points": 100,
  "message": "Successfully processed symbol"
}
```

### Health Checks

- **Database**: Connection and query performance
- **API**: Response times and success rates
- **Data Quality**: Completeness and accuracy metrics

## 🚨 Error Handling

### Retry Logic

- **API Failures**: 3 retries with exponential backoff
- **Network Issues**: Automatic retry with increasing delays
- **Database Errors**: Transaction rollback and retry

### Graceful Degradation

- **Partial Failures**: Continue processing other symbols
- **Missing Data**: Log warnings without pipeline failure
- **API Limits**: Handle rate limiting gracefully

## 🔒 Security

### Best Practices

- **Environment Variables**: No hardcoded secrets
- **Database Isolation**: Dedicated database user
- **Network Security**: Containerized services
- **API Key Management**: Secure credential storage

### Access Control

- **Airflow Authentication**: Basic auth with configurable credentials
- **Database Permissions**: Minimal required privileges
- **Container Security**: Non-root user execution

## 📊 Data Analysis

### Sample Queries

```sql
-- Get latest prices for all symbols
SELECT symbol, timestamp, close, volume
FROM stock_prices 
WHERE timestamp = (SELECT MAX(timestamp) FROM stock_prices);

-- Calculate daily returns
SELECT 
    symbol,
    timestamp,
    ((close - open) / open * 100) as daily_return_pct
FROM stock_prices 
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY symbol, timestamp;

-- Find highest volume days
SELECT symbol, timestamp, volume
FROM stock_prices 
WHERE volume = (
    SELECT MAX(volume) 
    FROM stock_prices p2 
    WHERE p2.symbol = stock_prices.symbol
);
```

## 🚀 Scaling and Extensions

### Adding New Data Sources

The pipeline is designed to be easily extended:

1. **Create new fetcher class** in `scripts/fetch_data.py`
2. **Add new DAG tasks** for data source
3. **Configure environment variables** for new APIs
4. **Update database schema** if needed

### Performance Optimization

- **Batch Processing**: Efficient bulk database operations
- **Connection Pooling**: Database connection management
- **Parallel Processing**: Multiple symbols processed concurrently
- **Memory Management**: Optimized data structures

## 🐛 Troubleshooting

### Common Issues

#### Pipeline Not Starting
```bash
# Check service status
docker-compose ps

# View logs
docker-compose logs airflow-scheduler

# Verify environment variables
docker-compose exec airflow-webserver env | grep ALPHA_VANTAGE
```

#### Database Connection Issues
```bash
# Test database connectivity
docker-compose exec postgres psql -U airflow -d stockmarket -c "SELECT 1;"

# Check database logs
docker-compose logs postgres
```

#### API Rate Limiting
```bash
# Check API response logs
docker-compose logs airflow-worker | grep "API rate limit"

# Adjust scheduling if needed
# Edit the DAG schedule_interval in dags/stock_pipeline.py
```

### Debug Mode

Enable verbose logging by setting:

```bash
LOG_LEVEL=DEBUG
```

## 📚 API Documentation

### Alpha Vantage API

- **Endpoint**: https://www.alphavantage.co/query
- **Function**: TIME_SERIES_DAILY
- **Rate Limits**: 5 calls per minute (free tier)
- **Data Format**: JSON with OHLCV data

### Response Structure

```json
{
  "Meta Data": {
    "1. Information": "Daily Prices (open, high, low, close) and Volumes",
    "2. Symbol": "AAPL",
    "3. Last Refreshed": "2024-01-15",
    "4. Output Size": "Compact",
    "5. Time Zone": "US/Eastern"
  },
  "Time Series (Daily)": {
    "2024-01-15": {
      "1. open": "185.59",
      "2. high": "186.12",
      "3. low": "183.62",
      "4. close": "185.14",
      "5. volume": "52455980"
    }
  }
}
```

## 🤝 Contributing

### Development Setup

1. **Fork the repository**
2. **Create feature branch**: `git checkout -b feature/new-feature`
3. **Make changes** and test locally
4. **Submit pull request** with detailed description

### Testing

```bash
# Run tests
docker-compose exec airflow-worker python -m pytest

# Check code quality
docker-compose exec airflow-worker flake8 scripts/ dags/
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Alpha Vantage**: Free stock market data API
- **Apache Airflow**: Workflow orchestration platform
- **PostgreSQL**: Reliable database system
- **Docker**: Containerization platform

## 📞 Support

For issues and questions:

1. **Check the troubleshooting section** above
2. **Review Airflow logs** for detailed error information
3. **Open an issue** on the project repository
4. **Check Airflow documentation** for general questions

---

**Happy Data Pipelining! 🚀📊**
