#!/usr/bin/env python3
"""
Stock Market Data Pipeline - Data Fetching and Processing Script

This script fetches stock market data from Alpha Vantage API, processes the JSON response,
and stores it in PostgreSQL with proper error handling and retry logic.
"""

import os
import sys
import json
import logging
import requests
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

@dataclass
class StockData:
    """Data class for stock price information"""
    symbol: str
    timestamp: datetime
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[int]

class StockDataFetcher:
    """Handles fetching stock data from Alpha Vantage API"""
    
    def __init__(self, api_key: str, base_url: str = "https://www.alphavantage.co/query"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'StockMarketPipeline/1.0'
        })
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout))
    )
    def fetch_stock_data(self, symbol: str, function: str = "TIME_SERIES_DAILY") -> Dict[str, Any]:
        """
        Fetch stock data from Alpha Vantage API with retry logic
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL', 'MSFT')
            function: API function to call
            
        Returns:
            Dictionary containing the API response
            
        Raises:
            requests.RequestException: If API request fails after retries
        """
        params = {
            'function': function,
            'symbol': symbol,
            'apikey': self.api_key,
            'outputsize': 'compact'  # Get last 100 data points
        }
        
        try:
            logger.info("Fetching stock data", symbol=symbol, function=function)
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                raise ValueError(f"API Error: {data['Error Message']}")
            
            if 'Note' in data:
                logger.warning("API rate limit warning", note=data['Note'])
                # For production, you might want to implement rate limiting here
            
            return data
            
        except requests.Timeout:
            logger.error("Request timeout", symbol=symbol)
            raise
        except requests.RequestException as e:
            logger.error("Request failed", symbol=symbol, error=str(e))
            raise
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON response", symbol=symbol, error=str(e))
            raise ValueError(f"Invalid JSON response: {e}")
    
    def parse_stock_data(self, raw_data: Dict[str, Any], symbol: str) -> List[StockData]:
        """
        Parse the raw API response into structured StockData objects
        
        Args:
            raw_data: Raw API response
            symbol: Stock symbol
            
        Returns:
            List of StockData objects
        """
        stock_data_list = []
        
        try:
            # Extract time series data
            time_series_key = None
            for key in raw_data.keys():
                if 'Time Series' in key:
                    time_series_key = key
                    break
            
            if not time_series_key:
                logger.error("No time series data found", symbol=symbol, keys=list(raw_data.keys()))
                return []
            
            time_series_data = raw_data[time_series_key]
            
            for timestamp_str, price_data in time_series_data.items():
                try:
                    # Parse timestamp
                    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d')
                    
                    # Extract price data with safe conversion
                    open_price = self._safe_float(price_data.get('1. open'))
                    high_price = self._safe_float(price_data.get('2. high'))
                    low_price = self._safe_float(price_data.get('3. low'))
                    close_price = self._safe_float(price_data.get('4. close'))
                    volume = self._safe_int(price_data.get('5. volume'))
                    
                    # Create StockData object
                    stock_data = StockData(
                        symbol=symbol,
                        timestamp=timestamp,
                        open=open_price,
                        high=high_price,
                        low=low_price,
                        close=close_price,
                        volume=volume
                    )
                    
                    stock_data_list.append(stock_data)
                    
                except (ValueError, TypeError) as e:
                    logger.warning("Failed to parse data point", 
                                 symbol=symbol, timestamp=timestamp_str, error=str(e))
                    continue
            
            logger.info("Successfully parsed stock data", 
                       symbol=symbol, data_points=len(stock_data_list))
            
        except Exception as e:
            logger.error("Failed to parse stock data", symbol=symbol, error=str(e))
            raise
        
        return stock_data_list
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to integer"""
        if value is None:
            return None
        try:
            return int(float(value))  # Handle cases where volume might be float
        except (ValueError, TypeError):
            return None

class DatabaseManager:
    """Handles database operations for stock data"""
    
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params
    
    def get_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**self.connection_params)
            return conn
        except psycopg2.Error as e:
            logger.error("Failed to connect to database", error=str(e))
            raise
    
    def insert_stock_data(self, stock_data_list: List[StockData]) -> int:
        """
        Insert stock data into database with upsert logic
        
        Args:
            stock_data_list: List of StockData objects to insert
            
        Returns:
            Number of records inserted/updated
        """
        if not stock_data_list:
            logger.info("No stock data to insert")
            return 0
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Prepare upsert query
            upsert_query = """
                INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # Prepare data for batch insert
            data_to_insert = []
            for stock_data in stock_data_list:
                data_to_insert.append((
                    stock_data.symbol,
                    stock_data.timestamp,
                    stock_data.open,
                    stock_data.high,
                    stock_data.low,
                    stock_data.close,
                    stock_data.volume
                ))
            
            # Execute batch insert
            cursor.executemany(upsert_query, data_to_insert)
            conn.commit()
            
            inserted_count = len(data_to_insert)
            logger.info("Successfully inserted/updated stock data", 
                       count=inserted_count, symbol=stock_data_list[0].symbol)
            
            return inserted_count
            
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            logger.error("Database operation failed", error=str(e))
            raise
        finally:
            if conn:
                conn.close()
    
    def get_latest_data_timestamp(self, symbol: str) -> Optional[datetime]:
        """
        Get the latest timestamp for a given symbol
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Latest timestamp or None if no data exists
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT MAX(timestamp) 
                FROM stock_prices 
                WHERE symbol = %s
            """
            
            cursor.execute(query, (symbol,))
            result = cursor.fetchone()
            
            return result[0] if result and result[0] else None
            
        except psycopg2.Error as e:
            logger.error("Failed to get latest timestamp", symbol=symbol, error=str(e))
            raise
        finally:
            if conn:
                conn.close()

def main():
    """Main function to orchestrate the data pipeline"""
    
    # Load environment variables
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        logger.error("ALPHA_VANTAGE_API_KEY environment variable not set")
        sys.exit(1)
    
    # Database connection parameters
    db_params = {
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'stockmarket'),
        'user': os.getenv('POSTGRES_USER', 'airflow'),
        'password': os.getenv('POSTGRES_PASSWORD', 'airflow')
    }
    
    # Stock symbols to fetch (configurable via environment variable)
    symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOGL,AMZN,TSLA')
    symbols = [s.strip() for s in symbols_str.split(',')]
    
    try:
        # Initialize components
        fetcher = StockDataFetcher(api_key)
        db_manager = DatabaseManager(db_params)
        
        total_records = 0
        
        # Process each symbol
        for symbol in symbols:
            try:
                logger.info("Processing symbol", symbol=symbol)
                
                # Fetch data from API
                raw_data = fetcher.fetch_stock_data(symbol)
                
                # Parse the data
                stock_data_list = fetcher.parse_stock_data(raw_data, symbol)
                
                if stock_data_list:
                    # Insert into database
                    records_inserted = db_manager.insert_stock_data(stock_data_list)
                    total_records += records_inserted
                    
                    logger.info("Successfully processed symbol", 
                               symbol=symbol, records=records_inserted)
                else:
                    logger.warning("No data to insert for symbol", symbol=symbol)
                
            except Exception as e:
                logger.error("Failed to process symbol", symbol=symbol, error=str(e))
                # Continue with next symbol instead of failing entire pipeline
                continue
        
        logger.info("Pipeline completed successfully", 
                   total_records=total_records, symbols_processed=len(symbols))
        
    except Exception as e:
        logger.error("Pipeline failed", error=str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
