"""
Stock Market Data Pipeline DAG

This DAG fetches stock market data from Alpha Vantage API on a daily schedule,
processes the data, and stores it in PostgreSQL with comprehensive error handling.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import os

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    'stock_market_pipeline',
    default_args=default_args,
    description='Daily stock market data pipeline',
    schedule_interval='0 18 * * 1-5',  # Run at 6 PM on weekdays (after market close)
    catchup=False,
    max_active_runs=1,
    tags=['stock-market', 'data-pipeline', 'financial-data'],
    doc_md="""
    ## Stock Market Data Pipeline
    
    This DAG fetches daily stock market data from Alpha Vantage API and stores it in PostgreSQL.
    
    ### Pipeline Steps:
    1. **Validate Environment**: Check required environment variables
    2. **Fetch Stock Data**: Retrieve data for configured stock symbols
    3. **Process and Store**: Parse JSON and insert into database
    4. **Verify Data**: Check data quality and completeness
    
    ### Configuration:
    - **STOCK_SYMBOLS**: Comma-separated list of stock symbols (default: AAPL,MSFT,GOOGL,AMZN,TSLA)
    - **ALPHA_VANTAGE_API_KEY**: API key for Alpha Vantage
    - **POSTGRES_***: Database connection parameters
    
    ### Schedule:
    - Runs daily at 6 PM on weekdays (Monday-Friday)
    - Designed to run after market close for complete daily data
    """
)

def validate_environment():
    """Validate that all required environment variables are set"""
    import logging
    
    logger = logging.getLogger(__name__)
    required_vars = ['ALPHA_VANTAGE_API_KEY']
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info("Environment validation successful")
    return "Environment validation passed"

def fetch_stock_data(**context):
    """Execute the stock data fetching script"""
    import subprocess
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        # Execute the fetch_data.py script
        result = subprocess.run(
            ['python', '/opt/airflow/scripts/fetch_data.py'],
            capture_output=True,
            text=True,
            cwd='/opt/airflow/scripts',
            env=os.environ.copy()
        )
        
        if result.returncode != 0:
            logger.error(f"Script execution failed: {result.stderr}")
            raise RuntimeError(f"Script execution failed with return code {result.returncode}")
        
        logger.info("Stock data fetching completed successfully")
        logger.info(f"Script output: {result.stdout}")
        
        return "Data fetching completed successfully"
        
    except subprocess.SubprocessError as e:
        logger.error(f"Subprocess error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during data fetching: {e}")
        raise

def verify_data_quality(**context):
    """Verify the quality and completeness of fetched data"""
    import psycopg2
    import logging
    from datetime import datetime, timedelta
    
    logger = logging.getLogger(__name__)
    
    # Database connection parameters
    db_params = {
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'stockmarket'),
        'user': os.getenv('POSTGRES_USER', 'airflow'),
        'password': os.getenv('POSTGRES_PASSWORD', 'airflow')
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Get today's date (market close)
        today = datetime.now().date()
        
        # Check if we have data for today
        cursor.execute("""
            SELECT symbol, COUNT(*) as data_points
            FROM stock_prices 
            WHERE DATE(timestamp) = %s
            GROUP BY symbol
        """, (today,))
        
        today_data = cursor.fetchall()
        
        # Get configured symbols
        symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,MSFT,GOOGL,AMZN,TSLA')
        expected_symbols = [s.strip() for s in symbols_str.split(',')]
        
        # Verify data completeness
        missing_symbols = []
        for symbol in expected_symbols:
            symbol_data = [row for row in today_data if row[0] == symbol]
            if not symbol_data:
                missing_symbols.append(symbol)
            else:
                data_points = symbol_data[0][1]
                logger.info(f"Symbol {symbol}: {data_points} data points for today")
        
        if missing_symbols:
            logger.warning(f"Missing data for symbols: {missing_symbols}")
        else:
            logger.info("All expected symbols have data for today")
        
        # Check for any NULL values in critical fields
        cursor.execute("""
            SELECT symbol, COUNT(*) as null_records
            FROM stock_prices 
            WHERE DATE(timestamp) = %s 
            AND (open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL)
            GROUP BY symbol
        """, (today,))
        
        null_data = cursor.fetchall()
        if null_data:
            for symbol, null_count in null_data:
                logger.warning(f"Symbol {symbol}: {null_count} records with NULL values")
        else:
            logger.info("No NULL values found in critical fields")
        
        # Get total record count for today
        cursor.execute("""
            SELECT COUNT(*) FROM stock_prices WHERE DATE(timestamp) = %s
        """, (today,))
        
        total_records = cursor.fetchone()[0]
        logger.info(f"Total records for today: {total_records}")
        
        conn.close()
        
        return f"Data quality verification completed. Total records: {total_records}"
        
    except psycopg2.Error as e:
        logger.error(f"Database error during verification: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during verification: {e}")
        raise

# Task definitions
validate_env_task = PythonOperator(
    task_id='validate_environment',
    python_callable=validate_environment,
    dag=dag,
)

fetch_data_task = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=fetch_stock_data,
    dag=dag,
)

verify_data_task = PythonOperator(
    task_id='verify_data_quality',
    python_callable=verify_data_quality,
    dag=dag,
)

# Task dependencies
validate_env_task >> fetch_data_task >> verify_data_task

# Add documentation
dag.doc_md = """
## Stock Market Data Pipeline

This DAG automates the daily collection of stock market data from Alpha Vantage API.

### What it does:
- Fetches daily OHLCV data for configured stock symbols
- Processes and validates the data
- Stores data in PostgreSQL with upsert logic
- Verifies data quality and completeness

### When it runs:
- **Schedule**: Daily at 6 PM on weekdays (Monday-Friday)
- **Timing**: After market close to ensure complete daily data

### Configuration:
Set these environment variables in your `.env` file:
- `ALPHA_VANTAGE_API_KEY`: Your Alpha Vantage API key
- `STOCK_SYMBOLS`: Comma-separated list of stock symbols
- `POSTGRES_*`: Database connection parameters

### Monitoring:
- Check the Airflow UI for task status and logs
- Monitor data quality metrics in the verification task
- Review logs for any API rate limiting or data issues
"""
