#!/usr/bin/env python3
"""
Test script for Stock Market Data Pipeline
This script tests the pipeline components without starting Docker services
"""

import os
import sys
import json
from pathlib import Path

def test_environment():
    """Test environment configuration"""
    print("🔍 Testing Environment Configuration...")
    
    # Check if .env exists
    if not Path('.env').exists():
        print("❌ .env file not found. Please run setup.sh first or copy env.example to .env")
        return False
    
    # Check required environment variables
    required_vars = ['ALPHA_VANTAGE_API_KEY']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var) or os.getenv(var) == f'your_{var.lower().replace("_", "_")}_here':
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing or unconfigured environment variables: {', '.join(missing_vars)}")
        print("   Please update your .env file with actual values")
        return False
    
    print("✅ Environment configuration looks good")
    return True

def test_file_structure():
    """Test project file structure"""
    print("\n📁 Testing Project File Structure...")
    
    required_files = [
        'docker-compose.yml',
        'dags/stock_pipeline.py',
        'scripts/fetch_data.py',
        'requirements.txt',
        'init.sql',
        'README.md',
        'setup.sh'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing required files: {', '.join(missing_files)}")
        return False
    
    print("✅ All required files are present")
    return True

def test_docker_compose():
    """Test Docker Compose configuration"""
    print("\n🐳 Testing Docker Compose Configuration...")
    
    try:
        import yaml
        
        with open('docker-compose.yml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Check required services
        required_services = ['postgres', 'redis', 'airflow-webserver', 'airflow-scheduler', 'airflow-worker']
        missing_services = []
        
        for service in required_services:
            if service not in config.get('services', {}):
                missing_services.append(service)
        
        if missing_services:
            print(f"❌ Missing required services: {', '.join(missing_services)}")
            return False
        
        print("✅ Docker Compose configuration is valid")
        return True
        
    except ImportError:
        print("⚠️  PyYAML not installed, skipping Docker Compose validation")
        return True
    except Exception as e:
        print(f"❌ Error validating Docker Compose: {e}")
        return False

def test_python_scripts():
    """Test Python script syntax"""
    print("\n🐍 Testing Python Scripts...")
    
    python_files = [
        'scripts/fetch_data.py',
        'dags/stock_pipeline.py'
    ]
    
    for file_path in python_files:
        try:
            with open(file_path, 'r') as f:
                compile(f.read(), file_path, 'exec')
            print(f"✅ {file_path} - Syntax OK")
        except SyntaxError as e:
            print(f"❌ {file_path} - Syntax Error: {e}")
            return False
        except Exception as e:
            print(f"❌ {file_path} - Error: {e}")
            return False
    
    return True

def test_database_schema():
    """Test database initialization script"""
    print("\n🗄️  Testing Database Schema...")
    
    try:
        with open('init.sql', 'r') as f:
            sql_content = f.read()
        
        # Check for required table creation
        if 'CREATE TABLE' not in sql_content or 'stock_prices' not in sql_content:
            print("❌ Database schema missing required table creation")
            return False
        
        # Check for required constraints
        if 'UNIQUE' not in sql_content or 'PRIMARY KEY' not in sql_content:
            print("❌ Database schema missing required constraints")
            return False
        
        print("✅ Database schema looks good")
        return True
        
    except Exception as e:
        print(f"❌ Error reading database schema: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Stock Market Data Pipeline - Pre-flight Tests")
    print("=" * 50)
    
    tests = [
        test_environment,
        test_file_structure,
        test_docker_compose,
        test_python_scripts,
        test_database_schema
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Your pipeline is ready to run.")
        print("\n🚀 Next steps:")
        print("   1. Run: ./setup.sh")
        print("   2. Or manually: docker-compose up -d")
        print("   3. Access Airflow UI at: http://localhost:8080")
    else:
        print("❌ Some tests failed. Please fix the issues before running the pipeline.")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
