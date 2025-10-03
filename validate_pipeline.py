
# Pipeline validation script

import requests
import subprocess
import time
import os

def run_validation():
    print("=== PIPELINE VALIDATION ===")
    print("#" * 50)
    
    # Test 1: Check ETL data
    print("1. Checking ETL data...")
    metrics_exists = os.path.exists("/tmp/daily-metrics")
    sample_exists = os.path.exists("/tmp/sample-data")
    
    if metrics_exists and sample_exists:
        print("   ETL data found in /tmp/")
    else:
        print("    ETL data not found - run ETL pipeline first")
    
    # Test 2: Test API
    print("2. Testing API service...")
    try:
        # Start API if not running
        try:
            response = requests.get("http://localhost:8000/health", timeout=2)
            api_running = response.status_code == 200
        except:
            api_running = False
            
        if not api_running:
            print("   Starting API service...")
            subprocess.Popen(["python", "api/main.py"], cwd="api")
            time.sleep(3)
        
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            print("   API is healthy")
        else:
            print(f"   API status: {response.status_code}")
    except Exception as e:
        print(f"   API test failed: {e}")
    
    # Test 3: Test analytics endpoints
    print("3. Testing analytics endpoints...")
    try:
        headers = {"Authorization": "Bearer demo-token-123"}
        
        # Test daily metrics
        response = requests.get(
            "http://localhost:8000/analytics/daily?days=3",
            headers=headers, timeout=5
        )
        if response.status_code == 200:
            data = response.json()
            print(f"   Daily analytics: {data['records_returned']} records")
        else:
            print(f"   Daily analytics: {response.status_code}")
        
        # Test product analytics
        response = requests.get(
            "http://localhost:8000/analytics/products?top_n=3",
            headers=headers, timeout=5
        )
        if response.status_code == 200:
            products = response.json()
            print(f"   Product analytics: {len(products)} products")
        else:
            print(f"  Product analytics: {response.status_code}")
            
    except Exception as e:
        print(f"   Analytics test failed: {e}")
    
    # Test 4: Check Airflow
    print("4. Checking Airflow...")
    try:
        result = subprocess.run([
            "airflow", "version"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("   Airflow is installed")
        else:
            print("    Airflow not installed or configured")
    except Exception as e:
        print(f"   Airflow check failed: {e}")
    
    print("\n" + "=" * 50)
    print("VALIDATION COMPLETE")
    print("=" * 50)
    
    print("\nNext steps:")
    print("1. Run the ETL pipeline: python databricks/notebooks/02_etl_pipeline.py")
    print("2. Start the API: cd api && python main.py")
    print("3. Access Airflow: airflow api-server -p 8080")
    print("4. Test endpoints with: curl -H 'Authorization: Bearer demo-token-123' http://localhost:8000/analytics/daily")

if __name__ == "__main__":
    run_validation()