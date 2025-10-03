from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import subprocess
import os
import pandas as pd


# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Python callable functions
def run_data_generation():
    print("Running data generation...")
    result = subprocess.run(
        ["python", "databricks/notebooks/01_data_generation.py"],
        capture_output=True, text=True, cwd=".."
    )
    print("Data generation output:", result.stdout)
    if result.stderr:
        print("Data generation errors:", result.stderr)
    return result.returncode == 0

def run_etl_pipeline():
    print("Running ETL pipeline...")
    result = subprocess.run(
        ["python", "databricks/notebooks/02_etl_pipeline.py"],
        capture_output=True, text=True, cwd=".."
    )
    print("ETL pipeline output:", result.stdout)
    if result.stderr:
        print("ETL pipeline errors:", result.stderr)
    return result.returncode == 0

def run_benchmark():
    print("Running performance benchmark...")
    result = subprocess.run(
        ["python", "databricks/notebooks/03_performance_benchmark.py"],
        capture_output=True, text=True, cwd=".."
    )
    print("Benchmark output:", result.stdout)
    if result.stderr:
        print("Benchmark errors:", result.stderr)
    return result.returncode == 0

def check_api_health():
    try:
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "http://localhost:8000/health"],
            capture_output=True, text=True, timeout=10
        )
        if result.stdout.strip() == "200":
            print("API health check: ✓ Healthy")
            return True
        else:
            print(f"API returned status: {result.stdout}")
            return False
    except Exception as e:
        print(f"API health check failed: {str(e)}")
        return False

def validate_data_quality():
    print("Validating data quality...")
    checks = {}
    
    daily_metrics_path = "/tmp/daily-metrics"
    if os.path.exists(daily_metrics_path):
        try:
            df = pd.read_parquet(daily_metrics_path)
            checks['daily_metrics'] = {
                'exists': True,
                'record_count': len(df),
                'columns': list(df.columns)
            }
        except Exception as e:
            checks['daily_metrics'] = {'exists': True, 'error': str(e)}
    else:
        checks['daily_metrics'] = {'exists': False}
    
    sample_data_path = "/tmp/sample-data"
    if os.path.exists(sample_data_path):
        try:
            df = pd.read_parquet(sample_data_path)
            checks['sample_data'] = {
                'exists': True,
                'record_count': len(df),
                'columns': list(df.columns)
            }
        except Exception as e:
            checks['sample_data'] = {'exists': True, 'error': str(e)}
    else:
        checks['sample_data'] = {'exists': False}
    
    print("Data quality checks:", checks)
    return checks

# DAG definition
with DAG(
    dag_id='data_warehouse_etl',
    default_args=default_args,
    description='Orchestrated ETL pipeline for data warehouse',
    schedule='@daily',  # Airflow 3.x uses 'schedule' instead of 'schedule_interval'
    catchup=False,
    tags=['data-warehouse', 'etl', 'orchestration']
) as dag:

    start = EmptyOperator(task_id='start')

    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=run_data_generation,
        retries=1,
    )

    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl_pipeline,
        retries=2,
    )

    run_benchmark_task = PythonOperator(
        task_id='run_benchmark',
        python_callable=run_benchmark,
        retries=1,
    )

    check_api = PythonOperator(
        task_id='check_api_health',
        python_callable=check_api_health,
        retries=2,
    )

    validate_data = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
    )

    notify_complete = BashOperator(
        task_id='notify_completion',
        bash_command='echo "ETL pipeline completed successfully on $(date)"',
    )

    end = EmptyOperator(task_id='end')

    # Define DAG workflow
    start >> generate_data >> run_etl >> run_benchmark_task >> check_api >> validate_data >> notify_complete >> end





