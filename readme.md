
# Cloud Data Warehouse Pipeline
A production-ready data engineering pipeline for processing e-commerce data using PySpark for ETL, FastAPI for analytics APIs, and Apache Airflow for orchestration, with data stored in AWS S3.

## Architecture

- **Data Processing**: PySpark ETL with optimized transformations
- **Storage**: AWS S3 with Parquet format and Snappy compression
- **API Layer**: FastAPI with JWT authentication
- **Orchestration**: Apache Airflow for workflow management
- **Deployment**: Docker containerization

## Project Structure
cloud-data-warehouse/
├── databricks/notebooks/
│   ├── 01_data_generation.py
│   ├── 02_etl_pipeline.py
│   └── 03_performance_benchmark.py
├── api/
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── airflow/dags/
│   └── data_warehouse_etl.py
├── docker-compose.yml
└── README.md

## Quick Start

### Prerequisites
- Python 3.8+
- Apache Spark
- Docker & Docker Compose
- AWS Account (for S3 storage)

### Local Development

1. **Clone and setup environment**


git clone https://github.com/yashtyagii8447/cloud-data-warehouse.git
cd cloud-data-warehouse
python -m venv venv
source venv/bin/activate
pip install pyspark fastapi uvicorn apache-airflow boto3


2. **Run the complete pipeline**


# 1. Generate sample data
python databricks/notebooks/01_data_generation.py

# 2. Execute ETL with S3 upload
python databricks/notebooks/02_etl_pipeline.py

# 3. Start analytics API
cd api && python main.py


3. **Test API endpoints**


# Health check
curl http://localhost:8000/health

# Get analytics (requires authentication)
curl -H "Authorization: Bearer demo-token-123" "http://localhost:8000/analytics/daily?days=7"


### Docker Deployment


docker-compose up -d


### Airflow Orchestration


export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --password admin --role Admin

# Start services
airflow scheduler
airflow api-server -p 8080

Access Airflow UI at http://localhost:8080

**Note**: If you encounter deprecation warnings (e.g., `grid_view_sorting_order` or `access_logfile`) or auth manager errors, update the `airflow.cfg` file:
- Move `grid_view_sorting_order` from `[webserver]` to `[api]`.
- Rename `access_logfile` to `log_config` in `[api]`.
- Set `auth_manager` in `[core]` to `airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` and install the FAB provider:
  ```bash
  pip install apache-airflow-providers-fab

  airflow db init


## Features

### ETL Pipeline
- Processes 100K+ e-commerce transactions
- Calculates business metrics (profit, margins, aggregations)
- Optimized with Spark adaptive query execution
- Automatic S3 upload of processed data
- Data partitioning and compression

### API Endpoints
- `GET /analytics/daily` - Daily business metrics
- `GET /analytics/products` - Product performance
- `GET /health` - Service status
- JWT authentication for secure access

### AWS S3 Integration
- Automated upload of processed Parquet files
- Organized bucket structure with date partitioning
- Cost-optimized storage with compression

## Configuration
Set environment variables for AWS:


export AWS_ACCESS_KEY_ID=access_key
export AWS_SECRET_ACCESS_KEY=secret_key
export S3_BUCKET=bucket-name


## Production Deployment
For production environments:
- Configure Databricks for Spark processing
- Set up AWS credentials properly
- Use managed Airflow service (MWAA)
- Implement monitoring and alerting


