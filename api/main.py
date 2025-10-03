from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
import pyarrow.parquet as pq
import os
from datetime import datetime, timedelta
import time

# Initialize app
app = FastAPI(
    title="Data Warehouse API",
    description="API for accessing processed analytics data",
    version="1.0.0"
)
security = HTTPBearer()

# Models
class DailyMetrics(BaseModel):
    date: str
    total_revenue: float
    total_profit: float
    avg_profit_margin: float
    transaction_count: int
    unique_products: int
    unique_customers: int

class AnalyticsResponse(BaseModel):
    data: List[DailyMetrics]
    query_time_ms: float
    records_returned: int

class ProductAnalytics(BaseModel):
    product_id: int
    total_revenue: float
    total_profit: float
    transaction_count: int
    avg_profit_margin: float

# Authentication
def validate_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Simple token validation for demo"""
    token = credentials.credentials
    if token != "demo-token-123":
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"user": "demo-user", "role": "analyst"}

def read_parquet_files(directory_path):
    """Read parquet files from directory"""
    try:
        table = pq.read_table(directory_path)
        return table.to_pandas()
    except Exception as e:
        print(f"Error reading parquet files: {e}")
        return pd.DataFrame()

# API Endpoints
@app.get("/")
async def root():
    return {
        "message": "Data Warehouse Analytics API",
        "version": "1.0.0",
        "endpoints": {
            "/analytics/daily": "Get daily metrics",
            "/analytics/products": "Get product analytics",
            "/health": "API health check",
            "/performance": "ETL performance metrics"
        }
    }

@app.get("/analytics/daily", response_model=AnalyticsResponse)
async def get_daily_metrics(
    days: int = 7,
    min_revenue: Optional[float] = None,
    token: dict = Depends(validate_token)
):
    start_time = time.time()
    
    try:
        # Read metrics from our ETL output
        metrics_df = read_parquet_files("/tmp/daily-metrics")
        
        if metrics_df.empty:
            # Fallback to mock data if no ETL data found
            metrics_df = generate_mock_metrics()
        
        # Filter data
        if min_revenue is not None:
            metrics_df = metrics_df[metrics_df['total_revenue'] >= min_revenue]
        
        # Get latest N days
        metrics_df = metrics_df.sort_values('date', ascending=False).head(days)
        
        # Convert to response format
        metrics_data = []
        for _, row in metrics_df.iterrows():
            metrics_data.append(DailyMetrics(
                date=row['date'],
                total_revenue=float(row['total_revenue']),
                total_profit=float(row['total_profit']),
                avg_profit_margin=float(row['avg_profit_margin']),
                transaction_count=int(row['transaction_count']),
                unique_products=int(row['unique_products']),
                unique_customers=int(row['unique_customers'])
            ))
        
        query_time = (time.time() - start_time) * 1000
        
        return AnalyticsResponse(
            data=metrics_data,
            query_time_ms=round(query_time, 2),
            records_returned=len(metrics_data)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {str(e)}")

@app.get("/analytics/products", response_model=List[ProductAnalytics])
async def get_product_analytics(
    top_n: int = 10,
    token: dict = Depends(validate_token)
):
    """Get top N products by revenue"""
    start_time = time.time()
    
    try:
        # Read sample data to analyze products
        sample_df = read_parquet_files("/tmp/sample-data")
        
        if sample_df.empty:
            # Generate mock product data if no sample data
            products = []
            for i in range(1, top_n + 1):
                products.append({
                    "product_id": i,
                    "total_revenue": 5000 + i * 1000,
                    "total_profit": 1500 + i * 300,
                    "transaction_count": 50 + i * 10,
                    "avg_profit_margin": 0.25 + (i * 0.02)
                })
            return products
        
        # Calculate product metrics from sample data
        product_metrics = sample_df.groupby('product_id').agg({
            'revenue': 'sum',
            'profit': 'sum',
            'transaction_id': 'count',
            'profit_margin': 'mean'
        }).reset_index()
        
        product_metrics = product_metrics.rename(columns={
            'revenue': 'total_revenue',
            'profit': 'total_profit',
            'transaction_id': 'transaction_count',
            'profit_margin': 'avg_profit_margin'
        })
        
        # Get top N products by revenue
        top_products = product_metrics.nlargest(top_n, 'total_revenue')
        
        # Convert to response format
        products_data = []
        for _, row in top_products.iterrows():
            products_data.append(ProductAnalytics(
                product_id=int(row['product_id']),
                total_revenue=float(row['total_revenue']),
                total_profit=float(row['total_profit']),
                transaction_count=int(row['transaction_count']),
                avg_profit_margin=float(row['avg_profit_margin'])
            ))
        
        return products_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving product data: {str(e)}")

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

@app.get("/performance")
async def performance_stats():
    """Show ETL performance metrics"""
    try:
        # Check if ETL data exists
        metrics_exists = os.path.exists("/tmp/daily-metrics")
        sample_exists = os.path.exists("/tmp/sample-data")
        
        return {
            "etl_status": "completed" if metrics_exists and sample_exists else "no_data",
            "data_available": {
                "daily_metrics": metrics_exists,
                "sample_data": sample_exists
            },
            "last_checked": datetime.now().isoformat(),
            "message": "Run ETL pipeline first if no data available"
        }
    except Exception as e:
        return {"error": str(e)}

def generate_mock_metrics():
    """Generate mock metrics data for demo purposes"""
    metrics = []
    for i in range(30):
        day = (datetime.now() - timedelta(days=30-i)).strftime("%Y-%m-%d")
        metrics.append({
            'date': day,
            'total_revenue': 10000 + i * 500,
            'total_profit': 3000 + i * 150,
            'avg_profit_margin': 0.25 + (i * 0.01),
            'transaction_count': 500 + i * 20,
            'unique_products': 20 + i,
            'unique_customers': 150 + i * 5
        })
    return pd.DataFrame(metrics)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)



