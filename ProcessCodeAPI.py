from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import pyodbc
import uvicorn
from typing import Optional, List, Dict, Any
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Process Code API",
    description="API for accessing ModuleBOM data from PQRA database",
    version="1.0.0"
)

security = HTTPBasic()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables to store data
module_bom_simple_data = pd.DataFrame()
last_data_load = None

# Database configuration
DATABASE_CONFIG = {
    "server": "BOMSSPROD367\\BOMSSPROD367",
    "database": "PQRA",
    "driver": "ODBC Driver 17 for SQL Server",  # or "SQL Server" if ODBC Driver 17 is not available
    "trusted_connection": "yes",  # Use Windows Authentication
    "timeout": 30
}

def get_connection_string():
    """Create SQL Server connection string for PQRA database"""
    return (
        f"mssql+pyodbc://@{DATABASE_CONFIG['server']}/{DATABASE_CONFIG['database']}"
        f"?driver={DATABASE_CONFIG['driver'].replace(' ', '+')}"
        f"&trusted_connection={DATABASE_CONFIG['trusted_connection']}"
        f"&timeout={DATABASE_CONFIG['timeout']}"
    )

def get_direct_pyodbc_connection():
    """Create direct pyodbc connection for better error handling"""
    conn_str = (
        f"DRIVER={{{DATABASE_CONFIG['driver']}}};"
        f"SERVER={DATABASE_CONFIG['server']};"
        f"DATABASE={DATABASE_CONFIG['database']};"
        f"Trusted_Connection={DATABASE_CONFIG['trusted_connection']};"
        f"Connection Timeout={DATABASE_CONFIG['timeout']};"
    )
    return pyodbc.connect(conn_str)

def test_database_connection():
    """Test database connectivity"""
    try:
        logger.info("Testing database connection...")
        
        # Test with pyodbc first
        conn = get_direct_pyodbc_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        logger.info("Database connection successful")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return False

def load_data_from_database():
    """Load ModuleBOM data from PQRA database"""
    global module_bom_simple_data, last_data_load
    
    try:
        logger.info("Starting data load from PQRA database...")
        
        # Create SQLAlchemy engine
        connection_string = get_connection_string()
        engine = create_engine(connection_string, echo=False)
        
        # Load ModuleBOM_Simple data
        logger.info("Loading ModuleBOM_Simple data...")
        modulebom_simple_query = """
        SELECT 
            [Material Number],
            [Material Description],
            [Process Code],
            [Supplier],
            [Component Type],
            [Plant],
            [Storage Location],
            [Batch],
            [Special Stock],
            [Stock Type],
            [Unit of Entry],
            [Quantity],
            [Amount in LC],
            [Currency],
            [Price Unit],
            [Material Document],
            [Material Doc Year],
            [Material Doc Item],
            [Posting Date],
            [Document Date],
            [Reference],
            [User Name],
            [Transaction Code],
            [Movement Type],
            [Vendor],
            [Customer],
            [Sales Document],
            [Sales Document Item],
            [WBS Element],
            [GL Account],
            [Cost Center],
            [Profit Center],
            [Order Number],
            [Reservation],
            [Reservation Item],
            [Goods Movement Reason Code],
            [Delivery Note],
            [Delivery Note Item],
            [Version],
            [Fiscal Year Variant],
            [Period],
            [Fiscal Year],
            [Entry Date],
            [Time of Entry],
            [Created By],
            [Changed By],
            [Last Changed]
        FROM [PQRA].[dbo].[ModuleBOM_Simple]
        WHERE [Material Description] IS NOT NULL
        """
        
        module_bom_simple_data = pd.read_sql(modulebom_simple_query, engine)
        logger.info(f"Loaded {len(module_bom_simple_data)} records from ModuleBOM_Simple")
        
        # Close the engine
        engine.dispose()
        
        # Update last load time
        last_data_load = datetime.now()
        
        logger.info("Data load completed successfully")
        
        # Clean up data
        clean_data()
        
    except Exception as e:
        logger.error(f"Error loading data from database: {str(e)}")
        # Initialize empty DataFrame on error
        module_bom_simple_data = pd.DataFrame()
        raise

def clean_data():
    """Clean and standardize the loaded data"""
    global module_bom_simple_data
    
    try:
        # Clean ModuleBOM_Simple data
        if not module_bom_simple_data.empty:
            # Remove rows with null Material Description
            module_bom_simple_data = module_bom_simple_data.dropna(subset=['Material Description'])
            
            # Clean Material Description column
            module_bom_simple_data['Material Description'] = module_bom_simple_data['Material Description'].astype(str).str.strip()
            
            # Remove rows where Material Description is empty or 'nan'
            module_bom_simple_data = module_bom_simple_data[
                (module_bom_simple_data['Material Description'] != '') &
                (module_bom_simple_data['Material Description'].str.lower() != 'nan')
            ]
            
            # Standardize column names for API consistency
            column_mapping = {
                'Material Description': 'Material_Description',
                'Material Number': 'Material_Number',
                'Process Code': 'Process_Code',
                'Component Type': 'Component_Type'
            }
            
            for old_col, new_col in column_mapping.items():
                if old_col in module_bom_simple_data.columns:
                    module_bom_simple_data[new_col] = module_bom_simple_data[old_col]
        
        logger.info("Data cleaning completed")
        
    except Exception as e:
        logger.error(f"Error cleaning data: {str(e)}")

def verify_credentials(credentials: HTTPBasicCredentials):
    """Verify basic authentication credentials"""
    # Replace with your actual authentication logic
    valid_users = {
        "api_user": "ProcessCodeAdmin",
        "admin": "MicronPC123",
        "process_code_user": "process_code_pass"
    }
    
    if credentials.username not in valid_users or valid_users[credentials.username] != credentials.password:
        raise HTTPException(
            status_code=401, 
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"}
        )
    
    return credentials.username

@app.on_event("startup")
async def startup_event():
    """Initialize the API on startup"""
    logger.info("Starting Process Code API...")
    
    # Test database connection
    if test_database_connection():
        try:
            load_data_from_database()
            logger.info("API startup completed successfully")
        except Exception as e:
            logger.error(f"Failed to load data during startup: {str(e)}")
            logger.warning("API will start with empty data - use /reload-data endpoint to retry")
    else:
        logger.error("Database connection failed during startup")
        logger.warning("API will start with empty data - check database connectivity")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    db_status = "connected" if test_database_connection() else "disconnected"
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "database_status": db_status,
        "last_data_load": last_data_load.isoformat() if last_data_load else None,
        "records_loaded": {
            "modulebom_simple": len(module_bom_simple_data)
        }
    }

@app.get("/info")
async def get_info():
    """Get API information"""
    return {
        "title": "Process Code API",
        "version": "1.0.0",
        "description": "API for accessing ModuleBOM data from PQRA database",
        "server": "BOMSSPROD367\\BOMSSPROD367",
        "database": "PQRA",
        "last_data_load": last_data_load.isoformat() if last_data_load else None,
        "records_loaded": {
            "modulebom_simple": len(module_bom_simple_data)
        }
    }

@app.get("/modulebom-simple")
async def get_modulebom_simple(
    limit: Optional[int] = Query(None, description="Limit number of records"),
    offset: Optional[int] = Query(0, description="Offset for pagination"),
    username: str = Depends(verify_credentials)
):
    """Get ModuleBOM_Simple data"""
    
    if module_bom_simple_data.empty:
        return []
    
    # Apply pagination
    start_idx = offset
    end_idx = start_idx + limit if limit else len(module_bom_simple_data)
    
    result_data = module_bom_simple_data.iloc[start_idx:end_idx]
    
    # Convert to list of dictionaries, handling NaN values
    records = result_data.to_dict('records')
    
    # Clean up NaN values
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if pd.isna(value):
                cleaned_record[key] = None
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)
    
    return cleaned_records

@app.get("/modulebom-simple/count")
async def get_modulebom_count(username: str = Depends(verify_credentials)):
    """Get count of ModuleBOM_Simple records"""
    return {"count": len(module_bom_simple_data)}

@app.get("/modulebom-simple/info")
async def get_modulebom_info(username: str = Depends(verify_credentials)):
    """Get ModuleBOM_Simple table information"""
    
    if module_bom_simple_data.empty:
        return {"record_count": 0, "columns": []}
    
    return {
        "record_count": len(module_bom_simple_data),
        "columns": list(module_bom_simple_data.columns),
        "last_updated": last_data_load.isoformat() if last_data_load else None
    }

@app.get("/modulebom-simple/search")
async def search_modulebom(
    field: str = Query(..., description="Field to search in"),
    query: str = Query(..., description="Search query"),
    distinct: bool = Query(False, description="Return distinct values only"),
    username: str = Depends(verify_credentials)
):
    """Search ModuleBOM_Simple data"""
    
    if module_bom_simple_data.empty:
        return []
    
    # Check if field exists
    if field not in module_bom_simple_data.columns:
        # Try with standardized field name
        if field == 'Material_Description' and 'Material Description' in module_bom_simple_data.columns:
            field = 'Material Description'
        else:
            raise HTTPException(status_code=400, detail=f"Field '{field}' not found in data")
    
    # Perform case-insensitive search
    mask = module_bom_simple_data[field].astype(str).str.contains(query, case=False, na=False)
    filtered_data = module_bom_simple_data[mask]
    
    if distinct:
        # Return distinct values for the specified field
        unique_values = filtered_data[field].dropna().unique().tolist()
        return [{"Material_Description": val} for val in unique_values]
    else:
        records = filtered_data.to_dict('records')
        
        # Clean up NaN values
        cleaned_records = []
        for record in records:
            cleaned_record = {}
            for key, value in record.items():
                if pd.isna(value):
                    cleaned_record[key] = None
                else:
                    cleaned_record[key] = value
            cleaned_records.append(cleaned_record)
        
        return cleaned_records

@app.get("/modulebom-simple/lookup")
async def lookup_modulebom(
    mpn: str = Query(..., description="MPN to lookup"),
    username: str = Depends(verify_credentials)
):
    """Lookup specific MPN in ModuleBOM_Simple"""
    
    if module_bom_simple_data.empty:
        return []
    
    # Search for MPN in Material_Description column (try both column names)
    search_columns = ['Material_Description', 'Material Description']
    filtered_data = pd.DataFrame()
    
    for col in search_columns:
        if col in module_bom_simple_data.columns:
            mask = module_bom_simple_data[col].astype(str).str.contains(mpn, case=False, na=False)
            filtered_data = module_bom_simple_data[mask]
            break
    
    if filtered_data.empty:
        return []
    
    records = filtered_data.to_dict('records')
    
    # Clean up NaN values
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if pd.isna(value):
                cleaned_record[key] = None
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)
    
    return cleaned_records

@app.get("/modulebom-simple/process-codes")
async def get_process_codes(username: str = Depends(verify_credentials)):
    """Get unique process codes"""
    
    if module_bom_simple_data.empty:
        return {"process_codes": []}
    
    # Try both column names
    process_code_columns = ['Process_Code', 'Process Code']
    process_codes = []
    
    for col in process_code_columns:
        if col in module_bom_simple_data.columns:
            process_codes = module_bom_simple_data[col].dropna().unique().tolist()
            break
    
    return {"process_codes": process_codes}

@app.get("/modulebom-simple/mpn-count")
async def get_mpn_count(username: str = Depends(verify_credentials)):
    """Get unique MPN count"""
    
    if module_bom_simple_data.empty:
        return {"unique_mpns": 0}
    
    # Try both column names
    mpn_columns = ['Material_Description', 'Material Description']
    unique_mpns = 0
    
    for col in mpn_columns:
        if col in module_bom_simple_data.columns:
            unique_mpns = module_bom_simple_data[col].dropna().nunique()
            break
    
    return {"unique_mpns": unique_mpns}

@app.post("/reload-data")
async def reload_data(username: str = Depends(verify_credentials)):
    """Reload data from PQRA database"""
    
    try:
        logger.info(f"Data reload requested by user: {username}")
        load_data_from_database()
        
        return {
            "status": "success",
            "message": "Data reloaded successfully",
            "timestamp": datetime.now().isoformat(),
            "records_loaded": {
                "modulebom_simple": len(module_bom_simple_data)
            }
        }
    except Exception as e:
        logger.error(f"Error reloading data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error reloading data: {str(e)}")

@app.get("/database/test-connection")
async def test_db_connection(username: str = Depends(verify_credentials)):
    """Test database connection"""
    
    try:
        connection_successful = test_database_connection()
        
        if connection_successful:
            return {
                "status": "success",
                "message": "Database connection successful",
                "server": DATABASE_CONFIG["server"],
                "database": DATABASE_CONFIG["database"]
            }
        else:
            return {
                "status": "failed",
                "message": "Database connection failed",
                "server": DATABASE_CONFIG["server"],
                "database": DATABASE_CONFIG["database"]
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error testing connection: {str(e)}")

if __name__ == "__main__":
    # Run the server
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        log_level="info"
    )