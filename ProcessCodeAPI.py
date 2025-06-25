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
    "server": "BOMSSPROD367\\BOMSSPROD367",  # Include instance name
    "database": "PQRA",
    "driver": "SQL Server",  # Start with the most basic driver
    "username": "AUTOQA_BP367_RDWR",
    "password": "AutoQA_SQL_20",
    "timeout": 30
}

def get_available_sql_drivers():
    """Get available SQL Server drivers"""
    import pyodbc
    drivers = pyodbc.drivers()
    sql_drivers = [d for d in drivers if 'SQL Server' in d]
    return sql_drivers

def get_connection_string():
    """Create SQL Server connection string with fallback drivers and SSL handling"""
    # Try different drivers in order of preference
    drivers_to_try = [
        "ODBC Driver 13 for SQL Server",  # This one worked for pyodbc
        "ODBC Driver 17 for SQL Server", 
        "ODBC Driver 18 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server"
    ]
    
    available_drivers = get_available_sql_drivers()
    logger.info(f"Available SQL drivers: {available_drivers}")
    
    # Find the first available driver
    driver_to_use = None
    for driver in drivers_to_try:
        if driver in available_drivers:
            driver_to_use = driver
            break
    
    if not driver_to_use:
        raise Exception(f"No suitable SQL Server driver found. Available: {available_drivers}")
    
    logger.info(f"Using driver for SQLAlchemy: {driver_to_use}")
    
    # Build connection string with SSL handling
    driver_encoded = driver_to_use.replace(' ', '+')
    
    # Add SSL parameters for newer drivers
    ssl_params = ""
    if "18" in driver_to_use:
        ssl_params = "&TrustServerCertificate=yes&Encrypt=no"
    elif "17" in driver_to_use:
        ssl_params = "&TrustServerCertificate=yes"
    
    return (
        f"mssql+pyodbc://{DATABASE_CONFIG['username']}:{DATABASE_CONFIG['password']}"
        f"@{DATABASE_CONFIG['server']}/{DATABASE_CONFIG['database']}"
        f"?driver={driver_encoded}"
        f"&timeout={DATABASE_CONFIG['timeout']}"
        f"{ssl_params}"
    )

def get_direct_pyodbc_connection():
    """Create direct pyodbc connection with fallback drivers"""
    drivers_to_try = [
        "ODBC Driver 13 for SQL Server",  # This one worked
        "ODBC Driver 17 for SQL Server", 
        "ODBC Driver 18 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server"
    ]
    
    available_drivers = get_available_sql_drivers()
    
    for driver in drivers_to_try:
        if driver in available_drivers:
            try:
                # Build connection string with SSL handling
                conn_str = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={DATABASE_CONFIG['server']};"
                    f"DATABASE={DATABASE_CONFIG['database']};"
                    f"UID={DATABASE_CONFIG['username']};"
                    f"PWD={DATABASE_CONFIG['password']};"
                    f"Connection Timeout={DATABASE_CONFIG['timeout']};"
                )
                
                # Add SSL parameters for newer drivers
                if "18" in driver:
                    conn_str += "TrustServerCertificate=yes;Encrypt=no;"
                elif "17" in driver:
                    conn_str += "TrustServerCertificate=yes;"
                
                logger.info(f"Trying connection with driver: {driver}")
                return pyodbc.connect(conn_str)
                
            except Exception as e:
                logger.warning(f"Failed with driver {driver}: {str(e)}")
                continue
    
    raise Exception(f"Could not connect with any available driver. Available: {available_drivers}")

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
    
def get_table_structure():
    """Get the actual column names from ModuleBOM_Simple table"""
    try:
        conn = get_direct_pyodbc_connection()
        cursor = conn.cursor()
        
        # Get column information
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'ModuleBOM_Simple' 
            AND TABLE_SCHEMA = 'dbo'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        cursor.close()
        conn.close()
        
        column_info = [(row[0], row[1]) for row in columns]
        logger.info(f"Found {len(column_info)} columns in ModuleBOM_Simple table")
        
        return column_info
        
    except Exception as e:
        logger.error(f"Error getting table structure: {str(e)}")
        return []

def load_data_from_database():
    """Load ModuleBOM data from PQRA database"""
    global module_bom_simple_data, last_data_load
    
    try:
        logger.info("Starting data load from PQRA database...")
        
        # First, get the actual table structure
        table_columns = get_table_structure()
        if not table_columns:
            raise Exception("Could not retrieve table structure")
        
        logger.info("Available columns in ModuleBOM_Simple:")
        for col_name, col_type in table_columns:
            logger.info(f"  - {col_name} ({col_type})")
        
        # Create SQLAlchemy engine
        connection_string = get_connection_string()
        engine = create_engine(connection_string, echo=False)
        
        # Load ModuleBOM_Simple data with correct column names
        logger.info("Loading ModuleBOM_Simple data...")
        
        # Use a simple query first to get all data, then we'll filter
        modulebom_simple_query = "SELECT * FROM [PQRA].[dbo].[ModuleBOM_Simple]"
        
        module_bom_simple_data = pd.read_sql(modulebom_simple_query, engine)
        logger.info(f"Loaded {len(module_bom_simple_data)} records from ModuleBOM_Simple")
        
        # Log the actual column names we got
        logger.info("Actual columns in loaded data:")
        for col in module_bom_simple_data.columns:
            logger.info(f"  - '{col}'")
        
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
            logger.info("Starting data cleaning...")
            
            # Find the material description column (could have different names)
            material_desc_col = None
            possible_names = ['Material Description', 'Material_Description', 'MaterialDescription', 'MATERIAL_DESCRIPTION']
            
            for col_name in possible_names:
                if col_name in module_bom_simple_data.columns:
                    material_desc_col = col_name
                    break
            
            if material_desc_col:
                logger.info(f"Found material description column: '{material_desc_col}'")
                
                # Remove rows with null Material Description
                initial_count = len(module_bom_simple_data)
                module_bom_simple_data = module_bom_simple_data.dropna(subset=[material_desc_col])
                
                # Clean Material Description column
                module_bom_simple_data[material_desc_col] = module_bom_simple_data[material_desc_col].astype(str).str.strip()
                
                # Remove rows where Material Description is empty or 'nan'
                module_bom_simple_data = module_bom_simple_data[
                    (module_bom_simple_data[material_desc_col] != '') &
                    (module_bom_simple_data[material_desc_col].str.lower() != 'nan')
                ]
                
                final_count = len(module_bom_simple_data)
                logger.info(f"Cleaned data: {initial_count} -> {final_count} records")
                
                # Create standardized column names for API consistency
                if material_desc_col != 'Material_Description':
                    module_bom_simple_data['Material_Description'] = module_bom_simple_data[material_desc_col]
            else:
                logger.warning("Could not find material description column")
            
            # Look for other important columns and create standardized names
            column_mappings = {
                'Material Number': 'Material_Number',
                'Process Code': 'Process_Code',
                'Component Type': 'Component_Type'
            }
            
            for original_col, standard_col in column_mappings.items():
                if original_col in module_bom_simple_data.columns:
                    module_bom_simple_data[standard_col] = module_bom_simple_data[original_col]
                    logger.info(f"Mapped '{original_col}' to '{standard_col}'")
        
        logger.info("Data cleaning completed")
        
    except Exception as e:
        logger.error(f"Error cleaning data: {str(e)}")

def verify_credentials(credentials: HTTPBasicCredentials):
    """Verify basic authentication credentials"""
    # Updated to match Streamlit app credentials
    valid_users = {
        "ProcessCodeAdmin": "MicronPC123",
        "admin": "MicronPC123",
        "api_user": "ProcessCodeAdmin",
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
        "server": "BOMSSPROD367",
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