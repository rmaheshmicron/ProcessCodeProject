import secrets
import pandas as pd
import sqlalchemy
import pyodbc
import uvicorn
import logging
import os
from fastapi import FastAPI, HTTPException, Query, Depends, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from datetime import datetime
from typing import Optional, List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Process Code API",
    description="API for accessing ModuleBOM data from PQRA database",
    version="1.0.0"
)

security = HTTPBasic()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

module_bom_simple_data = pd.DataFrame()
last_data_load = None

DATABASE_CONFIG = {
    "server": "BOMSSPROD367\\BOMSSPROD367",
    "database": "PQRA",
    "driver": "SQL Server",
    "username": "AUTOQA_BP367_RDWR",
    "password": "AutoQA_SQL_20",
    "timeout": 600
}

def get_available_sql_drivers():
    import pyodbc
    drivers = pyodbc.drivers()
    sql_drivers = [d for d in drivers if 'SQL Server' in d]
    return sql_drivers

def get_connection_string():
    drivers_to_try = [
        "ODBC Driver 13 for SQL Server",
        "ODBC Driver 17 for SQL Server", 
        "ODBC Driver 18 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server"
    ]
    
    available_drivers = get_available_sql_drivers()
    logger.info(f"Available SQL drivers: {available_drivers}")
    
    driver_to_use = None
    for driver in drivers_to_try:
        if driver in available_drivers:
            driver_to_use = driver
            break
    
    if not driver_to_use:
        raise Exception(f"No suitable SQL Server driver found. Available: {available_drivers}")
    
    logger.info(f"Using driver for SQLAlchemy: {driver_to_use}")
    
    driver_encoded = driver_to_use.replace(' ', '+')
    
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
    drivers_to_try = [
        "ODBC Driver 13 for SQL Server",
        "ODBC Driver 17 for SQL Server", 
        "ODBC Driver 18 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server"
    ]
    
    available_drivers = get_available_sql_drivers()
    
    for driver in drivers_to_try:
        if driver in available_drivers:
            try:
                conn_str = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={DATABASE_CONFIG['server']};"
                    f"DATABASE={DATABASE_CONFIG['database']};"
                    f"UID={DATABASE_CONFIG['username']};"
                    f"PWD={DATABASE_CONFIG['password']};"
                    f"Connection Timeout={DATABASE_CONFIG['timeout']};"
                )
                
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
    try:
        logger.info("Testing database connection...")
        
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
    try:
        conn = get_direct_pyodbc_connection()
        cursor = conn.cursor()
        
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
    global module_bom_simple_data, last_data_load
    
    try:
        logger.info("Starting data load from PQRA database...")
        
        table_columns = get_table_structure()
        if not table_columns:
            raise Exception("Could not retrieve table structure")
        
        logger.info("Available columns in ModuleBOM_Simple:")
        for col_name, col_type in table_columns:
            logger.info(f"  - {col_name} ({col_type})")
        
        connection_string = get_connection_string()
        engine = create_engine(connection_string, echo=False)
        
        logger.info("Loading ModuleBOM_Simple data...")
        
        modulebom_simple_query = "SELECT * FROM [PQRA].[dbo].[ModuleBOM_Simple]"
        
        module_bom_simple_data = pd.read_sql(modulebom_simple_query, engine)
        logger.info(f"Loaded {len(module_bom_simple_data)} records from ModuleBOM_Simple")
        
        logger.info("Actual columns in loaded data:")
        for col in module_bom_simple_data.columns:
            logger.info(f"  - '{col}'")
        
        engine.dispose()
        
        last_data_load = datetime.now()
        
        logger.info("Data load completed successfully")
        
        clean_data()
        
    except Exception as e:
        logger.error(f"Error loading data from database: {str(e)}")
        module_bom_simple_data = pd.DataFrame()
        raise

def clean_data():
    global module_bom_simple_data
    
    try:
        if not module_bom_simple_data.empty:
            logger.info("Starting data cleaning...")
            
            material_desc_col = None
            possible_names = ['Material Description', 'Material_Description', 'MaterialDescription', 'MATERIAL_DESCRIPTION']
            
            for col_name in possible_names:
                if col_name in module_bom_simple_data.columns:
                    material_desc_col = col_name
                    break
            
            if material_desc_col:
                logger.info(f"Found material description column: '{material_desc_col}'")
                
                initial_count = len(module_bom_simple_data)
                module_bom_simple_data = module_bom_simple_data.dropna(subset=[material_desc_col])
                
                module_bom_simple_data[material_desc_col] = module_bom_simple_data[material_desc_col].astype(str).str.strip()
                
                module_bom_simple_data = module_bom_simple_data[
                    (module_bom_simple_data[material_desc_col] != '') &
                    (module_bom_simple_data[material_desc_col].str.lower() != 'nan')
                ]
                
                final_count = len(module_bom_simple_data)
                logger.info(f"Cleaned data: {initial_count} -> {final_count} records")
                
                if material_desc_col != 'Material_Description':
                    module_bom_simple_data['Material_Description'] = module_bom_simple_data[material_desc_col]
            else:
                logger.warning("Could not find material description column")
            
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
    valid_users = {
        "ProcessCodeAdmin": "MicronPC123",
        "admin": "MicronPC123",
        "api_user": "ProcessCodeAdmin",
        "process_code_user": "process_code_pass"
    }
    
    is_correct_username = credentials.username in valid_users
    is_correct_password = False
    
    if is_correct_username:
        is_correct_password = secrets.compare_digest(
            credentials.password, valid_users[credentials.username]
        )
    
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    
    return credentials.username

@app.on_event("startup")
async def startup_event():
    logger.info("Starting Process Code API...")
    
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

@app.get("/test-auth")
async def test_auth(credentials: HTTPBasicCredentials = Depends(security)):
    username = verify_credentials(credentials)
    return {
        "status": "success", 
        "message": "Authentication successful",
        "user": username,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/database/test-connection")
async def test_db_connection(credentials: HTTPBasicCredentials = Depends(security)):
    username = verify_credentials(credentials)
    
    try:
        connection_successful = test_database_connection()
        
        if connection_successful:
            return {
                "status": "success",
                "message": "Database connection successful",
                "server": DATABASE_CONFIG["server"],
                "database": DATABASE_CONFIG["database"],
                "user": username
            }
        else:
            return {
                "status": "failed",
                "message": "Database connection failed",
                "server": DATABASE_CONFIG["server"],
                "database": DATABASE_CONFIG["database"],
                "user": username
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error testing connection: {str(e)}")

@app.get("/debug/table-structure")
async def debug_table_structure(credentials: HTTPBasicCredentials = Depends(security)):
    username = verify_credentials(credentials)
    
    try:
        table_columns = get_table_structure()
        return {
            "status": "success",
            "table_columns": table_columns,
            "user": username
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting table structure: {str(e)}")

@app.post("/reload-data")
async def reload_data(credentials: HTTPBasicCredentials = Depends(security)):
    username = verify_credentials(credentials)
    
    try:
        logger.info(f"Data reload requested by user: {username}")
        load_data_from_database()
        
        return {
            "status": "success",
            "message": "Data reloaded successfully",
            "timestamp": datetime.now().isoformat(),
            "records_loaded": {
                "modulebom_simple": len(module_bom_simple_data)
            },
            "user": username
        }
    except Exception as e:
        logger.error(f"Error reloading data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error reloading data: {str(e)}")

@app.post("/force-load-data")
async def force_load_data(credentials: HTTPBasicCredentials = Depends(security)):
    username = verify_credentials(credentials)
    
    try:
        logger.info(f"Force data load requested by user: {username}")
        
        if not test_database_connection():
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        load_data_from_database()
        
        return {
            "status": "success",
            "message": "Data force loaded successfully",
            "timestamp": datetime.now().isoformat(),
            "records_loaded": {
                "modulebom_simple": len(module_bom_simple_data)
            },
            "columns": list(module_bom_simple_data.columns) if not module_bom_simple_data.empty else [],
            "user": username
        }
    except Exception as e:
        logger.error(f"Error force loading data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error force loading data: {str(e)}")

@app.get("/modulebom-simple")
async def get_modulebom_simple(
    limit: Optional[int] = Query(None, description="Limit number of records"),
    offset: Optional[int] = Query(0, description="Offset for pagination"),
    credentials: HTTPBasicCredentials = Depends(security)
):
    username = verify_credentials(credentials)
    
    if module_bom_simple_data.empty:
        return []
    

    start_idx = offset
    end_idx = start_idx + limit if limit else len(module_bom_simple_data)
    
    result_data = module_bom_simple_data.iloc[start_idx:end_idx]
    
    records = result_data.to_dict('records')
    
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
async def get_modulebom_count(credentials: HTTPBasicCredentials = Depends(security)):
    username = verify_credentials(credentials)
    return {"count": len(module_bom_simple_data), "user": username}

@app.get("/modulebom-simple/info")
async def get_modulebom_info(credentials: HTTPBasicCredentials = Depends(security)):
    username = verify_credentials(credentials)
    
    if module_bom_simple_data.empty:
        return {"record_count": 0, "columns": [], "user": username}
    
    return {
        "record_count": len(module_bom_simple_data),
        "columns": list(module_bom_simple_data.columns),
        "last_updated": last_data_load.isoformat() if last_data_load else None,
        "user": username
    }

@app.get("/modulebom-simple/search")
async def search_modulebom(
    field: str = Query(..., description="Field to search in"),
    query: str = Query(..., description="Search query"),
    distinct: bool = Query(False, description="Return distinct values only"),
    credentials: HTTPBasicCredentials = Depends(security)
):
    username = verify_credentials(credentials)
    
    if module_bom_simple_data.empty:
        return []
    
    if field not in module_bom_simple_data.columns:
        if field == 'Material_Description' and 'Material Description' in module_bom_simple_data.columns:
            field = 'Material Description'
        else:
            raise HTTPException(status_code=400, detail=f"Field '{field}' not found in data")
    
    mask = module_bom_simple_data[field].astype(str).str.contains(query, case=False, na=False)
    filtered_data = module_bom_simple_data[mask]
    
    if distinct:
        unique_values = filtered_data[field].dropna().unique().tolist()
        return [{"Material_Description": val} for val in unique_values]
    else:
        records = filtered_data.to_dict('records')
        
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
    credentials: HTTPBasicCredentials = Depends(security)
):
    username = verify_credentials(credentials)
    
    if module_bom_simple_data.empty:
        return []
    
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
async def get_process_codes(credentials: HTTPBasicCredentials = Depends(security)):
    username = verify_credentials(credentials)
    
    if module_bom_simple_data.empty:
        return {"process_codes": [], "user": username}
    
    process_code_columns = ['Process_Code', 'Process Code']
    process_codes = []
    
    for col in process_code_columns:
        if col in module_bom_simple_data.columns:
            process_codes = module_bom_simple_data[col].dropna().unique().tolist()
            break
    
    return {"process_codes": process_codes, "user": username}

@app.get("/modulebom-simple/mpn-count")
async def get_mpn_count(credentials: HTTPBasicCredentials = Depends(security)):
    username = verify_credentials(credentials)
    
    if module_bom_simple_data.empty:
        return {"unique_mpns": 0, "user": username}
    
    mpn_columns = ['Material_Description', 'Material Description']
    unique_mpns = 0
    
    for col in mpn_columns:
        if col in module_bom_simple_data.columns:
            unique_mpns = module_bom_simple_data[col].dropna().nunique()
            break
    
    return {"unique_mpns": unique_mpns, "user": username}

@app.get("/modulebom-simple/columns")
async def get_available_columns(credentials: HTTPBasicCredentials = Depends(security)):
    username = verify_credentials(credentials)
    
    if module_bom_simple_data.empty:
        return {"columns": [], "user": username}
    
    return {
        "columns": list(module_bom_simple_data.columns),
        "user": username
    }

@app.get("/modulebom-simple/sample")
async def get_sample_data(
    limit: int = Query(5, description="Number of sample records"),
    credentials: HTTPBasicCredentials = Depends(security)
):
    username = verify_credentials(credentials)
    
    if module_bom_simple_data.empty:
        return {"sample_data": [], "user": username}
    
    sample_data = module_bom_simple_data.head(limit)
    records = sample_data.to_dict('records')
    
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if pd.isna(value):
                cleaned_record[key] = None
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)
    
    return {
        "sample_data": cleaned_records,
        "total_records": len(module_bom_simple_data),
        "user": username
    }

if __name__ == "__main__":
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        log_level="info"
    )