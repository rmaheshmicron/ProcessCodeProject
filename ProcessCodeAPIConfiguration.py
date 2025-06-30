import os

class DatabaseConfig:
    SERVER = "BOMSSPROD367\\BOMSSPROD367"
    DATABASE = "PQRA"
    DRIVER = "ODBC Driver 17 for SQL Server"
    USERNAME = "AUTOQA_BP367_RDWR"
    PASSWORD = "AutoQA_SQL_20"
    TIMEOUT = 600

class APIConfig:
    TITLE = "Process Code API"
    VERSION = "1.0.0"
    DESCRIPTION = "API for accessing ModuleBOM data from PQRA database"
    HOST = "0.0.0.0"
    PORT = 8000

class AuthConfig:
    USERS = {
        "admin": "MicronPC123",
        "api_user": "ProcessCodeAdmin", 
        "process_code_user": "process_code_pass",
        "ProcessCodeAdmin": "MicronPC123"
    }