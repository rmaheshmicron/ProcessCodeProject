import streamlit as st
import pandas as pd
import pyodbc
from datetime import datetime
import pytz
import logging
import ssl
import urllib3
import requests
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.listitems.caml.query import CamlQuery
import os
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_CONFIG = {
    "server": "BOMSSPROD367\\BOMSSPROD367",
    "database": "PQRA",
    "driver": "SQL Server",
    "username": "AUTOQA_BP367_RDWR",
    "password": "AutoQA_SQL_20",
    "timeout": 600
}

def get_available_sql_drivers():
    drivers = pyodbc.drivers()
    sql_drivers = [d for d in drivers if 'SQL Server' in d]
    return sql_drivers

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

def load_data_from_database():
    try:
        conn = get_direct_pyodbc_connection()
        
        # Load ModuleBOM_Simple data
        query = "SELECT * FROM ModuleBOM_Simple"
        module_bom_simple_df = pd.read_sql(query, conn)
        
        conn.close()
        
        logger.info(f"Loaded {len(module_bom_simple_df)} records from ModuleBOM_Simple")
        
        return module_bom_simple_df
        
    except Exception as e:
        logger.error(f"Error loading data from database: {str(e)}")
        raise

def load_data_from_sharepoint():
    """Load data from SharePoint with comprehensive error handling."""
    data = {
        'component_validations_df': pd.DataFrame(),
        'module_validation_df': pd.DataFrame(),
        'end_products_df': pd.DataFrame()
    }
    
    # Disable SSL warnings and verification for corporate networks
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Set environment variables to disable SSL verification
    os.environ['PYTHONHTTPSVERIFY'] = '0'
    os.environ['CURL_CA_BUNDLE'] = ''
    os.environ['REQUESTS_CA_BUNDLE'] = ''
    
    # Create an SSL context that doesn't verify certificates
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    sharepoint_site = "https://microncorp.sharepoint.com/sites/mdg"
    list_name = "Module HW Design Component Validations"
    
    # Always get credentials from session state first, then fall back to secrets
    username = st.session_state.get("sharepoint_username", "")
    password = st.session_state.get("sharepoint_password", "")
    
    # If not in session state, try secrets
    if not username and "sharepoint_username" in st.secrets:
        username = st.secrets["sharepoint_username"]
    if not password and "sharepoint_password" in st.secrets:
        password = st.secrets["sharepoint_password"]
    
    if not (username and password):
        st.sidebar.warning("Please provide SharePoint credentials to load data.")
        return data
    
    # Store original request methods
    original_request = requests.Session.request
    original_get = requests.get
    original_post = requests.post
    
    def patched_request(self, method, url, **kwargs):
        kwargs['verify'] = False
        kwargs['timeout'] = kwargs.get('timeout', 30)
        return original_request(self, method, url, **kwargs)
    
    def patched_get(url, **kwargs):
        kwargs['verify'] = False
        kwargs['timeout'] = kwargs.get('timeout', 30)
        return original_get(url, **kwargs)
    
    def patched_post(url, **kwargs):
        kwargs['verify'] = False
        kwargs['timeout'] = kwargs.get('timeout', 30)
        return original_post(url, **kwargs)
    
    try:
        # Patch requests to disable SSL verification
        requests.Session.request = patched_request
        requests.get = patched_get
        requests.post = patched_post
        
        # Create user credentials with error handling
        try:
            user_credentials = UserCredential(username, password)
        except Exception as cred_error:
            st.sidebar.error(f"Failed to create user credentials: {str(cred_error)}")
            return data
        
        # Create SharePoint context with error handling
        try:
            ctx = ClientContext(sharepoint_site).with_credentials(user_credentials)
            
            # Test the connection by trying to get web properties
            web = ctx.web.get().execute_query()
            st.sidebar.success(f"Successfully connected to SharePoint site")
            
        except Exception as ctx_error:
            st.sidebar.error(f"Failed to connect to SharePoint context: {str(ctx_error)}")
            return data
        
        # Get all lists with error handling
        try:
            all_lists = ctx.web.lists.get().execute_query()
            available_lists = [list_item.properties.get('Title', '') for list_item in all_lists]
            
        except Exception as list_error:
            st.sidebar.error(f"Failed to retrieve SharePoint lists: {str(list_error)}")
            return data
        
        # Find the target list
        target_list = None
        if list_name in available_lists:
            try:
                target_list = ctx.web.lists.get_by_title(list_name)
            except Exception as target_error:
                return data
        
        if not target_list:
            st.sidebar.error("Could not establish connection to any suitable list")
            return data
        
        # Retrieve all items with pagination and error handling
        all_items = []
        page_size = 500
        
        try:
            caml_query = CamlQuery()
            caml_query.ViewXml = f"<View><RowLimit>{page_size}</RowLimit></View>"
            
            items = target_list.get_items(caml_query).execute_query()
            all_items.extend(items)
            
            # Continue pagination if needed
            while len(items) == page_size:
                try:
                    last_id = items[-1].properties.get('ID')
                    
                    caml_query = CamlQuery()
                    caml_query.ViewXml = f"""
                    <View>
                        <Query>
                            <Where>
                                <Gt>
                                    <FieldRef Name='ID' />
                                    <Value Type='Number'>{last_id}</Value>
                                </Gt>
                            </Where>
                            <OrderBy>
                                <FieldRef Name='ID' Ascending='True' />
                            </OrderBy>
                        </Query>
                        <RowLimit>{page_size}</RowLimit>
                    </View>
                    """
                    
                    items = target_list.get_items(caml_query).execute_query()
                    all_items.extend(items)
                    
                    if len(items) < page_size:
                        break
                        
                except Exception as pagination_error:
                    st.sidebar.warning(f"Pagination error, stopping at {len(all_items)} items: {str(pagination_error)}")
                    break
                    
        except Exception as items_error:
            st.sidebar.error(f"Failed to retrieve items from target list: {str(items_error)}")
            return data
        
        if len(all_items) == 0:
            st.sidebar.error("No items found in the list")
            return data
        
        st.sidebar.success(f"Retrieved {len(all_items)} items from SharePoint")
        
        # Process component validations data
        component_validations_data = []
        
        field_mapping = {
            'Segment': 'Segment',
            'Supplier': 'Supplier',
            'Component_Generation': 'Product_x0020_Family',
            'Revision': 'REV',
            'Component_Type': 'ts0w',
            'Process_Code': 'Process_x0020_Code',
            'SPN': 'Supplier_x0020_PN',
            'Speed': 'Product_x0020_Comment',
            'Product_Description': 'Title',
            'Product_Status': 'Product_x0020_Status',
            'SAP_Number': 'SAP_x0020_Number'
        }

        for item in all_items:
            try:
                item_properties = item.properties
                
                record = {}
                for key, field in field_mapping.items():
                    if field and field in item_properties:
                        if key == 'Component_Type':
                            component_type_value = str(item_properties[field])
                            if component_type_value and component_type_value.lower() not in ['none', 'nan', 'null', '']:
                                record[key] = component_type_value
                            else:
                                # Don't try to infer from title - leave as provided or empty
                                record[key] = component_type_value if component_type_value else ''
                        else:
                            record[key] = str(item_properties[field])
                    else:
                        record[key] = ""
                
                # Ensure Product_Description is set
                if not record.get('Product_Description'):
                    record['Product_Description'] = str(item_properties.get('Title', ''))
                
                # Only add records with meaningful data
                if record.get('Segment') and (record.get('Supplier') or record.get('Component_Type') or record.get('Process_Code')):
                    component_validations_data.append(record)
                    
            except Exception as item_process_error:
                st.sidebar.warning(f"Error processing component validation item: {str(item_process_error)}")
                continue
        
        data['component_validations_df'] = pd.DataFrame(component_validations_data)
        
    except Exception as e:
        st.sidebar.error(f"Error connecting to SharePoint: {str(e)}")
    
    finally:
        # Restore original request methods
        try:
            requests.Session.request = original_request
            requests.get = original_get
            requests.post = original_post
        except:
            pass
        
        # Clean up environment variables
        try:
            if 'PYTHONHTTPSVERIFY' in os.environ:
                del os.environ['PYTHONHTTPSVERIFY']
            if 'CURL_CA_BUNDLE' in os.environ:
                del os.environ['CURL_CA_BUNDLE']
            if 'REQUESTS_CA_BUNDLE' in os.environ:
                del os.environ['REQUESTS_CA_BUNDLE']
        except:
            pass

    return data

@st.cache_data(ttl=3600)
def load_data_cached():
    return load_data_from_database()

@st.cache_data(ttl=3600)
def load_sharepoint_data_cached():
    return load_data_from_sharepoint()

def show_process_code_info():
    with st.expander("Process Code Information", expanded=False):
        st.markdown("""
        ## Process Code Information
        
        **Process codes** identify the specific components used in memory modules. Each position in the code represents a different component type:
        
        ### Server Modules (4-5 Characters):
        - **Position 1**: PMIC
        - **Position 2**: SPD/Hub
        - **Position 3**: Temp Sensor
        - **Position 4**: RCD/MRCD
        - **Position 5**: Data Buffer (Optional)
                    
        Print Order: PMIC →  RCD/MRCD →  SPD/Hub → Temp Sensor → Data Buffer
        
        ### Client Modules (2-3 Characters):
        - **Position 1**: PMIC
        - **Position 2**: SPD/Hub
        - **Position 3**: CKD (Optional)
        
        ### SOCAMM Modules (3 Characters):
        - **Position 1**: SPD/Hub
        - **Position 2-3**: Voltage Regulator
        
        """)

def get_predefined_options(component_validations_df):
    """Get all options directly from SharePoint data."""
    options = {
        'segment': ['Client', 'Server'],
        'supplier': [],
        'component_generation': [],
        'revision': [],
        'component_type': []
    }
    
    if component_validations_df is not None and not component_validations_df.empty:
        # Get component types directly from SharePoint
        options['component_type'] = get_valid_component_types_from_sharepoint(component_validations_df)
        
        # Get other options from SharePoint data
        if 'Supplier' in component_validations_df.columns:
            suppliers = component_validations_df['Supplier'].dropna().astype(str)
            suppliers = suppliers[suppliers.str.strip() != '']
            options['supplier'] = sorted(suppliers.unique().tolist())
        
        if 'Component_Generation' in component_validations_df.columns:
            generations = component_validations_df['Component_Generation'].dropna().astype(str)
            generations = generations[generations.str.strip() != '']
            options['component_generation'] = sorted(generations.unique().tolist())
        
        if 'Revision' in component_validations_df.columns:
            revisions = component_validations_df['Revision'].dropna().astype(str)
            revisions = revisions[revisions.str.strip() != '']
            options['revision'] = sorted(revisions.unique().tolist())
    
    return options

def get_component_type_options_by_segment(segment, component_validations_df):
    """Get component options based on segment from SharePoint data."""
    if component_validations_df is None or component_validations_df.empty:
        return []
    
    # Get all component types from SharePoint
    valid_component_types = get_valid_component_types_from_sharepoint(component_validations_df)
    
    if not valid_component_types:
        return []
    
    # Filter by segment if needed - return all types found in SharePoint for the segment
    if 'Segment' in component_validations_df.columns:
        segment_mask = (
            (component_validations_df['Segment'] == segment) |
            (component_validations_df['Segment'] == 'Server/Client') |
            (component_validations_df['Segment'] == 'Client/Server')
        )
        segment_data = component_validations_df[segment_mask]
        
        if not segment_data.empty and 'Component_Type' in segment_data.columns:
            segment_types = segment_data['Component_Type'].dropna().astype(str)
            segment_types = segment_types[segment_types.str.strip() != '']
            segment_types = segment_types[~segment_types.str.lower().isin(['nan', 'none', 'null', 'unknown'])]
            return sorted(segment_types.unique().tolist())
    
    return valid_component_types

def get_module_component_options_by_segment(segment, component_validations_df):
    """Get component options based on segment with proper filtering."""
    
    if segment.lower() == 'server':
        # Server components in position order
        component_options = {
            'PMIC': {'required': True, 'position': 1},
            'SPD/Hub': {'required': True, 'position': 2},
            'Temp Sensor': {'required': True, 'position': 3},
            'RCD/MRCD': {'required': True, 'position': 4},
            'Data Buffer': {'required': False, 'position': 5}
        }
    elif segment.lower() == 'client':
        # Client components in position order
        component_options = {
            'PMIC': {'required': True, 'position': 1},
            'SPD/Hub': {'required': True, 'position': 2},
            'CKD': {'required': False, 'position': 3}
        }
    else:
        # Unknown segment - return empty options
        return {}
    
    # Filter to only include component types that actually exist in SharePoint data for this segment
    if not component_validations_df.empty and 'Component_Type' in component_validations_df.columns:
        # Get components for this segment
        segment_mask = (
            (component_validations_df['Segment'].str.lower() == segment.lower()) |
            (component_validations_df['Segment'].str.contains('Server/Client', case=False, na=False)) |
            (component_validations_df['Segment'].str.contains('Client/Server', case=False, na=False))
        )
        
        segment_data = component_validations_df[segment_mask]
        available_component_types = set()
        
        if not segment_data.empty:
            # Get all unique component types for this segment
            unique_component_types = segment_data['Component_Type'].dropna().unique()
            
            # Map SharePoint component types to our standard names using more comprehensive matching
            for component_type in unique_component_types:
                component_type_lower = str(component_type).lower()
                
                # PMIC mapping
                if any(keyword in component_type_lower for keyword in ['pmic', 'power management', 'power mgmt']):
                    available_component_types.add('PMIC')
                
                # SPD/Hub mapping - including Voltage Regulator which might be used for SPD/Hub
                elif any(keyword in component_type_lower for keyword in ['spd', 'hub', 'serial presence', 'voltage regulator', 'vr']):
                    available_component_types.add('SPD/Hub')
                
                # Temperature Sensor mapping
                elif any(keyword in component_type_lower for keyword in ['temp', 'temperature', 'sensor', 'thermal']):
                    available_component_types.add('Temp Sensor')
                
                # RCD/MRCD mapping
                elif any(keyword in component_type_lower for keyword in ['rcd', 'mrcd', 'registering', 'clock driver', 'muxed']):
                    available_component_types.add('RCD/MRCD')
                
                # Data Buffer mapping
                elif any(keyword in component_type_lower for keyword in ['data buffer', 'buffer', 'db']):
                    available_component_types.add('Data Buffer')
                
                # CKD mapping (Client only)
                elif any(keyword in component_type_lower for keyword in ['ckd', 'clock driver']) and segment.lower() == 'client':
                    available_component_types.add('CKD')
        
        # Filter component_options to only include available types
        filtered_options = {}
        for comp_type, config in component_options.items():
            if comp_type in available_component_types:
                filtered_options[comp_type] = config
            else:
                st.sidebar.warning(f"⚠️ {comp_type} not found in SharePoint data")
        
        return filtered_options
    
    return component_options

def normalize_component_type(component_type):
    """Normalize component type to handle alternative names - no hardcoded fallbacks."""
    if not component_type or pd.isna(component_type):
        return component_type
    
    component_str = str(component_type).strip()
    
    # Handle HTML entities
    component_str = component_str.replace('&#X2F;', '/').replace('&#x2f;', '/')
    
    # Return the normalized string without hardcoded mappings
    return component_str

def get_filtered_options(df, column, **filters):
    """Get filtered options from DataFrame based on filters with improved component type mapping."""
    if df.empty or column not in df.columns:
        return []
    
    filtered_df = df.copy()
    
    # Apply filters
    for filter_col, filter_value in filters.items():
        if filter_col in filtered_df.columns and filter_value:
            if filter_col == 'Component_Type':
                # Handle component type mapping for filtering
                component_type_mask = pd.Series([False] * len(filtered_df), index=filtered_df.index)
                
                for idx, row_component_type in filtered_df['Component_Type'].items():
                    if pd.isna(row_component_type):
                        continue
                    
                    row_component_type_lower = str(row_component_type).lower()
                    filter_value_lower = filter_value.lower()
                    
                    # Map filter value to SharePoint component types
                    if filter_value_lower == 'pmic':
                        if any(keyword in row_component_type_lower for keyword in ['pmic', 'power management', 'power mgmt']):
                            component_type_mask.loc[idx] = True
                    elif filter_value_lower == 'spd/hub':
                        if any(keyword in row_component_type_lower for keyword in ['spd', 'hub', 'serial presence', 'voltage regulator', 'vr']):
                            component_type_mask.loc[idx] = True
                    elif filter_value_lower == 'temp sensor':
                        if any(keyword in row_component_type_lower for keyword in ['temp', 'temperature', 'sensor', 'thermal']):
                            component_type_mask.loc[idx] = True
                    elif filter_value_lower == 'rcd/mrcd':
                        if any(keyword in row_component_type_lower for keyword in ['rcd', 'mrcd', 'registering', 'clock driver', 'muxed']):
                            component_type_mask.loc[idx] = True
                    elif filter_value_lower == 'data buffer':
                        if any(keyword in row_component_type_lower for keyword in ['data buffer', 'buffer', 'db']):
                            component_type_mask.loc[idx] = True
                    elif filter_value_lower == 'ckd':
                        if any(keyword in row_component_type_lower for keyword in ['ckd', 'clock driver']):
                            component_type_mask.loc[idx] = True
                
                filtered_df = filtered_df[component_type_mask]
            else:
                # Regular filtering for other columns
                if filtered_df[filter_col].dtype == 'object':
                    filtered_df = filtered_df[filtered_df[filter_col].str.contains(str(filter_value), case=False, na=False)]
                else:
                    filtered_df = filtered_df[filtered_df[filter_col] == filter_value]
    
    # Get unique values and sort them
    unique_values = filtered_df[column].dropna().unique()
    
    # Filter out empty/invalid values
    valid_values = []
    for value in unique_values:
        if value and str(value).strip() and str(value).lower() not in ['nan', 'none', 'null', '']:
            valid_values.append(str(value).strip())
    
    return sorted(list(set(valid_values)))

def get_component_process_code(segment, supplier, generation, revision, component_type, component_validations_df):
    """Get process code for a specific component from SharePoint data."""
    if component_validations_df is None or component_validations_df.empty:
        return "No SharePoint data available", component_type, pd.DataFrame()
    
    # Create filters
    filters = {
        'Segment': segment,
        'Supplier': supplier,
        'Component_Generation': generation,
        'Revision': revision,
        'Component_Type': component_type
    }
    
    # Apply filters
    filtered_df = component_validations_df.copy()
    
    for column, value in filters.items():
        if column in filtered_df.columns and value:
            # Handle segment variations
            if column == 'Segment':
                segment_mask = (
                    (filtered_df[column] == value) |
                    (filtered_df[column] == 'Server/Client') |
                    (filtered_df[column] == 'Client/Server')
                )
                filtered_df = filtered_df[segment_mask]
            else:
                filtered_df = filtered_df[filtered_df[column] == value]
    
    if filtered_df.empty:
        return "No matching component found", component_type, pd.DataFrame()
    
    # Get process code
    if 'Process_Code' in filtered_df.columns:
        process_codes = filtered_df['Process_Code'].dropna().astype(str)
        process_codes = process_codes[process_codes.str.strip() != '']
        
        if not process_codes.empty:
            # Return the first valid process code
            return process_codes.iloc[0], component_type, filtered_df
        else:
            return "Process code is empty", component_type, filtered_df
    else:
        return "No Process_Code column found", component_type, filtered_df

def get_module_process_code(pmic_code, spd_hub_code, temp_sensor_code, rcd_mrcd_code, data_buffer_code, segment):
    codes = [pmic_code, spd_hub_code]
    if segment.lower() == 'server':
        codes.extend([temp_sensor_code, rcd_mrcd_code])
        if data_buffer_code:
            codes.append(data_buffer_code)
    else:
        if temp_sensor_code:  # This would be CKD for client
            codes.append(temp_sensor_code)
    
    return ''.join([code for code in codes if code])

def convert_process_code_to_print_order(process_code, segment):
    if not process_code or segment.lower() != 'server':
        return process_code
    
    if len(process_code) >= 4:
        # Server: PMIC → RCD → SPD/Hub → Temp Sensor → Data Buffer
        print_order = process_code[0] + process_code[3] + process_code[1] + process_code[2]
        if len(process_code) >= 5:
            print_order += process_code[4]
        return print_order
    
    return process_code

def convert_print_order_to_process_code(print_order_code, segment):
    if not print_order_code or segment.lower() != 'server':
        return print_order_code
    
    if len(print_order_code) >= 4:
        # Convert back: Print order → Position order
        position_order = print_order_code[0] + print_order_code[2] + print_order_code[3] + print_order_code[1]
        if len(print_order_code) >= 5:
            position_order += print_order_code[4]
        return position_order
    
    return print_order_code

def explain_process_code(process_code, segment):
    explanation = f"Process Code: {process_code} (Segment: {segment})\n"
    explanation += "Position Breakdown:\n"
    
    if segment.lower() == 'server':
        components = ['PMIC', 'SPD/Hub', 'Temp Sensor', 'RCD/MRCD', 'Data Buffer']
    else:
        components = ['PMIC', 'SPD/Hub', 'CKD']
    
    for i, char in enumerate(process_code):
        if i < len(components):
            explanation += f"- Position {i+1}: {components[i]} → {char}\n"
    
    return explanation

def get_valid_component_types_from_sharepoint(component_validations_df):
    """Extract all unique component types directly from SharePoint data."""
    if component_validations_df is None or component_validations_df.empty:
        return []
    
    if 'Component_Type' not in component_validations_df.columns:
        return []
    
    # Get all unique component types from SharePoint, excluding null/empty values
    component_types = component_validations_df['Component_Type'].dropna().astype(str)
    component_types = component_types[component_types.str.strip() != '']
    component_types = component_types[~component_types.str.lower().isin(['nan', 'none', 'null', 'unknown'])]
    
    return sorted(component_types.unique().tolist())

def lookup_process_code_components(process_code, segment, component_validations_df, is_socamm=False):
    """Look up components for a given process code using positional component type mapping."""
    if component_validations_df is None or component_validations_df.empty:
        return pd.DataFrame(), "No SharePoint data available"
    
    if not process_code or str(process_code).strip() == '':
        return pd.DataFrame(), "No process code provided"
    
    process_code = str(process_code).strip().upper()
    
    # Define positional component type mapping with simplified expected types
    if is_socamm:
        # SOCAMM: 1st character = SPD/Hub, 2nd-3rd characters = Voltage Regulator
        position_mapping = {
            1: {
                'expected_type': 'SPD/Hub',
                'search_terms': ['SPD/Hub', 'SPD', 'Hub']
            },
            2: {
                'expected_type': 'Voltage Regulator',
                'search_terms': ['Voltage Regulator', 'VR', 'Regulator']
            }
        }
    elif segment.lower() == 'server':
        position_mapping = {
            1: {
                'expected_type': 'PMIC',
                'search_terms': ['PMIC']
            },
            2: {
                'expected_type': 'SPD/Hub',
                'search_terms': ['SPD/Hub', 'SPD', 'Hub']
            },
            3: {
                'expected_type': 'Temp Sensor',
                'search_terms': ['Temp Sensor', 'Temperature Sensor']
            },
            4: {
                'expected_type': 'RCD/MRCD',
                'search_terms': ['RCD', 'MRCD', 'Muxed RCD', 'RCD/MRCD']
            },
            5: {
                'expected_type': 'Data Buffer',
                'search_terms': ['Data Buffer', 'Buffer']
            }
        }
    elif segment.lower() == 'client':
        position_mapping = {
            1: {
                'expected_type': 'PMIC',
                'search_terms': ['PMIC']
            },
            2: {
                'expected_type': 'SPD/Hub',
                'search_terms': ['SPD/Hub', 'SPD', 'Hub']
            },
            3: {
                'expected_type': 'CKD',
                'search_terms': ['CKD', 'Clock Driver']
            }
        }
    else:
        return pd.DataFrame(), f"Unknown segment: {segment}"
    
    # Get all component types available in SharePoint for this segment
    segment_mask = (
        (component_validations_df['Segment'] == segment) |
        (component_validations_df['Segment'] == 'Server/Client') |
        (component_validations_df['Segment'] == 'Client/Server')
    )
    
    segment_data = component_validations_df[segment_mask]
    
    if segment_data.empty:
        return pd.DataFrame(), f"No data found for segment: {segment}"
    
    results = []
    
    if is_socamm:
        # Handle SOCAMM special case
        if len(process_code) >= 1:
            # Position 1: SPD/Hub
            char = process_code[0]
            mapping = position_mapping[1]
            expected_type = mapping['expected_type']
            search_terms = mapping['search_terms']
            position_matches = []
            
            for search_term in search_terms:
                type_matches = segment_data[
                    (segment_data['Process_Code'].astype(str).str.upper() == char) &
                    (segment_data['Component_Type'].str.contains(search_term, case=False, na=False))
                ]
                
                if not type_matches.empty:
                    position_matches.extend(type_matches.to_dict('records'))
            
            if position_matches:
                # Deduplicate matches
                seen_components = set()
                unique_matches = []
                
                for match in position_matches:
                    component_key = (
                        match.get('Component_Type', ''),
                        match.get('Supplier', ''),
                        match.get('Component_Generation', ''),
                        match.get('Revision', ''),
                        match.get('Process_Code', ''),
                        match.get('Product_Description', '')
                    )
                    
                    if component_key not in seen_components:
                        seen_components.add(component_key)
                        unique_matches.append(match)
                
                # Add ALL unique matches for this position
                for i, match in enumerate(unique_matches):
                    results.append({
                        'Position': 1,
                        'Expected_Component_Type': expected_type,
                        'Process_Code_Character': char,
                        'Product_Description': match.get('Product_Description', ''),
                        'Supplier': match.get('Supplier', ''),
                        'Component_Generation': match.get('Component_Generation', ''),
                        'Revision': match.get('Revision', ''),
                        'SAP_Number': match.get('SAP_Number', ''),
                        'SPN': match.get('SPN', ''),
                        'Actual_Component_Type': match.get('Component_Type', ''),
                        'Actual_Segment': match.get('Segment', ''),
                        'Option_Number': i + 1 if len(unique_matches) > 1 else None
                    })
            else:
                results.append({
                    'Position': 1,
                    'Expected_Component_Type': expected_type,
                    'Process_Code_Character': char,
                    'Product_Description': f'No {expected_type} found with code "{char}"',
                    'Supplier': '',
                    'Component_Generation': '',
                    'Revision': '',
                    'SAP_Number': '',
                    'SPN': '',
                    'Actual_Component_Type': '',
                    'Actual_Segment': '',
                    'Option_Number': None
                })
        
        if len(process_code) >= 3:
            # Position 2-3: Voltage Regulator (2-character code)
            voltage_regulator_code = process_code[1:3]
            mapping = position_mapping[2]
            expected_type = mapping['expected_type']
            search_terms = mapping['search_terms']
            position_matches = []
            
            for search_term in search_terms:
                type_matches = segment_data[
                    (segment_data['Process_Code'].astype(str).str.upper() == voltage_regulator_code) &
                    (segment_data['Component_Type'].str.contains(search_term, case=False, na=False))
                ]
                
                if not type_matches.empty:
                    position_matches.extend(type_matches.to_dict('records'))
            
            if position_matches:
                # Deduplicate matches
                seen_components = set()
                unique_matches = []
                
                for match in position_matches:
                    component_key = (
                        match.get('Component_Type', ''),
                        match.get('Supplier', ''),
                        match.get('Component_Generation', ''),
                        match.get('Revision', ''),
                        match.get('Process_Code', ''),
                        match.get('Product_Description', '')
                    )
                    
                    if component_key not in seen_components:
                        seen_components.add(component_key)
                        unique_matches.append(match)
                
                # Add ALL unique matches for this position
                for i, match in enumerate(unique_matches):
                    results.append({
                        'Position': '2-3',
                        'Expected_Component_Type': expected_type,
                        'Process_Code_Character': voltage_regulator_code,
                        'Product_Description': match.get('Product_Description', ''),
                        'Supplier': match.get('Supplier', ''),
                        'Component_Generation': match.get('Component_Generation', ''),
                        'Revision': match.get('Revision', ''),
                        'SAP_Number': match.get('SAP_Number', ''),
                        'SPN': match.get('SPN', ''),
                        'Actual_Component_Type': match.get('Component_Type', ''),
                        'Actual_Segment': match.get('Segment', ''),
                        'Option_Number': i + 1 if len(unique_matches) > 1 else None
                    })
            else:
                results.append({
                    'Position': '2-3',
                    'Expected_Component_Type': expected_type,
                    'Process_Code_Character': voltage_regulator_code,
                    'Product_Description': f'No {expected_type} found with code "{voltage_regulator_code}"',
                    'Supplier': '',
                    'Component_Generation': '',
                    'Revision': '',
                    'SAP_Number': '',
                    'SPN': '',
                    'Actual_Component_Type': '',
                    'Actual_Segment': '',
                    'Option_Number': None
                })
    else:
        # Handle regular (non-SOCAMM) process codes
        for position, char in enumerate(process_code, 1):
            if position not in position_mapping:
                # Position beyond expected range
                results.append({
                    'Position': position,
                    'Expected_Component_Type': 'Unknown',
                    'Process_Code_Character': char,
                    'Product_Description': f'Position {position} not defined for {segment}',
                    'Supplier': '',
                    'Component_Generation': '',
                    'Revision': '',
                    'SAP_Number': '',
                    'SPN': '',
                    'Actual_Component_Type': '',
                    'Actual_Segment': '',
                    'Option_Number': None
                })
                continue
            
            mapping = position_mapping[position]
            expected_type = mapping['expected_type']
            search_terms = mapping['search_terms']
            position_matches = []
            
            # Look for components that match both the process code character AND the expected component type for this position
            for search_term in search_terms:
                # Find components that match the process code character and are of the expected type for this position
                type_matches = segment_data[
                    (segment_data['Process_Code'].astype(str).str.upper() == char) &
                    (segment_data['Component_Type'].str.contains(search_term, case=False, na=False))
                ]
                
                if not type_matches.empty:
                    position_matches.extend(type_matches.to_dict('records'))
            
            if position_matches:
                # Deduplicate matches based on key fields to avoid showing the same component multiple times
                seen_components = set()
                unique_matches = []
                
                for match in position_matches:
                    # Create a unique key based on important fields
                    component_key = (
                        match.get('Component_Type', ''),
                        match.get('Supplier', ''),
                        match.get('Component_Generation', ''),
                        match.get('Revision', ''),
                        match.get('Process_Code', ''),
                        match.get('Product_Description', '')
                    )
                    
                    if component_key not in seen_components:
                        seen_components.add(component_key)
                        unique_matches.append(match)
                
                # If we have multiple unique matches, sort by preference
                if len(unique_matches) > 1:
                    # Sort by preference: prefer non-empty SAP numbers, then by supplier name
                    unique_matches.sort(key=lambda x: (
                        x.get('SAP_Number', '') == '',  # False (non-empty SAP) comes first
                        x.get('Supplier', ''),
                        x.get('Component_Generation', ''),
                        x.get('Revision', '')
                    ))
                
                # Add ALL unique matches for this position
                for i, match in enumerate(unique_matches):
                    results.append({
                        'Position': position,
                        'Expected_Component_Type': expected_type,
                        'Process_Code_Character': char,
                        'Product_Description': match.get('Product_Description', ''),
                        'Supplier': match.get('Supplier', ''),
                        'Component_Generation': match.get('Component_Generation', ''),
                        'Revision': match.get('Revision', ''),
                        'SAP_Number': match.get('SAP_Number', ''),
                        'SPN': match.get('SPN', ''),
                        'Actual_Component_Type': match.get('Component_Type', ''),
                        'Actual_Segment': match.get('Segment', ''),
                        'Option_Number': i + 1 if len(unique_matches) > 1 else None
                    })
            else:
                # No matches found for this position and character
                results.append({
                    'Position': position,
                    'Expected_Component_Type': expected_type,
                    'Process_Code_Character': char,
                    'Product_Description': f'No {expected_type} found with code "{char}"',
                    'Supplier': '',
                    'Component_Generation': '',
                    'Revision': '',
                    'SAP_Number': '',
                    'SPN': '',
                    'Actual_Component_Type': '',
                    'Actual_Segment': '',
                    'Option_Number': None
                })
    
    results_df = pd.DataFrame(results)
    
    if results_df.empty:
        return pd.DataFrame(), "No components found for this process code"
    
    return results_df, f"Found {len(results_df)} component options"

def search_mpn_in_rest_api(search_term, module_bom_simple_df):
    matching_mpns = []
    
    try:
        if not module_bom_simple_df.empty:
            # Filter out invalid Design IDs first
            filtered_df = filter_valid_design_ids(module_bom_simple_df)
            
            # Updated to prioritize MATERIAL_DESCRIPTION
            material_desc_columns = ['MATERIAL_DESCRIPTION', 'Material_Description', 'Material Description']
            material_desc_col = None
            
            for col_name in material_desc_columns:
                if col_name in filtered_df.columns:
                    material_desc_col = col_name
                    break
            
            if material_desc_col:
                # Use partial matching - search for MPNs that contain the search term
                matches = filtered_df[
                    filtered_df[material_desc_col].astype(str).str.contains(search_term, case=False, na=False)
                ][material_desc_col].unique()
                matching_mpns.extend(matches)
            else:
                st.error("No material description column found in the database")
                return []
        
        matching_mpns = sorted(list(set([mpn for mpn in matching_mpns if mpn and str(mpn).strip() and str(mpn).lower() != 'nan'])))
        
    except Exception as e:
        st.error(f"Error searching MPNs: {e}")
    
    return matching_mpns

def get_process_code_from_rest_api(selected_mpn, module_bom_simple_df):
    try:
        if module_bom_simple_df.empty:
            return "No data available", None
        
        # Filter out invalid Design IDs first
        filtered_df = filter_valid_design_ids(module_bom_simple_df)
        
        # Updated to prioritize MATERIAL_DESCRIPTION
        material_desc_columns = ['MATERIAL_DESCRIPTION', 'Material_Description', 'Material Description']
        material_desc_col = None
        
        for col_name in material_desc_columns:
            if col_name in filtered_df.columns:
                material_desc_col = col_name
                break
        
        if not material_desc_col:
            return "No material description column found in database", None
        
        matches = filtered_df[
            filtered_df[material_desc_col].astype(str).str.contains(selected_mpn, case=False, na=False)
        ]
        
        if matches.empty:
            return f"No records found for MPN: {selected_mpn}", None
        
        return f"Found {len(matches)} records", matches
        
    except Exception as e:
        return f"Error: {str(e)}", None

def filter_valid_design_ids(df):
    """Filter out records with invalid Design IDs (starting with Z, V, or U)."""
    if df.empty:
        return df
    
    # Find Design ID column
    design_id_column = None
    design_id_candidates = ['DESIGN_ID', 'Design_ID', 'DesignID', 'Design ID']
    
    for candidate in design_id_candidates:
        if candidate in df.columns:
            design_id_column = candidate
            break
    
    if not design_id_column:
        # If no Design ID column found, return original dataframe
        return df
    
    # Filter out invalid Design IDs
    initial_count = len(df)
    filtered_df = df[df[design_id_column].apply(is_valid_design_id)]
    final_count = len(filtered_df)
    
    return filtered_df

def is_valid_design_id(design_id):
    """Check if Design ID is valid (doesn't start with Z, V, or U)."""
    if not design_id or str(design_id).lower() in ['nan', 'none', '', 'null', 'na']:
        return False
    
    design_str = str(design_id).strip().upper()
    
    # Filter out Design IDs starting with Z, V, or U
    if design_str.startswith(('Z', 'V', 'U')):
        return False
    
    return True

def extract_form_factors_from_sql(module_bom_simple_df):
    default_form_factors = ["No Filter", "CSODIMM", "CUDIMM", "MRDIMM", "RDIMM", "SODIMM", "UDIMM", "LPCAMM", "SlimCAMM", "SOCAMM", "NA"]
    
    if module_bom_simple_df is None or module_bom_simple_df.empty:
        return default_form_factors
    
    try:
        form_factors = set()
        
        if 'FORM_FACTOR' in module_bom_simple_df.columns:
            ff_values = module_bom_simple_df['FORM_FACTOR'].dropna().astype(str)
            for ff in ff_values:
                if ff and ff.strip() and ff.strip().upper() not in ['NAN', 'NULL', 'NONE', '']:
                    form_factors.add(ff.strip().upper())
        
        all_form_factors = ["No Filter"]
        
        priority_order = ['SODIMM', 'UDIMM', 'RDIMM', 'CUDIMM', 'CSODIMM', 'MRDIMM', 'LPCAMM', 'SlimCAMM', 'SOCAMM', 'CAMM']
        
        for ff in priority_order:
            if ff in form_factors:
                all_form_factors.append(ff)
                form_factors.remove(ff)
        
        for ff in sorted(form_factors):
            all_form_factors.append(ff)
        
        for ff in default_form_factors:
            if ff not in all_form_factors:
                all_form_factors.append(ff)
        
        return all_form_factors
        
    except Exception as e:
        st.sidebar.warning(f"Error extracting form factors from SQL data: {e}")
        return default_form_factors

def extract_speeds_from_sql(module_bom_simple_df):
    default_speeds = ["No Filter", "6400", "7200", "8000", "8800", "8000+", "12800", "12800+", "NA"]
    
    if module_bom_simple_df is None or module_bom_simple_df.empty:
        return default_speeds
    
    try:
        speeds = set()
        
        if 'TRANSFERS_PER_SECOND' in module_bom_simple_df.columns:
            speed_values = module_bom_simple_df['TRANSFERS_PER_SECOND'].dropna().astype(str)
            
            for value in speed_values:
                if value and value.strip() and value.strip().upper() not in ['NAN', 'NULL', 'NONE', '']:
                    speed_matches = re.findall(r'\b(\d{4,5})\b', value)
                    for speed in speed_matches:
                        if 4000 <= int(speed) <= 15000:
                            speeds.add(speed)
                    
                    plus_matches = re.findall(r'\b(\d{4,5}\+)\b', value)
                    for speed in plus_matches:
                        base_speed = speed.replace('+', '')
                        if 4000 <= int(base_speed) <= 15000:
                            speeds.add(speed)
        
        all_speeds = ["No Filter"]
        
        numeric_speeds = sorted([s for s in speeds if s.replace('+', '').isdigit()], 
                               key=lambda x: int(x.replace('+', '')))
        all_speeds.extend(numeric_speeds)
        
        for speed in default_speeds:
            if speed not in all_speeds:
                all_speeds.append(speed)
        
        return all_speeds
        
    except Exception as e:
        st.sidebar.warning(f"Error extracting speeds from SQL data: {e}")
        return default_speeds

def determine_segment_and_widget(process_code):
    if not process_code or str(process_code).lower() in ['nan', 'none', '', 'null', 'na']:
        return 'Unknown', 'standard'
    
    process_code_str = str(process_code).strip().upper()
    
    non_zero_count = 0
    for char in process_code_str:
        if char != '0':
            non_zero_count += 1
        else:
            break
    
    if len(process_code_str) >= 4 or non_zero_count >= 4:
        return 'Server', 'standard'
    elif len(process_code_str) >= 2 or non_zero_count >= 2:
        return 'Client', 'standard'
    else:
        return 'Unknown', 'standard'

def initialize_data_loading():
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    
    if 'sql_data' not in st.session_state:
        st.session_state.sql_data = None
    
    if 'sharepoint_data' not in st.session_state:
        st.session_state.sharepoint_data = None
    
    if 'data_load_error' not in st.session_state:
        st.session_state.data_load_error = None

def load_all_data():
    try:
        st.session_state.data_load_error = None
        
        with st.spinner("Loading SQL data..."):
            sql_data = load_data_cached()
            st.session_state.sql_data = sql_data
        
        with st.spinner("Loading SharePoint data..."):
            sharepoint_data = load_sharepoint_data_cached()
            st.session_state.sharepoint_data = sharepoint_data
        
        st.session_state.data_loaded = True
        return True
        
    except Exception as e:
        st.session_state.data_load_error = str(e)
        st.session_state.data_loaded = False
        return False

def get_cached_data():
    if not st.session_state.data_loaded:
        return None, None
    
    return st.session_state.sql_data, st.session_state.sharepoint_data

def show_data_loading_interface():
    st.sidebar.header("Data Management")
    
    # SharePoint Credentials Section
    st.sidebar.subheader("SharePoint Credentials")
    
    # Get current credentials from secrets or use defaults
    default_sp_username = st.secrets.get("sharepoint_username", "your_username@micron.com")
    default_sp_password = st.secrets.get("sharepoint_password", "")
    
    sharepoint_username = st.sidebar.text_input(
        "SharePoint Username",
        value=st.session_state.get("sharepoint_username", default_sp_username),
        key="sharepoint_username_input",
        help="Enter your SharePoint username (email)"
    )
    
    sharepoint_password = st.sidebar.text_input(
        "SharePoint Password",
        value=st.session_state.get("sharepoint_password", default_sp_password),
        type="password",
        key="sharepoint_password_input",
        help="Enter your SharePoint password"
    )
    
    # Update session state with credentials
    st.session_state["sharepoint_username"] = sharepoint_username
    st.session_state["sharepoint_password"] = sharepoint_password
    
    if st.session_state.data_loaded:
        sql_data, sharepoint_data = get_cached_data()
        
        if 'last_refresh_time' in st.session_state:
            local_timezone = pytz.timezone('America/Denver')
            local_time_obj = datetime.now(local_timezone)
            formatted_time = local_time_obj.strftime('%Y-%m-%d %H:%M:%S')
            tz_abbr = local_time_obj.strftime('%Z')
            st.sidebar.info(f"Last refreshed: {formatted_time} {tz_abbr}")
    
    else:
        st.sidebar.warning("Data not loaded")
        if st.session_state.data_load_error:
            st.sidebar.error(f"Error: {st.session_state.data_load_error}")
    
    if st.sidebar.button("Refresh Data", key="refresh_data_button", help="Reload data from SQL Database and SharePoint"):
        with st.spinner("Refreshing data..."):
            if hasattr(load_data_cached, 'clear'):
                load_data_cached.clear()
            if hasattr(load_sharepoint_data_cached, 'clear'):
                load_sharepoint_data_cached.clear()
            
            for key in ['sql_data', 'sharepoint_data', 'data_loaded', 'data_load_error']:
                if key in st.session_state:
                    del st.session_state[key]
            
            success = load_all_data()
            
            if success:
                st.session_state.last_refresh_time = datetime.now()
                st.sidebar.success("Data refreshed successfully!")
                st.rerun()
            else:
                st.sidebar.error("Failed to refresh data")

def explain_process_code_without_zeros(process_code, segment):
    """Explain process code using only non-zero characters."""
    if not process_code:
        return "No process code provided"
    
    # Remove zeros and get clean code
    clean_code = str(process_code).replace('0', '').strip()
    
    if not clean_code:
        return "Process code contains only zeros"
    
    explanation = f"Process Code: {clean_code} (Segment: {segment})\n"
    explanation += "Position Breakdown:\n"
    
    if segment.lower() == 'server':
        components = ['PMIC', 'SPD/Hub', 'Temp Sensor', 'RCD/MRCD', 'Data Buffer']
    elif segment.lower() == 'client':
        components = ['PMIC', 'SPD/Hub', 'CKD']
    else:
        components = ['Unknown'] * len(clean_code)
    
    for i, char in enumerate(clean_code):
        if i < len(components):
            explanation += f"- Position {i+1}: {components[i]} → {char}\n"
    
    return explanation

def lookup_process_code_components_without_zeros(process_code, segment, component_validations_df, is_socamm=False):
    """Look up components for a given process code using only non-zero characters."""
    if component_validations_df is None or component_validations_df.empty:
        return pd.DataFrame(), "No SharePoint data available"
    
    if not process_code or str(process_code).strip() == '':
        return pd.DataFrame(), "No process code provided"
    
    # Remove zeros from process code
    clean_process_code = str(process_code).replace('0', '').strip().upper()
    
    if not clean_process_code:
        return pd.DataFrame(), "Process code contains only zeros"
    
    # Define positional component type mapping with simplified expected types
    if is_socamm:
        # SOCAMM: 1st character = SPD/Hub, 2nd-3rd characters = Voltage Regulator
        position_mapping = {
            1: {
                'expected_type': 'SPD/Hub',
                'search_terms': ['SPD/Hub', 'SPD', 'Hub']
            },
            2: {
                'expected_type': 'Voltage Regulator',
                'search_terms': ['Voltage Regulator', 'VR', 'Regulator']
            }
        }
    elif segment.lower() == 'server':
        position_mapping = {
            1: {
                'expected_type': 'PMIC',
                'search_terms': ['PMIC']
            },
            2: {
                'expected_type': 'SPD/Hub',
                'search_terms': ['SPD/Hub', 'SPD', 'Hub']
            },
            3: {
                'expected_type': 'Temp Sensor',
                'search_terms': ['Temp Sensor', 'Temperature Sensor']
            },
            4: {
                'expected_type': 'RCD/MRCD',
                'search_terms': ['RCD', 'MRCD', 'Muxed RCD', 'RCD/MRCD']
            },
            5: {
                'expected_type': 'Data Buffer',
                'search_terms': ['Data Buffer', 'Buffer']
            }
        }
    elif segment.lower() == 'client':
        position_mapping = {
            1: {
                'expected_type': 'PMIC',
                'search_terms': ['PMIC']
            },
            2: {
                'expected_type': 'SPD/Hub',
                'search_terms': ['SPD/Hub', 'SPD', 'Hub']
            },
            3: {
                'expected_type': 'CKD',
                'search_terms': ['CKD', 'Clock Driver']
            }
        }
    else:
        return pd.DataFrame(), f"Unknown segment: {segment}"
    
    # Get all component types available in SharePoint for this segment
    segment_mask = (
        (component_validations_df['Segment'] == segment) |
        (component_validations_df['Segment'] == 'Server/Client') |
        (component_validations_df['Segment'] == 'Client/Server')
    )
    
    segment_data = component_validations_df[segment_mask]
    
    if segment_data.empty:
        return pd.DataFrame(), f"No data found for segment: {segment}"
    
    results = []
    
    if is_socamm:
        # Handle SOCAMM special case
        if len(clean_process_code) >= 1:
            # Position 1: SPD/Hub
            char = clean_process_code[0]
            mapping = position_mapping[1]
            expected_type = mapping['expected_type']
            search_terms = mapping['search_terms']
            position_matches = []
            
            for search_term in search_terms:
                type_matches = segment_data[
                    (segment_data['Process_Code'].astype(str).str.upper() == char) &
                    (segment_data['Component_Type'].str.contains(search_term, case=False, na=False))
                ]
                
                if not type_matches.empty:
                    position_matches.extend(type_matches.to_dict('records'))
            
            if position_matches:
                # Deduplicate matches
                seen_components = set()
                unique_matches = []
                
                for match in position_matches:
                    component_key = (
                        match.get('Component_Type', ''),
                        match.get('Supplier', ''),
                        match.get('Component_Generation', ''),
                        match.get('Revision', ''),
                        match.get('Process_Code', ''),
                        match.get('Product_Description', '')
                    )
                    
                    if component_key not in seen_components:
                        seen_components.add(component_key)
                        unique_matches.append(match)
                
                # Add ALL unique matches for this position
                for i, match in enumerate(unique_matches):
                    results.append({
                        'Position': 1,
                        'Expected_Component_Type': expected_type,
                        'Process_Code_Character': char,
                        'Product_Description': match.get('Product_Description', ''),
                        'Supplier': match.get('Supplier', ''),
                        'Component_Generation': match.get('Component_Generation', ''),
                        'Revision': match.get('Revision', ''),
                        'SAP_Number': match.get('SAP_Number', ''),
                        'SPN': match.get('SPN', ''),
                        'Actual_Component_Type': match.get('Component_Type', ''),
                        'Actual_Segment': match.get('Segment', ''),
                        'Option_Number': i + 1 if len(unique_matches) > 1 else None
                    })
            else:
                results.append({
                    'Position': 1,
                    'Expected_Component_Type': expected_type,
                    'Process_Code_Character': char,
                    'Product_Description': f'No {expected_type} found with code "{char}"',
                    'Supplier': '',
                    'Component_Generation': '',
                    'Revision': '',
                    'SAP_Number': '',
                    'SPN': '',
                    'Actual_Component_Type': '',
                    'Actual_Segment': '',
                    'Option_Number': None
                })
        
        if len(clean_process_code) >= 3:
            # Position 2-3: Voltage Regulator (2-character code)
            voltage_regulator_code = clean_process_code[1:3]
            mapping = position_mapping[2]
            expected_type = mapping['expected_type']
            search_terms = mapping['search_terms']
            position_matches = []
            
            for search_term in search_terms:
                type_matches = segment_data[
                    (segment_data['Process_Code'].astype(str).str.upper() == voltage_regulator_code) &
                    (segment_data['Component_Type'].str.contains(search_term, case=False, na=False))
                ]
                
                if not type_matches.empty:
                    position_matches.extend(type_matches.to_dict('records'))
            
            if position_matches:
                # Deduplicate matches
                seen_components = set()
                unique_matches = []
                
                for match in position_matches:
                    component_key = (
                        match.get('Component_Type', ''),
                        match.get('Supplier', ''),
                        match.get('Component_Generation', ''),
                        match.get('Revision', ''),
                        match.get('Process_Code', ''),
                        match.get('Product_Description', '')
                    )
                    
                    if component_key not in seen_components:
                        seen_components.add(component_key)
                        unique_matches.append(match)
                
                # Add ALL unique matches for this position
                for i, match in enumerate(unique_matches):
                    results.append({
                        'Position': '2-3',
                        'Expected_Component_Type': expected_type,
                        'Process_Code_Character': voltage_regulator_code,
                        'Product_Description': match.get('Product_Description', ''),
                        'Supplier': match.get('Supplier', ''),
                        'Component_Generation': match.get('Component_Generation', ''),
                        'Revision': match.get('Revision', ''),
                        'SAP_Number': match.get('SAP_Number', ''),
                        'SPN': match.get('SPN', ''),
                        'Actual_Component_Type': match.get('Component_Type', ''),
                        'Actual_Segment': match.get('Segment', ''),
                        'Option_Number': i + 1 if len(unique_matches) > 1 else None
                    })
            else:
                results.append({
                    'Position': '2-3',
                    'Expected_Component_Type': expected_type,
                    'Process_Code_Character': voltage_regulator_code,
                    'Product_Description': f'No {expected_type} found with code "{voltage_regulator_code}"',
                    'Supplier': '',
                    'Component_Generation': '',
                    'Revision': '',
                    'SAP_Number': '',
                    'SPN': '',
                    'Actual_Component_Type': '',
                    'Actual_Segment': '',
                    'Option_Number': None
                })
    else:
        # Handle regular (non-SOCAMM) process codes
        for position, char in enumerate(clean_process_code, 1):
            if position not in position_mapping:
                # Position beyond expected range
                results.append({
                    'Position': position,
                    'Expected_Component_Type': 'Unknown',
                    'Process_Code_Character': char,
                    'Product_Description': f'Position {position} not defined for {segment}',
                    'Supplier': '',
                    'Component_Generation': '',
                    'Revision': '',
                    'SAP_Number': '',
                    'SPN': '',
                    'Actual_Component_Type': '',
                    'Actual_Segment': '',
                    'Option_Number': None
                })
                continue
            
            mapping = position_mapping[position]
            expected_type = mapping['expected_type']
            search_terms = mapping['search_terms']
            position_matches = []
            
            # Look for components that match both the process code character AND the expected component type for this position
            for search_term in search_terms:
                # Find components that match the process code character and are of the expected type for this position
                type_matches = segment_data[
                    (segment_data['Process_Code'].astype(str).str.upper() == char) &
                    (segment_data['Component_Type'].str.contains(search_term, case=False, na=False))
                ]
                
                if not type_matches.empty:
                    position_matches.extend(type_matches.to_dict('records'))
            
            if position_matches:
                # Deduplicate matches based on key fields to avoid showing the same component multiple times
                seen_components = set()
                unique_matches = []
                
                for match in position_matches:
                    # Create a unique key based on important fields
                    component_key = (
                        match.get('Component_Type', ''),
                        match.get('Supplier', ''),
                        match.get('Component_Generation', ''),
                        match.get('Revision', ''),
                        match.get('Process_Code', ''),
                        match.get('Product_Description', '')
                    )
                    
                    if component_key not in seen_components:
                        seen_components.add(component_key)
                        unique_matches.append(match)
                
                # If we have multiple unique matches, sort by preference
                if len(unique_matches) > 1:
                    # Sort by preference: prefer non-empty SAP numbers, then by supplier name
                    unique_matches.sort(key=lambda x: (
                        x.get('SAP_Number', '') == '',  # False (non-empty SAP) comes first
                        x.get('Supplier', ''),
                        x.get('Component_Generation', ''),
                        x.get('Revision', '')
                    ))
                
                # Add ALL unique matches for this position
                for i, match in enumerate(unique_matches):
                    results.append({
                        'Position': position,
                        'Expected_Component_Type': expected_type,
                        'Process_Code_Character': char,
                        'Product_Description': match.get('Product_Description', ''),
                        'Supplier': match.get('Supplier', ''),
                        'Component_Generation': match.get('Component_Generation', ''),
                        'Revision': match.get('Revision', ''),
                        'SAP_Number': match.get('SAP_Number', ''),
                        'SPN': match.get('SPN', ''),
                        'Actual_Component_Type': match.get('Component_Type', ''),
                        'Actual_Segment': match.get('Segment', ''),
                        'Option_Number': i + 1 if len(unique_matches) > 1 else None
                    })
            else:
                # No matches found for this position and character
                results.append({
                    'Position': position,
                    'Expected_Component_Type': expected_type,
                    'Process_Code_Character': char,
                    'Product_Description': f'No {expected_type} found with code "{char}"',
                    'Supplier': '',
                    'Component_Generation': '',
                    'Revision': '',
                    'SAP_Number': '',
                    'SPN': '',
                    'Actual_Component_Type': '',
                    'Actual_Segment': '',
                    'Option_Number': None
                })
    
    results_df = pd.DataFrame(results)
    
    if results_df.empty:
        return pd.DataFrame(), "No components found for this process code"
    
    return results_df, f"Found {len(results_df)} component options"

def main():
    st.title("Process Code & MPN Lookup")
    
    show_process_code_info()
    
    initialize_data_loading()
    
    show_data_loading_interface()
    
    if not st.session_state.data_loaded:
        st.info("Loading data for the first time...")
        success = load_all_data()
        
        if success:
            st.session_state.last_refresh_time = datetime.now()
            st.success("Data loaded successfully!")
            st.rerun()
        else:
            st.error("Failed to load data. Please check your connections and try refreshing.")
            st.stop()
    
    module_bom_simple_df, sharepoint_data = get_cached_data()
    
    if module_bom_simple_df is None:
        st.error("SQL data not available. Please refresh the data.")
        st.stop()
    
    component_validations_df = sharepoint_data['component_validations_df'] if sharepoint_data else pd.DataFrame()
    
    predefined_options = get_predefined_options(component_validations_df)
    
    tab1, tab2, tab3 = st.tabs(["Process Code Lookup", "Process Code Generator", "MPN Lookup"])

    with tab1:
        st.write("Enter a process code to look up the associated parts:")
        
        col1, col2 = st.columns(2)
        
        with col1:
            default_segment = "Client"
            if "lookup_segment" in st.session_state and st.session_state["lookup_segment"]:
                default_segment = st.session_state["lookup_segment"]
            
            segment_options = predefined_options['segment']
            try:
                default_segment_index = segment_options.index(default_segment)
            except ValueError:
                default_segment_index = 0
            
            lookup_segment = st.selectbox(
                "Segment", 
                options=segment_options, 
                index=default_segment_index,
                key="lookup_segment_select"
            )
        
        with col2:
            # Add some spacing before the checkbox
            st.write("")  # Empty line for spacing
            st.write("")  # Empty line for spacing
            is_socamm = st.checkbox(
                "Is this a SOCAMM Form Factor?",
                value=False,
                key="is_socamm_checkbox",
                help="Check this box if the process code is for a SOCAMM form factor"
            )
        
        if is_socamm:
            st.info("**SOCAMM Form Factor Selected**: Enter a 3-character process code where:")
            st.markdown("- **1st character**: SPD/Hub")
            st.markdown("- **2nd & 3rd characters**: Voltage Regulator (e.g., AA, AB, AC)")
            
            socamm_process_code = st.text_input(
                "SOCAMM Process Code (3 characters)",
                value="",
                key="socamm_process_code_input",
                help="Enter exactly 3 characters: 1st for SPD/Hub, 2nd-3rd for Voltage Regulator",
                max_chars=3
            )
            
            if socamm_process_code and socamm_process_code.strip():
                socamm_process_code = socamm_process_code.strip().upper()
                
                # Remove zeros from input
                clean_socamm_code = socamm_process_code.replace('0', '')
                
                if not clean_socamm_code:
                    st.error("Process code cannot contain only zeros")
                elif len(clean_socamm_code) < 3:
                    st.warning(f"SOCAMM process code should have 3 non-zero characters. You entered: {clean_socamm_code}")
                else:
                    st.success(f"SOCAMM process code entered: {clean_socamm_code}")
                    
                    # Look up SOCAMM components using the zero-filtered function
                    if not component_validations_df.empty:
                        results_df, message = lookup_process_code_components_without_zeros(clean_socamm_code, "Client", component_validations_df, is_socamm=True)
                        
                        if not results_df.empty:
                            st.subheader("SOCAMM Component Breakdown")
                            
                            # Format column headers by replacing underscores with spaces
                            display_results_df = results_df.copy()
                            display_results_df.columns = [col.replace('_', ' ') for col in display_results_df.columns]
                            
                            # Group by position to show multiple options clearly
                            if 'Option Number' in display_results_df.columns:
                                # Sort by Position and Option Number
                                display_results_df = display_results_df.sort_values(['Position', 'Option Number'], na_position='first')
                                
                                # Add option indicator to Product Description for clarity
                                for idx, row in display_results_df.iterrows():
                                    if pd.notna(row.get('Option Number')):
                                        display_results_df.at[idx, 'Product Description'] = f"[Option {int(row['Option Number'])}] {row['Product Description']}"
                            
                            st.dataframe(display_results_df, use_container_width=True)
                            
                            # Show summary of options per position
                            if 'Option Number' in display_results_df.columns:
                                option_summary = display_results_df.groupby('Position')['Option Number'].count().reset_index()
                                option_summary.columns = ['Position', 'Number of Options']
                                option_summary = option_summary[option_summary['Number of Options'] > 1]
                    
                        else:
                            st.warning(f"No components found for SOCAMM process code: {clean_socamm_code}")
                    else:
                        st.warning("SharePoint data not available for component lookup")
        else:
            lookup_process_code = st.text_input(
                "Process Code Lookup (Position Order)", 
                value="",
                key="lookup_process_code_input",
                help="Enter the process code in position order (zeros will be automatically filtered out)"
            )
            
            print_order_code = st.text_input(
                "Process Code Lookup (Print Order)",
                value="",
                help="Enter the process code as it appears on the product label (zeros will be automatically filtered out)",
                key="print_order_process_code_input"
            )
            
            if lookup_process_code and lookup_process_code.strip():
                # Remove zeros from the input
                clean_lookup_code = lookup_process_code.replace('0', '').strip()
                
                if not clean_lookup_code:
                    st.error("Process code cannot contain only zeros")
                else:
                    st.success(f"Position order process code entered: {clean_lookup_code}")
                    
                    # Show print order equivalent only if it's different and for server codes
                    if lookup_segment.lower() == 'server':
                        print_order_equivalent = convert_process_code_to_print_order(clean_lookup_code, lookup_segment)
                        if print_order_equivalent != clean_lookup_code:
                            # Also remove zeros from print order display
                            clean_print_order = print_order_equivalent.replace('0', '')
                            st.info(f"Print order equivalent: {clean_print_order}")
                    
                    explanation = explain_process_code_without_zeros(clean_lookup_code, lookup_segment)
                    with st.expander("Process Code Explanation", expanded=False):
                        st.text(explanation)
                
                # Look up components for this process code using the zero-filtered function
                if not component_validations_df.empty:
                    results_df, message = lookup_process_code_components_without_zeros(clean_lookup_code, lookup_segment, component_validations_df)
                    
                    if not results_df.empty:
                        st.subheader("Component Breakdown")
                        
                        # Format column headers by replacing underscores with spaces
                        display_results_df = results_df.copy()
                        display_results_df.columns = [col.replace('_', ' ') for col in display_results_df.columns]
                        
                        # Group by position to show multiple options clearly
                        if 'Option Number' in display_results_df.columns:
                            # Sort by Position and Option Number
                            display_results_df = display_results_df.sort_values(['Position', 'Option Number'], na_position='first')
                            
                            # Add option indicator to Product Description for clarity
                            for idx, row in display_results_df.iterrows():
                                if pd.notna(row.get('Option Number')):
                                    display_results_df.at[idx, 'Product Description'] = f"[Option {int(row['Option Number'])}] {row['Product Description']}"
                        
                        st.dataframe(display_results_df, use_container_width=True)
                        
                        # Show summary of options per position
                        if 'Option Number' in display_results_df.columns:
                            option_summary = display_results_df.groupby('Position')['Option Number'].count().reset_index()
                            option_summary.columns = ['Position', 'Number of Options']
                            option_summary = option_summary[option_summary['Number of Options'] > 1]
                            
                    else:
                        st.warning(f"No components found for process code: {clean_lookup_code}")
                else:
                    st.warning("SharePoint data not available for component lookup")    
            elif print_order_code and print_order_code.strip():
                # Remove zeros from the input
                clean_print_code = print_order_code.replace('0', '').strip()
                
                if not clean_print_code:
                    st.error("Process code cannot contain only zeros")
                else:
                    st.success(f"Print order process code entered: {clean_print_code}")
                    
                    # Convert to position order and remove zeros
                    position_order_equivalent = convert_print_order_to_process_code(clean_print_code, lookup_segment)
                    clean_position_order = position_order_equivalent.replace('0', '')
                    
                    if clean_position_order != clean_print_code:
                        st.info(f"Position order equivalent: {clean_position_order}")
                    
                    explanation = explain_process_code_without_zeros(clean_position_order, lookup_segment)
                    with st.expander("Process Code Explanation", expanded=False):
                        st.text(explanation)
                
                # Look up components for this process code using the zero-filtered function
                if not component_validations_df.empty:
                    results_df, message = lookup_process_code_components_without_zeros(clean_position_order, lookup_segment, component_validations_df)
                    
                    if not results_df.empty:
                        st.subheader("Component Breakdown")
                        
                        # Format column headers by replacing underscores with spaces
                        display_results_df = results_df.copy()
                        display_results_df.columns = [col.replace('_', ' ') for col in display_results_df.columns]
                        
                        st.dataframe(display_results_df, use_container_width=True)
                    else:
                        st.warning(f"No components found: {message}")
                else:
                    st.warning("SharePoint data not available for component lookup")

    with tab2:
        st.write("Generate a process code by selecting individual components:")
        
        col1, col2 = st.columns(2)
        
        with col1:
            generator_segment = st.selectbox(
                "Segment", 
                options=predefined_options['segment'], 
                key="generator_segment"
            )
        
        if not component_validations_df.empty:
            component_options = get_module_component_options_by_segment(generator_segment, component_validations_df)
            
            if not component_options:
                st.warning(f"No component data available for {generator_segment} segment in SharePoint")
                st.stop()
            
            selected_components = {}
            component_codes = {}
            
            # Sort components by position to maintain proper order
            sorted_components = sorted(component_options.items(), key=lambda x: x[1]['position'])
            
            for component_type, config in sorted_components:
                required_text = "Required" if config['required'] else "Optional"
                
                st.subheader(f"{component_type} ({required_text})")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    suppliers = get_filtered_options(component_validations_df, 'Supplier', 
                                                   Segment=generator_segment, Component_Type=component_type)
                    if suppliers:
                        selected_supplier = st.selectbox(f"Supplier", suppliers, key=f"{component_type}_supplier")
                    else:
                        selected_supplier = None
                        st.warning(f"No suppliers found for {component_type}")
                
                with col2:
                    if selected_supplier:
                        generations = get_filtered_options(component_validations_df, 'Component_Generation',
                                                         Segment=generator_segment, Component_Type=component_type, 
                                                         Supplier=selected_supplier)
                        if generations:
                            selected_generation = st.selectbox(f"Generation", generations, key=f"{component_type}_generation")
                        else:
                            selected_generation = None
                            st.warning(f"No generations found")
                    else:
                        selected_generation = None
                        st.selectbox(f"Generation", [], key=f"{component_type}_generation", disabled=True)
                
                with col3:
                    if selected_supplier and selected_generation:
                        revisions = get_filtered_options(component_validations_df, 'Revision',
                                                       Segment=generator_segment, Component_Type=component_type,
                                                       Supplier=selected_supplier, Component_Generation=selected_generation)
                        if revisions:
                            selected_revision = st.selectbox(f"Revision", revisions, key=f"{component_type}_revision")
                        else:
                            selected_revision = None
                            st.warning(f"No revisions found")
                    else:
                        selected_revision = None
                        st.selectbox(f"Revision", [], key=f"{component_type}_revision", disabled=True)
                
                with col4:
                    st.write("")  # Empty line for spacing
                    if selected_supplier and selected_generation and selected_revision:
                        code, comp_type, filtered_df = get_component_process_code(
                            generator_segment, selected_supplier, selected_generation, 
                            selected_revision, component_type, component_validations_df
                        )
                        
                        if code and "No process code found" not in code and "Process code is empty" not in code:
                            st.success(f"Code: {code}")
                            component_codes[component_type] = code
                            selected_components[component_type] = {
                                'supplier': selected_supplier,
                                'generation': selected_generation,
                                'revision': selected_revision,
                                'code': code,
                                'position': config['position']
                            }
                        else:
                            st.error(f"Error: {code}")
                            component_codes[component_type] = ""
                    else:
                        st.info("Select all fields above")
                        component_codes[component_type] = ""
            
            # Check if all required components are selected
            required_components = [comp_type for comp_type, config in component_options.items() if config['required']]
            missing_required = [comp_type for comp_type in required_components if not component_codes.get(comp_type)]
            
            if missing_required:
                st.warning(f"Please select the following required components: {', '.join(missing_required)}")
            else:
                # Show component summary only
                st.subheader("Component Summary")
                summary_data = []
                # Sort by position for display
                sorted_selected = sorted(selected_components.items(), key=lambda x: x[1]['position'])
                
                for comp_type, details in sorted_selected:
                    summary_data.append({
                        'Position': details['position'],
                        'Component Type': comp_type,
                        'Code': details['code'],
                        'Supplier': details['supplier'],
                        'Generation': details['generation'],
                        'Revision': details['revision']
                    })
                
                if summary_data:
                    summary_df = pd.DataFrame(summary_data)
                    st.dataframe(summary_df, use_container_width=True)
                else:
                    st.error("Failed to generate process code")
        
        else:
            st.warning("SharePoint data not available. Cannot generate process codes.")

    with tab3:
        st.write("Search for MPNs in the database:")
        
        # Show available columns for debugging
        if not module_bom_simple_df.empty:
            material_desc_columns = ['MATERIAL_DESCRIPTION', 'Material_Description', 'Material Description']
            available_col = None
            for col_name in material_desc_columns:
                if col_name in module_bom_simple_df.columns:
                    available_col = col_name
                    break
        
        search_term = st.text_input("Enter search term for MPN (partial match supported):", key="mpn_search")
        
        if search_term and len(search_term.strip()) >= 1:  # Allow search with just 1 character
            with st.spinner("Searching for MPNs..."):
                matching_mpns = search_mpn_in_rest_api(search_term.strip(), module_bom_simple_df)
            
            if matching_mpns:
                st.success(f"Found {len(matching_mpns)} matching MPNs")
                
                selected_mpn = st.selectbox("Select an MPN:", matching_mpns, key="selected_mpn")
                
                if selected_mpn:
                    with st.spinner("Getting process code information..."):
                        result_message, result_data = get_process_code_from_rest_api(selected_mpn, module_bom_simple_df)
                    
                    st.info(result_message)
                    
                    if result_data is not None and not result_data.empty:
                        # Get process codes from the data
                        process_code_columns = ['PROCESS_CODE', 'Process_Code', 'Process Code']
                        process_code_col = None
                        
                        for col_name in process_code_columns:
                            if col_name in result_data.columns:
                                process_code_col = col_name
                                break
                        
                        if process_code_col:
                            unique_process_codes = result_data[process_code_col].dropna().unique()
                            valid_process_codes = []
                            
                            # Filter out invalid process codes and remove zeros
                            for process_code in unique_process_codes:
                                if process_code and str(process_code).strip() and str(process_code).lower() not in ['nan', 'none', 'null', '']:
                                    # Remove zeros from process code
                                    clean_code = str(process_code).strip().replace('0', '')
                                    if clean_code:  # Only add if there are non-zero characters
                                        valid_process_codes.append({
                                            'original': str(process_code).strip(),
                                            'clean': clean_code
                                        })
                            
                            if valid_process_codes:
                                st.subheader("Process Code Lookup")
                                st.write("Click on a process code to see its component breakdown (zeros filtered out):")
                                
                                # Create columns for process code buttons (3 per row)
                                cols_per_row = 3
                                for i in range(0, len(valid_process_codes), cols_per_row):
                                    cols = st.columns(cols_per_row)
                                    
                                    for j, process_code_info in enumerate(valid_process_codes[i:i+cols_per_row]):
                                        with cols[j]:
                                            clean_code = process_code_info['clean']
                                            original_code = process_code_info['original']
                                            
                                            # Determine segment based on clean code character count
                                            char_count = len(clean_code)
                                            
                                            if char_count in [2, 3]:
                                                segment = "Client"
                                            elif char_count in [4, 5]:
                                                segment = "Server"
                                            else:
                                                segment = "Unknown"
                                            
                                            # Display only non-zero characters in button
                                            button_label = f"{clean_code}\n({segment})"
                                            
                                            if st.button(button_label, key=f"process_code_btn_{original_code}_{i}_{j}"):
                                                # Store the CLEAN process code and segment in session state
                                                st.session_state['selected_process_code_lookup'] = clean_code
                                                st.session_state['selected_segment_lookup'] = segment
                                                st.session_state['selected_display_code'] = clean_code
                                
                                # Show process code lookup results if a button was clicked
                                if 'selected_process_code_lookup' in st.session_state and 'selected_segment_lookup' in st.session_state:
                                    selected_process_code = st.session_state['selected_process_code_lookup']
                                    selected_segment = st.session_state['selected_segment_lookup']
                                    selected_display_code = st.session_state.get('selected_display_code', selected_process_code)
                                    
                                    st.subheader(f"Process Code Analysis: {selected_display_code}")
                                    
                                    # Show print order equivalent for server codes (also without zeros)
                                    if selected_segment.lower() == 'server':
                                        print_order_equivalent = convert_process_code_to_print_order(selected_display_code, selected_segment)
                                        if print_order_equivalent != selected_display_code:
                                            # Remove zeros from print order display too
                                            print_order_display = print_order_equivalent.replace('0', '')
                                            if print_order_display:  # Only show if there are non-zero characters
                                                st.info(f"Print order equivalent: {print_order_display}")
                                    
                                    # Look up components for this process code using the zero-filtered function
                                    if not component_validations_df.empty:
                                        results_df, message = lookup_process_code_components_without_zeros(selected_display_code, selected_segment, component_validations_df)
                                        
                                        if not results_df.empty:
                                            st.subheader("Component Breakdown")
                                            
                                            # Format column headers by replacing underscores with spaces
                                            display_results_df = results_df.copy()
                                            display_results_df.columns = [col.replace('_', ' ') for col in display_results_df.columns]
                                            
                                            # Group by position to show multiple options clearly
                                            if 'Option Number' in display_results_df.columns:
                                                # Sort by Position and Option Number
                                                display_results_df = display_results_df.sort_values(['Position', 'Option Number'], na_position='first')
                                                
                                                # Add option indicator to Product Description for clarity
                                                for idx, row in display_results_df.iterrows():
                                                    if pd.notna(row.get('Option Number')):
                                                        display_results_df.at[idx, 'Product Description'] = f"[Option {int(row['Option Number'])}] {row['Product Description']}"
                                            
                                            st.dataframe(display_results_df, use_container_width=True)
                                            
                                            # Show summary of options per position
                                            if 'Option Number' in display_results_df.columns:
                                                option_summary = display_results_df.groupby('Position')['Option Number'].count().reset_index()
                                                option_summary.columns = ['Position', 'Number of Options']
                                                option_summary = option_summary[option_summary['Number of Options'] > 1]
                                                
                                        else:
                                            st.warning(f"No components found: {message}")
                                    else:
                                        st.warning("SharePoint data not available for component lookup")
                                    
                                    # Add a button to clear the selection
                                    if st.button("Clear Selection", key="clear_process_code_selection"):
                                        # Clear the session state
                                        for key in ['selected_process_code_lookup', 'selected_segment_lookup', 'selected_display_code']:
                                            if key in st.session_state:
                                                del st.session_state[key]
                                        st.rerun()
                            else:
                                st.warning("No valid process codes found (all contained only zeros)")
                        else:
                            st.warning("No process code column found in the data")
                    else:
                        st.warning("No data found for the selected MPN")
            else:
                st.warning("No matching MPNs found")
        elif search_term and len(search_term.strip()) < 1:
            st.info("Please enter at least 1 character to search")

if __name__ == "__main__":
    main()