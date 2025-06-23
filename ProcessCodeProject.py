import streamlit as st
import pandas as pd
import pytz
import urllib.parse
import pymssql
import base64
import requests
from datetime import datetime
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.listitems.caml.query import CamlQuery

class PartSpecification:
    def __init__(self):
        self.segment = ""
        self.supplier = ""
        self.component_gen = ""
        self.revision = ""
        self.process_code = ""
        self.pmic = ""
        self.spd_hub = ""
        self.temp_sensor = ""
        self.rcd_mrcd = ""
        self.data_buffer = ""
        self.associated_parts = ""
    
    def set_segment(self, segment):
        self.segment = segment
    
    def set_supplier(self, supplier):
        self.supplier = supplier
    
    def set_component_gen(self, component_gen):
        self.component_gen = component_gen
    
    def set_revision(self, revision):
        self.revision = revision
    
    def set_process_code(self, process_code):
        self.process_code = process_code
    
    def set_pmic(self, pmic):
        self.pmic = pmic
    
    def set_spd_hub(self, spd_hub):
        self.spd_hub = spd_hub
    
    def set_temp_sensor(self, temp_sensor):
        self.temp_sensor = temp_sensor
    
    def set_rcd_mrcd(self, rcd_mrcd):
        self.rcd_mrcd = rcd_mrcd
    
    def set_data_buffer(self, data_buffer):
        self.data_buffer = data_buffer
    
    def set_associated_parts(self, associated_parts):
        self.associated_parts = associated_parts

def show_process_code_info():
    with st.expander("Process Code Information", expanded=False):
        st.write("""
        ## Process Code Structure
        
        Process codes are used to identify the specific components used in a module. The structure varies by segment:
        
        ### Server Process Code (4-5 Characters)
        1. PMIC
        2. SPD/Hub
        3. Temp Sensor
        4. RCD/MRCD
        5. Data Buffer (Optional)
        
        ### Client Process Code (2-3 Characters)
        1. PMIC
        2. SPD/Hub
        3. CKD (Optional)
        
        ## Print Order on Product Label
        
        ### Server
        PMIC → RCD → SPD/Hub → Temp Sensor → Data Buffer (If applicable)
        
        ### Client
        PMIC → SPD/Hub → CKD (If applicable)
        """)

def get_sql_connection_params():
    """Get SQL Server connection parameters from user input or secrets"""
    # Try to get from secrets first
    if all(key in st.secrets for key in ["sql_server", "sql_database", "sql_username", "sql_password"]):
        return (
            st.secrets["sql_server"],
            st.secrets["sql_database"], 
            st.secrets["sql_username"],
            st.secrets["sql_password"]
        )
    
    # Otherwise get from sidebar
    st.sidebar.subheader("SQL Server Connection")
    
    server = st.sidebar.text_input(
        "Server", 
        value=st.session_state.get('sql_server', 'BOMSSPROD367\\BOMSSPROD367'),
        key="sql_server",
        help="Format: SERVER\\INSTANCE or SERVER,PORT"
    )
    
    database = st.sidebar.text_input(
        "Database", 
        value=st.session_state.get('sql_database', 'BOMSSProd'),
        key="sql_database"
    )
    
    username = st.sidebar.text_input(
        "Username", 
        value=st.session_state.get('sql_username', ''),
        key="sql_username",
        help="SQL Server Authentication username"
    )
    
    password = st.sidebar.text_input(
        "Password", 
        type="password",
        key="sql_password",
        help="SQL Server Authentication password"
    )
    
    return server, database, username, password

def create_pymssql_connection(server, database, username, password):
    """Create SQL connection using pymssql (no ODBC required)"""
    if not all([server, database, username, password]):
        missing = []
        if not server: missing.append("Server")
        if not database: missing.append("Database")
        if not username: missing.append("Username")
        if not password: missing.append("Password")
        
        st.sidebar.error(f"Missing required fields: {', '.join(missing)}")
        return None
    
    try:
        st.sidebar.info("Connecting with pymssql (no ODBC required)...")
        
        # Parse server name for instance or port
        if '\\' in server:
            server_name, instance = server.split('\\')
            port = 1433  # Default port
        elif ',' in server:
            server_name, port = server.split(',')
            port = int(port)
            instance = None
        else:
            server_name = server
            instance = None
            port = 1433
        
        # Create connection using pymssql
        conn = pymssql.connect(
            server=server_name,
            user=username,
            password=password,
            database=database,
            timeout=60,
            login_timeout=60,
            as_dict=True,
            port=port
        )
        
        # Test the connection
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        
        if result and result['test'] == 1:
            st.sidebar.success("✅ Connected successfully using pymssql")
            
            # Test access to required tables
            tables_to_check = ["ModuleBOM_59only", "ModuleBOM_Simple"]
            for table in tables_to_check:
                try:
                    cursor.execute(f"SELECT TOP 1 * FROM {table}")
                    cursor.fetchone()
                    st.sidebar.success(f"✅ Access confirmed to {table}")
                except Exception as table_error:
                    st.sidebar.warning(f"⚠️ {table}: {str(table_error)}")
            
            cursor.close()
            return conn
        
    except Exception as e:
        error_msg = str(e)
        st.sidebar.error(f"❌ Connection failed: {error_msg}")
        
        # Provide specific error guidance
        if "login failed" in error_msg.lower():
            st.sidebar.error("🔐 **Authentication Issue**: Username or password incorrect")
        elif "timeout" in error_msg.lower():
            st.sidebar.error("⏱️ **Timeout Issue**: Connection is timing out")
            
            with st.sidebar.expander("🔧 Timeout Troubleshooting", expanded=True):
                st.write("""
                **Possible Solutions:**
                
                1. **Network Issues:**
                   - Ensure you're connected to Micron VPN
                   - Check if firewall is blocking port 1433
                   - Try from a different network location
                
                2. **Server Load:**
                   - Server may be under heavy load
                   - Try again in a few minutes
                   - Contact IT if persistent
                
                3. **Connection Settings:**
                   - Verify server name is correct
                   - Check if SQL Server is running
                   - Ensure SQL Server Browser service is running
                """)
                
        elif "server not found" in error_msg.lower() or "network" in error_msg.lower():
            st.sidebar.error("🌐 **Network Issue**: Cannot reach server")
            
            with st.sidebar.expander("🔧 Network Troubleshooting", expanded=True):
                st.write("""
                **Check These Items:**
                
                1. **VPN Connection**: Ensure you're connected to Micron VPN
                2. **Server Name**: Verify server name is correct
                3. **Network Access**: Server may only accept connections from specific networks
                4. **DNS Resolution**: Server name may not be resolving correctly
                5. **Firewall**: Port 1433 may be blocked
                """)
        
        return None

def test_sql_connection_detailed():
    """Enhanced connection test function"""
    st.sidebar.subheader("Test SQL Connection")
    
    if st.sidebar.button("Test Connection", key="test_sql_conn_detailed"):
        server, database, username, password = get_sql_connection_params()
        
        if not all([server, database, username, password]):
            st.sidebar.error("Please provide all connection details")
            return
        
        with st.sidebar:
            with st.spinner("Testing connection to SQL Server..."):
                st.info(f"Connecting to: {server}")
                st.info(f"Database: {database}")
                st.info(f"User: {username}")
                
                conn = create_pymssql_connection(server, database, username, password)
                
                if conn:
                    try:
                        cursor = conn.cursor()
                        
                        # Get server info
                        try:
                            cursor.execute("SELECT @@VERSION as Version, @@SERVERNAME as ServerName")
                            server_info = cursor.fetchone()
                            st.info(f"Server: {server_info['ServerName']}")
                            st.info(f"Version: {server_info['Version'][:50]}...")
                        except:
                            pass
                        
                        # Get database info
                        try:
                            cursor.execute("SELECT DB_NAME() as CurrentDB")
                            db_info = cursor.fetchone()
                            st.success(f"Connected to database: {db_info['CurrentDB']}")
                        except:
                            pass
                        
                        # Test table access
                        tables_to_check = ["ModuleBOM_59only", "ModuleBOM_Simple"]
                        for table in tables_to_check:
                            try:
                                cursor.execute(f"SELECT COUNT(*) as RecordCount FROM {table}")
                                count_result = cursor.fetchone()
                                count = count_result['RecordCount']
                                st.success(f"{table}: {count:,} records")
                            except Exception as table_error:
                                st.error(f"{table}: {str(table_error)}")
                        
                        cursor.close()
                        
                    finally:
                        conn.close()

@st.cache_data(ttl=3600)
def load_data_from_sql_server_cached(server, database, username, password):
    """Load data using pymssql with better error handling for remote server"""
    data = {
        'module_bom_59only_df': pd.DataFrame(),
        'module_bom_simple_df': pd.DataFrame()
    }
    
    conn = create_pymssql_connection(server, database, username, password)
    
    if conn is None:
        return data
    
    try:
        cursor = conn.cursor()
        
        # Query ModuleBOM_59only table
        try:
            st.sidebar.info("Loading ModuleBOM_59only...")
            cursor.execute("SELECT * FROM ModuleBOM_59only")
            
            # Fetch all rows
            rows = cursor.fetchall()
            if rows:
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                # Convert to DataFrame
                module_bom_59only_df = pd.DataFrame(rows, columns=columns)
                data['module_bom_59only_df'] = module_bom_59only_df
                st.sidebar.success(f"ModuleBOM_59only: {len(module_bom_59only_df):,} records loaded")
            else:
                st.sidebar.warning("ModuleBOM_59only: No records found")
                
        except Exception as e:
            st.sidebar.error(f"ModuleBOM_59only: {str(e)}")
        
        # Query ModuleBOM_Simple table
        try:
            st.sidebar.info("Loading ModuleBOM_Simple...")
            cursor.execute("SELECT * FROM ModuleBOM_Simple")
            
            # Fetch all rows
            rows = cursor.fetchall()
            if rows:
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                # Convert to DataFrame
                module_bom_simple_df = pd.DataFrame(rows, columns=columns)
                data['module_bom_simple_df'] = module_bom_simple_df
                st.sidebar.success(f"ModuleBOM_Simple: {len(module_bom_simple_df):,} records loaded")
            else:
                st.sidebar.warning("ModuleBOM_Simple: No records found")
                
        except Exception as e:
            st.sidebar.error(f"ModuleBOM_Simple: {str(e)}")
        
        cursor.close()
        
    except Exception as e:
        st.sidebar.error(f"Database connection error: {e}")
        
        # Provide specific guidance for remote connection issues
        if "login failed" in str(e).lower():
            st.sidebar.error("🔐 Authentication failed - Check username/password")
        elif "timeout" in str(e).lower():
            st.sidebar.error("⏱️ Connection timeout - Server may be slow or network issues")
        elif "network" in str(e).lower():
            st.sidebar.error("🌐 Network error - Check VPN connection or firewall")
    
    finally:
        if conn:
            conn.close()
    
    return data

def load_data_from_sql_server():
    """Wrapper function that gets connection params and calls cached version"""
    server, database, username, password = get_sql_connection_params()
    return load_data_from_sql_server_cached(server, database, username, password)

def search_mpn_optimized(search_term, server, database, username, password):
    """Search for MPNs directly in the database for better performance"""
    conn = create_pymssql_connection(server, database, username, password)
    
    if not conn or not search_term:
        return []
    
    try:
        cursor = conn.cursor()
        
        # Search in both tables with optimized queries
        query_59only = """
            SELECT DISTINCT [Material Description] as MPN
            FROM ModuleBOM_59only
            WHERE [Material Description] LIKE %s
            AND [Material Description] IS NOT NULL
            AND [Material Description] != ''
        """
        
        query_simple = """
            SELECT DISTINCT [Material Description] as MPN
            FROM ModuleBOM_Simple
            WHERE [Material Description] LIKE %s
            AND [Material Description] IS NOT NULL
            AND [Material Description] != ''
        """
        
        search_pattern = f"%{search_term}%"
        matching_mpns = []
        
        # Execute queries
        cursor.execute(query_59only, (search_pattern,))
        results_59only = cursor.fetchall()
        matching_mpns.extend([row['MPN'] for row in results_59only if row['MPN']])
        
        cursor.execute(query_simple, (search_pattern,))
        results_simple = cursor.fetchall()
        matching_mpns.extend([row['MPN'] for row in results_simple if row['MPN']])
        
        # Also search in other potential MPN columns
        other_columns_query_59only = """
            SELECT DISTINCT COALESCE([Material Number], [MPN], [Part Number]) as MPN
            FROM ModuleBOM_59only
            WHERE COALESCE([Material Number], [MPN], [Part Number]) LIKE %s
            AND COALESCE([Material Number], [MPN], [Part Number]) IS NOT NULL
            AND COALESCE([Material Number], [MPN], [Part Number]) != ''
        """
        
        other_columns_query_simple = """
            SELECT DISTINCT COALESCE([Material Number], [MPN], [Part Number]) as MPN
            FROM ModuleBOM_Simple
            WHERE COALESCE([Material Number], [MPN], [Part Number]) LIKE %s
            AND COALESCE([Material Number], [MPN], [Part Number]) IS NOT NULL
            AND COALESCE([Material Number], [MPN], [Part Number]) != ''
        """
        
        cursor.execute(other_columns_query_59only, (search_pattern,))
        results_other_59only = cursor.fetchall()
        matching_mpns.extend([row['MPN'] for row in results_other_59only if row['MPN']])
        
        cursor.execute(other_columns_query_simple, (search_pattern,))
        results_other_simple = cursor.fetchall()
        matching_mpns.extend([row['MPN'] for row in results_other_simple if row['MPN']])
        
        cursor.close()
        
        # Remove duplicates and sort
        matching_mpns = sorted(list(set([mpn for mpn in matching_mpns if mpn and str(mpn).strip() and str(mpn).lower() != 'nan'])))
        return matching_mpns
        
    except Exception as e:
        st.error(f"Error searching MPNs: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_process_code_optimized(mpn, server, database, username, password):
    """Look up process code directly from database with optimized query"""
    conn = create_pymssql_connection(server, database, username, password)
    
    if not conn or not mpn:
        return "No MPN provided", None
    
    try:
        cursor = conn.cursor()
        
        # Search in ModuleBOM_Simple first (has process codes)
        query_simple = """
            SELECT 
                'ModuleBOM_Simple' as Source,
                [Material Description] as MPN,
                [Material Number],
                [Material Description],
                [Process Code],
                [Supplier],
                [Component Type]
            FROM ModuleBOM_Simple
            WHERE [Material Description] LIKE %s
            AND [Material Description] IS NOT NULL
        """
        
        # Search in ModuleBOM_59only
        query_59only = """
            SELECT 
                'ModuleBOM_59only' as Source,
                [Material Description] as MPN,
                [Material Number],
                [Material Description],
                'Not Available' as [Process Code],
                [Supplier],
                [Component Type]
            FROM ModuleBOM_59only
            WHERE [Material Description] LIKE %s
            AND [Material Description] IS NOT NULL
        """
        
        search_pattern = f"%{mpn}%"
        results = []
        
        # Execute queries
        cursor.execute(query_simple, (search_pattern,))
        results_simple = cursor.fetchall()
        results.extend(results_simple)
        
        cursor.execute(query_59only, (search_pattern,))
        results_59only = cursor.fetchall()
        results.extend(results_59only)
        
        # Also search in other potential MPN columns
        other_query_simple = """
            SELECT 
                'ModuleBOM_Simple_Other' as Source,
                COALESCE([Material Number], [MPN], [Part Number]) as MPN,
                [Material Number],
                [Material Description],
                [Process Code],
                [Supplier],
                [Component Type]
            FROM ModuleBOM_Simple
            WHERE COALESCE([Material Number], [MPN], [Part Number]) LIKE %s
            AND COALESCE([Material Number], [MPN], [Part Number]) IS NOT NULL
        """
        
        other_query_59only = """
            SELECT 
                'ModuleBOM_59only_Other' as Source,
                COALESCE([Material Number], [MPN], [Part Number]) as MPN,
                [Material Number],
                [Material Description],
                'Not Available' as [Process Code],
                [Supplier],
                [Component Type]
            FROM ModuleBOM_59only
            WHERE COALESCE([Material Number], [MPN], [Part Number]) LIKE %s
            AND COALESCE([Material Number], [MPN], [Part Number]) IS NOT NULL
        """
        
        cursor.execute(other_query_simple, (search_pattern,))
        results_other_simple = cursor.fetchall()
        results.extend(results_other_simple)
        
        cursor.execute(other_query_59only, (search_pattern,))
        results_other_59only = cursor.fetchall()
        results.extend(results_other_59only)
        
        cursor.close()
        
        if not results:
            return f"No records found for MPN: {mpn}", None
        
        # Convert to DataFrame
        result_df = pd.DataFrame(results)
        return "Success", result_df
        
    except Exception as e:
        return f"Error looking up MPN: {e}", None
    finally:
        if conn:
            conn.close()

def analyze_sql_data_optimized(server, database, username, password):
    """Analyze SQL data using direct database queries for better performance"""
    st.subheader("SQL Data Analysis")
    
    conn = create_pymssql_connection(server, database, username, password)
    
    if conn is None:
        st.error("No database connection available")
        return
    
    try:
        cursor = conn.cursor()
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**ModuleBOM_59only Analysis:**")
            try:
                # Get table info
                cursor.execute("SELECT COUNT(*) as record_count FROM ModuleBOM_59only")
                count_result = cursor.fetchone()
                record_count = count_result['record_count'] if count_result else 0
                
                st.write(f"- Total records: {record_count}")
                
                if record_count > 0:
                    # Get column info
                    cursor.execute("""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = 'ModuleBOM_59only'
                        ORDER BY ORDINAL_POSITION
                    """)
                    columns_result = cursor.fetchall()
                    column_names = [row['COLUMN_NAME'] for row in columns_result]
                    
                    st.write(f"- Columns: {len(column_names)}")
                    
                    # Show column names
                    with st.expander("Column Names", expanded=False):
                        for col in column_names:
                            st.write(f"- {col}")
                    
                    # Show sample data
                    with st.expander("Sample Data", expanded=False):
                        cursor.execute("SELECT TOP 5 * FROM ModuleBOM_59only")
                        sample_data = cursor.fetchall()
                        if sample_data:
                            sample_df = pd.DataFrame(sample_data)
                            st.dataframe(sample_df)
                
            except Exception as e:
                st.error(f"Error analyzing ModuleBOM_59only: {e}")
        
        with col2:
            st.write("**ModuleBOM_Simple Analysis:**")
            try:
                # Get table info
                cursor.execute("SELECT COUNT(*) as record_count FROM ModuleBOM_Simple")
                count_result = cursor.fetchone()
                record_count = count_result['record_count'] if count_result else 0
                
                st.write(f"- Total records: {record_count}")
                
                if record_count > 0:
                    # Get column info
                    cursor.execute("""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = 'ModuleBOM_Simple'
                        ORDER BY ORDINAL_POSITION
                    """)
                    columns_result = cursor.fetchall()
                    column_names = [row['COLUMN_NAME'] for row in columns_result]
                    
                    st.write(f"- Columns: {len(column_names)}")
                    
                    # Show column names
                    with st.expander("Column Names", expanded=False):
                        for col in column_names:
                            st.write(f"- {col}")
                    
                    # Show sample data
                    with st.expander("Sample Data", expanded=False):
                        cursor.execute("SELECT TOP 5 * FROM ModuleBOM_Simple")
                        sample_data = cursor.fetchall()
                        if sample_data:
                            sample_df = pd.DataFrame(sample_data)
                            st.dataframe(sample_df)
                    
                    # Check for process codes
                    try:
                        cursor.execute("""
                            SELECT DISTINCT [Process Code] 
                            FROM ModuleBOM_Simple 
                            WHERE [Process Code] IS NOT NULL 
                            AND [Process Code] != ''
                            ORDER BY [Process Code]
                        """)
                        pc_result = cursor.fetchall()
                        process_codes = [row['Process Code'] for row in pc_result if row['Process Code']]
                        
                        st.write(f"- Unique Process Codes: {len(process_codes)}")
                        
                        with st.expander("Process Codes Found", expanded=False):
                            for pc in process_codes:
                                if pc and str(pc).strip():
                                    st.write(f"- {pc}")
                    except Exception as e:
                        st.warning(f"Could not analyze process codes: {e}")
                
            except Exception as e:
                st.error(f"Error analyzing ModuleBOM_Simple: {e}")
        
        cursor.close()
        
    except Exception as e:
        st.error(f"Database connection error during analysis: {e}")
    finally:
        if conn:
            conn.close()

@st.cache_data(ttl=3600)
def load_data_from_sharepoint():
    data = {
        'component_validations_df': pd.DataFrame(),
        'module_validation_df': pd.DataFrame()
    }
    
    sharepoint_site = "https://microncorp.sharepoint.com/sites/mdg"
    list_name = "Module HW Design Component Validations"
    
    if "sharepoint_username" in st.secrets and "sharepoint_password" in st.secrets:
        username = st.secrets["sharepoint_username"]
        password = st.secrets["sharepoint_password"]
    else:
        st.sidebar.subheader("SharePoint Authentication")
        username = st.sidebar.text_input("SharePoint Username (include @micron.com)", key="sp_username")
        password = st.sidebar.text_input("SharePoint Password", type="password", key="sp_password")
    
    if not (username and password):
        st.sidebar.warning("Please provide SharePoint credentials to load data.")
        return data
    
    try:
        user_credentials = UserCredential(username, password)
        ctx = ClientContext(sharepoint_site).with_credentials(user_credentials)
        
        target_list = ctx.web.lists.get_by_title(list_name)
        
        all_lists = ctx.web.lists.get().execute_query()
        available_lists = [list_item.properties.get('Title', '') for list_item in all_lists]
        
        if list_name not in available_lists:
            st.sidebar.warning(f"List '{list_name}' not found. Looking for similar lists...")
            similar_lists = [l for l in available_lists if 
                            any(term in l.lower() for term in ['component', 'validation', 'module', 'design'])]
            
            if similar_lists:
                list_name = similar_lists[0]
                st.sidebar.success(f"Using list: {list_name}")
                target_list = ctx.web.lists.get_by_title(list_name)
            else:
                st.sidebar.error("No suitable lists found")
                return data
        
        end_products_list_name = "End Products"
        end_products_list = None
        
        if end_products_list_name in available_lists:
            end_products_list = ctx.web.lists.get_by_title(end_products_list_name)
            st.sidebar.success(f"Found '{end_products_list_name}' list for speed information")
        else:
            st.sidebar.warning(f"List '{end_products_list_name}' not found. Speed information may be incomplete.")
        
        list_fields = target_list.fields.get().execute_query()
        
        all_items = []
        page_size = 1000
        
        caml_query = CamlQuery()
        caml_query.ViewXml = f"<View><RowLimit>{page_size}</RowLimit></View>"
        
        items = target_list.get_items(caml_query).execute_query()
        all_items.extend(items)
        
        while len(items) == page_size:
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
        
        end_products_items = []
        if end_products_list:
            caml_query = CamlQuery()
            caml_query.ViewXml = f"<View><RowLimit>5000</RowLimit></View>"
            end_products_items = end_products_list.get_items(caml_query).execute_query()
            st.sidebar.success(f"Retrieved {len(end_products_items)} items from End Products list")
            
            speed_mapping = {}
            for item in end_products_items:
                item_props = item.properties
                product_name = str(item_props.get('Title', ''))
                speed_value = str(item_props.get('Speed', ''))
                
                import re
                speed_numbers = re.findall(r'\d+', speed_value)
                if speed_numbers:
                    speed_mapping[product_name] = speed_numbers[0]
        
        if len(all_items) == 0:
            st.sidebar.error("No items found in the list")
            return data
        
        st.sidebar.success(f"Retrieved {len(all_items)} items from SharePoint")
        
        component_validations_data = []
        valid_component_types = ["CKD", "Data Buffer", "Inductor", "Muxed RCD", "PMIC", "RCD", "SPD/Hub", "Temp Sensor", "Voltage Regulator"]
        field_mapping = {
            'Segment': 'Segment',
            'Supplier': 'Supplier',
            'Component_Generation': 'Product_x0020_Family',
            'Revision': 'REV',
            'Component_Type': 'Title',
            'Process_Code': 'Process_x0020_Code',
            'SPN': 'Supplier_x0020_PN',
            'Speed': 'Product_x0020_Comment'
        }

        for item in all_items:
            item_properties = item.properties
            
            record = {}
            for key, field in field_mapping.items():
                if field and field in item_properties:
                    if key == 'Component_Type':
                        title = str(item_properties[field])
                        record[key] = next((ct for ct in valid_component_types if ct in title), "Unknown")
                    else:
                        record[key] = str(item_properties[field])
                else:
                    record[key] = ""
            
            if 'Speed' in record and not record['Speed'] and 'Product_x0020_Comment' in item_properties:
                comment = str(item_properties['Product_x0020_Comment'])
                import re
                speed_numbers = re.findall(r'\b\d{4,5}\b', comment)
                if speed_numbers:
                    record['Speed'] = speed_numbers[0]
            
            product_name = str(item_properties.get('Title', ''))
            if end_products_list and not record['Speed'] and product_name in speed_mapping:
                record['Speed'] = speed_mapping[product_name]
            
            if not any(record.values()):
                for prop_key, prop_value in item_properties.items():
                    if prop_key not in ['_ObjectType_', '_ObjectIdentity_', 'FileSystemObjectType', 'ServerRedirectedEmbedUri', 
                                       'ServerRedirectedEmbedUrl', 'ContentTypeId', 'ComplianceAssetId', 'OData__UIVersionString']:
                        if prop_key == 'Title':
                            title = str(prop_value)
                            record['Component_Type'] = next((ct for ct in valid_component_types if ct in title), "Unknown")
                        elif prop_key == 'Segment':
                            record['Segment'] = str(prop_value)
                        elif prop_key == 'Supplier':
                            record['Supplier'] = str(prop_value)
                        elif prop_key == 'Family_x0020_Description':
                            record['Component_Generation'] = str(prop_value)
                        elif prop_key == 'REV':
                            record['Revision'] = str(prop_value)
                        elif prop_key == 'Process_x0020_Code':
                            record['Process_Code'] = str(prop_value)
                        elif prop_key == 'Supplier_x0020_PN':
                            record['SPN'] = str(prop_value)
                        elif prop_key == 'Speed':
                            record['Speed'] = str(prop_value)
                        elif 'speed' in prop_key.lower() and not record.get('Speed'):
                            record['Speed'] = str(prop_value)
            
            if record.get('Segment') and (record.get('Supplier') or record.get('Component_Type') or record.get('Process_Code')):
                component_validations_data.append(record)
        
        data['component_validations_df'] = pd.DataFrame(component_validations_data)
        
        module_validation_data = []
        module_field_mapping = {
            'Segment': field_mapping['Segment'],
            'Form_Factor': 'Product_x0020_Family',
            'Speed': 'Speed',
            'PMIC': 'Process_x0020_Code_x0020_Char',
            'SPD_Hub': 'Process_x0020_Code_x0020_Char',
            'Temp_Sensor': 'Process_x0020_Code_x0020_Char',
            'RCD_MRCD': 'Process_x0020_Code_x0020_Char',
            'Data_Buffer': 'Process_x0020_Code_x0020_Char',
            'Process_Code': field_mapping['Process_Code']
        }
        
        for item in all_items:
            item_properties = item.properties
            
            record = {}
            for key, field in module_field_mapping.items():
                if field and field in item_properties:
                    if key in ['PMIC', 'SPD_Hub', 'Temp_Sensor', 'RCD_MRCD', 'Data_Buffer']:
                        process_code_chars = str(item_properties[field])
                        if len(process_code_chars) >= 5:
                            record['PMIC'] = process_code_chars[0]
                            record['SPD_Hub'] = process_code_chars[1]
                            record['Temp_Sensor'] = process_code_chars[2]
                            record['RCD_MRCD'] = process_code_chars[3]
                            record['Data_Buffer'] = process_code_chars[4] if len(process_code_chars) > 4 else ''
                    else:
                        record[key] = str(item_properties[field])
                else:
                    record[key] = ""
            
            if not record.get('Speed') and 'Product_x0020_Comment' in item_properties:
                comment = str(item_properties['Product_x0020_Comment'])
                import re
                speed_numbers = re.findall(r'\b\d{4,5}\b', comment)
                if speed_numbers:
                    record['Speed'] = speed_numbers[0]
            
            product_name = str(item_properties.get('Title', ''))
            if end_products_list and not record.get('Speed') and product_name in speed_mapping:
                record['Speed'] = speed_mapping[product_name]
            
            if record.get('Segment') and record.get('Process_Code'):
                module_validation_data.append(record)
        
        data['module_validation_df'] = pd.DataFrame(module_validation_data)
        
    except Exception as e:
        st.sidebar.error(f"Error connecting to SharePoint: {str(e)}")
        
        with st.sidebar.expander("Detailed Error Information", expanded=False):
            st.write(str(e))
            import traceback
            st.write(traceback.format_exc())
            
            st.write("Common solutions:")
            st.write("1. Verify your username and password are correct")
            st.write("2. Ensure you have access to the SharePoint site and list")
            st.write("3. Check if MFA is required for your account")
            st.write("4. Verify the SharePoint site URL and list name are correct")
    
    return data

def get_component_process_code(segment, supplier, component_gen, revision, component_type, component_validations_df):
    try:
        if component_validations_df.empty:
            return "No component validation data available", None, None
        
        df = component_validations_df.copy()
        
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.lower()
        
        if component_gen and component_type:
            component_type_lower = component_type.lower()
            component_gen_lower = component_gen.lower()
            
            if segment and segment.lower() == 'server':
                if any(ct in component_type_lower for ct in ['temp sensor', 'rcd', 'mrcd', 'data buffer']):
                    valid_gen_options = ["gen1", "gen2", "gen3", "gen4", "gen5", "na"]
                    if not any(valid_gen.lower() in component_gen_lower for valid_gen in valid_gen_options):
                        return f"Invalid component generation for {component_type}. Must be one of: Gen1, Gen2, Gen3, Gen4, Gen5, or NA", None, None
        
        if segment and 'Segment' in df.columns:
            segment_mask = (df['Segment'].str.lower() == segment.lower()) | \
                           (df['Segment'].str.lower() == 'server/client')
            df = df[segment_mask]
            if df.empty:
                return f"No components found for segment: {segment}", None, None
        
        filters = []
        if supplier and 'Supplier' in df.columns:
            filters.append(df['Supplier'].str.lower() == supplier.lower())
        if component_gen and 'Component_Generation' in df.columns:
            filters.append(df['Component_Generation'].str.lower() == component_gen.lower())
        if revision and 'Revision' in df.columns:
            filters.append(df['Revision'].str.lower() == revision.lower())
        if component_type and 'Component_Type' in df.columns:
            if segment and segment.lower() == 'client':
                if 'ckd' in component_type.lower():
                    filters.append(df['Component_Type'].str.lower() == 'ckd')
                elif 'spd/hub' in component_type.lower():
                    filters.append(df['Component_Type'].str.lower() == 'spd/hub')
                elif 'pmic' in component_type.lower():
                    filters.append(df['Component_Type'].str.lower() == 'pmic')
            else:
                if 'rcd/mrcd' in component_type.lower():
                    filters.append(df['Component_Type'].str.lower().isin(['rcd', 'muxed rcd']))
                else:
                    filters.append(df['Component_Type'].str.lower() == component_type.lower())
        
        if filters:
            filtered_df = df.copy()
            for f in filters:
                filtered_df = filtered_df[f]
        else:
            filtered_df = df.copy()
        
        if filtered_df.empty:
            relaxed_filters = []
            if supplier and 'Supplier' in df.columns:
                relaxed_filters.append(df['Supplier'].str.contains(supplier.lower(), na=False))
            if component_gen and 'Component_Generation' in df.columns:
                relaxed_filters.append(df['Component_Generation'].str.contains(component_gen.lower(), na=False))
            if revision and 'Revision' in df.columns:
                relaxed_filters.append(df['Revision'].str.contains(revision.lower(), na=False))
            if component_type and 'Component_Type' in df.columns:
                if segment and segment.lower() == 'client':
                    if 'ckd' in component_type.lower():
                        relaxed_filters.append(df['Component_Type'].str.lower().str.contains('ckd|clock', na=False))
                    elif 'spd/hub' in component_type.lower():
                        relaxed_filters.append(df['Component_Type'].str.lower().str.contains('spd|hub|serial', na=False))
                    elif 'pmic' in component_type.lower():
                        relaxed_filters.append(df['Component_Type'].str.lower().str.contains('pmic|power', na=False))
                else:
                    if 'rcd/mrcd' in component_type.lower():
                        relaxed_filters.append(df['Component_Type'].str.lower().str.contains('rcd|muxed|register', na=False))
                    else:
                        relaxed_filters.append(df['Component_Type'].str.contains(component_type.lower(), na=False))
            
            if relaxed_filters:
                filtered_df = df.copy()
                for f in relaxed_filters:
                    filtered_df = filtered_df[f]
            
            if filtered_df.empty and component_type and 'Component_Type' in df.columns:
                component_type_lower = component_type.lower()
                
                if segment and segment.lower() == 'client':
                    type_variations = {
                        'pmic': ['pmic', 'power', 'power management'],
                        'spd/hub': ['spd', 'hub', 'spd/hub', 'serial presence detect'],
                        'ckd': ['ckd', 'clock driver', 'clock']
                    }
                else:
                    type_variations = {
                        'pmic': ['pmic', 'power', 'power management'],
                        'spd/hub': ['spd', 'hub', 'spd/hub', 'serial presence detect'],
                        'temp sensor': ['temp', 'sensor', 'temperature', 'temp sensor'],
                        'rcd/mrcd': ['rcd', 'mrcd', 'register', 'registering clock driver', 'muxed rcd'],
                        'data buffer': ['buffer', 'data buffer', 'db'],
                    }
                
                for key, variations in type_variations.items():
                    if any(var in component_type_lower for var in variations):
                        component_matches = df[df['Component_Type'].str.contains('|'.join(variations), na=False)]
                        if not component_matches.empty:
                            filtered_df = component_matches
                            break
                
                if filtered_df.empty:
                    return f"No matching process code found for {component_type} in {segment} segment", None, None
        
        if 'Process_Code' not in filtered_df.columns or filtered_df['Process_Code'].isna().all():
            return "Process code information not available in the data", None, None
        
        process_codes = filtered_df['Process_Code'].dropna().unique()
        
        if len(process_codes) == 0:
            return "No process code found for the given criteria", None, None
        
        process_code = process_codes[0]
        
        if process_code.lower() == "none" or not process_code:
            return "", filtered_df.iloc[0]['Component_Type'] if 'Component_Type' in filtered_df.columns else "Unknown", filtered_df
        
        if len(process_code) > 1:
            process_code = process_code[0]
        
        process_code = process_code.upper()
        component_type_result = filtered_df.iloc[0]['Component_Type'] if 'Component_Type' in filtered_df.columns else "Unknown"
        
        return process_code, component_type_result, filtered_df
    
    except Exception as e:
        return f"Error generating process code: {e}", None, None

def get_module_process_code(pmic, spd_hub, temp_sensor, rcd_mrcd, data_buffer, segment):
    try:
        if segment.lower() == 'server':
            if not pmic or not spd_hub or not temp_sensor or not rcd_mrcd:
                return "For server modules, PMIC, SPD/Hub, Temp Sensor, and RCD/MRCD are required"
            
            if data_buffer:
                return f"{pmic}{spd_hub}{temp_sensor}{rcd_mrcd}{data_buffer}"
            else:
                return f"{pmic}{spd_hub}{temp_sensor}{rcd_mrcd}"
        
        elif segment.lower() == 'client':
            if not pmic or not spd_hub:
                return "For client modules, PMIC and SPD/Hub are required"
            
            if temp_sensor:
                return f"{pmic}{spd_hub}{temp_sensor}"
            else:
                return f"{pmic}{spd_hub}"
        
        else:
            return "Invalid segment selected. Please select 'Client' or 'Server'"
    
    except Exception as e:
        return f"Error generating module process code: {e}"

def lookup_parts_by_process_code(process_code, component_validations_df, speed=None):
    try:
        if not process_code:
            return "No process code provided"
        
        if component_validations_df.empty:
            return "No component validation data available"
        
        component_codes = list(process_code)
        result_parts = []
        
        for i, code in enumerate(component_codes):
            filtered_df = component_validations_df[
                component_validations_df['Process_Code'].str.contains(code, na=False)
            ]
            
            if not filtered_df.empty:
                if speed and speed.lower() != "na" and 'Speed' in filtered_df.columns:
                    speed_filtered_df = filtered_df.copy()
                    
                    if speed.startswith("<"):
                        try:
                            speed_value = int(speed[1:])
                            
                            def is_less_than_speed(x):
                                if pd.isna(x) or not str(x).strip():
                                    return False
                                try:
                                    import re
                                    num_str = re.search(r'\d+', str(x))
                                    if num_str:
                                        return int(num_str.group()) < speed_value
                                    return False
                                except:
                                    return False
                            
                            speed_mask = speed_filtered_df['Speed'].apply(is_less_than_speed)
                            speed_filtered_df = speed_filtered_df[speed_mask]
                        except:
                            pass
                            
                    elif speed.startswith(">"):
                        try:
                            speed_value = int(speed[1:])
                            
                            def is_greater_than_speed(x):
                                if pd.isna(x) or not str(x).strip():
                                    return False
                                try:
                                    import re
                                    num_str = re.search(r'\d+', str(x))
                                    if num_str:
                                        return int(num_str.group()) > speed_value
                                    return False
                                except:
                                    return False
                            
                            speed_mask = speed_filtered_df['Speed'].apply(is_greater_than_speed)
                            speed_filtered_df = speed_filtered_df[speed_mask]
                        except:
                            pass
                            
                    else:
                        speed_filtered_df = speed_filtered_df[
                            speed_filtered_df['Speed'].astype(str).str.contains(speed, na=False)
                        ]
                    
                    if not speed_filtered_df.empty:
                        filtered_df = speed_filtered_df
                
                for _, row in filtered_df.iterrows():
                    segment_value = row.get('Segment', "Unknown")
                    if segment_value.lower() == 'server/client':
                        segment_display = "Server/Client"
                    else:
                        segment_display = segment_value
                        
                    result_parts.append({
                        'Position': i + 1,
                        'Process Code': code,
                        'Segment': segment_display,
                        'Supplier': row.get('Supplier', "Unknown"),
                        'Component Generation': row.get('Component_Generation', "Unknown"),
                        'Revision': row.get('Revision', "Unknown"),
                        'Component Type': row.get('Component_Type', "Unknown"),
                        'Speed': row.get('Speed', "Unknown"),
                        'SPN': row.get('SPN', "Unknown")
                    })
        
        if not result_parts:
            return "No matching parts found for the given process code"
        
        result_df = pd.DataFrame(result_parts)
        result_df = result_df.sort_values('Position')
        
        for col in ['Position', 'Process Code', 'Segment', 'Supplier', 'Component Generation', 'Revision', 'Component Type', 'Speed', 'SPN']:
            if col not in result_df.columns:
                result_df[col] = ""
        
        result_df = result_df[['Position', 'Process Code', 'Segment', 'Supplier', 'Component Generation', 'Revision', 'Component Type', 'Speed', 'SPN']]
        
        return result_df
    
    except Exception as e:
        return f"Error looking up parts: {e}"

def explain_process_code(process_code, segment):
    if not process_code or not isinstance(process_code, str):
        return "Invalid process code"
    
    explanation = []
    explanation.append(f"Process Code: {process_code}")
    explanation.append("Component Breakdown:")
    
    if segment.lower() == 'server':
        components = ["PMIC", "SPD/Hub", "Temp Sensor", "RCD/MRCD", "Data Buffer"]
        for i, char in enumerate(process_code):
            if i < len(components):
                explanation.append(f"Position {i+1}: {components[i]} - {char}")
        
        explanation.append("\nProcess Code Print Order (as shown on product label):")
        explanation.append("PMIC → RCD → SPD/Hub → Temp Sensor → Data Buffer (if applicable)")
    
    elif segment.lower() == 'client':
        components = ["PMIC", "SPD/Hub", "CKD"]
        for i, char in enumerate(process_code):
            if i < len(components):
                explanation.append(f"Position {i+1}: {components[i]} - {char}")
        
        explanation.append("\nProcess Code Print Order (as shown on product label):")
        explanation.append("PMIC → SPD/Hub → CKD (if applicable)")
    
    else:
        explanation.append("Unknown market segment. Cannot provide detailed breakdown.")
    
    return "\n".join(explanation)

def get_predefined_options(component_validations_df):
    default_options = {
        'segment': ["Client", "Server"],
        'supplier': ["ALPS", "ANPEC", "BOURNS", "DIODES", "LITTELFUSE", "MICRON", "MONTAGE", 
                    "MPS", "ONESEMI", "PANASONIC", "PULSE", "RAMBUS", "RENESAS", "RICHTEK", 
                    "SAMSUNG", "SEMCO", "SILERGY", "TAIYO YUDEN", "TI", "YAGEO"],
        'component_generation': ["5000", "5010", "5020", "5030", "5100", "5120", "5200", 
                                "Gen1", "Gen2", "Gen3", "Gen4", "Gen5", "NA"],
        'revision': ["01", "A0", "A1", "B0", "B1", "C0", "D0", "E0"],
        'component_type': ["CKD", "Data Buffer", "Inductor", "Muxed RCD", "PMIC", "RCD", "SPD/Hub", "Temp Sensor", "Voltage Regulator"]
    }
    
    if not component_validations_df.empty:
        try:
            for field, col_name in {
                'supplier': 'Supplier', 
                'component_generation': 'Component_Generation',
                'revision': 'Revision',
                'component_type': 'Component_Type'
            }.items():
                if col_name in component_validations_df.columns:
                    values = component_validations_df[col_name].dropna().unique().tolist()
                    if values:
                        cleaned_values = [v.strip() for v in values if v and v.strip()]
                        if cleaned_values:
                            default_options[field] = sorted(list(set(cleaned_values)))
        
        except Exception as e:
            st.sidebar.warning(f"Error extracting options from data: {e}")
    
    return default_options

def get_filtered_options(df, field, segment=None, supplier=None, component_type=None):
    if df.empty or field not in df.columns:
        return []
    
    filtered_df = df.copy()
    
    if segment and 'Segment' in filtered_df.columns:
        segment_mask = (filtered_df['Segment'].str.lower() == segment.lower()) | \
                       (filtered_df['Segment'].str.lower() == 'server/client')
        filtered_df = filtered_df[segment_mask]
    
    if supplier and 'Supplier' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['Supplier'].str.lower() == supplier.lower()]
    
    if field == 'Supplier' and component_type:
        component_type_lower = component_type.lower()
        
        if segment and segment.lower() == 'client':
            component_supplier_mapping = {
                'pmic': ['ANPEC', 'MICRON', 'MONTAGE', 'MPS', 'RAMBUS', 'RENESAS', 'RICHTEK', 'SAMSUNG', 'TI'],
                'spd/hub': ['MONTAGE', 'MPS', 'RAMBUS', 'RENESAS'],
                'ckd': ['MONTAGE', 'RAMBUS', 'RENESAS'],
                'inductor': ['ALPS', 'BOURNS', 'PULSE', 'SEMCO', 'TAIYO YUDEN', 'YAGEO'],
                'voltage regulator': ['DIODES', 'LITTELFUSE', 'PANASONIC', 'SILERGY']
            }
        else:
            component_supplier_mapping = {
                'pmic': ['ANPEC', 'MICRON', 'MONTAGE', 'MPS', 'RAMBUS', 'RENESAS', 'RICHTEK', 'SAMSUNG', 'TI'],
                'spd/hub': ['MONTAGE', 'MPS', 'RAMBUS', 'RENESAS'],
                'temp sensor': ['MONTAGE', 'MPS', 'RAMBUS', 'RENESAS', 'TI'],
                'rcd': ['MONTAGE', 'ONESEMI', 'RAMBUS', 'RENESAS'],
                'muxed rcd': ['MONTAGE', 'ONESEMI', 'RAMBUS', 'RENESAS'],
                'data buffer': ['MONTAGE', 'RAMBUS', 'RENESAS'],
                'inductor': ['ALPS', 'BOURNS', 'PULSE', 'SEMCO', 'TAIYO YUDEN', 'YAGEO'],
                'voltage regulator': ['DIODES', 'LITTELFUSE', 'PANASONIC', 'SILERGY']
            }
        
        for key, suppliers in component_supplier_mapping.items():
            if key in component_type_lower or (key == 'rcd' and 'rcd/mrcd' in component_type_lower):
                return suppliers
    
    if field == 'Component_Generation' and component_type:
        component_type_lower = component_type.lower()
        
        if segment and segment.lower() == 'client':
            if 'pmic' in component_type_lower:
                gen_options = [opt for opt in filtered_df[field].dropna().unique() 
                              if isinstance(opt, str) and any(c.isdigit() for c in opt)]
                if gen_options:
                    return sorted(gen_options)
            elif 'spd/hub' in component_type_lower or 'ckd' in component_type_lower:
                gen_options = [opt for opt in filtered_df[field].dropna().unique() 
                              if isinstance(opt, str) and (opt.lower().startswith('gen') or opt.lower() == 'na')]
                if gen_options:
                    return sorted(gen_options)
        else:
            if any(ct in component_type_lower for ct in ['temp sensor', 'rcd', 'mrcd', 'data buffer']):
                valid_gen_options = ["Gen1", "Gen2", "Gen3", "Gen4", "Gen5", "NA"]
                
                gen_options = [opt for opt in filtered_df[field].dropna().unique() 
                              if isinstance(opt, str) and (
                                  opt.lower() in [g.lower() for g in valid_gen_options] or
                                  any(g.lower() in opt.lower() for g in valid_gen_options)
                              )]
                
                if not gen_options:
                    return valid_gen_options
                
                return sorted(gen_options)
    
    if filtered_df.empty:
        return []
    
    options = filtered_df[field].dropna().unique().tolist()
    cleaned_options = list(set([option for option in options if option]))
    
    if field == 'Segment':
        valid_segments = ["Client", "Server"]
        cleaned_options = [opt for opt in cleaned_options if opt.lower() in [s.lower() for s in valid_segments]]
    
    return sorted(cleaned_options, key=lambda x: str(x).lower())

def search_mpn_in_sql(search_term, module_bom_59only_df, module_bom_simple_df):
    """Search for MPNs containing the search term in SQL data"""
    matching_mpns = []
    
    try:
        # Search in ModuleBOM_59only - look in Material Description column
        if not module_bom_59only_df.empty and 'Material Description' in module_bom_59only_df.columns:
            matches = module_bom_59only_df[
                module_bom_59only_df['Material Description'].astype(str).str.contains(search_term, case=False, na=False)
            ]['Material Description'].unique()
            matching_mpns.extend(matches)
        
        # Search in ModuleBOM_Simple - look in Material Description column
        if not module_bom_simple_df.empty and 'Material Description' in module_bom_simple_df.columns:
            matches = module_bom_simple_df[
                module_bom_simple_df['Material Description'].astype(str).str.contains(search_term, case=False, na=False)
            ]['Material Description'].unique()
            matching_mpns.extend(matches)
        
        # Also search in other potential MPN columns as fallback
        for df, df_name in [(module_bom_59only_df, '59only'), (module_bom_simple_df, 'Simple')]:
            if not df.empty:
                for col in df.columns:
                    if any(term in col.lower() for term in ['mpn', 'material number', 'part number']):
                        matches = df[
                            df[col].astype(str).str.contains(search_term, case=False, na=False)
                        ][col].unique()
                        matching_mpns.extend(matches)
        
        # Remove duplicates, empty values, and sort
        matching_mpns = sorted(list(set([mpn for mpn in matching_mpns if mpn and str(mpn).strip() and str(mpn).lower() != 'nan'])))
        
    except Exception as e:
        st.error(f"Error searching MPNs: {e}")
    
    return matching_mpns

def get_process_code_from_sql(mpn, module_bom_59only_df, module_bom_simple_df):
    """Look up process code for a given MPN in SQL data"""
    try:
        results = []
        
        # Search in ModuleBOM_59only - look in Material Description column
        if not module_bom_59only_df.empty and 'Material Description' in module_bom_59only_df.columns:
            matches = module_bom_59only_df[
                module_bom_59only_df['Material Description'].astype(str).str.contains(mpn, case=False, na=False)
            ]
            
            for _, row in matches.iterrows():
                result_row = {
                    'Source': 'ModuleBOM_59only',
                    'MPN': row.get('Material Description', ''),
                    'Material Number': row.get('Material Number', ''),
                    'Material Description': row.get('Material Description', ''),
                    'Process Code': 'Not Available in 59only Table',
                    'Supplier': row.get('Supplier', ''),
                    'Component Type': row.get('Component Type', '')
                }
                
                # Add other relevant columns
                for c in row.index:
                    if c not in result_row and not pd.isna(row[c]):
                        result_row[c] = row[c]
                
                results.append(result_row)
        
        # Search in ModuleBOM_Simple - look in Material Description column
        if not module_bom_simple_df.empty and 'Material Description' in module_bom_simple_df.columns:
            matches = module_bom_simple_df[
                module_bom_simple_df['Material Description'].astype(str).str.contains(mpn, case=False, na=False)
            ]
            
            for _, row in matches.iterrows():
                result_row = {
                    'Source': 'ModuleBOM_Simple',
                    'MPN': row.get('Material Description', ''),
                    'Material Number': row.get('Material Number', ''),
                    'Material Description': row.get('Material Description', ''),
                    'Process Code': row.get('Process Code', 'Not Available'),
                    'Supplier': row.get('Supplier', ''),
                    'Component Type': row.get('Component Type', '')
                }
                
                # Add other relevant columns
                for c in row.index:
                    if c not in result_row and not pd.isna(row[c]):
                        result_row[c] = row[c]
                
                results.append(result_row)
        
        # Also search in other potential MPN columns as fallback
        for df, source_name in [(module_bom_59only_df, 'ModuleBOM_59only'), (module_bom_simple_df, 'ModuleBOM_Simple')]:
            if not df.empty:
                for col in df.columns:
                    if any(term in col.lower() for term in ['mpn', 'material number', 'part number']) and col != 'Material Description':
                        matches = df[
                            df[col].astype(str).str.contains(mpn, case=False, na=False)
                        ]
                        
                        for _, row in matches.iterrows():
                            result_row = {
                                'Source': source_name,
                                'MPN': row.get(col, ''),
                                'Material Number': row.get('Material Number', ''),
                                'Material Description': row.get('Material Description', ''),
                                'Process Code': row.get('Process Code', 'Not Available') if source_name == 'ModuleBOM_Simple' else 'Not Available in 59only Table',
                                'Supplier': row.get('Supplier', ''),
                                'Component Type': row.get('Component Type', '')
                            }
                            
                            # Add other relevant columns
                            for c in row.index:
                                if c not in result_row and not pd.isna(row[c]):
                                    result_row[c] = row[c]
                            
                            results.append(result_row)
        
        if not results:
            return f"No records found for MPN: {mpn}", None
        
        # Remove duplicate results
        seen = set()
        unique_results = []
        for result in results:
            # Create a key based on source and MPN to identify duplicates
            key = (result['Source'], result['MPN'])
            if key not in seen:
                seen.add(key)
                unique_results.append(result)
        
        result_df = pd.DataFrame(unique_results)
        return "Success", result_df
        
    except Exception as e:
        return f"Error looking up MPN: {e}", None

def analyze_sql_data(module_bom_59only_df, module_bom_simple_df):
    """Analyze and display SQL data information"""
    st.subheader("SQL Data Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**ModuleBOM_59only Analysis:**")
        if not module_bom_59only_df.empty:
            st.write(f"- Total records: {len(module_bom_59only_df)}")
            st.write(f"- Columns: {len(module_bom_59only_df.columns)}")
            
            # Show column names
            with st.expander("Column Names", expanded=False):
                for col in sorted(module_bom_59only_df.columns):
                    st.write(f"- {col}")
            
            # Show sample data
            with st.expander("Sample Data", expanded=False):
                st.dataframe(module_bom_59only_df.head())
        else:
            st.write("No data available")
    
    with col2:
        st.write("**ModuleBOM_Simple Analysis:**")
        if not module_bom_simple_df.empty:
            st.write(f"- Total records: {len(module_bom_simple_df)}")
            st.write(f"- Columns: {len(module_bom_simple_df.columns)}")
            
            # Show column names
            with st.expander("Column Names", expanded=False):
                for col in sorted(module_bom_simple_df.columns):
                    st.write(f"- {col}")
            
            # Show sample data
            with st.expander("Sample Data", expanded=False):
                st.dataframe(module_bom_simple_df.head())
                
            # Check for process codes
            if 'Process Code' in module_bom_simple_df.columns:
                process_codes = module_bom_simple_df['Process Code'].dropna().unique()
                st.write(f"- Unique Process Codes: {len(process_codes)}")
                
                with st.expander("Process Codes Found", expanded=False):
                    for pc in sorted(process_codes):
                        if pc and str(pc).strip():
                            st.write(f"- {pc}")
        else:
            st.write("No data available")

def main():
    st.title("Process Code & Part Specification Generator")

    st.sidebar.info("📋 **Database Connection**: Using pymssql (no ODBC drivers required)")
    
    show_process_code_info()
    
    st.sidebar.header("Data Source")
    st.sidebar.info("Data is being loaded from SharePoint lists and SQL Server")

    test_sql_connection_detailed()
    
    # Use a unique key for the refresh button
    if st.sidebar.button("Refresh Data", key="refresh_data_main_unique"):
        # Clear cached data
        if hasattr(load_data_from_sharepoint, 'clear'):
            load_data_from_sharepoint.clear()
        if hasattr(load_data_from_sql_server_cached, 'clear'):
            load_data_from_sql_server_cached.clear()
        
        # Clear session state for connection parameters
        for key in ['sql_database', 'sql_username', 'sql_password']:
            if key in st.session_state:
                del st.session_state[key]
        
        st.rerun()
    
    data = load_data_from_sharepoint()
    component_validations_df = data['component_validations_df']
    module_validation_df = data['module_validation_df']

    sql_data = load_data_from_sql_server()
    module_bom_59only_df = sql_data['module_bom_59only_df']
    module_bom_simple_df = sql_data['module_bom_simple_df']

    predefined_options = get_predefined_options(component_validations_df)
    
    local_timezone = pytz.timezone('America/Denver')  
    local_time_obj = datetime.now(local_timezone)
    formatted_time = local_time_obj.strftime('%Y-%m-%d %H:%M:%S')
    tz_abbr = local_time_obj.strftime('%Z')
    st.sidebar.info(f"Data last refreshed: {formatted_time} {tz_abbr}")
    
    tab1, tab2, tab3 = st.tabs(["Module Process Code Lookup", "Module Process Code Generator", "MPN Lookup"])
    
    with tab1:
        st.write("Enter a process code to look up the associated parts:")
        
        lookup_segment = st.selectbox("Segment", options=predefined_options['segment'], key="lookup_segment")
        lookup_process_code = st.text_input("Process Code", key="lookup_process_code")
        
        speed_options = ["NA", "4800", "5600", "6400", "7200", "8000", "8800", "9600", "12800", "<8000", ">8000"]
        lookup_speed = st.selectbox("Speed (MT/s)", options=speed_options, key="lookup_speed", 
                                   help="Use '<8000' for speeds below 8000 MT/s, '>8000' for speeds above 8000 MT/s.")
        
        if st.button("Look Up Parts"):
            if not lookup_process_code:
                st.error("Please enter a process code")
            else:
                parts_lookup = lookup_parts_by_process_code(lookup_process_code, component_validations_df, lookup_speed)
                if isinstance(parts_lookup, str):
                    st.error(parts_lookup)
                else:
                    st.success(f"Found components for process code: {lookup_process_code}")
                            
                    explanation = explain_process_code(lookup_process_code, lookup_segment)
                    st.info(explanation)
                            
                    st.subheader("Component Details")
                            
                    for col in parts_lookup.columns:
                        parts_lookup[col] = parts_lookup[col].apply(lambda x: str(x).upper())
                            
                    st.dataframe(parts_lookup, height=400)
    
    with tab2:
        subtab2, subtab1 = st.tabs(["Module", "Component"])
        
        with subtab2:
            st.write("Enter the module component details to generate a combined module process code:")
            
            module_segment = st.selectbox("Segment", options=predefined_options['segment'], key="module_segment")
            
            if module_segment.lower() == 'client':
                components = {
                    "PMIC": {"required": True},
                    "SPD/Hub": {"required": True},
                    "CKD": {"required": False}
                }
            else:
                components = {
                    "PMIC": {"required": True},
                    "SPD/Hub": {"required": True},
                    "Temp Sensor": {"required": True},
                    "RCD/MRCD": {"required": True},
                    "Data Buffer": {"required": False}
                }
            
            component_codes = {}
            
            for component_name, config in components.items():
                st.subheader(component_name)
                component_key = component_name.replace("/", "_").replace(" ", "_").lower()
                
                component_codes[f"{component_key}_segment"] = module_segment
                
                supplier_options = get_filtered_options(component_validations_df, 'Supplier', 
                                                      segment=module_segment, 
                                                      component_type=component_name) or predefined_options['supplier']
                
                if not config["required"]:
                    supplier_options = supplier_options + ["None"]
                
                supplier = st.selectbox("Supplier", options=supplier_options, key=f"{component_key}_supplier")
                
                if supplier != "None":
                    if module_segment.lower() == 'server' and component_name.lower() in ['temp sensor', 'rcd/mrcd', 'data buffer']:
                        valid_gen_options = ["Gen1", "Gen2", "Gen3", "Gen4", "Gen5", "NA"]
                        
                        data_gen_options = get_filtered_options(component_validations_df, 'Component_Generation', 
                                                             segment=module_segment, 
                                                             supplier=supplier, 
                                                             component_type=component_name)
                        
                        if data_gen_options:
                            valid_data_options = [opt for opt in data_gen_options 
                                                if any(valid.lower() in opt.lower() for valid in valid_gen_options)]
                            if valid_data_options:
                                gen_options = valid_data_options
                            else:
                                gen_options = valid_gen_options
                        else:
                            gen_options = valid_gen_options
                    else:
                        gen_options = get_filtered_options(component_validations_df, 'Component_Generation', 
                                                         segment=module_segment, 
                                                         supplier=supplier, 
                                                         component_type=component_name) or predefined_options['component_generation']
                    
                    gen = st.selectbox("Component Generation", options=gen_options, key=f"{component_key}_gen")
                    
                    rev_options = get_filtered_options(component_validations_df, 'Revision', 
                                                    segment=module_segment, 
                                                    supplier=supplier, 
                                                    component_type=component_name) or predefined_options['revision']
                    rev = st.selectbox("Revision", options=rev_options, key=f"{component_key}_rev")
                    
                    if module_segment.lower() == 'server' and component_name.lower() in ['temp sensor', 'rcd/mrcd', 'data buffer']:
                        valid_gen_options_lower = [opt.lower() for opt in ["Gen1", "Gen2", "Gen3", "Gen4", "Gen5", "NA"]]
                        if gen.lower() not in valid_gen_options_lower:
                            st.error(f"Invalid component generation for {component_name}. Must be one of: Gen1, Gen2, Gen3, Gen4, Gen5, or NA")
                            continue
                    
                    process_code_result, component_type_result, filtered_data = get_component_process_code(
                        module_segment, supplier, gen, rev, component_name, component_validations_df
                    )
                    
                    if process_code_result and not process_code_result.startswith("Error") and not process_code_result.startswith("No") and not process_code_result.startswith("Invalid"):
                        component_codes[component_key] = process_code_result
                        st.success(f"Process Code: {process_code_result}")
                        
                        if isinstance(filtered_data, pd.DataFrame) and not filtered_data.empty:
                            with st.expander(f"View {component_name} Details", expanded=False):
                                display_df = filtered_data.copy()
                                for col in display_df.columns:
                                    if display_df[col].dtype == 'object':
                                        display_df[col] = display_df[col].str.upper()
                                st.dataframe(display_df)
                    else:
                        st.error(f"{component_name}: {process_code_result}")
                        component_codes[component_key] = ""
                else:
                    component_codes[component_key] = ""
            
            if st.button("Generate Module Process Code", key="generate_module_code"):
                pmic_code = component_codes.get('pmic', '')
                spd_hub_code = component_codes.get('spd_hub', '')
                temp_sensor_code = component_codes.get('temp_sensor', '')
                rcd_mrcd_code = component_codes.get('rcd_mrcd', '')
                data_buffer_code = component_codes.get('data_buffer', '')
                ckd_code = component_codes.get('ckd', '')
                
                if module_segment.lower() == 'client':
                    module_process_code = get_module_process_code(pmic_code, spd_hub_code, ckd_code, "", "", module_segment)
                else:
                    module_process_code = get_module_process_code(pmic_code, spd_hub_code, temp_sensor_code, rcd_mrcd_code, data_buffer_code, module_segment)
                
                if module_process_code and not module_process_code.startswith("Error") and not module_process_code.startswith("For") and not module_process_code.startswith("Invalid"):
                    st.success(f"**Module Process Code: {module_process_code}**")
                    
                    explanation = explain_process_code(module_process_code, module_segment)
                    st.info(explanation)
                    
                    parts_lookup = lookup_parts_by_process_code(module_process_code, component_validations_df)
                    if isinstance(parts_lookup, pd.DataFrame):
                        st.subheader("Component Details for Generated Process Code")
                        
                        for col in parts_lookup.columns:
                            parts_lookup[col] = parts_lookup[col].apply(lambda x: str(x).upper())
                        
                        st.dataframe(parts_lookup, height=400)
                else:
                    st.error(module_process_code)
        
        with subtab1:
            st.write("Enter component details to get the process code:")
            
            segment = st.selectbox("Segment", options=predefined_options['segment'], key="component_segment")
            
            component_type_options = predefined_options['component_type']
            if segment.lower() == 'client':
                component_type_options = [ct for ct in component_type_options if ct.lower() in ['pmic', 'spd/hub', 'ckd', 'inductor', 'voltage regulator']]
            
            component_type = st.selectbox("Component Type", options=component_type_options, key="component_type")
            
            supplier_options = get_filtered_options(component_validations_df, 'Supplier', 
                                                  segment=segment, 
                                                  component_type=component_type) or predefined_options['supplier']
            supplier = st.selectbox("Supplier", options=supplier_options, key="component_supplier")
            
            if segment.lower() == 'server' and component_type.lower() in ['temp sensor', 'rcd', 'muxed rcd', 'data buffer']:
                valid_gen_options = ["Gen1", "Gen2", "Gen3", "Gen4", "Gen5", "NA"]
                
                data_gen_options = get_filtered_options(component_validations_df, 'Component_Generation', 
                                                       segment=segment, 
                                                       supplier=supplier, 
                                                       component_type=component_type)
                
                if data_gen_options:
                    valid_data_options = [opt for opt in data_gen_options 
                                        if any(valid.lower() in opt.lower() for valid in valid_gen_options)]
                    if valid_data_options:
                        gen_options = valid_data_options
                    else:
                        gen_options = valid_gen_options
                else:
                    gen_options = valid_gen_options
            else:
                gen_options = get_filtered_options(component_validations_df, 'Component_Generation', 
                                                 segment=segment, 
                                                 supplier=supplier, 
                                                 component_type=component_type) or predefined_options['component_generation']
            
            component_gen = st.selectbox("Component Generation", options=gen_options, key="component_gen")
            
            rev_options = get_filtered_options(component_validations_df, 'Revision', 
                                             segment=segment, 
                                             supplier=supplier, 
                                             component_type=component_type) or predefined_options['revision']
            revision = st.selectbox("Revision", options=rev_options, key="component_revision")
            
            if st.button("Get Process Code", key="get_component_code"):
                if segment.lower() == 'server' and component_type.lower() in ['temp sensor', 'rcd', 'muxed rcd', 'data buffer']:
                    valid_gen_options_lower = [opt.lower() for opt in ["Gen1", "Gen2", "Gen3", "Gen4", "Gen5", "NA"]]
                    if component_gen.lower() not in valid_gen_options_lower:
                        st.error(f"Invalid component generation for {component_type}. Must be one of: Gen1, Gen2, Gen3, Gen4, Gen5, or NA")
                    else:
                        process_code_result, component_type_result, filtered_data = get_component_process_code(
                            segment, supplier, component_gen, revision, component_type, component_validations_df
                        )
                        
                        if process_code_result and not process_code_result.startswith("Error") and not process_code_result.startswith("No") and not process_code_result.startswith("Invalid"):
                            st.success(f"**Process Code: {process_code_result}**")
                            st.info(f"Component Type: {component_type_result}")
                            
                            if isinstance(filtered_data, pd.DataFrame) and not filtered_data.empty:
                                st.subheader("Matching Components")
                                
                                display_df = filtered_data.copy()
                                for col in display_df.columns:
                                    if display_df[col].dtype == 'object':
                                        display_df[col] = display_df[col].str.upper()
                                
                                st.dataframe(display_df, height=300)
                        else:
                            st.error(process_code_result)
                else:
                    process_code_result, component_type_result, filtered_data = get_component_process_code(
                        segment, supplier, component_gen, revision, component_type, component_validations_df
                    )
                    
                    if process_code_result and not process_code_result.startswith("Error") and not process_code_result.startswith("No") and not process_code_result.startswith("Invalid"):
                        st.success(f"**Process Code: {process_code_result}**")
                        st.info(f"Component Type: {component_type_result}")
                        
                        if isinstance(filtered_data, pd.DataFrame) and not filtered_data.empty:
                            st.subheader("Matching Components")
                            
                            display_df = filtered_data.copy()
                            for col in display_df.columns:
                                if display_df[col].dtype == 'object':
                                    display_df[col] = display_df[col].str.upper()
                            
                            st.dataframe(display_df, height=300)
                    else:
                        st.error(process_code_result)
    
    with tab3:
        st.write("Search for MPNs and look up their process codes from SQL data:")
        
        # Get connection parameters for optimized search
        server, database, username, password = get_sql_connection_params()
        
        search_term = st.text_input("Search MPN (partial match)", key="mpn_search")
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            if st.button("Search MPNs", key="search_mpns"):
                if not search_term:
                    st.error("Please enter a search term")
                elif not all([server, database, username, password]):
                    st.error("Please provide SQL Server connection details in the sidebar")
                else:
                    with st.spinner("Searching MPNs..."):
                        # Use optimized search if connection is available
                        if all([server, database, username, password]):
                            matching_mpns = search_mpn_optimized(search_term, server, database, username, password)
                        else:
                            matching_mpns = search_mpn_in_sql(search_term, module_bom_59only_df, module_bom_simple_df)
                        
                        if matching_mpns:
                            st.success(f"Found {len(matching_mpns)} matching MPNs")
                            
                            # Display in a more compact format
                            for i, mpn in enumerate(matching_mpns[:20]):  # Show first 20 results
                                st.write(f"{i+1}. {mpn}")
                            
                            if len(matching_mpns) > 20:
                                st.info(f"Showing first 20 results. Total found: {len(matching_mpns)}")
                            
                            # Store results in session state for lookup
                            st.session_state['search_results'] = matching_mpns
                        else:
                            st.warning("No matching MPNs found")
        
        with col2:
            # MPN selection for lookup
            if 'search_results' in st.session_state and st.session_state['search_results']:
                selected_mpn = st.selectbox(
                    "Select MPN for Process Code Lookup", 
                    options=st.session_state['search_results'],
                    key="selected_mpn"
                )
                
                if st.button("Get Process Code", key="get_mpn_process_code"):
                    if selected_mpn:
                        with st.spinner("Looking up process code..."):
                            # Use optimized lookup if connection is available
                            if all([server, database, username, password]):
                                status, result_df = get_process_code_optimized(selected_mpn, server, database, username, password)
                            else:
                                status, result_df = get_process_code_from_sql(selected_mpn, module_bom_59only_df, module_bom_simple_df)
                            
                            if status == "Success" and result_df is not None:
                                st.success(f"Process code information for: {selected_mpn}")
                                st.dataframe(result_df, height=400)
                                
                                # Show process codes found
                                if 'Process Code' in result_df.columns:
                                    process_codes = result_df['Process Code'].dropna().unique()
                                    valid_process_codes = [pc for pc in process_codes if pc and str(pc).strip() and str(pc) != 'Not Available']
                                    
                                    if valid_process_codes:
                                        st.subheader("Process Codes Found:")
                                        for pc in valid_process_codes:
                                            st.write(f"**{pc}**")
                                            
                                            # Try to explain the process code
                                            explanation = explain_process_code(str(pc), "Server")  # Default to server for explanation
                                            st.info(explanation)
                                    else:
                                        st.warning("No valid process codes found for this MPN")
                            else:
                                st.error(status)
            else:
                st.info("Search for MPNs first to enable process code lookup")
        
        # Direct MPN lookup
        st.subheader("Direct MPN Lookup")
        direct_mpn = st.text_input("Enter exact MPN", key="direct_mpn")
        
        if st.button("Look Up Direct MPN", key="lookup_direct_mpn"):
            if not direct_mpn:
                st.error("Please enter an MPN")
            elif not all([server, database, username, password]):
                st.error("Please provide SQL Server connection details in the sidebar")
            else:
                with st.spinner("Looking up MPN..."):
                    # Use optimized lookup if connection is available
                    if all([server, database, username, password]):
                        status, result_df = get_process_code_optimized(direct_mpn, server, database, username, password)
                    else:
                        status, result_df = get_process_code_from_sql(direct_mpn, module_bom_59only_df, module_bom_simple_df)
                    
                    if status == "Success" and result_df is not None:
                        st.success(f"Information found for: {direct_mpn}")
                        st.dataframe(result_df, height=400)
                        
                        # Show process codes found
                        if 'Process Code' in result_df.columns:
                            process_codes = result_df['Process Code'].dropna().unique()
                            valid_process_codes = [pc for pc in process_codes if pc and str(pc).strip() and str(pc) != 'Not Available']
                            
                            if valid_process_codes:
                                st.subheader("Process Codes Found:")
                                for pc in valid_process_codes:
                                    st.write(f"**{pc}**")
                                    
                                    # Try to explain the process code
                                    explanation = explain_process_code(str(pc), "Server")  # Default to server for explanation
                                    st.info(explanation)
                            else:
                                st.warning("No valid process codes found for this MPN")
                    else:
                        st.error(status)
        
        # SQL Data Analysis section
        st.subheader("SQL Data Analysis")
        if st.button("Analyze SQL Data", key="analyze_sql_data"):
            if all([server, database, username, password]):
                analyze_sql_data_optimized(server, database, username, password)
            else:
                analyze_sql_data(module_bom_59only_df, module_bom_simple_df)

if __name__ == "__main__":
    main()