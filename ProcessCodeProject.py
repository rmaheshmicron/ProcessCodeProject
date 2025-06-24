import streamlit as st
import pandas as pd
import pytz
import urllib.parse
import mysql.connector
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

def get_mysql_connection_params():
    """Get MySQL connection parameters from user input or secrets"""
    # Try to get from secrets first
    if all(key in st.secrets for key in ["mysql_host", "mysql_database", "mysql_username", "mysql_password"]):
        return (
            st.secrets["mysql_host"],
            st.secrets["mysql_database"], 
            st.secrets["mysql_username"],
            st.secrets["mysql_password"],
            st.secrets.get("mysql_port", 3306)
        )
    
    # Otherwise get from sidebar
    st.sidebar.subheader("MySQL Connection")
    
    host = st.sidebar.text_input(
        "Host", 
        value=st.session_state.get('mysql_host', 'localhost'),
        key="mysql_host",
        help="MySQL server hostname or IP address"
    )
    
    port = st.sidebar.number_input(
        "Port", 
        value=st.session_state.get('mysql_port', 3306),
        key="mysql_port",
        min_value=1,
        max_value=65535
    )
    
    database = st.sidebar.text_input(
        "Database", 
        value=st.session_state.get('mysql_database', 'PQRA'),
        key="mysql_database"
    )
    
    username = st.sidebar.text_input(
        "Username", 
        value=st.session_state.get('mysql_username', ''),
        key="mysql_username",
        help="MySQL username"
    )
    
    password = st.sidebar.text_input(
        "Password", 
        type="password",
        key="mysql_password",
        help="MySQL password"
    )
    
    return host, database, username, password, port

def create_mysql_connection(host, database, username, password, port):
    """Create MySQL connection using mysql.connector"""
    if not all([host, database, username, password]):
        missing = []
        if not host: missing.append("Host")
        if not database: missing.append("Database")
        if not username: missing.append("Username")
        if not password: missing.append("Password")
        
        st.sidebar.error(f"Missing required fields: {', '.join(missing)}")
        return None
    
    try:
        st.sidebar.info("Connecting with mysql.connector...")
        
        # Create connection using mysql.connector
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database,
            connection_timeout=60,
            autocommit=True
        )
        
        # Test the connection
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        
        if result and result['test'] == 1:
            st.sidebar.success("✅ Connected successfully using mysql.connector")
            
            # Test access to ModuleBOM_Simple table
            try:
                cursor.execute("SELECT * FROM dbo.ModuleBOM_Simple LIMIT 1")
                cursor.fetchone()
                st.sidebar.success("✅ Access confirmed to dbo.ModuleBOM_Simple")
            except Exception as table_error:
                st.sidebar.warning(f"⚠️ dbo.ModuleBOM_Simple: {str(table_error)}")
            
            cursor.close()
            return conn
        
    except mysql.connector.Error as e:
        error_msg = str(e)
        st.sidebar.error(f"❌ Connection failed: {error_msg}")
        
        # Provide specific error guidance
        if "access denied" in error_msg.lower():
            st.sidebar.error("🔐 **Authentication Issue**: Username or password incorrect")
        elif "timeout" in error_msg.lower():
            st.sidebar.error("⏱️ **Timeout Issue**: Connection is timing out")
            
            with st.sidebar.expander("🔧 Timeout Troubleshooting", expanded=True):
                st.write("""
                **Possible Solutions:**
                
                1. **Network Issues:**
                   - Ensure you're connected to the network
                   - Check if firewall is blocking the MySQL port
                   - Try from a different network location
                
                2. **Server Load:**
                   - Server may be under heavy load
                   - Try again in a few minutes
                   - Contact IT if persistent
                
                3. **Connection Settings:**
                   - Verify host name is correct
                   - Check if MySQL Server is running
                   - Ensure port number is correct
                """)
                
        elif "can't connect" in error_msg.lower() or "network" in error_msg.lower():
            st.sidebar.error("🌐 **Network Issue**: Cannot reach server")
            
            with st.sidebar.expander("🔧 Network Troubleshooting", expanded=True):
                st.write("""
                **Check These Items:**
                
                1. **Network Connection**: Ensure you're connected to the network
                2. **Host Name**: Verify host name is correct
                3. **Network Access**: Server may only accept connections from specific networks
                4. **DNS Resolution**: Host name may not be resolving correctly
                5. **Firewall**: MySQL port may be blocked
                """)
        
        return None
    except Exception as e:
        st.sidebar.error(f"❌ Unexpected error: {str(e)}")
        return None

def test_mysql_connection_detailed():
    """Enhanced connection test function"""
    st.sidebar.subheader("Test MySQL Connection")
    
    if st.sidebar.button("Test Connection", key="test_mysql_conn_detailed"):
        host, database, username, password, port = get_mysql_connection_params()
        
        if not all([host, database, username, password]):
            st.sidebar.error("Please provide all connection details")
            return
        
        with st.sidebar:
            with st.spinner("Testing connection to MySQL Server..."):
                st.info(f"Connecting to: {host}:{port}")
                st.info(f"Database: {database}")
                st.info(f"User: {username}")
                
                conn = create_mysql_connection(host, database, username, password, port)
                
                if conn:
                    try:
                        cursor = conn.cursor(dictionary=True)
                        
                        # Get server info
                        try:
                            cursor.execute("SELECT VERSION() as Version, @@hostname as ServerName")
                            server_info = cursor.fetchone()
                            st.info(f"Server: {server_info['ServerName']}")
                            st.info(f"Version: {server_info['Version'][:50]}...")
                        except:
                            pass
                        
                        # Get database info
                        try:
                            cursor.execute("SELECT DATABASE() as CurrentDB")
                            db_info = cursor.fetchone()
                            st.success(f"Connected to database: {db_info['CurrentDB']}")
                        except:
                            pass
                        
                        # Test table access
                        try:
                            cursor.execute("SELECT COUNT(*) as RecordCount FROM dbo.ModuleBOM_Simple")
                            count_result = cursor.fetchone()
                            count = count_result['RecordCount']
                            st.success(f"dbo.ModuleBOM_Simple: {count:,} records")
                        except Exception as table_error:
                            st.error(f"dbo.ModuleBOM_Simple: {str(table_error)}")
                        
                        cursor.close()
                        
                    finally:
                        conn.close()

@st.cache_data(ttl=3600)
def load_data_from_mysql_cached(host, database, username, password, port):
    """Load data using mysql.connector with better error handling"""
    data = {
        'module_bom_simple_df': pd.DataFrame()
    }
    
    conn = create_mysql_connection(host, database, username, password, port)
    
    if conn is None:
        return data
    
    try:
        cursor = conn.cursor(dictionary=True)
        
        # Query ModuleBOM_Simple table
        try:
            st.sidebar.info("Loading ModuleBOM_Simple...")
            cursor.execute("SELECT * FROM dbo.ModuleBOM_Simple")
            
            # Fetch all rows
            rows = cursor.fetchall()
            if rows:
                # Convert to DataFrame
                module_bom_simple_df = pd.DataFrame(rows)
                data['module_bom_simple_df'] = module_bom_simple_df
                st.sidebar.success(f"ModuleBOM_Simple: {len(module_bom_simple_df):,} records loaded")
            else:
                st.sidebar.warning("ModuleBOM_Simple: No records found")
                
        except Exception as e:
            st.sidebar.error(f"ModuleBOM_Simple: {str(e)}")
        
        cursor.close()
        
    except Exception as e:
        st.sidebar.error(f"Database connection error: {e}")
        
        # Provide specific guidance for connection issues
        if "access denied" in str(e).lower():
            st.sidebar.error("🔐 Authentication failed - Check username/password")
        elif "timeout" in str(e).lower():
            st.sidebar.error("⏱️ Connection timeout - Server may be slow or network issues")
        elif "network" in str(e).lower():
            st.sidebar.error("🌐 Network error - Check network connection or firewall")
    
    finally:
        if conn:
            conn.close()
    
    return data

def load_data_from_mysql():
    """Wrapper function that gets connection params and calls cached version"""
    host, database, username, password, port = get_mysql_connection_params()
    return load_data_from_mysql_cached(host, database, username, password, port)

def search_mpn_optimized(search_term, host, database, username, password, port):
    """Search for MPNs directly in the database for better performance"""
    conn = create_mysql_connection(host, database, username, password, port)
    
    if not conn or not search_term:
        return []
    
    try:
        cursor = conn.cursor(dictionary=True)
        
        # Search in ModuleBOM_Simple table - Material_Description column
        query = """
            SELECT DISTINCT Material_Description as MPN
            FROM dbo.ModuleBOM_Simple
            WHERE Material_Description LIKE %s
            AND Material_Description IS NOT NULL
            AND Material_Description != ''
        """
        
        search_pattern = f"%{search_term}%"
        matching_mpns = []
        
        # Execute query
        cursor.execute(query, (search_pattern,))
        results = cursor.fetchall()
        matching_mpns.extend([row['MPN'] for row in results if row['MPN']])
        
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

def get_process_code_optimized(mpn, host, database, username, password, port):
    """Look up process code directly from database with optimized query"""
    conn = create_mysql_connection(host, database, username, password, port)
    
    if not conn or not mpn:
        return "No MPN provided", None
    
    try:
        cursor = conn.cursor(dictionary=True)
        
        # Search in ModuleBOM_Simple table
        query = """
            SELECT 
                Material_Description as MPN,
                Process_Code,
                *
            FROM dbo.ModuleBOM_Simple
            WHERE Material_Description LIKE %s
            AND Material_Description IS NOT NULL
        """
        
        search_pattern = f"%{mpn}%"
        
        # Execute query
        cursor.execute(query, (search_pattern,))
        results = cursor.fetchall()
        
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

def analyze_mysql_data_optimized(host, database, username, password, port):
    """Analyze MySQL data using direct database queries for better performance"""
    st.subheader("MySQL Data Analysis")
    
    conn = create_mysql_connection(host, database, username, password, port)
    
    if conn is None:
        st.error("No database connection available")
        return
    
    try:
        cursor = conn.cursor(dictionary=True)
        
        st.write("**ModuleBOM_Simple Analysis:**")
        try:
            # Get table info
            cursor.execute("SELECT COUNT(*) as record_count FROM dbo.ModuleBOM_Simple")
            count_result = cursor.fetchone()
            record_count = count_result['record_count'] if count_result else 0
            
            st.write(f"- Total records: {record_count}")
            
            if record_count > 0:
                # Get column info
                cursor.execute("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'ModuleBOM_Simple' AND TABLE_SCHEMA = %s
                    ORDER BY ORDINAL_POSITION
                """, (database,))
                columns_result = cursor.fetchall()
                column_names = [row['COLUMN_NAME'] for row in columns_result]
                
                st.write(f"- Columns: {len(column_names)}")
                
                # Show column names
                with st.expander("Column Names", expanded=False):
                    for col in column_names:
                        st.write(f"- {col}")
                
                # Show sample data
                with st.expander("Sample Data", expanded=False):
                    cursor.execute("SELECT * FROM dbo.ModuleBOM_Simple LIMIT 5")
                    sample_data = cursor.fetchall()
                    if sample_data:
                        sample_df = pd.DataFrame(sample_data)
                        st.dataframe(sample_df)
                
                # Check for process codes
                try:
                    cursor.execute("""
                        SELECT DISTINCT Process_Code 
                        FROM dbo.ModuleBOM_Simple 
                        WHERE Process_Code IS NOT NULL 
                        AND Process_Code != ''
                        ORDER BY Process_Code
                    """)
                    pc_result = cursor.fetchall()
                    process_codes = [row['Process_Code'] for row in pc_result if row['Process_Code']]
                    
                    st.write(f"- Unique Process Codes: {len(process_codes)}")
                    
                    with st.expander("Process Codes Found", expanded=False):
                        for pc in process_codes:
                            if pc and str(pc).strip():
                                st.write(f"- {pc}")
                except Exception as e:
                    st.warning(f"Could not analyze process codes: {e}")
                
                # Check for MPNs
                try:
                    cursor.execute("""
                        SELECT COUNT(DISTINCT Material_Description) as unique_mpns
                        FROM dbo.ModuleBOM_Simple 
                        WHERE Material_Description IS NOT NULL 
                        AND Material_Description != ''
                    """)
                    mpn_result = cursor.fetchone()
                    unique_mpns = mpn_result['unique_mpns'] if mpn_result else 0
                    
                    st.write(f"- Unique MPNs: {unique_mpns}")
                except Exception as e:
                    st.warning(f"Could not analyze MPNs: {e}")
            
        except Exception as e:
            st.error(f"Error analyzing ModuleBOM_Simple: {e}")
        
        cursor.close()
        
    except Exception as e:
        st.error(f"Database connection error during analysis: {e}")
    finally:
        if conn:
            conn.close()

def search_mpn_in_mysql(search_term, module_bom_simple_df):
    """Search for MPNs containing the search term in ModuleBOM_Simple data"""
    matching_mpns = []
    
    try:
        # Search in ModuleBOM_Simple - look in Material_Description column
        if not module_bom_simple_df.empty and 'Material_Description' in module_bom_simple_df.columns:
            matches = module_bom_simple_df[
                module_bom_simple_df['Material_Description'].astype(str).str.contains(search_term, case=False, na=False)
            ]['Material_Description'].unique()
            matching_mpns.extend(matches)
        
        # Remove duplicates, empty values, and sort
        matching_mpns = sorted(list(set([mpn for mpn in matching_mpns if mpn and str(mpn).strip() and str(mpn).lower() != 'nan'])))
        
    except Exception as e:
        st.error(f"Error searching MPNs: {e}")
    
    return matching_mpns

def get_process_code_from_mysql(mpn, module_bom_simple_df):
    """Look up process code for a given MPN in ModuleBOM_Simple data"""
    try:
        results = []
        
        # Search in ModuleBOM_Simple - look in Material_Description column
        if not module_bom_simple_df.empty and 'Material_Description' in module_bom_simple_df.columns:
            matches = module_bom_simple_df[
                module_bom_simple_df['Material_Description'].astype(str).str.contains(mpn, case=False, na=False)
            ]
            
            for _, row in matches.iterrows():
                result_row = {
                    'Source': 'ModuleBOM_Simple',
                    'MPN': row.get('Material_Description', ''),
                    'Process_Code': row.get('Process_Code', 'Not Available')
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
            # Create a key based on MPN to identify duplicates
            key = result['MPN']
            if key not in seen:
                seen.add(key)
                unique_results.append(result)
        
        result_df = pd.DataFrame(unique_results)
        return "Success", result_df
        
    except Exception as e:
        return f"Error looking up MPN: {e}", None

def analyze_mysql_data(module_bom_simple_df):
    """Analyze and display ModuleBOM_Simple data information"""
    st.subheader("ModuleBOM_Simple Data Analysis")
    
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
        if 'Process_Code' in module_bom_simple_df.columns:
            process_codes = module_bom_simple_df['Process_Code'].dropna().unique()
            st.write(f"- Unique Process Codes: {len(process_codes)}")
            
            with st.expander("Process Codes Found", expanded=False):
                for pc in sorted(process_codes):
                    if pc and str(pc).strip():
                        st.write(f"- {pc}")
        
        # Check for MPNs
        if 'Material_Description' in module_bom_simple_df.columns:
            unique_mpns = module_bom_simple_df['Material_Description'].dropna().nunique()
            st.write(f"- Unique MPNs: {unique_mpns}")
    else:
        st.write("No data available")

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

    st.sidebar.info("📋 **Database Connection**: Using mysql.connector (no ODBC drivers required)")
    
    show_process_code_info()
    
    st.sidebar.header("Data Source")
    st.sidebar.info("Data is being loaded from SharePoint lists and MySQL Server")

    test_mysql_connection_detailed()
    
    # Use a unique key for the refresh button
    if st.sidebar.button("Refresh Data", key="refresh_data_main_unique"):
        # Clear cached data
        if hasattr(load_data_from_sharepoint, 'clear'):
            load_data_from_sharepoint.clear()
        if hasattr(load_data_from_mysql_cached, 'clear'):
            load_data_from_mysql_cached.clear()
        
        # Clear session state for connection parameters
        for key in ['mysql_host', 'mysql_database', 'mysql_username', 'mysql_password']:
            if key in st.session_state:
                del st.session_state[key]
        
        st.rerun()
    
    data = load_data_from_sharepoint()
    component_validations_df = data['component_validations_df']
    module_validation_df = data['module_validation_df']

    mysql_data = load_data_from_mysql()
    module_bom_simple_df = mysql_data['module_bom_simple_df']

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
                    
                    component_gen = st.selectbox("Component Generation", options=gen_options, key=f"{component_key}_gen")
                    
                    revision_options = get_filtered_options(component_validations_df, 'Revision', 
                                                          segment=module_segment, 
                                                          supplier=supplier, 
                                                          component_type=component_name) or predefined_options['revision']
                    
                    revision = st.selectbox("Revision", options=revision_options, key=f"{component_key}_revision")
                    
                    component_codes[f"{component_key}_supplier"] = supplier
                    component_codes[f"{component_key}_gen"] = component_gen
                    component_codes[f"{component_key}_revision"] = revision
                    
                    process_code_result, component_type_result, filtered_df = get_component_process_code(
                        module_segment, supplier, component_gen, revision, component_name, component_validations_df
                    )
                    
                    if process_code_result and not process_code_result.startswith("Error") and not process_code_result.startswith("No"):
                        st.success(f"Process Code: {process_code_result}")
                        component_codes[f"{component_key}_code"] = process_code_result
                        
                        if filtered_df is not None and not filtered_df.empty:
                            with st.expander(f"View {component_name} Details", expanded=False):
                                st.dataframe(filtered_df)
                    else:
                        st.error(process_code_result)
                        component_codes[f"{component_key}_code"] = ""
                else:
                    component_codes[f"{component_key}_code"] = ""
            
            if st.button("Generate Module Process Code"):
                pmic_code = component_codes.get("pmic_code", "")
                spd_hub_code = component_codes.get("spd_hub_code", "")
                temp_sensor_code = component_codes.get("temp_sensor_code", "")
                rcd_mrcd_code = component_codes.get("rcd_mrcd_code", "")
                data_buffer_code = component_codes.get("data_buffer_code", "")
                ckd_code = component_codes.get("ckd_code", "")
                
                if module_segment.lower() == 'client':
                    module_process_code = get_module_process_code(pmic_code, spd_hub_code, ckd_code, "", "", module_segment)
                else:
                    module_process_code = get_module_process_code(pmic_code, spd_hub_code, temp_sensor_code, rcd_mrcd_code, data_buffer_code, module_segment)
                
                if module_process_code and not module_process_code.startswith("Error") and not module_process_code.startswith("For"):
                    st.success(f"**Module Process Code: {module_process_code}**")
                    
                    explanation = explain_process_code(module_process_code, module_segment)
                    st.info(explanation)
                    
                    st.subheader("Component Summary")
                    summary_data = []
                    
                    if module_segment.lower() == 'client':
                        component_list = ["PMIC", "SPD/Hub", "CKD"]
                    else:
                        component_list = ["PMIC", "SPD/Hub", "Temp Sensor", "RCD/MRCD", "Data Buffer"]
                    
                    for i, component_name in enumerate(component_list):
                        if i < len(module_process_code):
                            component_key = component_name.replace("/", "_").replace(" ", "_").lower()
                            summary_data.append({
                                "Position": i + 1,
                                "Component": component_name,
                                "Process Code": module_process_code[i],
                                "Supplier": component_codes.get(f"{component_key}_supplier", ""),
                                "Generation": component_codes.get(f"{component_key}_gen", ""),
                                "Revision": component_codes.get(f"{component_key}_revision", "")
                            })
                    
                    summary_df = pd.DataFrame(summary_data)
                    st.dataframe(summary_df, height=300)
                else:
                    st.error(module_process_code)
        
        with subtab1:
            st.write("Enter component details to generate an individual process code:")
            
            segment = st.selectbox("Segment", options=predefined_options['segment'], key="component_segment")
            
            component_type_options = predefined_options['component_type']
            if segment.lower() == 'client':
                component_type_options = [ct for ct in component_type_options if ct in ['PMIC', 'SPD/Hub', 'CKD', 'Inductor', 'Voltage Regulator']]
            
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
            
            revision_options = get_filtered_options(component_validations_df, 'Revision', 
                                                  segment=segment, 
                                                  supplier=supplier, 
                                                  component_type=component_type) or predefined_options['revision']
            
            revision = st.selectbox("Revision", options=revision_options, key="component_revision")
            
            if st.button("Generate Process Code"):
                process_code_result, component_type_result, filtered_df = get_component_process_code(
                    segment, supplier, component_gen, revision, component_type, component_validations_df
                )
                
                if process_code_result and not process_code_result.startswith("Error") and not process_code_result.startswith("No"):
                    st.success(f"Process Code: {process_code_result}")
                    
                    if filtered_df is not None and not filtered_df.empty:
                        st.subheader("Component Details")
                        st.dataframe(filtered_df)
                else:
                    st.error(process_code_result)
    
    with tab3:
        st.write("Search for MPNs and look up their process codes:")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            search_term = st.text_input("Enter MPN or part of MPN to search:", key="mpn_search")
        
        with col2:
            use_optimized = st.checkbox("Use Direct Database Search", value=True, 
                                      help="Search directly in database for better performance")
        
        if search_term:
            if use_optimized:
                # Use optimized database search
                host, database, username, password, port = get_mysql_connection_params()
                matching_mpns = search_mpn_optimized(search_term, host, database, username, password, port)
            else:
                # Use cached data search
                matching_mpns = search_mpn_in_mysql(search_term, module_bom_simple_df)
            
            if matching_mpns:
                st.write(f"Found {len(matching_mpns)} matching MPNs:")
                
                selected_mpn = st.selectbox("Select an MPN to look up process code:", 
                                          options=[""] + matching_mpns, 
                                          key="selected_mpn")
                
                if selected_mpn:
                    if use_optimized:
                        # Use optimized database lookup
                        host, database, username, password, port = get_mysql_connection_params()
                        lookup_result, result_df = get_process_code_optimized(selected_mpn, host, database, username, password, port)
                    else:
                        # Use cached data lookup
                        lookup_result, result_df = get_process_code_from_mysql(selected_mpn, module_bom_simple_df)
                    
                    if lookup_result == "Success" and result_df is not None:
                        st.success(f"Found records for MPN: {selected_mpn}")
                        
                        # Display process codes prominently
                        if 'Process_Code' in result_df.columns:
                            process_codes = result_df['Process_Code'].dropna().unique()
                            if len(process_codes) > 0:
                                st.subheader("Process Codes Found:")
                                for pc in process_codes:
                                    if pc and str(pc).strip() and str(pc).lower() != 'not available':
                                        st.success(f"**Process Code: {pc}**")
                                    else:
                                        st.info("Process Code: Not Available")
                        
                        st.subheader("Detailed Results")
                        st.dataframe(result_df, height=400)
                    else:
                        st.error(lookup_result)
            else:
                st.warning(f"No MPNs found containing '{search_term}'")
        
        # Data analysis section
        if st.checkbox("Show Data Analysis", key="show_data_analysis"):
            if use_optimized:
                host, database, username, password, port = get_mysql_connection_params()
                analyze_mysql_data_optimized(host, database, username, password, port)
            else:
                analyze_mysql_data(module_bom_simple_df)

if __name__ == "__main__":
    main()