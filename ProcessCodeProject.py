import streamlit as st
import pandas as pd
import pytz
import urllib.parse
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
    if "sql_server" in st.secrets and "sql_database" in st.secrets:
        server = st.secrets["sql_server"]
        database = st.secrets["sql_database"]
        username = st.secrets.get("sql_username", "")
        password = st.secrets.get("sql_password", "")
    else:
        st.sidebar.subheader("SQL Server Connection")
        server = st.sidebar.text_input("SQL Server", key="sql_server_input")
        database = st.sidebar.text_input("Database", key="sql_database_input")
        username = st.sidebar.text_input("Username (leave blank for Windows Auth)", key="sql_username_input")
        password = st.sidebar.text_input("Password", type="password", key="sql_password_input")

@st.cache_data(ttl=3600)
def load_data_from_sql_server_cached(server, database, username, password):
    """Load data from SQL Server with caching - no widgets allowed here"""
    data = {
        'module_bom_59only_df': pd.DataFrame(),
        'module_bom_simple_df': pd.DataFrame()
    }
    
    if not (server and database):
        return data
    
    try:
        import pyodbc
        
        # Build connection string
        if username and password:
            # SQL Server authentication
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        else:
            # Windows authentication
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes"
        
        conn = pyodbc.connect(conn_str)
        
        # Query ModuleBOM_59only table
        try:
            query_59only = "SELECT * FROM ModuleBOM_59only"
            module_bom_59only_df = pd.read_sql(query_59only, conn)
            data['module_bom_59only_df'] = module_bom_59only_df
        except Exception as e:
            pass  # Will be handled in the UI layer
        
        # Query ModuleBOM_Simple table
        try:
            query_simple = "SELECT * FROM ModuleBOM_Simple"
            module_bom_simple_df = pd.read_sql(query_simple, conn)
            data['module_bom_simple_df'] = module_bom_simple_df
        except Exception as e:
            pass  # Will be handled in the UI layer
        
        conn.close()
        
    except Exception as e:
        pass  # Will be handled in the UI layer
    
    return data

def load_data_from_sql_server():
    """Wrapper function that gets connection params and calls cached function"""
    server, database, username, password = get_sql_connection_params()
    return load_data_from_sql_server_cached(server, database, username, password)

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
            return "Invalid segment selected. Please select 'Client' or 'Sever'"
    
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
        # Search in ModuleBOM_59only
        if not module_bom_59only_df.empty:
            for col in module_bom_59only_df.columns:
                if 'mpn' in col.lower() or 'material' in col.lower() or 'part' in col.lower():
                    matches = module_bom_59only_df[
                        module_bom_59only_df[col].astype(str).str.contains(search_term, case=False, na=False)
                    ][col].unique()
                    matching_mpns.extend(matches)
        
        # Search in ModuleBOM_Simple
        if not module_bom_simple_df.empty:
            for col in module_bom_simple_df.columns:
                if 'mpn' in col.lower() or 'material' in col.lower() or 'part' in col.lower():
                    matches = module_bom_simple_df[
                        module_bom_simple_df[col].astype(str).str.contains(search_term, case=False, na=False)
                    ][col].unique()
                    matching_mpns.extend(matches)
        
        # Remove duplicates and sort
        matching_mpns = sorted(list(set([mpn for mpn in matching_mpns if mpn and str(mpn).strip()])))
        
    except Exception as e:
        st.error(f"Error searching MPNs: {e}")
    
    return matching_mpns

def get_process_code_from_sql(mpn, module_bom_59only_df, module_bom_simple_df):
    """Look up process code for a given MPN in SQL data"""
    try:
        results = []
        
        # Search in ModuleBOM_59only
        if not module_bom_59only_df.empty:
            for col in module_bom_59only_df.columns:
                if 'mpn' in col.lower() or 'material' in col.lower() or 'part' in col.lower():
                    matches = module_bom_59only_df[
                        module_bom_59only_df[col].astype(str).str.contains(mpn, case=False, na=False)
                    ]
                    
                    for _, row in matches.iterrows():
                        result_row = {
                            'Source': 'ModuleBOM_59only',
                            'MPN': row.get(col, ''),
                            'Material Description': row.get('Material Description', ''),
                            'Process Code': 'Not Available in 59only Table'
                        }
                        
                        # Add other relevant columns
                        for c in row.index:
                            if c not in result_row and not pd.isna(row[c]):
                                result_row[c] = row[c]
                        
                        results.append(result_row)
        
        # Search in ModuleBOM_Simple
        if not module_bom_simple_df.empty:
            for col in module_bom_simple_df.columns:
                if 'mpn' in col.lower() or 'material' in col.lower() or 'part' in col.lower():
                    matches = module_bom_simple_df[
                        module_bom_simple_df[col].astype(str).str.contains(mpn, case=False, na=False)
                    ]
                    
                    for _, row in matches.iterrows():
                        result_row = {
                            'Source': 'ModuleBOM_Simple',
                            'MPN': row.get(col, ''),
                            'Material Description': row.get('Material Description', ''),
                            'Process Code': row.get('Process Code', 'Not Available in Simple Table')
                        }
                        
                        # Add other relevant columns
                        for c in row.index:
                            if c not in result_row and not pd.isna(row[c]):
                                result_row[c] = row[c]
                        
                        results.append(result_row)
        
        if not results:
            return f"No records found for MPN: {mpn}", None
        
        result_df = pd.DataFrame(results)
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
    
    show_process_code_info()
    
    st.sidebar.header("Data Source")
    st.sidebar.info("Data is being loaded from SharePoint lists")
    
    if st.sidebar.button("Refresh Data"):
        if hasattr(load_data_from_sharepoint, 'clear'):
            load_data_from_sharepoint.clear()
        st.rerun()
    
    if st.sidebar.button("Refresh Data"):
        if hasattr(load_data_from_sharepoint, 'clear'):
            load_data_from_sharepoint.clear()
        if hasattr(load_data_from_sql_server_cached, 'clear'):
            load_data_from_sql_server_cached.clear()
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
                        if not any(valid_gen.lower() in gen.lower() for valid_gen in valid_gen_options_lower):
                            st.error(f"Invalid component generation for {component_name}. Must be one of: Gen1, Gen2, Gen3, Gen4, Gen5, or NA")
                            component_codes[component_key] = ""
                            continue
                    
                    code, _, _ = get_component_process_code(
                        module_segment, supplier, gen, rev, component_name, component_validations_df
                    )
                    
                    if isinstance(code, str) and not code.startswith("No matching") and not code.startswith("Error") and not code.startswith("Invalid"):
                        st.success(f"{component_name} Process Code: {code}")
                        component_codes[component_key] = code
                    else:
                        st.error(f"{component_name} Process Code: {code}")
                        component_codes[component_key] = ""
                else:
                    component_codes[component_key] = ""
            
            if st.button("Generate Module Process Code"):
                if not component_codes.get("pmic", ""):
                    st.error("Invalid PMIC process code. Please check PMIC selection.")
                elif not component_codes.get("spd_hub", ""):
                    st.error("Invalid SPD/Hub process code. Please check SPD/Hub selection.")
                else:
                    if module_segment.lower() == 'client':
                        process_code = get_module_process_code(
                            component_codes.get("pmic", ""),
                            component_codes.get("spd_hub", ""),
                            component_codes.get("ckd", ""),
                            "",
                            "",
                            module_segment
                        )
                    else:
                        if not component_codes.get("temp_sensor", ""):
                            st.error("Invalid Temp Sensor process code. Please check Temp Sensor selection.")
                            return
                        if not component_codes.get("rcd_mrcd", ""):
                            st.error("Invalid RCD/MRCD process code. Please check RCD/MRCD selection.")
                            return
                            
                        process_code = get_module_process_code(
                            component_codes.get("pmic", ""),
                            component_codes.get("spd_hub", ""),
                            component_codes.get("temp_sensor", ""),
                            component_codes.get("rcd_mrcd", ""),
                            component_codes.get("data_buffer", ""),
                            module_segment
                        )
                    
                    if process_code.startswith("For server") or process_code.startswith("For client") or process_code.startswith("Unknown"):
                        st.error(process_code)
                    else:
                        st.success(f"Generated Module Process Code: {process_code}")
                        
                        if module_segment.lower() == 'server':
                            component_chars = list(process_code)
                            print_order = []
                            
                            positions = [0, 3, 1, 2, 4]
                            for pos in positions:
                                if pos < len(component_chars):
                                    print_order.append(component_chars[pos])
                                    
                            print_code = ''.join(print_order)
                            st.success(f"Generated Module Process Print Code: {print_code}")
                            st.caption("(Print order: PMIC → RCD → SPD/Hub → Temp Sensor → Data Buffer)")
                            
                        elif module_segment.lower() == 'client':
                            st.success(f"Generated Module Process Print Code: {process_code}")
                            st.caption("(Print order: PMIC → SPD/Hub → CKD)")
                        
                        explanation = explain_process_code(process_code, module_segment)
                        st.info(explanation)
        
        with subtab1:
            st.write("Enter the component details to generate a single component process code:")
            
            selected_segment = st.selectbox("Segment", options=predefined_options['segment'], key="segment_component")
            selected_component_type = st.selectbox("Component Type", options=predefined_options['component_type'], key="component_type")
            
            supplier_options = get_filtered_options(component_validations_df, 'Supplier', 
                                                  segment=selected_segment, 
                                                  component_type=selected_component_type) or predefined_options['supplier']
            selected_supplier = st.selectbox("Supplier", options=supplier_options, key="supplier_component")
            
            if selected_segment.lower() == 'server' and selected_component_type.lower() in ['temp sensor', 'rcd', 'muxed rcd', 'data buffer']:
                valid_gen_options = ["Gen1", "Gen2", "Gen3", "Gen4", "Gen5", "NA"]
                
                data_gen_options = get_filtered_options(component_validations_df, 'Component_Generation', 
                                                     segment=selected_segment, 
                                                     supplier=selected_supplier, 
                                                     component_type=selected_component_type)
                
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
                                                 segment=selected_segment, 
                                                 supplier=selected_supplier,
                                                 component_type=selected_component_type) or predefined_options['component_generation']
            
            selected_component_gen = st.selectbox("Component Generation", options=gen_options, key="component_gen")
            
            rev_options = get_filtered_options(component_validations_df, 'Revision', 
                                             segment=selected_segment, 
                                             supplier=selected_supplier,
                                             component_type=selected_component_type) or predefined_options['revision']
            selected_revision = st.selectbox("Revision", options=rev_options, key="revision_component")
            
            if st.button("Generate Component Process Code"):
                if selected_segment.lower() == 'server' and selected_component_type.lower() in ['temp sensor', 'rcd', 'muxed rcd', 'data buffer']:
                    valid_gen_options_lower = [opt.lower() for opt in ["Gen1", "Gen2", "Gen3", "Gen4", "Gen5", "NA"]]
                    if not any(valid_gen.lower() in selected_component_gen.lower() for valid_gen in valid_gen_options_lower):
                        st.error(f"Invalid component generation for {selected_component_type}. Must be one of: Gen1, Gen2, Gen3, Gen4, Gen5, or NA")
                        return
                
                process_code, _, code_details = get_component_process_code(
                    selected_segment, selected_supplier, selected_component_gen, selected_revision, 
                    selected_component_type, component_validations_df
                )
                
                if isinstance(process_code, str) and not process_code.startswith("No matching") and not process_code.startswith("Error") and not process_code.startswith("Invalid"):
                    st.success(f"Generated Process Code: {process_code}")
                    
                    if code_details is not None and not code_details.empty:
                        st.subheader("Component Details")
                        for col in code_details.columns:
                            if code_details[col].dtype == 'object':
                                code_details[col] = code_details[col].str.upper()
                        
                        st.table(code_details)
                else:
                    st.error(process_code)

    with tab3:
        st.write("Look up Process Code using MPN (Material Part Number) from SQL Server databases:")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # MPN input with search functionality
            search_term = st.text_input("Search MPN", key="mpn_search", 
                                      help="Enter part or full MPN to search")
            
            if search_term:
                matching_mpns = search_mpn_in_sql(search_term, module_bom_59only_df, module_bom_simple_df)
                if matching_mpns:
                    selected_mpn = st.selectbox("Select MPN", options=matching_mpns, key="selected_mpn")
                else:
                    st.warning(f"No MPNs found containing '{search_term}'")
                    selected_mpn = st.text_input("Enter MPN manually", key="manual_mpn")
            else:
                selected_mpn = st.text_input("Enter MPN", key="direct_mpn")
        
        with col2:
            st.write("**Available Data Sources:**")
            if not module_bom_59only_df.empty:
                st.success(f"✅ ModuleBOM_59only ({len(module_bom_59only_df)} records)")
            else:
                st.error("❌ ModuleBOM_59only (No data)")
            
            if not module_bom_simple_df.empty:
                st.success(f"✅ ModuleBOM_Simple ({len(module_bom_simple_df)} records)")
            else:
                st.error("❌ ModuleBOM_Simple (No data)")
        
        if st.button("Look Up Process Code by MPN"):
            if not selected_mpn:
                st.error("Please enter or select an MPN")
            else:
                result_message, result_df = get_process_code_from_sql(selected_mpn, module_bom_59only_df, module_bom_simple_df)
                
                if result_message == "Success" and result_df is not None:
                    st.success(f"Found {len(result_df)} matching record(s) for MPN: {selected_mpn}")
                    
                    # Display results
                    st.subheader("Process Code Lookup Results")
                    
                    # Format the dataframe for better display
                    display_df = result_df.copy()
                    for col in display_df.columns:
                        if display_df[col].dtype == 'object':
                            display_df[col] = display_df[col].astype(str)
                    
                    st.dataframe(display_df, height=400)
                    
                    # Show process codes found
                    process_codes = result_df[result_df['Process Code'] != 'Not Available in Simple Table']['Process Code'].unique()
                    process_codes = [pc for pc in process_codes if pc and pc != 'Not Available in Simple Table']
                    
                    if process_codes:
                        st.subheader("Process Codes Found")
                        for pc in process_codes:
                            st.info(f"Process Code: **{pc}**")
                            
                            # Try to explain the process code if we can determine the segment
                            # Look for segment information in the results
                            segment_info = result_df[result_df['Process Code'] == pc]
                            if not segment_info.empty:
                                # Try to determine segment from BOM or material description
                                material_desc = segment_info.iloc[0]['Material Description']
                                if any(term in material_desc.lower() for term in ['server', 'rdimm', 'lrdimm']):
                                    segment = 'server'
                                elif any(term in material_desc.lower() for term in ['client', 'udimm', 'sodimm']):
                                    segment = 'client'
                                else:
                                    segment = 'unknown'
                                
                                if segment != 'unknown':
                                    explanation = explain_process_code(pc, segment)
                                    st.info(explanation)
                    else:
                        st.warning("Process codes found but not available in the current data sources")
                
                else:
                    st.error(result_message)
        
        # Add data analysis section
        if st.checkbox("Show SQL Data Analysis", key="show_sql_analysis"):
            analyze_sql_data(module_bom_59only_df, module_bom_simple_df)

if __name__ == "__main__":
    main()