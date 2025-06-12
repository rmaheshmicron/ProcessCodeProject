import streamlit as st
import pandas as pd
import requests
from urllib.parse import urlparse, quote
import msal
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
        
        ### Server Process Code (4-5 characters)
        1. PMIC
        2. SPD/Hub
        3. Temp Sensor
        4. RCD/MRCD
        5. Data Buffer (optional)
        
        ### Client Process Code (2-3 characters)
        1. PMIC
        2. SPD/Hub
        3. CKD (optional)
        
        ## Print Order on Product Label
        
        ### Server
        PMIC → RCD → SPD/Hub → Temp Sensor → Data Buffer (if applicable)
        
        ### Client
        PMIC → SPD/Hub → CKD (if applicable)
        """)

@st.cache_data(ttl=3600)
def load_data_from_sharepoint():
    data = {
        'component_validations_df': pd.DataFrame(),
        'module_validation_df': pd.DataFrame()
    }
    
    sharepoint_site = "https://microncorp.sharepoint.com/sites/mdg"
    list_name = "Basic List"
    
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
        
        with st.sidebar.expander("Available SharePoint Lists", expanded=False):
            st.write(", ".join(available_lists))
        
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
        
        list_fields = target_list.fields.get().execute_query()
        field_names = [field.properties.get('InternalName', '') for field in list_fields 
                      if not field.properties.get('Hidden', True) and field.properties.get('InternalName', '')]
        
        with st.sidebar.expander("Available Fields", expanded=False):
            st.write(", ".join(field_names))
        
        all_items = []
        page_size = 1000
        
        caml_query = CamlQuery()
        caml_query.ViewXml = f"""
        <View>
            <RowLimit>{page_size}</RowLimit>
        </View>
        """
        
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
        
        if len(all_items) == 0:
            st.sidebar.error("No items found in the list")
            return data
        
        st.sidebar.success(f"Retrieved {len(all_items)} items from SharePoint")
        
        if all_items:
            with st.sidebar.expander("Sample Item Properties", expanded=False):
                st.write(list(all_items[0].properties.keys()))
        
        component_validations_data = []
        
        field_mapping = {
            'Segment': next((f for f in field_names if any(term in f.lower() for term in ['segment', 'market', 'title'])), 'Title'),
            'Supplier': next((f for f in field_names if any(term in f.lower() for term in ['supplier', 'vendor', 'manufacturer'])), None),
            'Component_Generation': next((f for f in field_names if any(term in f.lower() for term in ['generation', 'gen', 'componentgen'])), None),
            'Revision': next((f for f in field_names if any(term in f.lower() for term in ['revision', 'rev', 'version'])), None),
            'Component_Type': next((f for f in field_names if any(term in f.lower() for term in ['component type', 'componenttype', 'type'])), None),
            'Process_Code': next((f for f in field_names if any(term in f.lower() for term in ['process code', 'processcode', 'code'])), None),
            'MPN': next((f for f in field_names if any(term in f.lower() for term in ['mpn', 'part number', 'partnumber'])), None)
        }
        
        with st.sidebar.expander("Field Mapping", expanded=False):
            st.write(field_mapping)
        
        for item in all_items:
            item_properties = item.properties
            
            record = {}
            for key, field in field_mapping.items():
                if field and field in item_properties:
                    record[key] = str(item_properties[field])
                else:
                    record[key] = ""
            
            if not any(record.values()):
                for prop_key, prop_value in item_properties.items():
                    if prop_key not in ['_ObjectType_', '_ObjectIdentity_', 'FileSystemObjectType', 'ServerRedirectedEmbedUri', 
                                       'ServerRedirectedEmbedUrl', 'ContentTypeId', 'ComplianceAssetId', 'OData__UIVersionString']:
                        if prop_key == 'Title':
                            record['Segment'] = str(prop_value)
                        elif 'supplier' in prop_key.lower():
                            record['Supplier'] = str(prop_value)
                        elif 'gen' in prop_key.lower():
                            record['Component_Generation'] = str(prop_value)
                        elif 'rev' in prop_key.lower():
                            record['Revision'] = str(prop_value)
                        elif 'type' in prop_key.lower():
                            record['Component_Type'] = str(prop_value)
                        elif 'code' in prop_key.lower():
                            record['Process_Code'] = str(prop_value)
                        elif 'mpn' in prop_key.lower() or 'part' in prop_key.lower():
                            record['MPN'] = str(prop_value)
            
            if record.get('Segment') and (record.get('Supplier') or record.get('Component_Type') or record.get('Process_Code')):
                component_validations_data.append(record)
        
        component_validations_df = pd.DataFrame(component_validations_data)
        data['component_validations_df'] = component_validations_df
        
        st.sidebar.success(f"Successfully loaded {len(component_validations_data)} component validations from SharePoint")
        
        with st.sidebar.expander("Sample Data (First 5 Rows)", expanded=False):
            st.dataframe(component_validations_df.head())
        
        module_validation_data = []
        
        module_field_mapping = {
            'Segment': field_mapping['Segment'],
            'Form_Factor': next((f for f in field_names if any(term in f.lower() for term in ['form factor', 'formfactor', 'form'])), None),
            'Speed': next((f for f in field_names if any(term in f.lower() for term in ['speed', 'frequency', 'mhz'])), None),
            'PMIC': next((f for f in field_names if any(term in f.lower() for term in ['pmic', 'power'])), None),
            'SPD_Hub': next((f for f in field_names if any(term in f.lower() for term in ['spd', 'hub', 'spdhub'])), None),
            'Temp_Sensor': next((f for f in field_names if any(term in f.lower() for term in ['temp', 'sensor', 'temperature'])), None),
            'RCD_MRCD': next((f for f in field_names if any(term in f.lower() for term in ['rcd', 'mrcd', 'register'])), None),
            'Data_Buffer': next((f for f in field_names if any(term in f.lower() for term in ['data buffer', 'databuffer', 'buffer'])), None),
            'Process_Code': field_mapping['Process_Code']
        }
        
        for item in all_items:
            item_properties = item.properties
            
            record = {}
            for key, field in module_field_mapping.items():
                if field and field in item_properties:
                    record[key] = str(item_properties[field])
                else:
                    record[key] = ""
            
            if record.get('Segment') and record.get('Process_Code'):
                module_validation_data.append(record)
        
        module_validation_df = pd.DataFrame(module_validation_data)
        data['module_validation_df'] = module_validation_df
        
        if not module_validation_df.empty:
            st.sidebar.success(f"Successfully loaded {len(module_validation_data)} module validations from SharePoint")
        
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
        
        for col in ['Segment', 'Supplier', 'Component_Generation', 'Revision', 'Component_Type']:
            if col in df.columns:
                df[col] = df[col].str.lower()
        
        filters = []
        if segment and 'Segment' in df.columns:
            filters.append(df['Segment'] == segment.lower())
        if supplier and 'Supplier' in df.columns:
            filters.append(df['Supplier'] == supplier.lower())
        if component_gen and 'Component_Generation' in df.columns:
            filters.append(df['Component_Generation'] == component_gen.lower())
        if revision and 'Revision' in df.columns:
            filters.append(df['Revision'] == revision.lower())
        if component_type and 'Component_Type' in df.columns:
            filters.append(df['Component_Type'] == component_type.lower())
        
        if filters:
            filtered_df = df.copy()
            for f in filters:
                filtered_df = filtered_df[f]
        else:
            filtered_df = df.copy()
        
        if filtered_df.empty:
            relaxed_filters = []
            if segment and 'Segment' in df.columns:
                relaxed_filters.append(df['Segment'].str.contains(segment.lower()))
            if supplier and 'Supplier' in df.columns:
                relaxed_filters.append(df['Supplier'].str.contains(supplier.lower()))
            if component_gen and 'Component_Generation' in df.columns:
                relaxed_filters.append(df['Component_Generation'].str.contains(component_gen.lower()))
            if revision and 'Revision' in df.columns:
                relaxed_filters.append(df['Revision'].str.contains(revision.lower()))
            if component_type and 'Component_Type' in df.columns:
                relaxed_filters.append(df['Component_Type'].str.contains(component_type.lower()))
            
            if relaxed_filters:
                filtered_df = df.copy()
                for f in relaxed_filters:
                    filtered_df = filtered_df[f]
            
            if filtered_df.empty:
                return "No matching process code found for the given criteria", None, None
        
        process_code = filtered_df.iloc[0]['Process_Code']
        component_type_result = filtered_df.iloc[0]['Component_Type'] if 'Component_Type' in filtered_df.columns else "Unknown"
        
        return process_code, component_type_result, filtered_df
    
    except Exception as e:
        return f"Error generating process code: {e}", None, None

def filter_module_process_code(segment, form_factor, speed, module_validation_df):
    try:
        if module_validation_df.empty:
            return "No module validation data available", None
        
        df = module_validation_df.copy()
        
        for col in ['Segment', 'Form_Factor', 'Speed']:
            if col in df.columns:
                df[col] = df[col].str.lower()
        
        filters = []
        if segment and 'Segment' in df.columns:
            filters.append(df['Segment'] == segment.lower())
        if form_factor and 'Form_Factor' in df.columns:
            filters.append(df['Form_Factor'] == form_factor.lower())
        if speed and 'Speed' in df.columns:
            filters.append(df['Speed'] == speed.lower())
        
        if filters:
            filtered_df = df.copy()
            for f in filters:
                filtered_df = filtered_df[f]
        else:
            filtered_df = df.copy()
        
        if filtered_df.empty:
            relaxed_filters = []
            if segment and 'Segment' in df.columns:
                relaxed_filters.append(df['Segment'].str.contains(segment.lower()))
            if form_factor and 'Form_Factor' in df.columns:
                relaxed_filters.append(df['Form_Factor'].str.contains(form_factor.lower()))
            if speed and 'Speed' in df.columns:
                relaxed_filters.append(df['Speed'].str.contains(speed.lower()))
            
            if relaxed_filters:
                filtered_df = df.copy()
                for f in relaxed_filters:
                    filtered_df = filtered_df[f]
            
            if filtered_df.empty:
                return "No matching process code found for the given criteria", None
        
        process_code = filtered_df.iloc[0]['Process_Code']
        
        component_codes = {}
        for component in ['PMIC', 'SPD_Hub', 'Temp_Sensor', 'RCD_MRCD', 'Data_Buffer']:
            if component in filtered_df.columns and not pd.isna(filtered_df.iloc[0][component]):
                component_codes[component] = filtered_df.iloc[0][component]
        
        return process_code, filtered_df
    
    except Exception as e:
        return f"Error generating process code: {e}", None

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
            return "Unknown segment. Cannot generate process code."
    
    except Exception as e:
        return f"Error generating module process code: {e}"

def lookup_parts_by_process_code(process_code, component_validations_df):
    try:
        if not process_code:
            return "No process code provided"
        
        if component_validations_df.empty:
            return "No component validation data available"
        
        component_codes = list(process_code)
        
        result_parts = []
        
        for i, code in enumerate(component_codes):
            filtered_df = component_validations_df[
                component_validations_df['Process_Code'] == code
            ]
            
            if not filtered_df.empty:
                for _, row in filtered_df.iterrows():
                    result_parts.append({
                        'Position': i + 1,
                        'Process_Code_Char': code,
                        'Component_Type': row['Component_Type'] if 'Component_Type' in row else "Unknown",
                        'Supplier': row['Supplier'] if 'Supplier' in row else "Unknown",
                        'Component_Generation': row['Component_Generation'] if 'Component_Generation' in row else "Unknown",
                        'Revision': row['Revision'] if 'Revision' in row else "Unknown",
                        'MPN': row['MPN'] if 'MPN' in row else "Unknown"
                    })
        
        if not result_parts:
            for i, code in enumerate(component_codes):
                for _, row in component_validations_df.iterrows():
                    if 'Process_Code' in row and code in str(row['Process_Code']):
                        result_parts.append({
                            'Position': i + 1,
                            'Process_Code_Char': code,
                            'Component_Type': row['Component_Type'] if 'Component_Type' in row else "Unknown",
                            'Supplier': row['Supplier'] if 'Supplier' in row else "Unknown",
                            'Component_Generation': row['Component_Generation'] if 'Component_Generation' in row else "Unknown",
                            'Revision': row['Revision'] if 'Revision' in row else "Unknown",
                            'MPN': row['MPN'] if 'MPN' in row else "Unknown"
                        })
        
        if not result_parts:
            return "No matching parts found for the given process code"
        
        result_df = pd.DataFrame(result_parts)
        result_df = result_df.sort_values('Position')
        
        result = result_df.to_string(index=False)
        return result
    
    except Exception as e:
        return f"Error looking up parts: {e}"

def explain_process_code(process_code, segment):
    if not process_code or not isinstance(process_code, str):
        return "Invalid process code"
    
    explanation = []
    explanation.append(f"Process Code: {process_code}")
    explanation.append("Component Breakdown:")
    
    if segment.lower() == 'server':
        for i, char in enumerate(process_code):
            if i == 0:
                explanation.append(f"Position 1: PMIC - {char}")
            elif i == 1:
                explanation.append(f"Position 2: SPD/Hub - {char}")
            elif i == 2:
                explanation.append(f"Position 3: Temp Sensor - {char}")
            elif i == 3:
                explanation.append(f"Position 4: RCD/MRCD - {char}")
            elif i == 4:
                explanation.append(f"Position 5: Data Buffer - {char}")
        
        explanation.append("\nProcess Code Print Order (as shown on product label):")
        explanation.append("PMIC → RCD → SPD/Hub → Temp Sensor → Data Buffer (if applicable)")
    
    elif segment.lower() == 'client':
        for i, char in enumerate(process_code):
            if i == 0:
                explanation.append(f"Position 1: PMIC - {char}")
            elif i == 1:
                explanation.append(f"Position 2: SPD/Hub - {char}")
            elif i == 2:
                explanation.append(f"Position 3: CKD - {char}")
        
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
        'revision': ["01", "01/A0", "3", "A0", "A0/01", "A0/ES0", "A00", "A1", "A2", "A3", 
                    "A4", "A5", "A6", "B0", "B0/ES1", "B0/G1A", "B08", "B1", "B1A", "B2", 
                    "B2-A", "B3", "C0", "C1", "C2", "C3", "C5", "D0", "D1", "D1/G1EX", "D2", 
                    "D3", "D5", "E0", "G1A", "G1B", "G1DX", "G1E", "MB2", "PG3.2", "R0", "R1", 
                    "R1.1", "R1.2", "R1.3", "R2", "R3.5", "R4.0", "R6.0", "R6.1", "R6.2", "X2"],
        'component_type': ["PMIC", "SPD/Hub", "Temp Sensor", "RCD/MRCD", "Data Buffer", "CKD"]
    }
    
    if not component_validations_df.empty:
        try:
            if 'Segment' in component_validations_df.columns:
                segments = component_validations_df['Segment'].dropna().unique().tolist()
                if segments:
                    default_options['segment'] = sorted(list(set([s for s in segments if s])))
            
            if 'Supplier' in component_validations_df.columns:
                suppliers = component_validations_df['Supplier'].dropna().unique().tolist()
                if suppliers:
                    default_options['supplier'] = sorted(list(set([s for s in suppliers if s])))
            
            if 'Component_Generation' in component_validations_df.columns:
                gens = component_validations_df['Component_Generation'].dropna().unique().tolist()
                if gens:
                    default_options['component_generation'] = sorted(list(set([g for g in gens if g])))
            
            if 'Revision' in component_validations_df.columns:
                revs = component_validations_df['Revision'].dropna().unique().tolist()
                if revs:
                    default_options['revision'] = sorted(list(set([r for r in revs if r])))
            
            if 'Component_Type' in component_validations_df.columns:
                types = component_validations_df['Component_Type'].dropna().unique().tolist()
                if types:
                    default_options['component_type'] = sorted(list(set([t for t in types if t])))
        
        except Exception as e:
            st.sidebar.warning(f"Error extracting options from data: {e}")
    
    return default_options

def get_filtered_options(df, field, segment=None, supplier=None, component_type=None):
    if df.empty or field not in df.columns:
        return []
    
    filtered_df = df.copy()
    
    if segment and 'Segment' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['Segment'].str.lower() == segment.lower()]
    
    if supplier and 'Supplier' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['Supplier'].str.lower() == supplier.lower()]
    
    if component_type and 'Component_Type' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['Component_Type'].str.lower() == component_type.lower()]
    
    if filtered_df.empty:
        return []
    
    options = filtered_df[field].dropna().unique().tolist()
    return sorted(list(set([o for o in options if o])))

def main():
    st.title("Process Code & Part Specification Generator")
    
    show_process_code_info()
    
    st.sidebar.header("Data Source")
    st.sidebar.info("Data is being loaded from SharePoint lists")
    
    if st.sidebar.button("Refresh Data"):
        if hasattr(load_data_from_sharepoint, 'clear'):
            load_data_from_sharepoint.clear()
        st.rerun()
    
    data = load_data_from_sharepoint()
    
    component_validations_df = data['component_validations_df']
    module_validation_df = data['module_validation_df']
    
    predefined_options = get_predefined_options(component_validations_df)
    
    st.sidebar.info(f"Data last refreshed: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = "process_code"
    
    part = PartSpecification()
    
    tab1, tab2 = st.tabs(["Process Code Generator", "Part Specification Generator"])
    
    with tab1:
        subtab1, subtab2 = st.tabs(["Component", "Module"])
        
        with subtab1:
            st.write("Enter the component details to generate a single component process code:")
            
            selected_segment = st.selectbox("Segment", options=predefined_options['segment'], key="segment_component")
            
            selected_component_type = st.selectbox("Component Type", options=predefined_options['component_type'], key="component_type")
            
            supplier_options = get_filtered_options(component_validations_df, 'Supplier', 
                                                  segment=selected_segment, 
                                                  component_type=selected_component_type) or predefined_options['supplier']
            selected_supplier = st.selectbox("Supplier", options=supplier_options, key="supplier_component")
            
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
                st.session_state.active_tab = "component_process_code"
                
                part.set_segment(selected_segment)
                part.set_supplier(selected_supplier)
                part.set_component_gen(selected_component_gen)
                part.set_revision(selected_revision)
                
                process_code, component_type, code_details = get_component_process_code(
                    selected_segment, selected_supplier, selected_component_gen, selected_revision, 
                    selected_component_type, component_validations_df
                )
                
                if isinstance(process_code, str) and not process_code.startswith("No matching") and not process_code.startswith("Error"):
                    result_text = f"Generated Process Code: {process_code}\nComponent Type: {component_type}"
                else:
                    result_text = process_code
                
                st.session_state.result = result_text
                st.session_state.show_result = True
                
                if code_details is not None and not code_details.empty:
                    st.session_state.code_details = code_details
                else:
                    st.session_state.code_details = None
        
        with subtab2:
            st.write("Enter the module component details to generate a combined module process code:")
            
            st.subheader("PMIC")
            pmic_segment = st.selectbox("Segment", options=predefined_options['segment'], key="pmic_segment")
            
            pmic_supplier_options = get_filtered_options(component_validations_df, 'Supplier', 
                                                       segment=pmic_segment, component_type="PMIC") or predefined_options['supplier']
            pmic_supplier = st.selectbox("Supplier", options=pmic_supplier_options, key="pmic_supplier")
            
            pmic_gen_options = get_filtered_options(component_validations_df, 'Component_Generation', 
                                                  segment=pmic_segment, supplier=pmic_supplier) or predefined_options['component_generation']
            pmic_gen = st.selectbox("Component Generation", options=pmic_gen_options, key="pmic_gen")
            
            pmic_rev_options = get_filtered_options(component_validations_df, 'Revision', 
                                                 segment=pmic_segment, supplier=pmic_supplier) or predefined_options['revision']
            pmic_rev = st.selectbox("Revision", options=pmic_rev_options, key="pmic_rev")
            
            pmic_code, _, _ = get_component_process_code(
                pmic_segment, pmic_supplier, pmic_gen, pmic_rev, "PMIC", component_validations_df
            )
            if isinstance(pmic_code, str) and not pmic_code.startswith("No matching") and not pmic_code.startswith("Error"):
                st.success(f"PMIC Process Code: {pmic_code}")
            else:
                st.error(f"PMIC Process Code: {pmic_code}")
            
            st.subheader("SPD/Hub")
            spd_segment = st.selectbox("Segment", options=predefined_options['segment'], key="spd_segment")
            
            spd_supplier_options = get_filtered_options(component_validations_df, 'Supplier', 
                                                      segment=spd_segment, component_type="SPD/Hub") or predefined_options['supplier']
            spd_supplier = st.selectbox("Supplier", options=spd_supplier_options, key="spd_supplier")
            
            spd_gen_options = get_filtered_options(component_validations_df, 'Component_Generation', 
                                                 segment=spd_segment, supplier=spd_supplier) or predefined_options['component_generation']
            spd_gen = st.selectbox("Component Generation", options=spd_gen_options, key="spd_gen")
            
            spd_rev_options = get_filtered_options(component_validations_df, 'Revision', 
                                                segment=spd_segment, supplier=spd_supplier) or predefined_options['revision']
            spd_rev = st.selectbox("Revision", options=spd_rev_options, key="spd_rev")
            
            spd_code, _, _ = get_component_process_code(
                spd_segment, spd_supplier, spd_gen, spd_rev, "SPD/Hub", component_validations_df
            )
            if isinstance(spd_code, str) and not spd_code.startswith("No matching") and not spd_code.startswith("Error"):
                st.success(f"SPD/Hub Process Code: {spd_code}")
            else:
                st.error(f"SPD/Hub Process Code: {spd_code}")
            
            st.subheader("Temp Sensor")
            temp_segment = st.selectbox("Segment", options=predefined_options['segment'], key="temp_segment")
            
            temp_supplier_options = get_filtered_options(component_validations_df, 'Supplier', 
                                                       segment=temp_segment, component_type="Temp Sensor") or predefined_options['supplier']
            temp_supplier_options = temp_supplier_options + ["None"]
            temp_supplier = st.selectbox("Supplier", options=temp_supplier_options, key="temp_supplier")
            
            if temp_supplier != "None":
                temp_gen_options = get_filtered_options(component_validations_df, 'Component_Generation', 
                                                      segment=temp_segment, supplier=temp_supplier) or predefined_options['component_generation']
                temp_gen = st.selectbox("Component Generation", options=temp_gen_options, key="temp_gen")
                
                temp_rev_options = get_filtered_options(component_validations_df, 'Revision', 
                                                     segment=temp_segment, supplier=temp_supplier) or predefined_options['revision']
                temp_rev = st.selectbox("Revision", options=temp_rev_options, key="temp_rev")
                
                temp_code, _, _ = get_component_process_code(
                    temp_segment, temp_supplier, temp_gen, temp_rev, "Temp Sensor", component_validations_df
                )
                if isinstance(temp_code, str) and not temp_code.startswith("No matching") and not temp_code.startswith("Error"):
                    st.success(f"Temp Sensor Process Code: {temp_code}")
                else:
                    st.error(f"Temp Sensor Process Code: {temp_code}")
            else:
                temp_code = ""
            
            st.subheader("RCD/MRCD")
            rcd_segment = st.selectbox("Segment", options=predefined_options['segment'], key="rcd_segment")
            
            rcd_supplier_options = get_filtered_options(component_validations_df, 'Supplier', 
                                                      segment=rcd_segment, component_type="RCD/MRCD") or predefined_options['supplier']
            rcd_supplier_options = rcd_supplier_options + ["None"]
            rcd_supplier = st.selectbox("Supplier", options=rcd_supplier_options, key="rcd_supplier")
            
            if rcd_supplier != "None":
                rcd_gen_options = get_filtered_options(component_validations_df, 'Component_Generation', 
                                                     segment=rcd_segment, supplier=rcd_supplier) or predefined_options['component_generation']
                rcd_gen = st.selectbox("Component Generation", options=rcd_gen_options, key="rcd_gen")
                
                rcd_rev_options = get_filtered_options(component_validations_df, 'Revision', 
                                                    segment=rcd_segment, supplier=rcd_supplier) or predefined_options['revision']
                rcd_rev = st.selectbox("Revision", options=rcd_rev_options, key="rcd_rev")
                
                rcd_code, _, _ = get_component_process_code(
                    rcd_segment, rcd_supplier, rcd_gen, rcd_rev, "RCD/MRCD", component_validations_df
                )
                if isinstance(rcd_code, str) and not rcd_code.startswith("No matching") and not rcd_code.startswith("Error"):
                    st.success(f"RCD/MRCD Process Code: {rcd_code}")
                else:
                    st.error(f"RCD/MRCD Process Code: {rcd_code}")
            else:
                rcd_code = ""
            
            st.subheader("Data Buffer")
            db_segment = st.selectbox("Segment", options=predefined_options['segment'], key="db_segment")
            
            db_supplier_options = get_filtered_options(component_validations_df, 'Supplier', 
                                                     segment=db_segment, component_type="Data Buffer") or predefined_options['supplier']
            db_supplier_options = db_supplier_options + ["None"]
            db_supplier = st.selectbox("Supplier", options=db_supplier_options, key="db_supplier")
            
            if db_supplier != "None":
                db_gen_options = get_filtered_options(component_validations_df, 'Component_Generation', 
                                                    segment=db_segment, supplier=db_supplier) or predefined_options['component_generation']
                db_gen = st.selectbox("Component Generation", options=db_gen_options, key="db_gen")
                
                db_rev_options = get_filtered_options(component_validations_df, 'Revision', 
                                                   segment=db_segment, supplier=db_supplier) or predefined_options['revision']
                db_rev = st.selectbox("Revision", options=db_rev_options, key="db_rev")
                
                db_code, _, _ = get_component_process_code(
                    db_segment, db_supplier, db_gen, db_rev, "Data Buffer", component_validations_df
                )
                if isinstance(db_code, str) and not db_code.startswith("No matching") and not db_code.startswith("Error"):
                    st.success(f"Data Buffer Process Code: {db_code}")
                else:
                    st.error(f"Data Buffer Process Code: {db_code}")
            else:
                db_code = ""
            
            if st.button("Generate Module Process Code"):
                st.session_state.active_tab = "module_process_code"
                
                module_segment = pmic_segment
                
                if pmic_code.startswith("No matching") or pmic_code.startswith("Error"):
                    st.error("Invalid PMIC process code. Please check PMIC selection.")
                elif spd_code.startswith("No matching") or spd_code.startswith("Error"):
                    st.error("Invalid SPD/Hub process code. Please check SPD/Hub selection.")
                elif temp_supplier != "None" and (temp_code.startswith("No matching") or temp_code.startswith("Error")):
                    st.error("Invalid Temp Sensor process code. Please check Temp Sensor selection.")
                elif rcd_supplier != "None" and (rcd_code.startswith("No matching") or rcd_code.startswith("Error")):
                    st.error("Invalid RCD/MRCD process code. Please check RCD/MRCD selection.")
                elif db_supplier != "None" and (db_code.startswith("No matching") or db_code.startswith("Error")):
                    st.error("Invalid Data Buffer process code. Please check Data Buffer selection.")
                else:
                    process_code = get_module_process_code(
                        pmic_code, spd_code, temp_code, rcd_code, db_code, module_segment
                    )
                    
                    if process_code.startswith("For server") or process_code.startswith("For client") or process_code.startswith("Unknown"):
                        st.error(process_code)
                    else:
                        st.success(f"Generated Module Process Code: {process_code}")
                        
                        explanation = explain_process_code(process_code, module_segment)
                        st.info(explanation)
                        
                        parts_lookup = lookup_parts_by_process_code(process_code, component_validations_df)
                        if not parts_lookup.startswith("No matching") and not parts_lookup.startswith("Error"):
                            st.subheader("Component Details")
                            st.text(parts_lookup)
    
    with tab2:
        st.write("Enter a process code to look up the associated parts:")
        
        lookup_segment = st.selectbox("Segment", options=predefined_options['segment'], key="lookup_segment")
        lookup_process_code = st.text_input("Process Code", key="lookup_process_code")
        
        if st.button("Look Up Parts"):
            if not lookup_process_code:
                st.error("Please enter a process code")
            else:
                parts_lookup = lookup_parts_by_process_code(lookup_process_code, component_validations_df)
                if parts_lookup.startswith("No matching") or parts_lookup.startswith("Error"):
                    st.error(parts_lookup)
                else:
                    st.success(f"Found components for process code: {lookup_process_code}")
                    
                    explanation = explain_process_code(lookup_process_code, lookup_segment)
                    st.info(explanation)
                    
                    st.subheader("Component Details")
                    st.text(parts_lookup)
        
        st.write("---")
        
        st.write("Or build a part specification by selecting components:")
        
        part_spec = PartSpecification()
        
        part_segment = st.selectbox("Segment", options=predefined_options['segment'], key="part_segment")
        part_spec.set_segment(part_segment)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("PMIC")
            part_pmic_supplier = st.selectbox("Supplier", options=predefined_options['supplier'], key="part_pmic_supplier")
            part_pmic_gen = st.selectbox("Generation", options=predefined_options['component_generation'], key="part_pmic_gen")
            part_pmic_rev = st.selectbox("Revision", options=predefined_options['revision'], key="part_pmic_rev")
            
            pmic_code, _, _ = get_component_process_code(
                part_segment, part_pmic_supplier, part_pmic_gen, part_pmic_rev, "PMIC", component_validations_df
            )
            if isinstance(pmic_code, str) and not pmic_code.startswith("No matching") and not pmic_code.startswith("Error"):
                st.success(f"PMIC Process Code: {pmic_code}")
                part_spec.set_pmic(pmic_code)
            else:
                st.error(f"PMIC Process Code: {pmic_code}")
            
            st.subheader("Temp Sensor")
            part_temp_supplier = st.selectbox("Supplier", options=predefined_options['supplier'] + ["None"], key="part_temp_supplier")
            
            if part_temp_supplier != "None":
                part_temp_gen = st.selectbox("Generation", options=predefined_options['component_generation'], key="part_temp_gen")
                part_temp_rev = st.selectbox("Revision", options=predefined_options['revision'], key="part_temp_rev")
                
                temp_code, _, _ = get_component_process_code(
                    part_segment, part_temp_supplier, part_temp_gen, part_temp_rev, "Temp Sensor", component_validations_df
                )
                if isinstance(temp_code, str) and not temp_code.startswith("No matching") and not temp_code.startswith("Error"):
                    st.success(f"Temp Sensor Process Code: {temp_code}")
                    part_spec.set_temp_sensor(temp_code)
                else:
                    st.error(f"Temp Sensor Process Code: {temp_code}")
            
            st.subheader("Data Buffer")
            part_db_supplier = st.selectbox("Supplier", options=predefined_options['supplier'] + ["None"], key="part_db_supplier")
            
            if part_db_supplier != "None":
                part_db_gen = st.selectbox("Generation", options=predefined_options['component_generation'], key="part_db_gen")
                part_db_rev = st.selectbox("Revision", options=predefined_options['revision'], key="part_db_rev")
                
                db_code, _, _ = get_component_process_code(
                    part_segment, part_db_supplier, part_db_gen, part_db_rev, "Data Buffer", component_validations_df
                )
                if isinstance(db_code, str) and not db_code.startswith("No matching") and not db_code.startswith("Error"):
                    st.success(f"Data Buffer Process Code: {db_code}")
                    part_spec.set_data_buffer(db_code)
                else:
                    st.error(f"Data Buffer Process Code: {db_code}")
        
        with col2:
            st.subheader("SPD/Hub")
            part_spd_supplier = st.selectbox("Supplier", options=predefined_options['supplier'], key="part_spd_supplier")
            part_spd_gen = st.selectbox("Generation", options=predefined_options['component_generation'], key="part_spd_gen")
            part_spd_rev = st.selectbox("Revision", options=predefined_options['revision'], key="part_spd_rev")
            
            spd_code, _, _ = get_component_process_code(
                part_segment, part_spd_supplier, part_spd_gen, part_spd_rev, "SPD/Hub", component_validations_df
            )
            if isinstance(spd_code, str) and not spd_code.startswith("No matching") and not spd_code.startswith("Error"):
                st.success(f"SPD/Hub Process Code: {spd_code}")
                part_spec.set_spd_hub(spd_code)
            else:
                st.error(f"SPD/Hub Process Code: {spd_code}")
            
            st.subheader("RCD/MRCD")
            part_rcd_supplier = st.selectbox("Supplier", options=predefined_options['supplier'] + ["None"], key="part_rcd_supplier")
            
            if part_rcd_supplier != "None":
                part_rcd_gen = st.selectbox("Generation", options=predefined_options['component_generation'], key="part_rcd_gen")
                part_rcd_rev = st.selectbox("Revision", options=predefined_options['revision'], key="part_rcd_rev")
                
                rcd_code, _, _ = get_component_process_code(
                    part_segment, part_rcd_supplier, part_rcd_gen, part_rcd_rev, "RCD/MRCD", component_validations_df
                )
                if isinstance(rcd_code, str) and not rcd_code.startswith("No matching") and not rcd_code.startswith("Error"):
                    st.success(f"RCD/MRCD Process Code: {rcd_code}")
                    part_spec.set_rcd_mrcd(rcd_code)
                else:
                    st.error(f"RCD/MRCD Process Code: {rcd_code}")
        
        if st.button("Generate Part Specification"):
            process_code = get_module_process_code(
                part_spec.pmic, part_spec.spd_hub, part_spec.temp_sensor, 
                part_spec.rcd_mrcd, part_spec.data_buffer, part_spec.segment
            )
            
            if process_code.startswith("For server") or process_code.startswith("For client") or process_code.startswith("Unknown"):
                st.error(process_code)
            else:
                part_spec.set_process_code(process_code)
                
                st.success(f"Generated Process Code: {process_code}")
                
                explanation = explain_process_code(process_code, part_spec.segment)
                st.info(explanation)
                
                parts_lookup = lookup_parts_by_process_code(process_code, component_validations_df)
                if not parts_lookup.startswith("No matching") and not parts_lookup.startswith("Error"):
                    st.subheader("Component Details")
                    st.text(parts_lookup)
                    
                    part_spec.set_associated_parts(parts_lookup)
                    
                    st.download_button(
                        label="Download Part Specification",
                        data=f"""Part Specification
Segment: {part_spec.segment}
Process Code: {part_spec.process_code}

Component Breakdown:
PMIC: {part_spec.pmic}
SPD/Hub: {part_spec.spd_hub}
Temp Sensor: {part_spec.temp_sensor if part_spec.temp_sensor else 'N/A'}
RCD/MRCD: {part_spec.rcd_mrcd if part_spec.rcd_mrcd else 'N/A'}
Data Buffer: {part_spec.data_buffer if part_spec.data_buffer else 'N/A'}

Component Details:
{part_spec.associated_parts}
""",
                        file_name=f"part_spec_{process_code}_{pd.Timestamp.now().strftime('%Y%m%d')}.txt",
                        mime="text/plain",
                    )

if __name__ == "__main__":
    main()