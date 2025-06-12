import streamlit as st
import pandas as pd
import requests
from urllib.parse import urlparse, quote
import msal
from datetime import datetime

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
    
    parsed_url = urlparse(sharepoint_site)
    tenant = parsed_url.netloc.split('.')[0]
    site_path = parsed_url.path
    
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
        authority = f"https://login.microsoftonline.com/micron.com"
        scope = ["https://microncorp.sharepoint.com/.default"]
        
        client_id = "1fec8e78-bce4-4aaf-ab1b-5451cc387264"
        
        app = msal.PublicClientApplication(
            client_id=client_id,
            authority=authority
        )
        
        result = app.acquire_token_by_username_password(
            username=username,
            password=password,
            scopes=scope
        )
        
        if "access_token" not in result:
            error_msg = result.get('error_description', 'Unknown error')
            st.sidebar.error(f"Authentication failed: {error_msg}")
            
            if "AADSTS9001023" in error_msg:
                st.sidebar.info("Try using your organization's tenant ID instead of 'common' in the authority URL.")
            elif "AADSTS50126" in error_msg:
                st.sidebar.info("Invalid username or password. Please check your credentials.")
            elif "AADSTS50076" in error_msg or "AADSTS50079" in error_msg:
                st.sidebar.info("Multi-factor authentication (MFA) is required. Consider using a different authentication method.")
            
            with st.sidebar.expander("Detailed Error Information", expanded=False):
                st.write(result)
            
            return data
        
        access_token = result["access_token"]
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json;odata=verbose",
            "Content-Type": "application/json;odata=verbose"
        }
        
        site_url = f"https://{tenant}.sharepoint.com/_api/web/GetSiteByUrl('{quote(site_path)}')"
        response = requests.get(site_url, headers=headers)
        
        if response.status_code != 200:
            st.sidebar.error(f"Error accessing SharePoint site: {response.status_code}")
            return data
        
        site_info = response.json()
        site_id = site_info['d']['Id']
        
        st.sidebar.success(f"Connected to SharePoint site: {site_info['d']['Title']}")
        
        lists_url = f"https://{tenant}.sharepoint.com/_api/web/GetSiteByUrl('{quote(site_path)}')/lists"
        response = requests.get(lists_url, headers=headers)
        
        if response.status_code != 200:
            st.sidebar.error(f"Error getting SharePoint lists: {response.status_code}")
            return data
        
        lists_info = response.json()
        available_lists = [list_item['Title'] for list_item in lists_info['d']['results']]
        
        with st.sidebar.expander("Available SharePoint Lists", expanded=False):
            st.write(", ".join(available_lists))
        
        component_validations_list_name = None
        for list_name in ["Module HW Design Component Validations", "Component Validations", "HW Validation", 
                         "Components", "Parts", "Validations", "Hardware"]:
            if any(list_name.lower() in list_title.lower() for list_title in available_lists):
                matching_lists = [list_title for list_title in available_lists if list_name.lower() in list_title.lower()]
                component_validations_list_name = matching_lists[0]
                break
        
        if component_validations_list_name:
            st.sidebar.success(f"Found Component Validations list: {component_validations_list_name}")
            
            list_items_url = f"https://{tenant}.sharepoint.com/_api/web/GetSiteByUrl('{quote(site_path)}')/lists/GetByTitle('{quote(component_validations_list_name)}')/items?$top=5000"
            response = requests.get(list_items_url, headers=headers)
            
            if response.status_code != 200:
                st.sidebar.error(f"Error getting list items: {response.status_code}")
                return data
            
            items_info = response.json()
            items = items_info['d']['results']
            
            component_validations_data = []
            
            if len(items) > 0:
                with st.sidebar.expander(f"Sample {component_validations_list_name} Item Fields", expanded=False):
                    st.write(", ".join(items[0].keys()))
                
                for item in items:
                    segment_field = next((f for f in item.keys() 
                                       if any(term in f.lower() for term in ['segment', 'market', 'seg'])), None)
                    supplier_field = next((f for f in item.keys() 
                                        if any(term in f.lower() for term in ['supplier', 'vendor', 'manufacturer'])), None)
                    component_gen_field = next((f for f in item.keys() 
                                             if any(term in f.lower() for term in ['generation', 'gen', 'component gen'])), None)
                    revision_field = next((f for f in item.keys() 
                                        if any(term in f.lower() for term in ['revision', 'rev', 'version'])), None)
                    component_type_field = next((f for f in item.keys() 
                                              if any(term in f.lower() for term in ['component type', 'type', 'comp type'])), None)
                    process_code_field = next((f for f in item.keys() 
                                            if any(term in f.lower() for term in ['process code', 'code', 'pc'])), None)
                    mpn_field = next((f for f in item.keys() 
                                   if any(term in f.lower() for term in ['mpn', 'part number', 'part'])), None)
                    
                    if segment_field and supplier_field and component_gen_field and revision_field and component_type_field and process_code_field:
                        component_validations_data.append({
                            'Segment': str(item.get(segment_field, '')),
                            'Supplier': str(item.get(supplier_field, '')),
                            'Component_Generation': str(item.get(component_gen_field, '')),
                            'Revision': str(item.get(revision_field, '')),
                            'Component_Type': str(item.get(component_type_field, '')),
                            'Process_Code': str(item.get(process_code_field, '')),
                            'MPN': str(item.get(mpn_field, '')) if mpn_field else ''
                        })
            
            component_validations_df = pd.DataFrame(component_validations_data)
            data['component_validations_df'] = component_validations_df
            
            st.sidebar.success(f"Successfully loaded {len(component_validations_data)} component validations from SharePoint")
        else:
            st.sidebar.warning("Could not find Component Validations list")
        
        module_validation_list_name = None
        for list_name in ["Module HW Design Validation", "Module Validation", "Module HW Validation", 
                         "Module Design", "Module", "Design Validation"]:
            if any(list_name.lower() in list_title.lower() for list_title in available_lists):
                matching_lists = [list_title for list_title in available_lists if list_name.lower() in list_title.lower()]
                module_validation_list_name = matching_lists[0]
                break
        
        if module_validation_list_name:
            st.sidebar.success(f"Found Module Validation list: {module_validation_list_name}")
            
            list_items_url = f"https://{tenant}.sharepoint.com/_api/web/GetSiteByUrl('{quote(site_path)}')/lists/GetByTitle('{quote(module_validation_list_name)}')/items?$top=5000"
            response = requests.get(list_items_url, headers=headers)
            
            if response.status_code != 200:
                st.sidebar.error(f"Error getting list items: {response.status_code}")
                return data
            
            items_info = response.json()
            items = items_info['d']['results']
            
            module_validation_data = []
            
            if len(items) > 0:
                with st.sidebar.expander(f"Sample {module_validation_list_name} Item Fields", expanded=False):
                    st.write(", ".join(items[0].keys()))
                
                for item in items:
                    segment_field = next((f for f in item.keys() 
                                       if any(term in f.lower() for term in ['segment', 'market', 'seg'])), None)
                    form_factor_field = next((f for f in item.keys() 
                                           if any(term in f.lower() for term in ['form factor', 'form', 'ff'])), None)
                    speed_field = next((f for f in item.keys() 
                                     if any(term in f.lower() for term in ['speed', 'spd', 'bin'])), None)
                    pmic_field = next((f for f in item.keys() 
                                    if any(term in f.lower() for term in ['pmic', 'power'])), None)
                    spd_hub_field = next((f for f in item.keys() 
                                       if any(term in f.lower() for term in ['spd', 'hub', 'spd/hub'])), None)
                    temp_sensor_field = next((f for f in item.keys() 
                                           if any(term in f.lower() for term in ['temp', 'sensor', 'temperature'])), None)
                    rcd_field = next((f for f in item.keys() 
                                   if any(term in f.lower() for term in ['rcd', 'mrcd', 'register'])), None)
                    data_buffer_field = next((f for f in item.keys() 
                                           if any(term in f.lower() for term in ['data buffer', 'buffer', 'db'])), None)
                    process_code_field = next((f for f in item.keys() 
                                            if any(term in f.lower() for term in ['process code', 'code', 'pc'])), None)
                    
                    if segment_field and form_factor_field and speed_field and process_code_field:
                        module_validation_data.append({
                            'Segment': str(item.get(segment_field, '')),
                            'Form_Factor': str(item.get(form_factor_field, '')),
                            'Speed': str(item.get(speed_field, '')),
                            'PMIC': str(item.get(pmic_field, '')) if pmic_field else '',
                            'SPD_Hub': str(item.get(spd_hub_field, '')) if spd_hub_field else '',
                            'Temp_Sensor': str(item.get(temp_sensor_field, '')) if temp_sensor_field else '',
                            'RCD_MRCD': str(item.get(rcd_field, '')) if rcd_field else '',
                            'Data_Buffer': str(item.get(data_buffer_field, '')) if data_buffer_field else '',
                            'Process_Code': str(item.get(process_code_field, ''))
                        })
            
            module_validation_df = pd.DataFrame(module_validation_data)
            data['module_validation_df'] = module_validation_df
            
            st.sidebar.success(f"Successfully loaded {len(module_validation_data)} module validations from SharePoint")
        else:
            st.sidebar.warning("Could not find Module Validation list")
        
    except Exception as e:
        st.sidebar.error(f"Error connecting to SharePoint: {str(e)}")
    
    return data

def get_component_process_code(segment, supplier, component_gen, revision, component_validations_df):
    try:
        filtered_df = component_validations_df[
            (component_validations_df['Segment'].str.lower() == segment.lower()) & 
            (component_validations_df['Supplier'].str.lower() == supplier.lower()) & 
            (component_validations_df['Component_Generation'].str.lower() == component_gen.lower()) & 
            (component_validations_df['Revision'].str.lower() == revision.lower())
        ]
        
        if filtered_df.empty:
            return "No matching process code found for the given criteria", None, None
        
        process_code = filtered_df.iloc[0]['Process_Code']
        component_type = filtered_df.iloc[0]['Component_Type']
        
        return process_code, component_type, filtered_df
    
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
            return "Unknown segment. Cannot generate process code."
    
    except Exception as e:
        return f"Error generating module process code: {e}"

def lookup_parts_by_process_code(process_code, component_validations_df):
    try:
        if not process_code:
            return "No process code provided"
        
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
                        'Component_Type': row['Component_Type'],
                        'Supplier': row['Supplier'],
                        'Component_Generation': row['Component_Generation'],
                        'Revision': row['Revision'],
                        'MPN': row['MPN']
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

def get_predefined_options():
    options = {
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
    return options

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
    
    predefined_options = get_predefined_options()
    
    component_validations_df = data['component_validations_df']
    module_validation_df = data['module_validation_df']
    
    st.sidebar.info(f"Data last refreshed: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = "process_code"
    
    part = PartSpecification()
    
    tab1, tab2 = st.tabs(["Process Code Generator", "Part Specification Generator"])
    
    with tab1:
        subtab1, subtab2 = st.tabs(["Component", "Module"])
        
        with subtab1:
            st.write("Enter the component details to generate a process code:")
            
            selected_segment = st.selectbox("Segment", options=predefined_options['segment'], key="segment_component")
            
            selected_supplier = st.selectbox("Supplier", options=predefined_options['supplier'], key="supplier_component")
            
            selected_component_gen = st.selectbox("Component Generation", options=predefined_options['component_generation'], key="component_gen")
            
            selected_revision = st.selectbox("Revision", options=predefined_options['revision'], key="revision_component")
            
            if st.button("Generate Component Process Code"):
                st.session_state.active_tab = "component_process_code"
                
                part.set_segment(selected_segment)
                part.set_supplier(selected_supplier)
                part.set_component_gen(selected_component_gen)
                part.set_revision(selected_revision)
                
                process_code, component_type, code_details = get_component_process_code(
                    selected_segment, selected_supplier, selected_component_gen, selected_revision, component_validations_df
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
            st.write("Enter the module component details to generate a combined process code:")
            
            st.subheader("PMIC")
            pmic_segment = st.selectbox("Segment", options=predefined_options['segment'], key="pmic_segment")
            pmic_supplier = st.selectbox("Supplier", options=predefined_options['supplier'], key="pmic_supplier")
            pmic_gen = st.selectbox("Component Generation", options=predefined_options['component_generation'], key="pmic_gen")
            pmic_rev = st.selectbox("Revision", options=predefined_options['revision'], key="pmic_rev")
            
            pmic_code, _, _ = get_component_process_code(
                pmic_segment, pmic_supplier, pmic_gen, pmic_rev, component_validations_df
            )
            if isinstance(pmic_code, str) and not pmic_code.startswith("No matching") and not pmic_code.startswith("Error"):
                st.success(f"PMIC Process Code: {pmic_code}")
            else:
                st.error(f"PMIC Process Code: {pmic_code}")
            
            st.subheader("SPD/Hub")
            spd_segment = st.selectbox("Segment", options=predefined_options['segment'], key="spd_segment")
            spd_supplier = st.selectbox("Supplier", options=predefined_options['supplier'], key="spd_supplier")
            spd_gen = st.selectbox("Component Generation", options=predefined_options['component_generation'], key="spd_gen")
            spd_rev = st.selectbox("Revision", options=predefined_options['revision'], key="spd_rev")
            
            spd_code, _, _ = get_component_process_code(
                spd_segment, spd_supplier, spd_gen, spd_rev, component_validations_df
            )
            if isinstance(spd_code, str) and not spd_code.startswith("No matching") and not spd_code.startswith("Error"):
                st.success(f"SPD/Hub Process Code: {spd_code}")
            else:
                st.error(f"SPD/Hub Process Code: {spd_code}")
            
            st.subheader("Temp Sensor")
            temp_segment = st.selectbox("Segment", options=predefined_options['segment'], key="temp_segment")
            temp_supplier_options = predefined_options['supplier'] + ["None"]
            temp_supplier = st.selectbox("Supplier", options=temp_supplier_options, key="temp_supplier")
            
            if temp_supplier != "None":
                temp_gen = st.selectbox("Component Generation", options=predefined_options['component_generation'], key="temp_gen")
                temp_rev = st.selectbox("Revision", options=predefined_options['revision'], key="temp_rev")
                
                temp_code, _, _ = get_component_process_code(
                    temp_segment, temp_supplier, temp_gen, temp_rev, component_validations_df
                )
                if isinstance(temp_code, str) and not temp_code.startswith("No matching") and not temp_code.startswith("Error"):
                    st.success(f"Temp Sensor Process Code: {temp_code}")
                else:
                    st.error(f"Temp Sensor Process Code: {temp_code}")
            else:
                temp_code = ""
            
            st.subheader("RCD/MRCD")
            rcd_segment = st.selectbox("Segment", options=predefined_options['segment'], key="rcd_segment")
            rcd_supplier = st.selectbox("Supplier", options=predefined_options['supplier'], key="rcd_supplier")
            rcd_gen = st.selectbox("Component Generation", options=predefined_options['component_generation'], key="rcd_gen")
            rcd_rev = st.selectbox("Revision", options=predefined_options['revision'], key="rcd_rev")
            
            rcd_code, _, _ = get_component_process_code(
                rcd_segment, rcd_supplier, rcd_gen, rcd_rev, component_validations_df
            )
            if isinstance(rcd_code, str) and not rcd_code.startswith("No matching") and not rcd_code.startswith("Error"):
                st.success(f"RCD/MRCD Process Code: {rcd_code}")
            else:
                st.error(f"RCD/MRCD Process Code: {rcd_code}")
            
            st.subheader("Data Buffer (Optional)")
            db_segment = st.selectbox("Segment", options=predefined_options['segment'], key="db_segment")
            db_supplier_options = predefined_options['supplier'] + ["None"]
            db_supplier = st.selectbox("Supplier", options=db_supplier_options, key="db_supplier")
            
            if db_supplier != "None":
                db_gen = st.selectbox("Component Generation", options=predefined_options['component_generation'], key="db_gen")
                db_rev = st.selectbox("Revision", options=predefined_options['revision'], key="db_rev")
                
                db_code, _, _ = get_component_process_code(
                    db_segment, db_supplier, db_gen, db_rev, component_validations_df
                )
                if isinstance(db_code, str) and not db_code.startswith("No matching") and not db_code.startswith("Error"):
                    st.success(f"Data Buffer Process Code: {db_code}")
                else:
                    st.error(f"Data Buffer Process Code: {db_code}")
            else:
                db_code = ""
            
            if st.button("Generate Module Process Code"):
                st.session_state.active_tab = "module_process_code"
                
                # Use the segment from PMIC as the overall module segment
                selected_segment = pmic_segment
                part.set_segment(selected_segment)
                
                part.set_pmic(pmic_code if isinstance(pmic_code, str) and not pmic_code.startswith("No matching") and not pmic_code.startswith("Error") else "")
                part.set_spd_hub(spd_code if isinstance(spd_code, str) and not spd_code.startswith("No matching") and not spd_code.startswith("Error") else "")
                part.set_temp_sensor(temp_code if isinstance(temp_code, str) and not temp_code.startswith("No matching") and not temp_code.startswith("Error") else "")
                part.set_rcd_mrcd(rcd_code if isinstance(rcd_code, str) and not rcd_code.startswith("No matching") and not rcd_code.startswith("Error") else "")
                part.set_data_buffer(db_code if 'db_code' in locals() and isinstance(db_code, str) and not db_code.startswith("No matching") and not db_code.startswith("Error") else "")
                
                combined_code = get_module_process_code(
                    part.pmic, part.spd_hub, part.temp_sensor, 
                    part.rcd_mrcd, part.data_buffer,
                    selected_segment
                )
                
                if isinstance(combined_code, str) and not combined_code.startswith("For") and not combined_code.startswith("Error") and not combined_code.startswith("Unknown"):
                    st.session_state.process_code_explanation = explain_process_code(combined_code, selected_segment)
                    result_text = f"Generated Module Process Code: {combined_code}"
                else:
                    result_text = combined_code
                
                st.session_state.result = result_text
                st.session_state.show_result = True
                
                if isinstance(combined_code, str) and not combined_code.startswith("For") and not combined_code.startswith("Error") and not combined_code.startswith("Unknown"):
                    synthetic_data = {
                        'Segment': [selected_segment],
                        'PMIC': [part.pmic],
                        'SPD_Hub': [part.spd_hub],
                        'Temp_Sensor': [part.temp_sensor],
                        'RCD_MRCD': [part.rcd_mrcd],
                        'Data_Buffer': [part.data_buffer],
                        'Process_Code': [combined_code]
                    }
                    st.session_state.code_details = pd.DataFrame(synthetic_data)
                else:
                    st.session_state.code_details = None
    
    with tab2:
        st.write("Enter a process code to look up the associated parts:")
        
        process_code = st.text_input("Process Code", key="pc_lookup")
        
        if st.button("Look Up Parts"):
            st.session_state.active_tab = "part_spec"
            
            if not process_code:
                st.error("Please enter a process code")
            else:
                part.set_process_code(process_code)
                
                parts_result = lookup_parts_by_process_code(process_code, component_validations_df)
                part.set_associated_parts(parts_result)
                
                st.session_state.result = f"Parts for Process Code {process_code}:\n\n{parts_result}"
                st.session_state.show_result = True
                
                if not parts_result.startswith("No matching") and not parts_result.startswith("Error"):
                    segment = "Server" if len(process_code) >= 4 else "Client"
                    st.session_state.process_code_explanation = explain_process_code(process_code, segment)
    
    if 'show_result' not in st.session_state:
        st.session_state.show_result = False
        st.session_state.result = ""
    
    if st.session_state.show_result:
        st.header("Result")
        st.text_area("Specification", st.session_state.result, height=300)
        
        if 'process_code_explanation' in st.session_state:
            st.subheader("Process Code Explanation")
            st.text_area("Breakdown", st.session_state.process_code_explanation, height=250)
        
        if 'code_details' in st.session_state and st.session_state.code_details is not None:
            st.subheader("Process Code Details")
            st.dataframe(st.session_state.code_details)
        
        if st.button("Clear and Start Over"):
            st.session_state.show_result = False
            st.session_state.result = ""
            if 'code_details' in st.session_state:
                st.session_state.code_details = None
            if 'process_code_explanation' in st.session_state:
                del st.session_state.process_code_explanation
            st.rerun()

if __name__ == "__main__":
    main()