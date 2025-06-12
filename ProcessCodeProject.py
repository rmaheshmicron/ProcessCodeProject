import streamlit as st
import pandas as pd
import re
import os
from pathlib import Path
import tempfile
import msal
import requests
from urllib.parse import urlparse, quote

class PartSpecification:
    def __init__(self):
        self.segment = None
        self.supplier = None
        self.component_gen = None
        self.revision = None
        self.pmic = None
        self.spd_hub = None
        self.temp_sensor = None
        self.rcd_mrcd = None
        self.data_buffer = None
        self.process_code = None
        self.component_type = None
        self.generated_process_code = None
        self.associated_parts = None
    
    def set_segment(self, value):
        self.segment = value
        return self
    
    def set_supplier(self, value):
        self.supplier = value
        return self
    
    def set_component_gen(self, value):
        self.component_gen = value
        return self
    
    def set_revision(self, value):
        self.revision = value
        return self
    
    def set_pmic(self, value):
        self.pmic = value
        return self
    
    def set_spd_hub(self, value):
        self.spd_hub = value
        return self
    
    def set_temp_sensor(self, value):
        self.temp_sensor = value
        return self
    
    def set_rcd_mrcd(self, value):
        self.rcd_mrcd = value
        return self
    
    def set_data_buffer(self, value):
        self.data_buffer = value
        return self
    
    def set_process_code(self, value):
        self.process_code = value
        return self
    
    def set_component_type(self, value):
        self.component_type = value
        return self
    
    def set_generated_process_code(self, value):
        self.generated_process_code = value
        return self
    
    def set_associated_parts(self, value):
        self.associated_parts = value
        return self
    
    def __str__(self):
        result = []
        if self.segment:
            result.append(f"Segment: {self.segment}")
        if self.supplier:
            result.append(f"Supplier: {self.supplier}")
        if self.component_gen:
            result.append(f"Component Generation: {self.component_gen}")
        if self.revision:
            result.append(f"Revision: {self.revision}")
        if self.pmic:
            result.append(f"PMIC: {self.pmic}")
        if self.spd_hub:
            result.append(f"SPD/Hub: {self.spd_hub}")
        if self.temp_sensor:
            result.append(f"Temp Sensor: {self.temp_sensor}")
        if self.rcd_mrcd:
            result.append(f"RCD/MRCD: {self.rcd_mrcd}")
        if self.data_buffer:
            result.append(f"Data Buffer: {self.data_buffer}")
        if self.process_code:
            result.append(f"Process Code: {self.process_code}")
        if self.component_type:
            result.append(f"Component Type: {self.component_type}")
        if self.generated_process_code:
            result.append(f"Generated Process Code: {self.generated_process_code}")
        if self.associated_parts:
            result.append(f"Associated Parts:\n{self.associated_parts}")
        
        return "\n".join(result)


def show_process_code_info():
    """Display information about process code structure"""
    with st.expander("Process Code Information", expanded=False):
        st.markdown("""
        ### Process Code Structure
        
        The "Process Code" is a mechanism for BOM segregation and manufacturing purposes with each character representing a specific non-DRAM Active Component.
        
        #### D5 Server Process Code Structure
        
        | Component | Position in PROCESS_CODE | Position in PROCESS_CODE_PRINT |
        | --- | --- | --- |
        | PMIC | 1 | 1 |
        | SPD/Hub | 2 | 3 |
        | Temp Sensor | 3 | 4 |
        | RCD / MRCD | 4 | 2 |
        | Data Buffer (if applicable) | 5 | 5 |
        
        **D5 Server PROCESS CODE:** PMIC → SPD/Hub → Temp Sensor → RCD → Data Buffer (if applicable)
        
        **D5 Server PROCESS CODE PRINT:** PMIC → RCD → SPD/Hub → Temp Sensor → Data Buffer (if applicable)
        
        #### D5 Client Process Code Structure
        
        | Component | Position in PROCESS_CODE | Position in PROCESS_CODE_PRINT |
        | --- | --- | --- |
        | PMIC | 1 | 1 |
        | SPD/Hub | 2 | 2 |
        | CKD (if applicable) | 3 | 3 |
        
        **D5 Client PROCESS CODE and PROCESS CODE PRINT:** PMIC → SPD/Hub → CKD (if applicable)
        
        *Note: This view contains only components WITH an assigned PROCESS CODE character.
        """)


@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_data_from_sharepoint():
    """Load data directly from SharePoint lists using MSAL authentication"""
    data = {
        'component_validations_df': pd.DataFrame(),
        'module_validation_df': pd.DataFrame()
    }
    
    # SharePoint connection settings
    sharepoint_site = "https://microncorp.sharepoint.com/sites/mdg"
    
    # Parse the SharePoint URL to get the tenant and site
    parsed_url = urlparse(sharepoint_site)
    tenant = parsed_url.netloc.split('.')[0]
    site_path = parsed_url.path
    
    # Get credentials from secrets or sidebar inputs
    if "sharepoint_username" in st.secrets and "sharepoint_password" in st.secrets:
        username = st.secrets["sharepoint_username"]
        password = st.secrets["sharepoint_password"]
    else:
        # For security, in a real application you should use Streamlit secrets
        # instead of collecting credentials in the UI
        st.sidebar.subheader("SharePoint Authentication")
        username = st.sidebar.text_input("SharePoint Username (include @micron.com)", key="sp_username")
        password = st.sidebar.text_input("SharePoint Password", type="password", key="sp_password")
    
    # Check if we have the required credentials
    if not (username and password):
        st.sidebar.warning("Please provide SharePoint credentials to load data.")
        return data
    
    try:
        # Set up MSAL authentication
        # Change from /common to /organizations endpoint
        authority = f"https://login.microsoftonline.com/{tenant}.onmicrosoft.com"
        scope = [f"https://{tenant}.sharepoint.com/.default"]
        
        # Client ID for Microsoft Office (this is a well-known client ID for Office applications)
        client_id = "1fec8e78-bce4-4aaf-ab1b-5451cc387264"  # Microsoft Office client ID
        
        # Create the MSAL app
        app = msal.PublicClientApplication(
            client_id=client_id,
            authority=authority
        )
        
        # Acquire token using username/password flow
        result = app.acquire_token_by_username_password(
            username=username,
            password=password,
            scopes=scope
        )
        
        if "access_token" not in result:
            st.sidebar.error(f"Authentication failed: {result.get('error_description', 'Unknown error')}")
            return data
        
        access_token = result["access_token"]
        
        # Set up headers for SharePoint REST API calls
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json;odata=verbose",
            "Content-Type": "application/json;odata=verbose"
        }
        
        # Get SharePoint site ID
        site_url = f"https://{tenant}.sharepoint.com/_api/web/GetSiteByUrl('{quote(site_path)}')"
        response = requests.get(site_url, headers=headers)
        
        if response.status_code != 200:
            st.sidebar.error(f"Error accessing SharePoint site: {response.status_code}")
            return data
        
        site_info = response.json()
        site_id = site_info['d']['Id']
        
        st.sidebar.success(f"Connected to SharePoint site: {site_info['d']['Title']}")
        
        # Get all lists in the site
        lists_url = f"https://{tenant}.sharepoint.com/_api/web/GetSiteByUrl('{quote(site_path)}')/lists"
        response = requests.get(lists_url, headers=headers)
        
        if response.status_code != 200:
            st.sidebar.error(f"Error getting SharePoint lists: {response.status_code}")
            return data
        
        lists_info = response.json()
        available_lists = [list_item['Title'] for list_item in lists_info['d']['results']]
        
        # For debugging, show available lists in the sidebar
        with st.sidebar.expander("Available SharePoint Lists", expanded=False):
            st.write(", ".join(available_lists))
        
        # Try to find Module HW Design Component Validations list
        component_validations_list_name = None
        for list_name in ["Module HW Design Component Validations", "Component Validations", "HW Validation", 
                         "Components", "Parts", "Validations", "Hardware"]:
            if any(list_name.lower() in list_title.lower() for list_title in available_lists):
                matching_lists = [list_title for list_title in available_lists if list_name.lower() in list_title.lower()]
                component_validations_list_name = matching_lists[0]  # Take the first matching list
                break
        
        if component_validations_list_name:
            st.sidebar.success(f"Found Component Validations list: {component_validations_list_name}")
            
            # Get list items using REST API
            list_items_url = f"https://{tenant}.sharepoint.com/_api/web/GetSiteByUrl('{quote(site_path)}')/lists/GetByTitle('{quote(component_validations_list_name)}')/items?$top=5000"
            response = requests.get(list_items_url, headers=headers)
            
            if response.status_code != 200:
                st.sidebar.error(f"Error getting list items: {response.status_code}")
                return data
            
            items_info = response.json()
            items = items_info['d']['results']
            
            # Convert to DataFrame for component validations data
            component_validations_data = []
            
            if len(items) > 0:
                # For debugging, print the first item's properties
                with st.sidebar.expander(f"Sample {component_validations_list_name} Item Fields", expanded=False):
                    st.write(", ".join(items[0].keys()))
                
                for item in items:
                    # Extract component validation data - adjust field names if needed
                    # Look for fields that might contain the required information
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
                    
                    # Only add if we have the minimum required fields
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
        
        # Try to find Module HW Design Validation list
        module_validation_list_name = None
        for list_name in ["Module HW Design Validation", "Module Validation", "Module HW Validation", 
                         "Module Design", "Module", "Design Validation"]:
            if any(list_name.lower() in list_title.lower() for list_title in available_lists):
                matching_lists = [list_title for list_title in available_lists if list_name.lower() in list_title.lower()]
                module_validation_list_name = matching_lists[0]  # Take the first matching list
                break
        
        if module_validation_list_name:
            st.sidebar.success(f"Found Module Validation list: {module_validation_list_name}")
            
            # Get list items using REST API
            list_items_url = f"https://{tenant}.sharepoint.com/_api/web/GetSiteByUrl('{quote(site_path)}')/lists/GetByTitle('{quote(module_validation_list_name)}')/items?$top=5000"
            response = requests.get(list_items_url, headers=headers)
            
            if response.status_code != 200:
                st.sidebar.error(f"Error getting list items: {response.status_code}")
                return data
            
            items_info = response.json()
            items = items_info['d']['results']
            
            # Convert to DataFrame for module validation data
            module_validation_data = []
            
            if len(items) > 0:
                # For debugging, print the first item's properties
                with st.sidebar.expander(f"Sample {module_validation_list_name} Item Fields", expanded=False):
                    st.write(", ".join(items[0].keys()))
                
                for item in items:
                    # Extract module validation data - adjust field names if needed
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
                    
                    # Only add if we have the minimum required fields
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
    """Get process code for a component based on segment, supplier, component generation, and revision"""
    try:
        # Filter the dataframe based on the inputs
        filtered_df = component_validations_df[
            (component_validations_df['Segment'].str.lower() == segment.lower()) & 
            (component_validations_df['Supplier'].str.lower() == supplier.lower()) & 
            (component_validations_df['Component_Generation'].str.lower() == component_gen.lower()) & 
            (component_validations_df['Revision'].str.lower() == revision.lower())
        ]
        
        if filtered_df.empty:
            return "No matching process code found for the given criteria", None, None
        
        # Get the process code from the filtered dataframe
        process_code = filtered_df.iloc[0]['Process_Code']
        component_type = filtered_df.iloc[0]['Component_Type']
        
        # Return the process code, component type, and the filtered dataframe for display
        return process_code, component_type, filtered_df
    
    except Exception as e:
        return f"Error generating process code: {e}", None, None


def get_module_process_code(pmic, spd_hub, temp_sensor, rcd_mrcd, data_buffer, segment):
    """Combine individual component process codes into a module process code"""
    try:
        # For server, combine all 5 components
        if segment.lower() == 'server':
            if not pmic or not spd_hub or not temp_sensor or not rcd_mrcd:
                return "For server modules, PMIC, SPD/Hub, Temp Sensor, and RCD/MRCD are required"
            
            # Data buffer is optional
            if data_buffer:
                return f"{pmic}{spd_hub}{temp_sensor}{rcd_mrcd}{data_buffer}"
            else:
                return f"{pmic}{spd_hub}{temp_sensor}{rcd_mrcd}"
        
        # For client, combine PMIC and SPD/Hub (CKD is optional)
        elif segment.lower() == 'client':
            if not pmic or not spd_hub:
                return "For client modules, PMIC and SPD/Hub are required"
            
            # Temp sensor is treated as CKD for client
            if temp_sensor:
                return f"{pmic}{spd_hub}{temp_sensor}"
            else:
                return f"{pmic}{spd_hub}"
        
        else:
            return "Unknown segment. Cannot generate process code."
    
    except Exception as e:
        return f"Error generating module process code: {e}"


def lookup_parts_by_process_code(process_code, component_validations_df):
    """Look up parts based on a complete process code"""
    try:
        if not process_code:
            return "No process code provided"
        
        # Extract individual component codes from the process code
        component_codes = list(process_code)
        
        # Create a result dataframe to store all matching parts
        result_parts = []
        
        # For each character in the process code, find the matching component
        for i, code in enumerate(component_codes):
            # Filter the dataframe based on the process code character
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
        
        # Convert to DataFrame and sort by position
        result_df = pd.DataFrame(result_parts)
        result_df = result_df.sort_values('Position')
        
        # Format the results as a string
        result = result_df.to_string(index=False)
        return result
    
    except Exception as e:
        return f"Error looking up parts: {e}"


def explain_process_code(process_code, segment):
    """Explain the meaning of each character in the process code"""
    if not process_code or not isinstance(process_code, str):
        return "Invalid process code"
    
    explanation = []
    explanation.append(f"Process Code: {process_code}")
    explanation.append("Component Breakdown:")
    
    if segment.lower() == 'server':
        # Server process code explanation
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
        # Client process code explanation
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


def main():
    st.title("Process Code & Part Specification Generator")
    
    # Show process code information
    show_process_code_info()
    
    # Add data source information in sidebar
    st.sidebar.header("Data Source")
    st.sidebar.info("Data is being loaded from SharePoint lists")
    
    # Add manual refresh button
    if st.sidebar.button("Refresh Data"):
        # Clear the cache to force data reload
        load_data_from_sharepoint.clear()
        st.rerun()
    
    # Load data from SharePoint
    data = load_data_from_sharepoint()
    
    component_validations_df = data['component_validations_df']
    module_validation_df = data['module_validation_df']
    
    # Check if data is loaded
    if component_validations_df.empty or module_validation_df.empty:
        st.error("Failed to load data from SharePoint. Please check your credentials and try again.")
        st.stop()
    
    # Display data refresh time
    st.sidebar.info(f"Data last refreshed: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Track which tab is active
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = "process_code"
    
    part = PartSpecification()
    
    tab1, tab2 = st.tabs(["Process Code Generator", "Part Specification Generator"])
    
    with tab1:
        # Create subtabs for Component and Module
        subtab1, subtab2 = st.tabs(["Component", "Module"])
        
        with subtab1:
            st.write("Enter the component details to generate a process code:")
            
            # Get unique segments from the data
            try:
                segment_options = component_validations_df['Segment'].unique().tolist()
                segment_options = [str(x) for x in segment_options if x is not None and str(x).strip()]
                segment_options = sorted(segment_options) if segment_options else ["Client", "Server"]
            except (KeyError, AttributeError, ValueError, TypeError) as e:
                st.warning(f"Could not load Segment options: {e}")
                segment_options = ["Client", "Server"]
                
            selected_segment = st.selectbox("Segment", options=segment_options, key="segment_component")
            
            # Get unique suppliers from the data
            try:
                supplier_options = component_validations_df['Supplier'].unique().tolist()
                supplier_options = [str(x) for x in supplier_options if x is not None and str(x).strip()]
                supplier_options = sorted(supplier_options) if supplier_options else ["Renesas", "IDT", "Maxim", "Montage", "Rambus", "TI", "Infineon", "Microchip"]
            except (KeyError, AttributeError, ValueError, TypeError) as e:
                st.warning(f"Could not load Supplier options: {e}")
                supplier_options = ["Renesas", "IDT", "Maxim", "Montage", "Rambus", "TI", "Infineon", "Microchip"]
                
            selected_supplier = st.selectbox("Supplier", options=supplier_options, key="supplier_component")
            
            # Get unique component generations from the data
            try:
                component_gen_options = component_validations_df['Component_Generation'].unique().tolist()
                component_gen_options = [str(x) for x in component_gen_options if x is not None and str(x).strip()]
                component_gen_options = sorted(component_gen_options) if component_gen_options else ["Gen1", "Gen2", "Gen3"]
            except (KeyError, AttributeError, ValueError, TypeError) as e:
                st.warning(f"Could not load Component Generation options: {e}")
                component_gen_options = ["Gen1", "Gen2", "Gen3"]
                
            selected_component_gen = st.selectbox("Component Generation", options=component_gen_options, key="component_gen")
            
            # Get unique revisions from the data
            try:
                revision_options = component_validations_df['Revision'].unique().tolist()
                revision_options = [str(x) for x in revision_options if x is not None and str(x).strip()]
                revision_options = sorted(revision_options) if revision_options else ["A", "B", "C"]
            except (KeyError, AttributeError, ValueError, TypeError) as e:
                st.warning(f"Could not load Revision options: {e}")
                revision_options = ["A", "B", "C"]
                
            selected_revision = st.selectbox("Revision", options=revision_options, key="revision_component")
            
            if st.button("Generate Component Process Code"):
                st.session_state.active_tab = "component_process_code"
                
                part.set_segment(selected_segment)
                part.set_supplier(selected_supplier)
                part.set_component_gen(selected_component_gen)
                part.set_revision(selected_revision)
                
                # Generate process code for the component
                process_code, component_type, code_details = get_component_process_code(
                    selected_segment, selected_supplier, selected_component_gen, selected_revision, component_validations_df
                )
                
                if isinstance(process_code, str) and not process_code.startswith("No matching") and not process_code.startswith("Error"):
                    result_text = f"Generated Process Code: {process_code}\nComponent Type: {component_type}"
                else:
                    result_text = process_code  # Error message
                
                st.session_state.result = result_text
                st.session_state.show_result = True
                
                # If we have details to show in a table
                if code_details is not None and not code_details.empty:
                    st.session_state.code_details = code_details
                else:
                    st.session_state.code_details = None
        
        with subtab2:
            st.write("Enter the module component details to generate a combined process code:")
            
            # Get unique segments from the data
            try:
                segment_options = module_validation_df['Segment'].unique().tolist()
                segment_options = [str(x) for x in segment_options if x is not None and str(x).strip()]
                segment_options = sorted(segment_options) if segment_options else ["Client", "Server"]
            except (KeyError, AttributeError, ValueError, TypeError) as e:
                st.warning(f"Could not load Segment options: {e}")
                segment_options = ["Client", "Server"]
                
            selected_segment = st.selectbox("Segment", options=segment_options, key="segment_module")
            
            # Create columns for better layout
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("PMIC")
                
                # Get unique suppliers for PMIC
                try:
                    pmic_supplier_options = component_validations_df[
                        component_validations_df['Component_Type'] == 'PMIC'
                    ]['Supplier'].unique().tolist()
                    pmic_supplier_options = [str(x) for x in pmic_supplier_options if x is not None and str(x).strip()]
                    pmic_supplier_options = sorted(pmic_supplier_options) if pmic_supplier_options else ["Renesas", "TI", "Infineon"]
                except (KeyError, AttributeError, ValueError, TypeError) as e:
                    pmic_supplier_options = ["Renesas", "TI", "Infineon"]
                    
                pmic_supplier = st.selectbox("Supplier", options=pmic_supplier_options, key="pmic_supplier")
                
                # Get unique component generations for PMIC
                try:
                    pmic_gen_options = component_validations_df[
                        (component_validations_df['Component_Type'] == 'PMIC') & 
                        (component_validations_df['Supplier'] == pmic_supplier)
                    ]['Component_Generation'].unique().tolist()
                    pmic_gen_options = [str(x) for x in pmic_gen_options if x is not None and str(x).strip()]
                    pmic_gen_options = sorted(pmic_gen_options) if pmic_gen_options else ["Gen1", "Gen2", "Gen3"]
                except (KeyError, AttributeError, ValueError, TypeError) as e:
                    pmic_gen_options = ["Gen1", "Gen2", "Gen3"]
                    
                pmic_gen = st.selectbox("Component Generation", options=pmic_gen_options, key="pmic_gen")
                
                # Get unique revisions for PMIC
                try:
                    pmic_rev_options = component_validations_df[
                        (component_validations_df['Component_Type'] == 'PMIC') & 
                        (component_validations_df['Supplier'] == pmic_supplier) & 
                        (component_validations_df['Component_Generation'] == pmic_gen)
                    ]['Revision'].unique().tolist()
                    pmic_rev_options = [str(x) for x in pmic_rev_options if x is not None and str(x).strip()]
                    pmic_rev_options = sorted(pmic_rev_options) if pmic_rev_options else ["A", "B", "C"]
                except (KeyError, AttributeError, ValueError, TypeError) as e:
                    pmic_rev_options = ["A", "B", "C"]
                    
                pmic_rev = st.selectbox("Revision", options=pmic_rev_options, key="pmic_rev")
                
                # Get PMIC process code
                pmic_code, _, _ = get_component_process_code(
                    selected_segment, pmic_supplier, pmic_gen, pmic_rev, component_validations_df
                )
                if isinstance(pmic_code, str) and not pmic_code.startswith("No matching") and not pmic_code.startswith("Error"):
                    st.success(f"PMIC Process Code: {pmic_code}")
                else:
                    st.error(f"PMIC Process Code: {pmic_code}")
                
                st.subheader("Temp Sensor")
                
                # Get unique suppliers for Temp Sensor
                try:
                    temp_supplier_options = component_validations_df[
                        component_validations_df['Component_Type'] == 'Temp Sensor'
                    ]['Supplier'].unique().tolist()
                    temp_supplier_options = [str(x) for x in temp_supplier_options if x is not None and str(x).strip()]
                    temp_supplier_options = sorted(temp_supplier_options) + ["None"] if temp_supplier_options else ["Maxim", "Microchip", "None"]
                except (KeyError, AttributeError, ValueError, TypeError) as e:
                    temp_supplier_options = ["Maxim", "Microchip", "None"]
                    
                temp_supplier = st.selectbox("Supplier", options=temp_supplier_options, key="temp_supplier")
                
                if temp_supplier != "None":
                    # Get unique component generations for Temp Sensor
                    try:
                        temp_gen_options = component_validations_df[
                            (component_validations_df['Component_Type'] == 'Temp Sensor') & 
                            (component_validations_df['Supplier'] == temp_supplier)
                        ]['Component_Generation'].unique().tolist()
                        temp_gen_options = [str(x) for x in temp_gen_options if x is not None and str(x).strip()]
                        temp_gen_options = sorted(temp_gen_options) if temp_gen_options else ["Gen1", "Gen2", "Gen3"]
                    except (KeyError, AttributeError, ValueError, TypeError) as e:
                        temp_gen_options = ["Gen1", "Gen2", "Gen3"]
                        
                    temp_gen = st.selectbox("Component Generation", options=temp_gen_options, key="temp_gen")
                    
                    # Get unique revisions for Temp Sensor
                    try:
                        temp_rev_options = component_validations_df[
                            (component_validations_df['Component_Type'] == 'Temp Sensor') & 
                            (component_validations_df['Supplier'] == temp_supplier) & 
                            (component_validations_df['Component_Generation'] == temp_gen)
                        ]['Revision'].unique().tolist()
                        temp_rev_options = [str(x) for x in temp_rev_options if x is not None and str(x).strip()]
                        temp_rev_options = sorted(temp_rev_options) if temp_rev_options else ["A", "B", "C"]
                    except (KeyError, AttributeError, ValueError, TypeError) as e:
                        temp_rev_options = ["A", "B", "C"]
                        
                    temp_rev = st.selectbox("Revision", options=temp_rev_options, key="temp_rev")
                    
                    # Get Temp Sensor process code
                    temp_code, _, _ = get_component_process_code(
                        selected_segment, temp_supplier, temp_gen, temp_rev, component_validations_df
                    )
                    if isinstance(temp_code, str) and not temp_code.startswith("No matching") and not temp_code.startswith("Error"):
                        st.success(f"Temp Sensor Process Code: {temp_code}")
                    else:
                        st.error(f"Temp Sensor Process Code: {temp_code}")
                else:
                    temp_code = ""
            
            with col2:
                st.subheader("SPD/Hub")
                
                # Get unique suppliers for SPD/Hub
                try:
                    spd_supplier_options = component_validations_df[
                        component_validations_df['Component_Type'] == 'SPD/Hub'
                    ]['Supplier'].unique().tolist()
                    spd_supplier_options = [str(x) for x in spd_supplier_options if x is not None and str(x).strip()]
                    spd_supplier_options = sorted(spd_supplier_options) if spd_supplier_options else ["IDT", "Montage", "Rambus"]
                except (KeyError, AttributeError, ValueError, TypeError) as e:
                    spd_supplier_options = ["IDT", "Montage", "Rambus"]
                    
                spd_supplier = st.selectbox("Supplier", options=spd_supplier_options, key="spd_supplier")
                
                # Get unique component generations for SPD/Hub
                try:
                    spd_gen_options = component_validations_df[
                        (component_validations_df['Component_Type'] == 'SPD/Hub') & 
                        (component_validations_df['Supplier'] == spd_supplier)
                    ]['Component_Generation'].unique().tolist()
                    spd_gen_options = [str(x) for x in spd_gen_options if x is not None and str(x).strip()]
                    spd_gen_options = sorted(spd_gen_options) if spd_gen_options else ["Gen1", "Gen2", "Gen3"]
                except (KeyError, AttributeError, ValueError, TypeError) as e:
                    spd_gen_options = ["Gen1", "Gen2", "Gen3"]
                    
                spd_gen = st.selectbox("Component Generation", options=spd_gen_options, key="spd_gen")
                
                # Get unique revisions for SPD/Hub
                try:
                    spd_rev_options = component_validations_df[
                        (component_validations_df['Component_Type'] == 'SPD/Hub') & 
                        (component_validations_df['Supplier'] == spd_supplier) & 
                        (component_validations_df['Component_Generation'] == spd_gen)
                    ]['Revision'].unique().tolist()
                    spd_rev_options = [str(x) for x in spd_rev_options if x is not None and str(x).strip()]
                    spd_rev_options = sorted(spd_rev_options) if spd_rev_options else ["A", "B", "C"]
                except (KeyError, AttributeError, ValueError, TypeError) as e:
                    spd_rev_options = ["A", "B", "C"]
                    
                spd_rev = st.selectbox("Revision", options=spd_rev_options, key="spd_rev")
                
                # Get SPD/Hub process code
                spd_code, _, _ = get_component_process_code(
                    selected_segment, spd_supplier, spd_gen, spd_rev, component_validations_df
                )
                if isinstance(spd_code, str) and not spd_code.startswith("No matching") and not spd_code.startswith("Error"):
                    st.success(f"SPD/Hub Process Code: {spd_code}")
                else:
                    st.error(f"SPD/Hub Process Code: {spd_code}")
                
                # Only show RCD/MRCD for Server segment
                if selected_segment.lower() == 'server':
                    st.subheader("RCD/MRCD")
                    
                    # Get unique suppliers for RCD/MRCD
                    try:
                        rcd_supplier_options = component_validations_df[
                            component_validations_df['Component_Type'] == 'RCD/MRCD'
                        ]['Supplier'].unique().tolist()
                        rcd_supplier_options = [str(x) for x in rcd_supplier_options if x is not None and str(x).strip()]
                        rcd_supplier_options = sorted(rcd_supplier_options) if rcd_supplier_options else ["Montage", "Rambus", "Renesas"]
                    except (KeyError, AttributeError, ValueError, TypeError) as e:
                        rcd_supplier_options = ["Montage", "Rambus", "Renesas"]
                        
                    rcd_supplier = st.selectbox("Supplier", options=rcd_supplier_options, key="rcd_supplier")
                    
                    # Get unique component generations for RCD/MRCD
                    try:
                        rcd_gen_options = component_validations_df[
                            (component_validations_df['Component_Type'] == 'RCD/MRCD') & 
                            (component_validations_df['Supplier'] == rcd_supplier)
                        ]['Component_Generation'].unique().tolist()
                        rcd_gen_options = [str(x) for x in rcd_gen_options if x is not None and str(x).strip()]
                        rcd_gen_options = sorted(rcd_gen_options) if rcd_gen_options else ["Gen1", "Gen2", "Gen3"]
                    except (KeyError, AttributeError, ValueError, TypeError) as e:
                        rcd_gen_options = ["Gen1", "Gen2", "Gen3"]
                        
                    rcd_gen = st.selectbox("Component Generation", options=rcd_gen_options, key="rcd_gen")
                    
                    # Get unique revisions for RCD/MRCD
                    try:
                        rcd_rev_options = component_validations_df[
                            (component_validations_df['Component_Type'] == 'RCD/MRCD') & 
                            (component_validations_df['Supplier'] == rcd_supplier) & 
                            (component_validations_df['Component_Generation'] == rcd_gen)
                        ]['Revision'].unique().tolist()
                        rcd_rev_options = [str(x) for x in rcd_rev_options if x is not None and str(x).strip()]
                        rcd_rev_options = sorted(rcd_rev_options) if rcd_rev_options else ["A", "B", "C"]
                    except (KeyError, AttributeError, ValueError, TypeError) as e:
                        rcd_rev_options = ["A", "B", "C"]
                        
                    rcd_rev = st.selectbox("Revision", options=rcd_rev_options, key="rcd_rev")
                    
                    # Get RCD/MRCD process code
                    rcd_code, _, _ = get_component_process_code(
                        selected_segment, rcd_supplier, rcd_gen, rcd_rev, component_validations_df
                    )
                    if isinstance(rcd_code, str) and not rcd_code.startswith("No matching") and not rcd_code.startswith("Error"):
                        st.success(f"RCD/MRCD Process Code: {rcd_code}")
                    else:
                        st.error(f"RCD/MRCD Process Code: {rcd_code}")
                else:
                    rcd_code = ""
                
                # Only show Data Buffer for Server segment
                if selected_segment.lower() == 'server':
                    st.subheader("Data Buffer (Optional)")
                    
                    # Get unique suppliers for Data Buffer
                    try:
                        db_supplier_options = component_validations_df[
                            component_validations_df['Component_Type'] == 'Data Buffer'
                        ]['Supplier'].unique().tolist()
                        db_supplier_options = [str(x) for x in db_supplier_options if x is not None and str(x).strip()]
                        db_supplier_options = sorted(db_supplier_options) + ["None"] if db_supplier_options else ["Montage", "Rambus", "None"]
                    except (KeyError, AttributeError, ValueError, TypeError) as e:
                        db_supplier_options = ["Montage", "Rambus", "None"]
                        
                    db_supplier = st.selectbox("Supplier", options=db_supplier_options, key="db_supplier")
                    
                    if db_supplier != "None":
                        # Get unique component generations for Data Buffer
                        try:
                            db_gen_options = component_validations_df[
                                (component_validations_df['Component_Type'] == 'Data Buffer') & 
                                (component_validations_df['Supplier'] == db_supplier)
                            ]['Component_Generation'].unique().tolist()
                            db_gen_options = [str(x) for x in db_gen_options if x is not None and str(x).strip()]
                            db_gen_options = sorted(db_gen_options) if db_gen_options else ["Gen1", "Gen2", "Gen3"]
                        except (KeyError, AttributeError, ValueError, TypeError) as e:
                            db_gen_options = ["Gen1", "Gen2", "Gen3"]
                            
                        db_gen = st.selectbox("Component Generation", options=db_gen_options, key="db_gen")
                        
                        # Get unique revisions for Data Buffer
                        try:
                            db_rev_options = component_validations_df[
                                (component_validations_df['Component_Type'] == 'Data Buffer') & 
                                (component_validations_df['Supplier'] == db_supplier) & 
                                (component_validations_df['Component_Generation'] == db_gen)
                            ]['Revision'].unique().tolist()
                            db_rev_options = [str(x) for x in db_rev_options if x is not None and str(x).strip()]
                            db_rev_options = sorted(db_rev_options) if db_rev_options else ["A", "B", "C"]
                        except (KeyError, AttributeError, ValueError, TypeError) as e:
                            db_rev_options = ["A", "B", "C"]
                            
                        db_rev = st.selectbox("Revision", options=db_rev_options, key="db_rev")
                        
                        # Get Data Buffer process code
                        db_code, _, _ = get_component_process_code(
                            selected_segment, db_supplier, db_gen, db_rev, component_validations_df
                        )
                        if isinstance(db_code, str) and not db_code.startswith("No matching") and not db_code.startswith("Error"):
                            st.success(f"Data Buffer Process Code: {db_code}")
                        else:
                            st.error(f"Data Buffer Process Code: {db_code}")
                    else:
                        db_code = ""
                else:
                    db_code = ""
            
            # Button to generate combined process code
            if st.button("Generate Module Process Code"):
                st.session_state.active_tab = "module_process_code"
                
                # Set part specification values
                part.set_segment(selected_segment)
                part.set_pmic(pmic_code if isinstance(pmic_code, str) and not pmic_code.startswith("No matching") and not pmic_code.startswith("Error") else "")
                part.set_spd_hub(spd_code if isinstance(spd_code, str) and not spd_code.startswith("No matching") and not spd_code.startswith("Error") else "")
                part.set_temp_sensor(temp_code if isinstance(temp_code, str) and not temp_code.startswith("No matching") and not temp_code.startswith("Error") else "")
                
                if selected_segment.lower() == 'server':
                    part.set_rcd_mrcd(rcd_code if isinstance(rcd_code, str) and not rcd_code.startswith("No matching") and not rcd_code.startswith("Error") else "")
                    part.set_data_buffer(db_code if 'db_code' in locals() and isinstance(db_code, str) and not db_code.startswith("No matching") and not db_code.startswith("Error") else "")
                
                # Generate combined process code
                combined_code = get_module_process_code(
                    part.pmic, part.spd_hub, part.temp_sensor, 
                    part.rcd_mrcd if selected_segment.lower() == 'server' else "", 
                    part.data_buffer if selected_segment.lower() == 'server' else "",
                    selected_segment
                )
                
                if isinstance(combined_code, str) and not combined_code.startswith("For") and not combined_code.startswith("Error") and not combined_code.startswith("Unknown"):
                    # Store process code explanation
                    st.session_state.process_code_explanation = explain_process_code(combined_code, selected_segment)
                    result_text = f"Generated Module Process Code: {combined_code}"
                else:
                    result_text = combined_code  # Error message
                
                st.session_state.result = result_text
                st.session_state.show_result = True
                
                # Create a synthetic dataframe for display
                if isinstance(combined_code, str) and not combined_code.startswith("For") and not combined_code.startswith("Error") and not combined_code.startswith("Unknown"):
                    synthetic_data = {
                        'Segment': [selected_segment],
                        'PMIC': [part.pmic],
                        'SPD_Hub': [part.spd_hub],
                        'Temp_Sensor': [part.temp_sensor],
                        'RCD_MRCD': [part.rcd_mrcd if selected_segment.lower() == 'server' else ""],
                        'Data_Buffer': [part.data_buffer if selected_segment.lower() == 'server' else ""],
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
                
                # Look up parts based on the process code
                parts_result = lookup_parts_by_process_code(process_code, component_validations_df)
                part.set_associated_parts(parts_result)
                
                # For Part Specification Generator, show the associated parts in the result
                st.session_state.result = f"Parts for Process Code {process_code}:\n\n{parts_result}"
                st.session_state.show_result = True
                
                # Store process code explanation if it's a valid process code
                if not parts_result.startswith("No matching") and not parts_result.startswith("Error"):
                    # Try to determine the segment from the process code
                    segment = "Server" if len(process_code) >= 4 else "Client"
                    st.session_state.process_code_explanation = explain_process_code(process_code, segment)
    
    if 'show_result' not in st.session_state:
        st.session_state.show_result = False
        st.session_state.result = ""
    
    if st.session_state.show_result:
        st.header("Result")
        st.text_area("Specification", st.session_state.result, height=300)
        
        # Display process code explanation if available
        if 'process_code_explanation' in st.session_state:
            st.subheader("Process Code Explanation")
            st.text_area("Breakdown", st.session_state.process_code_explanation, height=250)
        
        # Display detailed results in a table if available
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