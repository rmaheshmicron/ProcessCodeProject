import streamlit as st
import pandas as pd
import re
import os
from pathlib import Path
import tempfile
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.listitems.caml.query import CamlQuery

class PartSpecification:
    def __init__(self):
        self.seg = None
        self.form_factor = None
        self.spd = None
        self.mpn = None
        self.process_code = None
        self.component_type = None
        self.generated_process_code = None
        self.associated_parts = None
        self.validation_results = None
        self.pcb_reference = None
    
    def set_seg(self, seg_value):
        self.seg = seg_value
        return self
    
    def set_form_factor(self, form_factor_value):
        self.form_factor = form_factor_value
        return self
    
    def set_spd(self, spd_value):
        self.spd = spd_value
        return self
    
    def set_mpn(self, mpn_value):
        self.mpn = mpn_value
        return self
    
    def set_process_code(self, process_code_value):
        self.process_code = process_code_value
        return self
    
    def set_component_type(self, component_type_value):
        self.component_type = component_type_value
        return self
    
    def set_generated_process_code(self, process_code_value):
        self.generated_process_code = process_code_value
        return self
    
    def set_associated_parts(self, parts_value):
        self.associated_parts = parts_value
        return self
    
    def set_validation_results(self, validation_results):
        self.validation_results = validation_results
        return self
    
    def set_pcb_reference(self, pcb_reference):
        self.pcb_reference = pcb_reference
        return self
    
    def __str__(self):
        result = []
        if self.seg:
            result.append(f"SEG: {self.seg}")
        if self.form_factor:
            result.append(f"Form Factor: {self.form_factor}")
        if self.spd:
            result.append(f"SPD: {self.spd}")
        if self.mpn:
            result.append(f"MPN: {self.mpn}")
        if self.component_type:
            result.append(f"Component Type: {self.component_type}")
        if self.process_code:
            result.append(f"Process Code: {self.process_code}")
        if self.generated_process_code:
            result.append(f"Generated Process Code: {self.generated_process_code}")
        if self.associated_parts:
            result.append(f"Associated Parts:\n{self.associated_parts}")
        if self.validation_results:
            result.append(f"Validation Results:\n{self.validation_results}")
        if self.pcb_reference:
            result.append(f"PCB Reference:\n{self.pcb_reference}")
        
        return "\n".join(result)


def show_process_code_info():
    """Display information about process code structure"""
    with st.expander("Process Code Information", expanded=False):
        st.markdown("""
        ### Process Code Structure
        
        The "Process Code" is a mechanism for BOM segregation and manufacturing purposes with each character representing a specific non-DRAM Active Component.
        
        #### D5 Server Process Code Structure
        | Component | Position in PROCESS_CODE | Position in PROCESS_CODE_PRINT |
        |-----------|--------------------------|--------------------------------|
        | PMIC | 1 | 1 |
        | SPD/Hub | 2 | 3 |
        | Temp Sensor | 3 | 4 |
        | RCD / MRCD | 4 | 2 |
        | Data Buffer (if applicable) | 5 | 5 |
        
        **D5 Server PROCESS CODE:** PMIC → SPD/Hub → Temp Sensor → RCD → Data Buffer (if applicable)
        
        **D5 Server PROCESS CODE PRINT:** PMIC → RCD → SPD/Hub → Temp Sensor → Data Buffer (if applicable)
        
        #### D5 Client Process Code Structure
        | Component | Position in PROCESS_CODE | Position in PROCESS_CODE_PRINT |
        |-----------|--------------------------|--------------------------------|
        | PMIC | 1 | 1 |
        | SPD/Hub | 2 | 2 |
        | CKD (if applicable) | 3 | 3 |
        
        **D5 Client PROCESS CODE and PROCESS CODE PRINT:** PMIC → SPD/Hub → CKD (if applicable)
        
        *Note: This view contains only components WITH an assigned PROCESS CODE character.
        """)


@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_data_from_sharepoint():
    """Load data directly from SharePoint lists using the specific URLs"""
    data = {}
    
    # SharePoint connection settings
    sharepoint_site = "https://microncorp.sharepoint.com/sites/mdg"
    
    # Get credentials from secrets or sidebar inputs
    if "sharepoint_username" in st.secrets and "sharepoint_password" in st.secrets:
        username = st.secrets["sharepoint_username"]
        password = st.secrets["sharepoint_password"]
    else:
        # For security, in a real application you should use Streamlit secrets
        # instead of collecting credentials in the UI
        username = st.sidebar.text_input("SharePoint Username (include @micron.com)", key="sp_username")
        password = st.sidebar.text_input("SharePoint Password", type="password", key="sp_password")
    
    # Check if we have the required SharePoint credentials
    if not (username and password):
        st.sidebar.warning("Please provide SharePoint credentials to load data.")
        return None
    
    try:
        # Connect to SharePoint
        auth_context = AuthenticationContext(sharepoint_site)
        auth_context.acquire_token_for_user(username, password)
        ctx = ClientContext(sharepoint_site, auth_context)
        
        # First, let's verify the connection and get available lists
        try:
            web = ctx.web
            ctx.load(web)
            ctx.execute_query()
            
            # Get all lists to help with debugging
            lists = ctx.web.lists
            ctx.load(lists)
            ctx.execute_query()
            available_lists = [list_obj.properties['Title'] for list_obj in lists]
            st.sidebar.success(f"Connected to SharePoint site: {web.properties['Title']}")
            
            # For debugging, show available lists in the sidebar
            with st.sidebar.expander("Available SharePoint Lists", expanded=True):
                st.write(", ".join(available_lists))
                
            # Check for Module HW Design Validation subsite
            subsites = ctx.web.webs
            ctx.load(subsites)
            ctx.execute_query()
            
            module_hw_design_subsite = None
            for subsite in subsites:
                if "Module HW Design Validation" in subsite.properties['Title']:
                    module_hw_design_subsite = subsite
                    st.sidebar.success(f"Found Module HW Design Validation subsite: {subsite.properties['Title']}")
                    break
            
            # If we found the Module HW Design Validation subsite, try to access its lists
            if module_hw_design_subsite:
                subsite_ctx = ClientContext(f"{sharepoint_site}{module_hw_design_subsite.properties['ServerRelativeUrl']}", auth_context)
                subsite_lists = subsite_ctx.web.lists
                subsite_ctx.load(subsite_lists)
                subsite_ctx.execute_query()
                
                subsite_available_lists = [list_obj.properties['Title'] for list_obj in subsite_lists]
                with st.sidebar.expander(f"Lists in {module_hw_design_subsite.properties['Title']} subsite", expanded=True):
                    st.write(", ".join(subsite_available_lists))
                
                # Try to find the Non-DRAM Component Validations list in the subsite
                hw_validation_list_name = None
                for list_name in ["Non-DRAM Component Validations", "Component Validations", "HW Validation"]:
                    if list_name in subsite_available_lists:
                        hw_validation_list_name = list_name
                        break
                
                if hw_validation_list_name:
                    st.sidebar.success(f"Found HW validation list: {hw_validation_list_name}")
                    hw_validation_list = subsite_ctx.web.lists.get_by_title(hw_validation_list_name)
                    
                    # Create a CAML query to get all items
                    caml_query = CamlQuery()
                    caml_query.ViewXml = "<View><RowLimit>5000</RowLimit></View>"
                    
                    # Execute the query
                    items = hw_validation_list.get_items(caml_query)
                    subsite_ctx.load(items)
                    subsite_ctx.execute_query()
                    
                    # Convert to DataFrame for process code data
                    process_code_data = []
                    parts_data = []
                    
                    for item in items:
                        item_properties = item.properties
                        
                        # For debugging, print the first item's properties
                        if len(process_code_data) == 0 and len(parts_data) == 0:
                            with st.sidebar.expander(f"Sample {hw_validation_list_name} Item Fields", expanded=True):
                                st.write(", ".join(item_properties.keys()))
                        
                        # Extract process code data - adjust field names if needed
                        market_segment_field = next((f for f in ['Market_Segment', 'MarketSegment', 'Segment', 'Title'] if f in item_properties), None)
                        form_factor_field = next((f for f in ['Form_Factor', 'FormFactor', 'Form Factor'] if f in item_properties), None)
                        speed_field = next((f for f in ['Speed', 'SPD', 'SpeedBin'] if f in item_properties), None)
                        process_code_field = next((f for f in ['Process_Code', 'ProcessCode', 'Process Code'] if f in item_properties), None)
                        
                        if market_segment_field and form_factor_field and speed_field and process_code_field:
                            process_code_data.append({
                                'Market_Segment': item_properties.get(market_segment_field, ''),
                                'Form_Factor': item_properties.get(form_factor_field, ''),
                                'Speed': item_properties.get(speed_field, ''),
                                'Process_Code': item_properties.get(process_code_field, '')
                            })
                        
                        # Extract parts data - adjust field names if needed
                        mpn_field = next((f for f in ['MPN', 'PartNumber', 'Part Number'] if f in item_properties), None)
                        component_type_field = next((f for f in ['Component_Type', 'ComponentType', 'Component Type'] if f in item_properties), None)
                        validation_status_field = next((f for f in ['Validation_Status', 'ValidationStatus', 'Status'] if f in item_properties), None)
                        
                        if mpn_field and process_code_field and component_type_field:
                            parts_data.append({
                                'MPN': item_properties.get(mpn_field, ''),
                                'Process_Code': item_properties.get(process_code_field, ''),
                                'Component_Type': item_properties.get(component_type_field, ''),
                                'Validation_Status': item_properties.get(validation_status_field, '') if validation_status_field else ''
                            })
                    
                    process_code_df = pd.DataFrame(process_code_data)
                    data['process_code_df'] = process_code_df
                    
                    parts_df = pd.DataFrame(parts_data)
                    data['parts_df'] = parts_df
                    data['module_hw_validation_df'] = parts_df.copy()
                    
                    st.sidebar.success(f"Successfully loaded {len(process_code_data)} process codes and {len(parts_data)} parts from SharePoint")
                else:
                    st.sidebar.warning("Could not find HW validation list in the subsite")
                    data['process_code_df'] = pd.DataFrame()
                    data['parts_df'] = pd.DataFrame()
                    data['module_hw_validation_df'] = pd.DataFrame()
                
                # Try to find the DRAM Module Validations list in the subsite
                pcb_reference_list_name = None
                for list_name in ["DRAM Module Validations (WIP)", "Module Validations", "PCB Reference"]:
                    if list_name in subsite_available_lists:
                        pcb_reference_list_name = list_name
                        break
                
                if pcb_reference_list_name:
                    st.sidebar.success(f"Found PCB reference list: {pcb_reference_list_name}")
                    pcb_reference_list = subsite_ctx.web.lists.get_by_title(pcb_reference_list_name)
                    
                    # Create a CAML query to get all items
                    caml_query = CamlQuery()
                    caml_query.ViewXml = "<View><RowLimit>5000</RowLimit></View>"
                    
                    # Execute the query
                    items = pcb_reference_list.get_items(caml_query)
                    subsite_ctx.load(items)
                    subsite_ctx.execute_query()
                    
                    # For debugging, print the first item's properties
                    if items.count > 0:
                        with st.sidebar.expander(f"Sample {pcb_reference_list_name} Item Fields", expanded=True):
                            st.write(", ".join(items[0].properties.keys()))
                    
                    # Convert to DataFrame - adjust field names if needed
                    pcb_reference_data = []
                    for item in items:
                        item_properties = item.properties
                        
                        process_code_field = next((f for f in ['Process_Code', 'ProcessCode', 'Process Code'] if f in item_properties), None)
                        pcb_reference_field = next((f for f in ['PCB_Reference', 'PCBReference', 'PCB Reference', 'PCB'] if f in item_properties), None)
                        
                        if process_code_field and pcb_reference_field:
                            pcb_reference_data.append({
                                'Process_Code': item_properties.get(process_code_field, ''),
                                'PCB_Reference': item_properties.get(pcb_reference_field, '')
                            })
                        elif process_code_field:  # If we only have process code but not PCB reference
                            pcb_reference_data.append({
                                'Process_Code': item_properties.get(process_code_field, ''),
                                'PCB_Reference': 'PCB-' + item_properties.get(process_code_field, '')  # Create a default PCB reference
                            })
                    
                    pcb_reference_df = pd.DataFrame(pcb_reference_data)
                    data['module_pcb_reference_df'] = pcb_reference_df
                    
                    st.sidebar.success(f"Successfully loaded {len(pcb_reference_data)} PCB references from SharePoint")
                else:
                    st.sidebar.warning("Could not find PCB reference list in the subsite")
                    data['module_pcb_reference_df'] = pd.DataFrame()
            else:
                # If we didn't find the subsite, try to find the lists in the main site
                st.sidebar.warning("Could not find Module HW Design Validation subsite")
                
                # Try to find the lists in the main site
                hw_validation_list_name = None
                for list_name in ["Non-DRAM Component Validations", "Component Validations", "HW Validation", "Basic List"]:
                    if list_name in available_lists:
                        hw_validation_list_name = list_name
                        break
                
                if hw_validation_list_name:
                    st.sidebar.success(f"Found HW validation list in main site: {hw_validation_list_name}")
                    hw_validation_list = ctx.web.lists.get_by_title(hw_validation_list_name)
                    
                    # Create a CAML query to get all items
                    caml_query = CamlQuery()
                    caml_query.ViewXml = "<View><RowLimit>5000</RowLimit></View>"
                    
                    # Execute the query
                    items = hw_validation_list.get_items(caml_query)
                    ctx.load(items)
                    ctx.execute_query()
                    
                    # Convert to DataFrame for process code data
                    process_code_data = []
                    parts_data = []
                    
                    for item in items:
                        item_properties = item.properties
                        
                        # For debugging, print the first item's properties
                        if len(process_code_data) == 0 and len(parts_data) == 0:
                            with st.sidebar.expander(f"Sample {hw_validation_list_name} Item Fields", expanded=True):
                                st.write(", ".join(item_properties.keys()))
                        
                        # Extract process code data - adjust field names if needed
                        market_segment_field = next((f for f in ['Market_Segment', 'MarketSegment', 'Segment', 'Title'] if f in item_properties), None)
                        form_factor_field = next((f for f in ['Form_Factor', 'FormFactor', 'Form Factor'] if f in item_properties), None)
                        speed_field = next((f for f in ['Speed', 'SPD', 'SpeedBin'] if f in item_properties), None)
                        process_code_field = next((f for f in ['Process_Code', 'ProcessCode', 'Process Code'] if f in item_properties), None)
                        
                        if market_segment_field and form_factor_field and speed_field and process_code_field:
                            process_code_data.append({
                                'Market_Segment': item_properties.get(market_segment_field, ''),
                                'Form_Factor': item_properties.get(form_factor_field, ''),
                                'Speed': item_properties.get(speed_field, ''),
                                'Process_Code': item_properties.get(process_code_field, '')
                            })
                        
                        # Extract parts data - adjust field names if needed
                        mpn_field = next((f for f in ['MPN', 'PartNumber', 'Part Number'] if f in item_properties), None)
                        component_type_field = next((f for f in ['Component_Type', 'ComponentType', 'Component Type'] if f in item_properties), None)
                        validation_status_field = next((f for f in ['Validation_Status', 'ValidationStatus', 'Status'] if f in item_properties), None)
                        
                        if mpn_field and process_code_field and component_type_field:
                            parts_data.append({
                                'MPN': item_properties.get(mpn_field, ''),
                                'Process_Code': item_properties.get(process_code_field, ''),
                                'Component_Type': item_properties.get(component_type_field, ''),
                                'Validation_Status': item_properties.get(validation_status_field, '') if validation_status_field else ''
                            })
                    
                    process_code_df = pd.DataFrame(process_code_data)
                    data['process_code_df'] = process_code_df
                    
                    parts_df = pd.DataFrame(parts_data)
                    data['parts_df'] = parts_df
                    data['module_hw_validation_df'] = parts_df.copy()
                    
                    st.sidebar.success(f"Successfully loaded {len(process_code_data)} process codes and {len(parts_data)} parts from SharePoint")
                else:
                    st.sidebar.warning("Could not find HW validation list in the main site")
                    data['process_code_df'] = pd.DataFrame()
                    data['parts_df'] = pd.DataFrame()
                    data['module_hw_validation_df'] = pd.DataFrame()
                
                pcb_reference_list_name = None
                for list_name in ["DRAM Module Validations (WIP)", "Module Validations", "PCB Reference", "PCB Reference Table"]:
                    if list_name in available_lists:
                        pcb_reference_list_name = list_name
                        break
                
                if pcb_reference_list_name:
                    st.sidebar.success(f"Found PCB reference list in main site: {pcb_reference_list_name}")
                    pcb_reference_list = ctx.web.lists.get_by_title(pcb_reference_list_name)
                    
                    # Create a CAML query to get all items
                    caml_query = CamlQuery()
                    caml_query.ViewXml = "<View><RowLimit>5000</RowLimit></View>"
                    
                    # Execute the query
                    items = pcb_reference_list.get_items(caml_query)
                    ctx.load(items)
                    ctx.execute_query()
                    
                    # For debugging, print the first item's properties
                    if items.count > 0:
                        with st.sidebar.expander(f"Sample {pcb_reference_list_name} Item Fields", expanded=True):
                            st.write(", ".join(items[0].properties.keys()))
                    
                    # Convert to DataFrame - adjust field names if needed
                    pcb_reference_data = []
                    for item in items:
                        item_properties = item.properties
                        
                        process_code_field = next((f for f in ['Process_Code', 'ProcessCode', 'Process Code'] if f in item_properties), None)
                        pcb_reference_field = next((f for f in ['PCB_Reference', 'PCBReference', 'PCB Reference', 'PCB'] if f in item_properties), None)
                        
                        if process_code_field and pcb_reference_field:
                            pcb_reference_data.append({
                                'Process_Code': item_properties.get(process_code_field, ''),
                                'PCB_Reference': item_properties.get(pcb_reference_field, '')
                            })
                        elif process_code_field:  # If we only have process code but not PCB reference
                            pcb_reference_data.append({
                                'Process_Code': item_properties.get(process_code_field, ''),
                                'PCB_Reference': 'PCB-' + item_properties.get(process_code_field, '')  # Create a default PCB reference
                            })
                    
                    pcb_reference_df = pd.DataFrame(pcb_reference_data)
                    data['module_pcb_reference_df'] = pcb_reference_df
                    
                    st.sidebar.success(f"Successfully loaded {len(pcb_reference_data)} PCB references from SharePoint")
                else:
                    st.sidebar.warning("Could not find PCB reference list in the main site")
                    data['module_pcb_reference_df'] = pd.DataFrame()
        except Exception as e:
            st.sidebar.error(f"Error connecting to SharePoint: {e}")
            return get_embedded_sample_data()
    
    except Exception as e:
        st.sidebar.error(f"Error connecting to SharePoint: {e}")
        st.sidebar.info("Using embedded sample data as fallback")
        return get_embedded_sample_data()
    
    # If any dataframes are empty, use the embedded data as fallback
    if (data.get('process_code_df', pd.DataFrame()).empty or 
        data.get('parts_df', pd.DataFrame()).empty or 
        data.get('module_hw_validation_df', pd.DataFrame()).empty or 
        data.get('module_pcb_reference_df', pd.DataFrame()).empty):
        
        st.sidebar.warning("Some data couldn't be loaded from SharePoint. Using embedded sample data as fallback.")
        embedded_data = get_embedded_sample_data()
        
        # Use embedded data for any empty dataframes
        if data.get('process_code_df', pd.DataFrame()).empty:
            data['process_code_df'] = embedded_data['process_code_df']
        if data.get('parts_df', pd.DataFrame()).empty:
            data['parts_df'] = embedded_data['parts_df']
        if data.get('module_hw_validation_df', pd.DataFrame()).empty:
            data['module_hw_validation_df'] = embedded_data['module_hw_validation_df']
        if data.get('module_pcb_reference_df', pd.DataFrame()).empty:
            data['module_pcb_reference_df'] = embedded_data['module_pcb_reference_df']
    
    return data


def get_embedded_sample_data():
    """Return embedded sample data as fallback"""
    # Process code data - mapping of segment, form factor, and speed to process codes
    process_code_data = {
        'Market_Segment': [
            'Client', 'Client', 'Client', 'Client', 'Client', 'Client', 'Client', 'Client',
            'Server', 'Server', 'Server', 'Server', 'Server', 'Server', 'Server', 'Server'
        ],
        'Form_Factor': [
            'SODIMM', 'UDIMM', 'CAMM2', 'SOCAMM', 'SOCAMM2', 'CSODIMM', 'CUDIMM', 'SOEDIMM',
            'RDIMM', 'LRDIMM', 'DDIMM', 'MINIRDIMM', 'EUDIMM', 'CDIMM', 'TFF MRDIMM', 'MINIRDIMM'
        ],
        'Speed': [
            '4800', '5600', '6400', '7200', '8000', '8800', '9600', '12800',
            '4800', '5600', '6400', '7200', '8000', '8800', '9600', '12800'
        ],
        'Process_Code': [
            'CL-SODIMM-U8K-ABC', 'CL-UDIMM-O8K-DEF', 'CL-CAMM2-U8K-GHI', 'CL-SOCAMM-O8K-JKL', 
            'CL-SOCAMM2-U8K-MNO', 'CL-CSODIMM-O8K-PQR', 'CL-CUDIMM-U8K-STU', 'CL-SOEDIMM-O8K-VWX',
            'SV-RDIMM-O8K-ABCDE', 'SV-LRDIMM-U8K-FGHIJ', 'SV-DDIMM-O8K-KLMNO', 'SV-MINIRDIMM-U8K-PQRST',
            'SV-EUDIMM-O8K-UVWXY', 'SV-CDIMM-U8K-Z1234', 'SV-TFFMRDIMM-O8K-56789', 'SV-MINIRDIMM-U8K-ABCDF'
        ],
        'PCB_Reference': [
            'PCB-CL-SODIMM-001', 'PCB-CL-UDIMM-002', 'PCB-CL-CAMM2-003', 'PCB-CL-SOCAMM-004',
            'PCB-CL-SOCAMM2-005', 'PCB-CL-CSODIMM-006', 'PCB-CL-CUDIMM-007', 'PCB-CL-SOEDIMM-008',
            'PCB-SV-RDIMM-001', 'PCB-SV-LRDIMM-002', 'PCB-SV-DDIMM-003', 'PCB-SV-MINIRDIMM-004',
            'PCB-SV-EUDIMM-005', 'PCB-SV-CDIMM-006', 'PCB-SV-TFFMRDIMM-007', 'PCB-SV-MINIRDIMM-008'
        ]
    }
    
    # Parts data - component information
    parts_data = {
        'MPN': [
            'MT40A1G16RC-062E:B', 'MT40A512M16TB-062E:R', 'MT40A256M16LY-075E:B', 'MT53E512M32D2DS-046:B',
            'MT53E1G32D2FS-046:B', 'MT40A512M16TB-062E:B', 'MT40A1G16RC-062E:R', 'MT40A256M16LY-075E:R',
            'MT53E512M32D2DS-046:R', 'MT53E1G32D2FS-046:R', 'MT40A512M16TB-062E:G', 'MT40A1G16RC-062E:G',
            'MT40A256M16LY-075E:G', 'MT53E512M32D2DS-046:G', 'MT53E1G32D2FS-046:G', 'MT40A512M16TB-062E:Y'
        ],
        'Process_Code': [
            'CL-SODIMM-U8K-ABC', 'CL-UDIMM-O8K-DEF', 'CL-CAMM2-U8K-GHI', 'CL-SOCAMM-O8K-JKL', 
            'CL-SOCAMM2-U8K-MNO', 'CL-CSODIMM-O8K-PQR', 'CL-CUDIMM-U8K-STU', 'CL-SOEDIMM-O8K-VWX',
            'SV-RDIMM-O8K-ABCDE', 'SV-LRDIMM-U8K-FGHIJ', 'SV-DDIMM-O8K-KLMNO', 'SV-MINIRDIMM-U8K-PQRST', 
            'SV-EUDIMM-O8K-UVWXY', 'SV-CDIMM-U8K-Z1234', 'SV-TFFMRDIMM-O8K-56789', 'SV-MINIRDIMM-U8K-ABCDF'
        ],
        'Component_Type': [
            'PMIC', 'RCD', 'CKD', 'Temp Sensor', 'SPD/Hub', 'Data Buffer', 'Voltage Regulator', 'Inductor',
            'PMIC', 'RCD', 'CKD', 'Temp Sensor', 'SPD/Hub', 'Data Buffer', 'Muxed RCD', 'Voltage Regulator'
        ],
        'Validation_Status': [
            'Validated', 'Validated', 'In Progress', 'Validated', 'Validated', 'In Progress', 'Validated', 'In Progress',
            'Validated', 'Validated', 'In Progress', 'Validated', 'Validated', 'In Progress', 'Validated', 'In Progress'
        ]
    }
    
    # Create DataFrames from the embedded data
    process_code_df = pd.DataFrame(process_code_data)
    parts_df = pd.DataFrame(parts_data)
    
    # Create a module_pcb_reference_df by combining process_code_df columns
    module_pcb_reference_df = pd.DataFrame({
        'Process_Code': process_code_df['Process_Code'],
        'PCB_Reference': process_code_df['PCB_Reference']
    })
    
    # Create a module_hw_validation_df by combining parts_df columns
    module_hw_validation_df = parts_df.copy()
    
    return {
        'process_code_df': process_code_df,
        'parts_df': parts_df,
        'module_hw_validation_df': module_hw_validation_df,
        'module_pcb_reference_df': module_pcb_reference_df
    }


def generate_process_code(seg, form_factor, spd, process_code_df):
    """Generate process code based on segment, form factor, and speed"""
    try:
        # Filter the dataframe based on the inputs
        filtered_df = process_code_df[
            (process_code_df['Market_Segment'].str.lower() == seg.lower()) & 
            (process_code_df['Form_Factor'].str.lower() == form_factor.lower()) & 
            (process_code_df['Speed'].str.lower() == spd.lower())
        ]
        
        if filtered_df.empty:
            return "No matching process code found for the given criteria", None
        
        # Get the process code from the filtered dataframe
        process_code = filtered_df.iloc[0]['Process_Code']
        
        # Return the process code and the filtered dataframe for display
        return process_code, filtered_df
    
    except Exception as e:
        return f"Error generating process code: {e}", None


def lookup_parts(mpn=None, process_code=None, component_type=None, parts_df=None):
    """Look up parts based on MPN, process code, or component type"""
    try:
        filtered_df = parts_df.copy()
        
        # Apply filters based on provided parameters
        if mpn:
            filtered_df = filtered_df[filtered_df['MPN'].str.contains(mpn, case=False, na=False)]
        
        if process_code:
            filtered_df = filtered_df[filtered_df['Process_Code'].str.contains(process_code, case=False, na=False)]
        
        if component_type and component_type != "Other":
            filtered_df = filtered_df[filtered_df['Component_Type'].str.contains(component_type, case=False, na=False)]
        
        if filtered_df.empty:
            return "No matching parts found for the given criteria"
        
        # Format the results as a string
        result = filtered_df.to_string(index=False)
        return result
    
    except Exception as e:
        return f"Error looking up parts: {e}"


def get_validation_results(mpn=None, process_code=None, component_type=None, validation_df=None):
    """Get validation results for the specified part"""
    try:
        if validation_df is None or validation_df.empty:
            return "No validation data available"
        
        filtered_df = validation_df.copy()
        
        # Apply filters based on provided parameters
        if mpn:
            filtered_df = filtered_df[filtered_df['MPN'].str.contains(mpn, case=False, na=False)]
        
        if process_code:
            filtered_df = filtered_df[filtered_df['Process_Code'].str.contains(process_code, case=False, na=False)]
        
        if component_type and component_type != "Other":
            filtered_df = filtered_df[filtered_df['Component_Type'].str.contains(component_type, case=False, na=False)]
        
        if filtered_df.empty:
            return "No validation results found for the given criteria"
        
        # Format the results as a string
        result = filtered_df.to_string(index=False)
        return result
    
    except Exception as e:
        return f"Error retrieving validation results: {e}"


def get_pcb_reference(process_code, pcb_reference_df):
    """Get PCB reference information for the specified process code"""
    try:
        if pcb_reference_df is None or pcb_reference_df.empty:
            return "No PCB reference data available"
        
        filtered_df = pcb_reference_df[pcb_reference_df['Process_Code'].str.contains(process_code, case=False, na=False)]
        
        if filtered_df.empty:
            return "No PCB reference found for the given process code"
        
        # Format the results as a string
        result = filtered_df.to_string(index=False)
        return result
    
    except Exception as e:
        return f"Error retrieving PCB reference: {e}"


def explain_process_code(process_code, market_segment):
    """Explain the meaning of each character in the process code"""
    if not process_code or not isinstance(process_code, str):
        return "Invalid process code"
    
    # Extract just the characters that represent components (after the last dash)
    match = re.search(r'-([A-Za-z0-9]+)$', process_code)
    if match:
        component_code = match.group(1)
    else:
        component_code = process_code
    
    explanation = []
    explanation.append(f"Process Code: {process_code}")
    explanation.append("Component Breakdown:")
    
    if market_segment.lower() == 'server':
        # Server process code explanation
        if len(component_code) >= 1:
            explanation.append(f"Position 1: PMIC - {component_code[0]}")
        if len(component_code) >= 2:
            explanation.append(f"Position 2: SPD/Hub - {component_code[1]}")
        if len(component_code) >= 3:
            explanation.append(f"Position 3: Temp Sensor - {component_code[2]}")
        if len(component_code) >= 4:
            explanation.append(f"Position 4: RCD/MRCD - {component_code[3]}")
        if len(component_code) >= 5:
            explanation.append(f"Position 5: Data Buffer - {component_code[4]}")
        
        explanation.append("\nProcess Code Print Order (as shown on product label):")
        explanation.append("PMIC → RCD → SPD/Hub → Temp Sensor → Data Buffer (if applicable)")

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
    if data is None:
        st.error("Failed to load data from SharePoint. Please check your credentials and try again.")
        st.stop()
    
    process_code_df = data['process_code_df']
    parts_df = data['parts_df']
    module_hw_validation_df = data['module_hw_validation_df']
    module_pcb_reference_df = data['module_pcb_reference_df']
    
    # Display data refresh time
    st.sidebar.info(f"Data last refreshed: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Track which tab is active
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = "process_code"
    
    part = PartSpecification()
    
    tab1, tab2 = st.tabs(["Process Code Generator", "Part Specification Generator"])
    
    with tab1:
        st.write("Enter the details for your process code below:")
        
        # Get unique market segments from the data
        seg_options = sorted(process_code_df['Market_Segment'].unique().tolist())
        selected_seg = st.selectbox("Market Segment", options=seg_options, key="seg_process")
        
        # Get unique form factors from the data
        form_factor_options = sorted(process_code_df['Form_Factor'].unique().tolist() + ["Other"])
        selected_form_factor = st.selectbox("Form Factor", options=form_factor_options, key="ff_process")
        
        if selected_form_factor == "Other":
            custom_form_factor = st.text_input("Enter custom form factor", key="custom_ff_process")
            form_factor_value = custom_form_factor if custom_form_factor else None
        else:
            form_factor_value = selected_form_factor
        
        # Get unique speed options from the data
        spd_options = sorted(process_code_df['Speed'].unique().tolist())
        selected_spd = st.selectbox("Speed", options=spd_options, key="spd_process")
        
        process_code_valid = True
        if st.button("Generate Process Code"):
            st.session_state.active_tab = "process_code"
            if not form_factor_value:
                st.error("Please enter a form factor")
                process_code_valid = False
            
            if process_code_valid:
                part.set_seg(selected_seg)
                part.set_form_factor(form_factor_value)
                part.set_spd(selected_spd)
                
                # Generate process code from the data
                generated_code, code_details = generate_process_code(
                    selected_seg, form_factor_value, selected_spd, process_code_df
                )
                part.set_generated_process_code(generated_code)
                
                # If we have a valid process code
                if isinstance(generated_code, str) and not generated_code.startswith("No matching") and not generated_code.startswith("Error"):
                    # Store process code explanation
                    st.session_state.process_code_explanation = explain_process_code(generated_code, selected_seg)
                
                # For Process Code Generator, only show the generated process code in the result
                st.session_state.result = f"Generated Process Code: {generated_code}"
                st.session_state.show_result = True
                
                # If we have details to show in a table
                if code_details is not None and not code_details.empty:
                    st.session_state.code_details = code_details
                else:
                    st.session_state.code_details = None
    
    with tab2:
        st.write("Enter the details for your part specification below:")
        
        mpn = st.text_input("Manufacturing Part Number (MPN)", key="mpn_part")
        
        # Get unique component types from the data
        component_type_options = sorted(parts_df['Component_Type'].unique().tolist() + ["Other"])
        selected_component_type = st.selectbox("Component Type", options=component_type_options, key="comp_part")
        
        process_code = st.text_input("Process Code", key="pc_part")
        
        part_spec_valid = True
        if st.button("Generate Part Specification"):
            st.session_state.active_tab = "part_spec"
            if not mpn and not process_code:
                st.error("Please enter either a manufacturing Part Number or a Process Code")
                part_spec_valid = False
            
            if part_spec_valid:
                if mpn:
                    part.set_mpn(mpn)
                if process_code:
                    part.set_process_code(process_code)
                part.set_component_type(selected_component_type)
                
                # Look up associated parts
                associated_parts = lookup_parts(
                    mpn=mpn, 
                    process_code=process_code, 
                    component_type=selected_component_type, 
                    parts_df=parts_df
                )
                part.set_associated_parts(associated_parts)
                
                # For Part Specification Generator, only show the associated parts in the result
                st.session_state.result = f"Associated Parts:\n{associated_parts}"
                st.session_state.show_result = True
    
    if 'show_result' not in st.session_state:
        st.session_state.show_result = False
        st.session_state.result = ""
    
    if st.session_state.show_result:
        st.header("Result")
        st.text_area("Specification", st.session_state.result, height=400)
        
        # Display process code explanation and details only for Process Code Generator
        if st.session_state.active_tab == "process_code":
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