import streamlit as st
import pandas as pd
import pytz
import base64
import requests
import json
import os
import re
from requests.adapters import HTTPAdapter
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


def get_rest_api_connection_params():
    if all(key in st.secrets for key in ["rest_api_base_url", "rest_api_username", "rest_api_password"]):
        return (
            st.secrets["rest_api_base_url"],
            st.secrets["rest_api_username"],
            st.secrets["rest_api_password"],
            st.secrets.get("rest_api_timeout", 600)
        )
    
    if all(key in st.session_state for key in ["rest_api_base_url", "rest_api_username", "rest_api_password"]):
        return (
            st.session_state["rest_api_base_url"],
            st.session_state["rest_api_username"],
            st.session_state["rest_api_password"],
            600
        )
    
    if "rest_api_widgets_created" not in st.session_state:
        st.sidebar.subheader("REST API Connection")
        
        base_url = st.sidebar.text_input(
            "API Base URL",
            value="http://localhost:8000",
            key="rest_api_base_url",
            help="Base URL for the REST API endpoint"
        )
        
        username = st.sidebar.text_input(
            "Username", 
            value="admin",
            key="rest_api_username",
            help="API username"
        )
        
        password = st.sidebar.text_input(
            "Password", 
            type="password",
            value="MicronPC123",
            key="rest_api_password",
            help="API password"
        )
        
        st.session_state["rest_api_widgets_created"] = True
        
        return base_url, username, password, 600
    else:
        return (
            st.session_state.get("rest_api_base_url", "http://localhost:8000"),
            st.session_state.get("rest_api_username", "admin"),
            st.session_state.get("rest_api_password", "MicronPC123"),
            600
        )
    
def create_rest_api_session(base_url, username, password, timeout):
    if not all([base_url, username, password]):
        missing = []
        if not base_url: missing.append("Base URL")
        if not username: missing.append("Username")
        if not password: missing.append("Password")
        
        st.sidebar.error(f"Missing required fields: {', '.join(missing)}")
        return None
    
    try:
        session = requests.Session()
        session.timeout = timeout
        
        auth_string = f"{username}:{password}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        session.headers.update({
            'Authorization': f'Basic {encoded_auth}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        health_url = f"{base_url.rstrip('/')}/health"
        response = session.get(health_url)
        
        if response.status_code == 200:
            try:
                test_data_url = f"{base_url.rstrip('/')}/modulebom-simple?limit=1"
                test_response = session.get(test_data_url)
                
                if test_response.status_code == 200:
                    content_type = test_response.headers.get('content-type', '')
                    if 'application/json' in content_type:
                        try:
                            test_data = test_response.json()
                        except json.JSONDecodeError:
                            st.sidebar.warning("ModuleBOM_Simple endpoint returned invalid JSON")
                    else:
                        st.sidebar.warning(f"ModuleBOM_Simple endpoint returned: {content_type}")
                else:
                    st.sidebar.warning(f"ModuleBOM_Simple endpoint: HTTP {test_response.status_code}")
            except Exception as endpoint_error:
                st.sidebar.warning(f"ModuleBOM_Simple endpoint: {str(endpoint_error)}")
            
            return session, base_url
        
        else:
            st.sidebar.error(f"Connection failed: HTTP {response.status_code}")
            
            content_type = response.headers.get('content-type', '')
            if 'text/html' in content_type:
                st.sidebar.error("Server returned HTML instead of JSON - check API URL")
            else:
                st.sidebar.error(f"Response: {response.text[:200]}")
            
            return None
    
    except requests.exceptions.Timeout:
        st.sidebar.error("**Timeout Issue**: Connection is timing out")
        return None
        
    except requests.exceptions.ConnectionError:
        st.sidebar.error("**Network Issue**: Cannot reach API server")
        return None
        
    except Exception as e:
        st.sidebar.error(f"Unexpected Error: {str(e)}")
        return None

def test_rest_api_connection_detailed():
    st.sidebar.subheader("Test REST API Connection")
    
    if st.sidebar.button("Test Connection", key="test_rest_api_conn_detailed"):
        base_url, username, password, timeout = get_rest_api_connection_params()
        
        if not all([base_url, username, password]):
            st.sidebar.error("Please provide all connection details")
            return
        
        with st.sidebar:
            with st.spinner("Testing connection to REST API..."):
                st.info(f"Connecting to: {base_url}")
                st.info(f"User: {username}")
                st.info(f"Timeout: {timeout}s")
                
                session_result = create_rest_api_session(base_url, username, password, timeout)
                
                if session_result:
                    session, api_base_url = session_result
                    
                    try:
                        try:
                            info_url = f"{api_base_url.rstrip('/')}/info"
                            info_response = session.get(info_url)
                            if info_response.status_code == 200:
                                info_data = info_response.json()
                                st.info(f"API Version: {info_data.get('version', 'Unknown')}")
                                st.info(f"Server: {info_data.get('server', 'Unknown')}")
                        except:
                            pass
                        
                        try:
                            count_url = f"{api_base_url.rstrip('/')}/modulebom-simple/count"
                            count_response = session.get(count_url)
                            if count_response.status_code == 200:
                                count_data = count_response.json()
                                count = count_data.get('count', 0)
                                st.success(f"ModuleBOM_Simple: {count:,} records")
                            else:
                                st.error(f"ModuleBOM_Simple: HTTP {count_response.status_code}")
                        except Exception as endpoint_error:
                            st.error(f"ModuleBOM_Simple: {str(endpoint_error)}")
                        
                    finally:
                        session.close()

@st.cache_data(ttl=300)
def load_data_from_rest_api_cached(base_url, username, password, timeout):
    data = {
        'module_bom_simple_df': pd.DataFrame()
    }
    
    session_result = create_rest_api_session(base_url, username, password, timeout)
    
    if session_result is None:
        st.sidebar.warning("REST API connection failed - using empty data")
        return data
    
    session, api_base_url = session_result
    
    try:
        count_url = f"{api_base_url.rstrip('/')}/modulebom-simple/count"
        count_response = session.get(count_url)
        
        total_records = 0
        if count_response.status_code == 200:
            try:
                count_data = count_response.json()
                total_records = count_data.get('count', 0)
            except json.JSONDecodeError:
                st.sidebar.warning("Could not get record count, loading all available data")
        
        chunk_size = 5000
        all_data = []
        
        offset = 0
        while True:
            modulebom_url = f"{api_base_url.rstrip('/')}/modulebom-simple"
            params = {
                'limit': chunk_size,
                'offset': offset
            }
            
            response = session.get(modulebom_url, params=params)
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '')
                
                if 'application/json' in content_type:
                    try:
                        chunk_data = response.json()
                        
                        if isinstance(chunk_data, list):
                            if len(chunk_data) == 0:
                                break
                                
                            all_data.extend(chunk_data)
                            
                            if len(chunk_data) < chunk_size:
                                break
                                
                        else:
                            st.sidebar.error(f"Expected list, got {type(chunk_data)}")
                            break
                            
                    except json.JSONDecodeError as e:
                        st.sidebar.error(f"JSON decode error at offset {offset}: {str(e)}")
                        break
                        
                else:
                    st.sidebar.error(f"Expected JSON response, got: {content_type}")
                    break
            else:
                st.sidebar.error(f"Failed to load data at offset {offset}: HTTP {response.status_code}")
                break
            
            offset += chunk_size
        
        if all_data:
            data['module_bom_simple_df'] = pd.DataFrame(all_data)
        else:
            st.sidebar.warning("No data was loaded from the API")
    
    except requests.exceptions.RequestException as e:
        st.sidebar.error(f"Request error: {str(e)}")
    except Exception as e:
        st.sidebar.error(f"Unexpected error loading data from REST API: {str(e)}")
        import traceback
        st.sidebar.error(f"Traceback: {traceback.format_exc()}")
    finally:
        if session:
            session.close()
    
    return data

def load_data_from_rest_api():
    base_url, username, password, timeout = get_rest_api_connection_params()
    return load_data_from_rest_api_cached(base_url, username, password, timeout)

def search_mpn_optimized_rest_api(search_term, base_url, username, password, timeout):
    try:
        session = requests.Session()
        session.timeout = timeout
        
        auth_string = f"{username}:{password}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        session.headers.update({
            'Authorization': f'Basic {encoded_auth}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        search_url = f"{base_url.rstrip('/')}/modulebom-simple/search"
        
        params = {
            'field': 'Material_Description',
            'query': search_term,
            'distinct': True
        }
        
        response = session.get(search_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                matching_mpns = []
                for item in data:
                    if isinstance(item, dict) and 'Material_Description' in item:
                        mpn = item['Material_Description']
                        if mpn and str(mpn).strip() and str(mpn).lower() != 'nan':
                            matching_mpns.append(str(mpn))
                
                return sorted(list(set(matching_mpns)))
            else:
                st.warning("Unexpected response format from search API")
                return []
        else:
            st.error(f"Search API request failed: HTTP {response.status_code}")
            return []
            
    except Exception as e:
        st.error(f"Error searching MPNs via REST API: {e}")
        return []
    finally:
        if 'session' in locals():
            session.close()


def get_process_code_optimized_rest_api(mpn, base_url, username, password, timeout):
    session_result = create_rest_api_session(base_url, username, password, timeout)
    
    if session_result is None:
        return "API connection failed", None
    
    session, api_base_url = session_result
    
    try:
        lookup_url = f"{api_base_url.rstrip('/')}/modulebom-simple/lookup"
        params = {'mpn': mpn}
        
        response = session.get(lookup_url, params=params)
        
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '')
            
            if 'application/json' in content_type:
                try:
                    lookup_results = response.json()
                    
                    if isinstance(lookup_results, list) and lookup_results:
                        processed_results = []
                        unique_process_codes = set()
                        
                        for item in lookup_results:
                            process_code = item.get('Process_Code', 'Not Available')
                            if process_code and str(process_code).strip() and str(process_code).lower() != 'nan':
                                unique_process_codes.add(str(process_code).strip())
                            
                            processed_item = {
                                'Source': 'ModuleBOM_Simple (REST API Direct)',
                                'MPN': item.get('Material_Description', ''),
                                'Process_Code': process_code
                            }
                            
                            for key, value in item.items():
                                if key not in processed_item and not pd.isna(value):
                                    clean_key = key.replace('_', ' ').title()
                                    processed_item[clean_key] = value
                            
                            processed_results.append(processed_item)
                        
                        seen = set()
                        unique_results = []
                        for result in processed_results:
                            key = (result['Source'], result['MPN'], result['Process_Code'])
                            if key not in seen:
                                seen.add(key)
                                unique_results.append(result)
                        
                        result_df = pd.DataFrame(unique_results)
                        
                        valid_process_codes = [pc for pc in unique_process_codes if pc != "Not Available"]
                        if valid_process_codes:
                            st.info(f"Found {len(valid_process_codes)} unique process codes: {', '.join(sorted(valid_process_codes))}")
                        
                        return "Success", result_df
                    else:
                        return f"No records found for MPN: {mpn}", None
                        
                except json.JSONDecodeError as e:
                    return f"JSON decode error: {str(e)}", None
            else:
                return f"Lookup returned non-JSON response: {content_type}", None
        else:
            return f"Lookup failed: HTTP {response.status_code}", None
    
    except Exception as e:
        return f"Error looking up MPN: {str(e)}", None
    finally:
        if session:
            session.close()

def get_process_code_from_rest_api(mpn, module_bom_simple_df):
    try:
        results = []
        
        if module_bom_simple_df.empty:
            return "No data available from REST API", None
        
        available_columns = module_bom_simple_df.columns.tolist()
        
        material_desc_col = None
        possible_material_cols = ['Material_Description', 'Material Description', 'MaterialDescription', 'MATERIAL_DESCRIPTION']
        
        for col_name in possible_material_cols:
            if col_name in available_columns:
                material_desc_col = col_name
                break
        
        process_code_col = None
        possible_process_cols = ['Process_Code', 'Process Code', 'ProcessCode', 'PROCESS_CODE']
        
        for col_name in possible_process_cols:
            if col_name in available_columns:
                process_code_col = col_name
                break
        
        if not material_desc_col:
            return "Material Description column not found in data", None
        
        matches = module_bom_simple_df[
            module_bom_simple_df[material_desc_col].astype(str).str.contains(mpn, case=False, na=False)
        ]
        
        if matches.empty:
            return f"No records found for MPN: {mpn}", None
        
        unique_process_codes = set()
        
        for _, row in matches.iterrows():
            process_code_value = "Not Available"
            if process_code_col and process_code_col in row.index:
                pc_val = row.get(process_code_col)
                if pd.notna(pc_val) and str(pc_val).strip() and str(pc_val).lower() != 'nan':
                    process_code_value = str(pc_val).strip()
                    unique_process_codes.add(process_code_value)
            
            result_row = {
                'Source': 'ModuleBOM_Simple (REST API)',
                'MPN': row.get(material_desc_col, ''),
                'Material Description': row.get(material_desc_col, ''),
                'Process Code': process_code_value,
            }
            
            results.append(result_row)
        
        if not results:
            return f"No records found for MPN: {mpn}", None
        
        seen = set()
        unique_results = []
        for result in results:
            key = (result['Source'], result['MPN'], result.get('Material Number', ''), result['Process Code'])
            if key not in seen:
                seen.add(key)
                unique_results.append(result)
        
        result_df = pd.DataFrame(unique_results)
        
        if process_code_col:
            valid_process_codes = [pc for pc in unique_process_codes if pc != "Not Available"]
            if valid_process_codes:
                st.info(f"Process Codes: {', '.join(sorted(valid_process_codes))}")
            else:
                st.warning(f"Found Process Code column: '{process_code_col}' but no valid process codes")
        else:
            st.warning(f"Process Code column not found. Available columns: {', '.join(available_columns)}")
        
        return "Success", result_df
        
    except Exception as e:
        return f"Error looking up MPN: {e}", None

def analyze_rest_api_data_optimized(base_url, username, password, timeout):
    session_result = create_rest_api_session(base_url, username, password, timeout)
    
    if session_result is None:
        st.error("Cannot analyze data - API connection failed")
        return
    
    session, api_base_url = session_result
    
    try:
        st.subheader("REST API Data Analysis")
        
        info_url = f"{api_base_url.rstrip('/')}/modulebom-simple/info"
        response = session.get(info_url)
        
        if response.status_code == 200:
            try:
                info_data = response.json()
                st.write(f"**Total Records:** {info_data.get('record_count', 'Unknown')}")
                st.write(f"**Available Columns:** {len(info_data.get('columns', []))}")
                
                if info_data.get('columns'):
                    with st.expander("View All Columns"):
                        for col in info_data['columns']:
                            st.write(f"- {col}")
                            
            except json.JSONDecodeError as e:
                st.error(f"Error parsing info response: {str(e)}")
        
        mpn_count_url = f"{api_base_url.rstrip('/')}/modulebom-simple/mpn-count"
        response = session.get(mpn_count_url)
        
        if response.status_code == 200:
            try:
                mpn_data = response.json()
                st.write(f"**Unique MPNs:** {mpn_data.get('unique_mpns', 'Unknown')}")
            except json.JSONDecodeError as e:
                st.error(f"Error parsing MPN count response: {str(e)}")
        
        sample_url = f"{api_base_url.rstrip('/')}/modulebom-simple/sample?limit=3"
        response = session.get(sample_url)
        
        if response.status_code == 200:
            try:
                sample_data = response.json()
                if sample_data.get('sample_data'):
                    st.subheader("Sample Data")
                    sample_df = pd.DataFrame(sample_data['sample_data'])
                    st.dataframe(sample_df)
            except json.JSONDecodeError as e:
                st.error(f"Error parsing sample data response: {str(e)}")
    
    except Exception as e:
        st.error(f"Error analyzing REST API data: {str(e)}")
    finally:
        if session:
            session.close()

def search_mpn_in_sql(search_term, module_bom_59only_df, module_bom_simple_df):
    matching_mpns = []
    
    try:
        if not module_bom_59only_df.empty and 'Material Description' in module_bom_59only_df.columns:
            matches = module_bom_59only_df[
                module_bom_59only_df['Material Description'].astype(str).str.contains(search_term, case=False, na=False)
            ]['Material Description'].unique()
            matching_mpns.extend(matches)
        
        if not module_bom_simple_df.empty and 'Material Description' in module_bom_simple_df.columns:
            matches = module_bom_simple_df[
                module_bom_simple_df['Material Description'].astype(str).str.contains(search_term, case=False, na=False)
            ]['Material Description'].unique()
            matching_mpns.extend(matches)
        
        for df, df_name in [(module_bom_59only_df, '59only'), (module_bom_simple_df, 'Simple')]:
            if not df.empty:
                for col in df.columns:
                    if any(term in col.lower() for term in ['mpn', 'material number', 'part number']):
                        matches = df[
                            df[col].astype(str).str.contains(search_term, case=False, na=False)
                        ][col].unique()
                        matching_mpns.extend(matches)
        
        matching_mpns = sorted(list(set([mpn for mpn in matching_mpns if mpn and str(mpn).strip() and str(mpn).lower() != 'nan'])))
        
    except Exception as e:
        st.error(f"Error searching MPNs: {e}")
    
    return matching_mpns


def search_mpn_in_rest_api(search_term, module_bom_simple_df):
    matching_mpns = []
    
    try:
        if module_bom_simple_df.empty:
            return matching_mpns
        
        material_desc_col = None
        possible_material_cols = ['Material_Description', 'Material Description', 'MaterialDescription', 'MATERIAL_DESCRIPTION']
        
        for col_name in possible_material_cols:
            if col_name in module_bom_simple_df.columns:
                material_desc_col = col_name
                break
        
        if material_desc_col:
            matches = module_bom_simple_df[
                module_bom_simple_df[material_desc_col].astype(str).str.contains(search_term, case=False, na=False)
            ][material_desc_col].unique()
            matching_mpns.extend(matches)
        
        for col in module_bom_simple_df.columns:
            if any(term in col.lower() for term in ['mpn', 'material number', 'part number']) and col != material_desc_col:
                matches = module_bom_simple_df[
                    module_bom_simple_df[col].astype(str).str.contains(search_term, case=False, na=False)
                ][col].unique()
                matching_mpns.extend(matches)
        
        matching_mpns = sorted(list(set([
            mpn for mpn in matching_mpns 
            if mpn and str(mpn).strip() and str(mpn).lower() != 'nan'
        ])))
        
    except Exception as e:
        st.error(f"Error searching MPNs in REST API data: {e}")
    
    return matching_mpns

def convert_process_code_to_print_order(process_code, segment):
    if not process_code or not isinstance(process_code, str):
        return ""
    
    process_code = process_code.upper().strip()
    
    if segment.lower() == 'server':
        if len(process_code) >= 4:
            print_order = f"{process_code[0]}{process_code[3]}{process_code[1]}{process_code[2]}"
            if len(process_code) >= 5:
                print_order += process_code[4]
            return print_order
        else:
            return process_code
    
    elif segment.lower() == 'client':
        return process_code
    
    else:
        return process_code

def convert_print_order_to_process_code(print_order_code, segment):
    if not print_order_code or not isinstance(print_order_code, str):
        return ""
    
    print_order_code = print_order_code.upper().strip()
    
    if segment.lower() == 'server':
        if len(print_order_code) >= 4:
            position_order = f"{print_order_code[0]}{print_order_code[2]}{print_order_code[3]}{print_order_code[1]}"
            if len(print_order_code) >= 5:
                position_order += print_order_code[4]
            return position_order
        else:
            return print_order_code
    
    elif segment.lower() == 'client':
        return print_order_code
    
    else:
        return print_order_code

def explain_print_order_process_code(print_order_code, segment):
    if not print_order_code or not isinstance(print_order_code, str):
        return "Invalid process code"
    
    explanation = []
    explanation.append(f"**Process Code (Print Order): {print_order_code}** (Segment: {segment})")
    explanation.append("**Print Order Breakdown:**")
    
    if segment.lower() == 'server':
        print_components = {
            1: "PMIC",
            2: "RCD/MRCD", 
            3: "SPD/Hub",
            4: "Temp Sensor",
            5: "Data Buffer"
        }
        
        for i, char in enumerate(print_order_code.upper(), 1):
            if i in print_components:
                component_type = print_components[i]
                explanation.append(f"- **Print Position {i}**: {component_type} → **{char}**")
        
        explanation.append(f"\n**This is the order shown on the product label**")
        
        position_order = convert_print_order_to_process_code(print_order_code, segment)
        if position_order != print_order_code:
            explanation.append(f"**Corresponding Position Order**: {position_order}")
    
    elif segment.lower() == 'client':
        print_components = {
            1: "PMIC",
            2: "SPD/Hub",
            3: "CKD"
        }
        
        for i, char in enumerate(print_order_code.upper(), 1):
            if i in print_components:
                component_type = print_components[i]
                explanation.append(f"- **Print Position {i}**: {component_type} → **{char}**")
        
        explanation.append(f"\n**This is the order shown on the product label**")
    
    else:
        explanation.append("Unknown market segment. Cannot provide detailed breakdown.")
    
    return "\n".join(explanation)

def get_process_code_optimized_rest_api(mpn, base_url, username, password, timeout):
    try:
        session = requests.Session()
        session.timeout = timeout
        
        auth_string = f"{username}:{password}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        session.headers.update({
            'Authorization': f'Basic {encoded_auth}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        lookup_url = f"{base_url.rstrip('/')}/modulebom-simple/lookup"
        
        params = {'mpn': mpn}
        
        response = session.get(lookup_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                simplified_results = []
                for item in data:
                    simplified_item = {
                        'Source': 'ModuleBOM_Simple (REST API Direct)',
                        'MPN': item.get('Material_Description', ''),
                        'Process_Code': item.get('Process_Code', 'Not Available')
                    }
                    simplified_results.append(simplified_item)
                
                result_df = pd.DataFrame(simplified_results)
                return "Success", result_df
            else:
                return f"No records found for MPN: {mpn}", None
        else:
            return f"Lookup API request failed: HTTP {response.status_code}", None
            
    except Exception as e:
        return f"Error looking up MPN via REST API: {e}", None
    finally:
        if 'session' in locals():
            session.close()

def analyze_rest_api_data(module_bom_simple_df):
    st.subheader("REST API Data Analysis")
    
    if not module_bom_simple_df.empty:
        st.write(f"**Total records**: {len(module_bom_simple_df)}")
        st.write(f"**Columns**: {len(module_bom_simple_df.columns)}")
        
        with st.expander("Column Names", expanded=False):
            for col in sorted(module_bom_simple_df.columns):
                st.write(f"- {col}")
        
        with st.expander("Sample Data", expanded=False):
            st.dataframe(module_bom_simple_df.head())
        
        process_code_cols = [col for col in module_bom_simple_df.columns if 'process' in col.lower() and 'code' in col.lower()]
        
        if process_code_cols:
            for col in process_code_cols:
                process_codes = module_bom_simple_df[col].dropna().unique()
                st.write(f"**Unique Process Codes in {col}**: {len(process_codes)}")
                
                with st.expander(f"Process Codes Found in {col}", expanded=False):
                    for pc in sorted(process_codes):
                        if pc and str(pc).strip():
                            st.write(f"- {pc}")
        else:
            st.warning("No Process Code column found in the data")
        
        material_desc_cols = [col for col in module_bom_simple_df.columns if 'material' in col.lower() and 'description' in col.lower()]
        
        if material_desc_cols:
            for col in material_desc_cols:
                unique_materials = module_bom_simple_df[col].dropna().nunique()
                st.write(f"**Unique Materials in {col}**: {unique_materials}")
    else:
        st.write("No REST API data available")

def load_data_from_sharepoint():
    data = {
        'component_validations_df': pd.DataFrame(),
        'module_validation_df': pd.DataFrame(),
        'end_products_df': pd.DataFrame()
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
        end_products_data = []
        
        if end_products_list_name in available_lists:
            end_products_list = ctx.web.lists.get_by_title(end_products_list_name)
            st.sidebar.success(f"Found '{end_products_list_name}' list for speed information")
        else:
            pass
        
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
        speed_mapping = {}
        
        if end_products_list:
            caml_query = CamlQuery()
            caml_query.ViewXml = f"<View><RowLimit>5000</RowLimit></View>"
            end_products_items = end_products_list.get_items(caml_query).execute_query()
            st.sidebar.success(f"Retrieved {len(end_products_items)} items from End Products list")
            
            for item in end_products_items:
                item_props = item.properties
                product_name = str(item_props.get('Title', ''))
                speed_value = str(item_props.get('Speed', ''))
                
                import re
                speed_numbers = re.findall(r'\d+', speed_value)
                if speed_numbers:
                    speed_mapping[product_name] = speed_numbers[0]
                
                end_product_record = {
                    'Title': product_name,
                    'Speed': speed_value,
                    'Process_Code': str(item_props.get('Process_Code', '')),
                    'End_Products': str(item_props.get('End_Products', '')),
                    'Product_Comment': str(item_props.get('Product_Comment', '')),
                    'Form_Factor': str(item_props.get('Form_Factor', '')),
                    'Segment': str(item_props.get('Segment', ''))
                }
                
                for prop_key, prop_value in item_props.items():
                    if prop_key not in end_product_record and prop_key not in ['_ObjectType_', '_ObjectIdentity_', 'FileSystemObjectType']:
                        end_product_record[prop_key] = str(prop_value)
                
                end_products_data.append(end_product_record)
        
        data['end_products_df'] = pd.DataFrame(end_products_data)
        
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
            'Component_Type': 'Component_x0020_Type',
            'Process_Code': 'Process_x0020_Code',
            'SPN': 'Supplier_x0020_PN',
            'Speed': 'Product_x0020_Comment',
            'Product_Description': 'Title',
            'Product_Status': 'Product_x0020_Status'
        }

        for item in all_items:
            item_properties = item.properties
            
            record = {}
            for key, field in field_mapping.items():
                if field and field in item_properties:
                    if key == 'Component_Type':
                        component_type_value = str(item_properties[field])
                        if component_type_value and component_type_value.lower() != 'none':
                            if component_type_value.lower() == 'pmic':
                                component_type_value = 'PMIC'
                            record[key] = component_type_value
                        else:
                            title = str(item_properties.get('Title', ''))
                            component_type = next((ct for ct in valid_component_types if ct.lower() in title.lower()), "Unknown")
                            if component_type.lower() == 'pmic':
                                component_type = 'PMIC'
                            record[key] = component_type
                    else:
                        record[key] = str(item_properties[field])
                else:
                    record[key] = ""
            
            if not record.get('Component_Type') or record['Component_Type'] == 'Unknown':
                title = str(item_properties.get('Title', ''))
                component_type = next((ct for ct in valid_component_types if ct.lower() in title.lower()), "Unknown")
                if component_type.lower() == 'pmic':
                    component_type = 'PMIC'
                record['Component_Type'] = component_type
            
            if 'Speed' in record and not record['Speed'] and 'Product_x0020_Comment' in item_properties:
                comment = str(item_properties['Product_x0020_Comment'])
                import re
                speed_numbers = re.findall(r'\b\d{4,5}\b', comment)
                if speed_numbers:
                    record['Speed'] = speed_numbers[0]
            
            product_name = str(item_properties.get('Title', ''))
            if end_products_list and not record['Speed'] and product_name in speed_mapping:
                record['Speed'] = speed_mapping[product_name]
            
            if not any(record.values()) or record.get('Component_Type') == 'Unknown':
                for prop_key, prop_value in item_properties.items():
                    if prop_key not in ['_ObjectType_', '_ObjectIdentity_', 'FileSystemObjectType', 'ServerRedirectedEmbedUri', 
                                       'ServerRedirectedEmbedUrl', 'ContentTypeId', 'ComplianceAssetId', 'OData__UIVersionString']:
                        if prop_key == 'Title' and not record.get('Product_Description'):
                            record['Product_Description'] = str(prop_value)
                            if not record.get('Component_Type') or record['Component_Type'] == 'Unknown':
                                title = str(prop_value)
                                component_type = next((ct for ct in valid_component_types if ct.lower() in title.lower()), "Unknown")
                                if component_type.lower() == 'pmic':
                                    component_type = 'PMIC'
                                record['Component_Type'] = component_type
                        elif prop_key == 'Segment' and not record.get('Segment'):
                            record['Segment'] = str(prop_value)
                        elif prop_key == 'Supplier' and not record.get('Supplier'):
                            record['Supplier'] = str(prop_value)
                        elif prop_key == 'Family_x0020_Description' and not record.get('Component_Generation'):
                            record['Component_Generation'] = str(prop_value)
                        elif prop_key == 'REV' and not record.get('Revision'):
                            record['Revision'] = str(prop_value)
                        elif prop_key == 'Process_x0020_Code' and not record.get('Process_Code'):
                            record['Process_Code'] = str(prop_value)
                        elif prop_key == 'Supplier_x0020_PN' and not record.get('SPN'):
                            record['SPN'] = str(prop_value)
                        elif prop_key == 'Speed' and not record.get('Speed'):
                            record['Speed'] = str(prop_value)
                        elif 'speed' in prop_key.lower() and not record.get('Speed'):
                            record['Speed'] = str(prop_value)
                        elif 'status' in prop_key.lower() and not record.get('Product_Status'):
                            record['Product_Status'] = str(prop_value)
            
            if not record.get('Product_Description'):
                record['Product_Description'] = str(item_properties.get('Title', ''))
            
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
    
    return data

def get_component_process_code(segment, supplier, component_gen, revision, component_type, component_validations_df):
    try:
        if component_validations_df.empty:
            return "No component validation data available", None, None
        
        df = component_validations_df.copy()
        
        if component_type.lower() == 'pmic':
            component_type = 'PMIC'
        
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.lower()
        
        if component_gen and component_type:
            component_type_lower = component_type.lower()
            component_gen_lower = component_gen.lower()
            
            if segment and segment.lower() == 'server':
                if any(ct in component_type_lower for ct in ['temp sensor', 'rcd', 'mrcd', 'data buffer']):
                    valid_gen_options = ["gen1", "gen2", "gen3", "gen4", "gen5", "na"]
                    if not any(valid_gen.lower() in component_gen_lower for valid_gen in valid_gen_options):
                        return f"Invalid component generation for {component_type}. Must be one of: Gen1, Gen2, Gen3, Gen4, Gen5, or NA", None, None
        
        if segment and 'Segment' in df.columns:
            segment_lower = segment.lower()
            
            segment_mask = (df['Segment'] == segment_lower) | \
                           (df['Segment'] == 'server/client')
            
            df = df[segment_mask]
            
            if df.empty:
                return f"No components found for segment: {segment}", None, None
        
        filters = []
        if supplier and 'Supplier' in df.columns:
            filters.append(df['Supplier'] == supplier.lower())
        if component_gen and 'Component_Generation' in df.columns:
            filters.append(df['Component_Generation'] == component_gen.lower())
        if revision and 'Revision' in df.columns:
            filters.append(df['Revision'] == revision.lower())
        if component_type and 'Component_Type' in df.columns:
            if segment and segment.lower() == 'client':
                if 'ckd' in component_type.lower():
                    filters.append(df['Component_Type'] == 'ckd')
                elif 'spd/hub' in component_type.lower():
                    filters.append(df['Component_Type'] == 'spd/hub')
                elif 'pmic' in component_type.lower():
                    filters.append(df['Component_Type'] == 'pmic')
                elif 'inductor' in component_type.lower():
                    filters.append(df['Component_Type'] == 'inductor')
                elif 'voltage regulator' in component_type.lower():
                    filters.append(df['Component_Type'] == 'voltage regulator')
                else:
                    filters.append(df['Component_Type'] == component_type.lower())
            else:
                if 'rcd/mrcd' in component_type.lower():
                    filters.append(df['Component_Type'].isin(['rcd', 'muxed rcd']))
                elif 'temp sensor' in component_type.lower():
                    filters.append(df['Component_Type'] == 'temp sensor')
                elif 'data buffer' in component_type.lower():
                    filters.append(df['Component_Type'] == 'data buffer')
                elif 'spd/hub' in component_type.lower():
                    filters.append(df['Component_Type'] == 'spd/hub')
                elif 'pmic' in component_type.lower():
                    filters.append(df['Component_Type'] == 'pmic')
                elif 'ckd' in component_type.lower():
                    filters.append(df['Component_Type'] == 'ckd')
                elif 'inductor' in component_type.lower():
                    filters.append(df['Component_Type'] == 'inductor')
                elif 'voltage regulator' in component_type.lower():
                    filters.append(df['Component_Type'] == 'voltage regulator')
                else:
                    filters.append(df['Component_Type'] == component_type.lower())
        
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
                        relaxed_filters.append(df['Component_Type'].str.contains('ckd|clock', na=False))
                    elif 'spd/hub' in component_type.lower():
                        relaxed_filters.append(df['Component_Type'].str.contains('spd|hub|serial', na=False))
                    elif 'pmic' in component_type.lower():
                        relaxed_filters.append(df['Component_Type'].str.contains('pmic|power', na=False))
                else:
                    if 'rcd/mrcd' in component_type.lower():
                        relaxed_filters.append(df['Component_Type'].str.contains('rcd|muxed|register', na=False))
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
                        'ckd': ['ckd', 'clock driver', 'clock'],
                        'inductor': ['inductor'],
                        'voltage regulator': ['voltage regulator', 'regulator']
                    }
                else:
                    type_variations = {
                        'pmic': ['pmic', 'power', 'power management'],
                        'spd/hub': ['spd', 'hub', 'spd/hub', 'serial presence detect'],
                        'temp sensor': ['temp', 'sensor', 'temperature', 'temp sensor'],
                        'rcd/mrcd': ['rcd', 'mrcd', 'register', 'registering clock driver', 'muxed rcd'],
                        'data buffer': ['buffer', 'data buffer', 'db'],
                        'ckd': ['ckd', 'clock driver', 'clock'],
                        'inductor': ['inductor'],
                        'voltage regulator': ['voltage regulator', 'regulator']
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
        if component_type_result.lower() == 'pmic':
            component_type_result = 'PMIC'
        
        original_df = component_validations_df.copy()
        original_filtered = original_df[original_df.index.isin(filtered_df.index)]
        
        if 'Component_Type' in original_filtered.columns:
            original_filtered = original_filtered.copy()
            original_filtered['Component_Type'] = original_filtered['Component_Type'].apply(
                lambda x: 'PMIC' if str(x).lower() == 'pmic' else str(x)
            )
        
        return process_code, component_type_result, original_filtered
    
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

def lookup_parts_by_process_code(process_code, component_validations_df, speed_filter="NA"):
    try:
        if component_validations_df.empty:
            return "No component validation data available"
        
        filtered_df = component_validations_df[
            component_validations_df['Process_Code'].astype(str).str.upper() == process_code.upper()
        ].copy()
        
        if filtered_df.empty:
            return f"No components found for process code: {process_code}"
        
        if speed_filter and speed_filter != "NA":
            if speed_filter.startswith("<"):
                speed_threshold = int(speed_filter[1:])
                speed_mask = pd.to_numeric(filtered_df['Speed'], errors='coerce') < speed_threshold
            elif speed_filter.startswith(">"):
                speed_threshold = int(speed_filter[1:])
                speed_mask = pd.to_numeric(filtered_df['Speed'], errors='coerce') > speed_threshold
            else:
                speed_mask = filtered_df['Speed'].astype(str) == speed_filter
            
            filtered_df = filtered_df[speed_mask | filtered_df['Speed'].isna()]
        
        filtered_df['Component_Type'] = filtered_df['Component_Type'].apply(
            lambda x: 'PMIC' if str(x).lower() == 'pmic' else str(x)
        )
        
        display_columns = {
            'Process_Code': 'Process Code',
            'Product_Description': 'Product Description',
            'Product_Status': 'Product Status',
            'Component_Type': 'Component Type',
            'Segment': 'Segment',
            'Component_Generation': 'Component Generation',
            'Supplier': 'Supplier',
            'SPN': 'Supplier PN',
            'Speed': 'Speed'
        }
        
        available_columns = {k: v for k, v in display_columns.items() if k in filtered_df.columns}
        
        result_df = filtered_df[list(available_columns.keys())].copy()
        result_df = result_df.rename(columns=available_columns)
        
        for col in result_df.columns:
            if col == 'Component Type':
                result_df[col] = result_df[col].apply(
                    lambda x: 'PMIC' if str(x).lower() == 'pmic' else str(x).title() if str(x) not in ['Unknown', 'NA', ''] else str(x)
                )
            elif col == 'Product Description':
                result_df[col] = result_df[col].apply(
                    lambda x: x if str(x) not in ['', 'nan', 'None', 'Unknown'] else 'Description not available'
                )
            elif col == 'Product Status':
                result_df[col] = result_df[col].apply(
                    lambda x: x if str(x) not in ['', 'nan', 'None', 'Unknown'] else 'Status not available'
                )
            else:
                result_df[col] = result_df[col].apply(
                    lambda x: str(x).upper() if str(x) not in ['Unknown', 'NA', ''] else str(x)
                )
        
        return result_df
        
    except Exception as e:
        return f"Error looking up process code: {str(e)}"
    
def create_position_based_filters(process_code, component_validations_df, segment):
    
    if not process_code:
        return {}
    
    if segment.lower() == 'server':
        position_components = {
            1: "PMIC",
            2: "SPD/Hub", 
            3: "Temp Sensor",
            4: "RCD/MRCD",
            5: "Data Buffer"
        }
    else:
        position_components = {
            1: "PMIC",
            2: "SPD/Hub",
            3: "CKD"
        }
    
    position_filters = {}
    
    for i, char in enumerate(process_code.upper(), 1):
        if i in position_components:
            component_type = position_components[i]
            
            if component_type == "RCD/MRCD":
                position_data = component_validations_df[
                    (component_validations_df['Component_Type'].str.contains('RCD', case=False, na=False)) |
                    (component_validations_df['Component_Type'].str.contains('Muxed RCD', case=False, na=False))
                ]
            elif component_type == "SPD/Hub":
                position_data = component_validations_df[
                    component_validations_df['Component_Type'].str.contains('SPD', case=False, na=False)
                ]
            elif component_type == "Temp Sensor":
                position_data = component_validations_df[
                    component_validations_df['Component_Type'].str.contains('Temp', case=False, na=False)
                ]
            elif component_type == "Data Buffer":
                position_data = component_validations_df[
                    component_validations_df['Component_Type'].str.contains('Buffer', case=False, na=False)
                ]
            else:
                position_data = component_validations_df[
                    component_validations_df['Component_Type'].str.contains(component_type, case=False, na=False)
                ]
            
            if not position_data.empty and 'Segment' in position_data.columns:
                segment_mask = (position_data['Segment'].astype(str).str.lower() == segment.lower()) | \
                               (position_data['Segment'].astype(str).str.lower() == 'server/client')
                position_data = position_data[segment_mask]
            
            if not position_data.empty and 'Process_Code' in position_data.columns:
                char_matches = position_data[
                    position_data['Process_Code'].astype(str).str.upper() == char
                ]
                if not char_matches.empty:
                    position_data = char_matches
            
            if not position_data.empty:
                suppliers = position_data['Supplier'].dropna().unique().tolist() if 'Supplier' in position_data.columns else []
                generations = position_data['Component_Generation'].dropna().unique().tolist() if 'Component_Generation' in position_data.columns else []
                revisions = position_data['Revision'].dropna().unique().tolist() if 'Revision' in position_data.columns else []
                
                suppliers = sorted([s for s in suppliers if s and str(s).strip() and str(s).lower() != 'nan'])
                generations = sorted([g for g in generations if g and str(g).strip() and str(g).lower() != 'nan'])
                revisions = sorted([r for r in revisions if r and str(r).strip() and str(r).lower() != 'nan'])
                
                position_filters[f"position_{i}"] = {
                    'component_type': component_type,
                    'process_code_char': char,
                    'suppliers': suppliers,
                    'generations': generations,
                    'revisions': revisions,
                    'available_parts': len(position_data)
                }
    
    return position_filters

def lookup_parts_with_position_filters(process_code, position_filters, component_validations_df, speed=None, form_factor=None, end_products_df=None, segment=None):
    
    try:
        if not process_code:
            return "No process code provided"
        
        if component_validations_df.empty:
            return "No component validation data available"
        
        all_results = []
        
        for i, char in enumerate(process_code.upper(), 1):
            position_key = f"position_{i}"
            
            if position_key in position_filters:
                filter_data = position_filters[position_key]
                component_type = filter_data['component_type']
                
                if component_type == "RCD/MRCD":
                    position_data = component_validations_df[
                        (component_validations_df['Component_Type'].str.contains('RCD', case=False, na=False)) |
                        (component_validations_df['Component_Type'].str.contains('Muxed RCD', case=False, na=False))
                    ]
                elif component_type == "SPD/Hub":
                    position_data = component_validations_df[
                        component_validations_df['Component_Type'].str.contains('SPD', case=False, na=False)
                    ]
                elif component_type == "Temp Sensor":
                    position_data = component_validations_df[
                        component_validations_df['Component_Type'].str.contains('Temp', case=False, na=False)
                    ]
                elif component_type == "Data Buffer":
                    position_data = component_validations_df[
                        component_validations_df['Component_Type'].str.contains('Buffer', case=False, na=False)
                    ]
                else:
                    position_data = component_validations_df[
                        component_validations_df['Component_Type'].str.contains(component_type, case=False, na=False)
                    ]
                
                if segment and not position_data.empty and 'Segment' in position_data.columns:
                    segment_mask = (position_data['Segment'].astype(str).str.lower() == segment.lower()) | \
                                   (position_data['Segment'].astype(str).str.lower() == 'server/client')
                    position_data = position_data[segment_mask]
                
                if not position_data.empty and 'Process_Code' in position_data.columns:
                    position_data = position_data[
                        position_data['Process_Code'].astype(str).str.upper() == char
                    ]
                
                if filter_data.get('supplier'):
                    position_data = position_data[
                        position_data['Supplier'].astype(str).str.lower() == filter_data['supplier'].lower()
                    ]
                
                if filter_data.get('generation'):
                    position_data = position_data[
                        position_data['Component_Generation'].astype(str).str.lower() == filter_data['generation'].lower()
                    ]
                
                if filter_data.get('revision'):
                    position_data = position_data[
                        position_data['Revision'].astype(str).str.lower() == filter_data['revision'].lower()
                    ]
                
                for _, row in position_data.iterrows():
                    segment_value = row.get('Segment', "Unknown")
                    if segment_value == 'server/client':
                        segment_display = "Server/Client"
                    else:
                        segment_display = segment_value.title() if segment_value != "unknown" else "Unknown"
                    
                    all_results.append({
                        'Position': f'Position {i} ({component_type})',
                        'Process Code': char,
                        'Segment': segment_display,
                        'Supplier': row.get('Supplier', "Unknown").upper(),
                        'Component Generation': row.get('Component_Generation', "Unknown").upper(),
                        'Revision': row.get('Revision', "Unknown").upper(),
                        'Component Type': row.get('Component_Type', "Unknown").title(),
                        'Speed': row.get('Speed', "Unknown"),
                        'SPN': row.get('SPN', "Unknown"),
                        'Product Status': row.get('Product_Status', "Unknown"),
                        'Package': row.get('Package', "Unknown")
                    })
        
        if not all_results:
            return f"No matching components found for process code: {process_code} with applied filters"
        
        result_df = pd.DataFrame(all_results)
        
        if speed and speed not in ["No Filter", "NA"] and end_products_df is not None and not end_products_df.empty:
            compatible_speed_codes = set()
            
            for _, row in end_products_df.iterrows():
                row_process_code = str(row.get('Process_Code', ''))
                if not row_process_code:
                    continue
                
                end_products_text = str(row.get('End Products', ''))
                product_comment = str(row.get('Product Comment', ''))
                combined_text = f"{end_products_text} {product_comment}"
                
                speed_compatible = False
                
                if speed.isdigit():
                    if speed in combined_text:
                        speed_compatible = True
                elif speed.startswith('<'):
                    try:
                        threshold = int(speed[1:])
                        import re
                        found_speeds = re.findall(r'\b(\d{4,5})\b', combined_text)
                        if any(int(s) < threshold for s in found_speeds if s.isdigit()):
                            speed_compatible = True
                        if f"<{threshold}" in combined_text:
                            speed_compatible = True
                    except:
                        pass
                elif speed.startswith('>'):
                    try:
                        threshold = int(speed[1:])
                        import re
                        found_speeds = re.findall(r'\b(\d{4,5})\b', combined_text)
                        if any(int(s) > threshold for s in found_speeds if s.isdigit()):
                            speed_compatible = True
                    except:
                        pass
                elif '-' in speed:
                    if speed in combined_text:
                        speed_compatible = True
                
                if speed_compatible:
                    compatible_speed_codes.add(row_process_code)
            
            if compatible_speed_codes:
                speed_filtered_results = []
                for _, row in result_df.iterrows():
                    row_process_code = row.get('Process Code', '')
                    if any(row_process_code in compatible_code for compatible_code in compatible_speed_codes):
                        speed_filtered_results.append(row.to_dict())
                
                if speed_filtered_results:
                    result_df = pd.DataFrame(speed_filtered_results)
                else:
                    return f"No components found that are compatible with speed {speed} MT/s"
        
        if form_factor and form_factor not in ["No Filter", "NA"] and end_products_df is not None and not end_products_df.empty:
            compatible_form_factor_codes = set()
            
            for _, row in end_products_df.iterrows():
                row_process_code = str(row.get('Process_Code', ''))
                if not row_process_code:
                    continue
                
                end_products_text = str(row.get('End Products', ''))
                form_factor_field = str(row.get('Form_Factor', ''))
                combined_text = f"{end_products_text} {form_factor_field}"
                
                if form_factor.upper() in combined_text.upper():
                    compatible_form_factor_codes.add(row_process_code)
            
            if compatible_form_factor_codes:
                ff_filtered_results = []
                for _, row in result_df.iterrows():
                    row_process_code = row.get('Process Code', '')
                    if any(row_process_code in compatible_code for compatible_code in compatible_form_factor_codes):
                        ff_filtered_results.append(row.to_dict())
                
                if ff_filtered_results:
                    result_df = pd.DataFrame(ff_filtered_results)
                else:
                    return f"No components found that are compatible with form factor {form_factor}"
            else:
                return f"No process codes found that are compatible with form factor {form_factor}"
        
        result_df = result_df.drop_duplicates(subset=['Position', 'Process Code', 'Supplier', 'Component Generation', 'Revision'])
        
        if not result_df.empty:
            position_order = {}
            for i in range(1, 6):
                for comp_type in ["PMIC", "SPD/Hub", "Temp Sensor", "RCD/MRCD", "Data Buffer", "CKD"]:
                    position_order[f'Position {i} ({comp_type})'] = i
            
            result_df['sort_order'] = result_df['Position'].map(position_order).fillna(50)
            result_df = result_df.sort_values(['sort_order', 'Component Type', 'Supplier'])
            result_df = result_df.drop('sort_order', axis=1)
        
        return result_df
        
    except Exception as e:
        return f"Error looking up parts with position filters: {e}"

def explain_position_based_process_code(process_code, segment, position_filters):
    if not process_code or not isinstance(process_code, str):
        return "Invalid process code"
    
    explanation = []
    explanation.append(f"**Process Code: {process_code}** (Segment: {segment})")
    explanation.append("**Position Breakdown:**")
    
    if segment.lower() == 'server':
        position_components = {
            1: "PMIC",
            2: "SPD/Hub", 
            3: "Temp Sensor",
            4: "RCD/MRCD",
            5: "Data Buffer"
        }
        print_order = "PMIC → RCD → SPD/Hub → Temp Sensor → Data Buffer (if applicable)"
    else:
        position_components = {
            1: "PMIC",
            2: "SPD/Hub",
            3: "CKD"
        }
        print_order = "PMIC → SPD/Hub → CKD (if applicable)"
    
    for i, char in enumerate(process_code.upper(), 1):
        if i in position_components:
            component_type = position_components[i]
            position_key = f"position_{i}"
            
            if position_key in position_filters:
                filter_data = position_filters[position_key]
                parts_count = filter_data.get('available_parts', 0)
                explanation.append(f"- **Position {i}**: {component_type} → **{char}** ({parts_count} parts available)")
                
                applied_filters = []
                if filter_data.get('supplier'):
                    applied_filters.append(f"Supplier: {filter_data['supplier']}")
                if filter_data.get('generation'):
                    applied_filters.append(f"Generation: {filter_data['generation']}")
                if filter_data.get('revision'):
                    applied_filters.append(f"Revision: {filter_data['revision']}")
                
                if applied_filters:
                    explanation.append(f"  *Filters applied: {', '.join(applied_filters)}*")
            else:
                explanation.append(f"- **Position {i}**: {component_type} → **{char}** (No data available)")
    
    explanation.append(f"\n**Process Code Print Order** (as shown on product label):")
    explanation.append(print_order)
    
    return "\n".join(explanation)

def get_component_type_options_by_segment(segment):
    if segment.lower() == 'client':
        return ['PMIC', 'SPD/Hub', 'CKD', 'Inductor', 'Voltage Regulator']
    else:
        return ["CKD", "Data Buffer", "Inductor", "Muxed RCD", "PMIC", "RCD", "SPD/Hub", "Temp Sensor", "Voltage Regulator"]

def get_module_component_options_by_segment(segment):
    if segment.lower() == 'client':
        return {
            "PMIC": {"required": True, "position": 1, "print_position": 1},
            "SPD/Hub": {"required": True, "position": 2, "print_position": 2},
            "CKD": {"required": False, "position": 3, "print_position": 3}
        }
    else:
        return {
            "PMIC": {"required": True, "position": 1, "print_position": 1},
            "SPD/Hub": {"required": True, "position": 2, "print_position": 3},
            "Temp Sensor": {"required": True, "position": 3, "print_position": 4},
            "RCD/MRCD": {"required": True, "position": 4, "print_position": 2},
            "Data Buffer": {"required": False, "position": 5, "print_position": 5}
        }
    
def get_speed_options_from_end_products(end_products_df):
    default_speeds = ["No Filter", "NA", "4800", "5600", "6400", "7200", "8000", "8800", "9600", "12800", "<8000", ">8000"]
    
    if end_products_df is None or end_products_df.empty:
        return default_speeds
    
    try:
        speeds = set()
        
        if 'End Products' in end_products_df.columns:
            end_products_text = end_products_df['End Products'].dropna().astype(str)
            
            import re
            for text in end_products_text:
                speed_matches = re.findall(r'(\d{4,5})', text)
                for speed in speed_matches:
                    speeds.add(speed)
                
                range_matches = re.findall(r'(\d{4,5})-(\d{4,5})', text)
                for start, end in range_matches:
                    speeds.add(f"{start}-{end}")
                
                if '<' in text:
                    comp_matches = re.findall(r'<(\d{4,5})', text)
                    for speed in comp_matches:
                        speeds.add(f"<{speed}")
                
                if '>' in text:
                    comp_matches = re.findall(r'>(\d{4,5})', text)
                    for speed in comp_matches:
                        speeds.add(f">{speed}")
        
        if 'Product Comment' in end_products_df.columns:
            product_comments = end_products_df['Product Comment'].dropna().astype(str)
            
            for comment in product_comments:
                speed_matches = re.findall(r'(\d{4,5})', comment)
                for speed in speed_matches:
                    speeds.add(speed)
        
        all_speeds = ["No Filter", "NA"]
        
        extracted_speeds = sorted([s for s in speeds if s.isdigit()], key=int)
        all_speeds.extend(extracted_speeds)
        
        range_speeds = sorted([s for s in speeds if '-' in s])
        all_speeds.extend(range_speeds)
        
        comp_speeds = sorted([s for s in speeds if '<' in s or '>' in s])
        all_speeds.extend(comp_speeds)
        
        for speed in default_speeds:
            if speed not in all_speeds:
                all_speeds.append(speed)
        
        return all_speeds
        
    except Exception as e:
        st.sidebar.warning(f"Error extracting speeds from End Products data: {e}")
        return default_speeds

def get_form_factor_options_from_end_products(end_products_df):
    default_form_factors = ["No Filter", "CSSODIMM", "CUDIMM", "MRDIMM", "RDIMM", "SODIMM", "UDIMM", "LPCAMM", "SlimCAMM", "NA"]
    
    if end_products_df is None or end_products_df.empty:
        return default_form_factors
    
    try:
        form_factors = set()
        
        if 'End Products' in end_products_df.columns:
            end_products_text = end_products_df['End Products'].dropna().astype(str)
            
            import re
            for text in end_products_text:
                ff_patterns = [
                    r'\b(SODIMM)\b', r'\b(UDIMM)\b', r'\b(RDIMM)\b', r'\b(CUDIMM)\b', 
                    r'\b(CSSODIMM)\b', r'\b(CSODIMM)\b', r'\b(MRDIMM)\b', 
                    r'\b(LPCAMM)\b', r'\b(SlimCAMM)\b', r'\b(CAMM)\b'
                ]
                
                for pattern in ff_patterns:
                    matches = re.findall(pattern, text, re.IGNORECASE)
                    for match in matches:
                        form_factors.add(match.upper())
                
                ddr5_matches = re.findall(r'DDR5\s+([A-Z]+)', text, re.IGNORECASE)
                for match in ddr5_matches:
                    if match.upper() in ['SODIMM', 'UDIMM', 'RDIMM', 'CUDIMM', 'CSSODIMM', 'CSODIMM', 'MRDIMM']:
                        form_factors.add(match.upper())
        
        if 'Form_Factor' in end_products_df.columns:
            ff_values = end_products_df['Form_Factor'].dropna().astype(str)
            for ff in ff_values:
                if ff and ff.strip() and ff.strip().upper() != 'NAN':
                    form_factors.add(ff.strip().upper())
        
        all_form_factors = ["No Filter"]
        
        priority_order = ['SODIMM', 'UDIMM', 'RDIMM', 'CUDIMM', 'CSSODIMM', 'CSODIMM', 'MRDIMM', 'LPCAMM', 'SlimCAMM', 'CAMM']
        
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
        st.sidebar.warning(f"Error extracting form factors from End Products data: {e}")
        return default_form_factors

def validate_speed_form_factor_compatibility(process_code, speed, form_factor, end_products_df):
    if not process_code or end_products_df is None or end_products_df.empty:
        return True, "No validation data available"
    
    if speed in ["No Filter", "NA"] and form_factor in ["No Filter", "NA"]:
        return True, "No speed or form factor filtering applied"
    
    try:
        matching_rows = end_products_df[
            end_products_df['Process_Code'].astype(str).str.contains(process_code, na=False, case=False)
        ]
        
        if matching_rows.empty:
            return True, f"No validation data found for process code {process_code}"
        
        validation_messages = []
        is_valid = True
        
        if speed and speed not in ["No Filter", "NA"]:
            speed_compatible = False
            
            for _, row in matching_rows.iterrows():
                end_products_text = str(row.get('End Products', ''))
                product_comment = str(row.get('Product Comment', ''))
                combined_text = f"{end_products_text} {product_comment}"
                
                if speed.isdigit():
                    if speed in combined_text:
                        speed_compatible = True
                        break
                elif speed.startswith('<'):
                    try:
                        threshold = int(speed[1:])
                        import re
                        found_speeds = re.findall(r'\b(\d{4,5})\b', combined_text)
                        if any(int(s) < threshold for s in found_speeds if s.isdigit()):
                            speed_compatible = True
                            break
                        if f"<{threshold}" in combined_text:
                            speed_compatible = True
                            break
                    except:
                        pass
                elif speed.startswith('>'):
                    try:
                        threshold = int(speed[1:])
                        import re
                        found_speeds = re.findall(r'\b(\d{4,5})\b', combined_text)
                        if any(int(s) > threshold for s in found_speeds if s.isdigit()):
                            speed_compatible = True
                            break
                    except:
                        pass
                elif '-' in speed:
                    if speed in combined_text:
                        speed_compatible = True
                        break
            
            if not speed_compatible:
                is_valid = False
                validation_messages.append(f"Speed {speed} may not be compatible with process code {process_code}")
            else:
                validation_messages.append(f"Speed {speed} is compatible")
        
        if form_factor and form_factor not in ["No Filter", "NA"]:
            form_factor_compatible = False
            
            for _, row in matching_rows.iterrows():
                end_products_text = str(row.get('End Products', ''))
                form_factor_field = str(row.get('Form_Factor', ''))
                combined_text = f"{end_products_text} {form_factor_field}"
                
                if form_factor.upper() in combined_text.upper():
                    form_factor_compatible = True
                    break
            
            if not form_factor_compatible:
                is_valid = False
                validation_messages.append(f"Form factor {form_factor} may not be compatible with process code {process_code}")
            else:
                validation_messages.append(f"Form factor {form_factor} is compatible")
        
        return is_valid, "; ".join(validation_messages) if validation_messages else "Validation passed"
        
    except Exception as e:
        return True, f"Error during validation: {e}"

def get_form_factor_options(end_products_df):
    default_form_factors = ["CSSODIMM", "CUDIMM", "MRDIMM", "RDIMM", "SODIMM", "UDIMM", "LPCAMM", "SlimCAMM", "NA"]
    
    if end_products_df is not None and not end_products_df.empty and 'Form_Factor' in end_products_df.columns:
        try:
            available_form_factors = end_products_df['Form_Factor'].dropna().unique().tolist()
            cleaned_form_factors = [ff.strip() for ff in available_form_factors if ff and ff.strip()]
            
            if cleaned_form_factors:
                combined_form_factors = []
                for ff in default_form_factors:
                    if ff not in combined_form_factors:
                        combined_form_factors.append(ff)
                
                for ff in cleaned_form_factors:
                    if ff not in combined_form_factors:
                        combined_form_factors.append(ff)
                
                return combined_form_factors
        except Exception as e:
            st.sidebar.warning(f"Error extracting form factors from data: {e}")
    
    return default_form_factors

def explain_process_code(process_code, segment):
    if not process_code or not isinstance(process_code, str):
        return "Invalid process code"
    
    explanation = []
    explanation.append(f"**Process Code: {process_code}** (Segment: {segment})")
    explanation.append("**Position Breakdown:**")
    
    if segment.lower() == 'server':
        position_components = {
            1: "PMIC",
            2: "SPD/Hub", 
            3: "Temp Sensor",
            4: "RCD/MRCD",
            5: "Data Buffer"
        }
        
        for i, char in enumerate(process_code.upper(), 1):
            if i in position_components:
                component_type = position_components[i]
                explanation.append(f"- **Position {i}**: {component_type} → **{char}**")
        
        explanation.append(f"\n**Process Code Print Order** (as shown on product label):")
        explanation.append("PMIC → RCD/MRCD → SPD/Hub → Temp Sensor → Data Buffer (if applicable)")
        
        if len(process_code) >= 4:
            print_order_chars = f"{process_code[0]} → {process_code[3]} → {process_code[1]} → {process_code[2]}"
            if len(process_code) >= 5:
                print_order_chars += f" → {process_code[4]}"
            explanation.append(f"**Print Order Characters**: {print_order_chars}")
            
            print_order_code = convert_process_code_to_print_order(process_code, segment)
            explanation.append(f"**Print Order Code**: {print_order_code}")
    
    elif segment.lower() == 'client':
        position_components = {
            1: "PMIC",
            2: "SPD/Hub",
            3: "CKD"
        }
        
        for i, char in enumerate(process_code.upper(), 1):
            if i in position_components:
                component_type = position_components[i]
                explanation.append(f"- **Position {i}**: {component_type} → **{char}**")
        
        explanation.append(f"\n**Process Code Print Order** (as shown on product label):")
        explanation.append("PMIC → SPD/Hub → CKD (if applicable)")
        
        print_order_chars = " → ".join(process_code.upper())
        explanation.append(f"**Print Order Characters**: {print_order_chars}")
        explanation.append(f"**Print Order Code**: {process_code} (same as position order)")
    
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
        if field == 'Component_Type' and segment:
            return get_component_type_options_by_segment(segment)
        return []
    
    filtered_df = df.copy()
    
    if segment and 'Segment' in filtered_df.columns:
        filtered_df['Segment_Lower'] = filtered_df['Segment'].astype(str).str.lower()
        segment_lower = segment.lower()
        
        segment_mask = (filtered_df['Segment_Lower'] == segment_lower) | \
                       (filtered_df['Segment_Lower'] == 'server/client')
        filtered_df = filtered_df[segment_mask]
        
        filtered_df = filtered_df.drop('Segment_Lower', axis=1)
    
    if supplier and 'Supplier' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['Supplier'].astype(str).str.lower() == supplier.lower()]
    
    if field == 'Component_Type' and segment:
        segment_lower = segment.lower()
        
        if segment_lower == 'client':
            valid_client_types = ['PMIC', 'SPD/Hub', 'CKD', 'Inductor', 'Voltage Regulator']
            return valid_client_types
            
        elif segment_lower == 'server':
            valid_server_types = ["CKD", "Data Buffer", "Inductor", "Muxed RCD", "PMIC", "RCD", "SPD/Hub", "Temp Sensor", "Voltage Regulator"]
            return valid_server_types
    
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
                else:
                    return ["5000", "5010", "5020", "5030", "5100", "5120", "5200"]
            elif 'spd/hub' in component_type_lower or 'ckd' in component_type_lower:
                gen_options = [opt for opt in filtered_df[field].dropna().unique() 
                              if isinstance(opt, str) and (opt.lower().startswith('gen') or opt.lower() == 'na')]
                if gen_options:
                    return sorted(gen_options)
                else:
                    return ["Gen1", "Gen2", "Gen3", "Gen4", "Gen5", "NA"]
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
        if field == 'Component_Type' and segment:
            return get_component_type_options_by_segment(segment)
        return []
    
    options = filtered_df[field].dropna().unique().tolist()
    cleaned_options = list(set([option for option in options if option and str(option).strip()]))
    
    if field == 'Segment':
        valid_segments = ["Client", "Server"]
        cleaned_options = [opt for opt in cleaned_options if opt.lower() in [s.lower() for s in valid_segments]]
    
    return sorted(cleaned_options, key=lambda x: str(x).lower())

def create_automatic_position_filters(process_code, component_validations_df, segment):
    
    if not process_code:
        return {}
    
    if component_validations_df.empty:
        return {}
    
    component_type_col = None
    possible_component_cols = ['Component_Type', 'Component Type', 'ComponentType', 'COMPONENT_TYPE']
    
    for col_name in possible_component_cols:
        if col_name in component_validations_df.columns:
            component_type_col = col_name
            break
    
    if component_type_col is None:
        st.warning("Component Type column not found in data")
        return {}
    
    if segment.lower() == 'server':
        position_components = {
            1: "PMIC",
            2: "SPD/Hub", 
            3: "Temp Sensor",
            4: "RCD/MRCD",
            5: "Data Buffer"
        }
    else:
        position_components = {
            1: "PMIC",
            2: "SPD/Hub",
            3: "CKD"
        }
    
    position_filters = {}
    
    for i, char in enumerate(process_code.upper(), 1):
        if i in position_components:
            component_type = position_components[i]
            
            if component_type == "RCD/MRCD":
                position_data = component_validations_df[
                    (component_validations_df[component_type_col].str.contains('RCD', case=False, na=False)) |
                    (component_validations_df[component_type_col].str.contains('Muxed RCD', case=False, na=False))
                ]
            elif component_type == "SPD/Hub":
                position_data = component_validations_df[
                    component_validations_df[component_type_col].str.contains('SPD', case=False, na=False)
                ]
            elif component_type == "Temp Sensor":
                position_data = component_validations_df[
                    component_validations_df[component_type_col].str.contains('Temp', case=False, na=False)
                ]
            elif component_type == "Data Buffer":
                position_data = component_validations_df[
                    component_validations_df[component_type_col].str.contains('Buffer', case=False, na=False)
                ]
            else:
                position_data = component_validations_df[
                    component_validations_df[component_type_col].str.contains(component_type, case=False, na=False)
                ]
            
            if not position_data.empty and 'Segment' in position_data.columns:
                segment_mask = (position_data['Segment'].astype(str).str.lower() == segment.lower()) | \
                               (position_data['Segment'].astype(str).str.lower() == 'server/client')
                position_data = position_data[segment_mask]
            
            if not position_data.empty and 'Process_Code' in position_data.columns:
                char_matches = position_data[
                    position_data['Process_Code'].astype(str).str.upper() == char
                ]
                if not char_matches.empty:
                    position_data = char_matches
            
            if not position_data.empty:
                print_position = i
                if segment.lower() == 'server':
                    print_position_map = {1: 1, 2: 3, 3: 4, 4: 2, 5: 5}
                    print_position = print_position_map.get(i, i)
                
                position_filters[f"position_{i}"] = {
                    'component_type': component_type,
                    'process_code_char': char,
                    'available_parts': len(position_data),
                    'print_position': print_position
                }
    
    return position_filters


def lookup_parts_with_automatic_position_filters(process_code, position_filters, component_validations_df, end_products_df=None, segment=None):
    
    try:
        if not process_code:
            return "No process code provided"
        
        if component_validations_df.empty:
            return "No component validation data available"
        
        all_results = []
        
        for i, char in enumerate(process_code.upper(), 1):
            position_key = f"position_{i}"
            
            if position_key in position_filters:
                filter_data = position_filters[position_key]
                component_type = filter_data['component_type']
                print_position = filter_data.get('print_position', i)
                
                if component_type == "RCD/MRCD":
                    position_data = component_validations_df[
                        (component_validations_df['Component_Type'].str.contains('RCD', case=False, na=False)) |
                        (component_validations_df['Component_Type'].str.contains('Muxed RCD', case=False, na=False))
                    ]
                elif component_type == "SPD/Hub":
                    position_data = component_validations_df[
                        component_validations_df['Component_Type'].str.contains('SPD', case=False, na=False)
                    ]
                elif component_type == "Temp Sensor":
                    position_data = component_validations_df[
                        component_validations_df['Component_Type'].str.contains('Temp', case=False, na=False)
                    ]
                elif component_type == "Data Buffer":
                    position_data = component_validations_df[
                        component_validations_df['Component_Type'].str.contains('Buffer', case=False, na=False)
                    ]
                else:
                    position_data = component_validations_df[
                        component_validations_df['Component_Type'].str.contains(component_type, case=False, na=False)
                    ]
                
                if segment and not position_data.empty and 'Segment' in position_data.columns:
                    segment_mask = (position_data['Segment'].astype(str).str.lower() == segment.lower()) | \
                                   (position_data['Segment'].astype(str).str.lower() == 'server/client')
                    position_data = position_data[segment_mask]
                
                if not position_data.empty and 'Process_Code' in position_data.columns:
                    position_data = position_data[
                        position_data['Process_Code'].astype(str).str.upper() == char
                    ]
                
                for _, row in position_data.iterrows():
                    segment_value = row.get('Segment', "Unknown")
                    if segment_value == 'server/client':
                        segment_display = "Server/Client"
                    else:
                        segment_display = segment_value.title() if segment_value != "unknown" else "Unknown"
                    
                    component_type_display = row.get('Component_Type', "Unknown")
                    if component_type_display.lower() == 'pmic':
                        component_type_display = 'PMIC'
                    else:
                        component_type_display = component_type_display.title()
                    
                    all_results.append({
                        'Position': f'Position {i} (Print: {print_position})',
                        'Component Type': component_type_display,
                        'Process Code': char,
                        'Segment': segment_display,
                        'Supplier': row.get('Supplier', "Unknown").upper(),
                        'Component Generation': row.get('Component_Generation', "Unknown").upper(),
                        'Revision': row.get('Revision', "Unknown").upper(),
                        'Speed': row.get('Speed', "Unknown"),
                        'SPN': row.get('SPN', "Unknown"),
                        'Product Description': row.get('Product_Description', "Unknown"),
                        'Product Status': row.get('Product_Status', "Unknown")
                    })
        
        if not all_results:
            return f"No matching components found for process code: {process_code}"
        
        result_df = pd.DataFrame(all_results)
        
        result_df = result_df.drop_duplicates(subset=['Position', 'Process Code', 'Supplier', 'Component Generation', 'Revision'])
        
        if not result_df.empty:
            position_order = {}
            for i in range(1, 6):
                for comp_type in ["PMIC", "SPD/Hub", "Temp Sensor", "RCD/MRCD", "Data Buffer", "CKD"]:
                    position_order[f'Position {i} (Print: {i})'] = i
                    if segment and segment.lower() == 'server':
                        print_position_map = {1: 1, 2: 3, 3: 4, 4: 2, 5: 5}
                        print_pos = print_position_map.get(i, i)
                        position_order[f'Position {i} (Print: {print_pos})'] = i
            
            result_df['sort_order'] = result_df['Position'].map(position_order).fillna(50)
            result_df = result_df.sort_values(['sort_order', 'Component Type', 'Supplier'])
            result_df = result_df.drop('sort_order', axis=1)
        
        return result_df
        
    except Exception as e:
        return f"Error looking up parts with automatic position filters: {e}"

def explain_automatic_position_process_code(process_code, segment):
    if not process_code or not isinstance(process_code, str):
        return "Invalid process code"
    
    explanation = []
    explanation.append(f"**Process Code: {process_code}** (Segment: {segment})")
    explanation.append("**Position Breakdown:**")
    
    if segment.lower() == 'server':
        position_components = {
            1: "PMIC",
            2: "SPD/Hub", 
            3: "Temp Sensor",
            4: "RCD/MRCD",
            5: "Data Buffer"
        }
        
        for i, char in enumerate(process_code.upper(), 1):
            if i in position_components:
                component_type = position_components[i]
                if i <= len(process_code):
                    explanation.append(f"- **Position {i}**: {component_type} → **{char}**")
        
        explanation.append(f"\n**Process Code Print Order** (as shown on product label):")
        explanation.append("PMIC → RCD/MRCD → SPD/Hub → Temp Sensor → Data Buffer (if applicable)")
        
        if len(process_code) >= 4:
            print_order_chars = f"{process_code[0]} → {process_code[3]} → {process_code[1]} → {process_code[2]}"
            if len(process_code) >= 5:
                print_order_chars += f" → {process_code[4]}"
            explanation.append(f"**Print Order Characters**: {print_order_chars}")
            
            print_order_code = convert_process_code_to_print_order(process_code, segment)
            explanation.append(f"**Print Order Code**: {print_order_code}")
    else:
        position_components = {
            1: "PMIC",
            2: "SPD/Hub",
            3: "CKD"
        }
        
        for i, char in enumerate(process_code.upper(), 1):
            if i in position_components:
                component_type = position_components[i]
                if i <= len(process_code):
                    explanation.append(f"- **Position {i}**: {component_type} → **{char}**")
        
        explanation.append(f"\n**Process Code Print Order** (as shown on product label):")
        explanation.append("PMIC → SPD/Hub → CKD (if applicable)")
        
        if process_code:
            print_order_chars = " → ".join(process_code.upper())
            explanation.append(f"**Print Order Characters**: {print_order_chars}")
            explanation.append(f"**Print Order Code**: {process_code} (same as position order)")
    
    return "\n".join(explanation)

def search_mpn_in_sql(search_term, module_bom_59only_df, module_bom_simple_df):
    matching_mpns = []
    
    try:
        if not module_bom_59only_df.empty and 'Material Description' in module_bom_59only_df.columns:
            matches = module_bom_59only_df[
                module_bom_59only_df['Material Description'].astype(str).str.contains(search_term, case=False, na=False)
            ]['Material Description'].unique()
            matching_mpns.extend(matches)
        
        if not module_bom_simple_df.empty and 'Material Description' in module_bom_simple_df.columns:
            matches = module_bom_simple_df[
                module_bom_simple_df['Material Description'].astype(str).str.contains(search_term, case=False, na=False)
            ]['Material Description'].unique()
            matching_mpns.extend(matches)
        
        for df, df_name in [(module_bom_59only_df, '59only'), (module_bom_simple_df, 'Simple')]:
            if not df.empty:
                for col in df.columns:
                    if any(term in col.lower() for term in ['mpn', 'material number', 'part number']):
                        matches = df[
                            df[col].astype(str).str.contains(search_term, case=False, na=False)
                        ][col].unique()
                        matching_mpns.extend(matches)
        
        matching_mpns = sorted(list(set([mpn for mpn in matching_mpns if mpn and str(mpn).strip() and str(mpn).lower() != 'nan'])))
        
    except Exception as e:
        st.error(f"Error searching MPNs: {e}")
    
    return matching_mpns

def get_process_code_from_sql(mpn, module_bom_59only_df, module_bom_simple_df):
    try:
        results = []
        
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
                
                for c in row.index:
                    if c not in result_row and not pd.isna(row[c]):
                        result_row[c] = row[c]
                
                results.append(result_row)
        
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
                
                for c in row.index:
                    if c not in result_row and not pd.isna(row[c]):
                        result_row[c] = row[c]
                
                results.append(result_row)
        
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
                            
                            for c in row.index:
                                if c not in result_row and not pd.isna(row[c]):
                                    result_row[c] = row[c]
                            
                            results.append(result_row)
        
        if not results:
            return f"No records found for MPN: {mpn}", None
        
        seen = set()
        unique_results = []
        for result in results:
            key = (result['Source'], result['MPN'])
            if key not in seen:
                seen.add(key)
                unique_results.append(result)
        
        result_df = pd.DataFrame(unique_results)
        return "Success", result_df
        
    except Exception as e:
        return f"Error looking up MPN: {e}", None

def analyze_sql_data(module_bom_59only_df, module_bom_simple_df):
    st.subheader("SQL Data Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**ModuleBOM_59only Analysis:**")
        if not module_bom_59only_df.empty:
            st.write(f"- Total records: {len(module_bom_59only_df)}")
            st.write(f"- Columns: {len(module_bom_59only_df.columns)}")
            
            with st.expander("Column Names", expanded=False):
                for col in sorted(module_bom_59only_df.columns):
                    st.write(f"- {col}")
            
            with st.expander("Sample Data", expanded=False):
                st.dataframe(module_bom_59only_df.head())
        else:
            st.write("No data available")
    
    with col2:
        st.write("**ModuleBOM_Simple Analysis:**")
        if not module_bom_simple_df.empty:
            st.write(f"- Total records: {len(module_bom_simple_df)}")
            st.write(f"- Columns: {len(module_bom_simple_df.columns)}")
            
            with st.expander("Column Names", expanded=False):
                for col in sorted(module_bom_simple_df.columns):
                    st.write(f"- {col}")
            
            with st.expander("Sample Data", expanded=False):
                st.dataframe(module_bom_simple_df.head())
                
            if 'Process Code' in module_bom_simple_df.columns:
                process_codes = module_bom_simple_df['Process Code'].dropna().unique()
                st.write(f"- Unique Process Codes: {len(process_codes)}")
                
                with st.expander("Process Codes Found", expanded=False):
                    for pc in sorted(process_codes):
                        if pc and str(pc).strip():
                            st.write(f"- {pc}")
        else:
            st.write("No data available")

def check_api_server_status():
    st.sidebar.subheader("API Server Diagnostics")
    
    if st.sidebar.button("Test All Credentials", key="test_all_creds"):
        base_url, _, _, timeout = get_rest_api_connection_params()
        
        st.sidebar.info("Testing all possible credential combinations...")
        
        test_credentials = [
            ("admin", "MicronPC123"),
            ("api_user", "ProcessCodeAdmin"), 
            ("process_code_user", "process_code_pass"),
            ("ProcessCodeAdmin", "MicronPC123")
        ]
        
        working_creds = None
        
        for username, password in test_credentials:
            try:
                st.sidebar.info(f"Testing: {username} / {password}")
                
                session = requests.Session()
                session.timeout = timeout
                
                auth_string = f"{username}:{password}"
                encoded_auth = base64.b64encode(auth_string.encode()).decode()
                session.headers.update({
                    'Authorization': f'Basic {encoded_auth}',
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                })
                
                health_url = f"{base_url.rstrip('/')}/health"
                health_response = session.get(health_url)
                
                if health_response.status_code == 200:
                    st.sidebar.success(f"Health check passed for: {username}")
                    
                    test_url = f"{base_url.rstrip('/')}/modulebom-simple/count"
                    response = session.get(test_url)
                    
                    if response.status_code == 200:
                        st.sidebar.success(f"**Credentials Found**")
                        st.sidebar.success(f"Username: {username}")
                        st.sidebar.success(f"Password: {password}")
                        working_creds = (username, password)
                        
                        try:
                            data = response.json()
                            st.sidebar.json(data)
                        except:
                            st.sidebar.info(f"Response: {response.text}")
                        break
                    else:
                        st.sidebar.error(f"Auth failed for {username}: HTTP {response.status_code}")
                        if response.status_code == 401:
                            st.sidebar.error("  → Invalid credentials")
                        elif response.status_code == 403:
                            st.sidebar.error("  → Access denied")
                else:
                    st.sidebar.error(f"Health check failed for {username}: HTTP {health_response.status_code}")
                    
            except Exception as e:
                st.sidebar.error(f"Error testing {username}: {str(e)}")
            finally:
                if 'session' in locals():
                    session.close()
        
        if working_creds:
            st.sidebar.success("**Update your default credentials to:**")
            st.sidebar.code(f'username = "{working_creds[0]}"')
            st.sidebar.code(f'password = "{working_creds[1]}"')
        else:
            st.sidebar.error("No working credentials found")

def determine_segment_and_widget(process_code):
    if not process_code or not isinstance(process_code, str):
        return None, "position_order"
    
    process_code = str(process_code).strip()
    
    zero_count = process_code.count('0')
    
    ends_with_1 = process_code.endswith('1')
    
    segment = None
    
    if len(process_code) in [2, 3]:
        segment = "Client"
    elif len(process_code) in [4, 5]:
        segment = "Server"
    
    if zero_count in [7, 8]:
        segment = "Client"
    elif zero_count in [5, 6]:
        segment = "Server"
    
    widget_type = "print_order" if ends_with_1 else "position_order"
    
    return segment, widget_type

def main():
    st.title("Process Code & MPN Lookup")
    
    show_process_code_info()
    
    st.sidebar.header("Data Source")
    st.sidebar.info("Data is being loaded from SharePoint and SQL")
    
    data = load_data_from_sharepoint()
    component_validations_df = data['component_validations_df']
    module_validation_df = data['module_validation_df']
    end_products_df = data['end_products_df']
    
    placeholder = st.empty()
    with placeholder.container():
        rest_api_data = load_data_from_rest_api_cached(*get_rest_api_connection_params())
    placeholder.empty()
    
    if rest_api_data and not rest_api_data['module_bom_simple_df'].empty:
        st.sidebar.success("Connected Successfully to REST API")
    else:
        st.sidebar.error("Failed to connect to REST API")

    if st.sidebar.button("Refresh Data", key="refresh_data_main_unique"):
        if hasattr(load_data_from_sharepoint, 'clear'):
            load_data_from_sharepoint.clear()
        if hasattr(load_data_from_rest_api_cached, 'clear'):
            load_data_from_rest_api_cached.clear()
        
        for key in ['rest_api_base_url', 'rest_api_username', 'rest_api_password', 'rest_api_timeout', 'rest_api_widgets_created']:
            if key in st.session_state:
                del st.session_state[key]
        
        st.rerun()
    
    module_bom_simple_df = rest_api_data['module_bom_simple_df']

    predefined_options = get_predefined_options(component_validations_df)
    
    local_timezone = pytz.timezone('America/Denver')  
    local_time_obj = datetime.now(local_timezone)
    formatted_time = local_time_obj.strftime('%Y-%m-%d %H:%M:%S')
    tz_abbr = local_time_obj.strftime('%Z')
    st.sidebar.info(f"Data last refreshed: {formatted_time} {tz_abbr}")
    
    tab1, tab2, tab3 = st.tabs(["Process Code Lookup", "Process Code Generator", "MPN Lookup"])

    with tab1:
        st.write("Enter a process code to look up the associated parts:")
        
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
        
        if "lookup_process_code" not in st.session_state:
            st.session_state["lookup_process_code"] = ""
        if "print_order_process_code" not in st.session_state:
            st.session_state["print_order_process_code"] = ""
        
        lookup_process_code = st.text_input(
            "Process Code", 
            value=st.session_state["lookup_process_code"],
            key="lookup_process_code_input",
            help="Enter the process code in position order (PMIC → SPD/Hub → Temp Sensor → RCD → Data Buffer for server)"
        )
        
        if lookup_process_code != st.session_state["lookup_process_code"]:
            st.session_state["lookup_process_code"] = lookup_process_code
            if lookup_process_code and not st.session_state["print_order_process_code"]:
                auto_print_order = convert_process_code_to_print_order(lookup_process_code, lookup_segment)
                if auto_print_order and auto_print_order != lookup_process_code:
                    st.session_state["print_order_process_code"] = auto_print_order
                    st.rerun()
        
        if lookup_process_code:
            if st.button("Lookup Parts by Position Order Process Code", key="lookup_position_order"):
                position_filters = create_automatic_position_filters(lookup_process_code, component_validations_df, lookup_segment)
                
                parts_lookup = lookup_parts_with_automatic_position_filters(
                    lookup_process_code,
                    position_filters,
                    component_validations_df,
                    end_products_df,
                    lookup_segment
                )
                
                if isinstance(parts_lookup, str):
                    st.error(parts_lookup)
                else:
                    st.success(f"Found components for process code: {lookup_process_code}")
                    
                    if isinstance(parts_lookup, pd.DataFrame) and not parts_lookup.empty:
                        st.subheader("Component Details")
                        
                        display_df = parts_lookup.copy()
                        if 'Print Position' in display_df.columns:
                            display_df = display_df.drop('Print Position', axis=1)
                        
                        num_rows = len(display_df)
                        dynamic_height = min(max(100 + (num_rows * 35), 150), 600)
                        
                        st.dataframe(display_df, height=dynamic_height)
                    else:
                        st.warning("No component details available to display")
        
        print_order_code = st.text_input(
            "Process Code Print Order",
            value=st.session_state["print_order_process_code"],
            help="Enter the process code as it appears on the product label (print order)",
            key="print_order_process_code_input"
        )
        
        if print_order_code != st.session_state["print_order_process_code"]:
            st.session_state["print_order_process_code"] = print_order_code
            if print_order_code and not st.session_state["lookup_process_code"]:
                auto_position_order = convert_print_order_to_process_code(print_order_code, lookup_segment)
                if auto_position_order and auto_position_order != print_order_code:
                    st.session_state["lookup_process_code"] = auto_position_order
                    st.rerun()

        if print_order_code:
            position_order_code = convert_print_order_to_process_code(print_order_code, lookup_segment)
            
            if st.button("Lookup Parts by Print Order Process Code", key="lookup_print_order"):
                with st.spinner("Looking up parts..."):
                    position_filters = create_automatic_position_filters(position_order_code, component_validations_df, lookup_segment)
                    
                    parts_lookup = lookup_parts_with_automatic_position_filters(
                        position_order_code,
                        position_filters,
                        component_validations_df,
                        end_products_df,
                        lookup_segment
                    )
                    
                    if isinstance(parts_lookup, str):
                        st.error(parts_lookup)
                    else:
                        st.success(f"Found components for print order process code: {print_order_code}")
                        
                        if isinstance(parts_lookup, pd.DataFrame) and not parts_lookup.empty:
                            st.subheader("Component Details")
                            
                            display_df = parts_lookup.copy()
                            
                            num_rows = len(display_df)
                            dynamic_height = min(max(100 + (num_rows * 35), 150), 600)
                            
                            st.dataframe(display_df, height=dynamic_height)
                        else:
                            st.warning("No component details available to display")

    with tab2:
        subtab2, subtab1 = st.tabs(["Module", "Component"])
        with subtab2:
            st.write("Enter the module component details to generate a combined module process code:")
            
            module_segment = st.selectbox("Segment", options=predefined_options['segment'], key="module_segment")
            
            components = get_module_component_options_by_segment(module_segment)
            
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
                    st.success(f"**Module Process Code (Position Order): {module_process_code}**")
                    
                    print_order_equivalent = convert_process_code_to_print_order(module_process_code, module_segment)
                    if print_order_equivalent != module_process_code:
                        st.success(f"**Module Process Code (Print Order): {print_order_equivalent}**")
                    
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
            
            component_segment = st.selectbox("Segment", options=predefined_options['segment'], key="component_segment")
            
            component_type_options = get_component_type_options_by_segment(component_segment)
            
            component_type = st.selectbox("Component Type", options=component_type_options, key="component_type")
            
            supplier_options = get_filtered_options(component_validations_df, 'Supplier', 
                                                  segment=component_segment, 
                                                  component_type=component_type) or predefined_options['supplier']
            
            supplier = st.selectbox("Supplier", options=supplier_options, key="component_supplier")
            
            if component_segment.lower() == 'server' and component_type.lower() in ['temp sensor', 'rcd', 'muxed rcd', 'data buffer']:
                valid_gen_options = ["Gen1", "Gen2", "Gen3", "Gen4", "Gen5", "NA"]
                
                data_gen_options = get_filtered_options(component_validations_df, 'Component_Generation', 
                                                     segment=component_segment, 
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
                                                 segment=component_segment, 
                                                 supplier=supplier, 
                                                 component_type=component_type) or predefined_options['component_generation']
            
            component_gen = st.selectbox("Component Generation", options=gen_options, key="component_gen")
            
            revision_options = get_filtered_options(component_validations_df, 'Revision', 
                                                  segment=component_segment, 
                                                  supplier=supplier, 
                                                  component_type=component_type) or predefined_options['revision']
            
            revision = st.selectbox("Revision", options=revision_options, key="component_revision")
            
            if st.button("Generate Process Code"):
                process_code_result, component_type_result, filtered_df = get_component_process_code(
                    component_segment, supplier, component_gen, revision, component_type, component_validations_df
                )
                
                if process_code_result and not process_code_result.startswith("Error") and not process_code_result.startswith("No"):
                    st.success(f"Process Code: {process_code_result}")
                    
                    if filtered_df is not None and not filtered_df.empty:
                        st.subheader("Component Details")
                        st.dataframe(filtered_df)
                else:
                    st.error(process_code_result)

    with tab3:
        st.write("Search for an MPN to find its process code:")
        
        search_term = st.text_input("Enter MPN to search:", key="mpn_search")
        
        if search_term and len(search_term) >= 2:
            with st.spinner("Searching for matching MPNs..."):
                matching_mpns_rest = search_mpn_in_rest_api(search_term, module_bom_simple_df)
                
                if matching_mpns_rest:
                    st.write(f"Found {len(matching_mpns_rest)} matching MPNs:")
                    
                    selected_mpn = st.selectbox(
                        "Select an MPN to lookup:",
                        options=[""] + matching_mpns_rest,
                        key="selected_mpn"
                    )
                    
                    if selected_mpn:
                        if st.button("Get Process Code", key="get_process_code"):
                            with st.spinner("Looking up process code..."):
                                result_message, result_df = get_process_code_from_rest_api(selected_mpn, module_bom_simple_df)
                                
                                if result_df is not None and not result_df.empty:
                                    st.success(f"Process code information found for MPN: {selected_mpn}")
                                    
                                    num_rows = len(result_df)
                                    dynamic_height = min(max(100 + (num_rows * 35), 150), 600)
                                    
                                    st.dataframe(result_df, height=dynamic_height)
                                    
                                    if 'Process Code' in result_df.columns:
                                        unique_codes = result_df['Process Code'].dropna().unique()
                                        valid_codes = [code for code in unique_codes if code and str(code).strip() and str(code).lower() != 'nan']
                                        
                                        if len(valid_codes) > 0:
                                            st.session_state['valid_process_codes'] = valid_codes
                                            st.session_state['selected_mpn_for_lookup'] = selected_mpn
                                        else:
                                            st.warning("No process codes found in the results.")
                                    else:
                                        st.warning("No 'Process Code' column found in the results.")
                                else:
                                    st.error(result_message)
                
                if 'valid_process_codes' in st.session_state and st.session_state['valid_process_codes']:
                    st.markdown("---")
                    st.subheader("Lookup Component Details")
                    st.write("Click a button below to view the component details for each process code:")
                    
                    valid_codes = st.session_state['valid_process_codes']
                    
                    cols_per_row = 3
                    for i in range(0, len(valid_codes), cols_per_row):
                        cols = st.columns(min(cols_per_row, len(valid_codes) - i))
                        
                        for j, col in enumerate(cols):
                            if i + j < len(valid_codes):
                                process_code = valid_codes[i + j]
                                
                                segment, widget_type = determine_segment_and_widget(process_code)
                                
                                button_label = f"Lookup {process_code}"
                                if segment:
                                    button_label += f" ({segment})"
                                
                                with col:
                                    if st.button(button_label, key=f"lookup_pc_{process_code}_{i}_{j}"):
                                        st.session_state['lookup_process_code_selected'] = process_code
                                        st.session_state['lookup_segment_selected'] = segment if segment else "Client"
                                        st.session_state['lookup_widget_type'] = widget_type
                                        st.rerun()
                    
                    if 'lookup_process_code_selected' in st.session_state:
                        process_code = st.session_state['lookup_process_code_selected']
                        lookup_segment = st.session_state['lookup_segment_selected']
                        widget_type = st.session_state['lookup_widget_type']
                        
                        st.markdown("---")
                        st.subheader(f"Component Details for Process Code: {process_code}")
                        
                        with st.spinner(f"Looking up components for process code {process_code}..."):
                            if widget_type == "print_order":
                                position_order_code = convert_print_order_to_process_code(process_code, lookup_segment)
                            else:
                                position_order_code = process_code
                            
                            position_filters = create_automatic_position_filters(
                                position_order_code, 
                                component_validations_df, 
                                lookup_segment
                            )
                            
                            parts_lookup = lookup_parts_with_automatic_position_filters(
                                position_order_code,
                                position_filters,
                                component_validations_df,
                                end_products_df,
                                lookup_segment
                            )
                            
                            if isinstance(parts_lookup, str):
                                st.error(f"Error: {parts_lookup}")
                            elif isinstance(parts_lookup, pd.DataFrame) and not parts_lookup.empty:
                                st.success(f"Found {len(parts_lookup)} components for process code: {process_code}")
                                
                                display_df = parts_lookup.copy()
                                
                                num_rows = len(display_df)
                                dynamic_height = min(max(150 + (num_rows * 35), 200), 600)
                                
                                st.dataframe(display_df, height=dynamic_height, use_container_width=True)
                                
                                if widget_type == "print_order":
                                    st.info(f"Print Order Code: {process_code} converted to Position Order Code: {position_order_code}")
                                elif position_order_code != process_code:
                                    print_order_equivalent = convert_process_code_to_print_order(process_code, lookup_segment)
                                    if print_order_equivalent != process_code:
                                        st.info(f"Position Order Code: {process_code} equivalent to Print Order Code: {print_order_equivalent}")
                            else:
                                st.warning(f"No component details found for process code: {process_code}")
        elif search_term and len(search_term) < 2:
            st.info("Please enter at least 2 characters to search.")

if __name__ == "__main__":
    main()