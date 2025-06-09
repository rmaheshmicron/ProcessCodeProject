import streamlit as st

class PartSpecification:
    def __init__(self):
        self.seg = None
        self.form_factor = None
        self.spd = None
        self.mpn = None
        self.process_code = None
        self.component_type = None
    
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
        
        return "\n".join(result)


def main():
    st.title("Process Code & Part Specification Generator")
    
    part = PartSpecification()
    
    tab1, tab2 = st.tabs(["Process Code Generator", "Part Specification Generator"])
    
    with tab1:
        st.write("Enter the details for your process code below:")
        
        seg_options = ["Client", "Server"]
        selected_seg = st.selectbox("Market Segment", options=seg_options, key="seg_process")
        
        form_factor_options = [
            "CAMM2",
            "CDIMM",
            "CSODIMM",
            "CUDIMM",
            "DDIMM",
            "EUDIMM",
            "LRDIMM",
            "MINIRDIMM",
            "RDIMM",
            "SOCAMM",
            "SOCAMM2",
            "SODIMM",
            "SOEDIMM",
            "TFF MRDIMM",
            "UDIMM",
            "Other"
        ]
        selected_form_factor = st.selectbox("Form Factor", options=form_factor_options, key="ff_process")
        
        if selected_form_factor == "Other":
            custom_form_factor = st.text_input("Enter custom form factor", key="custom_ff_process")
            form_factor_value = custom_form_factor if custom_form_factor else None
        else:
            form_factor_value = selected_form_factor
        
        spd_options = ["Over 8000", "Under 8000"]
        selected_spd = st.selectbox("Speed", options=spd_options, key="spd_process")
        
        process_code_valid = True
        if st.button("Generate from Process Code Inputs"):
            if not form_factor_value:
                st.error("Please enter a form factor")
                process_code_valid = False
            
            if process_code_valid:
                part.set_seg(selected_seg)
                part.set_form_factor(form_factor_value)
                part.set_spd(selected_spd)
                st.session_state.result = str(part)
                st.session_state.show_result = True
    
    with tab2:
        st.write("Enter the details for your part specification below:")
        
        mpn = st.text_input("Marketing Part Number (MPN)", key="mpn_part")
        
        component_type_options = [
            "PMIC", 
            "RCD", 
            "CKD", 
            "Temp Sensor", 
            "SPD/Hub", 
            "Voltage Regulator", 
            "Inductor", 
            "Data Buffer", 
            "Muxed RCD", 
            "Other"
        ]
        selected_component_type = st.selectbox("Component Type", options=component_type_options, key="comp_part")
        
        process_code = st.text_input("Process Code", key="pc_part")
        
        part_spec_valid = True
        if st.button("Generate from Part Specification"):
            if not mpn and not process_code:
                st.error("Please enter either a Marketing Part Number or a Process Code")
                part_spec_valid = False
            
            if part_spec_valid:
                if mpn:
                    part.set_mpn(mpn)
                if process_code:
                    part.set_process_code(process_code)
                part.set_component_type(selected_component_type)
                st.session_state.result = str(part)
                st.session_state.show_result = True
    
    if 'show_result' not in st.session_state:
        st.session_state.show_result = False
        st.session_state.result = ""
    
    if st.session_state.show_result:
        st.header("Result")
        st.text_area("Specification", st.session_state.result, height=200)
        
        if st.button("Clear and Start Over"):
            st.session_state.show_result = False
            st.session_state.result = ""
            st.experimental_rerun()


if __name__ == "__main__":
    main()