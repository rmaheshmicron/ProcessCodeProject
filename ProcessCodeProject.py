import streamlit as st

class PartSpecification:
    def __init__(self):
        self.seg = None
        self.form_factor = None
        self.spd = None
        self.mpn = None
        self.process_code = None
    
    def set_seg(self, seg_value):
        """Set SEG: Client or Server"""
        self.seg = seg_value
        return self
    
    def set_form_factor(self, form_factor_value):
        """Set Form Factor: Footprint and Size (Overall Dimension - x, y, z plane & SODIMM or UDIMM)"""
        self.form_factor = form_factor_value
        return self
    
    def set_spd(self, spd_value):
        """Set SPD: Speed of the part (Over 8000/Under 8000)"""
        self.spd = spd_value
        return self
    
    def set_mpn(self, mpn_value):
        """Set MPN: Long text of code given for a part"""
        self.mpn = mpn_value
        return self
    
    def set_process_code(self, process_code_value):
        """Set Process Code: The actual process code itself (Should be in Text)"""
        self.process_code = process_code_value
        return self
    
    def __str__(self):
        """String representation of the part specification"""
        result = []
        if self.seg:
            result.append(f"SEG: {self.seg}")
        if self.form_factor:
            result.append(f"Form Factor: {self.form_factor}")
        if self.spd:
            result.append(f"SPD: {self.spd}")
        if self.mpn:
            result.append(f"MPN: {self.mpn}")
        if self.process_code:
            result.append(f"Process Code: {self.process_code}")
        
        return "\n".join(result)


def main():
    st.title("Part Specification Generator")
    st.write("Enter the details for your part specification below:")
    

    part = PartSpecification()
    
    seg_options = ["Client", "Server"]
    selected_seg = st.selectbox("SEG", options=seg_options)
    part.set_seg(selected_seg)
    
    form_factor_options = [
        "SODIMM - 69.6mm x 30mm x 3.0mm",
        "UDIMM - 133.35mm x 31.25mm x 3.8mm",
        "Other"
    ]
    selected_form_factor = st.selectbox("Form Factor", options=form_factor_options)
    
    if selected_form_factor == "Other":
        custom_form_factor = st.text_input("Enter custom form factor")
        if custom_form_factor:
            part.set_form_factor(custom_form_factor)
    else:
        part.set_form_factor(selected_form_factor)
    
    spd_options = ["Over 8000", "Under 8000"]
    selected_spd = st.selectbox("SPD", options=spd_options)
    part.set_spd(selected_spd)
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        st.markdown("<h3 style='text-align: center;'>Or</h3>", unsafe_allow_html=True)
    
    mpn = st.text_input("MPN (Part Number)")
    part.set_mpn(mpn)
    
    process_code = st.text_input("Process Code")
    part.set_process_code(process_code)
    
    st.header("Part Specification")
    if st.button("Generate Specification"):
        st.text_area("Specification", str(part), height=200)
        
        spec_text = str(part)
        st.download_button(
            label="Download Specification",
            data=spec_text,
            file_name="part_specification.txt",
            mime="text/plain"
        )


if __name__ == "__main__":
    main()