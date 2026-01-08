"""Import page."""
import streamlit as st
import requests
from pathlib import Path

API_BASE_URL = "http://localhost:8000/api"


class Import:
    @staticmethod
    def show():
        st.header("📥 Import Data")
        
        tab1, tab2 = st.tabs(["Adesk", "1C"])
        
        with tab1:
            st.subheader("Import Adesk XLS")
            adesk_file = st.file_uploader("Upload Adesk XLS file", type=["xlsx", "xls"], key="adesk")
            
            if st.button("Import Adesk", key="import_adesk"):
                if adesk_file:
                    with st.spinner("Importing..."):
                        files = {"file": (adesk_file.name, adesk_file.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
                        response = requests.post(f"{API_BASE_URL}/import/adesk", files=files)
                        
                        if response.status_code == 200:
                            result = response.json()
                            st.success(f"✅ Imported {result['rows_imported']} rows")
                            
                            if result.get("quality_issues"):
                                st.warning("⚠️ Quality issues detected:")
                                for issue in result["quality_issues"]:
                                    st.write(f"- {issue.get('description')}")
                        else:
                            st.error(f"❌ Import failed: {response.text}")
                else:
                    st.error("Please select a file")
        
        with tab2:
            st.subheader("Import 1C Files")
            
            source_type = st.selectbox("Source type", ["sales", "purchases", "arap", "mapping"])
            onec_file = st.file_uploader("Upload 1C file", type=["xlsx", "xls", "csv"], key="onec")
            
            if st.button("Import 1C", key="import_onec"):
                if onec_file:
                    with st.spinner("Importing..."):
                        files = {"file": (onec_file.name, onec_file.getvalue())}
                        response = requests.post(
                            f"{API_BASE_URL}/import/onec/{source_type}",
                            files=files
                        )
                        
                        if response.status_code == 200:
                            result = response.json()
                            st.success(f"✅ Imported {result['rows_imported']} rows")
                        else:
                            st.error(f"❌ Import failed: {response.text}")
                else:
                    st.error("Please select a file")
