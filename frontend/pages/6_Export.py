"""Export page."""
import streamlit as st
import requests
from datetime import date

API_BASE_URL = "http://localhost:8000/api"


class Export:
    @staticmethod
    def show():
        st.header("📤 Export Reports")
        
        report_type = st.selectbox("Report Type", ["dashboard", "forecast", "recommendations", "risks"])
        export_format = st.selectbox("Format", ["xlsx", "pdf"])
        
        if report_type == "forecast":
            forecast_horizon = st.slider("Forecast Horizon (days)", 7, 90, 14)
        else:
            forecast_horizon = None
        
        if st.button("Export Report", key="export_report"):
            with st.spinner("Generating report..."):
                payload = {
                    "format": export_format,
                    "report_type": report_type
                }
                
                if forecast_horizon:
                    payload["forecast_horizon"] = forecast_horizon
                
                response = requests.post(f"{API_BASE_URL}/export/report", json=payload)
                
                if response.status_code == 200:
                    result = response.json()
                    st.success(f"✅ Report generated: {result['file_name']}")
                    st.info(f"File size: {result['file_size']} bytes")
                    st.download_button(
                        "Download",
                        data=requests.get(f"{API_BASE_URL}{result['download_url']}").content,
                        file_name=result["file_name"],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if export_format == "xlsx" else "application/pdf"
                    )
                else:
                    st.error(f"Failed to export: {response.text}")
