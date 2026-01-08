"""Streamlit main application."""
import streamlit as st

st.set_page_config(
    page_title="AI Financial Analytics",
    page_icon="📊",
    layout="wide"
)

st.title("📊 AI Financial Analytics")
st.sidebar.title("Navigation")

# Navigation
pages = {
    "Import": "1_Import",
    "Dashboard": "2_Dashboard",
    "Forecast": "3_Forecast",
    "Recommendations": "4_Recommendations",
    "Risks": "5_Risks",
    "Export": "6_Export"
}

selected = st.sidebar.selectbox("Go to", list(pages.keys()))
page_file = pages[selected]

# Import and run the selected page
if page_file == "1_Import":
    from pages import Import
    Import.show()
elif page_file == "2_Dashboard":
    from pages import Dashboard
    Dashboard.show()
elif page_file == "3_Forecast":
    from pages import Forecast
    Forecast.show()
elif page_file == "4_Recommendations":
    from pages import Recommendations
    Recommendations.show()
elif page_file == "5_Risks":
    from pages import Risks
    Risks.show()
elif page_file == "6_Export":
    from pages import Export
    Export.show()
