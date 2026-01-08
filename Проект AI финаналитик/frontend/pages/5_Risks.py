"""Risks page."""
import streamlit as st
import requests

API_BASE_URL = "http://localhost:8000/api"


class Risks:
    @staticmethod
    def show():
        st.header("⚠️ Risk Scoring")
        
        if st.button("Calculate Risk Score", key="calculate_risks"):
            with st.spinner("Calculating risks..."):
                response = requests.get(f"{API_BASE_URL}/risks/score")
                
                if response.status_code == 200:
                    risk_data = response.json()
                    
                    # Overall risk
                    overall_risk = risk_data.get("overall_risk", "Unknown")
                    risk_colors = {"Low": "🟢", "Medium": "🟡", "High": "🔴"}
                    st.metric("Overall Risk", f"{risk_colors.get(overall_risk, '⚪')} {overall_risk}")
                    
                    # Cash Risk
                    st.subheader("💰 Cash Risk")
                    cash_risk = risk_data.get("cash_risk", {})
                    st.metric("Days of Cash", f"{cash_risk.get('days_of_cash', 0):.1f}")
                    st.metric("Probability of Gap", f"{cash_risk.get('probability_of_gap', 0)*100:.1f}%")
                    st.write(f"**Risk Level:** {cash_risk.get('risk_level', 'Unknown')}")
                    if cash_risk.get("indicators"):
                        st.write("**Indicators:**")
                        for indicator in cash_risk["indicators"]:
                            st.write(f"- {indicator}")
                    
                    # Counterparty Risk
                    st.subheader("👥 Counterparty Risk")
                    cp_risk = risk_data.get("counterparty_risk", {})
                    st.metric("Overdue AR %", f"{cp_risk.get('overdue_ar_percentage', 0):.1f}%")
                    st.metric("Top 3 Concentration", f"{cp_risk.get('concentration_top3', 0):.1f}%")
                    st.write(f"**Risk Level:** {cp_risk.get('risk_level', 'Unknown')}")
                    if cp_risk.get("indicators"):
                        st.write("**Indicators:**")
                        for indicator in cp_risk["indicators"]:
                            st.write(f"- {indicator}")
                    
                    # Anomaly Risk
                    st.subheader("🔍 Anomaly Risk")
                    anomaly_risk = risk_data.get("anomaly_risk", {})
                    st.metric("Anomaly Count", anomaly_risk.get("anomaly_count", 0))
                    st.metric("Uncategorized %", f"{anomaly_risk.get('uncategorized_percentage', 0):.1f}%")
                    st.write(f"**Risk Level:** {anomaly_risk.get('risk_level', 'Unknown')}")
                    if anomaly_risk.get("indicators"):
                        st.write("**Indicators:**")
                        for indicator in anomaly_risk["indicators"]:
                            st.write(f"- {indicator}")
                else:
                    st.error(f"Failed to calculate risks: {response.text}")
