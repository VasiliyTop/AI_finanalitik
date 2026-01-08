"""Dashboard page."""
import streamlit as st
import requests
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
import pandas as pd

API_BASE_URL = "http://localhost:8000/api"


class Dashboard:
    @staticmethod
    def show():
        st.header("📊 Dashboard")
        
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start date", value=date.today() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End date", value=date.today())
        
        # Get metrics
        if st.button("Refresh", key="refresh_dashboard"):
            with st.spinner("Loading metrics..."):
                params = {
                    "start_date": str(start_date),
                    "end_date": str(end_date)
                }
                response = requests.get(f"{API_BASE_URL}/dashboard/metrics", params=params)
                
                if response.status_code == 200:
                    metrics = response.json()
                    
                    # Balances
                    st.subheader("💰 Balances")
                    if metrics.get("balances"):
                        balances_df = pd.DataFrame(metrics["balances"])
                        st.dataframe(balances_df)
                        
                        fig = px.bar(balances_df, x="entity_name", y="balance", title="Balances by Entity")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Cash Flow
                    st.subheader("💸 Cash Flow")
                    if metrics.get("cashflow"):
                        cf_df = pd.DataFrame(metrics["cashflow"])
                        cf_df["period"] = pd.to_datetime(cf_df["period"])
                        
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=cf_df["period"], y=cf_df["inflow"], name="Inflow", fill="tonexty"))
                        fig.add_trace(go.Scatter(x=cf_df["period"], y=cf_df["outflow"], name="Outflow", fill="tonexty"))
                        fig.add_trace(go.Scatter(x=cf_df["period"], y=cf_df["net_cf"], name="Net CF"))
                        fig.update_layout(title="Cash Flow Over Time", xaxis_title="Date", yaxis_title="Amount")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Category Structure
                    st.subheader("📈 Category Structure")
                    if metrics.get("category_structure"):
                        cat_df = pd.DataFrame(metrics["category_structure"])
                        fig = px.pie(cat_df, values="amount", names="category_name", title="Expenses by Category")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Top Counterparties
                    st.subheader("👥 Top Counterparties")
                    if metrics.get("top_counterparties"):
                        cp_df = pd.DataFrame(metrics["top_counterparties"])
                        st.dataframe(cp_df)
                    
                    # Gap Analysis
                    if metrics.get("gap_analysis"):
                        st.subheader("📊 Gap Analysis")
                        gap_df = pd.DataFrame(metrics["gap_analysis"])
                        st.dataframe(gap_df)
                    
                    # AR Aging
                    if metrics.get("ar_aging"):
                        st.subheader("⏰ AR Aging")
                        ar_df = pd.DataFrame(metrics["ar_aging"])
                        st.dataframe(ar_df)
                else:
                    st.error(f"Failed to load metrics: {response.text}")
