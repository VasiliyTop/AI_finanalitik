"""Forecast page."""
import streamlit as st
import requests
import plotly.graph_objects as go
from datetime import date

API_BASE_URL = "http://localhost:8000/api"


class Forecast:
    @staticmethod
    def show():
        st.header("🔮 Cash Flow Forecast")
        
        horizon = st.slider("Forecast horizon (days)", 7, 90, 14)
        include_uncertainty = st.checkbox("Include uncertainty intervals", value=True)
        
        if st.button("Generate Forecast", key="generate_forecast"):
            with st.spinner("Generating forecast..."):
                params = {
                    "horizon_days": horizon,
                    "include_uncertainty": include_uncertainty
                }
                response = requests.get(f"{API_BASE_URL}/forecast/cashflow", params=params)
                
                if response.status_code == 200:
                    forecast = response.json()
                    
                    # Current balance
                    st.metric("Current Balance", f"{forecast['current_balance']:,.0f} руб.")
                    st.metric("Forecasted Balance (End)", f"{forecast['forecasted_balance_end']:,.0f} руб.")
                    
                    # Forecast chart
                    if forecast.get("forecast_points"):
                        points = forecast["forecast_points"]
                        dates = [p["date"] for p in points]
                        forecasted = [float(p["forecasted_cf"]) for p in points]
                        balances = [float(p.get("projected_balance", 0)) for p in points]
                        
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=dates, y=forecasted, name="Forecasted CF", mode="lines+markers"))
                        fig.add_trace(go.Scatter(x=dates, y=balances, name="Projected Balance", mode="lines+markers"))
                        
                        if include_uncertainty and points[0].get("lower_bound"):
                            lower = [float(p.get("lower_bound", 0)) for p in points]
                            upper = [float(p.get("upper_bound", 0)) for p in points]
                            fig.add_trace(go.Scatter(x=dates, y=lower, name="Lower Bound", line=dict(dash="dash")))
                            fig.add_trace(go.Scatter(x=dates, y=upper, name="Upper Bound", line=dict(dash="dash"), fill="tonexty"))
                        
                        fig.update_layout(title="Cash Flow Forecast", xaxis_title="Date", yaxis_title="Amount")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Cash gaps
                    if forecast.get("cash_gaps"):
                        st.subheader("⚠️ Cash Gaps")
                        for gap in forecast["cash_gaps"]:
                            severity_color = {"low": "🟡", "medium": "🟠", "high": "🔴"}
                            st.warning(
                                f"{severity_color.get(gap['severity'], '⚪')} {gap['date']}: "
                                f"Gap of {gap['gap_amount']:,.0f} руб. (Severity: {gap['severity']})"
                            )
                else:
                    st.error(f"Failed to generate forecast: {response.text}")
