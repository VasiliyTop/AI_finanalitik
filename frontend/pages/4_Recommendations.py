"""Recommendations page."""
import streamlit as st
import requests

import os
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000/api")


class Recommendations:
    @staticmethod
    def show():
        st.header("💡 Recommendations")
        
        if st.button("Load Recommendations", key="load_recommendations"):
            with st.spinner("Loading recommendations..."):
                response = requests.get(f"{API_BASE_URL}/recommendations")
                
                if response.status_code == 200:
                    data = response.json()
                    recommendations = data.get("recommendations", [])
                    
                    st.metric("Total Recommendations", len(recommendations))
                    
                    # Sort by priority
                    recommendations.sort(key=lambda x: x.get("priority", 0), reverse=True)
                    
                    for i, rec in enumerate(recommendations, 1):
                        with st.expander(f"#{i} Priority: {rec.get('priority', 0)} - {rec.get('action', 'N/A')}"):
                            st.write(f"**Action:** {rec.get('action')}")
                            st.write(f"**Basis:** {rec.get('basis')}")
                            st.write(f"**Expected Effect:** {rec.get('expected_effect')}")
                            st.write(f"**Risk:** {rec.get('risk')}")
                            if rec.get("deadline"):
                                st.write(f"**Deadline:** {rec.get('deadline')}")
                            st.write(f"**Category:** {rec.get('category')}")
                else:
                    st.error(f"Failed to load recommendations: {response.text}")
