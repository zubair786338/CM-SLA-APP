#!/bin/bash
# Azure App Service startup script for Streamlit
pip install -r requirements.txt
streamlit run dashboard.py --server.port 8000 --server.address 0.0.0.0 --server.headless true --browser.gatherUsageStats false
