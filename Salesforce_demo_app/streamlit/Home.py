# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import plotly.express as px
import base64

from datetime import date


sp_session = get_active_session()
app_db = sp_session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
app_sch = 'CRM'
fun_db = app_db
fun_sch = 'PYTHON_FUNCTIONS'


st.set_page_config(
    page_title="Resort Cortex AI Assistant",
    layout="wide"
)

# Title
st.title("🏝️ Resort Cortex AI Assistant")
st.markdown("#### Powered by Snowflake Native App + Cortex Analyst")

# Intro
st.write("""
Welcome to the **Resort Cortex AI Assistant**.  
This application uses Snowflake Cortex to analyze vision inference, guest check-ins, weather patterns, and lost & found activity to surface meaningful insights across your resort.

""")

# Divider
st.markdown("---")

# Project Overview
st.subheader("🔍 What this App Does")
st.markdown("""
- Uses camera inference data to detect objects like **passports, jewelry, wallets, and phones** across resort zones.
- Matches detections with **Lost & Found reports**.
- Associates detected items with **guests** based on room assignments and check-in windows.
- Enriches data with **weather context** (rain, heat, wind) from Snowflake Marketplace.
- Provides **natural language question support** via **Cortex Analyst** (e.g. *"Were any valuables found during the rain near the spa?"*).
""")

# Features
st.subheader("💡 Example Use Cases")
st.markdown("""
- Identify **valuable items left unattended** during storms or in high-risk areas.
- Match detected objects with **reported lost items** and alert resort staff.
- Explore trends by **guest tier**, **zone type**, or **weather condition**.
- Generate reports for **security**, **housekeeping**, or **VIP guest relations**.
""")

# System Architecture (simple text for now)
st.subheader("🧠 Architecture Overview")
st.markdown("""
1. **Vision data** is loaded into `resort_vision_inference`
2. Enriched with:
   - `resort_guest_checkin`
   - `resort_camera_zones`
   - `resort_lost_and_found`
   - Marketplace weather data
3. Unified into `resort_event_unified_table`
4. Analyzed via **Cortex Analyst** using a semantic model
5. Connected to this Streamlit UI + Chatbot
""")

# Footer
st.markdown("---")
st.info("Built with ❤️ on Snowflake using Cortex, Streamlit, and Native Apps.")

