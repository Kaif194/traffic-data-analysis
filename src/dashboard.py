import os
import sys
import pandas as pd
import streamlit as st

import io
import boto3

# Ensure parent directory is in sys.path for config import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import (
    LOCAL_PROCESSED_DIR, S3_BUCKET_NAME, S3_ENDPOINT_URL, AWS_REGION,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, S3_PROCESSED_PREFIX
)

st.set_page_config(
    page_title="Traffic Data Analytics Dashboard",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling for modern dark/light UI
st.markdown("""
    <style>
    .main-header {
        font-size: 2.3rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #4B5563;
        margin-bottom: 2rem;
    }
    .metric-box {
        background-color: #F3F4F6;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        text-align: center;
    }
    </style>
""", unsafe_allow_html=True)

def load_processed_data_from_s3():
    """Loads all pre-aggregated PySpark output tables directly from Garage Object Storage."""
    try:
        kwargs = {
            'aws_access_key_id': AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
            'region_name': AWS_REGION,
            'endpoint_url': S3_ENDPOINT_URL
        }
        if AWS_SESSION_TOKEN:
            kwargs['aws_session_token'] = AWS_SESSION_TOKEN
        s3 = boto3.client('s3', **kwargs)
        data = {}
        modules = ["peak_hours", "location_analysis", "day_type_analysis", "weather_analysis", "traffic_summary"]
        for mod in modules:
            key = f"{S3_PROCESSED_PREFIX}/{mod}/results.csv"
            obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            csv_text = obj['Body'].read().decode('utf-8')
            data[mod] = pd.read_csv(io.StringIO(csv_text))
        data["summary"] = data.get("traffic_summary")
        return data, True, None
    except Exception as e:
        return None, False, str(e)

@st.cache_data
def load_processed_data_local():
    """Loads all pre-aggregated PySpark output tables from local filesystem."""
    data = {}
    modules = ["peak_hours", "location_analysis", "day_type_analysis", "weather_analysis", "traffic_summary"]
    for mod in modules:
        path = os.path.join(LOCAL_PROCESSED_DIR, mod, "results.csv")
        if os.path.exists(path):
            data[mod] = pd.read_csv(path)
        else:
            data[mod] = None
    data["summary"] = data.get("traffic_summary")
    return data

def main():
    st.markdown('<div class="main-header">🚗 Traffic Data Analytics Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Powered by <b>Apache Spark / PySpark</b> & <b>Garage Object Storage / AWS S3</b></div>', unsafe_allow_html=True)

    # Sidebar Information & Source Toggle
    st.sidebar.title("📌 Navigation & Settings")
    data_source_mode = st.sidebar.radio(
        "Data Source Mode",
        ["Garage Object Storage (S3)", "Local Filesystem"]
    )

    if data_source_mode == "Garage Object Storage (S3)":
        data, s3_ok, err_msg = load_processed_data_from_s3()
        if not s3_ok or data is None or data.get("summary") is None:
            st.sidebar.warning("Could not load from Garage S3; falling back to local files.")
            if st.sidebar.button("🔄 Retry Connection to Garage"):
                st.rerun()
            with st.sidebar.expander("Troubleshooting Garage Connection"):
                st.markdown("""
                If the Garage Docker container is stopped, run in PowerShell:
                ```powershell
                docker start garage
                ```
                """)
                if err_msg:
                    st.caption(f"Error details: `{err_msg}`")
            data = load_processed_data_local()
            st.sidebar.markdown("**Active Source:** `Local Filesystem (Fallback)`")
        else:
            st.sidebar.success("Connected to Garage S3 🟢")
            st.sidebar.markdown(f"**Endpoint:** `{S3_ENDPOINT_URL}`")
            st.sidebar.markdown(f"**Bucket:** `{S3_BUCKET_NAME}`")
            st.sidebar.markdown(f"**Region:** `{AWS_REGION}`")
            st.sidebar.markdown("**Active Source:** `Garage Object Storage (S3)`")
    else:
        data = load_processed_data_local()
        st.sidebar.markdown("**Active Source:** `Local Filesystem`")

    st.sidebar.markdown("---")

    if data.get("summary") is None or data.get("peak_hours") is None:
        st.error("⚠️ Processed dataset not found! Please run `python src/traffic_analysis.py` first to process raw traffic data.")
        st.info("Execution Command: `python src/traffic_analysis.py --use-s3`")
        return

    # KPI Metric Cards Section
    summary_df = data["summary"]
    peak_df = data["peak_hours"]
    loc_df = data["location_analysis"]

    total_vehicles = int(summary_df["Total_Vehicles_Recorded"].iloc[0])
    avg_speed = float(summary_df["Overall_Avg_Speed"].iloc[0])
    busiest_loc = loc_df.iloc[0]["Location"] if loc_df is not None else "N/A"
    peak_hour = int(peak_df.sort_values(by="Avg_Vehicle_Count", ascending=False).iloc[0]["Hour"])

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Vehicles Processed", f"{total_vehicles:,}")
    with col2:
        st.metric("Overall Average Speed", f"{avg_speed:.1f} km/h")
    with col3:
        st.metric("Busiest Traffic Location", busiest_loc)
    with col4:
        st.metric("Peak Traffic Hour", f"{peak_hour:02d}:00 - {peak_hour+1:02d}:00")

    st.markdown("---")

    # Tabs for Data Exploration
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Hourly Trends & Peak Hours", 
        "📍 High-Traffic Locations", 
        "🌤️ Weather & Road Analysis", 
        "📅 Weekday vs Weekend"
    ])

    with tab1:
        st.subheader("Hourly Traffic Volume and Speed Pattern")
        df_hourly = peak_df.sort_values("Hour")
        
        st.line_chart(df_hourly.set_index("Hour")[["Avg_Vehicle_Count", "Avg_Speed_KMH"]])
        
        st.markdown("#### Hourly Summary Breakdown")
        st.dataframe(df_hourly, width=None)

    with tab2:
        st.subheader("Top Busiest Locations across Highway Network")
        if loc_df is not None:
            top_n = st.slider("Select number of top locations to display:", 3, len(loc_df), 10)
            df_top = loc_df.head(top_n)
            
            st.bar_chart(df_top.set_index("Location")["Total_Vehicles"])
            st.markdown("#### Location Ranking Data Table")
            st.dataframe(df_top, width=None)

    with tab3:
        st.subheader("Impact of Weather Conditions on Vehicle Speed & Congestion")
        weather_df = data["weather_analysis"]
        if weather_df is not None:
            col_w1, col_w2 = st.columns(2)
            with col_w1:
                st.markdown("**Average Speed by Weather Condition (km/h)**")
                st.bar_chart(weather_df.set_index("Weather")["Avg_Speed_KMH"])
            with col_w2:
                st.markdown("**Total Vehicles Recorded by Weather**")
                st.bar_chart(weather_df.set_index("Weather")["Total_Vehicles"])
                
            st.dataframe(weather_df, width=None)

    with tab4:
        st.subheader("Traffic Pattern Comparison: Weekdays vs Weekends")
        day_df = data["day_type_analysis"]
        if day_df is not None:
            st.bar_chart(day_df.set_index("Day_Type")[["Avg_Vehicle_Count", "Avg_Speed_KMH"]])
            st.dataframe(day_df, width=None)

    st.markdown("---")
    st.caption("College Big Data & Cloud Computing Project | Apache Spark & AWS S3 Integration")

if __name__ == "__main__":
    main()
