import os
import time
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# 1. Setup Dashboard Configuration
STATE_FILE = Path(os.getenv("STATE_FILE", "dashboard_state.json"))
AUTO_REFRESH_SECONDS = int(os.getenv("AUTO_REFRESH_SECONDS", "15"))


def load_snapshot():
    if not STATE_FILE.exists():
        return None

    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        st.error(f"Could not read state file: {e}")
        return None


def draw_station_map(stations_df):
    if stations_df.empty:
        st.info("No station prediction data available yet.")
        return

    fig = px.scatter_mapbox(
        stations_df,
        lat="lat",
        lon="lon",
        size="predicted_ridership",
        color="live_traffic_count",
        hover_name="station_name",
        hover_data={
            "station_abbr": True,
            "origin": True,
            "predicted_ridership": ":,.0f",
            "live_traffic_count": True,
            "footfall_score": ":,.0f",
            "lat": False,
            "lon": False,
        },
        zoom=8.7,
        center={"lat": 37.77, "lon": -122.27},
        color_continuous_scale="YlOrRd",
        size_max=35,
        title="BART Station Footfall Forecast (Live Traffic + ML Prediction)",
    )

    fig.update_layout(
        mapbox_style="open-street-map",
        margin={"l": 0, "r": 0, "t": 50, "b": 0},
        height=620,
    )
    st.plotly_chart(fig, use_container_width=True)


def draw_vehicle_map(vehicles_df):
    if vehicles_df.empty:
        st.info("No live vehicle positions available yet.")
        return

    fig = px.scatter_mapbox(
        vehicles_df,
        lat="lat",
        lon="lon",
        color="route",
        hover_name="vehicle_id",
        hover_data={"trip_id": True, "timestamp": True, "lat": ":.5f", "lon": ":.5f"},
        zoom=8.7,
        center={"lat": 37.77, "lon": -122.27},
        title="Live Transit Vehicle Stream",
    )
    fig.update_layout(
        mapbox_style="carto-positron",
        margin={"l": 0, "r": 0, "t": 50, "b": 0},
        height=500,
        legend_title_text="Route",
    )
    st.plotly_chart(fig, use_container_width=True)


# 2. Build Streamlit Dashboard
st.set_page_config(page_title="BART Footfall Intelligence Dashboard", layout="wide")
st.title("BART Footfall Intelligence Dashboard")
st.caption("Business decision support from real-time transit traffic + ML ridership prediction")

snapshot = load_snapshot()
if snapshot is None:
    st.warning(
        f"Waiting for live state file at '{STATE_FILE}'. "
        "Start the consumer first: python sub.py"
    )
    st.stop()

stations_df = pd.DataFrame(snapshot.get("stations", []))
vehicles_df = pd.DataFrame(snapshot.get("vehicles", []))

col1, col2, col3, col4 = st.columns(4)
col1.metric("Live Vehicles", f"{snapshot.get('vehicle_count', 0):,}")
col2.metric("Stations Tracked", f"{snapshot.get('station_count', 0):,}")
col3.metric("Live Station Traffic", f"{snapshot.get('total_live_station_traffic', 0):,}")

if not stations_df.empty:
    total_predicted = stations_df["predicted_ridership"].sum()
    col4.metric("Predicted Total Ridership", f"{total_predicted:,.0f}")
else:
    col4.metric("Predicted Total Ridership", "0")

st.write(
    f"**Snapshot Time (Local):** {snapshot.get('generated_at_local', 'N/A')}  |  "
    f"**Day Type:** {snapshot.get('day_type', 'N/A')}  |  "
    f"**Month:** {snapshot.get('month', 'N/A')}"
)

left_col, right_col = st.columns([2, 1])
with left_col:
    draw_station_map(stations_df)
with right_col:
    st.subheader("Top Stations by Footfall Score")
    if stations_df.empty:
        st.info("No station rows available.")
    else:
        top_df = (
            stations_df[["station_name", "live_traffic_count", "predicted_ridership", "footfall_score"]]
            .sort_values("footfall_score", ascending=False)
            .head(15)
        )
        st.dataframe(top_df, use_container_width=True)

st.subheader("Live Vehicle Overlay")
draw_vehicle_map(vehicles_df)

st.caption(
    f"Auto-refresh every {AUTO_REFRESH_SECONDS}s. "
    "Use this dashboard to estimate likely walk-in footfall by station area."
)
time.sleep(AUTO_REFRESH_SECONDS)
st.rerun()
