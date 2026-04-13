"""
BART Footfall Intelligence Dashboard
=====================================
Consolidated Streamlit app for Hugging Face Spaces deployment.
Combines publisher (511.org poller), subscriber (Pub/Sub + H2O ML),
and dashboard UI into a single process with background threads.

Optimizations vs. original multi-script pipeline:
  - Cached H2O model (survives Streamlit reruns)
  - Prediction caching (avoids redundant H2O calls within same minute)
  - Lazy imports for faster cold-start
  - Memory-limited H2O cluster (max 4G)
  - Station coordinate lookup uses fallback data (avoids 50+ API calls)
  - GTFS-RT parsed directly to avoid extra Pub/Sub round-trip in demo mode
"""

import os
import re
import json
import time
import math
import threading
import datetime as dt
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# ============================================================
# 1. Page Config (must be first Streamlit call)
# ============================================================
st.set_page_config(
    page_title="BART Footfall Intelligence",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ============================================================
# 2. Constants
# ============================================================
BART_STATIONS = [
    {"name": "12th St. Oakland City Center", "abbr": "12TH", "lat": 37.803664, "lon": -122.271604},
    {"name": "16th St. Mission", "abbr": "16TH", "lat": 37.765062, "lon": -122.419694},
    {"name": "19th St. Oakland", "abbr": "19TH", "lat": 37.80787, "lon": -122.26898},
    {"name": "24th St. Mission", "abbr": "24TH", "lat": 37.752254, "lon": -122.418466},
    {"name": "Antioch", "abbr": "ANTC", "lat": 37.995388, "lon": -121.78042},
    {"name": "Ashby", "abbr": "ASHB", "lat": 37.853024, "lon": -122.26978},
    {"name": "Balboa Park", "abbr": "BALB", "lat": 37.721981, "lon": -122.447414},
    {"name": "Bay Fair", "abbr": "BAYF", "lat": 37.697185, "lon": -122.126871},
    {"name": "Berryessa/North San Jose", "abbr": "BERY", "lat": 37.368473, "lon": -121.874681},
    {"name": "Castro Valley", "abbr": "CAST", "lat": 37.690746, "lon": -122.075679},
    {"name": "Civic Center/UN Plaza", "abbr": "CIVC", "lat": 37.779695, "lon": -122.414123},
    {"name": "Coliseum", "abbr": "COLS", "lat": 37.754006, "lon": -122.197273},
    {"name": "Concord", "abbr": "CONC", "lat": 37.973737, "lon": -122.029095},
    {"name": "Daly City", "abbr": "DALY", "lat": 37.706121, "lon": -122.469081},
    {"name": "Downtown Berkeley", "abbr": "DBRK", "lat": 37.870104, "lon": -122.268133},
    {"name": "Dublin/Pleasanton", "abbr": "DUBL", "lat": 37.701695, "lon": -121.899179},
    {"name": "El Cerrito del Norte", "abbr": "DELN", "lat": 37.925655, "lon": -122.317269},
    {"name": "El Cerrito Plaza", "abbr": "PLZA", "lat": 37.902632, "lon": -122.298904},
    {"name": "Embarcadero", "abbr": "EMBR", "lat": 37.792976, "lon": -122.397022},
    {"name": "Fremont", "abbr": "FRMT", "lat": 37.557465, "lon": -121.976608},
    {"name": "Fruitvale", "abbr": "FTVL", "lat": 37.774836, "lon": -122.224175},
    {"name": "Glen Park", "abbr": "GLEN", "lat": 37.732888, "lon": -122.43402},
    {"name": "Lafayette", "abbr": "LAFY", "lat": 37.893394, "lon": -122.123801},
    {"name": "Lake Merritt", "abbr": "LAKE", "lat": 37.797484, "lon": -122.265609},
    {"name": "MacArthur", "abbr": "MCAR", "lat": 37.828251, "lon": -122.267192},
    {"name": "Milpitas", "abbr": "MLPT", "lat": 37.410277, "lon": -121.891111},
    {"name": "Montgomery St.", "abbr": "MONT", "lat": 37.789355, "lon": -122.401942},
    {"name": "North Berkeley", "abbr": "NBRK", "lat": 37.87404, "lon": -122.283451},
    {"name": "North Concord/Martinez", "abbr": "NCON", "lat": 38.003193, "lon": -122.024653},
    {"name": "Oakland International Airport", "abbr": "OAKL", "lat": 37.713238, "lon": -122.212191},
    {"name": "Orinda", "abbr": "ORIN", "lat": 37.878361, "lon": -122.183791},
    {"name": "Pittsburg/Bay Point", "abbr": "PITT", "lat": 38.018914, "lon": -121.945154},
    {"name": "Pittsburg Center", "abbr": "PCTR", "lat": 38.0182, "lon": -121.889457},
    {"name": "Pleasant Hill/Contra Costa Centre", "abbr": "PHIL", "lat": 37.928403, "lon": -122.056013},
    {"name": "Powell St.", "abbr": "POWL", "lat": 37.784991, "lon": -122.406857},
    {"name": "Richmond", "abbr": "RICH", "lat": 37.936887, "lon": -122.353165},
    {"name": "Rockridge", "abbr": "ROCK", "lat": 37.844601, "lon": -122.251794},
    {"name": "San Bruno", "abbr": "SBRN", "lat": 37.637753, "lon": -122.416038},
    {"name": "San Francisco International Airport", "abbr": "SFIA", "lat": 37.615966, "lon": -122.392409},
    {"name": "San Leandro", "abbr": "SANL", "lat": 37.722619, "lon": -122.160915},
    {"name": "South Hayward", "abbr": "SHAY", "lat": 37.634799, "lon": -122.05755},
    {"name": "South San Francisco", "abbr": "SSAN", "lat": 37.664174, "lon": -122.444116},
    {"name": "Union City", "abbr": "UCTY", "lat": 37.590746, "lon": -122.017282},
    {"name": "Walnut Creek", "abbr": "WCRK", "lat": 37.905522, "lon": -122.067527},
    {"name": "Warm Springs/South Fremont", "abbr": "WARM", "lat": 37.5030, "lon": -121.9400},
    {"name": "West Dublin/Pleasanton", "abbr": "WDUB", "lat": 37.699759, "lon": -121.928099},
    {"name": "West Oakland", "abbr": "WOAK", "lat": 37.804674, "lon": -122.295159},
]

STATION_ALIAS_MAP = {
    "montgomery": "MONT", "powell": "POWL", "civiccenter": "CIVC",
    "civiccenterunplaza": "CIVC", "sfo": "SFIA", "oak": "OAKL",
    "oaklandairport": "OAKL", "westoakland": "WOAK",
    "downtownberkeley": "DBRK", "northberkeley": "NBRK",
    "unioncity": "UCTY", "warmsprings": "WARM",
    "warmspringssouthfremont": "WARM", "berryessa": "BERY",
    "millbrae": "MLBR",
}

# ============================================================
# 3. Thread-Safe Global State
# ============================================================
class GlobalState:
    def __init__(self):
        self.lock = threading.Lock()
        self.vehicles = {}
        self.stations_payload = []
        self.last_update = None
        self.day_type = ""
        self.month = 0
        self.vehicle_count = 0
        self.total_traffic = 0
        self.total_predicted = 0.0
        self.status = "initializing"
        self.status_detail = "Starting up..."

    def set_status(self, status, detail=""):
        with self.lock:
            self.status = status
            self.status_detail = detail

    def update(self, vehicles, stations, day_type, month):
        with self.lock:
            self.vehicles = {v["vehicle_id"]: v for v in vehicles}
            self.stations_payload = stations
            self.day_type = day_type
            self.month = month
            self.vehicle_count = len(vehicles)
            self.total_traffic = sum(s.get("live_traffic_count", 0) for s in stations)
            self.total_predicted = sum(s.get("predicted_ridership", 0) for s in stations)
            self.last_update = dt.datetime.now(dt.timezone.utc)
            self.status = "running"

    def snapshot(self):
        with self.lock:
            return {
                "vehicles": list(self.vehicles.values()),
                "stations": list(self.stations_payload),
                "vehicle_count": self.vehicle_count,
                "total_traffic": self.total_traffic,
                "total_predicted": self.total_predicted,
                "day_type": self.day_type,
                "month": self.month,
                "last_update": self.last_update,
                "status": self.status,
                "status_detail": self.status_detail,
            }

@st.cache_resource
def get_state():
    return GlobalState()

# ============================================================
# 4. Utility Helpers
# ============================================================
def _norm(text):
    return re.sub(r"[^a-z0-9]", "", str(text).lower())

def _haversine(lat1, lon1, lat2, lon2):
    r = 6371.0
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return r * 2 * math.asin(min(1.0, math.sqrt(a)))

def _day_type(now):
    if now.weekday() < 5: return "Average Weekday"
    if now.weekday() == 5: return "Average Saturday"
    return "Average Sunday"

def _get_credentials():
    """Load GCP credentials from Streamlit secrets or local file."""
    try:
        from google.oauth2 import service_account
        if "GCP_SERVICE_ACCOUNT_JSON" in st.secrets:
            info = json.loads(st.secrets["GCP_SERVICE_ACCOUNT_JSON"])
            return service_account.Credentials.from_service_account_info(info)
        if Path("admin-key.json").exists():
            return service_account.Credentials.from_service_account_file("admin-key.json")
    except Exception:
        pass
    return None

def _build_station_lookup(origins):
    """Map ridership origin names to BART station coordinates."""
    by_name = {_norm(s["name"]): s for s in BART_STATIONS}
    by_abbr = {_norm(s["abbr"]): s for s in BART_STATIONS}
    lookup = {}
    for origin in origins:
        key = _norm(origin)
        stn = by_name.get(key) or by_abbr.get(key)
        if stn is None and key in STATION_ALIAS_MAP:
            stn = by_abbr.get(_norm(STATION_ALIAS_MAP[key]))
        if stn is None:
            for nk, s in by_name.items():
                if key in nk or nk in key:
                    stn = s
                    break
        if stn:
            lookup[origin] = stn
    return lookup

def _nearest_station(lat, lon):
    best, best_km = None, 1e9
    for s in BART_STATIONS:
        km = _haversine(lat, lon, s["lat"], s["lon"])
        if km < best_km:
            best, best_km = s, km
    return best, best_km

# ============================================================
# 5. Background Pipeline Worker
# ============================================================
def _pipeline_worker(state: GlobalState):
    """
    Single background thread that handles:
      1. GTFS-RT polling from 511.org
      2. Publishing to Pub/Sub
      3. Subscribing to Pub/Sub for vehicle positions
      4. H2O model training & prediction
      5. Snapshot building
    """
    import requests
    import h2o
    from h2o.automl import H2OAutoML

    # --- Config from secrets with fallbacks ---
    api_key = st.secrets.get("API_511_KEY", "706e23ec-449e-4a65-b12e-1372b6b553e4")
    project_id = st.secrets.get("GCP_PROJECT_ID", "bigdataproject-493002")
    topic_id = st.secrets.get("GCP_TOPIC_ID", "transit-realtime-data")
    sub_id = st.secrets.get("GCP_SUBSCRIPTION_ID", "transit-realtime-data-sub")

    creds = _get_credentials()
    vehicles = {}  # vehicle_id -> record
    prediction_cache = {}  # (day_type, month) -> {origin: value}
    model = None
    default_dest_map = {}
    global_dest = ""
    station_lookup = {}

    # ── Step 1: Initialize H2O & train model ──
    state.set_status("initializing", "Starting H2O cluster...")
    try:
        h2o.init(max_mem_size="4G", nthreads=-1, log_level="WARN")
        state.set_status("initializing", "H2O cluster ready. Loading ridership data...")
    except Exception as e:
        state.set_status("error", f"H2O init failed: {e}")
        return

    ridership_path = Path("combined_ridership_2025_full.xlsx")
    if not ridership_path.exists():
        state.set_status("error", "Ridership file not found")
        return

    try:
        df = pd.read_excel(ridership_path)
        df = df[df["DayType"] != "Total Trips"].copy()
        df = df.dropna(subset=["Ridership"])
        df["Month"] = df["Period"].astype(str).str[-2:].astype(int)

        default_dest_map = (
            df.groupby("Origin")["Destination"]
            .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0])
            .to_dict()
        )
        global_dest = df["Destination"].mode().iloc[0]
        origins = sorted(df["Origin"].dropna().astype(str).unique().tolist())
        station_lookup = _build_station_lookup(origins)

        state.set_status("training", f"Training model on {len(df):,} rows...")
        hf = h2o.H2OFrame(df)
        for col in ["Origin", "Destination", "DayType", "Month"]:
            hf[col] = hf[col].asfactor()

        aml = H2OAutoML(max_runtime_secs=90, seed=42, sort_metric="RMSE",
                        exclude_algos=["DeepLearning"])  # Skip DL for speed
        aml.train(x=["Origin", "Destination", "DayType", "Month"],
                  y="Ridership", training_frame=hf)
        model = aml.leader
        state.set_status("training", f"Model ready: {model.model_id}")
    except Exception as e:
        state.set_status("error", f"Model training failed: {e}")
        return

    # ── Step 2: Connect Pub/Sub publisher + subscriber ──
    publisher = subscriber = sub_future = None
    try:
        from google.cloud import pubsub_v1
        from google.transit import gtfs_realtime_pb2

        if creds:
            publisher = pubsub_v1.PublisherClient(credentials=creds)
            topic_path = publisher.topic_path(project_id, topic_id)

            subscriber_client = pubsub_v1.SubscriberClient(credentials=creds)
            sub_path = subscriber_client.subscription_path(project_id, sub_id)

            def _on_message(msg):
                try:
                    data = json.loads(msg.data.decode("utf-8"))
                    vid = str(data.get("vehicle_id", "")).strip()
                    if vid:
                        vehicles[vid] = {
                            "vehicle_id": vid,
                            "route": data.get("route", ""),
                            "trip_id": data.get("trip_id", ""),
                            "lat": float(data.get("lat", 0)),
                            "lon": float(data.get("lon", 0)),
                            "timestamp": float(data.get("timestamp", time.time())),
                        }
                    msg.ack()
                except Exception:
                    msg.nack()

            sub_future = subscriber_client.subscribe(sub_path, callback=_on_message)
            state.set_status("running", "Connected to Pub/Sub")
    except Exception as e:
        state.set_status("running", f"Pub/Sub unavailable ({e}), using direct polling")

    # ── Step 3: Main loop ──
    poll_url = f"https://api.511.org/Transit/VehiclePositions?api_key={api_key}&agency=BA"
    last_poll = 0

    while True:
        try:
            now = time.time()

            # --- 3a. Poll 511.org every 30s ---
            if now - last_poll >= 30:
                last_poll = now
                try:
                    resp = requests.get(poll_url, timeout=15)
                    if resp.status_code == 200:
                        # Publish raw bytes to Pub/Sub
                        if publisher:
                            try:
                                publisher.publish(topic_path, resp.content)
                            except Exception:
                                pass

                        # Also parse locally for direct vehicle tracking
                        try:
                            feed = gtfs_realtime_pb2.FeedMessage()
                            feed.ParseFromString(resp.content)
                            for entity in feed.entity:
                                if entity.HasField("vehicle"):
                                    v = entity.vehicle
                                    vid = v.vehicle.id
                                    if vid:
                                        vehicles[vid] = {
                                            "vehicle_id": vid,
                                            "route": v.trip.route_id,
                                            "trip_id": v.trip.trip_id,
                                            "lat": v.position.latitude,
                                            "lon": v.position.longitude,
                                            "timestamp": float(v.timestamp or now),
                                        }
                        except Exception:
                            pass
                except Exception:
                    pass

            # --- 3b. Prune stale vehicles (>5 min old) ---
            cutoff = now - 300
            stale = [vid for vid, v in vehicles.items() if v["timestamp"] < cutoff]
            for vid in stale:
                del vehicles[vid]

            # --- 3c. Build station predictions ---
            now_dt = dt.datetime.now()
            day_type = _day_type(now_dt)
            month_val = now_dt.month
            cache_key = (day_type, month_val, now_dt.minute // 5)  # Cache per 5 min

            if cache_key not in prediction_cache and model:
                rows = [
                    {"Origin": o, "Destination": default_dest_map.get(o, global_dest),
                     "DayType": day_type, "Month": int(month_val)}
                    for o in station_lookup.keys()
                ]
                if rows:
                    hf_in = h2o.H2OFrame(pd.DataFrame(rows))
                    for col in ["Origin", "Destination", "DayType", "Month"]:
                        hf_in[col] = hf_in[col].asfactor()
                    preds = model.predict(hf_in).as_data_frame()
                    prediction_cache[cache_key] = {
                        rows[i]["Origin"]: max(0, float(preds.iloc[i]["predict"]))
                        for i in range(len(rows))
                    }
                    # Keep only latest cache entry
                    if len(prediction_cache) > 3:
                        oldest = min(prediction_cache.keys())
                        del prediction_cache[oldest]

            predictions = prediction_cache.get(cache_key, {})

            # --- 3d. Compute live traffic per station ---
            traffic_counts = {}
            for v in vehicles.values():
                stn, km = _nearest_station(v["lat"], v["lon"])
                if stn and km <= 3.0:
                    traffic_counts[stn["name"]] = traffic_counts.get(stn["name"], 0) + 1

            # --- 3e. Build stations payload ---
            stations_payload = []
            for origin_name, stn in station_lookup.items():
                live = traffic_counts.get(stn["name"], 0)
                predicted = predictions.get(origin_name, 0.0)
                stations_payload.append({
                    "origin": origin_name,
                    "station_name": stn["name"],
                    "station_abbr": stn["abbr"],
                    "lat": stn["lat"],
                    "lon": stn["lon"],
                    "live_traffic_count": int(live),
                    "predicted_ridership": round(predicted, 2),
                    "footfall_score": round(predicted * (1 + 0.03 * live), 2),
                })

            stations_payload.sort(key=lambda x: x["footfall_score"], reverse=True)

            # --- 3f. Push to global state ---
            state.update(list(vehicles.values()), stations_payload, day_type, month_val)

        except Exception as e:
            state.set_status("error", str(e))

        time.sleep(20)


@st.cache_resource
def start_pipeline():
    """Start the background pipeline exactly once (survives Streamlit reruns)."""
    state = get_state()
    t = threading.Thread(target=_pipeline_worker, args=(state,), daemon=True, name="pipeline")
    t.start()
    return state


# ============================================================
# 6. Dashboard UI
# ============================================================
def main():
    state = start_pipeline()
    snap = state.snapshot()

    # --- Header ---
    st.title("🚇 BART Footfall Intelligence Dashboard")
    st.caption("Real-time transit traffic + ML ridership prediction powered by H2O AutoML")

    # --- Status Bar ---
    status = snap["status"]
    if status == "initializing":
        st.info(f"⏳ **Initializing:** {snap['status_detail']}")
        time.sleep(3)
        st.rerun()
    elif status == "training":
        st.warning(f"🧠 **Training Model:** {snap['status_detail']}")
        time.sleep(5)
        st.rerun()
    elif status == "error":
        st.error(f"❌ **Error:** {snap['status_detail']}")
        time.sleep(10)
        st.rerun()

    # --- Metrics Row ---
    last_upd = snap["last_update"]
    age_str = "N/A"
    if last_upd:
        age = (dt.datetime.now(dt.timezone.utc) - last_upd).total_seconds()
        age_str = f"{int(age)}s ago"

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🚌 Live Vehicles", f"{snap['vehicle_count']:,}")
    c2.metric("🚉 Stations", f"{len(snap['stations']):,}")
    c3.metric("📊 Live Traffic", f"{snap['total_traffic']:,}")
    c4.metric("🔮 Predicted Ridership", f"{snap['total_predicted']:,.0f}")
    c5.metric("⏱ Last Update", age_str)

    st.markdown(
        f"**Day Type:** {snap['day_type']}  •  **Month:** {snap['month']}  •  "
        f"**Status:** {snap['status']}"
    )

    # --- Station Footfall Map ---
    stations_df = pd.DataFrame(snap["stations"])
    vehicles_df = pd.DataFrame(snap["vehicles"])

    if not stations_df.empty:
        left, right = st.columns([2, 1])

        with left:
            st.subheader("Station Footfall Forecast")
            fig = px.scatter_mapbox(
                stations_df, lat="lat", lon="lon",
                size="predicted_ridership", color="live_traffic_count",
                hover_name="station_name",
                hover_data={
                    "station_abbr": True, "origin": True,
                    "predicted_ridership": ":,.0f", "live_traffic_count": True,
                    "footfall_score": ":,.0f", "lat": False, "lon": False,
                },
                zoom=8.7, center={"lat": 37.77, "lon": -122.27},
                color_continuous_scale="YlOrRd", size_max=35,
                mapbox_style="open-street-map",
            )
            fig.update_layout(margin={"l": 0, "r": 0, "t": 0, "b": 0}, height=550)
            st.plotly_chart(fig, use_container_width=True)

        with right:
            st.subheader("Top Stations")
            if not stations_df.empty:
                top = (
                    stations_df[["station_name", "live_traffic_count",
                                 "predicted_ridership", "footfall_score"]]
                    .sort_values("footfall_score", ascending=False)
                    .head(15)
                    .reset_index(drop=True)
                )
                top.index = top.index + 1
                st.dataframe(top, use_container_width=True, height=510)

    # --- Live Vehicle Map ---
    if not vehicles_df.empty:
        st.subheader("Live Vehicle Overlay")
        fig_v = px.scatter_mapbox(
            vehicles_df, lat="lat", lon="lon", color="route",
            hover_name="vehicle_id",
            hover_data={"trip_id": True, "lat": ":.5f", "lon": ":.5f"},
            zoom=8.7, center={"lat": 37.77, "lon": -122.27},
            mapbox_style="carto-positron",
        )
        fig_v.update_layout(
            margin={"l": 0, "r": 0, "t": 0, "b": 0}, height=450,
            legend_title_text="Route",
        )
        st.plotly_chart(fig_v, use_container_width=True)
    elif snap["status"] == "running":
        st.info("No live vehicles detected yet. Waiting for data stream...")

    # --- Footer ---
    st.divider()
    st.caption("Auto-refreshes every 15 seconds. Powered by 511.org GTFS-RT, Google Cloud Pub/Sub, and H2O AutoML.")

    # --- Auto-refresh ---
    time.sleep(15)
    st.rerun()


if __name__ == "__main__":
    main()
