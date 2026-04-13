"""
BART Footfall Intelligence Dashboard
=====================================
Consolidated Dash app for Hugging Face Spaces deployment.
Combines publisher (511.org poller), subscriber (Pub/Sub + H2O ML),
and dashboard UI into a single process with background threads.
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
import plotly.graph_objects as go
from dash import Dash, html, dcc, dash_table, Input, Output, callback

# ============================================================
# 1. Constants & Station Data
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
    {"name": "Hayward", "abbr": "HAYW", "lat": 37.670399, "lon": -122.086143},
    {"name": "Colma", "abbr": "COLM", "lat": 37.684638, "lon": -122.466233},
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
    "berryessanorthsanjose": "BERY", "milpitas": "MLPT",
    "millbrae": "MLBR", "pittsburgcenter": "PCTR", "pittsburgbaypoint": "PITT",
    "northconcord": "NCON", "pleasanthill": "PHIL", "bayfair": "BAYF",
    "coliseum": "COLS", "sanleandro": "SANL", "southhayward": "SHAY",
    "hayward": "HAYW", "fremont": "FRMT"
}

# Complete mapping of BART's internal 2-letter ridership codes → 4-letter GTFS abbreviations.
# The ridership CSV uses these 2-letter codes, which don't match station names or GTFS abbrs.
# This map is why only ~25/50 stations were matching previously.
BART_2LETTER_TO_ABBR = {
    "12": "12TH",  # 12th St. Oakland City Center
    "16": "16TH",  # 16th St. Mission
    "19": "19TH",  # 19th St. Oakland
    "24": "24TH",  # 24th St. Mission
    "AN": "ANTC",  # Antioch
    "AS": "ASHB",  # Ashby
    "BE": "BERY",  # Berryessa/North San Jose
    "BF": "BAYF",  # Bay Fair
    "BK": "DBRK",  # Downtown Berkeley
    "BP": "BALB",  # Balboa Park
    "CC": "CIVC",  # Civic Center/UN Plaza
    "CL": "COLS",  # Coliseum
    "CM": "COLM",  # Colma
    "CN": "CONC",  # Concord
    "CV": "CAST",  # Castro Valley
    "DC": "DALY",  # Daly City
    "ED": "DELN",  # El Cerrito del Norte
    "EM": "EMBR",  # Embarcadero
    "EN": "PITT",  # Pittsburg/Bay Point
    "EP": "PLZA",  # El Cerrito Plaza
    "FM": "FRMT",  # Fremont
    "FV": "FTVL",  # Fruitvale
    "GP": "GLEN",  # Glen Park
    "HY": "HAYW",  # Hayward
    "LF": "LAFY",  # Lafayette
    "LM": "LAKE",  # Lake Merritt
    "MA": "MCAR",  # MacArthur
    "MB": "MLBR",  # Millbrae
    "ML": "MLPT",  # Milpitas
    "MT": "MONT",  # Montgomery St.
    "NB": "NBRK",  # North Berkeley
    "NC": "NCON",  # North Concord/Martinez
    "OA": "OAKL",  # Oakland International Airport
    "OR": "ORIN",  # Orinda
    "OW": "WOAK",  # West Oakland
    "PC": "PCTR",  # Pittsburg Center
    "PH": "PHIL",  # Pleasant Hill
    "PL": "POWL",  # Powell St.
    "RM": "RICH",  # Richmond
    "RR": "ROCK",  # Rockridge
    "SB": "SBRN",  # San Bruno
    "SH": "SHAY",  # South Hayward
    "SL": "SANL",  # San Leandro
    "SO": "SSAN",  # South San Francisco
    "SS": "SFIA",  # San Francisco International Airport (SFO)
    "UC": "UCTY",  # Union City
    "WC": "WCRK",  # Walnut Creek
    "WD": "WDUB",  # West Dublin/Pleasanton
    "WP": "WARM",  # Warm Springs/South Fremont
    "WS": "DUBL",  # Dublin/Pleasanton
}

# ============================================================
# 2. Thread-Safe Global State
# ============================================================
class GlobalState:
    def __init__(self):
        self.lock = threading.Lock()
        self.vehicles = {}
        self.stations_payload = []
        self.last_update = None
        self.day_type = "Unknown"
        self.month = 1
        self.vehicle_count = 0
        self.total_traffic = 0
        self.total_predicted = 0.0
        self.status = "initializing"
        self.status_detail = "Initializing..."
        self.agencies = {}

    def set_status(self, status, detail=""):
        with self.lock:
            self.status = status
            self.status_detail = detail

    def update(self, vehicles, stations, day_type, month, agencies=None):
        with self.lock:
            self.vehicles = {v["vehicle_id"]: v for v in vehicles}
            self.stations_payload = stations
            self.day_type = day_type
            self.month = month
            self.vehicle_count = len(vehicles)
            self.total_traffic = sum(s.get("live_traffic_count", 0) for s in stations)
            self.total_predicted = sum(s.get("predicted_ridership", 0) for s in stations)
            self.last_update = dt.datetime.now(dt.timezone.utc)
            self.status = "running" if len(vehicles) > 0 else "running (no live data)"
            if agencies is not None:
                self.agencies = agencies

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
                "agencies": dict(self.agencies),
            }

STATE = GlobalState()

# ============================================================
# 3. Utility Helpers
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
    try:
        from google.oauth2 import service_account
        gcp_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
        if gcp_json:
            return service_account.Credentials.from_service_account_info(json.loads(gcp_json))
        if Path("admin-key.json").exists():
            return service_account.Credentials.from_service_account_file("admin-key.json")
    except Exception: pass
    return None

def _build_station_lookup(origins):
    by_name = {_norm(s["name"]): s for s in BART_STATIONS}
    by_abbr = {_norm(s["abbr"]): s for s in BART_STATIONS}
    lookup = {}
    unmatched = []
    for origin in origins:
        key = _norm(origin)
        stn = None
        # 1. Try BART 2-letter internal code map first (covers all 50 ridership CSV codes)
        if origin.upper() in BART_2LETTER_TO_ABBR:
            stn = by_abbr.get(_norm(BART_2LETTER_TO_ABBR[origin.upper()]))
        # 2. Exact match on full name or abbreviation
        if stn is None:
            stn = by_name.get(key) or by_abbr.get(key)
        # 3. Known alias map
        if stn is None and key in STATION_ALIAS_MAP:
            stn = by_abbr.get(_norm(STATION_ALIAS_MAP[key]))
        # 4. Loose containment match (last resort)
        if stn is None:
            for nk, s in by_name.items():
                if key in nk or nk in key:
                    stn = s; break
        if stn:
            lookup[origin] = stn
        else:
            unmatched.append(origin)
    if unmatched:
        print(f"[station_lookup] Could not match {len(unmatched)} origins: {unmatched}")
    print(f"[station_lookup] Matched {len(lookup)}/{len(origins)} origins to stations.")
    return lookup

def _nearest_station(lat, lon):
    best, best_km = None, 1e9
    for s in BART_STATIONS:
        km = _haversine(lat, lon, s["lat"], s["lon"])
        if km < best_km: best, best_km = s, km
    return best, best_km

# ============================================================
# 4. Background Pipeline Worker
# ============================================================
def _pipeline_worker():
    import requests, h2o
    from h2o.automl import H2OAutoML
    from google.cloud import pubsub_v1
    from google.transit import gtfs_realtime_pb2

    # Config
    api_key = os.environ.get("API_511_KEY", "706e23ec-449e-4a65-b12e-1372b6b553e4")
    project_id = os.environ.get("GCP_PROJECT_ID", "bigdataproject-493002")
    topic_id = os.environ.get("GCP_TOPIC_ID", "transit-realtime-data")
    sub_id = os.environ.get("GCP_SUBSCRIPTION_ID", "transit-realtime-data-sub")
    
    creds = _get_credentials()
    vehicles = {}
    prediction_cache = {}
    model = None
    station_lookup = {}
    default_dest_map = {}
    global_dest = "Embarcadero"
    
    # Lock for thread-safe access to vehicles dict
    vehicles_lock = threading.Lock()

    # H2O Setup
    STATE.set_status("initializing", "Starting H2O cluster...")
    try:
        h2o.init(max_mem_size="4G", nthreads=-1, log_level="WARN")
        STATE.set_status("initializing", "H2O ready. Loading data...")
    except Exception as e:
        STATE.set_status("error", f"H2O init failed: {e}")
        return

    # Data Load
    ridership_path = Path("combined_ridership_2025.csv")
    if not ridership_path.exists():
        STATE.set_status("error", "CSV file not found")
        return

    try:
        df = pd.read_csv(ridership_path)
        df = df[df["DayType"] != "Total Trips"].copy()
        df = df.dropna(subset=["Ridership"])
        df["Month"] = df["Period"].astype(str).str[-2:].astype(int)
        
        origins = df["Origin"].unique().tolist()
        station_lookup = _build_station_lookup(origins)
        default_dest_map = df.groupby("Origin")["Destination"].agg(lambda x: x.mode().iloc[0]).to_dict()
        global_dest = df["Destination"].mode().iloc[0]

        STATE.set_status("training", f"Training model on {len(df):,} rows...")
        hf = h2o.H2OFrame(df)
        for col in ["Origin", "Destination", "DayType", "Month"]: hf[col] = hf[col].asfactor()
        
        aml = H2OAutoML(max_runtime_secs=90, seed=42, sort_metric="RMSE", exclude_algos=["DeepLearning"])
        aml.train(x=["Origin", "Destination", "DayType", "Month"], y="Ridership", training_frame=hf)
        model = aml.leader
        STATE.set_status("running", "Model trained. Connecting to stream...")
    except Exception as e:
        STATE.set_status("error", f"Training failed: {e}")
        return

    # Pub/Sub Setup
    publisher = None
    topic_path = None
    sub_client = None
    if creds:
        try:
            publisher = pubsub_v1.PublisherClient(credentials=creds)
            topic_path = publisher.topic_path(project_id, topic_id)

            def _on_message(msg):
                try:
                    # Handle JSON (from pub.py) or raw Protobuf
                    try:
                        data = json.loads(msg.data.decode("utf-8"))
                        vid = str(data.get("vehicle_id", "")).strip()
                        if vid:
                            with vehicles_lock:
                                vehicles[vid] = {
                                    "vehicle_id": vid, "lat": float(data.get("lat", 0)),
                                    "lon": float(data.get("lon", 0)), "route": data.get("route", ""),
                                    "timestamp": float(data.get("timestamp", time.time())),
                                }
                            print(f"[pubsub] Received vehicle {vid} (JSON)")
                    except:
                        feed = gtfs_realtime_pb2.FeedMessage()
                        feed.ParseFromString(msg.data)
                        for entity in feed.entity:
                            if entity.HasField("vehicle"):
                                v = entity.vehicle
                                vid = v.vehicle.id
                                if vid:
                                    with vehicles_lock:
                                        vehicles[vid] = {
                                            "vehicle_id": vid, "lat": v.position.latitude,
                                            "lon": v.position.longitude, "route": v.trip.route_id,
                                            "timestamp": float(v.timestamp or time.time()),
                                        }
                                    print(f"[pubsub] Received vehicle {vid} (Protobuf)")
                    msg.ack()
                except:
                    msg.nack()

            sub_client = pubsub_v1.SubscriberClient(credentials=creds)
            sub_path = sub_client.subscription_path(project_id, sub_id)
            sub_client.subscribe(sub_path, callback=_on_message)
            STATE.set_status("running", f"Pub/Sub connected to {sub_id}. Connecting to stream...")
            print(f"[pipeline] Pub/Sub listener started on {sub_id}")
        except Exception as e:
            STATE.set_status("running", f"Pub/Sub setup failed: {e}. Falling back to direct poll.")
            print(f"[pipeline] Pub/Sub setup error: {e}")
    else:
        STATE.set_status("running", "No GCP credentials. Using direct 511 poll (check HF secrets if on Space).")
        print("[pipeline] No credentials found. Pub/Sub disabled.")

    # Main Loop
    poll_api_key = os.environ.get("API_511_KEY", "c8565f6e-dae6-4779-8b1b-be266e3b0b6a")
    poll_url = f"https://api.511.org/Transit/VehiclePositions?api_key={poll_api_key}&agency=RG"
    last_poll = 0

    while True:
        try:
            now = time.time()
            # 1. Poll 511.org API directly every 65s (Rate limit: 60/hr)
            if now - last_poll >= 65:
                last_poll = now
                try:
                    resp = requests.get(poll_url, timeout=15)
                    if resp.status_code == 200:
                        raw = resp.content
                        if raw.startswith(b'\xef\xbb\xbf'): raw = raw[3:]
                        # Forward to Pub/Sub if available
                        if publisher and topic_path:
                            try:
                                publisher.publish(topic_path, raw)
                            except:
                                pass
                        # Always parse directly so vehicles populate even without Pub/Sub
                        feed = gtfs_realtime_pb2.FeedMessage()
                        feed.ParseFromString(raw)
                        for entity in feed.entity:
                            if entity.HasField("vehicle"):
                                v = entity.vehicle
                                vid = v.vehicle.id
                                if vid:
                                    with vehicles_lock:
                                        vehicles[vid] = {
                                            "vehicle_id": vid, "lat": v.position.latitude,
                                            "lon": v.position.longitude, "route": v.trip.route_id,
                                            "timestamp": now,
                                        }
                        with vehicles_lock:
                            v_count = len(vehicles)
                        print(f"[pipeline] Polled 511 API: {v_count} vehicles tracked")
                    else:
                        print(f"[pipeline] 511 API returned {resp.status_code}: {resp.text[:200]}")
                        STATE.set_status("running (no live data)", f"511 API error {resp.status_code} — check API key")
                except requests.exceptions.Timeout:
                    print("[pipeline] 511 API timed out")
                    STATE.set_status("running (no live data)", "511 API timed out — possible network block on HF Spaces")
                except Exception as poll_err:
                    print(f"[pipeline] 511 poll error: {poll_err}")
                    STATE.set_status("running (no live data)", f"Poll error: {poll_err}")

            # 2. Prune
            with vehicles_lock:
                stale = [vid for vid, v in vehicles.items() if v["timestamp"] < now - 300]
                for vid in stale: del vehicles[vid]
                current_vehicles = list(vehicles.values())

            # 3. Predict & Payload
            now_dt = dt.datetime.now()
            day_type = _day_type(now_dt)
            month_val = now_dt.month
            cache_key = (day_type, month_val, now_dt.minute // 10)

            if cache_key not in prediction_cache and model:
                try:
                    prows = [
                        {"Origin": o, "Destination": default_dest_map.get(o, global_dest),
                         "DayType": day_type, "Month": int(month_val)}
                        for o in station_lookup.keys()
                    ]
                    hf_in = h2o.H2OFrame(pd.DataFrame(prows))
                    for col in ["Origin", "Destination", "DayType", "Month"]: hf_in[col] = hf_in[col].asfactor()
                    preds = model.predict(hf_in).as_data_frame()
                    prediction_cache[cache_key] = { prows[i]["Origin"]: max(0, float(preds.iloc[i]["predict"])) for i in range(len(prows)) }
                except: prediction_cache[cache_key] = {}

            preds_dict = prediction_cache.get(cache_key, {})
            traffic_counts = {}
            agency_counts = {}
            for v in current_vehicles:
                # 1. Nearby Traffic
                stn, km = _nearest_station(v["lat"], v["lon"])
                if stn and km <= 3.0: traffic_counts[stn["name"]] = traffic_counts.get(stn["name"], 0) + 1
                
                # 2. Improved Agency Distribution Logic
                route = v.get("route", "")
                agency = "Other"
                if route:
                    if ":" in route:
                        agency = route.split(":")[0]
                    elif any(p in route for p in ["YL-", "RD-", "GN-", "OR-", "BL-"]) or route in ["YL","RD","GN","OR","BL"]:
                        agency = "BA" # BART
                    elif route.isdigit():
                        agency = "Muni/Bus"
                    elif len(route) >= 2 and route[:2].isalpha():
                        agency = route[:2] 
                agency_counts[agency] = agency_counts.get(agency, 0) + 1

            stations_payload = []
            for origin_name, stn in station_lookup.items():
                live = traffic_counts.get(stn["name"], 0)
                pred = preds_dict.get(origin_name, 0.0)
                stations_payload.append({
                    "origin": origin_name, "station_name": stn["name"], "station_abbr": stn["abbr"],
                    "lat": stn["lat"], "lon": stn["lon"], "live_traffic_count": int(live),
                    "predicted_ridership": round(pred, 2), "footfall_score": round(pred * (1 + 0.03 * live), 2),
                })
            stations_payload.sort(key=lambda x: x["footfall_score"], reverse=True)
            STATE.update(current_vehicles, stations_payload, day_type, month_val, agencies=agency_counts)
            STATE.set_status("running", f"Tracking {len(current_vehicles)} Regional Vehicles | {len(station_lookup)} stations matched")

        except Exception as e:
            STATE.set_status("error", str(e))
        time.sleep(20)

# Start Thread
threading.Thread(target=_pipeline_worker, daemon=True).start()

# ============================================================
# 5. Dash UI (Premium Theme)
# ============================================================
COLORS = {"bg": "#0a0e1a", "card": "rgba(20, 27, 45, 0.85)", "text": "#e2e8f0"}

app = Dash(__name__, title="BART Intelligence")
app.index_string = '''<!DOCTYPE html><html><head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&family=JetBrains+Mono:wght@500&display=swap" rel="stylesheet"><style>
body { font-family: 'Inter', sans-serif; background: #0a0e1a; color: #e2e8f0; margin: 0; }
.metric-card { background: rgba(20, 27, 45, 0.85); border: 1px solid rgba(99, 140, 255, 0.2); border-radius: 12px; padding: 20px; text-align: center; }
.metric-label { font-size: 11px; text-transform: uppercase; color: #94a3b8; margin-bottom: 5px; }
.metric-value { font-size: 28px; font-weight: 800; font-family: 'JetBrains Mono'; }
.section-card { background: rgba(20, 27, 45, 0.7); border-radius: 12px; padding: 20px; border: 1px solid rgba(255,255,255,0.05); }
.header { padding: 30px 40px; display: flex; justify-content: space-between; align-items: center; }
.title { font-size: 24px; font-weight: 800; background: linear-gradient(90deg, #638cff, #ffb347); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
.status-badge { padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 600; background: rgba(99,140,255,0.1); color: #638cff; border: 1px solid rgba(99,140,255,0.2); }
.detail-card { background: linear-gradient(135deg, rgba(99, 140, 255, 0.1), rgba(255, 179, 71, 0.05)); border-radius: 12px; padding: 20px; border: 1px solid rgba(255,255,255,0.1); }
.dropdown-custom .Select-control { background-color: rgba(20, 27, 45, 0.9); border: 1px solid rgba(255,255,255,0.1); color: white; }
.dropdown-custom .Select-menu-outer { background-color: #1a1f2e; color: white; }
</style></head><body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body></html>'''

app.layout = html.Div([
    dcc.Interval(id="ui-refresh", interval=10_000, n_intervals=0),
    html.Div([
        html.Div([
            html.Div("BART Footfall Intelligence", className="title"),
            html.Div("Real-time Predictions & Transit Analytics", style={"fontSize": "13px", "color": "#94a3b8"})
        ]),
        html.Div(id="ui-status", className="status-badge")
    ], className="header"),
    
    html.Div([
        html.Div(id="ui-metrics", style={"display":"grid", "gridTemplateColumns":"repeat(4, 1fr)", "gap":"20px", "marginBottom":"20px"}),
        
        html.Div([
            html.Div([
                html.Div("System Controls", style={"fontWeight":"bold", "marginBottom":"10px", "fontSize":"13px", "color":"#94a3b8"}),
                dcc.Dropdown(id="ui-station-select", placeholder="Select or Search Station...", className="dropdown-custom"),
                html.Div(id="ui-station-details", style={"marginTop":"20px"})
            ], className="section-card", style={"flex":"1"}),
            html.Div([
                html.Div("Live Regional Connection Hubs", style={"fontWeight":"bold", "marginBottom":"15px"}),
                dcc.Graph(id="ui-agency-chart", style={"height":"200px"}, config={"displayModeBar":False})
            ], className="section-card", style={"flex":"1.5"})
        ], style={"display":"flex", "gap":"20px", "marginBottom":"20px"}),
        
        html.Div([
            html.Div([
                html.Div("Live Traffic & Forecast Map", style={"fontWeight":"bold", "marginBottom":"15px"}),
                dcc.Graph(id="ui-map", style={"height":"500px"}, config={"displayModeBar":False})
            ], className="section-card", style={"flex":"2"}),
            html.Div([
                html.Div("Top Stations", style={"fontWeight":"bold", "marginBottom":"15px"}),
                html.Div(id="ui-table")
            ], className="section-card", style={"flex":"1"})
        ], style={"display":"flex", "gap":"20px"})
    ], style={"padding":"0 40px 40px"})
])

@app.callback(
    [Output("ui-map", "figure"), Output("ui-table", "children"), Output("ui-metrics", "children"), 
     Output("ui-status", "children"), Output("ui-station-select", "options"), 
     Output("ui-station-details", "children"), Output("ui-agency-chart", "figure")],
    [Input("ui-refresh", "n_intervals"), Input("ui-station-select", "value")]
)
def update_ui(n, selected_station):
    snap = STATE.snapshot()
    stations = snap.get("stations", [])
    v_count = snap.get("vehicle_count", 0)
    t_traffic = snap.get("total_traffic", 0)
    t_predicted = snap.get("total_predicted", 0.0)
    agencies = snap.get("agencies", {})
    
    # Options
    options = [{"label": s["station_name"], "value": s["origin"]} for s in sorted(stations, key=lambda x: x["station_name"])]
    
    # 1. Map
    map_fig = go.Figure()
    if stations:
        df = pd.DataFrame(stations)
        # Standard scatter for better control
        map_fig.add_trace(go.Scattermapbox(
            lat=df["lat"], lon=df["lon"], mode="markers",
            marker=go.scattermapbox.Marker(
                size=12, color=df["footfall_score"], colorscale="YlOrRd", showscale=True,
                colorbar=dict(title="Score", thickness=15, x=1.02, tickcolor="white", tickfont=dict(color="white"))
            ),
            text=df.apply(lambda r: f"{r['station_name']}<br>Live: {r['live_traffic_count']}<br>Pred: {r['predicted_ridership']}", axis=1),
            hoverinfo="text"
        ))
    
    map_layout = dict(
        mapbox=dict(style="carto-darkmatter", center=dict(lat=37.77, lon=-122.27), zoom=9),
        margin={"r":0,"t":0,"l":0,"b":0}, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"
    )
    if selected_station:
        stn = next((s for s in stations if s["origin"] == selected_station), None)
        if stn:
            map_layout["mapbox"]["center"] = {"lat": stn["lat"], "lon": stn["lon"]}
            map_layout["mapbox"]["zoom"] = 13
    map_fig.update_layout(map_layout)

    # 2. Table
    # Using simple HTML table for stability
    table_rows = [html.Tr([html.Th(c, style={"textAlign":"right" if c in ["Live","Pred","Score"] else "left"}) for c in ["Station", "Live", "Pred", "Score"]])]
    for s in stations[:10]:
        table_rows.append(html.Tr([
            html.Td(s["station_name"]),
            html.Td(s["live_traffic_count"], style={"textAlign":"right"}),
            html.Td(int(s["predicted_ridership"]), style={"textAlign":"right"}),
            html.Td(s["footfall_score"], style={"textAlign":"right", "fontWeight":"bold", "color":"#638cff"})
        ]))
    table = html.Table(table_rows, style={"width":"100%", "color":"white", "fontSize":"12px"})

    # 3. Metrics
    metrics = [
        html.Div([html.Div("LIVE VEHICLES", className="metric-label"), html.Div(v_count, className="metric-value")], className="metric-card"),
        html.Div([html.Div("STATIONS", className="metric-label"), html.Div(len(stations), className="metric-value")], className="metric-card"),
        html.Div([html.Div("CURRENT TRAFFIC", className="metric-label"), html.Div(t_traffic, className="metric-value")], className="metric-card"),
        html.Div([html.Div("TOTAL PREDICTED", className="metric-label"), html.Div(f"{int(t_predicted):,}", className="metric-value")], className="metric-card")
    ]
    
    # 4. Station Details
    details = html.Div("Select a station to view details", style={"color":"#94a3b8", "fontSize":"12px", "fontStyle":"italic", "padding":"10px"})
    if selected_station:
        s = next((s for s in stations if s["origin"] == selected_station), None)
        if s:
            details = html.Div([
                html.Div(s["station_name"], style={"fontSize":"18px", "fontWeight":"bold", "color":"#638cff", "marginBottom":"15px"}),
                html.Div([
                    html.Div([
                        html.Div("LIVE TRAFFIC", style={"fontSize":"10px", "color":"#94a3b8"}),
                        html.Div(s["live_traffic_count"], style={"fontSize":"20px", "fontWeight":"bold", "color":"#47ffa5"})
                    ], style={"flex":"1"}),
                    html.Div([
                        html.Div("BASELINE PRED", style={"fontSize":"10px", "color":"#94a3b8"}),
                        html.Div(int(s["predicted_ridership"]), style={"fontSize":"20px", "fontWeight":"bold"})
                    ], style={"flex":"1"})
                ], style={"display":"flex", "marginBottom":"15px"}),
                html.Div([
                    html.Div("FOOTFALL SCORE", style={"fontSize":"10px", "color":"#94a3b8"}),
                    html.Div(s["footfall_score"], style={"fontSize":"24px", "fontWeight":"800", "color":"#ffb347"})
                ])
            ], className="detail-card")

    # 5. Live Hubs Chart (Replaces Agency Chart)
    agency_fig = go.Figure()
    if stations:
        # Sort by live traffic to identify hubs
        hub_stations = sorted(stations, key=lambda x: x["live_traffic_count"], reverse=True)[:10]
        # Only show if there is actually traffic
        hub_stations = [s for s in hub_stations if s["live_traffic_count"] > 0]
        
        if hub_stations:
            agency_fig.add_trace(go.Bar(
                x=[s["station_name"] for s in hub_stations],
                y=[s["live_traffic_count"] for s in hub_stations],
                marker_color="#47ffa5",
                text=[f"{s['live_traffic_count']}" for s in hub_stations],
                textposition='auto',
            ))
    
    agency_fig.update_layout(
        margin={"r":10,"t":10,"l":10,"b":30}, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font={"color":"white"}, xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.05)")
    )

    return map_fig, table, metrics, f"{snap['status'].upper()} | {snap['status_detail']}", options, details, agency_fig

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860)
