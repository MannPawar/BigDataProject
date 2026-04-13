import os
import sys
import re
import json
import time

# --- Auto-configure Environment ---
# Point to the extracted OpenJDK bin folder so H2O can find Java
java_path = r'c:\Users\mspaw\Downloads\openjdk-26\jdk-26'
if os.path.exists(java_path):
    os.environ['JAVA_HOME'] = java_path
    os.environ['PATH'] = os.path.join(java_path, 'bin') + os.pathsep + os.environ['PATH']

# Point to data and output files in the project directory
project_dir = r'c:\Users\mspaw\Downloads\BigDataProj'
os.environ['RIDERSHIP_FILE'] = r'C:\Users\mspaw\Downloads\ridership_OD_2025\combined_ridership_2025_full.xlsx'
os.environ['CLEANED_CSV_FILE'] = os.path.join(project_dir, 'bart_cleaned_for_h2o.csv')
os.environ['STATE_FILE'] = os.path.join(project_dir, 'dashboard_state.json')
os.environ['H2O_SAVE_DIR'] = project_dir
import math
import threading
import datetime as dt
from pathlib import Path

import h2o
import pandas as pd
import requests
from h2o.automl import H2OAutoML
from google.cloud import pubsub_v1

# 1. Setup Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "bigdataproject-493002")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID", "transit-realtime-data-sub")
DASHBOARD_TOPIC_ID = os.getenv("DASHBOARD_TOPIC_ID", "transit-dashboard-data")

STATE_FILE = Path(os.getenv("STATE_FILE", "dashboard_state.json"))
RIDERSHIP_FILE = Path(os.getenv("RIDERSHIP_FILE", "combined_ridership_2025_full.xlsx"))
CLEANED_CSV_FILE = Path(os.getenv("CLEANED_CSV_FILE", "bart_cleaned_for_h2o.csv"))
H2O_MODEL_PATH = os.getenv("H2O_MODEL_PATH", "")
H2O_SAVE_DIR = Path(os.getenv("H2O_SAVE_DIR", "."))

BART_API_KEY = os.getenv("BART_API_KEY", "MW9S-E7SL-26DU-VV8V")
SNAPSHOT_INTERVAL_SECONDS = int(os.getenv("SNAPSHOT_INTERVAL_SECONDS", "20"))
MAX_VEHICLE_AGE_SECONDS = int(os.getenv("MAX_VEHICLE_AGE_SECONDS", "300"))
TRAIN_IF_MODEL_MISSING = os.getenv("TRAIN_IF_MODEL_MISSING", "true").lower() == "true"
H2O_TRAIN_TIME_SECONDS = int(os.getenv("H2O_TRAIN_TIME_SECONDS", "120"))


# 2. Fallback BART station coordinates (used only if API fetch is unavailable)
FALLBACK_STATIONS = [
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
    {"name": "Oakland Airport Connector", "abbr": "OAC", "lat": 37.713238, "lon": -122.212191},
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


def ensure_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def normalize_text(text):
    return re.sub(r"[^a-z0-9]", "", str(text).lower())


def get_day_type(now_local):
    if now_local.weekday() < 5:
        return "Average Weekday"
    if now_local.weekday() == 5:
        return "Average Saturday"
    return "Average Sunday"


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


# 3. Build station metadata with lat/lon
def fetch_station_metadata():
    print("Fetching BART station metadata...")
    stations = []

    try:
        url = f"https://api.bart.gov/api/stn.aspx?cmd=stns&json=y&key={BART_API_KEY}"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        payload = response.json()

        station_list = ensure_list(payload.get("root", {}).get("stations", {}).get("station"))
        for item in station_list:
            abbr = item.get("abbr")
            if not abbr:
                continue

            info_url = f"https://api.bart.gov/api/stn.aspx?cmd=stninfo&orig={abbr}&json=y&key={BART_API_KEY}"
            info_response = requests.get(info_url, timeout=15)
            info_response.raise_for_status()
            info_payload = info_response.json()

            station_info = info_payload.get("root", {}).get("stations", {}).get("station", {})
            lat = station_info.get("gtfs_latitude") or station_info.get("latitude")
            lon = station_info.get("gtfs_longitude") or station_info.get("longitude")
            if lat is None or lon is None:
                continue

            stations.append(
                {
                    "name": station_info.get("name", item.get("name", abbr)),
                    "abbr": abbr,
                    "lat": float(lat),
                    "lon": float(lon),
                }
            )

    except Exception as e:
        print(f"Could not fetch station metadata from BART API: {e}")

    if stations:
        print(f"Loaded {len(stations)} stations from BART API.")
        return stations

    print(f"Using fallback station metadata ({len(FALLBACK_STATIONS)} stations).")
    return FALLBACK_STATIONS


# 4. H2O model manager for station-level ridership prediction
class RidershipPredictor:
    def __init__(self, ridership_path, cleaned_csv_path, model_path):
        self.ridership_path = Path(ridership_path)
        self.cleaned_csv_path = Path(cleaned_csv_path)
        self.model_path = model_path
        self.model = None
        self.df_history = None
        self.default_destination_by_origin = {}
        self.global_default_destination = ""

    def prepare_history_dataframe(self):
        if self.cleaned_csv_path.exists():
            df = pd.read_csv(self.cleaned_csv_path)
        elif self.ridership_path.exists():
            if self.ridership_path.suffix.lower() in [".xlsx", ".xls"]:
                df = pd.read_excel(self.ridership_path)
            else:
                df = pd.read_csv(self.ridership_path)

            df = df[df["DayType"] != "Total Trips"].copy()
            df = df.dropna(subset=["Ridership"])
            df["Month"] = df["Period"].astype(str).str[-2:].astype(int)
            df.to_csv(self.cleaned_csv_path, index=False)
            print(f"Saved cleaned ridership file: {self.cleaned_csv_path}")
        else:
            raise FileNotFoundError(
                f"Could not find ridership file. Set RIDERSHIP_FILE. Tried: {self.ridership_path}"
            )

        self.df_history = df.copy()

        destination_mode = (
            self.df_history.groupby("Origin")["Destination"]
            .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0])
            .to_dict()
        )
        self.default_destination_by_origin = destination_mode

        mode_series = self.df_history["Destination"].mode()
        self.global_default_destination = mode_series.iloc[0] if not mode_series.empty else ""

    def load_or_train_model(self):
        print("Initializing H2O...")
        h2o.init()
        self.prepare_history_dataframe()

        if self.model_path and Path(self.model_path).exists():
            print(f"Loading existing H2O model from {self.model_path}")
            self.model = h2o.load_model(self.model_path)
            return

        if not TRAIN_IF_MODEL_MISSING:
            raise FileNotFoundError(
                f"H2O model not found at {self.model_path}. Set H2O_MODEL_PATH or enable training."
            )

        print("H2O model path not found. Training fallback model from cleaned data...")
        hf = h2o.import_file(str(self.cleaned_csv_path))

        hf["Origin"] = hf["Origin"].asfactor()
        hf["Destination"] = hf["Destination"].asfactor()
        hf["DayType"] = hf["DayType"].asfactor()
        hf["Month"] = hf["Month"].asfactor()

        response = "Ridership"
        predictors = ["Origin", "Destination", "DayType", "Month"]

        train, _ = hf.split_frame(ratios=[0.8], seed=42)

        aml = H2OAutoML(
            max_runtime_secs=H2O_TRAIN_TIME_SECONDS,
            seed=42,
            sort_metric="RMSE",
        )
        aml.train(x=predictors, y=response, training_frame=train)
        self.model = aml.leader
        print(f"Trained fallback model: {self.model.model_id}")

        try:
            saved_path = h2o.save_model(model=self.model, path=str(H2O_SAVE_DIR), force=True)
            print(f"Saved fallback model to: {saved_path}")
        except Exception as e:
            print(f"Could not save fallback model: {e}")

    def predict_current_ridership(self, origins, day_type, month_value):
        if self.model is None:
            return {}

        rows = []
        for origin in origins:
            destination = self.default_destination_by_origin.get(origin, self.global_default_destination)
            rows.append(
                {
                    "Origin": origin,
                    "Destination": destination,
                    "DayType": day_type,
                    "Month": int(month_value),
                }
            )

        if not rows:
            return {}

        input_df = pd.DataFrame(rows)
        hf_input = h2o.H2OFrame(input_df)
        hf_input["Origin"] = hf_input["Origin"].asfactor()
        hf_input["Destination"] = hf_input["Destination"].asfactor()
        hf_input["DayType"] = hf_input["DayType"].asfactor()
        hf_input["Month"] = hf_input["Month"].asfactor()

        pred_df = self.model.predict(hf_input).as_data_frame()
        predictions = {}
        for i, row in input_df.iterrows():
            predictions[row["Origin"]] = float(pred_df.iloc[i]["predict"])
        return predictions


# 5. Real-time Pub/Sub consumer + dashboard snapshot builder
class RealTimeDashboardProcessor:
    def __init__(self):
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

        self.publisher = pubsub_v1.PublisherClient()
        self.dashboard_topic_path = self.publisher.topic_path(PROJECT_ID, DASHBOARD_TOPIC_ID)

        self.vehicle_positions = {}
        self.lock = threading.Lock()
        self.last_message_time = time.time()

        self.stations = fetch_station_metadata()
        self.predictor = RidershipPredictor(RIDERSHIP_FILE, CLEANED_CSV_FILE, H2O_MODEL_PATH)
        self.predictor.load_or_train_model()

        self.station_lookup = self.build_origin_to_station_lookup()

    def build_origin_to_station_lookup(self):
        # Build fast station lookup by normalized station name/abbr
        by_name = {}
        by_abbr = {}
        for stn in self.stations:
            by_name[normalize_text(stn["name"])] = stn
            by_abbr[normalize_text(stn["abbr"])] = stn

        # Manual aliases for common naming differences in ridership datasets
        alias_map = {
            "montgomery": "MONT",
            "powell": "POWL",
            "civiccenter": "CIVC",
            "civiccenterunplaza": "CIVC",
            "sfo": "SFIA",
            "oak": "OAKL",
            "oaklandairport": "OAKL",
            "westoakland": "WOAK",
            "downtownberkeley": "DBRK",
            "northberkeley": "NBRK",
            "unioncity": "UCTY",
            "warmsprings": "WARM",
            "warmspringssouthfremont": "WARM",
            "berryessa": "BERY",
            "millbrae": "MLBR",
        }

        origin_to_station = {}
        origins = sorted(self.predictor.df_history["Origin"].dropna().astype(str).unique().tolist())

        for origin in origins:
            key = normalize_text(origin)
            station = by_name.get(key) or by_abbr.get(key)

            if station is None and key in alias_map:
                station = by_abbr.get(normalize_text(alias_map[key]))

            if station is None:
                # Loose containment match fallback
                for name_key, stn in by_name.items():
                    if key in name_key or name_key in key:
                        station = stn
                        break

            if station is not None:
                origin_to_station[origin] = station

        print(
            f"Matched {len(origin_to_station)} dataset origins to station coordinates "
            f"(out of {len(origins)} origins)."
        )
        return origin_to_station

    def callback(self, message):
        try:
            data = json.loads(message.data.decode("utf-8"))
            vehicle_id = str(data.get("vehicle_id", "")).strip()
            if vehicle_id:
                record = {
                    "vehicle_id": vehicle_id,
                    "route": data.get("route"),
                    "trip_id": data.get("trip_id"),
                    "lat": float(data.get("lat")),
                    "lon": float(data.get("lon")),
                    "timestamp": int(data.get("timestamp", int(time.time()))),
                }

                with self.lock:
                    self.vehicle_positions[vehicle_id] = record
                    self.last_message_time = time.time()

            message.ack()
        except Exception as e:
            print(f"Error processing Pub/Sub message: {e}")
            message.nack()

    def prune_old_vehicles(self):
        cutoff = int(time.time()) - MAX_VEHICLE_AGE_SECONDS
        with self.lock:
            stale_ids = [vid for vid, item in self.vehicle_positions.items() if item["timestamp"] < cutoff]
            for vid in stale_ids:
                self.vehicle_positions.pop(vid, None)

            if time.time() - self.last_message_time > MAX_VEHICLE_AGE_SECONDS:
                self.vehicle_positions.clear()

            return list(self.vehicle_positions.values())

    def find_nearest_station(self, lat, lon):
        best_station = None
        best_km = 999999
        for stn in self.stations:
            km = haversine_km(lat, lon, stn["lat"], stn["lon"])
            if km < best_km:
                best_km = km
                best_station = stn
        return best_station, best_km

    def build_station_traffic(self, vehicles):
        traffic_counts = {}
        for v in vehicles:
            station, distance_km = self.find_nearest_station(v["lat"], v["lon"])
            if station is None:
                continue

            # Ignore vehicles too far from the nearest station (noise filter)
            if distance_km > 3.0:
                continue

            key = station["name"]
            traffic_counts[key] = traffic_counts.get(key, 0) + 1
        return traffic_counts

    def build_snapshot(self):
        now_local = dt.datetime.now()
        day_type = get_day_type(now_local)
        month_value = now_local.month

        vehicles = self.prune_old_vehicles()
        station_traffic = self.build_station_traffic(vehicles)

        origins = list(self.station_lookup.keys())
        predictions = self.predictor.predict_current_ridership(origins, day_type, month_value)

        stations_payload = []
        for origin_name, station in self.station_lookup.items():
            live_count = station_traffic.get(station["name"], 0)
            predicted = predictions.get(origin_name, 0.0)

            stations_payload.append(
                {
                    "origin": origin_name,
                    "station_name": station["name"],
                    "station_abbr": station["abbr"],
                    "lat": station["lat"],
                    "lon": station["lon"],
                    "live_traffic_count": int(live_count),
                    "predicted_ridership": round(float(predicted), 2),
                    "footfall_score": round(float(predicted) * (1 + (0.03 * live_count)), 2),
                }
            )

        stations_payload.sort(
            key=lambda x: (x["footfall_score"], x["live_traffic_count"], x["predicted_ridership"]),
            reverse=True,
        )

        snapshot = {
            "generated_at_utc": dt.datetime.utcnow().isoformat() + "Z",
            "generated_at_local": now_local.isoformat(),
            "project_id": PROJECT_ID,
            "subscription_id": SUBSCRIPTION_ID,
            "day_type": day_type,
            "month": month_value,
            "vehicle_count": len(vehicles),
            "station_count": len(stations_payload),
            "total_live_station_traffic": int(sum(s["live_traffic_count"] for s in stations_payload)),
            "stations": stations_payload,
            "vehicles": vehicles,
        }
        return snapshot

    def write_snapshot(self, snapshot):
        temp_file = STATE_FILE.with_suffix(".tmp")
        temp_file.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        temp_file.replace(STATE_FILE)

    def publish_snapshot(self, snapshot):
        try:
            message_bytes = json.dumps(snapshot).encode("utf-8")
            self.publisher.publish(self.dashboard_topic_path, data=message_bytes)
        except Exception as e:
            print(f"Could not publish dashboard snapshot: {e}")

    def run(self):
        print(f"Listening for messages on {self.subscription_path}...")
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)

        try:
            while True:
                snapshot = self.build_snapshot()
                self.write_snapshot(snapshot)
                self.publish_snapshot(snapshot)

                print(
                    f"[{time.strftime('%X')}] Snapshot updated | "
                    f"Vehicles: {snapshot['vehicle_count']} | "
                    f"Stations: {snapshot['station_count']} | "
                    f"State File: {STATE_FILE}"
                )
                time.sleep(SNAPSHOT_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\nSubscriber stopped by user.")
            streaming_pull_future.cancel()
            self.subscriber.close()


# --- Execution ---
if __name__ == "__main__":
    processor = RealTimeDashboardProcessor()
    processor.run()
