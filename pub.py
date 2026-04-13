import os
import time
import json
import requests
from google.cloud import pubsub_v1
from google.transit import gtfs_realtime_pb2

# 1. Setup Authentication & Pub/Sub Client
PROJECT_ID = os.getenv("PROJECT_ID", "bigdataproject-493002")
TOPIC_ID = os.getenv("TOPIC_ID", "transit-realtime-data")
API_KEY = os.getenv("TRANSIT_API_KEY", "1c6a0e84-c0a5-44f1-aa5d-a092294a48d4")
AGENCY = os.getenv("TRANSIT_AGENCY", "RG")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))

# Optional: set this in your environment if you do not already have auth configured.
# Example (PowerShell): $env:GOOGLE_APPLICATION_CREDENTIALS="C:\\path\\to\\admin-key.json"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


# 2. Stream real-time transit data to Pub/Sub
def stream_to_pubsub(api_key, interval=POLL_INTERVAL_SECONDS):
    url = f"https://api.511.org/Transit/VehiclePositions?api_key={api_key}&agency={AGENCY}"
    print(f"Starting real-time stream to Pub/Sub topic {topic_path} (polling every {interval}s)...")

    if GOOGLE_APPLICATION_CREDENTIALS:
        print(f"Using GOOGLE_APPLICATION_CREDENTIALS={GOOGLE_APPLICATION_CREDENTIALS}")

    try:
        while True:
            try:
                response = requests.get(url, timeout=HTTP_TIMEOUT_SECONDS)
            except requests.RequestException as e:
                print(f"[{time.strftime('%X')}] API request failed: {e}")
                time.sleep(interval)
                continue

            if response.status_code == 200:
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(response.content)

                publish_count = 0

                for entity in feed.entity:
                    if entity.HasField("vehicle"):
                        v = entity.vehicle

                        # 3. Package the data into a dictionary
                        vehicle_data = {
                            "vehicle_id": v.vehicle.id,
                            "route": v.trip.route_id,
                            "trip_id": v.trip.trip_id,
                            "lat": v.position.latitude,
                            "lon": v.position.longitude,
                            "timestamp": int(v.timestamp) if v.timestamp else int(time.time()),
                        }

                        # 4. Convert to JSON, then encode to bytes
                        message_json = json.dumps(vehicle_data)
                        message_bytes = message_json.encode("utf-8")

                        # 5. Publish to Google Cloud Pub/Sub
                        publisher.publish(topic_path, data=message_bytes)
                        publish_count += 1

                print(f"[{time.strftime('%X')}] Successfully published {publish_count} vehicle updates to Pub/Sub.")
            else:
                print(f"[{time.strftime('%X')}] Failed to fetch data: {response.status_code} {response.text[:200]}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStreaming stopped by user.")


# --- Execution ---
if __name__ == "__main__":
    stream_to_pubsub(API_KEY, POLL_INTERVAL_SECONDS)
