from fastapi import FastAPI
import requests
import json
import threading
import time
import os
from datetime import datetime, timezone
import uvicorn

app = FastAPI()

# Configuration
API_KEY = 'b9a99cfbc2971adba7d9b72b3264a66d'  # Use env variable in App Runner
API_URL = "http://api.aviationstack.com/v1/flights"
OFFSET_FILE = "offset.json"
LIMIT = 5
FETCH_INTERVAL = 10  # seconds between each batch fetch

# In-memory cache for flights
latest_flights = []

# Load offset from JSON file
def load_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, "r") as f:
            data = json.load(f)
            saved_date = data.get("date")
            saved_offset = data.get("offset", 0)

            # Reset offset if a new day
            if saved_date != datetime.now(timezone.utc).strftime("%Y-%m-%d"):
                return 0
            return saved_offset
    return 0

# Save offset to JSON file
def save_offset(offset):
    with open(OFFSET_FILE, "w") as f:
        json.dump({
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "offset": offset
        }, f)

# Fetch flights from API
def fetch_flights(offset):
    params = {
        "access_key": API_KEY,
        "limit": LIMIT,
        "offset": offset
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("data", [])

# Background thread to fetch flights continuously
def background_fetch():
    global latest_flights
    offset = load_offset()

    while True:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            flights = fetch_flights(offset)
            print(flights)
            if not flights:
                # No more flights today, wait until next day
                print("No more flights available for today. Sleeping until next day.")
                while datetime.now(timezone.utc).strftime("%Y-%m-%d") == today:
                    time.sleep(60)  # Check once a minute for a new day
                offset = 0
                latest_flights = []
                continue

            # Add new flights to cache
            latest_flights.extend(flights)
            print(f"Fetched {len(flights)} flights. Offset: {offset}")

            # Increment offset for next batch
            offset += LIMIT
            save_offset(offset)

            time.sleep(FETCH_INTERVAL)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching flight data: {e}")
            time.sleep(FETCH_INTERVAL)

# Start background thread when FastAPI starts
@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=background_fetch, daemon=True)
    thread.start()

# Endpoint to get latest flights
@app.get("/flights")
def get_flights():
    return latest_flights

# Run locally
if __name__ == "__main__":
   
    uvicorn.run(app, host="0.0.0.0", port=8080)
