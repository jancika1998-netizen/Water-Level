import os
import json
import requests
import gspread
import threading
import time
from datetime import datetime
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

# Load environment variables from .env
load_dotenv()

app = Flask(__name__)

# --- CONFIGURATION ---
ARCGIS_URL = "https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/gauges_2_view/FeatureServer/0/query"
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

def get_gspread_client():
    """Authenticates with Google Sheets API using the Service Account JSON."""
    creds_json = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    return gspread.authorize(creds)

def get_river_data():
    """Fetches the latest snapshot of all river gauges from the ArcGIS server."""
    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "orderByFields": "EditDate DESC",
        "resultRecordCount": 200
    }
    try:
        r = requests.get(ARCGIS_URL, params=params, timeout=15)
        data = r.json().get("features", [])
        
        latest_stations = {}
        for item in data:
            attr = item['attributes']
            geom = item['geometry']
            raw_name = attr.get("gauge")
            
            if raw_name and raw_name not in latest_stations:
                # Clean name for Google Sheet tab titles
                clean_name = raw_name.strip().replace("/", "_").replace(":", "-")
                edit_date = attr.get("EditDate")
                
                latest_stations[raw_name] = {
                    "name": clean_name,
                    "basin": attr.get("basin"),
                    "lat": geom.get("y"),
                    "lon": geom.get("x"),
                    "level": attr.get("water_level"),
                    "alert": attr.get("alertpull") or 0,
                    "minor": attr.get("minorpull") or 0,
                    "major": attr.get("majorpull") or 0,
                    "time": datetime.fromtimestamp(edit_date / 1000).strftime('%Y-%m-%d %H:%M:%S') if edit_date else "N/A"
                }
        return list(latest_stations.values())
    except Exception as e:
        print(f"Error fetching ArcGIS data: {e}")
        return []

def background_sheet_sync():
    """Background process to update Google Sheets every 10 minutes."""
    while True:
        try:
            print(f"[{datetime.now()}] Starting background sync...")
            data = get_river_data()
            if not data:
                print("No data fetched. Skipping this cycle.")
            else:
                client = get_gspread_client()
                spreadsheet = client.open_by_key(SHEET_ID)

                # 1. Update Master_Locations Sheet
                try:
                    master = spreadsheet.worksheet("Master_Locations")
                except gspread.WorksheetNotFound:
                    master = spreadsheet.add_worksheet(title="Master_Locations", rows="200", cols="4")
                    master.append_row(["Gauge", "Basin", "Lat", "Lon"])

                master_rows = [["Gauge", "Basin", "Lat", "Lon"]]
                for st in data:
                    master_rows.append([st['name'], st['basin'], st['lat'], st['lon']])
                
                master.clear()
                master.update('A1', master_rows)

                # 2. Update Individual Station Sheets
                for st in data:
                    s_title = st['name'][:30] # Sheet titles max 31 chars
                    try:
                        ws = spreadsheet.worksheet(s_title)
                    except gspread.WorksheetNotFound:
                        ws = spreadsheet.add_worksheet(title=s_title, rows="2000", cols="3")
                        ws.append_row(["DateTime", "Level (m)", "Status"])

                    # Check last recorded time to avoid duplicates
                    existing_data = ws.get_all_values()
                    last_recorded_time = existing_data[-1][0] if len(existing_data) > 1 else ""

                    if st['time'] != last_recorded_time:
                        status = "Normal"
                        if st['level'] >= st['major']: status = "MAJOR FLOOD"
                        elif st['level'] >= st['minor']: status = "MINOR FLOOD"
                        elif st['level'] >= st['alert']: status = "ALERT"
                        
                        ws.append_row([st['time'], st['level'], status])

                print(f"[{datetime.now()}] Sync completed successfully.")
        
        except Exception as e:
            print(f"Critical background sync error: {e}")
        
        # Wait for 10 minutes (600 seconds)
        time.sleep(600)

# Start the background synchronization thread
# use_reloader=False in app.run is important to prevent starting two threads
sync_thread = threading.Thread(target=background_sheet_sync, daemon=True)
sync_thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def data_api():
    return jsonify(get_river_data())

if __name__ == '__main__':
    # Local testing: use_reloader=False is mandatory when using threading
    app.run(debug=True, port=5000, use_reloader=False)