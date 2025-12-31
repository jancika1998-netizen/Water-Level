import os
import json
import requests
import gspread
import threading
import time
import csv
import io
from datetime import datetime
from flask import Flask, render_template, jsonify, make_response
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from collections import defaultdict

# Load environment variables from .env
load_dotenv()

app = Flask(__name__)

# --- CONFIGURATION ---
ARCGIS_URL = "https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/gauges_2_view/FeatureServer/0/query"
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
UPDATE_INTERVAL = 1200  # 20 Minutes in seconds

def get_gspread_client():
    """Authenticates with Google Sheets API using the Service Account JSON."""
    creds_json = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    return gspread.authorize(creds)

def get_all_river_data():
    """
    Fetches ALL available data from the ArcGIS server using pagination.
    Returns a dictionary grouped by Station Name containing all historical records.
    """
    all_features = []
    offset = 0
    record_count = 2000 # Max usually allowed by ArcGIS per request

    print(f"[{datetime.now()}] Fetching data from ArcGIS...")
    
    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "json",
            "orderByFields": "EditDate ASC", # Get oldest first to build history
            "resultRecordCount": record_count,
            "resultOffset": offset
        }
        
        try:
            r = requests.get(ARCGIS_URL, params=params, timeout=30)
            data = r.json()
            features = data.get("features", [])
            
            if not features:
                break
                
            all_features.extend(features)
            
            # If we got fewer records than the limit, we've reached the end
            if len(features) < record_count:
                break
                
            offset += record_count
            print(f"... fetched {len(all_features)} records so far...")
            
        except Exception as e:
            print(f"Error fetching ArcGIS data: {e}")
            break

    # Process and Group Data by Station Name
    grouped_data = defaultdict(list)
    
    for item in all_features:
        attr = item['attributes']
        geom = item['geometry']
        raw_name = attr.get("gauge")

        if raw_name:
            # Clean name for Google Sheet tab titles
            clean_name = raw_name.strip().replace("/", "_").replace(":", "-")
            edit_date = attr.get("EditDate")
            
            # Formatted time string
            time_str = datetime.fromtimestamp(edit_date / 1000).strftime('%Y-%m-%d %H:%M:%S') if edit_date else "N/A"

            # Determine Status
            level = attr.get("water_level") or 0
            alert_lvl = attr.get("alertpull") or 0
            minor_lvl = attr.get("minorpull") or 0
            major_lvl = attr.get("majorpull") or 0
            
            status = "Normal"
            if level >= major_lvl and major_lvl > 0: status = "MAJOR FLOOD"
            elif level >= minor_lvl and minor_lvl > 0: status = "MINOR FLOOD"
            elif level >= alert_lvl and alert_lvl > 0: status = "ALERT"

            record = {
                "name": clean_name,
                "basin": attr.get("basin"),
                "lat": geom.get("y"),
                "lon": geom.get("x"),
                "level": level,
                "status": status,
                "time": time_str,
                "timestamp_raw": edit_date # keep raw for sorting if needed
            }
            grouped_data[clean_name].append(record)

    return grouped_data

def background_sheet_sync():
    """Background process to update Google Sheets every 20 minutes."""
    while True:
        try:
            print(f"[{datetime.now()}] Starting sync cycle...")
            
            # 1. Fetch ALL Data (History included)
            grouped_data = get_all_river_data()
            
            if not grouped_data:
                print("No data fetched. Skipping this cycle.")
            else:
                client = get_gspread_client()
                spreadsheet = client.open_by_key(SHEET_ID)

                # --- PART A: Update Master Locations (Using only the latest entry per station) ---
                try:
                    master = spreadsheet.worksheet("Master_Locations")
                except gspread.WorksheetNotFound:
                    master = spreadsheet.add_worksheet(title="Master_Locations", rows="200", cols="4")
                    master.append_row(["Gauge", "Basin", "Lat", "Lon"])

                master_rows = [["Gauge", "Basin", "Lat", "Lon"]]
                
                # Iterate over stations to get metadata
                for station_name, records in grouped_data.items():
                    # records are sorted Oldest -> Newest (from API query), so last is latest
                    latest = records[-1] 
                    master_rows.append([latest['name'], latest['basin'], latest['lat'], latest['lon']])

                master.clear()
                master.update('A1', master_rows)
                print("Master location sheet updated.")

                # --- PART B: Update Individual Station Sheets (Backfill History) ---
                for station_name, records in grouped_data.items():
                    s_title = station_name[:30] # Sheet titles max 31 chars
                    
                    try:
                        ws = spreadsheet.worksheet(s_title)
                    except gspread.WorksheetNotFound:
                        ws = spreadsheet.add_worksheet(title=s_title, rows="2000", cols="3")
                        ws.append_row(["DateTime", "Level (m)", "Status"])
                    
                    # 1. Get existing timestamps to prevent duplicates
                    # Pulling column 1 (dates). efficient check.
                    existing_dates = set(ws.col_values(1)) 
                    
                    rows_to_add = []
                    
                    for record in records:
                        if record['time'] not in existing_dates:
                            rows_to_add.append([record['time'], record['level'], record['status']])
                    
                    # 2. Batch Append (Faster than one by one)
                    if rows_to_add:
                        ws.append_rows(rows_to_add)
                        print(f"Updated {station_name}: Added {len(rows_to_add)} new records.")
                    else:
                        # print(f"No new data for {station_name}")
                        pass

                print(f"[{datetime.now()}] Sync cycle completed successfully.")

        except Exception as e:
            print(f"Critical background sync error: {e}")

        # Wait for 20 minutes
        print(f"Sleeping for {UPDATE_INTERVAL} seconds...")
        time.sleep(UPDATE_INTERVAL)

# Start the background synchronization thread
sync_thread = threading.Thread(target=background_sheet_sync, daemon=True)
sync_thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def data_api():
    # Helper to return just the latest status for the map
    data = get_all_river_data()
    latest_only = []
    for name, records in data.items():
        latest_only.append(records[-1])
    return jsonify(latest_only)

@app.route('/api/history/<station_name>')
def history_api(station_name):
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(SHEET_ID)
        s_title = station_name[:30]
        ws = spreadsheet.worksheet(s_title)

        rows = ws.get_all_values()

        if len(rows) < 2:
            return jsonify([]) 

        history = []
        for row in rows[1:]: # Skip header
            if len(row) >= 2:
                try:
                    level = float(row[1])
                except ValueError:
                    level = 0
                history.append({
                    "time": row[0],
                    "level": level,
                    "status": row[2] if len(row) > 2 else ""
                })

        return jsonify(history)
    except Exception as e:
        print(f"Error fetching history for {station_name}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/download/<station_name>')
def download_data(station_name):
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(SHEET_ID)
        s_title = station_name[:30]
        ws = spreadsheet.worksheet(s_title)

        rows = ws.get_all_values()

        si = io.StringIO()
        cw = csv.writer(si)
        cw.writerows(rows)
        output = si.getvalue()

        response = make_response(output)
        response.headers["Content-Disposition"] = f"attachment; filename={station_name}.csv"
        response.headers["Content-type"] = "text/csv"
        return response
    except Exception as e:
        print(f"Error downloading data for {station_name}: {e}")
        return f"Error: {e}", 500

if __name__ == '__main__':
    # Local testing: use_reloader=False is mandatory when using threading
    app.run(debug=True, port=5000, use_reloader=False)
