import os
import json
import requests
import gspread
import threading
import time
import csv
import io
from datetime import datetime, timedelta
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

# Global variable to track the last sync time
LAST_SYNC_TIMESTAMP = None 

def get_gspread_client():
    """Authenticates with Google Sheets API using the Service Account JSON."""
    creds_json = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    return gspread.authorize(creds)

def fetch_arcgis_data(full_history=False):
    """
    Fetches data from ArcGIS. 
    If full_history=True, it paginates through ALL records (Startup).
    If full_history=False, it fetches only records from the last 24 hours (Updates).
    """
    all_features = []
    offset = 0
    record_count = 1000  # Conservative limit to prevent server timeouts
    
    # Define query parameters
    params = {
        "outFields": "*",
        "f": "json",
        # Sort by Date AND ObjectId to ensure pagination doesn't skip rows with identical times
        "orderByFields": "EditDate ASC, OBJECTID ASC", 
        "resultRecordCount": record_count,
    }

    if full_history:
        print(f"[{datetime.now()}] 🟡 STARTING FULL HISTORY SYNC (This may take time)...")
        params["where"] = "1=1" # Get EVERYTHING
    else:
        print(f"[{datetime.now()}] 🟢 STARTING INCREMENTAL SYNC (Last 24 hours)...")
        # ArcGIS query for "EditDate > 24 hours ago"
        # We calculate the timestamp in milliseconds
        yesterday = datetime.now() - timedelta(hours=24)
        ts_milliseconds = int(yesterday.timestamp() * 1000)
        params["where"] = f"EditDate > {ts_milliseconds}"

    while True:
        params["resultOffset"] = offset
        try:
            r = requests.get(ARCGIS_URL, params=params, timeout=45)
            
            if r.status_code != 200:
                print(f"Server returned error: {r.status_code} {r.text}")
                break

            data = r.json()
            features = data.get("features", [])
            
            if not features:
                break
                
            all_features.extend(features)
            
            # If we received fewer records than we asked for, we are at the end
            if len(features) < record_count:
                break
            
            # Move offset for next page
            offset += record_count
            print(f"... retrieved {len(all_features)} records so far...")
            
        except Exception as e:
            print(f"Error fetching ArcGIS data: {e}")
            break

    print(f"[{datetime.now()}] Fetch complete. Total records: {len(all_features)}")
    
    # --- Process Data into Dictionary ---
    grouped_data = defaultdict(list)
    
    for item in all_features:
        attr = item['attributes']
        geom = item['geometry'] if 'geometry' in item else {}
        
        # Use 'gauge' as the identifier. 
        # CAUTION: If 'gauge' is None, we skip it.
        raw_name = attr.get("gauge")

        if raw_name:
            # Clean name for Google Sheet tab titles
            clean_name = raw_name.strip().replace("/", "_").replace(":", "-")
            edit_date = attr.get("EditDate")
            
            if edit_date:
                time_str = datetime.fromtimestamp(edit_date / 1000).strftime('%Y-%m-%d %H:%M:%S')
            else:
                time_str = "N/A"

            # Determine Status
            level = attr.get("water_level")
            # Handle cases where level might be None
            level = float(level) if level is not None else 0.0

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
                "lat": geom.get("y", 0),
                "lon": geom.get("x", 0),
                "level": level,
                "status": status,
                "time": time_str,
                "timestamp_raw": edit_date
            }
            grouped_data[clean_name].append(record)
            
    return grouped_data

def update_google_sheets(grouped_data):
    """Writes the grouped data to Google Sheets."""
    if not grouped_data:
        return

    client = get_gspread_client()
    spreadsheet = client.open_by_key(SHEET_ID)

    # 1. Update Master Locations (using the absolute latest data point for each gauge)
    try:
        master = spreadsheet.worksheet("Master_Locations")
    except gspread.WorksheetNotFound:
        master = spreadsheet.add_worksheet(title="Master_Locations", rows="200", cols="4")
        master.append_row(["Gauge", "Basin", "Lat", "Lon"])

    master_rows = [["Gauge", "Basin", "Lat", "Lon"]]
    
    # Sort keys to keep master sheet tidy
    for station_name in sorted(grouped_data.keys()):
        records = grouped_data[station_name]
        # Sort records by timestamp (newest last) to find latest metadata
        records.sort(key=lambda x: x['timestamp_raw'] if x['timestamp_raw'] else 0)
        latest = records[-1]
        master_rows.append([latest['name'], latest['basin'], latest['lat'], latest['lon']])

    master.clear()
    master.update('A1', master_rows)
    print("Master location sheet updated.")

    # 2. Update Individual Station Sheets
    for station_name, new_records in grouped_data.items():
        s_title = station_name[:30] # Sheet titles max 31 chars
        
        try:
            ws = spreadsheet.worksheet(s_title)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=s_title, rows="2000", cols="3")
            ws.append_row(["DateTime", "Level (m)", "Status"])
        
        # Get existing timestamps to prevent duplicates
        # We assume column 1 is DateTime
        try:
            existing_dates = set(ws.col_values(1))
        except:
            existing_dates = set()

        rows_to_add = []
        
        # Filter out records that already exist in the sheet
        for record in new_records:
            if record['time'] not in existing_dates:
                rows_to_add.append([record['time'], record['level'], record['status']])
        
        # Batch append
        if rows_to_add:
            # Sort rows by time before inserting just in case
            rows_to_add.sort(key=lambda x: x[0]) 
            ws.append_rows(rows_to_add)
            print(f"  -> {station_name}: Added {len(rows_to_add)} new records.")

def background_manager():
    """Manages the full sync vs incremental sync logic."""
    first_run = True

    while True:
        try:
            if first_run:
                # FIRST RUN: Get EVERYTHING available on server
                data = fetch_arcgis_data(full_history=True)
                update_google_sheets(data)
                first_run = False
                print(f"[{datetime.now()}] Initial FULL sync complete.")
            else:
                # SUBSEQUENT RUNS: Get only recent updates (last 24h)
                # This catches the hourly updates without re-downloading years of data
                data = fetch_arcgis_data(full_history=False)
                if data:
                    update_google_sheets(data)
                    print(f"[{datetime.now()}] Incremental sync complete.")
                else:
                    print(f"[{datetime.now()}] No new data found in incremental check.")

        except Exception as e:
            print(f"CRITICAL ERROR in background loop: {e}")

        # Wait for 20 minutes
        print(f"Sleeping for {UPDATE_INTERVAL} seconds...")
        time.sleep(UPDATE_INTERVAL)

# Start the background thread
sync_thread = threading.Thread(target=background_manager, daemon=True)
sync_thread.start()

# --- FLASK ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def data_api():
    # Helper to return just the latest status for the map
    # Note: For the API response, we do a quick fetch of recent data
    # or you could cache this global variable. For now, fetching recent is safe.
    data = fetch_arcgis_data(full_history=False)
    latest_only = []
    for name, records in data.items():
        if records:
            # Sort by time, get last
            records.sort(key=lambda x: x['timestamp_raw'] or 0)
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

        if len(rows) < 2: return jsonify([]) 

        history = []
        for row in rows[1:]: # Skip header
            if len(row) >= 2:
                try: level = float(row[1])
                except ValueError: level = 0
                history.append({
                    "time": row[0],
                    "level": level,
                    "status": row[2] if len(row) > 2 else ""
                })
        return jsonify(history)
    except Exception as e:
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
        return f"Error: {e}", 500

if __name__ == '__main__':
    # use_reloader=False is mandatory when using threading
    app.run(debug=True, port=5000, use_reloader=False)
