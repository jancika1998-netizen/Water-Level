import os
import json
import requests
import gspread
import io
import csv
from flask import Flask, render_template, jsonify, make_response, request
from datetime import datetime, timedelta
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from collections import defaultdict

# Load environment variables
load_dotenv()

app = Flask(__name__)

# --- CONFIGURATION ---
ARCGIS_URL = "https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/gauges_2_view/FeatureServer/0/query"
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

def get_gspread_client():
    """Authenticates with Google Sheets API."""
    creds_json = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    return gspread.authorize(creds)

def fetch_arcgis_data(full_history=False):
    """
    Fetches data from ArcGIS.
    full_history=True -> Gets ALL records (for initial setup/reset).
    full_history=False -> Gets records from last 24 hours (for 20-min updates).
    """
    all_features = []
    offset = 0
    record_count = 1000  
    
    params = {
        "outFields": "*",
        "f": "json",
        "orderByFields": "EditDate ASC, OBJECTID ASC",
        "resultRecordCount": record_count,
    }

    if full_history:
        print(f"[{datetime.now()}] 🟡 STARTING FULL HISTORY SYNC...")
        params["where"] = "1=1"
    else:
        print(f"[{datetime.now()}] 🟢 STARTING INCREMENTAL SYNC (Last 24h)...")
        # Get data from last 24 hours
        yesterday = datetime.now() - timedelta(hours=24)
        ts_milliseconds = int(yesterday.timestamp() * 1000)
        params["where"] = f"EditDate > {ts_milliseconds}"

    while True:
        params["resultOffset"] = offset
        try:
            r = requests.get(ARCGIS_URL, params=params, timeout=45)
            if r.status_code != 200: break

            data = r.json()
            features = data.get("features", [])
            
            if not features: break
                
            all_features.extend(features)
            
            if len(features) < record_count: break
            
            offset += record_count
            print(f"... retrieved {len(all_features)} records...")
            
        except Exception as e:
            print(f"Error fetching ArcGIS data: {e}")
            break

    # Group Data
    grouped_data = defaultdict(list)
    for item in all_features:
        attr = item['attributes']
        raw_name = attr.get("gauge")

        if raw_name:
            clean_name = raw_name.strip().replace("/", "_").replace(":", "-")
            edit_date = attr.get("EditDate")
            time_str = datetime.fromtimestamp(edit_date / 1000).strftime('%Y-%m-%d %H:%M:%S') if edit_date else "N/A"

            level = float(attr.get("water_level") or 0)
            alert = attr.get("alertpull") or 0
            minor = attr.get("minorpull") or 0
            major = attr.get("majorpull") or 0
            
            status = "Normal"
            if level >= major and major > 0: status = "MAJOR FLOOD"
            elif level >= minor and minor > 0: status = "MINOR FLOOD"
            elif level >= alert and alert > 0: status = "ALERT"

            grouped_data[clean_name].append({
                "name": clean_name,
                "basin": attr.get("basin"),
                "lat": item.get('geometry', {}).get("y", 0),
                "lon": item.get('geometry', {}).get("x", 0),
                "level": level,
                "status": status,
                "time": time_str,
                "timestamp_raw": edit_date
            })
            
    return grouped_data

def update_google_sheets(grouped_data):
    """Writes data to Google Sheets."""
    if not grouped_data: return

    client = get_gspread_client()
    spreadsheet = client.open_by_key(SHEET_ID)

    # 1. Update Master Locations
    try:
        master = spreadsheet.worksheet("Master_Locations")
    except gspread.WorksheetNotFound:
        master = spreadsheet.add_worksheet(title="Master_Locations", rows="200", cols="4")
        master.append_row(["Gauge", "Basin", "Lat", "Lon"])

    master_rows = [["Gauge", "Basin", "Lat", "Lon"]]
    
    for station_name in sorted(grouped_data.keys()):
        records = grouped_data[station_name]
        records.sort(key=lambda x: x['timestamp_raw'] or 0)
        latest = records[-1]
        master_rows.append([latest['name'], latest['basin'], latest['lat'], latest['lon']])

    master.clear()
    master.update('A1', master_rows)

    # 2. Update Individual Sheets
    for station_name, new_records in grouped_data.items():
        s_title = station_name[:30]
        try:
            ws = spreadsheet.worksheet(s_title)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=s_title, rows="2000", cols="3")
            ws.append_row(["DateTime", "Level (m)", "Status"])
        
        try:
            existing_dates = set(ws.col_values(1))
        except:
            existing_dates = set()

        rows_to_add = []
        for record in new_records:
            if record['time'] not in existing_dates:
                rows_to_add.append([record['time'], record['level'], record['status']])
        
        if rows_to_add:
            rows_to_add.sort(key=lambda x: x[0]) 
            ws.append_rows(rows_to_add)

# --- ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/trigger-sync')
def trigger_sync():
    """
    CRON JOB TARGET:
    - Default: Updates only last 24h data (Fast).
    - Usage: Call this URL every 20 mins via cron-job.org.
    - Reset: Call /api/trigger-sync?full=true to force a full history download.
    """
    try:
        # Check if user wants full history (via URL parameter ?full=true)
        is_full_sync = request.args.get('full') == 'true'
        
        data = fetch_arcgis_data(full_history=is_full_sync)
        
        if data:
            update_google_sheets(data)
            return jsonify({
                "status": "success", 
                "message": "Sync completed", 
                "type": "Full History" if is_full_sync else "Incremental (24h)",
                "stations_updated": len(data)
            })
        else:
            return jsonify({"status": "success", "message": "No new data found"})
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/data')
def data_api():
    # Fetch recent data for map display (Fast)
    data = fetch_arcgis_data(full_history=False)
    latest_only = []
    for name, records in data.items():
        if records:
            records.sort(key=lambda x: x['timestamp_raw'] or 0)
            latest_only.append(records[-1])
    return jsonify(latest_only)

@app.route('/api/history/<station_name>')
def history_api(station_name):
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(SHEET_ID)
        ws = spreadsheet.worksheet(station_name[:30])
        rows = ws.get_all_values()
        
        if len(rows) < 2: return jsonify([])

        history = []
        for row in rows[1:]:
            history.append({
                "time": row[0],
                "level": float(row[1]) if row[1] else 0,
                "status": row[2] if len(row) > 2 else ""
            })
        return jsonify(history)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
