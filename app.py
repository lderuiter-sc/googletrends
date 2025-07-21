# app.py with caching for fast Geckoboard responses
from pytrends.request import TrendReq
import pandas as pd
from flask import Flask, jsonify
import time
import random
import os
from datetime import datetime, timedelta
import threading

# Set your keywords
KEYWORDS = ['e-invoicing', 'PEPPOL', 'peppol', 'E-invoicing']

app = Flask(__name__)

# Global cache variables
cached_data = None
cache_timestamp = None
cache_duration_hours = 6  # Refresh data every 6 hours
is_updating = False

def fetch_google_trends():
    """Fetch data from Google Trends (runs in background)"""
    global cached_data, cache_timestamp, is_updating
    
    try:
        is_updating = True
        print("Background: Starting Google Trends fetch...")
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
        all_data = []

        for i, keyword in enumerate(KEYWORDS):
            print(f"Background: Processing keyword {i+1}: {keyword}")
            
            if i > 0:
                delay = random.uniform(8, 12)
                print(f"Background: Waiting {delay:.1f} seconds...")
                time.sleep(delay)
            
            try:
                pytrends.build_payload([keyword], timeframe='today 1-m')
                region_data = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True)
                
                if not region_data.empty:
                    region_data = region_data.reset_index()
                    region_data = region_data[['geoName', keyword]]
                    region_data.columns = ['country', 'interest']
                    all_data.append(region_data)
                    print(f"Background: Added {len(region_data)} rows for {keyword}")
                    
            except Exception as e:
                print(f"Background: Error with keyword {keyword}: {e}")
                if "429" in str(e):
                    print("Background: Rate limited - waiting 30 seconds...")
                    time.sleep(30)
                continue

        if all_data:
            # Process data
            df = pd.concat(all_data)
            df_grouped = df.groupby('country').agg({'interest': 'sum'}).reset_index()
            df_grouped = df_grouped[df_grouped['interest'] > 0].sort_values('interest', ascending=False)

            # Update global cache
            cached_data = {
                "items": [
                    {"label": row['country'], "value": int(row['interest'])}
                    for _, row in df_grouped.head(10).iterrows()
                ]
            }
            cache_timestamp = datetime.now()
            print(f"Background: Successfully cached {len(cached_data['items'])} items")
        else:
            print("Background: No data collected")
            
    except Exception as e:
        print(f"Background: General error: {e}")
    finally:
        is_updating = False

def start_background_update():
    """Start background data fetch in a separate thread"""
    if not is_updating:
        thread = threading.Thread(target=fetch_google_trends)
        thread.daemon = True
        thread.start()

@app.route("/")
def serve_data():
    """Serve cached data instantly"""
    global cached_data, cache_timestamp
    
    # Check if we need to refresh data
    now = datetime.now()
    if (cached_data is None or 
        cache_timestamp is None or 
        now - cache_timestamp > timedelta(hours=cache_duration_hours)):
        
        # Start background update if not already running
        start_background_update()
        
        # If no cached data exists, return fallback
        if cached_data is None:
            return jsonify({
                "items": [
                    {"label": "Belgium", "value": 202},
                    {"label": "Mayotte", "value": 200},
                    {"label": "Luxembourg", "value": 93},
                    {"label": "Central African Republic", "value": 28},
                    {"label": "Sweden", "value": 18},
                    {"label": "Netherlands", "value": 14},
                    {"label": "St. Helena", "value": 8},
                    {"label": "Finland", "value": 8},
                    {"label": "Singapore", "value": 8},
                    {"label": "Malaysia", "value": 8}
                ]
            })
    
    # Return cached data instantly
    response = jsonify(cached_data)
    response.headers['Content-Type'] = 'application/json'
    return response

@app.route("/health")
def health_check():
    """Health check endpoint"""
    return "OK", 200

@app.route("/status")
def status():
    """Status endpoint to check cache info"""
    return jsonify({
        "cached_data_exists": cached_data is not None,
        "cache_timestamp": cache_timestamp.isoformat() if cache_timestamp else None,
        "is_updating": is_updating,
        "hours_since_update": (datetime.now() - cache_timestamp).total_seconds() / 3600 if cache_timestamp else None
    })

@app.route("/force-update")
def force_update():
    """Force a background data update"""
    start_background_update()
    return jsonify({"message": "Background update started"})

# Initialize cache on startup (Flask 3.x compatible)
def initialize_cache():
    """Initialize cache when app starts"""
    start_background_update()

# Call initialize_cache when the module loads
initialize_cache()

if __name__ == "__main__":
    # Get port from environment variable or use 8080 for local development
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)