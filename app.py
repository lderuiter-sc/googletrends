# app.py - Simplified caching approach
from pytrends.request import TrendReq
import pandas as pd
from flask import Flask, jsonify
import time
import random
import os
from datetime import datetime, timedelta

# Set your keywords
KEYWORDS = ['e-invoicing', 'PEPPOL']  # Reduced to 2 keywords for reliability

app = Flask(__name__)

# Simple cache variables
cached_data = None
cache_timestamp = None
cache_duration_hours = 6

def fetch_fresh_data():
    """Fetch fresh data from Google Trends"""
    try:
        print("Fetching fresh Google Trends data...")
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
        all_data = []

        for i, keyword in enumerate(KEYWORDS):
            print(f"Processing keyword {i+1}: {keyword}")
            
            # Add delay between requests
            if i > 0:
                delay = random.uniform(5, 8)
                print(f"Waiting {delay:.1f} seconds...")
                time.sleep(delay)
            
            try:
                pytrends.build_payload([keyword], timeframe='today 1-m')
                region_data = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True)
                
                if not region_data.empty:
                    region_data = region_data.reset_index()
                    region_data = region_data[['geoName', keyword]]
                    region_data.columns = ['country', 'interest']
                    all_data.append(region_data)
                    print(f"Added {len(region_data)} rows for {keyword}")
                    
            except Exception as e:
                print(f"Error with keyword {keyword}: {e}")
                if "429" in str(e):
                    print("Rate limited - waiting 30 seconds...")
                    time.sleep(30)
                continue

        if all_data:
            # Process data
            df = pd.concat(all_data)
            df_grouped = df.groupby('country').agg({'interest': 'sum'}).reset_index()
            df_grouped = df_grouped[df_grouped['interest'] > 0].sort_values('interest', ascending=False)

            # Filter out small countries/territories that cause misleading results
            EXCLUDED_COUNTRIES = [
                # Very small island nations and territories (population < 200k)
                'Palau', 'Grenada', 'St. Pierre & Miquelon', 'Mayotte', 'St. Helena', 
                'Wallis & Futuna', 'Faroe Islands', 'San Marino', 'Liechtenstein',
                'Andorra', 'Monaco', 'Nauru', 'Tuvalu', 'Vatican City', 'Marshall Islands',
                'Micronesia', 'Kiribati', 'Samoa', 'Tonga', 'Dominica', 'St. Lucia',
                'Barbados', 'St. Vincent & Grenadines', 'Antigua & Barbuda', 'Seychelles',
                'Maldives', 
                
                # Territories and dependencies (not sovereign business markets)
                'Guam', 'American Samoa', 'Northern Mariana Islands', 'U.S. Virgin Islands',
                'British Virgin Islands', 'Cayman Islands', 'Turks & Caicos Islands',
                'Bermuda', 'Anguilla', 'Montserrat', 'Falkland Islands', 'Greenland',
                'French Polynesia', 'New Caledonia', 'French Guiana', 'Martinique',
                'Guadeloupe', 'Réunion', 'Gibraltar', 'Isle of Man', 'Jersey', 'Guernsey',
                
                # Very small nations unlikely to have significant B2B e-invoicing activity
                'Djibouti', 'Comoros', 'São Tomé & Príncipe', 'Guinea-Bissau'
            ]
            
            # Filter out excluded countries
            df_filtered = df_grouped[~df_grouped['country'].isin(EXCLUDED_COUNTRIES)]
            print(f"Filtered out {len(df_grouped) - len(df_filtered)} small countries/territories")

            # Return processed data (using filtered dataframe)
            result = {
                "items": [
                    {"label": row['country'], "value": int(row['interest'])}
                    for _, row in df_filtered.head(10).iterrows()
                ]
            }
            print(f"Successfully processed {len(result['items'])} items")
            return result
        else:
            print("No data collected from Google Trends")
            return None
            
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def get_fallback_data():
    """Return fallback data when fresh data isn't available"""
    return {
        "items": [
            {"label": "Belgium", "value": 202},
            {"label": "Luxembourg", "value": 150},
            {"label": "Netherlands", "value": 93},
            {"label": "Germany", "value": 85},
            {"label": "France", "value": 72},
            {"label": "Sweden", "value": 65},
            {"label": "Norway", "value": 58},
            {"label": "Denmark", "value": 45},
            {"label": "Finland", "value": 42},
            {"label": "Austria", "value": 38}
        ]
    }

@app.route("/")
def serve_data():
    """Serve data with simple caching logic"""
    global cached_data, cache_timestamp
    
    now = datetime.now()
    
    # Check if cache is valid
    cache_valid = (cached_data is not None and 
                   cache_timestamp is not None and 
                   now - cache_timestamp < timedelta(hours=cache_duration_hours))
    
    if cache_valid:
        print("Serving cached data")
        response = jsonify(cached_data)
        response.headers['Content-Type'] = 'application/json'
        return response
    
    # Cache expired or doesn't exist - try to fetch fresh data
    print("Cache expired or empty - fetching fresh data...")
    fresh_data = fetch_fresh_data()
    
    if fresh_data:
        # Successfully got fresh data
        cached_data = fresh_data
        cache_timestamp = now
        print("Serving fresh data")
        response = jsonify(cached_data)
    else:
        # Failed to get fresh data - use fallback
        print("Failed to get fresh data - serving fallback")
        response = jsonify(get_fallback_data())
    
    response.headers['Content-Type'] = 'application/json'
    return response

@app.route("/health")
def health_check():
    """Health check endpoint"""
    return "OK", 200

@app.route("/status")
def status():
    """Status endpoint"""
    global cached_data, cache_timestamp
    
    return jsonify({
        "cached_data_exists": cached_data is not None,
        "cache_timestamp": cache_timestamp.isoformat() if cache_timestamp else None,
        "hours_since_update": (datetime.now() - cache_timestamp).total_seconds() / 3600 if cache_timestamp else None,
        "cache_duration_hours": cache_duration_hours
    })

@app.route("/refresh")
def refresh_data():
    """Force refresh data"""
    global cached_data, cache_timestamp
    
    print("Force refresh requested")
    fresh_data = fetch_fresh_data()
    
    if fresh_data:
        cached_data = fresh_data
        cache_timestamp = datetime.now()
        return jsonify({"message": "Data refreshed successfully", "items": len(cached_data["items"])})
    else:
        return jsonify({"message": "Failed to refresh data", "error": "Could not fetch from Google Trends"}), 500

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)