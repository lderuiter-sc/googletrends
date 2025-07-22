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

# Countries for time series analysis
TRACKED_COUNTRIES = ['France', 'Belgium', 'Malaysia', 'United Arab Emirates', 'Germany']

app = Flask(__name__)

# Simple cache variables
cached_data = None
cache_timestamp = None
cache_duration_hours = 6

# Time series cache
cached_timeseries = None
timeseries_cache_timestamp = None

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
            
def fetch_timeseries_data():
    """Fetch time series data for specific countries"""
    try:
        print("Fetching time series data...")
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
        
        # We'll get data for all keywords combined by fetching each country individually
        country_data = {}
        
        # Map display names to Google Trends geo codes
        country_geo_map = {
            'France': 'FR',
            'Belgium': 'BE', 
            'Malaysia': 'MY',
            'United Arab Emirates': 'AE',
            'Germany': 'DE'
        }
        
        for country in TRACKED_COUNTRIES:
            print(f"Processing time series for {country}...")
            geo_code = country_geo_map.get(country, '')
            
            if not geo_code:
                print(f"No geo code found for {country}")
                continue
                
            try:
                # Get time series data for this country (last 3 months for better granularity)
                pytrends.build_payload(KEYWORDS, timeframe='today 3-m', geo=geo_code)
                time_data = pytrends.interest_over_time()
                
                if not time_data.empty and len(time_data) > 0:
                    # Sum all keywords for each time point
                    time_data['total'] = time_data[KEYWORDS].sum(axis=1)
                    
                    # Convert to list of values (excluding 'isPartial' column)
                    country_data[country] = time_data['total'].tolist()
                    print(f"Added {len(time_data)} data points for {country}")
                else:
                    print(f"No time series data for {country}")
                
                # Add delay between countries
                time.sleep(random.uniform(3, 5))
                
            except Exception as e:
                print(f"Error fetching time series for {country}: {e}")
                if "429" in str(e):
                    print("Rate limited - waiting 30 seconds...")
                    time.sleep(30)
                continue
        
        if country_data:
            # Get dates from the last successful request
            pytrends.build_payload(KEYWORDS, timeframe='today 3-m', geo='FR')
            time_data = pytrends.interest_over_time()
            if not time_data.empty:
                dates = [date.strftime('%Y-%m-%d') for date in time_data.index]
                
                # Format for line chart
                result = {
                    "dates": dates,
                    "series": [
                        {
                            "name": country,
                            "data": values
                        }
                        for country, values in country_data.items()
                    ]
                }
                
                print(f"Successfully created time series with {len(dates)} dates and {len(country_data)} countries")
                return result
        
        print("No time series data collected")
        return None
        
    except Exception as e:
        print(f"Error fetching time series data: {e}")
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

@app.route("/timeseries")
def serve_timeseries():
    """Serve time series data for line plots"""
    global cached_timeseries, timeseries_cache_timestamp
    
    now = datetime.now()
    
    # Check if time series cache is valid (cache for 12 hours since it's more expensive to fetch)
    cache_valid = (cached_timeseries is not None and 
                   timeseries_cache_timestamp is not None and 
                   now - timeseries_cache_timestamp < timedelta(hours=12))
    
    if cache_valid:
        print("Serving cached time series data")
        response = jsonify(cached_timeseries)
        response.headers['Content-Type'] = 'application/json'
        return response
    
    # Cache expired or doesn't exist - try to fetch fresh data
    print("Time series cache expired - fetching fresh data...")
    fresh_data = fetch_timeseries_data()
    
    if fresh_data:
        # Successfully got fresh data
        cached_timeseries = fresh_data
        timeseries_cache_timestamp = now
        print("Serving fresh time series data")
        response = jsonify(cached_timeseries)
    else:
        # Failed to get fresh data - return fallback
        fallback_timeseries = get_fallback_timeseries()
        print("Failed to get fresh time series data - serving fallback")
        response = jsonify(fallback_timeseries)
    
    response.headers['Content-Type'] = 'application/json'
    return response

def get_fallback_timeseries():
    """Return fallback time series data"""
    # Generate sample dates for last 3 months (weekly data points)
    from datetime import timedelta
    dates = []
    now = datetime.now()
    for i in range(12):  # 12 weeks
        date = now - timedelta(weeks=i)
        dates.append(date.strftime('%Y-%m-%d'))
    dates.reverse()  # Oldest first
    
    # Sample data showing realistic trends
    return {
        "dates": dates,
        "series": [
            {"name": "Belgium", "data": [45, 48, 52, 47, 55, 58, 62, 59, 65, 68, 71, 75]},
            {"name": "France", "data": [35, 38, 42, 39, 45, 48, 52, 49, 55, 58, 61, 65]},
            {"name": "Germany", "data": [40, 43, 47, 44, 50, 53, 57, 54, 60, 63, 66, 70]},
            {"name": "Malaysia", "data": [15, 18, 22, 19, 25, 28, 32, 29, 35, 38, 41, 45]},
            {"name": "United Arab Emirates", "data": [20, 23, 27, 24, 30, 33, 37, 34, 40, 43, 46, 50]}
        ]
    }

@app.route("/health")
def health_check():
    """Health check endpoint"""
    return "OK", 200

@app.route("/status")
def status():
    """Status endpoint"""
    global cached_data, cache_timestamp, cached_timeseries, timeseries_cache_timestamp
    
    return jsonify({
        # Leaderboard cache info
        "cached_data_exists": cached_data is not None,
        "cache_timestamp": cache_timestamp.isoformat() if cache_timestamp else None,
        "hours_since_update": (datetime.now() - cache_timestamp).total_seconds() / 3600 if cache_timestamp else None,
        "cache_duration_hours": cache_duration_hours,
        
        # Time series cache info
        "timeseries_cached": cached_timeseries is not None,
        "timeseries_cache_timestamp": timeseries_cache_timestamp.isoformat() if timeseries_cache_timestamp else None,
        "timeseries_hours_since_update": (datetime.now() - timeseries_cache_timestamp).total_seconds() / 3600 if timeseries_cache_timestamp else None,
        
        # Tracked countries
        "tracked_countries": TRACKED_COUNTRIES
    })

@app.route("/refresh")
def refresh_data():
    """Force refresh leaderboard data"""
    global cached_data, cache_timestamp
    
    print("Force refresh requested")
    fresh_data = fetch_fresh_data()
    
    if fresh_data:
        cached_data = fresh_data
        cache_timestamp = datetime.now()
        return jsonify({"message": "Data refreshed successfully", "items": len(cached_data["items"])})
    else:
        return jsonify({"message": "Failed to refresh data", "error": "Could not fetch from Google Trends"}), 500

@app.route("/refresh-timeseries")
def refresh_timeseries():
    """Force refresh time series data"""
    global cached_timeseries, timeseries_cache_timestamp
    
    print("Force time series refresh requested")
    fresh_data = fetch_timeseries_data()
    
    if fresh_data:
        cached_timeseries = fresh_data
        timeseries_cache_timestamp = datetime.now()
        return jsonify({"message": "Time series refreshed successfully", "countries": len(fresh_data["series"])})
    else:
        return jsonify({"message": "Failed to refresh time series", "error": "Could not fetch from Google Trends"}), 500

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)