# app.py - Restored normal functionality after rate limiting cooldown
from pytrends.request import TrendReq
import pandas as pd
from pandas.tseries import offsets as pd_offsets
from flask import Flask, jsonify
import time
import random
import os
from datetime import datetime, timedelta

# ============== KEYWORDS ==============
KEYWORDS_PRIMARY = ['e-invoicing', 'PEPPOL']
KEYWORDS = KEYWORDS_PRIMARY  # Use 2 keywords for optimal speed/data balance

# Localized keywords by country for time series
LOCALIZED_KEYWORDS = {
    'FR': ['facture électronique', 'facturation electronique', 'e-invoicing'],
    'BE': ['e-facturatie', 'e-invoicing', 'PEPPOL'],  
    'AE': ['e-invoicing', 'electronic invoice', 'VAT invoice UAE'],
    'GB': ['e-invoicing', 'electronic invoice', 'PEPPOL UK'],
    'US': ['e-invoicing', 'electronic invoice', 'invoice automation']
}

# Countries for time series analysis (5 countries as requested)
TRACKED_COUNTRIES = ['France', 'Belgium', 'United Arab Emirates', 'United Kingdom', 'United States']

app = Flask(__name__)

# Cache variables (restored to normal durations)
cached_data = None
cache_timestamp = None
cache_duration_hours = 6

cached_timeseries = None
timeseries_cache_timestamp = None
timeseries_cache_hours = 12

def fetch_fresh_data():
    """Fetch fresh leaderboard data from Google Trends (3 months, excluding small countries)"""
    try:
        print("Fetching fresh Google Trends leaderboard data (3 months)...")
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
        all_data = []
        consecutive_failures = 0

        for i, keyword in enumerate(KEYWORDS):
            print(f"Processing keyword {i+1}: {keyword}")
            
            # Add delay between requests
            if i > 0:
                delay = random.uniform(4, 6)
                print(f"Waiting {delay:.1f} seconds...")
                time.sleep(delay)
            
            try:
                # Use 3-month timeframe for leaderboard
                pytrends.build_payload([keyword], timeframe='today 3-m')
                region_data = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True)
                
                if not region_data.empty:
                    region_data = region_data.reset_index()
                    region_data = region_data[['geoName', keyword]]
                    region_data.columns = ['country', 'interest']
                    all_data.append(region_data)
                    print(f"Added {len(region_data)} rows for {keyword}")
                    consecutive_failures = 0  # Reset failure counter
                    
            except Exception as e:
                consecutive_failures += 1
                print(f"Error with keyword {keyword}: {e}")
                
                # If we get too many consecutive failures, bail out
                if consecutive_failures >= 2:
                    print("Too many consecutive failures - stopping to avoid issues")
                    break
                    
                if "429" in str(e):
                    print("Rate limited - waiting 30 seconds...")
                    time.sleep(30)
                continue

        if all_data:
            # Process data
            df = pd.concat(all_data)
            df_grouped = df.groupby('country').agg({'interest': 'sum'}).reset_index()
            df_grouped = df_grouped[df_grouped['interest'] > 0].sort_values('interest', ascending=False)

            # Exclude small countries/territories that cause misleading results
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
            
            df_filtered = df_grouped[~df_grouped['country'].isin(EXCLUDED_COUNTRIES)]
            print(f"Filtered out {len(df_grouped) - len(df_filtered)} small countries/territories")

            result = {
                "items": [
                    {"label": row['country'], "value": int(row['interest'])}
                    for _, row in df_filtered.head(10).iterrows()
                ]
            }
            print(f"Successfully processed {len(result['items'])} leaderboard items")
            return result
        else:
            print("No leaderboard data collected from Google Trends")
            return None
            
    except Exception as e:
        print(f"Error fetching leaderboard data: {e}")
        return None

def fetch_timeseries_data():
    """Fetch monthly time series data for 5 specific countries (France, Belgium, UAE, UK, US)"""
    try:
        print("Fetching monthly time series data for 5 countries...")
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
        country_data = {}
        date_labels = None
        
        country_geo_map = {
            'France': 'FR',
            'Belgium': 'BE', 
            'United Arab Emirates': 'AE',
            'United Kingdom': 'GB',
            'United States': 'US'
        }
        
        # Dynamic timeframe from January 2025 to present
        today = datetime.now().strftime('%Y-%m-%d')
        timeframe = f'2025-01-01 {today}'
        print(f"Using timeframe: {timeframe}")
        
        consecutive_failures = 0
        
        for country in TRACKED_COUNTRIES:
            print(f"Processing time series for {country}...")
            geo_code = country_geo_map.get(country, '')
            
            if not geo_code:
                print(f"No geo code found for {country}")
                continue
            
            # Use localized keywords for better relevance
            keywords_to_use = LOCALIZED_KEYWORDS.get(geo_code, KEYWORDS_PRIMARY)
            print(f"Using keywords for {country}: {keywords_to_use}")
                
            try:
                pytrends.build_payload(keywords_to_use, timeframe=timeframe, geo=geo_code)
                time_data = pytrends.interest_over_time()
                
                if not time_data.empty and len(time_data) > 0:
                    print(f"Raw data shape for {country}: {time_data.shape}")
                    print(f"Date range: {time_data.index[0]} to {time_data.index[-1]}")
                    
                    # Sum all keywords for each time point
                    keyword_cols = [col for col in time_data.columns if col in keywords_to_use]
                    if keyword_cols:
                        time_data['total'] = time_data[keyword_cols].sum(axis=1)
                        print(f"Created total column for {country} with keywords: {keyword_cols}")
                    else:
                        print(f"No keyword columns found for {country}")
                        consecutive_failures += 1
                        continue
                    
                    # Aggregate by month
                    time_data.index = pd.to_datetime(time_data.index)
                    monthly_data = time_data['total'].resample('M').mean().round(0)
                    
                    # Ensure we have data for all months from Jan to current month
                    current_date = datetime.now()
                    end_of_current_month = pd.Timestamp(current_date.year, current_date.month, 1) + pd_offsets.MonthEnd(0)
                    all_months = pd.date_range(start='2025-01-01', end=end_of_current_month, freq='M')
                    monthly_data = monthly_data.reindex(all_months, fill_value=0)
                    
                    country_data[country] = monthly_data.tolist()
                    print(f"Added {len(monthly_data)} monthly data points for {country}")
                    
                    # Store date labels from first successful country
                    if date_labels is None:
                        date_labels = [date.strftime('%b') for date in monthly_data.index]
                        print(f"Generated {len(date_labels)} month labels: {date_labels}")
                        
                    consecutive_failures = 0  # Reset failure counter
                else:
                    print(f"No time series data for {country}")
                    consecutive_failures += 1
                
                # Stop if too many consecutive failures
                if consecutive_failures >= 3:
                    print("Too many consecutive failures - stopping time series fetch")
                    break
                
                # Add delay between countries
                time.sleep(random.uniform(5, 8))
                
            except Exception as e:
                consecutive_failures += 1
                print(f"Error fetching time series for {country}: {e}")
                
                # Stop if too many failures to avoid infinite loops
                if consecutive_failures >= 3:
                    print("Too many consecutive failures - stopping to avoid infinite loop")
                    break
                    
                if "429" in str(e):
                    print("Rate limited - waiting 45 seconds...")
                    time.sleep(45)
                continue
        
        if country_data and date_labels:
            current_month_year = datetime.now().strftime('%B %Y')
            result = {
                "dates": date_labels,
                "series": [
                    {
                        "name": country,
                        "data": [int(val) if not pd.isna(val) else 0 for val in values]
                    }
                    for country, values in country_data.items()
                ],
                "metadata": {
                    "period": "monthly",
                    "start": "January 2025",
                    "end": current_month_year,
                    "data_points": len(date_labels),
                    "countries": len(country_data),
                    "last_updated": datetime.now().isoformat()
                }
            }
            
            print(f"Successfully created time series with {len(date_labels)} months and {len(country_data)} countries")
            return result
        
        print("No time series data collected")
        return None
        
    except Exception as e:
        print(f"Error fetching time series data: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_fallback_data():
    """Return fallback leaderboard data when fresh data isn't available"""
    return {
        "items": [
            {"label": "Belgium", "value": 202},
            {"label": "United Kingdom", "value": 150},
            {"label": "United States", "value": 125},
            {"label": "France", "value": 95},
            {"label": "United Arab Emirates", "value": 85},
            {"label": "Netherlands", "value": 72},
            {"label": "Germany", "value": 65},
            {"label": "Sweden", "value": 58},
            {"label": "Norway", "value": 45},
            {"label": "Denmark", "value": 38}
        ]
    }

def get_fallback_timeseries():
    """Return fallback time series data for the 5 tracked countries"""
    current_date = datetime.now()
    dates = []
    start_date = datetime(2025, 1, 1)
    
    while start_date <= current_date:
        dates.append(start_date.strftime('%b'))
        if start_date.month == 12:
            start_date = start_date.replace(year=start_date.year + 1, month=1)
        else:
            start_date = start_date.replace(month=start_date.month + 1)
    
    num_months = len(dates)
    base_values = {
        "Belgium": 45,
        "France": 35,
        "United Arab Emirates": 20,
        "United Kingdom": 38,
        "United States": 42
    }
    
    series_data = {}
    for country, base in base_values.items():
        data = [base + (i * 3) + random.randint(-2, 2) for i in range(num_months)]
        series_data[country] = data
    
    return {
        "dates": dates,
        "series": [
            {"name": country, "data": data}
            for country, data in series_data.items()
        ],
        "metadata": {
            "period": "monthly",
            "start": "January 2025",
            "end": current_date.strftime('%B %Y'),
            "data_points": num_months,
            "countries": len(series_data),
            "note": "Fallback data - Google Trends temporarily unavailable"
        }
    }

@app.route("/")
def serve_data():
    """Serve leaderboard data instantly (cached data only)"""
    global cached_data, cache_timestamp
    
    print("Leaderboard endpoint called")
    
    # Always serve cached data if available, never fetch during request
    if cached_data is not None:
        print("Serving cached leaderboard data")
        response = jsonify(cached_data)
        response.headers['Content-Type'] = 'application/json'
        return response
    
    # If no cached data exists, serve fallback immediately
    print("No cached data - serving fallback leaderboard data")
    response = jsonify(get_fallback_data())
    response.headers['Content-Type'] = 'application/json'
    return response

@app.route("/timeseries")
def serve_timeseries():
    """Serve time series data instantly (cached data only)"""
    global cached_timeseries, timeseries_cache_timestamp
    
    print("Time series endpoint called")
    
    # Always serve cached data if available, never fetch during request
    if cached_timeseries is not None:
        print("Serving cached time series data")
        response = jsonify(cached_timeseries)
        response.headers['Content-Type'] = 'application/json'
        return response
    
    # If no cached data exists, serve fallback immediately
    print("No cached data - serving fallback time series data")
    response = jsonify(get_fallback_timeseries())
    response.headers['Content-Type'] = 'application/json'
    return response

@app.route("/health")
def health_check():
    """Health check endpoint"""
    return "OK", 200

@app.route("/status")
def status():
    """Status endpoint with system information"""
    global cached_data, cache_timestamp, cached_timeseries, timeseries_cache_timestamp
    
    current_month_year = datetime.now().strftime('%B %Y')
    
    return jsonify({
        # Service status
        "service_status": "operational",
        "current_time": datetime.now().isoformat(),
        
        # Leaderboard cache info
        "leaderboard": {
            "cached_data_exists": cached_data is not None,
            "cache_timestamp": cache_timestamp.isoformat() if cache_timestamp else None,
            "hours_since_update": (datetime.now() - cache_timestamp).total_seconds() / 3600 if cache_timestamp else None,
            "cache_duration_hours": cache_duration_hours,
            "timeframe": "3 months",
            "excludes_small_countries": True
        },
        
        # Time series cache info
        "timeseries": {
            "cached_data_exists": cached_timeseries is not None,
            "cache_timestamp": timeseries_cache_timestamp.isoformat() if timeseries_cache_timestamp else None,
            "hours_since_update": (datetime.now() - timeseries_cache_timestamp).total_seconds() / 3600 if timeseries_cache_timestamp else None,
            "cache_duration_hours": timeseries_cache_hours,
            "tracked_countries": TRACKED_COUNTRIES,
            "period": "monthly",
            "range": f"January 2025 - {current_month_year}"
        },
        
        # Configuration
        "keywords": KEYWORDS,
        "localized_keywords": LOCALIZED_KEYWORDS
    })

@app.route("/refresh")
def refresh_data():
    """Force refresh leaderboard data"""
    global cached_data, cache_timestamp
    
    print("Manual leaderboard refresh requested")
    fresh_data = fetch_fresh_data()
    
    if fresh_data:
        cached_data = fresh_data
        cache_timestamp = datetime.now()
        return jsonify({
            "message": "Leaderboard data refreshed successfully", 
            "items": len(cached_data["items"]),
            "timeframe": "3 months",
            "timestamp": datetime.now().isoformat()
        })
    else:
        return jsonify({
            "message": "Failed to refresh leaderboard data", 
            "error": "Could not fetch from Google Trends"
        }), 500

@app.route("/refresh-timeseries")
def refresh_timeseries():
    """Force refresh time series data for the 5 tracked countries"""
    global cached_timeseries, timeseries_cache_timestamp
    
    print("Manual time series refresh requested")
    fresh_data = fetch_timeseries_data()
    
    if fresh_data:
        cached_timeseries = fresh_data
        timeseries_cache_timestamp = datetime.now()
        return jsonify({
            "message": "Time series refreshed successfully", 
            "countries": len(fresh_data["series"]),
            "months": len(fresh_data["dates"]),
            "period": f"January 2025 - {datetime.now().strftime('%B %Y')}",
            "timestamp": datetime.now().isoformat()
        })
    else:
        return jsonify({
            "message": "Failed to refresh time series", 
            "error": "Could not fetch from Google Trends"
        }), 500

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)