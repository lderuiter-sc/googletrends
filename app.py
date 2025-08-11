# app.py - Monthly trends version for better stability
from pytrends.request import TrendReq
import pandas as pd
from pandas.tseries import offsets as pd_offsets
from flask import Flask, jsonify
import time
import random
import os
from datetime import datetime, timedelta

# ============== ENHANCED KEYWORD STRATEGY ==============
# Primary keywords (original)
KEYWORDS_PRIMARY = ['e-invoicing', 'PEPPOL']

# Additional broader keywords for better search volume
KEYWORDS_BROAD = ['electronic invoice', 'digital invoice', 'invoice automation']

# Combine for leaderboard (more keywords = more volume)
KEYWORDS = KEYWORDS_PRIMARY + KEYWORDS_BROAD

# Localized keywords by country for time series (better local relevance)
LOCALIZED_KEYWORDS = {
    'FR': ['facture électronique', 'facturation electronique', 'e-invoicing'],
    'DE': ['e-rechnung', 'elektronische rechnung', 'e-invoicing'],
    'MY': ['e-invois', 'e-invoice', 'e-invoicing'],
    'BE': ['e-facturatie', 'e-invoicing', 'PEPPOL'],  
    'AE': ['e-invoicing', 'electronic invoice', 'VAT invoice UAE']  # Added UAE-specific
}

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
                pytrends.build_payload([keyword], timeframe='today 3-m')
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
    """Fetch MONTHLY time series data from January 2025 to present"""
    try:
        print("Fetching monthly time series data (Jan 2025 - Present)...")
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
        
        country_data = {}
        date_labels = None
        
        # Map display names to Google Trends geo codes
        country_geo_map = {
            'France': 'FR',
            'Belgium': 'BE', 
            'Malaysia': 'MY',
            'United Arab Emirates': 'AE',
            'Germany': 'DE'
        }
        
        # ============== CHANGED: Dynamic timeframe from Jan 2025 to current date ==============
        # Use a dynamic date range from January 1, 2025 to today
        today = datetime.now().strftime('%Y-%m-%d')
        timeframe = f'2025-01-01 {today}'  # January 1, 2025 to current date
        print(f"Using dynamic timeframe: {timeframe}")
        
        for country in TRACKED_COUNTRIES:
            print(f"Processing monthly time series for {country}...")
            geo_code = country_geo_map.get(country, '')
            
            if not geo_code:
                print(f"No geo code found for {country}")
                continue
            
            # Use localized keywords if available, otherwise use primary keywords
            if geo_code in LOCALIZED_KEYWORDS:
                keywords_to_use = LOCALIZED_KEYWORDS[geo_code]
                print(f"Using localized keywords for {country}: {keywords_to_use}")
            else:
                keywords_to_use = KEYWORDS_PRIMARY
                print(f"Using default keywords for {country}: {keywords_to_use}")
                
            try:
                # Get time series data for this country with appropriate keywords
                pytrends.build_payload(keywords_to_use, timeframe=timeframe, geo=geo_code)
                time_data = pytrends.interest_over_time()
                
                if not time_data.empty and len(time_data) > 0:
                    print(f"Raw data columns: {time_data.columns.tolist()}")
                    print(f"Raw data shape: {time_data.shape}")
                    print(f"Date range: {time_data.index[0]} to {time_data.index[-1]}")
                    
                    # Sum all keywords for each time point (exclude 'isPartial' if it exists)
                    keyword_cols = [col for col in time_data.columns if col in keywords_to_use]
                    if keyword_cols:
                        time_data['total'] = time_data[keyword_cols].sum(axis=1)
                        print(f"Created total column with keywords: {keyword_cols}")
                    else:
                        print(f"No keyword columns found. Available columns: {time_data.columns.tolist()}")
                        continue
                    
                    # ============== CHANGED: Aggregate by MONTH instead of week ==============
                    time_data.index = pd.to_datetime(time_data.index)
                    monthly_data = time_data['total'].resample('M').mean().round(0)
                    
                    # Ensure we have data for all months from Jan to current month
                    # Fill any missing months with 0
                    current_date = datetime.now()
                    end_of_current_month = pd.Timestamp(current_date.year, current_date.month, 1) + pd_offsets.MonthEnd(0)
                    all_months = pd.date_range(start='2025-01-01', end=end_of_current_month, freq='M')
                    monthly_data = monthly_data.reindex(all_months, fill_value=0)
                    
                    # Convert to list of values
                    country_data[country] = monthly_data.tolist()
                    print(f"Added {len(monthly_data)} monthly data points for {country}")
                    
                    # Store date labels from first successful country
                    if date_labels is None:
                        # ============== CHANGED: Format as month names ==============
                        date_labels = [date.strftime('%b') for date in monthly_data.index]
                        print(f"Generated {len(date_labels)} month labels: {date_labels}")
                else:
                    print(f"No time series data for {country}")
                
                # Add delay between countries
                time.sleep(random.uniform(4, 6))
                
            except Exception as e:
                print(f"Error fetching time series for {country}: {e}")
                if "429" in str(e):
                    print("Rate limited - waiting 30 seconds...")
                    time.sleep(30)
                continue
        
        if country_data and date_labels:
            # Format for line chart
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
                    "last_updated": datetime.now().isoformat()
                }
            }
            
            print(f"Successfully created monthly time series with {len(date_labels)} months and {len(country_data)} countries")
            print(f"Date range: January 2025 to {current_month_year}")
            return result
        
        print("No time series data collected")
        return None
        
    except Exception as e:
        print(f"Error fetching time series data: {e}")
        import traceback
        traceback.print_exc()
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
    """Serve monthly time series data for line plots"""
    global cached_timeseries, timeseries_cache_timestamp
    
    now = datetime.now()
    
    # Check if time series cache is valid (cache for 12 hours)
    cache_valid = (cached_timeseries is not None and 
                   timeseries_cache_timestamp is not None and 
                   now - timeseries_cache_timestamp < timedelta(hours=12))
    
    if cache_valid:
        print("Serving cached monthly time series data")
        response = jsonify(cached_timeseries)
        response.headers['Content-Type'] = 'application/json'
        return response
    
    # Cache expired or doesn't exist - try to fetch fresh data
    print("Time series cache expired - fetching fresh monthly data...")
    fresh_data = fetch_timeseries_data()
    
    if fresh_data:
        # Successfully got fresh data
        cached_timeseries = fresh_data
        timeseries_cache_timestamp = now
        print("Serving fresh monthly time series data")
        response = jsonify(cached_timeseries)
    else:
        # Failed to get fresh data - return fallback
        fallback_timeseries = get_fallback_timeseries()
        print("Failed to get fresh time series data - serving fallback monthly data")
        response = jsonify(fallback_timeseries)
    
    response.headers['Content-Type'] = 'application/json'
    return response

def get_fallback_timeseries():
    """Return fallback MONTHLY time series data (Jan 2025 to current month)"""
    # ============== CHANGED: Dynamic monthly fallback data ==============
    # Generate month labels from January 2025 to current month
    current_date = datetime.now()
    dates = []
    start_date = datetime(2025, 1, 1)
    
    while start_date <= current_date:
        dates.append(start_date.strftime('%B %Y'))
        # Move to next month
        if start_date.month == 12:
            start_date = start_date.replace(year=start_date.year + 1, month=1)
        else:
            start_date = start_date.replace(month=start_date.month + 1)
    
    # Generate sample data for each month (increasing trend)
    num_months = len(dates)
    base_values = {
        "Belgium": 45,
        "France": 35,
        "Germany": 40,
        "Malaysia": 25,
        "United Arab Emirates": 20
    }
    
    series_data = {}
    for country, base in base_values.items():
        # Generate increasing trend with slight randomness
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
            "note": "Fallback data - Google Trends unavailable"
        }
    }

@app.route("/health")
def health_check():
    """Health check endpoint"""
    return "OK", 200

@app.route("/status")
def status():
    """Status endpoint"""
    global cached_data, cache_timestamp, cached_timeseries, timeseries_cache_timestamp
    
    current_month_year = datetime.now().strftime('%B %Y')
    
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
        "tracked_countries": TRACKED_COUNTRIES,
        
        # Keywords being used
        "keywords_leaderboard": KEYWORDS,
        "keywords_localized": LOCALIZED_KEYWORDS,
        
        # Time series info
        "timeseries_period": "monthly",
        "timeseries_range": f"January 2025 - {current_month_year}"
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
    """Force refresh monthly time series data"""
    global cached_timeseries, timeseries_cache_timestamp
    
    print("Force monthly time series refresh requested")
    fresh_data = fetch_timeseries_data()
    
    if fresh_data:
        cached_timeseries = fresh_data
        timeseries_cache_timestamp = datetime.now()
        current_month_year = datetime.now().strftime('%B %Y')
        return jsonify({
            "message": "Monthly time series refreshed successfully", 
            "countries": len(fresh_data["series"]),
            "months": len(fresh_data["dates"]),
            "period": f"January 2025 - {current_month_year}"
        })
    else:
        return jsonify({
            "message": "Failed to refresh monthly time series", 
            "error": "Could not fetch from Google Trends"
        }), 500

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)