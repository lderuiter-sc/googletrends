# app.py - Optimized for Render Free Tier with reliable background updates
from pytrends.request import TrendReq
import pandas as pd
from pandas.tseries import offsets as pd_offsets
from flask import Flask, jsonify
import time
import random
import os
from datetime import datetime, timedelta
import threading
import schedule
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============== CONFIGURATION ==============
KEYWORDS_PRIMARY = ['e-invoicing', 'PEPPOL']
KEYWORDS = KEYWORDS_PRIMARY

# Localized keywords for better relevance
LOCALIZED_KEYWORDS = {
    'FR': ['facture électronique', 'facturation electronique', 'e-invoicing'],
    'BE': ['e-facturatie', 'e-invoicing', 'PEPPOL'],  
    'AE': ['e-invoicing', 'electronic invoice', 'VAT invoice UAE'],
    'GB': ['e-invoicing', 'electronic invoice', 'PEPPOL UK'],
    'US': ['e-invoicing', 'electronic invoice', 'invoice automation']
}

# Fixed 5 countries for time series
TRACKED_COUNTRIES = ['France', 'Belgium', 'United Arab Emirates', 'United Kingdom', 'United States']

# Small countries to exclude from leaderboard
EXCLUDED_COUNTRIES = [
    'Palau', 'Grenada', 'St. Pierre & Miquelon', 'Mayotte', 'St. Helena', 
    'Wallis & Futuna', 'Faroe Islands', 'San Marino', 'Liechtenstein',
    'Andorra', 'Monaco', 'Nauru', 'Tuvalu', 'Vatican City', 'Marshall Islands',
    'Micronesia', 'Kiribati', 'Samoa', 'Tonga', 'Dominica', 'St. Lucia',
    'Barbados', 'St. Vincent & Grenadines', 'Antigua & Barbuda', 'Seychelles',
    'Maldives', 'Guam', 'American Samoa', 'Northern Mariana Islands', 
    'U.S. Virgin Islands', 'British Virgin Islands', 'Cayman Islands', 
    'Turks & Caicos Islands', 'Bermuda', 'Anguilla', 'Montserrat', 
    'Falkland Islands', 'Greenland', 'French Polynesia', 'New Caledonia', 
    'French Guiana', 'Martinique', 'Guadeloupe', 'Réunion', 'Gibraltar', 
    'Isle of Man', 'Jersey', 'Guernsey', 'Djibouti', 'Comoros', 
    'São Tomé & Príncipe', 'Guinea-Bissau'
]

app = Flask(__name__)

# Global cache variables
cached_data = None
cache_timestamp = None
cached_timeseries = None
timeseries_cache_timestamp = None

# Background thread control
background_thread = None
stop_background = False

def fetch_leaderboard_data():
    """Fetch leaderboard data with proper error handling"""
    try:
        logger.info("Starting leaderboard data fetch...")
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25), retries=2, backoff_factor=1.0)
        all_data = []
        
        for i, keyword in enumerate(KEYWORDS):
            if i > 0:
                delay = random.uniform(8, 12)  # Longer delay to avoid rate limits
                logger.info(f"Waiting {delay:.1f} seconds before next keyword...")
                time.sleep(delay)
            
            try:
                logger.info(f"Fetching data for keyword: {keyword}")
                pytrends.build_payload([keyword], timeframe='today 3-m')
                region_data = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True)
                
                if not region_data.empty:
                    region_data = region_data.reset_index()
                    region_data = region_data[['geoName', keyword]]
                    region_data.columns = ['country', 'interest']
                    all_data.append(region_data)
                    logger.info(f"Got {len(region_data)} countries for {keyword}")
                    
            except Exception as e:
                logger.error(f"Error with keyword {keyword}: {e}")
                if "429" in str(e):
                    logger.warning("Rate limited - waiting 60 seconds...")
                    time.sleep(60)
                continue

        if all_data:
            df = pd.concat(all_data)
            df_grouped = df.groupby('country').agg({'interest': 'sum'}).reset_index()
            df_grouped = df_grouped[df_grouped['interest'] > 0].sort_values('interest', ascending=False)
            df_filtered = df_grouped[~df_grouped['country'].isin(EXCLUDED_COUNTRIES)]
            
            result = {
                "items": [
                    {"label": row['country'], "value": int(row['interest'])}
                    for _, row in df_filtered.head(10).iterrows()
                ],
                "timestamp": datetime.now().isoformat(),
                "success": True
            }
            logger.info(f"Successfully fetched leaderboard with {len(result['items'])} countries")
            return result
        
        logger.warning("No leaderboard data collected")
        return None
            
    except Exception as e:
        logger.error(f"Failed to fetch leaderboard: {e}")
        return None

def fetch_timeseries_data():
    """Fetch time series data for the 5 tracked countries"""
    try:
        logger.info("Starting time series data fetch...")
        # Simplified initialization to avoid urllib3 compatibility issues
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
        
        # Dynamic timeframe from January 2025 to now
        today = datetime.now().strftime('%Y-%m-%d')
        timeframe = f'2025-01-01 {today}'
        
        for i, country in enumerate(TRACKED_COUNTRIES):
            if i > 0:
                delay = random.uniform(10, 15)  # Longer delay between countries
                logger.info(f"Waiting {delay:.1f} seconds before next country...")
                time.sleep(delay)
            
            geo_code = country_geo_map.get(country, '')
            if not geo_code:
                continue
            
            keywords_to_use = LOCALIZED_KEYWORDS.get(geo_code, KEYWORDS_PRIMARY)
            logger.info(f"Fetching {country} with keywords: {keywords_to_use}")
                
            try:
                pytrends.build_payload(keywords_to_use, timeframe=timeframe, geo=geo_code)
                time_data = pytrends.interest_over_time()
                
                if not time_data.empty and len(time_data) > 0:
                    # Sum all keywords
                    keyword_cols = [col for col in time_data.columns if col in keywords_to_use]
                    if keyword_cols:
                        time_data['total'] = time_data[keyword_cols].sum(axis=1)
                        
                        # Aggregate by month
                        time_data.index = pd.to_datetime(time_data.index)
                        monthly_data = time_data['total'].resample('M').mean().round(0)
                        
                        # Ensure all months are present
                        current_date = datetime.now()
                        end_of_current_month = pd.Timestamp(current_date.year, current_date.month, 1) + pd_offsets.MonthEnd(0)
                        all_months = pd.date_range(start='2025-01-01', end=end_of_current_month, freq='M')
                        monthly_data = monthly_data.reindex(all_months, fill_value=0)
                        
                        country_data[country] = monthly_data.tolist()
                        
                        if date_labels is None:
                            date_labels = [date.strftime('%b') for date in monthly_data.index]
                        
                        logger.info(f"Got {len(monthly_data)} months for {country}")
                
            except Exception as e:
                logger.error(f"Error fetching {country}: {e}")
                if "429" in str(e):
                    logger.warning("Rate limited - waiting 60 seconds...")
                    time.sleep(60)
                continue
        
        if country_data and date_labels:
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
                    "end": datetime.now().strftime('%B %Y'),
                    "last_updated": datetime.now().isoformat()
                },
                "success": True
            }
            logger.info(f"Successfully fetched time series for {len(country_data)} countries")
            return result
        
        logger.warning("No time series data collected")
        return None
        
    except Exception as e:
        logger.error(f"Failed to fetch time series: {e}")
        return None

def get_fallback_leaderboard():
    """Fallback data when API fails"""
    return {
        "items": [
            {"label": "Belgium", "value": 200},
            {"label": "United Kingdom", "value": 150},
            {"label": "United States", "value": 125},
            {"label": "France", "value": 95},
            {"label": "United Arab Emirates", "value": 85},
            {"label": "Netherlands", "value": 72},
            {"label": "Germany", "value": 65},
            {"label": "Sweden", "value": 58},
            {"label": "Norway", "value": 45},
            {"label": "Denmark", "value": 38}
        ],
        "timestamp": datetime.now().isoformat(),
        "fallback": True
    }

def get_fallback_timeseries():
    """Fallback time series data"""
    current_date = datetime.now()
    dates = []
    temp_date = datetime(2025, 1, 1)
    
    while temp_date <= current_date:
        dates.append(temp_date.strftime('%b'))
        if temp_date.month == 12:
            temp_date = temp_date.replace(year=temp_date.year + 1, month=1)
        else:
            temp_date = temp_date.replace(month=temp_date.month + 1)
    
    base_values = {
        "Belgium": 45,
        "France": 35,
        "United Arab Emirates": 20,
        "United Kingdom": 38,
        "United States": 42
    }
    
    return {
        "dates": dates,
        "series": [
            {"name": country, "data": [base + (i * 2) for i in range(len(dates))]}
            for country, base in base_values.items()
        ],
        "metadata": {
            "period": "monthly",
            "fallback": True,
            "last_updated": datetime.now().isoformat()
        }
    }

def update_cache():
    """Update both caches - called by scheduler"""
    global cached_data, cache_timestamp, cached_timeseries, timeseries_cache_timestamp
    
    logger.info("=== Starting scheduled cache update ===")
    
    # Update leaderboard
    new_data = fetch_leaderboard_data()
    if new_data and new_data.get('success'):
        cached_data = new_data
        cache_timestamp = datetime.now()
        logger.info("✓ Leaderboard cache updated successfully")
    else:
        logger.warning("Failed to update leaderboard - keeping existing cache")
    
    # Wait before fetching time series to avoid rate limits
    time.sleep(30)
    
    # Update time series
    new_timeseries = fetch_timeseries_data()
    if new_timeseries and new_timeseries.get('success'):
        cached_timeseries = new_timeseries
        timeseries_cache_timestamp = datetime.now()
        logger.info("✓ Time series cache updated successfully")
    else:
        logger.warning("Failed to update time series - keeping existing cache")
    
    logger.info("=== Cache update complete ===")

def run_scheduler():
    """Background thread that runs the scheduler"""
    global stop_background
    
    # Initial data fetch on startup
    logger.info("Performing initial data fetch...")
    update_cache()
    
    # Schedule updates every 6 hours
    schedule.every(6).hours.do(update_cache)
    logger.info("Scheduler started - will update every 6 hours")
    
    while not stop_background:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

# Flask Routes
@app.route("/")
def serve_leaderboard():
    """Serve leaderboard data"""
    global cached_data
    
    if cached_data:
        response = jsonify(cached_data)
    else:
        logger.warning("No cached data available - serving fallback")
        response = jsonify(get_fallback_leaderboard())
    
    response.headers['Content-Type'] = 'application/json'
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

@app.route("/timeseries")
def serve_timeseries():
    """Serve time series data"""
    global cached_timeseries
    
    if cached_timeseries:
        response = jsonify(cached_timeseries)
    else:
        logger.warning("No cached time series - serving fallback")
        response = jsonify(get_fallback_timeseries())
    
    response.headers['Content-Type'] = 'application/json'
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

@app.route("/health")
def health_check():
    """Health check for Render"""
    return "OK", 200

@app.route("/status")
def status():
    """Detailed status information"""
    global cached_data, cache_timestamp, cached_timeseries, timeseries_cache_timestamp
    
    return jsonify({
        "status": "operational",
        "current_time": datetime.now().isoformat(),
        "leaderboard": {
            "has_data": cached_data is not None,
            "last_update": cache_timestamp.isoformat() if cache_timestamp else None,
            "hours_old": round((datetime.now() - cache_timestamp).total_seconds() / 3600, 1) if cache_timestamp else None
        },
        "timeseries": {
            "has_data": cached_timeseries is not None,
            "last_update": timeseries_cache_timestamp.isoformat() if timeseries_cache_timestamp else None,
            "hours_old": round((datetime.now() - timeseries_cache_timestamp).total_seconds() / 3600, 1) if timeseries_cache_timestamp else None,
            "countries": TRACKED_COUNTRIES
        },
        "next_update": schedule.next_run().isoformat() if schedule.next_run() else None
    })

@app.route("/force-refresh")
def force_refresh():
    """Manual refresh (use sparingly!)"""
    logger.info("Manual refresh requested")
    update_cache()
    return jsonify({"message": "Cache refresh initiated", "timestamp": datetime.now().isoformat()})

# Initialize background scheduler on startup
def initialize():
    """Initialize the app with background scheduler"""
    global background_thread
    
    if background_thread is None:
        background_thread = threading.Thread(target=run_scheduler, daemon=True)
        background_thread.start()
        logger.info("Background scheduler thread started")

# Start the scheduler when module loads
initialize()

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)