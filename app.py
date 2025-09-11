# app.py - Anti-429 version with session management and human-like behavior
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
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

# Track failures for exponential backoff
consecutive_failures = 0
last_failure_time = None

def create_pytrends_session():
    """Create a pytrends session with better retry logic and headers"""
    # Create session with custom retry strategy
    session = requests.Session()
    
    # More conservative retry strategy
    retry_strategy = Retry(
        total=2,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # Add more browser-like headers
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    })
    
    return TrendReq(hl='en-US', tz=360, timeout=(30, 45), requests_args={'verify': True})

def wait_with_backoff():
    """Implement exponential backoff based on consecutive failures"""
    global consecutive_failures, last_failure_time
    
    if consecutive_failures == 0:
        return
    
    # Calculate wait time: 5 minutes * 2^failures (max 2 hours)
    wait_minutes = min(5 * (2 ** consecutive_failures), 120)
    wait_seconds = wait_minutes * 60
    
    logger.warning(f"Backing off for {wait_minutes} minutes due to {consecutive_failures} consecutive failures")
    time.sleep(wait_seconds)

def fetch_leaderboard_data():
    """Fetch leaderboard data with aggressive anti-429 measures"""
    global consecutive_failures, last_failure_time
    
    try:
        logger.info("Starting leaderboard data fetch...")
        
        # Wait if we've had recent failures
        wait_with_backoff()
        
        # Initial delay to appear more human-like
        initial_delay = random.uniform(30, 60)
        logger.info(f"Initial delay of {initial_delay:.1f} seconds...")
        time.sleep(initial_delay)
        
        pytrends = create_pytrends_session()
        all_data = []
        
        for i, keyword in enumerate(KEYWORDS):
            if i > 0:
                # Much longer delay between keywords
                delay = random.uniform(60, 90)
                logger.info(f"Waiting {delay:.1f} seconds before next keyword...")
                time.sleep(delay)
            
            try:
                logger.info(f"Fetching data for keyword: {keyword}")
                
                # Use shorter timeframe to reduce load
                pytrends.build_payload([keyword], timeframe='today 1-m')  # Changed from 3-m to 1-m
                
                # Add delay after building payload
                time.sleep(random.uniform(5, 10))
                
                region_data = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True)
                
                if not region_data.empty:
                    region_data = region_data.reset_index()
                    region_data = region_data[['geoName', keyword]]
                    region_data.columns = ['country', 'interest']
                    all_data.append(region_data)
                    logger.info(f"Got {len(region_data)} countries for {keyword}")
                    consecutive_failures = 0  # Reset on success
                    
            except Exception as e:
                logger.error(f"Error with keyword {keyword}: {e}")
                if "429" in str(e) or "too many" in str(e).lower():
                    consecutive_failures += 1
                    last_failure_time = datetime.now()
                    
                    # Exponential backoff for 429 errors
                    wait_time = min(300 * consecutive_failures, 3600)  # 5 min, 10 min, 15 min... max 1 hour
                    logger.warning(f"Rate limited - waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    return None  # Give up on this attempt
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
            consecutive_failures = 0  # Reset on full success
            return result
        
        logger.warning("No leaderboard data collected")
        return None
            
    except Exception as e:
        logger.error(f"Failed to fetch leaderboard: {e}")
        consecutive_failures += 1
        last_failure_time = datetime.now()
        return None

def fetch_timeseries_data():
    """Fetch time series data with aggressive anti-429 measures"""
    global consecutive_failures, last_failure_time
    
    try:
        logger.info("Starting time series data fetch...")
        
        # Wait if we've had recent failures
        wait_with_backoff()
        
        # Initial delay
        initial_delay = random.uniform(30, 60)
        logger.info(f"Initial delay of {initial_delay:.1f} seconds...")
        time.sleep(initial_delay)
        
        pytrends = create_pytrends_session()
        country_data = {}
        date_labels = None
        
        country_geo_map = {
            'France': 'FR',
            'Belgium': 'BE', 
            'United Arab Emirates': 'AE',
            'United Kingdom': 'GB',
            'United States': 'US'
        }
        
        # Use shorter timeframe - last 30 days only
        today = datetime.now()
        start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
        timeframe = f'{start_date} {end_date}'
        
        for i, country in enumerate(TRACKED_COUNTRIES):
            if i > 0:
                # Very long delay between countries
                delay = random.uniform(90, 120)
                logger.info(f"Waiting {delay:.1f} seconds before next country...")
                time.sleep(delay)
            
            geo_code = country_geo_map.get(country, '')
            if not geo_code:
                continue
            
            # Only use one keyword per country to reduce requests
            keywords_to_use = [LOCALIZED_KEYWORDS.get(geo_code, KEYWORDS_PRIMARY)[0]]
            logger.info(f"Fetching {country} with keyword: {keywords_to_use[0]}")
                
            try:
                pytrends.build_payload(keywords_to_use, timeframe=timeframe, geo=geo_code)
                
                # Add delay after building payload
                time.sleep(random.uniform(5, 10))
                
                time_data = pytrends.interest_over_time()
                
                if not time_data.empty and len(time_data) > 0:
                    # Use weekly data instead of daily
                    time_data.index = pd.to_datetime(time_data.index)
                    weekly_data = time_data[keywords_to_use[0]].resample('W').mean().round(0)
                    
                    country_data[country] = weekly_data.tolist()
                    
                    if date_labels is None:
                        date_labels = [date.strftime('%m/%d') for date in weekly_data.index]
                    
                    logger.info(f"Got {len(weekly_data)} weeks for {country}")
                    consecutive_failures = 0  # Reset on success
                
            except Exception as e:
                logger.error(f"Error fetching {country}: {e}")
                if "429" in str(e) or "too many" in str(e).lower():
                    consecutive_failures += 1
                    last_failure_time = datetime.now()
                    
                    # Give up on time series for now
                    logger.warning("Rate limited on time series - aborting")
                    return None
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
                    "period": "weekly",
                    "timeframe": "Last 30 days",
                    "last_updated": datetime.now().isoformat()
                },
                "success": True
            }
            logger.info(f"Successfully fetched time series for {len(country_data)} countries")
            consecutive_failures = 0  # Reset on full success
            return result
        
        logger.warning("No time series data collected")
        return None
        
    except Exception as e:
        logger.error(f"Failed to fetch time series: {e}")
        consecutive_failures += 1
        last_failure_time = datetime.now()
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
    # Generate weekly dates for last 30 days
    dates = []
    for i in range(4):
        date = datetime.now() - timedelta(weeks=i)
        dates.insert(0, date.strftime('%m/%d'))
    
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
            {"name": country, "data": [base + random.randint(-5, 5) for _ in range(len(dates))]}
            for country, base in base_values.items()
        ],
        "metadata": {
            "period": "weekly",
            "fallback": True,
            "last_updated": datetime.now().isoformat()
        }
    }

def update_cache_carefully():
    """Update caches with careful timing to avoid 429s"""
    global cached_data, cache_timestamp, cached_timeseries, timeseries_cache_timestamp
    global consecutive_failures
    
    logger.info("=== Starting careful cache update ===")
    
    # If we've had many failures recently, skip this update
    if consecutive_failures >= 3:
        hours_since_failure = (datetime.now() - last_failure_time).total_seconds() / 3600 if last_failure_time else 24
        if hours_since_failure < 6:
            logger.warning(f"Skipping update - too many recent failures ({consecutive_failures})")
            return
    
    # Try to update leaderboard
    new_data = fetch_leaderboard_data()
    if new_data and new_data.get('success'):
        cached_data = new_data
        cache_timestamp = datetime.now()
        logger.info("✓ Leaderboard cache updated successfully")
        
        # Only try time series if leaderboard succeeded
        # Wait a long time before trying time series
        logger.info("Waiting 10 minutes before time series fetch...")
        time.sleep(600)  # 10 minutes
        
        new_timeseries = fetch_timeseries_data()
        if new_timeseries and new_timeseries.get('success'):
            cached_timeseries = new_timeseries
            timeseries_cache_timestamp = datetime.now()
            logger.info("✓ Time series cache updated successfully")
    else:
        logger.warning("Failed to update leaderboard - skipping time series")
    
    logger.info("=== Cache update complete ===")

def run_scheduler():
    """Background thread that runs the scheduler"""
    global stop_background, cached_data, cached_timeseries
    
    # Load with fallback data immediately
    cached_data = get_fallback_leaderboard()
    cached_timeseries = get_fallback_timeseries()
    logger.info("Loaded with fallback data")
    
    # Wait before first real attempt
    logger.info("Waiting 5 minutes before first fetch attempt...")
    time.sleep(300)  # 5 minutes
    
    # Try initial fetch
    update_cache_carefully()
    
    # Schedule updates only once per day at 3 AM UTC
    schedule.every().day.at("03:00").do(update_cache_carefully)
    logger.info("Scheduler started - will update once daily at 3 AM UTC")
    
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
    global consecutive_failures, last_failure_time
    
    return jsonify({
        "status": "operational",
        "current_time": datetime.now().isoformat(),
        "consecutive_failures": consecutive_failures,
        "last_failure": last_failure_time.isoformat() if last_failure_time else None,
        "leaderboard": {
            "has_data": cached_data is not None,
            "is_fallback": cached_data.get('fallback', False) if cached_data else True,
            "last_update": cache_timestamp.isoformat() if cache_timestamp else None,
            "hours_old": round((datetime.now() - cache_timestamp).total_seconds() / 3600, 1) if cache_timestamp else None
        },
        "timeseries": {
            "has_data": cached_timeseries is not None,
            "is_fallback": cached_timeseries.get('fallback', False) if cached_timeseries else True,
            "last_update": timeseries_cache_timestamp.isoformat() if timeseries_cache_timestamp else None,
            "hours_old": round((datetime.now() - timeseries_cache_timestamp).total_seconds() / 3600, 1) if timeseries_cache_timestamp else None,
            "countries": TRACKED_COUNTRIES
        },
        "next_update": schedule.next_run().isoformat() if schedule.next_run() else None
    })

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