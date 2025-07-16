# app.py (rename your main.py to app.py)
from pytrends.request import TrendReq
import pandas as pd
from flask import Flask, jsonify
import time
import random
import os

# Set your keywords
KEYWORDS = ['e-invoicing', 'PEPPOL', 'peppol', 'E-invoicing']

app = Flask(__name__)

@app.route("/")
def serve_data():
    try:
        print("Starting Google Trends fetch...")
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
        all_data = []

        for i, keyword in enumerate(KEYWORDS):
            print(f"Processing keyword {i+1}: {keyword}")
            
            if i > 0:  # Add delay between requests
                time.sleep(random.uniform(3, 6))
            
            try:
                pytrends.build_payload([keyword], timeframe='today 1-m')
                region_data = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True)
                
                print(f"Data shape for {keyword}: {region_data.shape}")
                print(f"Data empty: {region_data.empty}")
                
                if not region_data.empty:
                    region_data = region_data.reset_index()
                    region_data = region_data[['geoName', keyword]]
                    region_data.columns = ['country', 'interest']
                    all_data.append(region_data)
                    print(f"Added {len(region_data)} rows for {keyword}")
                    
            except Exception as e:
                print(f"Error with keyword {keyword}: {e}")
                continue  # Skip failed keywords

        print(f"Total datasets collected: {len(all_data)}")
        
        if not all_data:
            print("No data collected - returning default response")
            return jsonify({"items": [{"label": "No Data Available", "value": 0}]})

        # Combine and process data
        print("Combining and processing data...")
        df = pd.concat(all_data)
        df_grouped = df.groupby('country').agg({'interest': 'sum'}).reset_index()
        df_grouped = df_grouped[df_grouped['interest'] > 0].sort_values('interest', ascending=False)

        print(f"Found {len(df_grouped)} countries with interest > 0")

        # Convert to Geckoboard format
        geckoboard_data = {
            "items": [
                {"label": row['country'], "value": int(row['interest'])}
                for _, row in df_grouped.head(10).iterrows()
            ]
        }

        print(f"Returning {len(geckoboard_data['items'])} items")
        
        response = jsonify(geckoboard_data)
        response.headers['Content-Type'] = 'application/json'
        return response

    except Exception as e:
        print(f"General error occurred: {e}")
        return jsonify({"items": [{"label": "Error", "value": 0}]}), 500

@app.route("/health")
def health_check():
    return "OK", 200

if __name__ == "__main__":
    # Get port from environment variable or use 8080 for local development
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)