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
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25), retries=2)
        all_data = []

        for i, keyword in enumerate(KEYWORDS):
            if i > 0:  # Add delay between requests
                time.sleep(random.uniform(3, 6))
            
            try:
                pytrends.build_payload([keyword], timeframe='today 1-m')
                region_data = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True)
                
                if not region_data.empty:
                    region_data = region_data.reset_index()
                    region_data = region_data[['geoName', keyword]]
                    region_data.columns = ['country', 'interest']
                    all_data.append(region_data)
                    
            except Exception:
                continue  # Skip failed keywords

        if not all_data:
            return jsonify({"items": [{"label": "No Data Available", "value": 0}]})

        # Combine and process data
        df = pd.concat(all_data)
        df_grouped = df.groupby('country').agg({'interest': 'sum'}).reset_index()
        df_grouped = df_grouped[df_grouped['interest'] > 0].sort_values('interest', ascending=False)

        # Convert to Geckoboard format
        geckoboard_data = {
            "items": [
                {"label": row['country'], "value": int(row['interest'])}
                for _, row in df_grouped.head(10).iterrows()
            ]
        }

        response = jsonify(geckoboard_data)
        response.headers['Content-Type'] = 'application/json'
        return response

    except Exception:
        return jsonify({"items": [{"label": "Error", "value": 0}]}), 500

if __name__ == "__main__":
    # For local development
    app.run(host='0.0.0.0', port=8080, debug=True)

