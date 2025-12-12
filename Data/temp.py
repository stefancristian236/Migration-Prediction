import pandas as pd
import json
import time
import os
import random
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

# ---------------------------------------------------------
# 1. AUTHENTICATION
# ---------------------------------------------------------
try:
    with open("log.json") as e:
        creds = json.load(e)
except FileNotFoundError:
    print("Error: 'log.json' not found. Please create it with your client_id and client_secret.")
    exit()

CLIENT_ID = creds.get("client_id")
CLIENT_SECRET = creds.get("client_secret")
TOKEN_URL = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'

if not CLIENT_ID or not CLIENT_SECRET:
    print("Error: client_id or client_secret missing from 'log.json'.")
    exit()

def get_authenticated_session():
    client = BackendApplicationClient(client_id=CLIENT_ID)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(
        token_url=TOKEN_URL,
        client_secret=CLIENT_SECRET,
        include_client_id=True
    )
    return oauth

oauth = get_authenticated_session()
print("Authentication successful.")

# ---------------------------------------------------------
# 2. SENTINEL-3 EVALSCRIPT (FIXED LOGIC)
# ---------------------------------------------------------
s3_temp_evalscript = """
//VERSION=3
function setup() {
  return {
    input: [{
      bands: ["S8", "dataMask"],
      units: "BRIGHTNESS_TEMPERATURE" // <--- This was the error
    }],
    output: [
      { id: "default", bands: 1, sampleType: "FLOAT32" },
      { id: "dataMask", bands: 1, sampleType: "UINT8" }
    ],
    mosaicking: "ORBIT"
  };
}

function evaluatePixel(samples) {
  // 'samples' is now a single array of objects (one input source).
  // Each object contains both S8 and dataMask for that orbit.
  
  for (let i = 0; i < samples.length; i++) {
      let mask = samples[i].dataMask;
      let val = samples[i].S8;
      
      // Check if masked is valid (1) and value is not null/undefined
      if (mask === 1 && val != null) {
          // Convert Kelvin to Celsius
          let temp_celsius = val - 273.15;
          
          return {
              default: [temp_celsius],
              dataMask: [1]
          };
      }
  }

  // If no valid pixel found in any orbit
  return {
    default: [NaN],
    dataMask: [0]
  };
}
"""

def fetch_monthly_temp(lat, lon, year, session):
    stat_url = "https://sh.dataspace.copernicus.eu/api/v1/statistics"
    
    offset = 0.01 
    bbox = [lon - offset, lat - offset, lon + offset, lat + offset]
    
    start_date = f"{int(year)}-01-01T00:00:00Z"
    end_date = f"{int(year)}-12-31T23:59:59Z"

    # Data Source Config
    s3_data_config = {
        "type": "S3SLSTR",
        "dataFilter": {
            "maxCloudCoverage": 40,
            "previewMode": "EXTENDED_PREVIEW"
        }
    }

    json_request = {
       "input": {
                "bounds": {
                    "bbox": bbox,
                    "properties": {"crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}
                },
                # FIX: Provide data source ONLY ONCE
                "data": [
                    s3_data_config
                ]
            },
        "aggregation": {
            "timeRange": {"from": start_date, "to": end_date},
            "aggregationInterval": {"of": "P1M", "lastIntervalBehavior": "SHORTEN"}, 
            "evalscript": s3_temp_evalscript
        }
    }

    # RETRY LOGIC
    max_retries = 5
    base_delay = 2

    for attempt in range(max_retries):
        try:
            response = session.post(stat_url, json=json_request)

            if response.status_code == 200:
                return response.json()['data'], session

            elif response.status_code == 401:
                print("\nToken expired. Refreshing...", end='')
                session = get_authenticated_session()
                continue

            elif response.status_code == 429:
                wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                print(f"\nRate limit (429). Retry {attempt+1}/{max_retries} in {wait_time:.1f}s...", end='')
                time.sleep(wait_time)
                continue
            
            else:
                print(f"\nAPI Error {response.status_code}: {response.text}")
                return None, session

        except Exception as e:
            print(f"\nError: {e}")
            time.sleep(2)
    
    return None, session

# ---------------------------------------------------------
# 3. MAIN EXECUTION
# ---------------------------------------------------------
output_file = 'specific_locations_s3_temp_S8.csv'

target_locations = [
    {'lat': 56.0, 'lon': 15.8, 'label': 'Worse_Zone_1'},
    {'lat': 57.7, 'lon': 18.4, 'label': 'Worse_Zone_2'},
    {'lat': 56.7, 'lon': 16.5, 'label': 'Better_Zone_1'},
    {'lat': 57.2, 'lon': 17.0, 'label': 'Better_Zone_2'}
]

years = range(2016, 2025)

print(f"Targeting {len(target_locations)} locations over {len(years)} years.")
print("Frequency: Monthly (12 samples/year)")

if not os.path.exists(output_file):
    pd.DataFrame(columns=['label', 'latitude', 'longitude', 'year', 'date', 's3_temp_celsius']).to_csv(output_file, index=False)

batch_data = []

try:
    for loc in target_locations:
        for year in years:
            print(f"Fetching {loc['label']} ({loc['lat']}, {loc['lon']}) for {year}...", end='\r')
            
            stats, oauth = fetch_monthly_temp(loc['lat'], loc['lon'], year, oauth)
            
            if stats:
                for entry in stats:
                    date_str = entry['interval']['from'].split("T")[0]
                    
                    if 'outputs' in entry and 'default' in entry['outputs']:
                        bands = entry['outputs']['default']['bands']
                        
                        # Use string key '0'
                        if '0' in bands:
                            stats_obj = bands['0']['stats']
                            if 'mean' in stats_obj and stats_obj['mean'] != 'NaN':
                                val = stats_obj['mean']
                                
                                batch_data.append({
                                    'label': loc['label'],
                                    'latitude': loc['lat'],
                                    'longitude': loc['lon'],
                                    'year': year,
                                    'date': date_str,
                                    's3_temp_celsius': val
                                })

    if batch_data:
        pd.DataFrame(batch_data).to_csv(output_file, mode='a', header=False, index=False)
        print(f"\n\nSuccess! Extracted {len(batch_data)} monthly temperature records.")
        print(f"Data saved to: {output_file}")
    else:
        print("\nNo valid data found.")

except KeyboardInterrupt:
    print("\nInterrupted.")