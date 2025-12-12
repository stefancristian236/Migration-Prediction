import requests
import pandas as pd

# CONFIGURATION
LOCATION_NAME = "Better_Zone_2"
LAT = 57.2
LON = 17.0
START_DATE = "2016-01-01"
END_DATE = "2024-12-31"

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": LAT,
    "longitude": LON,
    "start_date": START_DATE,
    "end_date": END_DATE,
    "daily": "temperature_2m_mean",
    "timezone": "auto"
}

print(f"Fetching data for {LOCATION_NAME}...")
response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame({
        "date": data['daily']['time'],
        "temp_celsius": data['daily']['temperature_2m_mean']
    })
    df['location'] = LOCATION_NAME
    df['latitude'] = LAT
    df['longitude'] = LON
    
    filename = f"{LOCATION_NAME}_data.csv"
    df.to_csv(filename, index=False)
    print(f"Success! Saved {len(df)} records to {filename}")
else:
    print(f"Error: {response.status_code} - {response.text}")