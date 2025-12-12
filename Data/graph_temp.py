import requests
import pandas as pd
import matplotlib.pyplot as plt

# ---------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------
locations = [
    {'label': 'Worse_Zone_1', 'lat': 56.0, 'lon': 15.8},
    {'label': 'Worse_Zone_2', 'lat': 57.7, 'lon': 18.4},
    {'label': 'Better_Zone_1', 'lat': 56.7, 'lon': 16.5},
    {'label': 'Better_Zone_2', 'lat': 57.2, 'lon': 17.0}
]

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2016-01-01"
END_DATE = "2024-12-31"

# Presentation styling
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "font.size": 12,
    "axes.titlesize": 16,
    "axes.labelsize": 13,
    "legend.fontsize": 12,
    "xtick.labelsize": 11,
    "ytick.labelsize": 11
})

# ---------------------------------------------------------
# 2. FETCH DATA
# ---------------------------------------------------------
dfs = []
print("Fetching data from Open-Meteo...")

for loc in locations:
    print(f"  - {loc['label']}...")
    params = {
        "latitude": loc['lat'],
        "longitude": loc['lon'],
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": "temperature_2m_mean",
        "timezone": "auto"
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        df = pd.DataFrame({
            'date': pd.to_datetime(data['daily']['time']),
            'temp': data['daily']['temperature_2m_mean']
        })
        
        df['label'] = loc['label']
        df['year'] = df['date'].dt.year
        df['day_of_year'] = df['date'].dt.dayofyear
        
        dfs.append(df)
        
    except Exception as e:
        print(f"  x Error fetching {loc['label']}: {e}")

if not dfs:
    print("No data fetched. Exiting.")
    exit()

full_df = pd.concat(dfs, ignore_index=True)
print("Data fetch complete.\n")

# ---------------------------------------------------------
# 3. GENERATE FOUR SEPARATE PLOTS
# ---------------------------------------------------------
print("Generating individual overlay plots...")

years_to_compare = [2016, 2024]
colors = {2016: "#1f77b4", 2024: "#d62728"}  # formal blue/red

month_starts = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

for loc in locations:
    label = loc['label']
    subset = full_df[full_df['label'] == label]

    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot all other years in soft gray
    other_years = subset[~subset['year'].isin(years_to_compare)]
    for year in sorted(other_years['year'].unique()):
        year_data = other_years[other_years['year'] == year]
        smooth = year_data.set_index('day_of_year')['temp'].rolling(window=7, center=True).mean()
        ax.plot(smooth.index, smooth, color="gray", alpha=0.12, linewidth=1)

    # Highlight 2016 and 2024
    for year in years_to_compare:
        year_data = subset[subset['year'] == year]
        smooth = year_data.set_index('day_of_year')['temp'].rolling(window=7, center=True).mean()
        ax.plot(
            smooth.index,
            smooth,
            color=colors[year],
            linewidth=2.8,
            label=f"{year}"
        )

    ax.set_title(f"Daily Mean Temperature Comparison", fontweight="bold")
    ax.set_ylabel("Temperature (°C)")
    ax.set_xlabel("Month")
    ax.set_xticks(month_starts)
    ax.set_xticklabels(month_names)

    ax.legend(title="Year")
    ax.grid(alpha=0.3)

    plt.tight_layout()

    output_file = f"plot_{label}_2016_vs_2024.png"
    plt.savefig(output_file, dpi=200)
    plt.close()

    print(f"Saved: {output_file}")

print("\nAll plots created successfully.")
