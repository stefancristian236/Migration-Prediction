from scipy import stats
import pandas as pd

df = pd.read_csv('Data/birds_db.csv')

winter_ndvi = df[(df['phase'] == 'Wintering') & (df['species'] == 'Collared flycatcher')]['ndvi']
breeding_ndvi = df[(df['phase'] == 'Spring Migration') & (df['species'] == 'Collared flycatcher')]['ndvi']

t_stat, p_val = stats.ttest_ind(winter_ndvi, breeding_ndvi, nan_policy='omit')

print(f"P-Value: {p_val:.4e}")  #p= 0.004



