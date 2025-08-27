import pandas as pd
import os


hubspot_df = pd.read_csv('hubspot_events.csv')

event_df = hubspot_df[['Contact', 'Event Date', 'Stage', 'Status']].copy()

event_df["Event Date"] = pd.to_datetime(event_df["Event Date"], format='mixed', utc=True).dt.strftime('%m-%d-%Y')

output_folder = r'C:\Users\devar\Documents\Code\JA'
os.makedirs(output_folder, exist_ok=True)

output_path = os.path.join(output_folder, 'events.csv')
event_df.to_csv(output_path, index=False)

print(f"File saved at: {output_path}")


