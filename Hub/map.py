import pandas as pd
import os

# Contact map

hubspot_df = pd.read_csv('calls.csv')

event_df = hubspot_df[['First Name', 'Last Name', 'Company', 'Position']].copy()

event_df=event_df.drop_duplicates()

output_folder = r'C:\Users\devar\Documents\Code\JA'
os.makedirs(output_folder, exist_ok=True)

output_path = os.path.join(output_folder, 'contacts.csv')
event_df.to_csv(output_path, index=False)

print(f"File saved at: {output_path}")


# Output folder
output_folder = r'C:\Users\devar\Documents\Code\JA'
os.makedirs(output_folder, exist_ok=True)
output_path = os.path.join(output_folder, 'events.csv')

# Read HubSpot events
events_df = pd.read_csv('hubspot_events_VISHRUTHA.csv')
events_df = events_df[['Contact', 'Event Date', 'Stage', 'Status']].copy()
events_df["Event Date"] = pd.to_datetime(events_df["Event Date"], format='mixed', utc=True).dt.strftime('%m-%d-%Y')
events_df['Source'] = 'Event'  # Identify source

# Read HubSpot tasks
tasks_df = pd.read_csv('hubspot_tasks.csv')
tasks_df = tasks_df[['Contact', 'Event Date', 'Stage', 'Status']].copy()
tasks_df["Event Date"] = pd.to_datetime(tasks_df["Event Date"], format='mixed', utc=True).dt.strftime('%m-%d-%Y')
tasks_df['Source'] = 'Task'  # Identify source

#Read Hubspot Call logs
call_df=pd.read_csv('calls.csv')
call_df['Timestamp'] = pd.to_datetime(call_df['Timestamp'], format='mixed', utc=True).dt.strftime('%m-%d-%Y')
call_df['Event Date'] = call_df['Timestamp']
call_df['Stage']=call_df['Direction']
call_df=call_df[['Contact', 'Event Date', 'Stage','Status']].copy()

# Merge
merged_df = pd.concat([events_df, tasks_df,call_df], ignore_index=True)

# Save merged file
merged_df.to_csv(output_path, index=False)

print(f"Merged file saved at: {output_path}")
