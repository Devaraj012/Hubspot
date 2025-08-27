import requests
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
api = os.getenv("IBT_TOKEN")
API_KEY = api

# Headers
headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

# Step 1: Get ticket pipelines and stages
pipeline_url = 'https://api.hubapi.com/crm/v3/pipelines/tickets'
pipeline_response = requests.get(pipeline_url, headers=headers)

pipeline_mapping = {}        # pipeline_id -> pipeline_label
pipeline_stage_mapping = {}  # stage_id -> stage_label

if pipeline_response.status_code == 200:
    pipelines = pipeline_response.json().get('results', [])
    for pipeline in pipelines:
        pipeline_id = pipeline['id']
        pipeline_label = pipeline['label']
        pipeline_mapping[pipeline_id] = pipeline_label

        for stage in pipeline.get('stages', []):
            pipeline_stage_mapping[stage['id']] = stage['label']
else:
    print("Failed to retrieve pipeline info")
    exit()

# Step 2: Get tickets with properties
tickets_url = 'https://api.hubapi.com/crm/v3/objects/tickets'
params = {
    'properties': 'subject,hs_pipeline,hs_pipeline_stage,createdate,closed_date'
}

response = requests.get(tickets_url, headers=headers, params=params)

if response.status_code == 200:
    tickets_data = response.json()
    tickets_list = []

    for ticket in tickets_data.get('results', []):
        props = ticket.get('properties', {})
        subject = props.get('subject', 'N/A')
        pipeline_id = props.get('hs_pipeline', 'N/A')
        pipeline_stage_id = props.get('hs_pipeline_stage', 'N/A')
        created_at = props.get('createdate', 'N/A')
        closed_at = props.get('closed_date', 'N/A')

        pipeline_name = pipeline_mapping.get(pipeline_id, pipeline_id)
        stage_name = pipeline_stage_mapping.get(pipeline_stage_id, pipeline_stage_id)

        tickets_list.append([
            subject,
            pipeline_name,
            stage_name,
            created_at,
            closed_at
        ])

    # Save to CSV
    df = pd.DataFrame(
        tickets_list,
        columns=['Subject', 'Pipeline Name', 'Stage Name', 'Created At', 'Closed At']
    )
    df.to_csv('hubspot_tickets.csv', index=False)
    print("Tickets have been saved to hubspot_tickets.csv")

else:
    print(f"Failed to retrieve tickets: {response.status_code}")
    print("Response content:", response.text)