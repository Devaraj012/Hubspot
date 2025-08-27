import requests
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
api = os.getenv("IBT_TOKEN")

API_KEY = api

# HubSpot Deals URL
deals_url = 'https://api.hubapi.com/crm/v3/objects/deals'

# Headers
headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

# Request deals with selected properties
params = {
    'properties': 'dealname,amount,dealstage,closedate,createdate'
}

# Make the request
response = requests.get(deals_url, headers=headers, params=params)

if response.status_code == 200:
    deals_data = response.json()
    deals_list = []

    for deal in deals_data.get('results', []):
        properties = deal.get('properties', {})
        deal_name = properties.get('dealname', 'N/A')
        amount = properties.get('amount', 'N/A')
        deal_stage = properties.get('dealstage', 'N/A')
        created_at = properties.get('createdate', 'N/A')
        closed_at = properties.get('closedate', 'N/A')

        deals_list.append([deal_name, amount, deal_stage, created_at, closed_at])

    # Create DataFrame
    df = pd.DataFrame(deals_list, columns=['Deal Name', 'Amount', 'Deal Stage', 'Created At', 'Closed At'])

    # Save to CSV
    deals_filename = 'hubspot_deals.csv'
    df.to_csv(deals_filename, index=False)
    print(f"Deals have been successfully saved to {deals_filename}")

else:
    print(f"Failed to retrieve deals: {response.status_code}")
    print("Response content:", response.text)
