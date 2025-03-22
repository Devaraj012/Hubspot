import requests
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
api = os.getenv("HUBSPOT_ACCESS_TOKEN")

API_KEY = api

url = 'https://api.hubapi.com/crm/v3/objects/contacts'

headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

params = {
    'properties': 'firstname,lastname,email,company,hs_lead_status,jobtitle'
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    contacts_data = response.json()
    contacts_list = []

    for contact in contacts_data.get('results', []):
        first_name = contact['properties'].get('firstname', 'N/A')
        last_name = contact['properties'].get('lastname', 'N/A')
        email = contact['properties'].get('email', 'N/A')
        company = contact['properties'].get('company', 'N/A')
        lead_status = contact['properties'].get('hs_lead_status', 'N/A')
        job_title = contact['properties'].get('jobtitle', 'N/A')
        contact_id = contact['id']  # Extract Contact Id here
        contacts_list.append([contact_id, first_name, last_name, email, company, lead_status, job_title])

    df = pd.DataFrame(contacts_list, columns=['Contact Id', 'First Name', 'Last Name', 'Email', 'Company', 'Lead Status', 'Job Title'])

    excel_filename = 'hubspot_contacts.csv'
    df.to_csv(excel_filename, index=False)
    print(f"Contacts have been successfully saved to {excel_filename}")

else:
    print(f"Failed to retrieve contacts: {response.status_code}")
    print("Response content:", response.text)
