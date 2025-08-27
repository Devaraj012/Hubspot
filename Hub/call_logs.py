import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("IBT")

url = "https://api.hubapi.com/crm/v3/objects/calls"
contact_url = "https://api.hubapi.com/crm/v3/objects/contacts/{}"
company_url = "https://api.hubapi.com/crm/v3/objects/companies/{}"

params = {
    'limit': 100,
    'properties': 'hs_call_title,hs_call_body,hs_call_status,hs_call_duration,hs_call_direction,hs_timestamp',
    'associations': 'contact'
}

headers = {
    'Authorization': f'Bearer {API_KEY} ',
    'Content-Type': 'application/json'
}

all_calls = []
has_more = True
after = None

def get_company_name(company_id):
    resp = requests.get(
        company_url.format(company_id),
        headers=headers,
        params={'properties': 'name'}
    )
    if resp.status_code == 200:
        return resp.json().get('properties', {}).get('name', 'N/A')
    return 'N/A'

def get_contact_details(contact_id):
    resp = requests.get(
        contact_url.format(contact_id),
        headers=headers,
        params={'properties': 'firstname,lastname,email,jobtitle', 'associations': 'company'}
    )
    if resp.status_code == 200:
        data = resp.json()
        props = data.get('properties', {})
        company_name = 'N/A'
        associations = data.get('associations', {})
        companies = associations.get('companies', {}).get('results', [])
        if companies:
            company_id = companies[0].get('id')
            company_name = get_company_name(company_id)
        return {
            'First Name': props.get('firstname', 'N/A'),
            'Last Name': props.get('lastname', 'N/A'),
            'Contact Email': props.get('email', 'N/A'),
            'Position': props.get('jobtitle', 'N/A'),
            'Company': company_name
        }
    return {
        'First Name': 'N/A',
        'Last Name': 'N/A',
        'Email': 'N/A',
        'Position': 'N/A',
        'Company': 'N/A'
    }

while has_more:
    if after:
        params['after'] = after

    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    for call in data.get('results', []):
        props = call.get('properties', {})
        contact_info = {
            'First Name': 'N/A',
            'Last Name': 'N/A',
            'Email': 'N/A',
            'Position': 'N/A',
            'Company': 'N/A'
        }

        associations = call.get('associations', {})
        contacts = associations.get('contacts', {}).get('results', [])
        if contacts:
            contact_id = contacts[0].get('id')
            contact_info = get_contact_details(contact_id)

        all_calls.append({
            'id': call.get('id'),
            'Title': props.get('hs_call_title', 'N/A'),
            'Status': props.get('hs_call_status', 'N/A'),
            'Duration (sec)': props.get('hs_call_duration', 'N/A'),
            'Direction': props.get('hs_call_direction', 'N/A'),
            'Timestamp': props.get('hs_timestamp', 'N/A'),
            **contact_info
        })

    # Check for pagination
    paging = data.get('paging', {}).get('next', {})
    after = paging.get('after')
    has_more = bool(after)

df = pd.DataFrame(all_calls)

df['contact']=df['First Name']+' '+df['Last Name']
df.to_csv('calls.csv', index=False)
print("✅ Call data with contact and company details exported to calls.csv")

