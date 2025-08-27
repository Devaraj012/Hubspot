import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

TOKEN=['IBT','SELVA','VISHRUTHA']
for token in TOKEN:  
   
# Load environment variables
    API_KEY = os.getenv(token)
    
    if token!='GREENESTEP':
        contacts_url = 'https://api.hubapi.com/crm/v3/objects/contacts'
        companies_url = 'https://api.hubapi.com/crm/v3/objects/companies'

        # Headers
        headers = {
            'Authorization': f'Bearer {API_KEY}',
            'Content-Type': 'application/json'
        }

        # Query parameters
        params = {
            'properties': 'firstname,lastname,email,hs_lead_status,jobtitle,createdate,lifecyclestage,known_since_date',
            'associations': 'company',
            'limit': 100
        }

        contacts_list = []
        has_more = True
        after = None

        while has_more:
            if after:
                params['after'] = after

            response = requests.get(contacts_url, headers=headers, params=params)

            if response.status_code == 200:
                contacts_data = response.json()

                for contact in contacts_data.get('results', []): 
                    properties = contact.get('properties', {})
                    contact_id = contact.get('id', 'N/A')
                    gmail = properties.get('email', 'N/A')
                    first_name = properties['firstname']
                    last_name = properties['lastname']
 
                    job_title = properties.get('jobtitle', 'N/A')
                    known_since = properties.get('known_since_date', 'N/A')
                    
                    stage = (properties.get('lifecyclestage') or '').lower()

                    # Normalize lifecycle stage
                    if stage in ['lead', 'marketingqualifiedlead']:
                        stage = 'Marketing'
                    elif stage in ['opportunity', 'salesqualifiedlead']:
                        stage = 'Sales'
                    elif stage == 'customer':
                        stage = 'Customer Support'
                    elif stage == 'other':
                        stage = 'Operations'
                    else:
                        stage = 'KYC'

                    # Company
                    company_name = 'N/A'
                    associations = contact.get('associations', {})
                    if 'companies' in associations and 'results' in associations['companies']:
                        company_id = associations['companies']['results'][0]['id']
                        company_response = requests.get(f"{companies_url}/{company_id}", headers=headers)
                        if company_response.status_code == 200:
                            company_data = company_response.json()
                            company_name = company_data.get('properties', {}).get('name', 'N/A')

                    # Append row
                    contacts_list.append([
                        contact_id, first_name, last_name, company_name,
                        job_title, stage, known_since, gmail
                    ])

                # Handle pagination
                after = contacts_data.get('paging', {}).get('next', {}).get('after')
                has_more = bool(after)
            else:
                print(f"❌ Failed to retrieve contacts: {response.status_code}")
                print("Response content:", response.text)
                break

        # Save to CSV
        df = pd.DataFrame(contacts_list, columns=[
            'Contact_ID', 'First Name', 'Last Name', 'Company',
            'Position', 'Stage', 'Known Since', 'Email'
        ])
        excel_filename = f'hubspot_contacts{token}.csv'
        df.to_csv(excel_filename, index=False)
        print(f"✅ Contacts have been successfully saved to {excel_filename}")
    
    
    
    if token=='GREENESTEP':
    # HubSpot URLs
        contacts_url = 'https://api.hubapi.com/crm/v3/objects/contacts'
        companies_url = 'https://api.hubapi.com/crm/v3/objects/companies'

        # Headers
        headers = {
            'Authorization': f'Bearer {API_KEY}',
            'Content-Type': 'application/json'
        }

        # Query parameters
        params = {
            'properties': 'firstname,lastname,email,hs_lead_status,jobtitle,createdate,lifecyclestage,known_since_date',
            'associations': 'company',
            'limit': 100
        }

        contacts_list = []
        has_more = True
        after = None

        while has_more:
            if after:
                params['after'] = after

            response = requests.get(contacts_url, headers=headers, params=params)

            if response.status_code == 200:
                contacts_data = response.json()

                for contact in contacts_data.get('results', []):
                    properties = contact.get('properties', {})
                    contact_id = contact.get('id', 'N/A')
                    gmail = properties.get('email', 'N/A')

                    # First Name
                    if properties.get('firstname'):
                        first_name = properties['firstname']
                    else:
                        first_name = gmail.split('@')[0].split('.')[0].capitalize() if gmail and '@' in gmail else 'N/A'

                    # Last Name
                    if properties.get('lastname'):
                        last_name = properties['lastname']
                    else:
                        parts = gmail.split('@')[0].split('.') if gmail and '@' in gmail else []
                        last_name = parts[1].capitalize() if len(parts) > 1 else first_name

                    # Other fields
                    job_title = properties.get('jobtitle', 'N/A')
                    known_since = properties.get('known_since_date', 'N/A')

                    # Normalize lifecycle stage
                    stage = (properties.get('lifecyclestage') or '').lower()
                    if stage in ['lead', 'marketingqualifiedlead']:
                        stage = 'Marketing'
                    elif stage in ['opportunity', 'salesqualifiedlead']:
                        stage = 'Sales'
                    elif stage == 'customer':
                        stage = 'Customer Support'
                    elif stage == 'other':
                        stage = 'Operations'
                    else:
                        stage = 'KYC'

                    # Company
                    company_name = first_name  # Default to first name
                    associations = contact.get('associations', {})
                    if 'companies' in associations and 'results' in associations['companies']:
                        company_id = associations['companies']['results'][0]['id']
                        company_response = requests.get(f"{companies_url}/{company_id}", headers=headers)
                        if company_response.status_code == 200:
                            company_data = company_response.json()
                            company_name = company_data.get('properties', {}).get('name', first_name)

                    # Append row
                    contacts_list.append([
                        contact_id, first_name, last_name, company_name,
                        job_title, stage, known_since, gmail
                    ])

                # Pagination
                after = contacts_data.get('paging', {}).get('next', {}).get('after')
                has_more = bool(after)
            else:
                print(f"❌ Failed to retrieve contacts: {response.status_code}")
                print("Response content:", response.text)
                break

        # Save to CSV
        df = pd.DataFrame(contacts_list, columns=[
            'Contact_ID', 'First Name', 'Last Name', 'Company',
            'Position', 'Stage', 'Known Since', 'Email'
        ])
        excel_filename = 'hubspot_contacts.csv'
        df.to_csv(excel_filename, index=False)
        print(f"✅ Contacts have been successfully saved to {excel_filename}")