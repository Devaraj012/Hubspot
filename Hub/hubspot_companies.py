import requests
import pandas as pd
from dotenv import load_dotenv
import os

TOKEN=['IBT','T4U','SELVA','GREENESTEP']

for token in TOKEN:
    load_dotenv()
    api = os.getenv(token)
    URL=os.getenv("COMPANY_URL")

    API_KEY = api
    companies_url = URL

    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Content-Type': 'application/json'
    }

    companies_params = {
        'properties': 'name,domain,industry,phone,annualrevenue'
    }
    response = requests.get(companies_url, headers=headers, params=companies_params)

    if response.status_code == 200:
        companies_data = response.json()
        companies_list = []

        for company in companies_data.get('results', []):
            company_id = company.get('id', 'N/A')
            name = company['properties'].get('name', 'N/A')
            domain = company['properties'].get('domain', 'N/A')
            industry = company['properties'].get('industry', 'N/A')
            phone = company['properties'].get('phone', 'N/A')
            annual_revenue = company['properties'].get('annualrevenue', 'N/A')
            created_at = company['properties'].get('createdate', 'N/A')
            companies_list.append([company_id, name, domain, industry, phone, annual_revenue, created_at])

        df_companies = pd.DataFrame(companies_list, columns=['Company ID', 'Name', 'Domain', 'Industry', 'Phone', 'Annual Revenue', 'Created At'])

        excel_companies_filename = f'hubspot_{token}_companies.csv'
        df_companies.to_csv(excel_companies_filename, index=False)
        print(f"Companies have been successfully saved to {excel_companies_filename}")

    else:
        print(f"Failed to retrieve companies: {response.status_code}")
        print("Response content:", response.text)