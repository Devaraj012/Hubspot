import requests
import pandas as pd
from dotenv import load_dotenv
import os
import time
from datetime import datetime, timezone
from requests.exceptions import RequestException

# Load API key
load_dotenv()
API_KEY = os.getenv("VISHRUTHA")

headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

def make_request(url, method='GET', headers=None, data=None, retries=3, timeout=30):
    for attempt in range(retries):
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, timeout=timeout)
            else:
                response = requests.post(url, headers=headers, json=data, timeout=timeout)
            response.raise_for_status()
            return response
        except RequestException as e:
            print(f"Request failed: {e}")
            time.sleep(1)
    return None

def batch_read_objects(object_type, ids, properties):
    url = f"https://api.hubapi.com/crm/v3/objects/{object_type}/batch/read"
    payload = {
        "properties": properties,
        "inputs": [{"id": i} for i in ids]
    }
    response = make_request(url, method='POST', headers=headers, data=payload)
    result_map = {}
    if response:
        for obj in response.json().get('results', []):
            obj_id = obj.get('id')
            props = obj.get('properties', {})
            result_map[obj_id] = props
    return result_map

# ---- DATE RANGE FILTER ----
start_date = datetime(2025, 5, 6, tzinfo=timezone.utc)
end_date = datetime(2025, 8, 14, tzinfo=timezone.utc)

print("🔍 Fetching and filtering tasks by created date...")

tasks_url = 'https://api.hubapi.com/crm/v3/objects/tasks'
params = {
    'properties': 'hs_task_subject,hs_task_status,hs_timestamp,hs_createdate,hs_task_type',
    'limit': 100
}

filtered_tasks = []
after = None
has_more = True

# Step 1: Fetch and filter by created date only
while has_more:
    if after:
        params['after'] = after

    response = requests.get(tasks_url, headers=headers, params=params, timeout=30)
    if response.status_code != 200:
        print("❌ Failed to fetch tasks:", response.text)
        break

    data = response.json()
    task_list = data.get('results', [])

    for task in task_list:
        created_at = task.get('properties', {}).get('hs_createdate')
        if created_at:
            created_dt = pd.to_datetime(created_at, utc=True, errors='coerce')
            if pd.notnull(created_dt) and start_date <= created_dt.to_pydatetime() <= end_date:
                filtered_tasks.append(task)

    after = data.get('paging', {}).get('next', {}).get('after')
    has_more = bool(after)

# 🛑 Exit if no tasks found
if not filtered_tasks:
    print("⚠️ No tasks found within the specified date range.")
    exit()

print(f"✅ Found {len(filtered_tasks)} tasks in date range. Proceeding...")

# Step 2: Process filtered tasks
all_tasks = []
contact_assoc = {}
company_assoc = {}

for task in filtered_tasks:
    task_id = task['id']
    assoc_contacts_url = f"https://api.hubapi.com/crm/v3/objects/tasks/{task_id}/associations/contacts"
    assoc_companies_url = f"https://api.hubapi.com/crm/v3/objects/tasks/{task_id}/associations/companies"

    contact_resp = make_request(assoc_contacts_url, headers=headers)
    company_resp = make_request(assoc_companies_url, headers=headers)

    contact_assoc[task_id] = [res['id'] for res in contact_resp.json().get('results', [])] if contact_resp else []
    company_assoc[task_id] = [res['id'] for res in company_resp.json().get('results', [])] if company_resp else []

# Step 3: Get contact & company details
all_contact_ids = list({cid for ids in contact_assoc.values() for cid in ids})
all_company_ids = list({cid for ids in company_assoc.values() for cid in ids})

contact_props = batch_read_objects("contacts", all_contact_ids, ["firstname", "lastname", "lifecyclestage"])
company_props = batch_read_objects("companies", all_company_ids, ["name"])

# Step 4: Final processing
for task in filtered_tasks:
    props = task.get('properties', {})
    task_id = task.get('id', 'N/A')
    status = props.get('hs_task_status', 'N/A')
    due_date = props.get('hs_timestamp', 'N/A')
    created_at = props.get('hs_createdate', 'N/A')
    task_type = props.get('hs_task_type', 'N/A')

    stage = {
        "TODO": "Activities",
        "CALL": "Calls",
        "EMAIL": "Emails"
    }.get(task_type, "Unknown")

    contacts = contact_assoc.get(task_id, [])
    companies = company_assoc.get(task_id, [])
    company_name = company_props.get(companies[0], {}).get('name', 'Unknown') if companies else 'Unknown'

    if not contacts:
        all_tasks.append(['N/A', 'Unknown', 'Unknown', 'Created Date', created_at, status, task_type, stage, company_name])
        all_tasks.append(['N/A', 'Unknown', 'Unknown', 'Due Date', due_date, status, task_type, stage, company_name])
    else:
        for contact_id in contacts:
            cprops = contact_props.get(contact_id, {})
            full_name = f"{cprops.get('firstname', '')} {cprops.get('lastname', '')}".strip() or 'Unknown'
            lifecycle = cprops.get('lifecyclestage', 'Unknown')
            all_tasks.append([int(contact_id), full_name, lifecycle, 'Created Date', created_at, status, task_type, stage, company_name])
            all_tasks.append([int(contact_id), full_name, lifecycle, 'Due Date', due_date, status, task_type, stage, company_name])

# Step 5: Lifecycle → Department mapping
for task in all_tasks:
    lifecycle = task[2].lower()
    task[7] = {
        'lead': 'Marketing',
        'marketing qualified lead': 'Marketing',
        'opportunity': 'Sales',
        'sales qualified lead': 'Sales',
        'customer': 'Customer Support',
        'other': 'Operations'
    }.get(lifecycle, task[7])

# Step 6: Status + type mapping
for task in all_tasks:
    task_status = task[5]
    task_type = task[6]
    if task_status == 'COMPLETED':
        task[5] = 'Task Completed'
    elif task_status == 'NOT_STARTED':
        task[5] = 'Task Created'
    if task_type == 'EMAIL':
        task[5] = 'Email Sent'
    elif task_type == 'CALL':
        task[5] = 'Call Made'

# Step 7: Final export
df = pd.DataFrame(all_tasks, columns=[
    'Contact_ID', 'Contact', 'Lifecycle_Stage', 'date_type', 'Event Date', 'Status', 'task_type', 'Stage', 'Company'
])

df.to_csv('hubspot_tasks.csv', index=False)
print("✅ All tasks saved to: hubspot_tasks.csv")
