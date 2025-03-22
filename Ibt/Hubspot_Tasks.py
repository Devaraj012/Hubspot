import hubspot
import pandas as pd
from pprint import pprint
from datetime import datetime
from hubspot.crm.objects.tasks import ApiException
from hubspot.crm.owners import OwnersApi
from dotenv import load_dotenv
import os

load_dotenv()
access_token = os.getenv("HUBSPOT_ACCESS_TOKEN")

# Get HubSpot Data
client = hubspot.Client.create(access_token=access_token)

def get_owner_names():
    """Fetch and map owner IDs to names."""
    try:
        owners_api = client.crm.owners.owners_api
        all_owners = owners_api.get_page().results
        return {owner.id: owner.first_name + " " + owner.last_name for owner in all_owners if owner.first_name}
    except ApiException as e:
        print(f"Exception when calling OwnersApi->get_page: {e}")
        return {}

try:
    properties = ['hubspot_owner_id', 'hs_createdate']
    all_tasks = []
    after = None  # For pagination

    owner_mapping = get_owner_names()

    while True:
        api_response = client.crm.objects.tasks.basic_api.get_page(
            limit=100, 
            archived=False,
            properties=properties,
            after=after  # Fetch next page
        )

        for task in api_response.results:
            task_data = task.properties.copy()
            task_data["task_id"] = task.id  
            task_data.pop("hs_lastmodifieddate", None)
            task_data.pop("hs_object_id", None)

            if "hs_createdate" in task_data:
                hs_createdate = datetime.strptime(task_data["hs_createdate"][:19], "%Y-%m-%dT%H:%M:%S")
                task_data["hs_createdate"] = hs_createdate.strftime("%m-%d-%Y")

            task_data["assigned_to"] = owner_mapping.get(task_data.get("hubspot_owner_id"), "Unknown")
            task_data["total_tasks"] = 1  
            task_data.pop("hubspot_owner_id", None)
            task_data.pop("task_id",None)

            all_tasks.append(task_data)

        # Check if there's another page
        after = api_response.paging.next.after if api_response.paging and api_response.paging.next else None
        if not after:
            break

    df = pd.DataFrame(all_tasks)
    column_order = ["assigned_to", "total_tasks", "hs_createdate"] + [col for col in df.columns if col not in ["assigned_to", "total_tasks", "hs_createdate"]]
    df = df[column_order]

    name = 'task.csv'
    df.to_csv(name, index=False)
    print(f"Exported {len(df)} tasks as exported")

except ApiException as e:
    print(f"Exception when calling basic_api->get_page: {e}")



#GIFT Upload Data
    
import requests

url = "https://greenestep.giftai.co.in/api/v1/csv/upload?d_type=none&"

payload = {'collection_id': '102',
'type': 'Replace',
'fieldMapped': 'Object'}

files=[
  ('csvFile',('task.csv',open(r'C:\Users\devar\Documents\Code\Hubspot\Ibt\task.csv','rb'),'text/csv'))
]

headers = {
  'Cookie': 'ticket=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImRldmFyYWpAaWJhY3VzdGVjaGxhYnMuaW4iLCJpZCI6NCwidHlwZSI6IkFETUlOIiwiaWF0IjoxNzQyNTM4Mzg0LCJleHAiOjE3NDI1ODE1ODR9.M2FXM5VskT1T7VHDouULwiVfOTnlsj5cpcyu0odrvKc'
}

response = requests.request("POST", url, headers=headers, data=payload, files=files)

print(response.text)
