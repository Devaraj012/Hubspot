import requests
import json
import os
from dotenv import load_dotenv
load_dotenv()

auth=os.getenv('TOKEN')

url = "https://greenestep.giftai.co.in/api/v1/csv"

headers = {
  'Cookie': 'ticket=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImRldmFyYWpAaWJhY3VzdGVjaGxhYnMuaW4iLCJpZCI6NCwidHlwZSI6IkFETUlOIiwiaWF0IjoxNzQxMTYwMzUxLCJleHAiOjE3NDEyMDM1NTF9.-mbQiZ4IFCMhXzDnsRNiKRvEK-K5ps6fheXiPKVBEo4',
  'Content-Type': 'application/json',
  'Authorization': f'Bearer {auth}'
}
Collections=[
  {
  "collection_description": "Hubspot",
  "collection_name": "Task_data",
  "collection_permission": "READ",
  "collection_type": "PUBLIC"
  }]

for collection in Collections:
  payload = json.dumps(collection)
  response = requests.request("POST", url, headers=headers, data=payload)
  print(response.text)