import requests
import json
import os
from dotenv import load_dotenv
load_dotenv()

auth=os.getenv('TOKEN')

url = "https://greenestep.giftai.co.in/api/v1/csv"

headers = {
  'Cookie': 'ticket=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InNoYXJlX2RldmFyYWpAdGVjaGNvYWNoNHUuY29tIiwiaWQiOjMsInR5cGUiOiJBRE1JTiIsImlhdCI6MTc0ODk1Nzk4MSwiZXhwIjoxNzQ5MDAxMTgxfQ.LOHabRG3foQcHtYXmPxNjFJbJ7_y0XaesZyZA1fNajM',
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