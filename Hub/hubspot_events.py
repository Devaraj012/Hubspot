import os
import requests
import pandas as pd
import time
from dotenv import load_dotenv
from requests.exceptions import RequestException

load_dotenv()

# Load API key from .env file
TOKEN=['SELVA','VISHRUTHA']

for token in TOKEN:
    API_KEY = os.getenv(token)
    HEADERS = {"Authorization": f"Bearer {API_KEY}"}

    # Retry wrapper with exponential backoff
    def make_request(url, params=None, max_retries=5):
        retries = 0
        backoff = 2
        while retries < max_retries:
            try:
                response = requests.get(url, headers=HEADERS, params=params)
                response.raise_for_status()
                return response
            except RequestException as e:
                print(f"⚠️ Request failed: {e}. Retrying in {backoff} seconds...")
                time.sleep(backoff)
                backoff *= 2
                retries += 1
        print(f"❌ Failed after {max_retries} retries: {url}")
        return None

    # Paginated HubSpot data fetcher
    def get_data_from_hubspot(base_url, properties=""):
        all_results = []
        params = {"limit": 100}
        if properties:
            params["properties"] = properties

        response = make_request(base_url, params)
        if response is None:
            return []

        data = response.json()
        all_results.extend(data.get("results", []))
        next_link = data.get("paging", {}).get("next", {}).get("link")

        while next_link:
            response = make_request(next_link)
            if response is None:
                break
            data = response.json()
            all_results.extend(data.get("results", []))
            next_link = data.get("paging", {}).get("next", {}).get("link")

        print(f"→ Total items fetched: {len(all_results)}")
        return all_results

    # 1. Fetch contacts
    print("📦 Fetching contacts...")
    contacts = get_data_from_hubspot(
        "https://api.hubapi.com/crm/v3/objects/contacts",
        "email,firstname,lastname,createdate,known_since_date"
    )

    # 2. Fetch company associations
    print("🔗 Matching contacts to companies...")
    contact_company_map = {}
    for contact in contacts:
        assoc_url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact['id']}/associations/companies"
        res = make_request(assoc_url)
        if res:
            results = res.json().get("results", [])
            if results:
                contact_company_map[contact["id"]] = results[0]["id"]

    # 3. Fetch company names
    print("🏢 Fetching company details...")
    unique_company_ids = list(set(contact_company_map.values()))
    company_map = {}
    for company_id in unique_company_ids:
        url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
        res = make_request(url, {"properties": "name"})
        if res:
            company_map[company_id] = res.json().get("properties", {}).get("name", "")

    # 4. Build contact list
    contact_list = []

    for contact in contacts:
        contact_id = contact["id"]
        company_id = contact_company_map.get(contact_id)
        company_name = company_map.get(company_id, "") if company_id else ""

        # Extract basic fields safely
        properties = contact.get("properties", {})
        email = properties.get("email", "")
        first_name = str(properties.get("firstname", "") or "").strip()
        last_name = str(properties.get("lastname", "") or "").strip()

        # Fallback name logic using email if names are missing
        if not first_name and not last_name:
            parts = email.split('@')[0].split('.') if email and '@' in email else []
            first_name = parts[0].capitalize() if len(parts) >= 1 else "Unknown"
            last_name = parts[1].capitalize() if len(parts) >= 2 else first_name
        elif not last_name:
            last_name = first_name

        # Append formatted contact record
        contact_list.append({
            "contact_id": contact_id,
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "contact_created": properties.get("createdate", ""),
            "known_since": properties.get("known_since_date", ""),
            "company_name": company_name
        })

    # 5. Fetch tickets
    print("📦 Fetching tickets...")
    tickets = get_data_from_hubspot(
        "https://api.hubapi.com/crm/v3/objects/tickets",
        "createdate,closed_date,hs_pipeline_stage"
    )

    ticket_list = []
    for ticket in tickets:
        ticket_list.append({
            "ticket_id": ticket["id"],
            "ticket_created": ticket["properties"].get("createdate", ""),
            "ticket_closed": ticket["properties"].get("closed_date", ""),
            "stage_id": ticket["properties"].get("hs_pipeline_stage", "")
        })

    # 6. Fetch ticket stage names
    print("📦 Getting ticket stage names...")
    stage_map = {}
    stage_url = "https://api.hubapi.com/crm/v3/pipelines/tickets"
    stage_res = make_request(stage_url)
    if stage_res:
        for pipeline in stage_res.json().get("results", []):
            for stage in pipeline.get("stages", []):
                stage_map[stage["id"]] = stage["label"]

    for ticket in ticket_list:
        ticket["stage_name"] = stage_map.get(ticket["stage_id"], ticket["stage_id"])

    # 7. Fetch ticket-contact associations
    print("🔗 Matching tickets to contacts...")
    associations = []
    for ticket in ticket_list:
        assoc_url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket['ticket_id']}/associations/contacts"
        res = make_request(assoc_url)
        if res:
            for assoc in res.json().get("results", []):
                associations.append({
                    "ticket_id": ticket["ticket_id"],
                    "contact_id": assoc["id"]
                })

    # 8. Build event list
    print("🛠️ Building event list...")
    event_list = []
    for contact in contact_list:
        full_name = f"{contact['first_name']} {contact['last_name']}".strip()
        company = contact.get("company_name", "")

        # Contact created
        event_list.append({
            "Contact": full_name,
            "Company": company,
            "Event Date": contact["contact_created"],
            "Stage": "KYC",
            "Status": "Contact Created",
            "create_date": contact["contact_created"]
        })

        # Known since
        if contact["known_since"]:
            event_list.append({
                "Contact": full_name,
                "Company": company,
                "Event Date": contact["known_since"],
                "Stage": "KYC",
                "Status": "First Contact",
                "create_date": contact["contact_created"]
            })

    # 9. Ticket events
    for assoc in associations:
        contact = next((c for c in contact_list if c["contact_id"] == assoc["contact_id"]), None)
        ticket = next((t for t in ticket_list if t["ticket_id"] == assoc["ticket_id"]), None)
        
        if contact and ticket:
            full_name = f"{contact['first_name']} {contact['last_name']}".strip()
            company = contact.get("company_name", "")

            if ticket["ticket_created"]:
                event_list.append({
                    "Contact": full_name,
                    "Company": company,
                    "Stage": "Support",
                    "Status": "Ticket Created",
                    "Event Date": ticket["ticket_created"],
                    "create_date": ticket["ticket_created"]
                })

            if ticket["ticket_closed"]:
                event_list.append({
                    "Contact": full_name,
                    "Company": company,
                    "Stage": "Support",
                    "Status": "Ticket Closed",
                    "Event Date": ticket["ticket_closed"],
                    "create_date": ticket["ticket_created"]
                })

    # 10. Save to CSV
    print("💾 Saving to CSV...")
    df = pd.DataFrame(event_list)
    df = df.sort_values("create_date")
    df["Event Date"] = pd.to_datetime(df["Event Date"], utc=True, errors="coerce").dt.strftime("%m-%d-%Y")
    df = df[["Contact", "Company", "Stage", "Status", "Event Date", "create_date"]]
    df.to_csv(f"hubspot_events_{token}.csv", index=False)
    print(f"✅ Done! File saved: hubspot_events_{token}.csv")
