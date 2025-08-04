import os
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime
from msal import ConfidentialClientApplication

# === ENVIRONMENT VARIABLES ===
TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID")
CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")
SITE_ID = os.getenv("SHAREPOINT_SITE_ID")
DRIVE_ID = os.getenv("SHAREPOINT_DRIVE_ID")
CLIO_ACCESS_TOKEN = os.getenv("CLIO_ACCESS_TOKEN")

# === CONSTANTS ===
ATTORNEY_NAMES = [
    "Darling, Craig",
    "Huang, Lily",
    "Voorhees, Elizabeth",
    "Parker, Gabriella"
]
FOLDER_PATH_PREFIX = "Attorneys and Paralegals/Attorney Case Lists"
TARGET_FIELDS = [
    "Client Notes", "Initial Client Goals", "Initial Strategy", "Has strategy changed Describe",
    "Current action Items", "Hearings", "Deadlines", "DV situation description",
    "Custody Visitation", "CS Add ons Extracurricular", "Spousal Support", "PDDs",
    "Discovery", "Judgment Trial", "Post Judgment", "collection efforts"
]

# === GRAPH AUTH ===
def get_graph_token():
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=authority,
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise Exception(f"Graph token error: {result.get('error_description')}")
    return result["access_token"]

# === FETCH LATEST FILE ===
def get_latest_excel_from_folder(graph_token, folder_path):
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{folder_path}:/children"
    headers = {"Authorization": f"Bearer {graph_token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    files = response.json().get("value", [])
    excel_files = [f for f in files if f['name'].endswith('.xlsx')]
    if not excel_files:
        raise Exception(f"No Excel files in folder: {folder_path}")
    latest_file = sorted(excel_files, key=lambda x: x['lastModifiedDateTime'], reverse=True)[0]
    download_url = latest_file['@microsoft.graph.downloadUrl']
    content = requests.get(download_url)
    return BytesIO(content.content)

# === FETCH CUSTOM FIELD DEFINITIONS ===
def get_clio_custom_fields():
    headers = {"Authorization": f"Bearer {CLIO_ACCESS_TOKEN}"}
    url = "https://app.clio.com/api/v4/custom_fields.json?limit=200"
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    records = res.json().get("data", [])
    return {item["name"]: item["id"] for item in records if item.get("name") in TARGET_FIELDS}

# === FIND MATTER BY NUMBER ===
def find_matter_id_by_number(matter_number):
    headers = {"Authorization": f"Bearer {CLIO_ACCESS_TOKEN}"}
    url = "https://app.clio.com/api/v4/matters.json"
    params = {"query": matter_number}
    res = requests.get(url, headers=headers, params=params)
    res.raise_for_status()
    matches = res.json().get("data", [])
    for m in matches:
        if str(m.get("number")) == str(matter_number):
            return m["id"]
    return None

# === GET FIELD INSTANCE IDS FOR A MATTER ===
def get_field_instances(matter_id):
    headers = {"Authorization": f"Bearer {CLIO_ACCESS_TOKEN}"}
    url = f"https://app.clio.com/api/v4/matters/{matter_id}.json?fields=custom_field_values"
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    values = res.json().get("data", {}).get("custom_field_values", [])
    return {v['custom_field']['id']: v['id'] for v in values if 'custom_field' in v and 'id' in v}

# === UPDATE CUSTOM FIELDS ===
def update_custom_fields_for_matter(matter_id, updates):
    headers = {
        "Authorization": f"Bearer {CLIO_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    instance_map = get_field_instances(matter_id)
    payload = {
        "data": {
            "custom_field_values": [
                {
                    "id": instance_map[field_id],
                    "custom_field": {"id": field_id},
                    "value": value
                } for field_id, value in updates.items() if field_id in instance_map
            ]
        }
    }
    url = f"https://app.clio.com/api/v4/matters/{matter_id}.json"
    res = requests.patch(url, headers=headers, json=payload)
    if res.status_code not in [200, 201]:
        print(f"Failed to update matter {matter_id}: {res.text}")

# === MAIN PROCESS ===
def process_attorney_case_files():
    token = get_graph_token()
    field_map = get_clio_custom_fields()
    for name in ATTORNEY_NAMES:
        folder_path = f"{FOLDER_PATH_PREFIX}/{name}"
        try:
            excel_io = get_latest_excel_from_folder(token, folder_path)
            df = pd.read_excel(excel_io)
            for _, row in df.iterrows():
                matter_number = row.get("Matter Number")
                if pd.isna(matter_number):
                    continue
                matter_id = find_matter_id_by_number(matter_number)
                if not matter_id:
                    print(f"Matter not found for number {matter_number}")
                    continue
                updates = {}
                for field in TARGET_FIELDS:
                    val = row.get(field)
                    if pd.notna(val) and field in field_map:
                        updates[field_map[field]] = str(val)
                update_custom_fields_for_matter(matter_id, updates)
                print(f"Updated matter {matter_number} with custom fields.")
        except Exception as e:
            print(f"Error processing {name}: {e}")

if __name__ == "__main__":
    process_attorney_case_files()

if __name__ == "__main__":
    process_attorney_case_files()
