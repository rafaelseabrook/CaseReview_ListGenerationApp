import os
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import quote
import msal
from openpyxl import Workbook

# Environment variables
CLIO_CLIENT_ID = os.getenv("CLIO_CLIENT_ID")
CLIO_CLIENT_SECRET = os.getenv("CLIO_CLIENT_SECRET")
CLIO_REFRESH_TOKEN = os.getenv("CLIO_REFRESH_TOKEN")
CLIO_REDIRECT_URI = os.getenv("CLIO_REDIRECT_URI")
CLIO_ACCESS_TOKEN = os.getenv("CLIO_ACCESS_TOKEN")
CLIO_EXPIRES_IN = float(os.getenv("CLIO_EXPIRES_IN", 0))

SHAREPOINT_CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
SHAREPOINT_CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")
SHAREPOINT_TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID")
SHAREPOINT_SITE_ID = os.getenv("SHAREPOINT_SITE_ID")
SHAREPOINT_DRIVE_ID = os.getenv("SHAREPOINT_DRIVE_ID")
SHAREPOINT_DOC_LIB = os.getenv("SHAREPOINT_DOC_LIB").strip('"')

API_VERSION = '4'
FIELD_NAMES = [
    'Matter Number', 'Client Name', 'CR ID', 'Net Trust Account Balance', 'Matter Stage',
    'Responsible Attorney', 'Main Paralegal', 'Client Notes', 'Initial Client Goals',
    'Initial Strategy', 'Has strategy changed Describe', 'Current action Items',
    'Hearings', 'Deadlines', 'DV situation description', 'Custody Visitation',
    'CS Add ons Extracurricular', 'Spousal Support', 'PDDs', 'Discovery',
    'Judgment Trial', 'Post Judgment', 'collection efforts'
]

# ==== CLIO API Authentication ====
def refresh_clio_token():
    token_url = 'https://app.clio.com/oauth/token'
    response = requests.post(token_url, data={
        'grant_type': 'refresh_token',
        'refresh_token': CLIO_REFRESH_TOKEN,
        'client_id': CLIO_CLIENT_ID,
        'client_secret': CLIO_CLIENT_SECRET
    })
    if response.status_code == 200:
        tokens = response.json()
        os.environ["CLIO_ACCESS_TOKEN"] = tokens['access_token']
        os.environ["CLIO_EXPIRES_IN"] = str(datetime.now().timestamp() + tokens['expires_in'])
        return tokens['access_token']
    else:
        raise Exception(f"Failed to refresh Clio token: {response.text}")

def get_clio_access_token():
    if datetime.now().timestamp() >= CLIO_EXPIRES_IN:
        return refresh_clio_token()
    return CLIO_ACCESS_TOKEN

# ==== SHAREPOINT Upload ====
def upload_to_sharepoint(file_path, file_name, folder):
    authority = f"https://login.microsoftonline.com/{SHAREPOINT_TENANT_ID}"
    scopes = ["https://graph.microsoft.com/.default"]
    app = msal.ConfidentialClientApplication(
        SHAREPOINT_CLIENT_ID, authority=authority, client_credential=SHAREPOINT_CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=scopes)
    if "access_token" not in result:
        raise Exception("Could not acquire token for SharePoint")

    headers = {"Authorization": f"Bearer {result['access_token']}", "Content-Type": "application/json"}

    now = datetime.now()
    path = f"{SHAREPOINT_DOC_LIB}/{now.year}/{now.strftime('%m %B %Y')}/{folder}"
    ensure_folder(path, headers)

    encoded_path = quote(f"{path}/{file_name}")
    upload_url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root:/{encoded_path}:/content"

    with open(file_path, "rb") as f:
        res = requests.put(upload_url, headers={"Authorization": f"Bearer {result['access_token']}"}, data=f)
        if res.status_code not in [200, 201]:
            raise Exception(f"Failed to upload file: {res.text}")
    print(f"✅ Uploaded {file_name} to SharePoint")


def ensure_folder(path, headers):
    segments = path.strip("/").split("/")
    parent_path = ""
    for segment in segments:
        full_path = f"{parent_path}/{segment}" if parent_path else segment
        url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root:/{full_path}"
        res = requests.get(url, headers=headers)
        if res.status_code == 404:
            create_url = (
                f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root:/{parent_path}:/children"
                if parent_path else
                f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root/children"
            )
            create_res = requests.post(create_url, headers=headers, json={
                "name": segment, "folder": {}, "@microsoft.graph.conflictBehavior": "replace"
            })
            create_res.raise_for_status()
        parent_path = full_path

# ==== CLIO Custom Field Fetch ====
def fetch_custom_fields():
    token = get_clio_access_token()
    url = f'https://app.clio.com/api/v{API_VERSION}/custom_fields.json'
    response = requests.get(url, headers={'Authorization': f'Bearer {token}'})
    response.raise_for_status()
    fields = response.json().get('data', [])
    return {f['name']: f['id'] for f in fields if isinstance(f, dict)}

def fetch_matters_with_custom_fields(field_ids):
    token = get_clio_access_token()
    matters = []
    page = 1
    while True:
        url = f'https://app.clio.com/api/v{API_VERSION}/matters.json?page={page}&status=open,pending&limit=200'
        res = requests.get(url, headers={'Authorization': f'Bearer {token}'})
        res.raise_for_status()
        page_data = res.json().get('data', [])
        if not page_data:
            break
        matters.extend(page_data)
        page += 1
    return matters

def extract_custom_data():
    fields = fetch_custom_fields()
    matters = fetch_matters_with_custom_fields(fields)
    data = []

    for m in matters:
        cfields = m.get('custom_field_values', [])
        custom_map = {cf['field_name']: cf.get('value') or cf.get('picklist_option', {}).get('option', '') for cf in cfields if isinstance(cf, dict)}
        row = {
            'Matter Number': m.get('number', ''),
            'Client Name': m.get('client', {}).get('name', ''),
            'Matter Stage': m.get('matter_stage', {}).get('name', ''),
            'Responsible Attorney': m.get('responsible_attorney', {}).get('name', ''),
            'Net Trust Account Balance': '',  # Optional to calculate
        }
        row.update({key: custom_map.get(key, '') for key in FIELD_NAMES if key not in row})
        data.append(row)

    df = pd.DataFrame(data)
    for col in FIELD_NAMES:
        if col not in df.columns:
            df[col] = ''
    return df[FIELD_NAMES]

# ==== MAIN ====
def main():
    df = extract_custom_data()
    for attorney, group in df.groupby("Responsible Attorney"):
        if not attorney:
            continue
        file_name = f"Case Review - {attorney}.xlsx"
        file_path = f"/tmp/{file_name}"
        group.to_excel(file_path, index=False)
        upload_to_sharepoint(file_path, file_name, attorney)
        os.remove(file_path)

if __name__ == '__main__':
    main()
