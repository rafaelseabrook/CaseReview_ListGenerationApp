import os
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import quote
import msal
from openpyxl import Workbook

# === ENVIRONMENT VARIABLES ===
CLIO_CLIENT_ID = os.getenv("CLIO_CLIENT_ID")
CLIO_CLIENT_SECRET = os.getenv("CLIO_CLIENT_SECRET")
CLIO_REFRESH_TOKEN = os.getenv("CLIO_REFRESH_TOKEN")
CLIO_ACCESS_TOKEN = os.getenv("CLIO_ACCESS_TOKEN")
CLIO_EXPIRES_IN = float(os.getenv("CLIO_EXPIRES_IN", 0))
CLIO_REDIRECT_URI = os.getenv("CLIO_REDIRECT_URI")

SHAREPOINT_TENANT_ID = os.getenv("GRAPH_TENANT_ID")
SHAREPOINT_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
SHAREPOINT_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
SHAREPOINT_SITE_ID = os.getenv("SHAREPOINT_SITE_ID")
SHAREPOINT_DRIVE_ID = os.getenv("SHAREPOINT_DRIVE_ID")
SHAREPOINT_DOC_LIB = "General Management/Global Case Review Lists"

API_VERSION = '4'
OUTPUT_FIELDS = [
    "Matter Number","Client Name","CR ID","Net Trust Account Balance","Matter Stage",
    "Responsible Attorney","Main Paralegal","Client Notes","Initial Client Goals","Initial Strategy",
    "Has strategy changed Describe","Current action Items","Hearings","Deadlines","DV situation description",
    "Custody Visitation","CS Add ons Extracurricular","Spousal Support","PDDs","Discovery",
    "Judgment Trial","Post Judgment","collection efforts"
]

# === CLIO AUTH ===
def refresh_clio_token():
    resp = requests.post("https://app.clio.com/oauth/token", data={
        "grant_type": "refresh_token",
        "refresh_token": CLIO_REFRESH_TOKEN,
        "client_id": CLIO_CLIENT_ID,
        "client_secret": CLIO_CLIENT_SECRET
    })
    resp.raise_for_status()
    tok = resp.json()
    os.environ["CLIO_ACCESS_TOKEN"] = tok["access_token"]
    os.environ["CLIO_EXPIRES_IN"] = str(datetime.now().timestamp() + tok["expires_in"])
    return tok["access_token"]

def get_clio_token():
    if datetime.now().timestamp() >= CLIO_EXPIRES_IN:
        return refresh_clio_token()
    return CLIO_ACCESS_TOKEN

# === FETCH CLIO DATA ===
def fetch_custom_fields():
    token = get_clio_token()
    resp = requests.get(f"https://app.clio.com/api/v{API_VERSION}/custom_fields.json",
        headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    return {f.get("field_name", f.get("name")): f["id"] for f in resp.json().get("data", []) if isinstance(f, dict)}

def fetch_open_matters():
    token = get_clio_token()
    matters = []
    page = 1
    while True:
        resp = requests.get(f"https://app.clio.com/api/v{API_VERSION}/matters.json",
            headers={"Authorization": f"Bearer {token}"},
            params={"page": page, "limit": 200, "status": "open",
                    "fields": "id,number,client{name},matter_stage{name},responsible_attorney{name},custom_field_values"})
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            break
        matters.extend(data)
        page += 1
    return matters

# === SHAREPOINT Upload ===
def ensure_folder(path, headers):
    segments = path.strip("/").split("/")
    parent = ""
    for seg in segments:
        full = parent + "/" + seg if parent else seg
        url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root:/{full}"
        r = requests.get(url, headers=headers)
        if r.status_code == 404:
            create = requests.post(f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root:/{parent}:/children",
                                   headers=headers,
                                   json={"name": seg, "folder": {}, "@microsoft.graph.conflictBehavior": "replace"})
            create.raise_for_status()
        parent = full

def upload_file(file_path, file_name, folder_path):
    authority = f"https://login.microsoftonline.com/{SHAREPOINT_TENANT_ID}"
    app = msal.ConfidentialClientApplication(SHAREPOINT_CLIENT_ID, authority=authority, client_credential=SHAREPOINT_CLIENT_SECRET)
    tok = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in tok:
        raise Exception("Failed to get Graph token")
    headers = {"Authorization": f"Bearer {tok['access_token']}"}

    ensure_folder(folder_path, headers)
    upload_url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root:/{quote(folder_path + '/' + file_name)}:/content"
    with open(file_path, "rb") as f:
        res = requests.put(upload_url, headers=headers, data=f)
    if res.status_code not in (200, 201):
        raise Exception(f"Upload error: {res.text}")

# === DATA PROCESSING ===
def extract_custom_data():
    fields = fetch_custom_fields()
    matters = fetch_open_matters()
    file_path = "/tmp/case_review.xlsx"

    wb = Workbook()
    ws = wb.active
    ws.title = "Case Review"

    ws.append(OUTPUT_FIELDS)

    for m in matters:
        cfields = m.get("custom_field_values", [])
        custom_map = {cf.get("field_name"): cf.get("value") or cf.get("picklist_option", {}).get("option", "") for cf in cfields if isinstance(cf, dict)}
        row = {
            "Matter Number": m.get("number", ""),
            "Client Name": m.get("client", {}).get("name", ""),
            "Matter Stage": m.get("matter_stage", {}).get("name", ""),
            "Responsible Attorney": m.get("responsible_attorney", {}).get("name", "")
        }
        row.update({key: custom_map.get(key, '') for key in OUTPUT_FIELDS if key not in row})
        row["Net Trust Account Balance"] = 0
        ws.append([row.get(key, '') for key in OUTPUT_FIELDS])

    wb.save(file_path)
    return file_path

def main():
    file_path = extract_custom_data()
    file_date = datetime.now().strftime("%y%m%d")
    file_name = f"{file_date}.Seabrook's Case Review List.xlsx"
    upload_file(file_path, file_name, SHAREPOINT_DOC_LIB)
    os.remove(file_path)

if __name__ == '__main__':
    main()
