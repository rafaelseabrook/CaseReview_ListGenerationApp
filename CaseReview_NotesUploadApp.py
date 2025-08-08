import os
import time
import re
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import quote
import msal
from openpyxl import Workbook

# ==============================
# Environment / Config
# ==============================
API_VERSION = "4"
CLIO_BASE = os.getenv("CLIO_BASE", "https://app.clio.com")
CLIO_API = f"{CLIO_BASE}/api/v{API_VERSION}"
CLIO_TOKEN_URL = f"{CLIO_BASE}/oauth/token"

# Clio OAuth (env should be pre-seeded on Render)
CLIO_CLIENT_ID = os.getenv("CLIO_CLIENT_ID")
CLIO_CLIENT_SECRET = os.getenv("CLIO_CLIENT_SECRET")
CLIO_REFRESH_TOKEN = os.getenv("CLIO_REFRESH_TOKEN")
CLIO_ACCESS_TOKEN = os.getenv("CLIO_ACCESS_TOKEN")
CLIO_EXPIRES_IN = float(os.getenv("CLIO_EXPIRES_IN", "0"))  # epoch seconds
CLIO_REDIRECT_URI = os.getenv("CLIO_REDIRECT_URI")

# SharePoint / Graph
SHAREPOINT_TENANT_ID = os.getenv("GRAPH_TENANT_ID")
SHAREPOINT_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
SHAREPOINT_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
SHAREPOINT_SITE_ID = os.getenv("SHAREPOINT_SITE_ID")
SHAREPOINT_DRIVE_ID = os.getenv("SHAREPOINT_DRIVE_ID")
SHAREPOINT_DOC_LIB = os.getenv("SHAREPOINT_DOC_LIB", "General Management/Global Case Review Lists")

# Report columns (unchanged for this report)
OUTPUT_FIELDS = [
    "Matter Number","Client Name","CR ID","Net Trust Account Balance","Matter Stage",
    "Responsible Attorney","Main Paralegal","Client Notes","Initial Client Goals","Initial Strategy",
    "Has strategy changed Describe","Current action Items","Hearings","Deadlines","DV situation description",
    "Custody Visitation","CS Add ons Extracurricular","Spousal Support","PDDs","Discovery",
    "Judgment Trial","Post Judgment","collection efforts"
]

# Rate limiting controls
PAGE_LIMIT = 50                 # Clio hard max
GLOBAL_MIN_SLEEP = float(os.getenv("CLIO_GLOBAL_MIN_SLEEP_SEC", "1.25"))

session = requests.Session()
session.headers.update({"Accept": "application/json"})

# ==============================
# Auth Helpers (env-backed)
# ==============================
def save_tokens_env(tokens: dict):
    os.environ["CLIO_ACCESS_TOKEN"] = tokens["access_token"]
    os.environ["CLIO_EXPIRES_IN"] = str(datetime.now().timestamp() + tokens["expires_in"])

def get_clio_token() -> str:
    global CLIO_ACCESS_TOKEN, CLIO_EXPIRES_IN
    now = datetime.now().timestamp()
    if not CLIO_ACCESS_TOKEN or now >= CLIO_EXPIRES_IN:
        return refresh_clio_token()
    return CLIO_ACCESS_TOKEN

def refresh_clio_token() -> str:
    global CLIO_ACCESS_TOKEN, CLIO_EXPIRES_IN
    if not (CLIO_CLIENT_ID and CLIO_CLIENT_SECRET and CLIO_REFRESH_TOKEN):
        raise RuntimeError("Missing CLIO_CLIENT_ID/CLIO_CLIENT_SECRET/CLIO_REFRESH_TOKEN.")
    resp = session.post(CLIO_TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": CLIO_REFRESH_TOKEN,
        "client_id": CLIO_CLIENT_ID,
        "client_secret": CLIO_CLIENT_SECRET
    }, timeout=45)
    resp.raise_for_status()
    tok = resp.json()
    CLIO_ACCESS_TOKEN = tok["access_token"]
    CLIO_EXPIRES_IN = datetime.now().timestamp() + tok["expires_in"]
    save_tokens_env(tok)
    return CLIO_ACCESS_TOKEN

def ensure_auth_header():
    token = get_clio_token()
    session.headers["Authorization"] = f"Bearer {token}"

# ==============================
# Backoff / Request Wrapper
# ==============================
def _sleep_with_floor(start_ts: float, retry_after: int | None = None):
    elapsed = time.time() - start_ts
    base_wait = max(0, GLOBAL_MIN_SLEEP - elapsed)
    wait = max(base_wait, retry_after or 0)
    if wait > 0:
        time.sleep(wait)

def _request(method: str, url: str, **kwargs) -> requests.Response:
    ensure_auth_header()
    max_tries = kwargs.pop("max_tries", 7)
    backoff = 1
    for _ in range(max_tries):
        t0 = time.time()
        resp = session.request(method, url, timeout=45, **kwargs)

        # Token expiry / auth issues -> refresh once
        if resp.status_code == 401:
            try:
                refresh_clio_token()
            except Exception as e:
                _sleep_with_floor(t0)
                raise
            _sleep_with_floor(t0)
            continue

        # Rate limit
        if resp.status_code == 429:
            ra = 30
            if resp.headers.get("Retry-After"):
                try: ra = int(resp.headers["Retry-After"])
                except ValueError: ra = 30
            else:
                try:
                    msg = (resp.json() or {}).get("error", {}).get("message", "")
                    m = re.search(r"Retry in (\d+)", msg)
                    if m: ra = int(m.group(1))
                except Exception:
                    pass
            print(f"[429] Rate limited. Waiting {ra}s …")
            _sleep_with_floor(t0, retry_after=ra)
            continue

        # Server errors
        if 500 <= resp.status_code < 600:
            print(f"[{resp.status_code}] Server error. Retrying in {backoff}s …")
            _sleep_with_floor(t0, retry_after=backoff)
            backoff = min(backoff * 2, 60)
            continue

        _sleep_with_floor(t0)
        return resp
    return resp

# ==============================
# Generic Clio fetch (limit=50 + page_token)
# ==============================
def fetch_data(url: str, params: dict):
    params = dict(params or {})
    params["limit"] = PAGE_LIMIT
    all_rows = []
    seen_tokens = set()
    page_token = None

    while True:
        if page_token:
            params["page_token"] = page_token
        resp = _request("GET", url, params=params)
        print(f"GET {url} params={params} → {resp.status_code}")
        if resp.status_code != 200:
            print(f"Failed: {resp.status_code} {resp.text[:200]}")
            break

        body = resp.json() or {}
        rows = body.get("data", [])
        meta = body.get("meta", {}) if isinstance(body, dict) else {}
        next_token = meta.get("next_page_token") or meta.get("next_token")

        all_rows.extend([r for r in rows if isinstance(r, dict)])

        if next_token and next_token not in seen_tokens:
            seen_tokens.add(next_token)
            page_token = next_token
            continue

        # stop on short page or no token
        if len(rows) < PAGE_LIMIT or not next_token:
            break

    return all_rows

# ==============================
# Clio data (custom fields + matters)
# ==============================
def fetch_custom_fields():
    url = f"{CLIO_API}/custom_fields.json"
    params = {"fields": "id,name,field_type,picklist_options"}
    rows = fetch_data(url, params)
    # Return a name->id map (handle both 'name' and legacy 'field_name')
    out = {}
    for f in rows:
        key = f.get("name") or f.get("field_name")
        if key and "id" in f:
            out[key] = f["id"]
    return out

def fetch_open_matters():
    url = f"{CLIO_API}/matters.json"
    # include client id+name in case you ever need ID-joins later
    params = {
        "status": "open,pending",
        "fields": (
            "id,number,display_number,"
            "client{id,name},"
            "matter_stage{name},"
            "responsible_attorney{name},"
            "custom_field_values{id,field_name,field_type,value,picklist_option}"
        )
    }
    return fetch_data(url, params)

# ==============================
# SharePoint upload (Graph)
# ==============================
def ensure_folder(path: str, headers: dict):
    segments = path.strip("/").split("/")
    parent = ""
    for seg in segments:
        full = f"{parent}/{seg}" if parent else seg
        url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root:/{full}"
        r = requests.get(url, headers=headers)
        if r.status_code == 404:
            if parent:
                create_url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root:/{parent}:/children"
            else:
                create_url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root/children"
            create = requests.post(create_url, headers=headers, json={
                "name": seg,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "replace"
            })
            create.raise_for_status()
        parent = full

def upload_file(file_path: str, file_name: str, folder_path: str):
    authority = f"https://login.microsoftonline.com/{SHAREPOINT_TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        SHAREPOINT_CLIENT_ID,
        authority=authority,
        client_credential=SHAREPOINT_CLIENT_SECRET
    )
    tok = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in tok:
        raise Exception(f"Failed to get Graph token: {tok.get('error_description')}")

    headers = {"Authorization": f"Bearer {tok['access_token']}", "Content-Type": "application/json"}
    ensure_folder(folder_path, headers)

    encoded_path = quote(f"{folder_path}/{file_name}")
    upload_url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root:/{encoded_path}:/content"

    with open(file_path, "rb") as f:
        res = requests.put(upload_url, headers={"Authorization": f"Bearer {tok['access_token']}"}, data=f)
    if res.status_code not in (200, 201):
        raise Exception(f"Upload error: {res.status_code} - {res.text}")
    print(f"✅ Uploaded {file_name} to SharePoint at {folder_path}/")

# ==============================
# Report generation
# ==============================
def extract_custom_data():
    # We don’t actually need the field-id map to build the rows,
    # but keeping the call here as a health check/log if you want.
    _ = fetch_custom_fields()
    matters = fetch_open_matters()

    file_path = "/tmp/case_review.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "Case Review"
    ws.append(OUTPUT_FIELDS)

    for m in matters:
        # Custom fields -> simple name:value map
        custom_map = {}
        for cf in (m.get("custom_field_values") or []):
            if not isinstance(cf, dict):
                continue
            fname = cf.get("field_name")
            # value or picklist option text
            val = cf.get("value")
            if not val and isinstance(cf.get("picklist_option"), dict):
                val = cf["picklist_option"].get("option", "")
            custom_map[fname] = val or ""

        row = {
            "Matter Number": m.get("number") or m.get("display_number") or "",
            "Client Name": (m.get("client") or {}).get("name", "") or "",
            "Matter Stage": (m.get("matter_stage") or {}).get("name", "") or "",
            "Responsible Attorney": (m.get("responsible_attorney") or {}).get("name", "") or "",
            # Your report keeps Net Trust Account Balance as a fixed field here
            "Net Trust Account Balance": 0
        }

        # Fill all remaining OUTPUT_FIELDS from custom_map, default ''
        for key in OUTPUT_FIELDS:
            if key not in row:
                row[key] = custom_map.get(key, '')

        # Append in the exact OUTPUT_FIELDS order
        ws.append([row.get(k, '') for k in OUTPUT_FIELDS])

    wb.save(file_path)
    return file_path

def main():
    file_path = extract_custom_data()
    file_date = datetime.now().strftime("%y%m%d")
    file_name = f"{file_date}.Seabrook's Case Review List.xlsx"
    upload_file(file_path, file_name, SHAREPOINT_DOC_LIB)
    try:
        os.remove(file_path)
    except Exception:
        pass

if __name__ == '__main__':
    main()


if __name__ == '__main__':
    main()
