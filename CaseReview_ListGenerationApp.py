# -*- coding: utf-8 -*-
"""
Case Review Report (fixed) + Splitter
- Correct picklist names (no more numeric IDs for paralegals)
- Adds Billing Cycle Hours (configurable date range) from Activities
- Correct Net Trust Account Balance = Trust - Outstanding - Unbilled
- Includes ALL open/pending matters
- Same color formatting (traffic light) + sort by Net Trust desc
- NEW: Splits master report into 4 Excel files by Responsible Attorney and uploads to each folder
"""

import os
import re
import time
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import quote
import msal
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill, Font
from openpyxl.worksheet.table import Table, TableStyleInfo

# ==============================
# Config (edit these 4 for billing cycle)
# ==============================
CYCLE_START_LABEL = "07/02/25"   # shown in column header
CYCLE_END_LABEL   = "07/15/25"   # shown in column header
CYCLE_START_DATE  = "2025-07-02" # used in Activities query
CYCLE_END_DATE    = "2025-07-15" # used in Activities query
TIMEZONE_OFFSET   = os.getenv("CLIO_TZ_OFFSET", "-08:00")  # used to build ISO times

def cycle_iso(start_date: str, end_date: str, tz: str) -> tuple[str, str]:
    return f"{start_date}T00:00:00{tz}", f"{end_date}T23:59:59{tz}"

BILLING_COL = f"Billing Cycle Hours ({CYCLE_START_LABEL} - {CYCLE_END_LABEL})"

# ==============================
# Environment / Clio / Graph
# ==============================
API_VERSION = "4"
CLIO_BASE = os.getenv("CLIO_BASE", "https://app.clio.com").rstrip("/")
CLIO_API = f"{CLIO_BASE}/api/v{API_VERSION}"
CLIO_TOKEN_URL = f"{CLIO_BASE}/oauth/token"

CLIO_CLIENT_ID = os.getenv("CLIO_CLIENT_ID")
CLIO_CLIENT_SECRET = os.getenv("CLIO_CLIENT_SECRET")
CLIO_REFRESH_TOKEN = os.getenv("CLIO_REFRESH_TOKEN")
_CLIO_ACCESS_TOKEN = os.getenv("CLIO_ACCESS_TOKEN") or ""
_CLIO_EXPIRES_AT_ENV = os.getenv("CLIO_EXPIRES_AT") or os.getenv("CLIO_EXPIRES_IN")
try:
    _CLIO_EXPIRES_AT = float(_CLIO_EXPIRES_AT_ENV) if _CLIO_EXPIRES_AT_ENV else 0.0
except Exception:
    _CLIO_EXPIRES_AT = 0.0

# SharePoint / Graph
SHAREPOINT_TENANT_ID = os.getenv("GRAPH_TENANT_ID")
SHAREPOINT_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
SHAREPOINT_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
SHAREPOINT_SITE_ID = os.getenv("SHAREPOINT_SITE_ID")
SHAREPOINT_DRIVE_ID = os.getenv("SHAREPOINT_DRIVE_ID")

# Master upload folder (unchanged)
SHAREPOINT_DOC_LIB = os.getenv("SHAREPOINT_DOC_LIB", "General Management/Global Case Review Lists")

# ==============================
# NEW: Attorney -> SharePoint folder map for split uploads
# ==============================
ATTORNEY_FOLDERS = {
    "Voorhees, Elizabeth": "Attorneys and Paralegals/Attorney Case Lists/Voorhees, Elizabeth",
    "Darling, Craig":      "Attorneys and Paralegals/Attorney Case Lists/Darling, Craig",
    "Huang, Lily":         "Attorneys and Paralegals/Attorney Case Lists/Huang, Lily",
    "Parker, Gabriella":   "Attorneys and Paralegals/Attorney Case Lists/Parker, Gabriella",
}

# ==============================
# Output columns (exact order you requested)
# ==============================
OUTPUT_FIELDS = [
    "Matter Number","Client Name","CR ID","Net Trust Account Balance","Matter Stage",
    BILLING_COL,  # dynamic column
    "Responsible Attorney","Main Paralegal","Supporting Attorney","Supporting Paralegal","Client Notes",
    "Initial Client Goals","Initial Strategy","Has strategy changed Describe","Current action Items",
    "Hearings","Deadlines","DV situation description","Custody Visitation","CS Add ons Extracurricular",
    "Spousal Support","PDDs","Discovery","Judgment Trial","Post Judgment","collection efforts"
]

# ==============================
# HTTP / Rate limiting
# ==============================
PAGE_LIMIT = 200
GLOBAL_MIN_SLEEP = float(os.getenv("CLIO_GLOBAL_MIN_SLEEP_SEC", "0.25"))

session = requests.Session()
session.headers.update({"Accept": "application/json"})

def _save_tokens_env(tokens: dict):
    global _CLIO_ACCESS_TOKEN, _CLIO_EXPIRES_AT, CLIO_REFRESH_TOKEN
    _CLIO_ACCESS_TOKEN = tokens.get("access_token", _CLIO_ACCESS_TOKEN)
    if tokens.get("refresh_token"):
        CLIO_REFRESH_TOKEN = tokens["refresh_token"]
        os.environ["CLIO_REFRESH_TOKEN"] = CLIO_REFRESH_TOKEN
    expires_in = tokens.get("expires_in", 0)
    try:
        _CLIO_EXPIRES_AT = time.time() + float(expires_in)
    except Exception:
        _CLIO_EXPIRES_AT = time.time() + 3000
    os.environ["CLIO_ACCESS_TOKEN"] = _CLIO_ACCESS_TOKEN or ""
    os.environ["CLIO_EXPIRES_AT"] = str(_CLIO_EXPIRES_AT)

def _refresh_clio_token() -> str:
    if not (CLIO_CLIENT_ID and CLIO_CLIENT_SECRET and CLIO_REFRESH_TOKEN):
        raise RuntimeError("Missing CLIO_CLIENT_ID/CLIO_CLIENT_SECRET/CLIO_REFRESH_TOKEN.")
    resp = session.post(CLIO_TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": CLIO_REFRESH_TOKEN,
        "client_id": CLIO_CLIENT_ID,
        "client_secret": CLIO_CLIENT_SECRET
    }, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Token refresh failed: {resp.status_code} {resp.text[:200]}")
    tokens = resp.json() or {}
    _save_tokens_env(tokens)
    return _CLIO_ACCESS_TOKEN

def _get_clio_token() -> str:
    global _CLIO_ACCESS_TOKEN, _CLIO_EXPIRES_AT
    now = time.time()
    if (not _CLIO_ACCESS_TOKEN) or now >= (_CLIO_EXPIRES_AT or 0):
        return _refresh_clio_token()
    return _CLIO_ACCESS_TOKEN

def _ensure_auth_header():
    token = _get_clio_token()
    session.headers["Authorization"] = f"Bearer {token}"

def _sleep_with_floor(start_ts: float, retry_after: int | float | None = None):
    elapsed = time.time() - start_ts
    base_wait = max(0, GLOBAL_MIN_SLEEP - elapsed)
    wait = max(base_wait, float(retry_after or 0))
    if wait > 0:
        time.sleep(wait)

def _request(method: str, url: str, **kwargs) -> requests.Response:
    _ensure_auth_header()
    max_tries = kwargs.pop("max_tries", 7)
    backoff = 1
    for _ in range(max_tries):
        t0 = time.time()
        resp = session.request(method, url, timeout=60, **kwargs)
        if resp.status_code == 401:
            try:
                _refresh_clio_token()
            finally:
                _sleep_with_floor(t0, retry_after=1)
            continue
        if resp.status_code == 429:
            ra = 30
            if resp.headers.get("Retry-After"):
                try:
                    ra = int(resp.headers["Retry-After"])
                except ValueError:
                    ra = 30
            else:
                try:
                    msg = (resp.json() or {}).get("error", {}).get("message", "")
                    m = re.search(r"Retry in (\d+)", msg)
                    if m:
                        ra = int(m.group(1))
                except Exception:
                    pass
            print(f"[429] Rate limited. Waiting {ra}s …")
            _sleep_with_floor(t0, retry_after=ra)
            continue
        if 500 <= resp.status_code < 600:
            print(f"[{resp.status_code}] Server error. Retrying in {backoff}s …")
            _sleep_with_floor(t0, retry_after=backoff)
            backoff = min(backoff * 2, 60)
            continue
        _sleep_with_floor(t0)
        return resp
    return resp

def paginate(url: str, params: dict | None = None):
    params = dict(params or {})
    params.setdefault("limit", PAGE_LIMIT)
    params.setdefault("order", "id(asc)")
    next_url = url
    next_params = params
    all_rows = []
    while True:
        resp = _request("GET", next_url, params=next_params)
        print(f"GET {next_url} params={next_params} → {resp.status_code}")
        if resp.status_code != 200:
            print(f"Failed: {resp.status_code} {resp.text[:200]}")
            break
        body = resp.json() or {}
        rows = body.get("data", [])
        if isinstance(rows, list):
            all_rows.extend([r for r in rows if isinstance(r, dict)])
        paging = (body.get("meta") or {}).get("paging") or {}
        next_link = paging.get("next")
        if next_link:
            next_url = next_link
            next_params = None
            continue
        if len(rows) < params["limit"]:
            break
        else:
            break
    return all_rows

# ==============================
# Clio fetchers
# ==============================
def fetch_custom_fields_meta():
    """
    Returns:
      { field_name: { 'id': int, 'type': str, 'options': {str(option_id): option_text } } }
    """
    url = f"{CLIO_API}/custom_fields.json"
    params = {"fields": "id,name,field_type,picklist_options"}
    rows = paginate(url, params)
    out = {}
    for f in rows:
        name = f.get("name")
        if not name:
            continue
        opts = {}
        for opt in (f.get("picklist_options") or []):
            oid = opt.get("id")
            text = opt.get("option")
            if oid is not None:
                opts[str(oid)] = text
        out[name] = {"id": f.get("id"), "type": f.get("field_type"), "options": opts}
    return out

def fetch_open_matters_with_cf():
    url = f"{CLIO_API}/matters.json"
    fields = (
        "id,display_number,number,"
        "client{id,name},"
        "matter_stage{name},"
        "responsible_attorney{name},"
        "account_balances{balance},"
        "custom_field_values{id,field_name,field_type,value,picklist_option}"
    )
    params = {"status": "open,pending", "fields": fields}
    return paginate(url, params)

def fetch_outstanding_client_balances():
    url = f"{CLIO_API}/outstanding_client_balances.json"
    params = {"fields": "contact{id,name},total_outstanding_balance"}
    return paginate(url, params)

def fetch_billable_matters():
    url = f"{CLIO_API}/billable_matters.json"
    params = {"fields": "id,display_number,client{id,name},unbilled_amount,unbilled_hours"}
    return paginate(url, params)

def fetch_cycle_hours(start_iso: str, end_iso: str):
    """
    Sum rounded hours per matter display_number within the cycle window.
    Returns { display_number: total_hours }
    """
    url = f"{CLIO_API}/activities"
    fields = "id,rounded_quantity,type,matter{id,display_number}"
    params = {
        "start_date": start_iso,
        "end_date": end_iso,
        "status": "billable",
        "fields": fields,
        "order": "id(asc)",
        "limit": PAGE_LIMIT
    }
    totals = {}
    entries = paginate(url, params)
    for e in entries:
        if e.get("type") != "TimeEntry":
            continue
        m = e.get("matter") or {}
        dn = m.get("display_number")
        if not dn:
            continue
        rq = e.get("rounded_quantity") or 0
        try:
            hours = float(rq) / 3600.0
        except Exception:
            hours = 0.0
        totals[dn] = totals.get(dn, 0.0) + hours
    return totals

# ==============================
# SharePoint upload
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
# Report builder
# ==============================
def _resolve_cf_value(cf: dict, meta_by_name: dict) -> str:
    """
    Prefer picklist option text. If only numeric value is present for picklist,
    translate via meta mapping. Otherwise use raw 'value'.
    """
    fname = cf.get("field_name")
    ftype = cf.get("field_type")
    if ftype == "picklist":
        # Try nested option first
        opt = (cf.get("picklist_option") or {}).get("option")
        if opt:
            return opt
        # Fallback: map the numeric/string ID to text using meta
        raw = cf.get("value")
        options = ((meta_by_name.get(fname) or {}).get("options")) or {}
        if raw is not None and str(raw) in options:
            return options[str(raw)] or ""
        return str(raw or "")
    # non-picklist
    return str(cf.get("value") or "")

def build_report_dataframe() -> pd.DataFrame:
    # Meta for picklist translations
    cf_meta = fetch_custom_fields_meta()

    # Base pulls
    matters = fetch_open_matters_with_cf()
    ocb = fetch_outstanding_client_balances()
    billable = fetch_billable_matters()

    start_iso, end_iso = cycle_iso(CYCLE_START_DATE, CYCLE_END_DATE, TIMEZONE_OFFSET)
    cycle_hours = fetch_cycle_hours(start_iso, end_iso)

    # Build frames
    m_rows = []
    for m in matters:
        # trust balances
        trust = 0.0
        for b in (m.get("account_balances") or []):
            try:
                trust += float((b or {}).get("balance", 0) or 0)
            except Exception:
                pass
        # flatten custom fields into dict of {name: resolved_value}
        cf_map = {}
        for cf in (m.get("custom_field_values") or []):
            if not isinstance(cf, dict):
                continue
            name = cf.get("field_name")
            if not name:
                continue
            cf_map[name] = _resolve_cf_value(cf, cf_meta)

        display_number = m.get("display_number") or m.get("number") or ""
        m_rows.append({
            "Matter ID": m.get("id"),
            "Matter Number": display_number,
            "Client ID": (m.get("client") or {}).get("id"),
            "Client Name": (m.get("client") or {}).get("name") or "",
            "Matter Stage": (m.get("matter_stage") or {}).get("name") or "",
            "Responsible Attorney": (m.get("responsible_attorney") or {}).get("name") or "",
            "Trust Account Balance": trust,
            "_cf_map": cf_map
        })
    matter_df = pd.DataFrame(m_rows)

    # Outstanding per client
    ocb_rows = []
    for r in ocb:
        ocb_rows.append({
            "Client ID": (r.get("contact") or {}).get("id"),
            "Outstanding Balance": r.get("total_outstanding_balance", 0) or 0
        })
    ocb_df = pd.DataFrame(ocb_rows)

    # Unbilled per matter
    b_rows = []
    for bm in billable:
        b_rows.append({
            "Matter ID": bm.get("id"),
            "Matter Number": bm.get("display_number") or "",
            "Client ID": (bm.get("client") or {}).get("id"),
            "Unbilled Amount": bm.get("unbilled_amount", 0) or 0,
            "Unbilled Hours": bm.get("unbilled_hours", 0) or 0
        })
    billable_df = pd.DataFrame(b_rows)

    # Defensive columns
    for df, cols in [
        (matter_df, ["Matter ID","Matter Number","Client ID","Client Name","Matter Stage","Responsible Attorney","Trust Account Balance","_cf_map"]),
        (ocb_df, ["Client ID","Outstanding Balance"]),
        (billable_df, ["Matter ID","Unbilled Amount","Unbilled Hours"]),
    ]:
        for c in cols:
            if c not in df.columns:
                df[c] = 0 if ("Amount" in c or c.endswith("Balance")) else ({} if c == "_cf_map" else "")

    # Merge (base = matters)
    combined = (
        matter_df
        .merge(ocb_df, on="Client ID", how="left")
        .merge(billable_df[["Matter ID","Unbilled Amount","Unbilled Hours"]], on="Matter ID", how="left")
    )

    # Fill NaNs numerics
    for c in ["Trust Account Balance","Outstanding Balance","Unbilled Amount","Unbilled Hours"]:
        if c in combined.columns:
            combined[c] = pd.to_numeric(combined[c], errors="coerce").fillna(0.0)

    # Net Trust Account Balance (same formula as traffic light)
    combined["Net Trust Account Balance"] = (
        combined["Trust Account Balance"] - combined["Outstanding Balance"] - combined["Unbilled Amount"]
    ).astype(float)

    # Billing Cycle Hours from activities map (by display number)
    combined[BILLING_COL] = combined["Matter Number"].map(lambda dn: float(cycle_hours.get(dn, 0.0)))

    # Build output rows in the exact OUTPUT_FIELDS order
    rows = []
    for _, r in combined.iterrows():
        cf = r.get("_cf_map", {}) or {}
        row = {
            "Matter Number": r.get("Matter Number", ""),
            "Client Name": r.get("Client Name", ""),
            "CR ID": cf.get("CR ID", ""),
            "Net Trust Account Balance": float(r.get("Net Trust Account Balance", 0.0)),
            "Matter Stage": r.get("Matter Stage", ""),
            BILLING_COL: float(r.get(BILLING_COL, 0.0)),
            "Responsible Attorney": r.get("Responsible Attorney", ""),
            "Main Paralegal": cf.get("Main Paralegal", ""),          # picklist option text
            "Supporting Attorney": cf.get("Supporting Attorney", ""),# picklist option text
            "Supporting Paralegal": cf.get("Supporting Paralegal", ""), # picklist option text
            "Client Notes": cf.get("Client Notes", ""),
            "Initial Client Goals": cf.get("Initial Client Goals", ""),
            "Initial Strategy": cf.get("Initial Strategy", ""),
            "Has strategy changed Describe": cf.get("Has strategy changed Describe", ""),
            "Current action Items": cf.get("Current action Items", ""),
            "Hearings": cf.get("Hearings", ""),
            "Deadlines": cf.get("Deadlines", ""),
            "DV situation description": cf.get("DV situation description", ""),
            "Custody Visitation": cf.get("Custody Visitation", ""),
            "CS Add ons Extracurricular": cf.get("CS Add ons Extracurricular", ""),
            "Spousal Support": cf.get("Spousal Support", ""),
            "PDDs": cf.get("PDDs", ""),
            "Discovery": cf.get("Discovery", ""),
            "Judgment Trial": cf.get("Judgment Trial", ""),
            "Post Judgment": cf.get("Post Judgment", ""),
            "collection efforts": cf.get("collection efforts", "")
        }
        rows.append(row)

    df = pd.DataFrame(rows, columns=OUTPUT_FIELDS)

    # Sort by Net Trust Account Balance (desc)
    df = df.sort_values(by="Net Trust Account Balance", ascending=False, kind="mergesort").reset_index(drop=True)
    return df

# ==============================
# Excel writer (traffic-light formatting)
# ==============================
def write_excel(df: pd.DataFrame, file_path: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "Case Review"

    # Headers
    ws.append(list(df.columns))

    # Rows
    for _, row in df.iterrows():
        ws.append([row.get(col, "") for col in df.columns])

    headers = {ws.cell(row=1, column=c).value: c for c in range(1, ws.max_column + 1)}
    net_col = headers.get("Net Trust Account Balance", None)

    # Styles (same as Traffic Light)
    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    yellow_fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    bold_font = Font(bold=True)

    # Currency / fills
    for r in range(2, ws.max_row + 1):
        if net_col:
            c = ws.cell(row=r, column=net_col)
            c.number_format = "$#,##0.00"
            try:
                val = float(c.value or 0)
                if val <= 0:
                    c.fill = red_fill
                elif 0 < val < 1000:
                    c.fill = yellow_fill
                else:
                    c.fill = green_fill
            except Exception:
                pass

    # Add table styling
    ref = f"A1:{ws.cell(row=ws.max_row, column=ws.max_column).coordinate}"
    table = Table(displayName="CaseReviewTable", ref=ref)
    style = TableStyleInfo(
        name="TableStyleMedium9",
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=True,
        showColumnStripes=False
    )
    table.tableStyleInfo = style
    ws.add_table(table)

    wb.save(file_path)
    print(f"✅ Saved Excel: {file_path}")

# ==============================
# NEW: Splitter helpers
# ==============================
def _clean_attorney(s: str) -> str:
    return (s or "").strip()

def split_and_upload_by_attorney(df: pd.DataFrame, file_date: str):
    # Ensure column exists
    if "Responsible Attorney" not in df.columns:
        print("⚠️ 'Responsible Attorney' column not found; skipping split.")
        return

    df = df.copy()
    df["Responsible Attorney"] = df["Responsible Attorney"].map(_clean_attorney)

    for atty, folder in ATTORNEY_FOLDERS.items():
        atty_df = df[df["Responsible Attorney"] == atty]
        if atty_df.empty:
            print(f"ℹ️ No rows for {atty}; skipping.")
            continue

        # File name: e.g., 250808.Case Review - Voorhees, Elizabeth.xlsx
        atty_file_name = f"{file_date}.TESTCase Review - {atty}.xlsx"

        # Write to /tmp and upload
        tmp_dir = "/tmp"
        os.makedirs(tmp_dir, exist_ok=True)
        safe_stub = atty.replace(",", "").replace(" ", "_")
        atty_file_path = os.path.join(tmp_dir, f"case_review_{safe_stub}.xlsx")

        write_excel(atty_df, atty_file_path)
        upload_file(atty_file_path, atty_file_name, folder)

        try:
            os.remove(atty_file_path)
        except Exception:
            pass

        print(f"✅ Uploaded split file for {atty} → {folder}/{atty_file_name}")

# ==============================
# Entrypoint: build + upload
# ==============================
def extract_custom_data_and_build_file() -> tuple[pd.DataFrame, str]:
    """Build the DataFrame, write the master Excel to /tmp, and return (df, path)."""
    df = build_report_dataframe()
    tmp_dir = "/tmp"
    os.makedirs(tmp_dir, exist_ok=True)
    file_path = os.path.join(tmp_dir, "case_review_master.xlsx")
    write_excel(df, file_path)
    return df, file_path

def main():
    df, file_path = extract_custom_data_and_build_file()

    file_date = datetime.now().strftime("%y%m%d")
    master_name = f"{file_date}.TESTSeabrook's Case Review List.xlsx"

    # 1) Upload the full master file to the Global Case Review List library/folder
    upload_file(file_path, master_name, SHAREPOINT_DOC_LIB)

    # 2) Split by attorney and upload to each attorney's folder
    split_and_upload_by_attorney(df, file_date)

    # Cleanup local temp
    try:
        os.remove(file_path)
    except Exception:
        pass

if __name__ == "__main__":
    main()
