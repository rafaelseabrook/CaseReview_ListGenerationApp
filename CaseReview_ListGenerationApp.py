# -*- coding: utf-8 -*-
"""
Case Review Report (fixed) + Splitter (TEST MODE)
- Adds 'TEST ' prefix to all uploaded filenames
- Matches Responsible Attorneys by last name (handles 'First Last' and 'Last, First')
- Keeps traffic-light formatting and SharePoint uploads
"""

import os
import re
import time
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import quote
import msal
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font
from openpyxl.worksheet.table import Table, TableStyleInfo

# ==============================
# Config (billing cycle)
# ==============================
CYCLE_START_LABEL = "07/29/25"
CYCLE_END_LABEL   = "08/12/25"
CYCLE_START_DATE  = "2025-07-29"
CYCLE_END_DATE    = "2025-08-12"
TIMEZONE_OFFSET   = os.getenv("CLIO_TZ_OFFSET", "-08:00")

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

SHAREPOINT_TENANT_ID = os.getenv("GRAPH_TENANT_ID")
SHAREPOINT_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
SHAREPOINT_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
SHAREPOINT_SITE_ID = os.getenv("SHAREPOINT_SITE_ID")
SHAREPOINT_DRIVE_ID = os.getenv("SHAREPOINT_DRIVE_ID")
SHAREPOINT_DOC_LIB = os.getenv("SHAREPOINT_DOC_LIB", "General Management/Global Case Review Lists")

# ==============================
# Attorney folder mapping (by last name)
# ==============================
ATTORNEY_FOLDERS = {
    "voorhees": "Attorneys and Paralegals/Attorney Case Lists/Voorhees, Elizabeth",
    "darling":  "Attorneys and Paralegals/Attorney Case Lists/Darling, Craig",
    "huang":    "Attorneys and Paralegals/Attorney Case Lists/Huang, Lily",
    "parker":   "Attorneys and Paralegals/Attorney Case Lists/Parker, Gabriella",
}

ATTORNEY_DISPLAY = {
    "voorhees": "Voorhees, Elizabeth",
    "darling":  "Darling, Craig",
    "huang":    "Huang, Lily",
    "parker":   "Parker, Gabriella",
}

# ==============================
# Output columns
# ==============================
OUTPUT_FIELDS = [
    "Matter Number","Client Name","CR ID","Net Trust Account Balance","Matter Stage",
    BILLING_COL,
    "Responsible Attorney","Main Paralegal","Supporting Attorney","Supporting Paralegal","Client Notes",
    "Initial Client Goals","Initial Strategy","Has strategy changed Describe","Current action Items",
    "Hearings","Deadlines","DV situation description","Custody Visitation","CS Add ons Extracurricular",
    "Spousal Support","PDDs","Discovery","Judgment Trial","Post Judgment","collection efforts"
]

# ==============================
# HTTP helpers
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
        raise RuntimeError("Missing CLIO credentials.")
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
            _refresh_clio_token()
            _sleep_with_floor(t0, retry_after=1)
            continue
        if resp.status_code == 429:
            ra = 30
            if resp.headers.get("Retry-After"):
                try: ra = int(resp.headers["Retry-After"])
                except: ra = 30
            print(f"[429] Waiting {ra}s…")
            _sleep_with_floor(t0, retry_after=ra)
            continue
        if 500 <= resp.status_code < 600:
            print(f"[{resp.status_code}] Server error. Retrying in {backoff}s…")
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
    next_url, next_params = url, params
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
        if paging.get("next"):
            next_url, next_params = paging["next"], None
            continue
        break
    return all_rows

# ==============================
# Clio fetchers
# ==============================
def fetch_custom_fields_meta():
    rows = paginate(f"{CLIO_API}/custom_fields.json", {"fields": "id,name,field_type,picklist_options"})
    out = {}
    for f in rows:
        name = f.get("name")
        if not name:
            continue
        opts = {str(o["id"]): o["option"] for o in f.get("picklist_options", []) if o.get("id")}
        out[name] = {"id": f.get("id"), "type": f.get("field_type"), "options": opts}
    return out

def fetch_open_matters_with_cf():
    fields = ("id,display_number,number,client{id,name},matter_stage{name},"
              "responsible_attorney{name},account_balances{balance},"
              "custom_field_values{id,field_name,field_type,value,picklist_option}")
    return paginate(f"{CLIO_API}/matters.json", {"status": "open,pending", "fields": fields})

def fetch_outstanding_client_balances():
    return paginate(f"{CLIO_API}/outstanding_client_balances.json",
                    {"fields": "contact{id,name},total_outstanding_balance"})

def fetch_billable_matters():
    return paginate(f"{CLIO_API}/billable_matters.json",
                    {"fields": "id,display_number,client{id,name},unbilled_amount,unbilled_hours"})

def fetch_cycle_hours(start_iso: str, end_iso: str):
    params = {
        "start_date": start_iso, "end_date": end_iso, "status": "billable",
        "fields": "id,rounded_quantity,type,matter{id,display_number}",
        "order": "id(asc)", "limit": PAGE_LIMIT
    }
    totals = {}
    for e in paginate(f"{CLIO_API}/activities", params):
        if e.get("type") == "TimeEntry":
            dn = (e.get("matter") or {}).get("display_number")
            if dn:
                try:
                    totals[dn] = totals.get(dn, 0.0) + float(e.get("rounded_quantity", 0) or 0) / 3600.0
                except Exception:
                    pass
    return totals

# ==============================
# SharePoint helpers
# ==============================
def ensure_folder(path: str, headers: dict):
    parent = ""
    for seg in path.strip("/").split("/"):
        full = f"{parent}/{seg}" if parent else seg
        r = requests.get(
            f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}/root:/{full}",
            headers=headers
        )
        if r.status_code == 404:
            if parent:
                create_url = (f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/"
                              f"{SHAREPOINT_DRIVE_ID}/root:/{parent}:/children")
            else:
                create_url = (f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/"
                              f"{SHAREPOINT_DRIVE_ID}/root/children")
            requests.post(create_url, headers=headers, json={
                "name": seg, "folder": {}, "@microsoft.graph.conflictBehavior": "replace"
            }).raise_for_status()
        parent = full

def upload_file(file_path: str, file_name: str, folder_path: str):
    authority = f"https://login.microsoftonline.com/{SHAREPOINT_TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        SHAREPOINT_CLIENT_ID, authority=authority, client_credential=SHAREPOINT_CLIENT_SECRET
    )
    tok = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in tok:
        raise Exception(f"Failed to get Graph token: {tok.get('error_description')}")
    headers = {"Authorization": f"Bearer {tok['access_token']}"}
    ensure_folder(folder_path, headers)
    upload_url = (
        f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_SITE_ID}/drives/{SHAREPOINT_DRIVE_ID}"
        f"/root:/{quote(folder_path + '/' + file_name)}:/content"
    )
    with open(file_path, "rb") as f:
        res = requests.put(upload_url, headers=headers, data=f)
    if res.status_code not in (200, 201):
        raise Exception(f"Upload error: {res.status_code} - {res.text}")
    print(f"✅ Uploaded {file_name} to {folder_path}/")

# ==============================
# Report builder
# ==============================
def _resolve_cf_value(cf: dict, meta_by_name: dict) -> str:
    if cf.get("field_type") == "picklist":
        opt = (cf.get("picklist_option") or {}).get("option")
        if opt:
            return opt
        raw = cf.get("value")
        options = ((meta_by_name.get(cf.get("field_name")) or {}).get("options")) or {}
        return options.get(str(raw), raw) or ""
    return str(cf.get("value") or "")

def build_report_dataframe() -> pd.DataFrame:
    cf_meta = fetch_custom_fields_meta()
    matters = fetch_open_matters_with_cf()
    ocb = fetch_outstanding_client_balances()
    billable = fetch_billable_matters()
    start_iso, end_iso = cycle_iso(CYCLE_START_DATE, CYCLE_END_DATE, TIMEZONE_OFFSET)
    cycle_hours = fetch_cycle_hours(start_iso, end_iso)

    m_rows = []
    for m in matters:
        trust = 0.0
        for b in (m.get("account_balances") or []):
            try:
                trust += float((b or {}).get("balance", 0) or 0)
            except Exception:
                pass
        cf_map = {}
        for cf in (m.get("custom_field_values") or []):
            name = cf.get("field_name")
            if not name:
                continue
            cf_map[name] = _resolve_cf_value(cf, cf_meta)
        m_rows.append({
            "Matter ID": m.get("id"),
            "Matter Number": m.get("display_number") or m.get("number") or "",
            "Client ID": (m.get("client") or {}).get("id"),
            "Client Name": (m.get("client") or {}).get("name") or "",
            "Matter Stage": (m.get("matter_stage") or {}).get("name") or "",
            "Responsible Attorney": (m.get("responsible_attorney") or {}).get("name") or "",
            "Trust Account Balance": trust,
            "_cf_map": cf_map
        })
    matter_df = pd.DataFrame(m_rows)

    ocb_df = pd.DataFrame([{
        "Client ID": (r.get("contact") or {}).get("id"),
        "Outstanding Balance": r.get("total_outstanding_balance", 0) or 0
    } for r in ocb])

    billable_df = pd.DataFrame([{
        "Matter ID": bm.get("id"),
        "Matter Number": bm.get("display_number") or "",
        "Client ID": (bm.get("client") or {}).get("id"),
        "Unbilled Amount": bm.get("unbilled_amount", 0) or 0,
        "Unbilled Hours": bm.get("unbilled_hours", 0) or 0
    } for bm in billable])

    # Defensive fill
    for df, cols in [
        (matter_df, ["Matter ID","Matter Number","Client ID","Client Name","Matter Stage","Responsible Attorney","Trust Account Balance","_cf_map"]),
        (ocb_df, ["Client ID","Outstanding Balance"]),
        (billable_df, ["Matter ID","Unbilled Amount","Unbilled Hours"]),
    ]:
        for c in cols:
            if c not in df.columns:
                df[c] = 0 if ("Amount" in c or c.endswith("Balance")) else ({} if c == "_cf_map" else "")

    combined = (
        matter_df
        .merge(ocb_df, on="Client ID", how="left")
        .merge(billable_df[["Matter ID","Unbilled Amount","Unbilled Hours"]], on="Matter ID", how="left")
    )

    for c in ["Trust Account Balance","Outstanding Balance","Unbilled Amount","Unbilled Hours"]:
        combined[c] = pd.to_numeric(combined[c], errors="coerce").fillna(0.0)

    # ===== Allocate client-level Outstanding Balance to matters (proportional by Unbilled Amount; even split if all zero) =====
    def _allocate_outstanding(group: pd.DataFrame) -> pd.DataFrame:
        total_ocb = float(group["Outstanding Balance"].iloc[0] or 0.0)
        if total_ocb == 0.0 or len(group) == 0:
            group["Allocated OCB"] = 0.0
            return group
        ub = group["Unbilled Amount"].astype(float)
        if ub.sum() > 0:
            weights = ub / ub.sum()
        else:
            weights = pd.Series([1.0/len(group)]*len(group), index=group.index)
        group["Allocated OCB"] = (total_ocb * weights).astype(float)
        return group

    combined = combined.groupby("Client ID", as_index=False, group_keys=False).apply(_allocate_outstanding)
    if "Allocated OCB" not in combined.columns:
        combined["Allocated OCB"] = 0.0
    combined["Allocated OCB"] = combined["Allocated OCB"].fillna(0.0)

    # Keep original client-level column for reference (name clarity)
    combined.rename(columns={"Outstanding Balance": "Client Outstanding Balance"}, inplace=True)

    # Updated Net Trust uses Allocated OCB (matter-level)
    combined["Net Trust Account Balance"] = (
        combined["Trust Account Balance"] - combined["Allocated OCB"] - combined["Unbilled Amount"]
    ).astype(float)

    combined[BILLING_COL] = combined["Matter Number"].map(lambda dn: float(cycle_hours.get(dn, 0.0)))

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
            "Main Paralegal": cf.get("Main Paralegal", ""),
            "Supporting Attorney": cf.get("Supporting Attorney", ""),
            "Supporting Paralegal": cf.get("Supporting Paralegal", ""),
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

    df_out = pd.DataFrame(rows, columns=OUTPUT_FIELDS)
    df_out = df_out.sort_values(by="Net Trust Account Balance", ascending=False, kind="mergesort").reset_index(drop=True)
    return df_out

# ==============================
# Excel writer
# ==============================
def write_excel(df: pd.DataFrame, file_path: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "Case Review"
    ws.append(list(df.columns))
    for _, row in df.iterrows():
        ws.append([row.get(col, "") for col in df.columns])

    headers = {ws.cell(row=1, column=c).value: c for c in range(1, ws.max_column + 1)}
    net_col = headers.get("Net Trust Account Balance")

    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    yellow_fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    _ = Font(bold=True)

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

    ref = f"A1:{ws.cell(row=ws.max_row, column=ws.max_column).coordinate}"
    table = Table(displayName="CaseReviewTable", ref=ref)
    table.tableStyleInfo = TableStyleInfo(
        name="TableStyleMedium9", showFirstColumn=False, showLastColumn=False,
        showRowStripes=True, showColumnStripes=False
    )
    ws.add_table(table)

    wb.save(file_path)
    print(f"✅ Saved Excel: {file_path}")

# ==============================
# Splitter
# ==============================
def _last_name_key(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return ""
    if "," in name:
        last = name.split(",")[0].strip()
    else:
        last = name.split()[-1].strip()
    return last.lower()

def split_and_upload_by_attorney(df: pd.DataFrame, file_date: str):
    df = df.copy()
    if "Responsible Attorney" not in df.columns:
        print("⚠️ 'Responsible Attorney' column not found; skipping split.")
        return
    df["_last_key"] = df["Responsible Attorney"].map(_last_name_key)

    # Debug counts so you can verify name formats
    try:
        print("Responsible Attorney counts:\n", df["Responsible Attorney"].value_counts())
    except Exception:
        pass

    for last_key, folder in ATTORNEY_FOLDERS.items():
        atty_df = df[df["_last_key"] == last_key]
        if atty_df.empty:
            print(f"ℹ️ No rows for {last_key.title()}; skipping.")
            continue
        display_name = ATTORNEY_DISPLAY[last_key]
        file_name = f"{file_date}.{display_name} Case Review List.xlsx"
        tmp_path = os.path.join("/tmp", f"case_review_{last_key}.xlsx")
        write_excel(atty_df, tmp_path)
        upload_file(tmp_path, file_name, folder)
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        print(f"✅ Uploaded split file for {display_name} → {folder}/{file_name}")

# ==============================
# Entrypoint
# ==============================
def extract_custom_data_and_build_file() -> tuple[pd.DataFrame, str]:
    df = build_report_dataframe()
    tmp_dir = "/tmp"
    os.makedirs(tmp_dir, exist_ok=True)
    file_path = os.path.join(tmp_dir, "case_review_master.xlsx")
    write_excel(df, file_path)
    return df, file_path

def main():
    df, file_path = extract_custom_data_and_build_file()
    file_date = datetime.now().strftime("%y%m%d")

    # Master upload (TEST prefix)
    master_name = f"{file_date}.Seabrook's Case Review List.xlsx"
    upload_file(file_path, master_name, SHAREPOINT_DOC_LIB)

    # Splits
    split_and_upload_by_attorney(df, file_date)

    # Cleanup
    try:
        os.remove(file_path)
    except Exception:
        pass

if __name__ == "__main__":
    main()
