# ============================================================
# RESOURCE DAILY ACTIVITY AND TASK REPORT
# Boilerplate infrastructure code extracted from main pipeline
# ============================================================

import os
import re
import time
import sys
import json
import base64
import pytz
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from tempfile import NamedTemporaryFile
from dotenv import load_dotenv
from tqdm import tqdm
from gspread.utils import rowcol_to_a1
from collections import defaultdict

# ============================================================
# LOGGER
# ============================================================

def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)

READ_DELAY = 0.35  # Keep reads gentle while avoiding unnecessary latency
MAX_SHEETS_RETRIES = 5

# ============================================================
# ENV VARIABLES
# ============================================================

load_dotenv()

CLICKUP_API_TOKEN = None
GOOGLE_SERVICE_ACCOUNT_JSON = None
AUDIT_SHEET_URL = None
LOCAL_RESOURCE_POOL_URL = None
PROJECT_NAME_FILTER = None
HIGHLIGHT_TRACKER_ERRORS = None
RESOURCE_LOOKUP_TAB = "Team List & Activity"
gs_client = None
audit_sheet = None
resource_type_map = {}
SERVICE_ACCOUNT_EMAIL = ""
AUDIT_LOG_TAB = "Run Errors"
spreadsheet_cache = {}
worksheet_cache = {}
local_resource_pool_cache = {
    "spreadsheet": None,
    "worksheet": None,
    "headers": None,
    "df": None,
    "header_row_idx": None,
    "resolved_pool_columns": None,
    "qai_project_row_map": None,
    "qai_first_row_map": None,
    "qai_blank_project_row_map": None,
}
project_audit_context = {}

TRACKER_SUMMARY_SHEET = "Datewise Summary"
CHARTER_TEAM_MEMBER_DETAILS_SHEET = "Team Member Details"
IGNORED_PDL_EMAILS = {
    "nayeem@quantigo.ai",
    "towfique@quantigo.ai",
    "imran@quantigo.ai",
}
CLEAR_IF_NOT_RUNNING_HEADERS = [
    "Task Tracker Activity (today)",
    "Today Task Tracker Activity (Annotation)",
    "Today Task Tracker Activity (QC)",
    "Annotation Accuracy (today)",
    "QC Accuracy (today)",
]
MANUAL_RESOURCE_POOL_REQUIRED_HEADERS = [
    "QAI ID",
    "Full Name",
    "Email",
    "Contact",
    "Client Alias",
    "Project Name",
    "Task Tracker Link",
    "Engagement (today)",
    "Task Tracker Activity (today)",
    "Today Task Tracker Activity (Annotation)",
    "Today Task Tracker Activity (QC)",
    "Annotation Accuracy (today)",
    "QC Accuracy (today)",
    "Activity STATUS",
    "Designation",
    "QAI ID Status",
    "Remarks",
    "Current Designation",
]

def normalize_service_account_json(value):
    if not value:
        return value

    text = str(value).strip()

    # Allow the env var to point to a local JSON file path.
    if os.path.exists(text) and os.path.isfile(text):
        with open(text, encoding="utf-8") as service_account_file:
            text = service_account_file.read().strip()

    # Handle secrets that escape newlines.
    if "\\n" in text and "{\n" not in text:
        text = text.replace("\\n", "\n")

    try:
        json.loads(text)
        return text
    except Exception as e:
        log(f"⚠️ Failed to parse JSON: {e}")
        pass

    # Handle base64-encoded JSON.
    try:
        decoded = base64.b64decode(text).decode("utf-8")
        json.loads(decoded)
        return decoded
    except Exception as e:
        log(f"⚠️ Failed to decode base64: {e}")
        return text

def load_runtime_config():
    global CLICKUP_API_TOKEN
    global GOOGLE_SERVICE_ACCOUNT_JSON
    global AUDIT_SHEET_URL
    global LOCAL_RESOURCE_POOL_URL
    global PROJECT_NAME_FILTER
    global HIGHLIGHT_TRACKER_ERRORS
    global RESOURCE_LOOKUP_TAB
    global SERVICE_ACCOUNT_EMAIL

    CLICKUP_API_TOKEN = os.getenv("CLICKUP_API_TOKEN")
    GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    AUDIT_SHEET_URL = os.getenv("AUDIT_SHEET_URL")
    LOCAL_RESOURCE_POOL_URL = os.getenv("LOCAL_RESOURCE_POOL_URL")
    PROJECT_NAME_FILTER = (os.getenv("PROJECT_NAME_FILTER") or "").strip()
    HIGHLIGHT_TRACKER_ERRORS = (os.getenv("HIGHLIGHT_TRACKER_ERRORS", "true") or "true").strip().lower() in {
        "1", "true", "yes", "y"
    }
    RESOURCE_LOOKUP_TAB = os.getenv("RESOURCE_LOOKUP_TAB", "Team List & Activity")

    if not GOOGLE_SERVICE_ACCOUNT_JSON and os.path.exists("service_account.json"):
        with open("service_account.json", encoding="utf-8") as service_account_file:
            GOOGLE_SERVICE_ACCOUNT_JSON = service_account_file.read()

    GOOGLE_SERVICE_ACCOUNT_JSON = normalize_service_account_json(GOOGLE_SERVICE_ACCOUNT_JSON)

    try:
        service_account_payload = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        SERVICE_ACCOUNT_EMAIL = str(service_account_payload.get("client_email", "")).strip()
    except Exception:
        SERVICE_ACCOUNT_EMAIL = ""

    required_env = {
        "CLICKUP_API_TOKEN": CLICKUP_API_TOKEN,
        "GOOGLE_SERVICE_ACCOUNT_JSON or service_account.json": GOOGLE_SERVICE_ACCOUNT_JSON,
        "AUDIT_SHEET_URL": AUDIT_SHEET_URL,
    }

    missing_env = [k for k, v in required_env.items() if not v]
    if missing_env:
        log(f"❌ Missing required environment variables: {', '.join(missing_env)}")
        sys.exit(1)

def authenticate_google():
    global gs_client
    global audit_sheet

    try:
        with NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
            f.write(GOOGLE_SERVICE_ACCOUNT_JSON)
            sa_path = f.name

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_name(sa_path, scope)
        gs_client = gspread.authorize(creds)

        audit_sheet = gs_client.open_by_url(AUDIT_SHEET_URL)

        log("✅ Google authenticated")
    except Exception as e:
        log(f"❌ Google authentication failed: {e}")
        sys.exit(1)


def format_sheet_access_error(sheet_kind, url):
    details = [f"{sheet_kind} is not accessible"]

    if SERVICE_ACCOUNT_EMAIL:
        details.append(
            f"share it with service account: {SERVICE_ACCOUNT_EMAIL}"
        )

    if url:
        details.append(f"url: {url}")

    return " | ".join(details)


def is_quota_error(exc):
    response = getattr(exc, "response", None)
    if response is not None and getattr(response, "status_code", None) == 429:
        return True
    return "429" in str(exc)


def get_retry_wait_seconds(attempt):
    return min(10 * (attempt + 1), 60)


def normalize_field_value(value):
    if value is None:
        return ""

    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(
                    str(
                        item.get("username")
                        or item.get("email")
                        or item.get("name")
                        or item.get("id")
                        or ""
                    ).strip()
                )
            else:
                parts.append(str(item).strip())
        return ", ".join([part for part in parts if part])

    if isinstance(value, dict):
        return str(
            value.get("username")
            or value.get("email")
            or value.get("name")
            or value.get("id")
            or ""
        ).strip()

    return str(value).strip()


def get_custom_field_value(task, field_name):
    for field in task.get("custom_fields", []):
        if field.get("name") == field_name:
            return normalize_field_value(field.get("value"))
    return ""


def extract_emails(text):
    return {
        match.strip().lower()
        for match in re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", str(text), flags=re.IGNORECASE)
    }


def should_ignore_project_by_pdl_email(pdl_value):
    return bool(extract_emails(pdl_value) & IGNORED_PDL_EMAILS)


def classify_issue_type(issue):
    text = str(issue).strip().lower()

    if "quota" in text or "429" in text:
        return "quota_limit"
    if "not accessible" in text or "permission" in text or "share it with service account" in text:
        return "permission_issue"
    if "worksheet not found" in text:
        return "worksheet_missing"
    if "column missing" in text:
        return "missing_column"
    if "local resource pool sheet column missing" in text:
        return "missing_column"
    if "missing at row" in text:
        return "missing_row_value"
    if "status missing for qai id" in text:
        return "missing_status"
    if "sheet is empty" in text:
        return "empty_sheet"
    if "missing in sheet for" in text:
        return "missing_activity"
    if "skipped charter parsing" in text or "skipped tracker processing" in text:
        return "skipped_step"
    if text.endswith(" missing") or "_url missing" in text or "client_alias missing" in text:
        return "missing_field"
    if "no activity rows were generated" in text:
        return "no_activity_rows"
    if "failed to open" in text:
        return "sheet_open_failed"

    return "other"


def safe_open_spreadsheet_by_url(url, project_name, all_errors, sheet_kind):
    if url in spreadsheet_cache:
        return spreadsheet_cache[url]

    message = None

    for attempt in range(MAX_SHEETS_RETRIES):
        try:
            time.sleep(READ_DELAY)
            spreadsheet = gs_client.open_by_url(url)
            spreadsheet_cache[url] = spreadsheet
            return spreadsheet
        except PermissionError:
            message = format_sheet_access_error(sheet_kind, url)
            break
        except gspread.exceptions.APIError as e:
            if is_quota_error(e) and attempt < MAX_SHEETS_RETRIES - 1:
                wait = get_retry_wait_seconds(attempt)
                log(f"⏳ {project_name}: quota hit opening {sheet_kind.lower()}, retrying in {wait}s")
                time.sleep(wait)
                continue
            if getattr(e, "response", None) is not None and e.response.status_code == 403:
                message = format_sheet_access_error(sheet_kind, url)
            else:
                message = f"Failed to open {sheet_kind.lower()}: {e}"
            break
        except Exception as e:
            message = f"Failed to open {sheet_kind.lower()}: {e}"
            break

    all_errors.append({
        "project": project_name,
        "issue": message,
    })
    log(f"⚠️ {project_name}: {message}")
    return None


def safe_get_worksheet(spreadsheet, title, project_name, all_errors, sheet_kind):
    cache_key = (spreadsheet.id, title)
    if cache_key in worksheet_cache:
        return worksheet_cache[cache_key]

    message = None

    for attempt in range(MAX_SHEETS_RETRIES):
        try:
            time.sleep(READ_DELAY)
            worksheet = spreadsheet.worksheet(title)
            worksheet_cache[cache_key] = worksheet
            return worksheet
        except gspread.exceptions.WorksheetNotFound:
            message = f"{sheet_kind} worksheet not found: '{title}'"
            break
        except PermissionError:
            message = f"{sheet_kind} worksheet is not accessible: '{title}'"
            break
        except gspread.exceptions.APIError as e:
            if is_quota_error(e) and attempt < MAX_SHEETS_RETRIES - 1:
                wait = get_retry_wait_seconds(attempt)
                log(f"⏳ {project_name}: quota hit opening worksheet '{title}', retrying in {wait}s")
                time.sleep(wait)
                continue
            if getattr(e, "response", None) is not None and e.response.status_code == 403:
                message = f"{sheet_kind} worksheet is not accessible: '{title}'"
            else:
                message = f"Failed to open {sheet_kind.lower()} worksheet '{title}': {e}"
            break
        except Exception as e:
            message = f"Failed to open {sheet_kind.lower()} worksheet '{title}': {e}"
            break

    all_errors.append({
        "project": project_name,
        "issue": message,
    })
    log(f"⚠️ {project_name}: {message}")
    return None


def write_audit_log(all_errors):
    if audit_sheet is None:
        log("⚠️ Audit sheet is unavailable, skipping audit log upload.")
        return

    rows = []
    run_time = datetime.now(timezone.utc).isoformat()

    for err in all_errors:
        project_name = str(err.get("project", "")).strip()
        context = project_audit_context.get(project_name, {})
        issue = str(err.get("issue", "")).strip()
        rows.append({
            "Run Timestamp UTC": run_time,
            "Project": project_name,
            "ClickUp Status": str(context.get("clickup_status", "")).strip(),
            "PDL": str(context.get("pdl", "")).strip(),
            "Delivery Lead": str(context.get("delivery_lead", "")).strip(),
            "Reason Type": str(err.get("reason_type", "")).strip() or classify_issue_type(issue),
            "Issue": issue,
        })

    audit_df = pd.DataFrame(
        rows,
        columns=[
            "Run Timestamp UTC",
            "Project",
            "ClickUp Status",
            "PDL",
            "Delivery Lead",
            "Reason Type",
            "Issue",
        ]
    )
    upload(audit_df, audit_sheet, AUDIT_LOG_TAB)
    log(f"📝 Audit log updated with {len(audit_df)} error row(s).")


def get_local_resource_pool_context(all_errors=None, project_name=""):
    if local_resource_pool_cache["worksheet"] is not None and local_resource_pool_cache["df"] is not None:
        return (
            local_resource_pool_cache["worksheet"],
            local_resource_pool_cache["headers"],
            local_resource_pool_cache["df"],
            local_resource_pool_cache["header_row_idx"],
            local_resource_pool_cache["resolved_pool_columns"],
            local_resource_pool_cache["qai_project_row_map"],
            local_resource_pool_cache["qai_first_row_map"],
            local_resource_pool_cache["qai_blank_project_row_map"],
        )

    sh = safe_open_spreadsheet_by_url(
        url=LOCAL_RESOURCE_POOL_URL,
        project_name=project_name or "Local Resource Pool",
        all_errors=all_errors if all_errors is not None else [],
        sheet_kind="Local resource pool sheet",
    )
    if sh is None:
        return None, None, None

    ws = safe_get_worksheet(
        spreadsheet=sh,
        title="Assign",
        project_name=project_name or "Local Resource Pool",
        all_errors=all_errors if all_errors is not None else [],
        sheet_kind="Local resource pool sheet",
    )
    if ws is None:
        return None, None, None, None

    data = read_worksheet_values(
        ws,
        project_name=project_name or "Local Resource Pool",
        sheet_name="Assign",
    )
    if not data:
        headers = []
        df = pd.DataFrame()
        header_row_idx = None
        resolved_pool_columns = None
        qai_project_row_map = {}
        qai_first_row_map = {}
        qai_blank_project_row_map = {}
    else:
        header_row_idx = find_exact_header_row(
            data,
            MANUAL_RESOURCE_POOL_REQUIRED_HEADERS,
        )
        if header_row_idx is None:
            add_project_error(
                all_errors if all_errors is not None else [],
                project_name or "Local Resource Pool",
                "Manual resource pool header row does not match expected column order",
            )
            return ws, [], pd.DataFrame(), None, None, {}, {}

        headers = [str(col).strip() for col in data[header_row_idx]]
        df = pd.DataFrame(data[header_row_idx + 1 :], columns=headers)
        df = df.replace(r'^\s*$', np.nan, regex=True)
        df.columns = df.columns.str.strip()
        resolved_pool_columns, _ = resolve_dataframe_columns(
            df,
            ["QAI ID", "Project Name", "Full Name", "Email", "Contact"],
        )
        if len(resolved_pool_columns) == 5:
            qai_project_row_map, qai_first_row_map, qai_blank_project_row_map = build_local_resource_pool_indexes(
                df,
                resolved_pool_columns,
            )
        else:
            qai_project_row_map = {}
            qai_first_row_map = {}
            qai_blank_project_row_map = {}

    local_resource_pool_cache["spreadsheet"] = sh
    local_resource_pool_cache["worksheet"] = ws
    local_resource_pool_cache["headers"] = headers
    local_resource_pool_cache["df"] = df
    local_resource_pool_cache["header_row_idx"] = header_row_idx
    local_resource_pool_cache["resolved_pool_columns"] = resolved_pool_columns
    local_resource_pool_cache["qai_project_row_map"] = qai_project_row_map
    local_resource_pool_cache["qai_first_row_map"] = qai_first_row_map
    local_resource_pool_cache["qai_blank_project_row_map"] = qai_blank_project_row_map

    return ws, headers, df, header_row_idx, resolved_pool_columns, qai_project_row_map, qai_first_row_map, qai_blank_project_row_map

# ============================================================
# CLICKUP FETCH
# ============================================================

LIST_ID = "900201326056"
TRACKER_FIELD_NAME = "Task/Progress Tracker"
CHARTER_FIELD_NAME = "Project Charter Link"


TARGET_STATUS_NAMES = [
    "project received",
    "project in progress",
    "pending schedule approval",
]

def fetch_clickup_tasks():
    headers = {"Authorization": CLICKUP_API_TOKEN}
    tasks = []
    page = 0

    while True:
        r = requests.get(
            f"https://api.clickup.com/api/v2/list/{LIST_ID}/task",
            headers=headers,
            params={
                "page": page,
                "include_closed": True,
                "include_archived": True,
                "statuses[]": TARGET_STATUS_NAMES,
            },
            timeout=30,
        )

        if r.status_code != 200:
            log(f"❌ ClickUp error: {r.text}")
            sys.exit(1)

        batch = r.json().get("tasks", [])
        if not batch:
            break

        tasks.extend(batch)
        page += 1

    log(f"✅ Total ClickUp projects (selected status): {len(tasks)}")
    return tasks

def filter_tasks_for_test_run(tasks):
    if not PROJECT_NAME_FILTER:
        log("ℹ️ PROJECT_NAME_FILTER is empty. Processing all matching ClickUp projects.")
        return tasks

    filtered_tasks = [
        task for task in tasks
        if str(task.get("name", "")).strip().lower() == PROJECT_NAME_FILTER.lower()
    ]

    log(
        f'🔎 PROJECT_NAME_FILTER enabled: "{PROJECT_NAME_FILTER}" '
        f"-> matched {len(filtered_tasks)} project(s)"
    )
    return filtered_tasks

# ============================================================
# GOOGLE SHEETS UTILITIES
# ============================================================

def read_worksheet_values(worksheet, range_name=None, project_name="", sheet_name=""):
    label = sheet_name or worksheet.title.strip()

    for attempt in range(5):
        try:
            time.sleep(READ_DELAY)
            if range_name:
                return worksheet.get(range_name)
            return worksheet.get_all_values()
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                wait = 10 * (attempt + 1)
                if project_name:
                    log(f"⏳ Quota hit: {project_name} - {label}, wait {wait}s")
                else:
                    log(f"⏳ Quota hit: {label}, wait {wait}s")
                time.sleep(wait)
            else:
                raise

    raise RuntimeError(f"Read quota exceeded for worksheet: {label}")


def worksheet_values_to_records(values):
    if not values:
        return []

    header = values[0]
    rows = values[1:]
    records = []

    for row in rows:
        record = {}
        for i, col in enumerate(header):
            key = str(col).strip()
            record[key] = row[i] if i < len(row) else ""
        records.append(record)

    return records

def clear_or_create_worksheet(spreadsheet, title, rows=1000, cols=20):
    try:
        ws = spreadsheet.worksheet(title)
        ws.clear()
        return ws
    except Exception as e:
        log(f"⚠️ Failed to clear worksheet '{title}': {e}, creating new one")
        return spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))

def delete_worksheet_if_exists(spreadsheet, title):
    try:
        ws = spreadsheet.worksheet(title)
        spreadsheet.del_worksheet(ws)
    except Exception as e:
        log(f"⚠️ Failed to delete worksheet '{title}': {e}")
        pass


def ensure_worksheet_has_rows(worksheet, required_row_count):
    current_rows = int(getattr(worksheet, "row_count", 0) or 0)
    if required_row_count <= current_rows:
        return

    worksheet.add_rows(required_row_count - current_rows)

def upload(df, sheet, tab):
    try:
        ws = sheet.worksheet(tab)
        ws.clear()
    except Exception as e:
        log(f"⚠️ Failed to clear worksheet '{tab}': {e}, creating new one")
        ws = sheet.add_worksheet(title=tab, rows="1000", cols="20")

    ws.update(range_name="A1", values=[df.columns.tolist()])

    if not df.empty:
        clean_df = df.copy()
        clean_df = clean_df.replace([float("inf"), float("-inf")], pd.NA)
        clean_df = clean_df.where(pd.notna(clean_df), "")
        values = clean_df.astype(str).values.tolist()
        ws.update(range_name="A2", values=values)

def format_header_row(worksheet, start_col, width):
    start_a1 = rowcol_to_a1(1, start_col)
    end_a1 = rowcol_to_a1(1, start_col + width - 1)
    worksheet.format(
        f"{start_a1}:{end_a1}",
        {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95},
        },
    )

def set_sheet_tab_red(spreadsheet, worksheet):
    spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": worksheet.id,
                            "tabColor": {"red": 1, "green": 0, "blue": 0},
                        },
                        "fields": "tabColor",
                    }
                }
            ]
        }
    )

def highlight_error_cell(worksheet, cell_a1):
    worksheet.format(
        cell_a1,
        {"backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}},
    )

def batch_highlight_errors(spreadsheet, error_logs):
    # Group cells by worksheet
    sheet_cells = defaultdict(list)

    for err in error_logs:
        sheet_cells[err["sheet"]].append(err["cell"])

    # Apply formatting in batches
    for sheet_name, cells in sheet_cells.items():
        try:
            ws = spreadsheet.worksheet(sheet_name)

            requests = []

            for cell in cells:
                requests.append({
                    "range": cell,
                    "format": {
                        "backgroundColor": {
                            "red": 1,
                            "green": 0.8,
                            "blue": 0.8
                        }
                    }
                })

            ws.batch_format(requests)

        except Exception as e:
            log(f"⚠️ Failed formatting {sheet_name}: {e}")

# ============================================================
# QAI ID AND MEMBER PARSING
# ============================================================

QAI_ID_REGEX = re.compile(r"(QAI[\s_-]*[A-Z]{2,}\d+)", re.IGNORECASE)

def normalize_qai_id(value):
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    m = QAI_ID_REGEX.search(text)
    if not m:
        return None

    raw = m.group(1).upper()
    compact = re.sub(r"[^A-Z0-9]", "", raw)
    if not compact.startswith("QAI"):
        return None

    return "QAI_" + compact[3:]

def split_member_id_and_name(value):
    if value is None:
        return None, None

    raw = str(value).strip()
    if not raw:
        return None, None

    qai_id = normalize_qai_id(raw)
    name = raw

    if qai_id:
        name = QAI_ID_REGEX.sub("", name)
        name = re.sub(r"[\(\)\[\]\-_,:|/]+", " ", name)
        name = re.sub(r"\s+", " ", name).strip()

    if not name:
        name = None

    return qai_id, name

def strip_qai_id_from_member_text(value):
    if value is None:
        return None

    raw = str(value).strip()
    if not raw:
        return None

    name = QAI_ID_REGEX.sub("", raw)
    name = re.sub(r"^[\s\(\)\[\]\-_,:|/]+|[\s\(\)\[\]\-_,:|/]+$", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name or raw

def extract_dashboard_member_parts(raw):
    if not raw:
        return "", ""

    text = str(raw).strip()
    match = re.search(r"QAI_[A-Z]+\d+", text, re.IGNORECASE)
    if match:
        member_id = match.group(0).upper()
        member_name = text.replace(match.group(0), "")
        member_name = re.sub(r"[\(\)\-_]+", " ", member_name)
        member_name = re.sub(r"\bQAI\b", " ", member_name, flags=re.IGNORECASE)
        member_name = re.sub(r"\s+", " ", member_name).strip()
        return member_id, member_name

    return text, ""

# ============================================================
# RESOURCE LOOKUP
# ============================================================

def _resolve_lookup_columns(header):
    normalized = [
        re.sub(r"[^a-z0-9]+", " ", str(h).strip().lower()).strip()
        for h in header
    ]

    id_candidates = {"qai_id", "qai id", "resource id", "employee id", "id"}
    type_candidates = {"designation", "resource_type", "resource type", "type"}

    id_idx = None
    type_idx = None

    for i, col in enumerate(normalized):
        if col in id_candidates and id_idx is None:
            id_idx = i
        if col in type_candidates and type_idx is None:
            type_idx = i

    # Flexible fallback for custom headers such as "QAI ID And Name"
    if id_idx is None:
        for i, col in enumerate(normalized):
            if "qai" in col and "id" in col:
                id_idx = i
                break

    if type_idx is None:
        for i, col in enumerate(normalized):
            if any(k in col for k in ["designation", "resource type", "resource_type", "type"]):
                type_idx = i
                break

    return id_idx, type_idx

def load_resource_type_lookup():
    return {}

# ============================================================
# DASHBOARD PARSING UTILITIES
# ============================================================

def _normalize_cell(value):
    return re.sub(r"[^a-z0-9]+", " ", str(value).strip().lower()).strip()


def clean_sheet_text(value):
    if value is None or pd.isna(value):
        return ""

    text = str(value).strip()
    if text.lower() in {"nan", "none"}:
        return ""

    return text


def clean_sheet_value(value):
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    if isinstance(value, (float, np.floating)) and not np.isfinite(value):
        return ""

    if isinstance(value, (int, np.integer)):
        return int(value)

    if isinstance(value, (float, np.floating)):
        return float(value)

    text = str(value).strip()
    if text.lower() in {"nan", "none", "inf", "-inf", "infinity", "-infinity"}:
        return ""

    return text


def clean_sheet_rows(rows):
    return [
        [clean_sheet_value(cell) for cell in row]
        for row in rows
    ]


def resolve_dataframe_columns(df, required_columns):
    normalized_to_actual = {}
    for col in df.columns:
        normalized_to_actual[_normalize_cell(col)] = col

    resolved = {}
    missing = []

    for required in required_columns:
        actual = normalized_to_actual.get(_normalize_cell(required))
        if actual is None:
            missing.append(required)
        else:
            resolved[required] = actual

    return resolved, missing


def build_values_for_sheet_headers(headers, row_data, existing_values=None):
    normalized_row_data = {
        _normalize_cell(key): "" if value is None else str(value).replace("nan", "")
        for key, value in row_data.items()
    }

    values = []
    for i, header in enumerate(headers):
        normalized_header = _normalize_cell(header)
        if normalized_header in normalized_row_data:
            values.append(normalized_row_data[normalized_header])
        elif existing_values is not None and i < len(existing_values):
            values.append("" if existing_values[i] is None else str(existing_values[i]).replace("nan", ""))
        else:
            values.append("")

    return values


def resolve_header_positions(headers, required_headers):
    normalized_positions = {}
    for idx, header in enumerate(headers):
        normalized = _normalize_cell(header)
        if normalized not in normalized_positions:
            normalized_positions[normalized] = idx

    resolved = {}
    missing = []
    for header in required_headers:
        idx = normalized_positions.get(_normalize_cell(header))
        if idx is None:
            missing.append(header)
        else:
            resolved[header] = idx

    return resolved, missing


def build_row_values_by_positions(headers, header_positions, row_data, existing_values=None):
    if existing_values is not None:
        values = [
            "" if value is None else str(value).replace("nan", "")
            for value in existing_values[: len(headers)]
        ]
        if len(values) < len(headers):
            values.extend([""] * (len(headers) - len(values)))
    else:
        values = [""] * len(headers)

    normalized_row_data = {
        _normalize_cell(key): "" if value is None else str(value).replace("nan", "")
        for key, value in row_data.items()
    }

    for header, idx in header_positions.items():
        normalized_header = _normalize_cell(header)
        values[idx] = normalized_row_data.get(normalized_header, "")

    return values


def build_local_resource_pool_indexes(df, resolved_pool_columns):
    qai_id_col = resolved_pool_columns["QAI ID"]
    project_name_col = resolved_pool_columns["Project Name"]

    qai_project_row_map = {}
    qai_first_row_map = {}
    qai_blank_project_row_map = {}

    for idx, row in df.iterrows():
        qai_id = clean_sheet_text(row.get(qai_id_col, ""))
        project_name = clean_sheet_text(row.get(project_name_col, ""))

        if not qai_id:
            continue

        qai_project_row_map[(qai_id, project_name)] = idx
        if qai_id not in qai_first_row_map:
            qai_first_row_map[qai_id] = idx
        if not project_name and qai_id not in qai_blank_project_row_map:
            qai_blank_project_row_map[qai_id] = idx

    return qai_project_row_map, qai_first_row_map, qai_blank_project_row_map

def _find_header_row(values, required_phrases):
    for i, row in enumerate(values):
        normalized_cells = [_normalize_cell(c) for c in row if str(c).strip()]
        if not normalized_cells:
            continue
        if all(any(req in cell for cell in normalized_cells) for req in required_phrases):
            return i
    return None


def find_exact_header_row(values, expected_headers):
    normalized_expected = [_normalize_cell(header) for header in expected_headers]

    for i, row in enumerate(values):
        normalized_row = [_normalize_cell(cell) for cell in row]
        if len(normalized_row) < len(normalized_expected):
            continue
        if normalized_row[: len(normalized_expected)] == normalized_expected:
            return i

    return None

def _find_col_index(header_row, phrases):
    normalized = [_normalize_cell(c) for c in header_row]
    for phrase in phrases:
        for i, cell in enumerate(normalized):
            if phrase in cell:
                return i
    return None

def _find_col_index_near(header_row, phrases, anchor_idx, direction="right"):
    normalized = [_normalize_cell(c) for c in header_row]
    candidates = []
    for phrase in phrases:
        for i, cell in enumerate(normalized):
            if phrase in cell:
                candidates.append(i)

    if not candidates:
        return None

    if anchor_idx is None:
        return min(candidates)

    if direction == "right":
        right = [i for i in candidates if i >= anchor_idx]
        if right:
            return min(right)
    elif direction == "left":
        left = [i for i in candidates if i <= anchor_idx]
        if left:
            return max(left)

    # Fallback to nearest
    return min(candidates, key=lambda i: abs(i - anchor_idx))

def _parse_table(values, header_idx, col_map):
    if header_idx is None:
        return []

    header = values[header_idx]
    col_idx = {}
    for key, phrases in col_map.items():
        idx = _find_col_index(header, phrases)
        if idx is None:
            return []
        col_idx[key] = idx

    rows = []
    empty_streak = 0
    for r in values[header_idx + 1 :]:
        row_values = {k: (r[i] if i < len(r) else "") for k, i in col_idx.items()}
        normalized_first = _normalize_cell(row_values.get("id", ""))
        if normalized_first == "total":
            break

        if not any(str(v).strip() for v in row_values.values()):
            empty_streak += 1
            if empty_streak >= 2:
                break
            continue

        empty_streak = 0
        rows.append(row_values)

    return rows

def _parse_table_with_anchor(values, header_idx, anchor_phrases, col_map):
    if header_idx is None:
        return []

    header = values[header_idx]
    anchor_idx = _find_col_index(header, anchor_phrases)
    if anchor_idx is None:
        return []

    col_idx = {}
    for key, phrases in col_map.items():
        idx = _find_col_index_near(header, phrases, anchor_idx, direction="right")
        if idx is None:
            return []
        col_idx[key] = idx

    rows = []
    empty_streak = 0
    for r in values[header_idx + 1 :]:
        row_values = {k: (r[i] if i < len(r) else "") for k, i in col_idx.items()}
        normalized_first = _normalize_cell(row_values.get("id", ""))
        if normalized_first == "total":
            break

        if not any(str(v).strip() for v in row_values.values()):
            empty_streak += 1
            if empty_streak >= 2:
                break
            continue

        empty_streak = 0
        rows.append(row_values)

    return rows

# ============================================================
# DATE UTILITIES
# ============================================================

def normalize_date_columns(df):
    date_columns = ['Annotation Date', 'QC Date']

    for col in date_columns:
        # Clean whitespace/newlines
        cleaned = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace(r'[\r\n]+', '', regex=True)
        )

        # Parse mixed formats
        # dayfirst=False because 5/4/2026 means Month/Date/Year
        datetime_series = pd.to_datetime(
            cleaned,
            errors='coerce',
            format='mixed',
            dayfirst=False
        )

        # Convert to Day-MonthName-Year
        df[col] = datetime_series.apply(
            lambda x: f"{x.day}-{x.strftime('%B')}-{x.year}"
            if pd.notnull(x) else None
        )

    return df

def convert_to_number(value):
    if isinstance(value, str):
        value = value.strip()
        
        if value.startswith("="):
            return ""
    
    try:
        return float(value)
    
    except Exception as e:
        log(f"⚠️ Failed to convert '{value}' to number: {e}")
        return ""

def find_missing_fields(project_name, fields_to_check):
    errors = []


    for field_name, value in fields_to_check.items():
        if value is None or str(value).strip() == "":
            errors.append({
                "project": project_name,
                "issue": f"{field_name} missing",
            })

    return errors


def add_project_error(all_errors, project_name, issue):
    all_errors.append({
        "project": project_name,
        "issue": issue,
    })
    log(f"⚠️ {project_name}: {issue}")


def get_qai_id_and_status_pair(charter_url, project_name, all_errors):

    REQUIRED_COLUMNS = ["QAI ID", "Status", "Type"]

    sh = safe_open_spreadsheet_by_url(
        url=charter_url,
        project_name=project_name,
        all_errors=all_errors,
        sheet_kind="Charter sheet",
    )
    if sh is None:
        return None, all_errors

    ws = safe_get_worksheet(
        spreadsheet=sh,
        title=CHARTER_TEAM_MEMBER_DETAILS_SHEET,
        project_name=project_name,
        all_errors=all_errors,
        sheet_kind="Charter sheet",
    )
    if ws is None:
        return None, all_errors

    try:
        data = read_worksheet_values(
            ws,
            range_name="A:E",
            project_name=project_name,
            sheet_name=CHARTER_TEAM_MEMBER_DETAILS_SHEET,
        )
    except RuntimeError as e:
        add_project_error(all_errors, project_name, str(e))
        return None, all_errors
    
    if not data or len(data) < 2:
        add_project_error(all_errors, project_name, "Charter sheet is empty")
        return None, all_errors

    # Extract header and rows from list of lists
    header = [str(h).strip() for h in data[0]]
    rows = data[1:]
    
    # Check columns existence
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in header]
    if missing_cols:
        for col in missing_cols:
            add_project_error(all_errors, project_name, f"Charter sheet column missing: {col}")
        return None, all_errors

    # Find column indices
    qai_id_idx = header.index("QAI ID")
    status_idx = header.index("Status")
    type_idx = header.index("Type")
    
    qai_id_status_map = {}

    for row_num, row in enumerate(rows, start=2):  # start=2 → sheet row number
        qai_id = str(row[qai_id_idx] if qai_id_idx < len(row) else "").strip()
        status = str(row[status_idx] if status_idx < len(row) else "").strip()
        resource_type = str(row[type_idx] if type_idx < len(row) else "").strip()

        if resource_type.lower() != "remote":
            continue

        # QAI ID missing → error
        if not qai_id:
            all_errors.append({
                "project": project_name,
                "issue": f"QAI ID missing at row {row_num}"
            })
            continue

        # Status missing → log but still store mapping
        if not status:
            all_errors.append({
                "project": project_name,
                "issue": f"Status missing for QAI ID {qai_id}"
            })
            qai_id_status_map[qai_id] = ""
        else:
            qai_id_status_map[qai_id] = status

    return qai_id_status_map, all_errors

def get_bd_date():
    bd_tz = pytz.timezone("Asia/Dhaka")
    bd_now = datetime.now(bd_tz)

    today_bd_date = bd_now.date()

    return today_bd_date


def build_today_activity_report(data, qai_id_status_map, today_bd_date, project_name, all_errors):
    output_columns = [
        "QAI ID",
        "Engagement (today)",
        "Today Task Tracker Activity (Annotation)",
        "Today Task Tracker Activity (QC)",
    ]

    # convert active users set
    active_qais = {
        k for k, v in qai_id_status_map.items()
        if str(v).strip().lower() == "active"
    }

    today_str = today_bd_date.strftime("%d-%b-%Y")

    

    # filter today's rows
    today_rows = [
        row for row in data
        if str(row.get("Date", "")).strip() == today_str
    ]


    annotation_map = defaultdict(int)
    qc_map = defaultdict(int)
    present_qais = set()

    # process rows
    for row in today_rows:
        annotator = str(row.get("Annotator", "")).strip()
        reviewer = str(row.get("Reviewer", "")).strip()

        annotation_count = int(row.get("Task Count") or 0)
        qc_count = int(row.get("Review Task Count") or 0)

        # Annotation side
        if annotator:
            annotation_map[annotator] += annotation_count
            present_qais.add(annotator)

        # QC side
        if reviewer:
            qc_map[reviewer] += qc_count
            present_qais.add(reviewer)

    # build output
    out_rows = []

    # breakpoint()

    for qai in active_qais:

        # missing active QAIs in sheet
        if qai not in present_qais:
            all_errors.append({
                "project": project_name,
                "issue": f"{qai} is Active but missing in sheet for {today_str}"
            })

        out_rows.append({
            "QAI ID": qai,
            "Engagement (today)": "Yes",
            "Today Task Tracker Activity (Annotation)": annotation_map.get(qai, 0),
            "Today Task Tracker Activity (QC)": qc_map.get(qai, 0),
        })

    return pd.DataFrame(out_rows, columns=output_columns).reset_index(drop=True)

def get_accuracy_from_dashboard(sh, dashboard_name, project_name, all_errors):
    dashboard_ws = safe_get_worksheet(
        spreadsheet=sh,
        title=dashboard_name,
        project_name=project_name,
        all_errors=all_errors,
        sheet_kind="Tracker sheet",
    )
    if dashboard_ws is None:
        return {}, {}
    
    # Use ws.get() instead of get_all_records() to bypass duplicate header check
    try:
        data = read_worksheet_values(
            dashboard_ws,
            project_name=project_name,
            sheet_name=dashboard_name,
        )
    except RuntimeError as e:
        add_project_error(all_errors, project_name, str(e))
        return {}, {}
    
    if not data or len(data) < 2:
        return {}, {}
    
    # Extract header and rows
    header = [str(h).strip() for h in data[0]]
    rows = data[1:]
    
    annotator_accuracy = {}
    reviewer_accuracy = {}

    # Find column indices (handle duplicates by position)
    annotator_id_idx = None
    annotator_acc_idx = None
    reviewer_id_idx = None
    reviewer_acc_idx = None
    
    # Find first occurrences
    for i, col in enumerate(header):
        col_lower = str(col).strip().lower()
        if "annotator id" in col_lower and annotator_id_idx is None:
            annotator_id_idx = i
        elif "accuracy" in col_lower and annotator_acc_idx is None:
            annotator_acc_idx = i
        elif "reviewer id" in col_lower and reviewer_id_idx is None:
            reviewer_id_idx = i
        elif "accuracy" in col_lower and reviewer_acc_idx is None and annotator_acc_idx is not None:
            reviewer_acc_idx = i  # Second accuracy column
    
    # Process rows
    for row in rows:
        # Annotator accuracy
        if annotator_id_idx is not None and annotator_id_idx < len(row):
            annotator_id = str(row[annotator_id_idx]).strip()
            if annotator_id and annotator_acc_idx is not None and annotator_acc_idx < len(row):
                annotator_accuracy[annotator_id] = str(row[annotator_acc_idx]) + "%" if str(row[annotator_acc_idx]).strip() else ""
        
        # Reviewer accuracy
        if reviewer_id_idx is not None and reviewer_id_idx < len(row):
            reviewer_id = str(row[reviewer_id_idx]).strip()
            if reviewer_id and reviewer_acc_idx is not None and reviewer_acc_idx < len(row):
                reviewer_accuracy[reviewer_id] = str(row[reviewer_acc_idx]) + "%" if str(row[reviewer_acc_idx]).strip() else ""

    return annotator_accuracy, reviewer_accuracy

def get_tracker_data(qai_id_and_status_map, all_errors, project_name,tracker_url):
    dashboard_name = "Dashboard"

    current_date = get_bd_date()

    sh = safe_open_spreadsheet_by_url(
        url=tracker_url,
        project_name=project_name,
        all_errors=all_errors,
        sheet_kind="Tracker sheet",
    )
    if sh is None:
        return None

    ws = safe_get_worksheet(
        spreadsheet=sh,
        title=TRACKER_SUMMARY_SHEET,
        project_name=project_name,
        all_errors=all_errors,
        sheet_kind="Tracker sheet",
    )
    if ws is None:
        return None

    try:
        raw_values = read_worksheet_values(
            ws,
            project_name=project_name,
            sheet_name=TRACKER_SUMMARY_SHEET,
        )
    except RuntimeError as e:
        add_project_error(all_errors, project_name, str(e))
        return None

    data = worksheet_values_to_records(raw_values)

    activity_report_df = build_today_activity_report(
        data, qai_id_and_status_map, current_date, project_name, all_errors
    )
    if "QAI ID" not in activity_report_df.columns:
        add_project_error(
            all_errors,
            project_name,
            "Tracker activity output is missing required columns",
        )
        return None

    annotator_acc, reviewer_acc = get_accuracy_from_dashboard(
        sh,
        dashboard_name,
        project_name,
        all_errors,
    )

    activity_report_df["Annotation Accuracy (today)"] = (
        activity_report_df["QAI ID"]
        .map(annotator_acc)
        .fillna("")
    )

    activity_report_df["QC Accuracy (today)"] = (
        activity_report_df["QAI ID"]
        .map(reviewer_acc)
        .fillna("")
    )

    return activity_report_df



# def update_local_resource_pool_sheet(today_activity_df):
#     # -----------------------------
#     # Read Google Sheet
#     # -----------------------------
#     sh = gs_client.open_by_url(LOCAL_RESOURCE_POOL_URL)
#     ws = sh.worksheet("Assign")

#     data = ws.get_all_values()

#     df = pd.DataFrame(data[1:], columns=data[0])

#     # Clean dataframe
#     df = df.replace(r'^\s*$', np.nan, regex=True)
#     df = df.dropna(axis=1, how='all')

#     # Strip column names
#     df.columns = df.columns.str.strip()
#     today_activity_df.columns = today_activity_df.columns.str.strip()

#     # Remove accidental index column if present
#     today_activity_df = today_activity_df.loc[
#         :,
#         ~today_activity_df.columns.str.contains("^Unnamed")
#     ]

#     new_rows = []

#     for _, activity_row in today_activity_df.iterrows():

#         qai_id = str(activity_row["QAI ID"]).strip()

#         # Match QAI ID
#         matched_rows = df[
#             df["QAI ID"].astype(str).str.strip() == qai_id
#         ]

#         if not matched_rows.empty:

#             # Take ONLY first match
#             matched = matched_rows.iloc[0]

#             new_row = {
#                 "QAI ID": qai_id,
#                 "Full Name": matched.get("Full Name", ""),
#                 "Email": matched.get("Email", ""),
#                 "Contact": matched.get("Contact", ""),
#                 "Client Alias": activity_row.get("Client Alias",""),
#                 "Project Name": activity_row.get("Project Name",""),
#                 "Task Tracker Link": activity_row.get("Task Tracker Link",""),
#                 "Engagement (today)": activity_row.get("Engagement (today)", ""),
#                 "Task Tracker Activity (today)": (
#                     "Yes"
#                     if str(activity_row.get("Today Task Tracker Activity (Annotation)", "")).strip() not in ["", "0", "0.0", "nan", "None",0,0.0]
#                     or str(activity_row.get("Today Task Tracker Activity (QC)", "")).strip() not in ["", "0", "0.0", "nan", "None",0,0.0]
#                     else ""
#                 ),
#                 "Today Task Tracker Activity (Annotation)": activity_row.get(
#                     "Today Task Tracker Activity (Annotation)", ""
#                 ),
#                 "Today Task Tracker Activity (QC)": activity_row.get(
#                     "Today Task Tracker Activity (QC)", ""
#                 ),
#                 "Annotation Accuracy (today)": activity_row.get(
#                     "Annotation Accuracy (today)", ""
#                 ),
#                 "QC Accuracy (today)": activity_row.get(
#                     "QC Accuracy (today)", ""
#                 ),
#             }

#             new_rows.append(new_row)

#     # -----------------------------
#     # Append rows
#     # -----------------------------
#     if new_rows:

#         new_rows_df = pd.DataFrame(new_rows).fillna("")

#         headers = data[0]

#         append_values = []

#         for _, row in new_rows_df.iterrows():
#             append_values.append([
#                 row.get(col, "") for col in headers
#             ])

#         ws.append_rows(
#             append_values,
#             value_input_option="USER_ENTERED"
#         )

#         print(f"Appended {len(append_values)} rows.")

#     else:
#         print("No matching QAI IDs found.")

def update_local_resource_pool_sheet(today_activity_df, all_errors=None, project_name=""):
    ws, headers, df, header_row_idx, cached_resolved_pool_columns, qai_project_row_map, qai_first_row_map, qai_blank_project_row_map = get_local_resource_pool_context(
        all_errors=all_errors,
        project_name=project_name,
    )
    if ws is None or header_row_idx is None:
        return

    resolved_pool_columns = cached_resolved_pool_columns or {}
    missing_pool_columns = [
        col for col in ["QAI ID", "Project Name", "Full Name", "Email", "Contact"]
        if col not in resolved_pool_columns
    ]
    managed_header_positions, missing_managed_headers = resolve_header_positions(
        headers,
        [
            "QAI ID",
            "Full Name",
            "Email",
            "Contact",
            "Client Alias",
            "Project Name",
            "Task Tracker Link",
            "Engagement (today)",
            "Task Tracker Activity (today)",
            "Today Task Tracker Activity (Annotation)",
            "Today Task Tracker Activity (QC)",
            "Annotation Accuracy (today)",
            "QC Accuracy (today)",
        ],
    )
    if missing_pool_columns:
        add_project_error(
            all_errors if all_errors is not None else [],
            project_name or "Local Resource Pool",
            "Local resource pool sheet column missing: "
            + ", ".join(sorted(missing_pool_columns)),
        )
        return
    if missing_managed_headers:
        add_project_error(
            all_errors if all_errors is not None else [],
            project_name or "Local Resource Pool",
            "Local resource pool sheet column missing: "
            + ", ".join(sorted(missing_managed_headers)),
        )
        return

    qai_id_col = resolved_pool_columns["QAI ID"]
    project_name_col = resolved_pool_columns["Project Name"]
    full_name_col = resolved_pool_columns["Full Name"]
    email_col = resolved_pool_columns["Email"]
    contact_col = resolved_pool_columns["Contact"]

    today_activity_df.columns = today_activity_df.columns.str.strip()

    # Remove accidental index column if present
    today_activity_df = today_activity_df.loc[
        :,
        ~today_activity_df.columns.str.contains("^Unnamed")
    ]

    new_rows = []
    rows_to_update = []

    for _, activity_row in today_activity_df.iterrows():
        qai_id = clean_sheet_text(activity_row["QAI ID"])
        project_name = clean_sheet_text(activity_row.get("Project Name", ""))

        # Build new row data - convert NaN to empty string immediately
        new_row_data = {
            "QAI ID": qai_id,
            "Full Name": "",
            "Email": "",
            "Contact": "",
            "Client Alias": str(activity_row.get("Client Alias","")).replace("nan", ""),
            "Project Name": str(activity_row.get("Project Name","")).replace("nan", ""),
            "Task Tracker Link": str(activity_row.get("Task Tracker Link","")).replace("nan", ""),
            "Engagement (today)": str(activity_row.get("Engagement (today)", "")).replace("nan", ""),
            "Task Tracker Activity (today)": (
                "Yes"
                if str(activity_row.get("Today Task Tracker Activity (Annotation)", "")).strip() not in ["", "0", "0.0", "nan", "None",0,0.0]
                or str(activity_row.get("Today Task Tracker Activity (QC)", "")).strip() not in ["", "0", "0.0", "nan", "None",0,0.0]
                else ""
            ),
            "Today Task Tracker Activity (Annotation)": str(activity_row.get("Today Task Tracker Activity (Annotation)", "")).replace("nan", ""),
            "Today Task Tracker Activity (QC)": str(activity_row.get("Today Task Tracker Activity (QC)", "")).replace("nan", ""),
            "Annotation Accuracy (today)": str(activity_row.get("Annotation Accuracy (today)", "")).replace("nan", ""),
            "QC Accuracy (today)": str(activity_row.get("QC Accuracy (today)", "")).replace("nan", ""),
        }

        matched_idx = qai_project_row_map.get((qai_id, project_name))
        if matched_idx is not None:
            matched = df.iloc[matched_idx]
            new_row_data["Full Name"] = str(matched.get(full_name_col, "")).replace("nan", "")
            new_row_data["Email"] = str(matched.get(email_col, "")).replace("nan", "")
            new_row_data["Contact"] = str(matched.get(contact_col, "")).replace("nan", "")

            sheet_row_number = header_row_idx + matched_idx + 2
            existing_values = df.iloc[matched_idx].tolist()
            rows_to_update.append((sheet_row_number, matched_idx, new_row_data, existing_values))
        elif qai_id in qai_blank_project_row_map:
            matched_idx = qai_blank_project_row_map[qai_id]
            matched = df.iloc[matched_idx]
            new_row_data["Full Name"] = str(matched.get(full_name_col, "")).replace("nan", "")
            new_row_data["Email"] = str(matched.get(email_col, "")).replace("nan", "")
            new_row_data["Contact"] = str(matched.get(contact_col, "")).replace("nan", "")

            sheet_row_number = header_row_idx + matched_idx + 2
            existing_values = df.iloc[matched_idx].tolist()
            rows_to_update.append((sheet_row_number, matched_idx, new_row_data, existing_values))
        elif qai_id in qai_first_row_map:
            matched = df.iloc[qai_first_row_map[qai_id]]
            new_row_data["Full Name"] = str(matched.get(full_name_col, "")).replace("nan", "")
            new_row_data["Email"] = str(matched.get(email_col, "")).replace("nan", "")
            new_row_data["Contact"] = str(matched.get(contact_col, "")).replace("nan", "")
            new_rows.append(new_row_data)
        else:
            new_rows.append(new_row_data)

    # Update existing rows first
    for sheet_row_number, matched_idx, row_data, existing_values in rows_to_update:
        row_values = build_row_values_by_positions(
            headers,
            managed_header_positions,
            row_data,
            existing_values=existing_values,
        )
        update_values = clean_sheet_rows([row_values])
        ensure_worksheet_has_rows(ws, sheet_row_number)
        ws.update(
            range_name=f"A{sheet_row_number}",
            values=update_values
        )
        log(f"✅ Updated row {sheet_row_number}: {row_data['QAI ID']}")
        df.iloc[matched_idx] = update_values[0]
        current_qai_id = clean_sheet_text(row_data["QAI ID"])
        current_project_name = clean_sheet_text(row_data["Project Name"])
        qai_project_row_map[(current_qai_id, current_project_name)] = matched_idx
        if current_qai_id not in qai_first_row_map:
            qai_first_row_map[current_qai_id] = matched_idx
        qai_blank_project_row_map.pop(current_qai_id, None)

    # Append new rows
    if new_rows:
        append_values = []
        for row_data in new_rows:
            append_values.append(
                build_row_values_by_positions(
                    headers,
                    managed_header_positions,
                    row_data,
                )
            )
        append_values = clean_sheet_rows(append_values)

        next_row_number = header_row_idx + len(df) + 2
        ensure_worksheet_has_rows(ws, next_row_number + len(append_values) - 1)
        ws.update(
            range_name=f"A{next_row_number}",
            values=append_values
        )
        log(f"✅ Appended {len(append_values)} new row(s) starting at A{next_row_number}.")
        append_df = pd.DataFrame(append_values, columns=headers)
        start_idx = len(df)
        df = pd.concat([df, append_df], ignore_index=True)
        local_resource_pool_cache["df"] = df
        for offset, row_data in enumerate(new_rows):
            idx = start_idx + offset
            qai_id = clean_sheet_text(row_data["QAI ID"])
            project_name = clean_sheet_text(row_data["Project Name"])
            qai_project_row_map[(qai_id, project_name)] = idx
            if qai_id not in qai_first_row_map:
                qai_first_row_map[qai_id] = idx
            qai_blank_project_row_map.pop(qai_id, None)
    else:
        log("ℹ️ No new rows to append.")


def clear_non_running_project_activity_fields(active_project_names, all_errors=None):
    ws, headers, df, header_row_idx, _, _, _, _ = get_local_resource_pool_context(
        all_errors=all_errors,
        project_name="Local Resource Pool",
    )
    if ws is None or header_row_idx is None or df.empty:
        return

    resolved_columns, missing_columns = resolve_dataframe_columns(
        df,
        ["Project Name", *CLEAR_IF_NOT_RUNNING_HEADERS],
    )
    if missing_columns:
        add_project_error(
            all_errors if all_errors is not None else [],
            "Local Resource Pool",
            "Local resource pool sheet column missing: " + ", ".join(sorted(missing_columns)),
        )
        return

    project_name_col = resolved_columns["Project Name"]
    clear_positions, missing_clear_headers = resolve_header_positions(
        headers,
        CLEAR_IF_NOT_RUNNING_HEADERS,
    )
    if missing_clear_headers:
        add_project_error(
            all_errors if all_errors is not None else [],
            "Local Resource Pool",
            "Local resource pool sheet column missing: " + ", ".join(sorted(missing_clear_headers)),
        )
        return

    cleared_count = 0

    for idx, row in df.iterrows():
        project_name = clean_sheet_text(row.get(project_name_col, ""))
        if not project_name or project_name in active_project_names:
            continue

        row_values = df.iloc[idx].tolist()
        should_update = False

        for header, col_idx in clear_positions.items():
            current_value = clean_sheet_text(row_values[col_idx] if col_idx < len(row_values) else "")
            if current_value:
                row_values[col_idx] = ""
                should_update = True

        if not should_update:
            continue

        cleaned_row_values = clean_sheet_rows([row_values])[0]
        sheet_row_number = header_row_idx + idx + 2
        ensure_worksheet_has_rows(ws, sheet_row_number)
        ws.update(
            range_name=f"A{sheet_row_number}",
            values=[cleaned_row_values],
        )
        df.iloc[idx] = cleaned_row_values
        cleared_count += 1

    if cleared_count:
        local_resource_pool_cache["df"] = df
        local_resource_pool_cache["qai_project_row_map"], local_resource_pool_cache["qai_first_row_map"], local_resource_pool_cache["qai_blank_project_row_map"] = build_local_resource_pool_indexes(
            df,
            local_resource_pool_cache["resolved_pool_columns"],
        )
        log(f"✅ Cleared non-running project activity fields for {cleared_count} row(s) in Assign.")
    else:
        log("ℹ️ No non-running project activity fields needed clearing in Assign.")


def main():
    global resource_type_map,gs_client

    load_runtime_config()
    authenticate_google()

    tasks = filter_tasks_for_test_run(fetch_clickup_tasks())
    resource_type_map = load_resource_type_lookup()
    active_project_names = {
        str(task.get("name", "")).strip()
        for task in tasks
        if str(task.get("name", "")).strip()
    }

    all_errors = []

    total_projects = len(tasks)

    try:
        for index, task in enumerate(tqdm(tasks, desc="Processing Projects"), start=1):
            project_name = task.get("name")
            clickup_status = normalize_field_value(task.get("status", {}).get("status"))
            pdl_email = get_custom_field_value(task, "PDL Email")
            pdl = get_custom_field_value(task, "PDL")
            delivery_lead = get_custom_field_value(task, "Delivery Lead")
            pdl_for_ignore = pdl_email or pdl

            project_audit_context[project_name] = {
                "clickup_status": clickup_status,
                "pdl": pdl_email or pdl,
                "delivery_lead": delivery_lead,
            }

            if should_ignore_project_by_pdl_email(pdl_for_ignore):
                log(
                    f"⏭️ Skipping project {index}/{total_projects}: {project_name} "
                    f"(ignored PDL email: {pdl_for_ignore})"
                )
                continue

            log(f"▶️ Processing project {index}/{total_projects}: {project_name}")

            charter_url = None
            tracker_url = None
            client_alias = ""

            for f in task.get("custom_fields", []):
                if f.get("name") == CHARTER_FIELD_NAME:
                    charter_url = f.get("value")
                if f.get("name") == TRACKER_FIELD_NAME:
                    tracker_url = f.get("value")
                if f.get("name") == "Client Alias":
                    client_alias = f.get("value") or ""

            fields_to_check = {
                "charter_url": charter_url,
                "tracker_url": tracker_url,
                "client_alias": client_alias,
            }

            all_errors.extend(
                find_missing_fields(
                    project_name=project_name,
                    fields_to_check=fields_to_check
                    )

            )

            qai_id_and_status_map = None
            if charter_url:
                qai_id_and_status_map, all_errors = get_qai_id_and_status_pair(
                    charter_url=charter_url,
                    project_name=project_name,
                    all_errors=all_errors
                )
            else:
                add_project_error(
                    all_errors,
                    project_name,
                    "Skipped charter parsing because charter_url is missing",
                )

            if not tracker_url:
                add_project_error(
                    all_errors,
                    project_name,
                    "Skipped tracker processing because tracker_url is missing",
                )
                continue

            if qai_id_and_status_map is None:
                add_project_error(
                    all_errors,
                    project_name,
                    "Skipped tracker processing because charter data could not be loaded",
                )
                continue

            activity_report_df = get_tracker_data(
                qai_id_and_status_map,
                all_errors,
                project_name,
                tracker_url,
            )
            if activity_report_df is None:
                continue

            if activity_report_df.empty:
                add_project_error(
                    all_errors,
                    project_name,
                    "No activity rows were generated from the available tracker and charter data",
                )
                continue

            activity_report_df["Client Alias"] = client_alias
            activity_report_df["Project Name"] = project_name
            activity_report_df["Task Tracker Link"] = tracker_url
            
            update_local_resource_pool_sheet(
                activity_report_df,
                all_errors=all_errors,
                project_name=project_name,
            )

        clear_non_running_project_activity_fields(
            active_project_names=active_project_names,
            all_errors=all_errors,
        )
    finally:
        write_audit_log(all_errors)




            


if __name__ == "__main__":
    main()
