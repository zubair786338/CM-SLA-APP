import streamlit as st
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from datetime import datetime, timezone, date
from pathlib import Path
from dotenv import load_dotenv
import os
import json
from concurrent.futures import ThreadPoolExecutor
import io

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

ADO_ORG = os.getenv("ADO_ORG", "") or st.secrets.get("ADO_ORG", "")
ADO_PROJECT = os.getenv("ADO_PROJECT", "") or st.secrets.get("ADO_PROJECT", "")
ADO_PAT = os.getenv("ADO_PAT", "") or st.secrets.get("ADO_PAT", "")
BASE_URL = f"https://{ADO_ORG}.visualstudio.com"
AUTH = ("", ADO_PAT)
AUTO_REFRESH_SECONDS = 300

# Manual SLA overrides — ticket IDs forced to "Completed" (e.g. forgot to pause)
SLA_OVERRIDES = {
    52233,  # Forgot to set Waiting for Info
}

# ---------------------------------------------------------------------------
# Robust HTTP session with retries
# ---------------------------------------------------------------------------
def _get_session():
    """Create a requests session with automatic retries."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.auth = AUTH
    return session

HTTP = _get_session()

# Notification tracking — use session_state (survives Streamlit Cloud restarts
# within a session) + file fallback for local runs
NOTIFIED_FILE = Path(__file__).parent / ".notified_tickets.json"


def _load_notified() -> set:
    """Load the set of ticket IDs that have already been notified."""
    # Session state is primary (works on Streamlit Cloud)
    if "notified_tickets" in st.session_state:
        return set(st.session_state["notified_tickets"])
    # File fallback (works locally)
    if NOTIFIED_FILE.exists():
        try:
            data = json.loads(NOTIFIED_FILE.read_text())
            st.session_state["notified_tickets"] = data
            return set(data)
        except Exception:
            return set()
    return set()


def _save_notified(notified: set):
    """Persist the notified ticket IDs."""
    notified_list = list(notified)
    st.session_state["notified_tickets"] = notified_list
    try:
        NOTIFIED_FILE.write_text(json.dumps(notified_list))
    except Exception:
        pass  # File write may fail on Streamlit Cloud — session_state is enough


SLA_ALERT_MARKER = "SLA Alert"


def _has_existing_alert(ticket_id: int) -> bool:
    """Check if an SLA alert comment already exists on a ticket."""
    try:
        resp = HTTP.get(
            f"{BASE_URL}/{ADO_PROJECT}/_apis/wit/workitems/{ticket_id}/comments?api-version=7.1-preview.4",
            timeout=15,
        )
        if resp.status_code == 200:
            for c in resp.json().get("comments", []):
                if SLA_ALERT_MARKER in c.get("text", ""):
                    return True
        return False
    except Exception:
        return False  # If we can't check, err on the side of not posting


def _post_sla_alert(ticket_id: int, ticket_title: str, sla_status: str, remaining: int, sla_target: str, assignee_name: str = "", assignee_id: str = ""):
    """Post an SLA alert comment on an ADO work item, @mentioning the assignee."""
    if assignee_id:
        mention = f'<a href="#" data-vss-mention="version:2.0,{assignee_id}">@{assignee_name}</a>'
    else:
        mention = assignee_name or "Unassigned"
    comment = (
        f"\ud83d\udd14 <b>SLA Alert \u2014 At Risk</b><br>"
        f"This ticket is approaching its SLA deadline.<br><br>"
        f"<b>SLA Target:</b> {sla_target}<br>"
        f"<b>Remaining:</b> {remaining} business day(s)<br>"
        f"<b>Status:</b> {sla_status}<br><br>"
        f"<i>Please prioritise this ticket to meet the SLA commitment.</i><br>"
        f"cc: {mention} \u2014 Change Management SLA App"
    )
    try:
        resp = HTTP.post(
            f"{BASE_URL}/{ADO_PROJECT}/_apis/wit/workitems/{ticket_id}/comments?api-version=7.1-preview.4",
            json={"text": comment}, timeout=15,
        )
        return resp.status_code in (200, 201)
    except Exception:
        return False


def check_and_notify_at_risk(dataframe: "pd.DataFrame"):
    """Check for at-risk tickets and post a one-time comment alert."""
    at_risk_df = dataframe[dataframe["SLA_Status"].str.contains("At Risk")]
    if at_risk_df.empty:
        return

    notified = _load_notified()
    new_alerts = 0

    for _, row in at_risk_df.iterrows():
        tid = int(row["ID"])
        if tid not in notified:
            # Double-check: look for existing alert comment on the ticket
            if _has_existing_alert(tid):
                notified.add(tid)  # Already alerted, just track it
                continue
            success = _post_sla_alert(
                ticket_id=tid,
                ticket_title=row.get("Title", ""),
                sla_status=row["SLA_Status"],
                remaining=int(row["Remaining_BDays"]),
                sla_target=row.get("SLA_Display", "N/A"),
                assignee_name=row.get("AssignedToFull", ""),
                assignee_id=row.get("AssignedToId", ""),
            )
            if success:
                notified.add(tid)
                new_alerts += 1

    if new_alerts > 0:
        _save_notified(notified)

# ---------------------------------------------------------------------------
# Team Mapping  (Category prefix -> display name)
# ---------------------------------------------------------------------------
TEAM_MAP = {
    "PS": "Performance Solutions",
    "Acquisition": "Acquisition & Growth",
    "SMB": "SMB",
    "MATS": "MATS",
    "Windows Store": "Windows Store",
    "AD": "Agency Development",
}


def resolve_team(category: str) -> str:
    if not category:
        return "Other"
    for prefix, team in TEAM_MAP.items():
        if category.strip().upper().startswith(prefix.upper()):
            return team
    return "Other"


# ---------------------------------------------------------------------------
# Assignee matching
# ---------------------------------------------------------------------------
ASSIGNEE_OPTIONS = [
    "Shanthi Sravanakumar",
    "Alioune Ba",
    "Zubair Patel",
    "James Libby",
    "Weemor Randolph",
]


def match_assignee(full_name: str) -> str:
    for short in ASSIGNEE_OPTIONS:
        if full_name.strip().lower().startswith(short.lower()):
            return short
    return full_name


# ---------------------------------------------------------------------------
# Scenario mapping  (ADO SubType -> display scenario)
# ---------------------------------------------------------------------------
SCENARIO_MAP = {
    # General CM
    "Grouping or HM": "Customer Grouping / Hierarchy Management",
    "Personnel": "Personnel Changes",
    "SL": "Owned-By / Agency / Service Location Override",
    "Reparenting": "Owned-By / Agency / Service Location Override",
    "Book Assignment": "Book Assignment Update",
    "Book update in Dynamics": "Book Assignment Update",
    "Book update in Tech": "Book Assignment Update",
    "Quota Move": "Quota Moves",
    "Quota Moves": "Quota Moves",
    "Channel Partner": "Channel Partner Linkages",
    # A&G
    "Valid Win": "Weekly Win Processing",
    "Win Override": "Acquisition Win Override",
    "Growth MPM": "Pre/Post Growth MPM Assignment",
    # Sales Houses
    "New Client Nomination": "New Client Nomination Processing",
    "Hierarchy Mapping": "Hierarchy Mapping",
    "New Joiner Mapping": "Mapping New Joiners to Sales Houses",
    # SMB
    "Bad Agency Setup": "Bad Agency Setup",
    "Missing Contacts": "Missing Contacts",
    "Unengaged": "Unengaged / Inactive Clients",
}


def resolve_scenario(sub_type: str) -> str:
    if not sub_type or sub_type == "Unknown":
        return "Other"
    for key, scenario in SCENARIO_MAP.items():
        if key.lower() in sub_type.lower() or sub_type.lower() in key.lower():
            return scenario
    return sub_type


# ---------------------------------------------------------------------------
# Scenario SLA definitions
# ---------------------------------------------------------------------------
SCENARIO_SLA = {
    "Customer Grouping / Hierarchy Management": {
        "default": 4,
        "description": "4 working days",
        "info": (
            "- Eligible evidence\n"
            "- MAN's needing to be grouped & assigned\n"
            "- Existing UCGID or client currently assigned\n"
            "- Full Account Team information\n"
            "- Effective Month\n"
            "- Answers to any follow-up questions"
        ),
    },
    "Personnel Changes": {
        "default": 3,
        "description": "3 working days",
        "info": (
            "- Personnel Change Type\n"
            "- For new joiners – new BoB\n"
            "- For leavers – coverage for BoB\n"
            "- For LOA – coverage for LOA\n"
            "- Effective date\n"
            "- Updates to D&V offline quota contracts (if applicable)\n"
            "- New joiners need UCM access (if applicable)"
        ),
    },
    "Owned-By / Agency / Service Location Override": {
        "default": 2,
        "description": "2 working days",
        "info": (
            "- XID's / CID's impacted\n"
            "- For agency overrides – current and new agency info\n"
            "- For Owned-By – current and new MAN info\n"
            "- Reason for change\n"
            "- Answers to any follow-up questions"
        ),
    },
    "Channel Partner Linkages": {
        "default": 3,
        "description": "3 days to communicate + 2 days to reassign",
        "info": (
            "- Confirmation of when client has returned to original segment "
            "in UCMA after delinking for CM team to reassign."
        ),
    },
    "Book Assignment Update": {
        "default": 2,
        "MATS": 4,
        "description": "2 working days (PS) / 4 working days (MATS)",
        "info": (
            "- Simple BoB update within business rules\n"
            "- MATS may require longer SLA – reliant on other teams"
        ),
    },
    "Quota Moves": {
        "default": 3,
        "description": "3 working days",
        "info": (
            "- XID's quota is moving to and from\n"
            "- Amount of quota and monthly split"
        ),
    },
    "Weekly Win Processing": {
        "default": 3,
        "description": "Submit by Friday EOD → processed Monday → reflects Wednesday EOD",
        "info": (
            "- Acquisition AE's submit weekly wins by Friday EOD\n"
            "- Win processed in upcoming BoB upload"
        ),
    },
    "Acquisition Win Override": {
        "default": 5,
        "description": "Processed by Monday once full info received",
        "info": (
            "- Advertiser details of invalid win\n"
            "- MSX Opportunity ID\n"
            "- Evidence & reasoning for override\n"
            "- Must be submitted by Friday EOD"
        ),
    },
    "Pre/Post Growth MPM Assignment": {
        "default": 2,
        "description": "2 working days",
        "info": (
            "- Advertiser information\n"
            "- Pre or Post Qualified Win\n"
            "- Growth team assignment\n"
            "- Effective date (month)"
        ),
    },
    "New Client Nomination Processing": {
        "default": 5,
        "description": "Weekly basis up to 25th of month",
        "info": "- Nomination through Athena with all required fields (CID, etc.)",
    },
    "Hierarchy Mapping": {
        "default": 5,
        "description": "Weekly basis up to 25th of month",
        "info": "- Nomination through Athena with CID and Sales House.",
    },
    "Mapping New Joiners to Sales Houses": {
        "default": 3,
        "description": "3 working days",
        "info": "- Confirmation of alias and Sales House mapping.",
    },
    "Bad Agency Setup": {
        "default": 5,
        "description": "5 working days",
        "info": (
            "- Agency / XID info\n"
            "- Assignment information\n"
            "- All client information"
        ),
    },
    "Missing Contacts": {
        "default": 5,
        "description": "5 working days",
        "info": (
            "- Client info (MAN / Adv Name)\n"
            "- CM team to reach out to Sales leads"
        ),
    },
    "Unengaged / Inactive Clients": {
        "default": 5,
        "description": "5 working days",
        "info": (
            "- Client info (MAN / Adv Name)\n"
            "- Number of outreaches"
        ),
    },
}

DEFAULT_SLA = {"default": 5, "description": "5 working days", "info": "N/A"}


# Team-level SLA overrides: {team: {scenario_keyword: days, "default": days}}
# "default" is the fallback for that team if no scenario match.
TEAM_SLA_OVERRIDES = {
    "SMB": {"default": 5},
    "Windows Store": {"default": 3},
    "Acquisition & Growth": {
        "Book Assignment": 2,
        "Grouping or HM": 4,
        "Customer Grouping / Hierarchy Management": 4,
        # Win Override / Weekly Win Processing handled via Monday deadline
    },
}


def get_sla_days(scenario: str, team: str) -> int:
    # Check team-level overrides first
    team_overrides = TEAM_SLA_OVERRIDES.get(team)
    if team_overrides:
        # Flat SLA for entire team (SMB, Windows Store)
        if len(team_overrides) == 1 and "default" in team_overrides:
            return team_overrides["default"]
        # Scenario-specific override within team (Acquisition)
        for key, val in team_overrides.items():
            if key != "default" and key.lower() in scenario.lower():
                return val
        if "default" in team_overrides:
            return team_overrides["default"]
    # Fall back to scenario-level SLA definitions
    sla = SCENARIO_SLA.get(scenario, DEFAULT_SLA)
    for key, val in sla.items():
        if key not in ("default", "description", "info") and isinstance(val, int):
            if key.upper() in team.upper():
                return val
    return sla.get("default", 5)


# ---------------------------------------------------------------------------
# Business-day helpers (US holiday-aware)
# ---------------------------------------------------------------------------
# Scenarios with a "next Monday" SLA deadline
MONDAY_DEADLINE_SCENARIOS = {"Acquisition Win Override", "Weekly Win Processing"}

# US public holidays observed by Microsoft
# Generated for 2025-2027; extend as needed
US_HOLIDAYS = set()

def _third_monday(year, month):
    """Return the third Monday of a given month."""
    d = pd.Timestamp(year=year, month=month, day=1)
    # Advance to first Monday
    while d.weekday() != 0:
        d += pd.Timedelta(days=1)
    return d + pd.Timedelta(weeks=2)

def _last_monday(year, month):
    """Return the last Monday of a given month."""
    d = pd.Timestamp(year=year, month=month, day=28)
    while d.month == month:
        d += pd.Timedelta(days=1)
    d -= pd.Timedelta(days=1)  # last day of month
    while d.weekday() != 0:
        d -= pd.Timedelta(days=1)
    return d

def _fourth_thursday(year, month):
    """Return the fourth Thursday of a given month."""
    d = pd.Timestamp(year=year, month=month, day=1)
    while d.weekday() != 3:
        d += pd.Timedelta(days=1)
    return d + pd.Timedelta(weeks=3)

for yr in range(2025, 2028):
    US_HOLIDAYS.update([
        pd.Timestamp(yr, 1, 1).normalize(),           # New Year's Day
        _third_monday(yr, 1).normalize(),              # MLK Day
        _third_monday(yr, 2).normalize(),              # Presidents' Day
        _last_monday(yr, 5).normalize(),               # Memorial Day
        pd.Timestamp(yr, 6, 19).normalize(),           # Juneteenth
        pd.Timestamp(yr, 7, 4).normalize(),            # Independence Day
        pd.Timestamp(yr, 9, 1).normalize() + pd.Timedelta(days=(0 - pd.Timestamp(yr, 9, 1).weekday()) % 7),  # Labor Day (1st Monday Sep)
        pd.Timestamp(yr, 11, 11).normalize(),          # Veterans Day
        _fourth_thursday(yr, 11).normalize(),           # Thanksgiving
        _fourth_thursday(yr, 11).normalize() + pd.Timedelta(days=1),  # Day after Thanksgiving
        pd.Timestamp(yr, 12, 25).normalize(),          # Christmas
    ])


def _is_business_day(dt):
    """Check if a date is a business day (Mon-Fri, not a US holiday)."""
    if dt.weekday() >= 5:
        return False
    # Strip timezone for comparison with tz-naive holiday set
    check_date = dt.normalize().tz_localize(None) if dt.tz else dt.normalize()
    return check_date not in US_HOLIDAYS


def next_monday(dt):
    """Return the next Monday at EOD (23:59) after the given timestamp."""
    if pd.isna(dt):
        return pd.NaT
    days_ahead = (7 - dt.weekday()) % 7  # 0 = Monday
    if days_ahead == 0:
        days_ahead = 7  # if submitted on Monday, deadline is *next* Monday
    return (dt + pd.Timedelta(days=days_ahead)).normalize() + pd.Timedelta(hours=23, minutes=59)


def add_business_days(start, n):
    """Add n business days (excluding weekends and US holidays)."""
    if pd.isna(start) or n is None:
        return pd.NaT
    current = start
    added = 0
    while added < n:
        current += pd.Timedelta(days=1)
        if _is_business_day(current):
            added += 1
    return current


def business_days_between(start, end):
    """Count business days between two dates (excluding weekends and US holidays)."""
    if pd.isna(start) or pd.isna(end):
        return 0
    s = start.normalize()
    e = end.normalize()
    if s >= e:
        return 0
    count = 0
    current = s + pd.Timedelta(days=1)
    while current <= e:
        if _is_business_day(current):
            count += 1
        current += pd.Timedelta(days=1)
    return count


# ---------------------------------------------------------------------------
# Page Config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Change Management - SLA App",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    :root {
        --bg-primary: #fafafa;
        --bg-card: #ffffff;
        --bg-card-hover: #f5f5f5;
        --bg-elevated: #f0f0f0;
        --text-primary: #1a1a1a;
        --text-secondary: #6b6b6b;
        --text-muted: #999;
        --accent-green: #16a34a;
        --accent-amber: #d97706;
        --accent-red: #dc2626;
        --accent-blue: #2563eb;
        --accent-purple: #7c3aed;
        --border: #e5e5e5;
        --border-subtle: #f0f0f0;
    }

    /* ── App shell ── */
    .stApp {
        background: var(--bg-primary);
        color: var(--text-primary);
        font-family: 'Inter', sans-serif;
    }
    [data-testid="stHeader"] { background: transparent; }
    header { background: transparent !important; }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background: var(--bg-card) !important;
        border-right: 1px solid var(--border) !important;
    }
    [data-testid="stSidebar"] > div:first-child { padding-top: 1rem; }
    [data-testid="stSidebarContent"] label {
        font-family: 'Inter', sans-serif !important;
        font-size: 0.82rem !important;
        color: var(--text-secondary) !important;
    }
    [data-testid="stSidebar"] .stTextInput input {
        font-family: 'Inter', sans-serif;
        font-size: 0.82rem;
        border-radius: 0.5rem;
        border: 1px solid var(--border);
    }
    [data-testid="stSidebar"] .stButton > button {
        font-size: 0.8rem;
        border-radius: 0.5rem;
    }
    .sidebar-section-title {
        font-family: 'Inter', sans-serif;
        font-size: 0.68rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: var(--text-muted);
        margin: 0.5rem 0 0.25rem;
    }

    /* ── Compact app title ── */
    .app-title {
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        font-size: 1.5rem;
        color: var(--text-primary);
        letter-spacing: -0.02em;
        margin: 0;
        padding: 0.3rem 0 0.1rem;
        line-height: 1.2;
    }

    /* ── Compliance bar ── */
    .compliance-bar-wrap {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 0.4rem 0;
        font-family: 'Inter', sans-serif;
        font-size: 0.78rem;
        color: var(--text-secondary);
    }
    .compliance-bar-bg {
        width: 160px;
        height: 5px;
        background: var(--border);
        border-radius: 4px;
        overflow: hidden;
    }
    .compliance-bar-fill {
        height: 100%;
        background: linear-gradient(90deg, #16a34a, #4ade80);
        border-radius: 4px;
    }

    /* ── Scenario section headers ── */
    .scenario-header {
        background: var(--bg-card);
        border: 1px solid var(--border);
        padding: 0.9rem 1.1rem;
        border-radius: 0.75rem;
        color: var(--text-primary);
        margin-bottom: 0.4rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        transition: box-shadow 0.2s ease;
    }
    .scenario-header:hover { box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
    .scenario-header h3 {
        margin: 0;
        font-size: 0.9rem;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        color: var(--text-primary);
        letter-spacing: -0.01em;
    }
    .scenario-header p {
        margin: 0.2rem 0 0 0;
        font-size: 0.72rem;
        color: var(--text-secondary);
        font-family: 'Inter', sans-serif;
        font-weight: 400;
    }

    /* ── KPI cards ── */
    .kpi-card {
        background: var(--bg-card);
        border: 1px solid var(--border);
        padding: 0.85rem 1rem;
        border-radius: 0.75rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        position: relative;
        overflow: hidden;
    }
    .kpi-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        background: linear-gradient(90deg, var(--accent-blue), var(--accent-purple));
        border-radius: 0.75rem 0.75rem 0 0;
    }
    .kpi-card:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(0,0,0,0.08); }
    .kpi-card .kpi-icon { font-size: 1.1rem; margin-bottom: 0.2rem; display: block; }
    .kpi-card h2 {
        margin: 0;
        font-size: 1.75rem;
        color: var(--text-primary);
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        letter-spacing: -0.03em;
        line-height: 1;
    }
    .kpi-card p {
        margin: 0.2rem 0 0 0;
        font-size: 0.65rem;
        color: var(--text-secondary);
        font-family: 'Inter', sans-serif;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    .kpi-green::before  { background: linear-gradient(90deg, #16a34a, #4ade80); }
    .kpi-orange::before { background: linear-gradient(90deg, #d97706, #fbbf24); }
    .kpi-red::before    { background: linear-gradient(90deg, #dc2626, #f87171); }
    .kpi-blue::before   { background: linear-gradient(90deg, #2563eb, #60a5fa); }

    /* ── Expander ── */
    .streamlit-expanderHeader {
        font-family: 'Inter', sans-serif;
        color: var(--text-secondary);
        font-size: 0.82rem;
    }

    /* ── Dataframes ── */
    [data-testid="stDataFrame"] {
        border-radius: 0.75rem;
        overflow: hidden;
        border: 1px solid var(--border);
    }

    /* ── Captions ── */
    .stCaption, small { font-family: 'Inter', sans-serif; color: var(--text-muted); }

    /* ── Buttons — ghost style ── */
    .stButton > button {
        background: var(--bg-card);
        border: 1px solid var(--border);
        color: var(--text-primary);
        border-radius: 0.5rem;
        font-family: 'Inter', sans-serif;
        font-weight: 500;
        font-size: 0.8rem;
        transition: all 0.15s ease;
    }
    .stButton > button:hover {
        background: var(--bg-elevated);
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }

    /* ── Markdown ── */
    .stMarkdown, .stMarkdown p {
        color: var(--text-primary);
        font-family: 'Inter', sans-serif;
    }

    /* ── Dividers ── */
    hr { border-color: var(--border) !important; opacity: 0.5; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Data Fetching
# ---------------------------------------------------------------------------
DETAIL_FIELDS = ",".join([
    "System.Id", "System.Title", "System.State", "System.CreatedDate",
    "System.AssignedTo", "System.AreaPath",
    "Microsoft.VSTS.Common.Priority", "Microsoft.VSTS.Common.ClosedDate",
    "Microsoft.VSTS.Common.StateChangeDate",
    "Custom.FeatureDescription", "Custom.State1", "Custom.Category",
    "Custom.RequesterName", "Custom.RequesterTeam", "Custom.EndDate",
    "Custom.Reactivated", "Custom.GeneratedbyIntakeForm",
    "Microsoft.VSTS.Scheduling.StartDate",
])


def _fetch_details(ids_list, as_of_iso=None):
    all_items = []
    for i in range(0, len(ids_list), 200):
        batch_ids = ",".join(str(x) for x in ids_list[i:i + 200])
        url = (
            f"{BASE_URL}/_apis/wit/workitems"
            f"?ids={batch_ids}&fields={DETAIL_FIELDS}&api-version=7.1"
        )
        if as_of_iso:
            url += f"&asOf={as_of_iso}"
        try:
            resp = HTTP.get(url, timeout=30)
            resp.raise_for_status()
            all_items.extend(resp.json().get("value", []))
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                st.error("\u26a0\ufe0f **Authentication failed.** Your PAT may have expired. Please generate a new one and update it.")
                st.stop()
            raise
        except requests.exceptions.ConnectionError:
            st.error("\u26a0\ufe0f **Connection error.** Could not reach Azure DevOps. Please check your network and try again.")
            st.stop()
        except requests.exceptions.Timeout:
            st.warning("\u23f3 Azure DevOps is taking too long to respond. Retrying...")
            continue
    return all_items


# ---------------------------------------------------------------------------
# Paused-time accounting — fetch state-change history from ADO Updates API
# ---------------------------------------------------------------------------
_PAUSED_STATES = {"waiting for info", "pending lockdown", "waiting validation"}
_REACTIVATION_STATES = {"cancelled", "completed"}  # SLA resets when leaving these states


@st.cache_data(ttl=AUTO_REFRESH_SECONDS, show_spinner=False)
def _fetch_history_map(_ids_tuple):
    """Return {item_id: {"paused_days": int, "reactivation_date": Timestamp|None}}."""

    def _get_history(item_id):
        try:
            resp = HTTP.get(
                f"{BASE_URL}/{ADO_PROJECT}/_apis/wit/workitems/{item_id}/updates"
                f"?api-version=7.1",
                timeout=30,
            )
            if resp.status_code != 200:
                return (item_id, {"paused_days": 0, "reactivation_date": None})
            updates = resp.json().get("value", [])
            paused_total = 0
            pause_start = None
            last_reactivation = None
            for u in updates:
                state_field = u.get("fields", {}).get("System.State", {})
                if not state_field:
                    continue
                new_val = (state_field.get("newValue") or "").lower()
                old_val = (state_field.get("oldValue") or "").lower()
                changed = (
                    u.get("fields", {})
                    .get("System.ChangedDate", {})
                    .get("newValue")
                )
                if not changed:
                    continue
                ts = pd.Timestamp(changed)
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                else:
                    ts = ts.tz_convert("UTC")
                # Entering a paused state
                if new_val in _PAUSED_STATES and old_val not in _PAUSED_STATES:
                    pause_start = ts
                # Leaving a paused state
                elif old_val in _PAUSED_STATES and new_val not in _PAUSED_STATES:
                    if pause_start is not None:
                        paused_total += business_days_between(pause_start, ts)
                        pause_start = None
                # Reactivation: leaving Cancelled/Completed for an active state
                if old_val in _REACTIVATION_STATES and new_val not in _REACTIVATION_STATES:
                    last_reactivation = ts
                    # Reset paused counters — only count pauses after reactivation
                    paused_total = 0
                    pause_start = None
            # If still paused, count up to now
            if pause_start is not None:
                paused_total += business_days_between(
                    pause_start, pd.Timestamp.now(tz=timezone.utc)
                )
            return (item_id, {"paused_days": paused_total, "reactivation_date": last_reactivation})
        except Exception:
            return (item_id, {"paused_days": 0, "reactivation_date": None})

    result = {}
    with ThreadPoolExecutor(max_workers=30) as pool:
        for item_id, info in pool.map(
            _get_history, _ids_tuple
        ):
            result[item_id] = info
    return result


def _parse_items(all_items, ref_time):
    rows = []
    for item in all_items:
        f = item.get("fields", {})
        assigned = f.get("System.AssignedTo", {})
        assigned_name = (
            assigned.get("displayName", "Unassigned")
            if isinstance(assigned, dict) else "Unassigned"
        )
        assigned_id = (
            assigned.get("id", "")
            if isinstance(assigned, dict) else ""
        )
        raw_cat = f.get("Custom.Category", "") or ""
        raw_sub = f.get("Custom.State1", "") or ""

        rows.append({
            "ID": f.get("System.Id"),
            "Title": f.get("System.Title", ""),
            "State": f.get("System.State", "Unknown"),
            "Priority": f.get("Microsoft.VSTS.Common.Priority", 0),
            "CreatedDate": f.get("System.CreatedDate"),
            "ClosedDate": f.get("Microsoft.VSTS.Common.ClosedDate"),
            "EndDate": f.get("Custom.EndDate"),
            "StartDate": f.get("Microsoft.VSTS.Scheduling.StartDate"),
            "AssignedToFull": assigned_name,
            "AssignedTo": match_assignee(assigned_name),
            "AssignedToId": assigned_id,
            "AreaPath": f.get("System.AreaPath", ""),
            "RequestType": f.get("Custom.FeatureDescription", "Unknown"),
            "SubType": raw_sub,
            "RawCategory": raw_cat,
            "Team": resolve_team(raw_cat),
            "Scenario": resolve_scenario(raw_sub),
            "RequesterName": f.get("Custom.RequesterName", ""),
            "StateChangeDate": f.get("Microsoft.VSTS.Common.StateChangeDate"),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    for col in ["CreatedDate", "ClosedDate", "EndDate", "StartDate", "StateChangeDate"]:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    df = df.dropna(subset=["CreatedDate"]).reset_index(drop=True)

    df["IsOpen"] = ~df["State"].isin(["Completed", "Cancelled"])
    df["CreatedDay"] = df["CreatedDate"].dt.tz_convert("America/Los_Angeles").dt.normalize().dt.date

    # For "Waiting for Info" and "Pending Lockdown" tickets, pause the SLA clock.
    df["IsPaused"] = df["State"].str.lower().str.contains("waiting for info|pending lockdown|waiting validation", na=False, regex=True)

    # ── Fetch history (paused time + reactivation dates) from ADO Updates API ──
    history_map = _fetch_history_map(tuple(df["ID"].tolist()))
    df["Paused_BDays"] = df["ID"].map(lambda i: history_map.get(i, {}).get("paused_days", 0)).astype(int)
    df["ReactivationDate"] = df["ID"].map(lambda i: history_map.get(i, {}).get("reactivation_date"))
    df["ReactivationDate"] = pd.to_datetime(df["ReactivationDate"], utc=True, errors="coerce")

    # For reactivated tickets, use the reactivation date as the effective start
    df["EffectiveStart"] = df.apply(
        lambda r: r["ReactivationDate"] if pd.notna(r["ReactivationDate"]) else r["CreatedDate"], axis=1
    )

    # Recompute SLA days and deadline from effective start
    df["SLA_Days"] = df.apply(lambda r: get_sla_days(r["Scenario"], r["Team"]), axis=1)
    df["IsMondayDeadline"] = (
        df["Scenario"].isin(MONDAY_DEADLINE_SCENARIOS)
        & (df["Team"] == "Acquisition & Growth")
    )

    def compute_deadline(row):
        if row["IsMondayDeadline"]:
            return next_monday(row["EffectiveStart"])
        return add_business_days(row["EffectiveStart"], row["SLA_Days"])

    df["SLA_Deadline"] = df.apply(compute_deadline, axis=1)
    df["SLA_Deadline"] = pd.to_datetime(df["SLA_Deadline"], utc=True)

    # Adjust SLA deadline & SLA days by adding paused business days
    def adjust_deadline(row):
        if row["Paused_BDays"] > 0:
            return add_business_days(row["SLA_Deadline"], row["Paused_BDays"])
        return row["SLA_Deadline"]

    df["SLA_Deadline"] = df.apply(adjust_deadline, axis=1)
    df["SLA_Deadline"] = pd.to_datetime(df["SLA_Deadline"], utc=True)
    df["SLA_Days"] = df["SLA_Days"] + df["Paused_BDays"]

    def calc_elapsed(row):
        start = row["EffectiveStart"]
        if not row["IsOpen"]:
            end = row["EndDate"] if pd.notna(row["EndDate"]) else (
                row["ClosedDate"] if pd.notna(row["ClosedDate"]) else ref_time
            )
        else:
            end = ref_time
        return business_days_between(start, end)

    df["Elapsed_BDays"] = df.apply(calc_elapsed, axis=1)

    # Remaining: for Monday-deadline scenarios, count business days left to the deadline
    # For standard scenarios, use SLA_Days - Elapsed
    def calc_remaining(row):
        if row["IsMondayDeadline"]:
            if not row["IsOpen"]:
                end = row["EndDate"] if pd.notna(row["EndDate"]) else (
                    row["ClosedDate"] if pd.notna(row["ClosedDate"]) else ref_time
                )
                return business_days_between(end, row["SLA_Deadline"]) if end <= row["SLA_Deadline"] else -business_days_between(row["SLA_Deadline"], end)
            else:
                return business_days_between(ref_time, row["SLA_Deadline"])
        else:
            return row["SLA_Days"] - row["Elapsed_BDays"]

    df["Remaining_BDays"] = df.apply(calc_remaining, axis=1)

    def sla_status(row):
        if not row["IsOpen"]:
            if row["IsMondayDeadline"]:
                end = row["EndDate"] if pd.notna(row["EndDate"]) else (
                    row["ClosedDate"] if pd.notna(row["ClosedDate"]) else ref_time
                )
                return "✅ Completed" if end <= row["SLA_Deadline"] else "⚠️ Completed Late"
            return ("✅ Completed" if row["Elapsed_BDays"] <= row["SLA_Days"]
                    else "⚠️ Completed Late")
        if row["IsPaused"]:
            return "⏸️ Paused"
        if row["Remaining_BDays"] > 1:
            return "🟢 On Track"
        elif row["Remaining_BDays"] >= 0:
            return "🟡 At Risk"
        else:
            return "🔴 Breached"

    df["SLA_Status"] = df.apply(sla_status, axis=1)

    # Apply manual overrides
    df.loc[
        (df["ID"].isin(SLA_OVERRIDES)) & (~df["IsOpen"]),
        "SLA_Status",
    ] = "✅ Completed"

    # Cancelled tickets are always marked as Completed
    df.loc[
        df["State"] == "Cancelled",
        "SLA_Status",
    ] = "✅ Completed"

    # Show deadline date as SLA target (same format as Submitted column)
    df["SLA_Display"] = df["SLA_Deadline"].apply(
        lambda d: d.strftime("%Y-%m-%d") if pd.notna(d) else "N/A"
    )

    return df


@st.cache_data(ttl=AUTO_REFRESH_SECONDS)
def fetch_work_items(days_back=365):
    wiql = {
        "query": f"""
            SELECT [System.Id]
            FROM WorkItems
            WHERE [System.TeamProject] = '{ADO_PROJECT}'
              AND [System.WorkItemType] = 'Product Backlog Item'
              AND [System.AreaPath] UNDER '{ADO_PROJECT}\\Change Management'
              AND [System.CreatedDate] >= @Today - {days_back}
            ORDER BY [System.CreatedDate] DESC
        """
    }
    try:
        resp = HTTP.post(
            f"{BASE_URL}/{ADO_PROJECT}/_apis/wit/wiql?api-version=7.1",
            json=wiql, timeout=30,
        )
        if resp.status_code == 401:
            st.error(
                "\u26a0\ufe0f **Authentication failed.** Your ADO Personal Access Token (PAT) may have expired.\n\n"
                "**To fix:** Generate a new PAT at "
                f"[{ADO_ORG}.visualstudio.com/_usersSettings/tokens](https://{ADO_ORG}.visualstudio.com/_usersSettings/tokens) "
                "and update it in your secrets/environment."
            )
            st.stop()
        resp.raise_for_status()
        refs = resp.json().get("workItems", [])
        # DEBUG — remove after fixing cloud issue
        st.session_state["_debug_wiql_count"] = len(refs)
        if not refs:
            return pd.DataFrame()
        items = _fetch_details([w["id"] for w in refs])
        st.session_state["_debug_detail_count"] = len(items)
        return _parse_items(items, pd.Timestamp.now(tz=timezone.utc))
    except requests.exceptions.ConnectionError:
        st.error(
            "\u26a0\ufe0f **Cannot connect to Azure DevOps.** \n\n"
            "This could be a network issue or ADO may be down. "
            "Please try refreshing in a minute."
        )
        st.stop()
    except requests.exceptions.Timeout:
        st.error(
            "\u23f3 **Request timed out.** Azure DevOps is not responding. "
            "Please try refreshing."
        )
        st.stop()
    except Exception as e:
        st.error(f"\u274c **Unexpected error:** {str(e)}")
        st.stop()


# ---------------------------------------------------------------------------
# Startup Validation
# ---------------------------------------------------------------------------
if not ADO_ORG or not ADO_PROJECT or not ADO_PAT:
    st.error(
        "**Configuration missing.** The app needs the following settings to connect to Azure DevOps:\n\n"
        "- `ADO_ORG`\n- `ADO_PROJECT`\n- `ADO_PAT`\n\n"
        "Set these in your `.env` file (local) or Streamlit secrets (cloud)."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Fetch data (needed before sidebar for dynamic filter options)
# ---------------------------------------------------------------------------
df_all = fetch_work_items(days_back=365)

# DEBUG — remove after confirming cloud works
with st.expander("🔍 Debug Info (remove later)", expanded=False):
    st.write(f"**ADO_ORG:** `{ADO_ORG}`")
    st.write(f"**ADO_PROJECT:** `{ADO_PROJECT}`")
    pat_preview = f"{ADO_PAT[:4]}...{ADO_PAT[-4:]}" if ADO_PAT and len(ADO_PAT) > 8 else "???"
    st.write(f"**PAT set:** `{bool(ADO_PAT)}` (length: {len(ADO_PAT) if ADO_PAT else 0}, preview: `{pat_preview}`)")
    st.write(f"**WIQL returned:** `{st.session_state.get('_debug_wiql_count', '?')}` work item refs")
    st.write(f"**Detail fetch returned:** `{st.session_state.get('_debug_detail_count', '?')}` items")
    st.write(f"**df_all shape:** `{df_all.shape if not df_all.empty else 'EMPTY'}`")
    if not df_all.empty:
        st.write(f"**States:** `{df_all['State'].value_counts().to_dict()}`")
        st.write(f"**Teams:** `{df_all['Team'].value_counts().to_dict()}`")
        st.write(f"**IsOpen count:** `{df_all['IsOpen'].sum()}`")

if df_all.empty:
    st.error("No work items found. Check your ADO connection.")
    st.stop()

# ---------------------------------------------------------------------------
# Sidebar — persistent filters
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown('<p style="font-family:\'Inter\',sans-serif;font-weight:700;font-size:1rem;color:#1a1a1a;margin-bottom:0.6rem;">📋 CM SLA Filters</p>', unsafe_allow_html=True)

    search_query = st.text_input(
        "Search",
        placeholder="🔍  Ticket ID, title, assignee…",
        label_visibility="collapsed",
    )

    st.divider()

    # 1) View Mode
    view_mode = st.radio(
        "View Mode",
        ["📸 Live View", "📅 Date Range"],
        index=0,
        help="**Live View** shows current open tickets. **Date Range** filters by submission date.",
    )
    if view_mode.startswith("📅"):
        dr = st.date_input(
            "Date Range",
            value=(date(2026, 1, 1), date.today()),
            label_visibility="collapsed",
        )
    else:
        dr = None

    st.divider()

    # 2) Team
    st.markdown('<p class="sidebar-section-title">Team</p>', unsafe_allow_html=True)
    team_options = ["Performance Solutions", "Acquisition & Growth", "SMB", "MATS", "Windows Store", "Agency Development"]
    team_checks = {}
    for t in team_options:
        team_checks[t] = st.checkbox(t, value=True, key=f"team_{t}")
    team_filter = [t for t, checked in team_checks.items() if checked]

    st.divider()

    # 3) Scenario — dynamic from data
    st.markdown('<p class="sidebar-section-title">Scenario</p>', unsafe_allow_html=True)
    available_scenarios = sorted(df_all["Scenario"].dropna().unique())
    scenario_checks = {}
    for s in available_scenarios:
        scenario_checks[s] = st.checkbox(s, value=True, key=f"scenario_{s}")
    scenario_filter = [s for s, checked in scenario_checks.items() if checked]

    st.divider()

    # 4) Assignee — dynamic from data
    st.markdown('<p class="sidebar-section-title">Assignee</p>', unsafe_allow_html=True)
    all_assignees = sorted(df_all["AssignedTo"].dropna().unique())
    assignee_checks = {}
    for a in all_assignees:
        assignee_checks[a] = st.checkbox(a, value=True, key=f"assignee_{a}")
    assignee_filter = [a for a, checked in assignee_checks.items() if checked]

    st.divider()

    if st.button("↻  Refresh", use_container_width=True, help="Reload data from Azure DevOps"):
        st.cache_data.clear()
        st.rerun()

# ---------------------------------------------------------------------------
# Compact header
# ---------------------------------------------------------------------------
st.markdown('<p class="app-title">Change Management SLA App</p>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Apply Filters
# ---------------------------------------------------------------------------
df = df_all.copy()
df = df[df["Team"].isin(team_filter)]
df = df[df["Scenario"].isin(scenario_filter)]

# Assignee: filter only when user has deselected someone
df = df[df["AssignedTo"].isin(assignee_filter)]

# Search query (sidebar)
if search_query:
    sq = search_query.strip()
    mask = (
        df["ID"].astype(str).str.contains(sq, case=False, na=False)
        | df["Title"].str.contains(sq, case=False, na=False)
        | df["AssignedTo"].str.contains(sq, case=False, na=False)
        | df["Scenario"].str.contains(sq, case=False, na=False)
        | df["Team"].str.contains(sq, case=False, na=False)
    )
    df = df[mask]

# View mode: Live = open only; Date Range = all tickets in range
if dr is not None:
    # Date Range mode – show all tickets (open + completed) in the range
    if hasattr(dr, '__len__') and len(dr) == 2:
        d_start, d_end = dr[0], dr[1]
    else:
        d_start = d_end = dr if not hasattr(dr, '__getitem__') else dr[0]
    df = df[(df["CreatedDay"] >= d_start) & (df["CreatedDay"] <= d_end)]
else:
    # Live View – only show currently open tickets
    df = df[df["IsOpen"]]

# ---------------------------------------------------------------------------
# KPI Summary
# ---------------------------------------------------------------------------
total = len(df)
open_count = int(df["IsOpen"].sum())
completed_count = total - open_count
on_track = int(df["SLA_Status"].str.contains("On Track").sum())
at_risk = int(df["SLA_Status"].str.contains("At Risk").sum())
breached = int(df["SLA_Status"].str.contains("Breached|Completed Late").sum())
waiting_info = int(df["SLA_Status"].str.contains("Paused").sum())
sla_met = int(((~df["IsOpen"]) & (df["Elapsed_BDays"] <= df["SLA_Days"])).sum())
sla_pct = (sla_met / completed_count * 100) if completed_count > 0 else 100.0

k1, k2, k3, k4, k5, k6, k7 = st.columns(7)


def _tile_table(filtered_df):
    """Render a small ticket table inside a dialog."""
    if filtered_df.empty:
        st.caption("No tickets.")
        return
    tbl = filtered_df[["ID", "SubType", "Team", "AssignedTo", "State", "CreatedDay", "SLA_Status"]].copy()
    tbl = tbl.rename(columns={
        "SubType": "Scenario", "AssignedTo": "Assignee",
        "CreatedDay": "Submitted", "SLA_Status": "Status",
    })
    st.dataframe(tbl, use_container_width=True, hide_index=True,
                 height=min(len(tbl) * 38 + 50, 400),
                 column_config={"ID": st.column_config.NumberColumn("ID", format="%d")})


@st.dialog("Total Tickets", width="large")
def show_total():
    _tile_table(df)

@st.dialog("Open Tickets", width="large")
def show_open():
    _tile_table(df[df["IsOpen"]])

@st.dialog("Completed Tickets", width="large")
def show_completed():
    _tile_table(df[~df["IsOpen"]])

@st.dialog("On Track", width="large")
def show_on_track():
    _tile_table(df[df["SLA_Status"].str.contains("On Track")])

@st.dialog("At Risk", width="large")
def show_at_risk():
    _tile_table(df[df["SLA_Status"].str.contains("At Risk")])

@st.dialog("Breached", width="large")
def show_breached():
    _tile_table(df[df["SLA_Status"].str.contains("Breached|Completed Late")])

@st.dialog("Waiting for Info", width="large")
def show_waiting():
    _tile_table(df[df["SLA_Status"].str.contains("Paused")])


with k1:
    st.markdown(f'<div class="kpi-card kpi-blue"><span class="kpi-icon">📊</span><h2>{total}</h2><p>Total Tickets</p></div>', unsafe_allow_html=True)
    if st.button("View", key="btn_total", use_container_width=True):
        show_total()
with k2:
    st.markdown(f'<div class="kpi-card kpi-orange"><span class="kpi-icon">📂</span><h2>{open_count}</h2><p>Open</p></div>', unsafe_allow_html=True)
    if st.button("View", key="btn_open", use_container_width=True):
        show_open()
with k3:
    st.markdown(f'<div class="kpi-card kpi-green"><span class="kpi-icon">✅</span><h2>{completed_count}</h2><p>Completed</p></div>', unsafe_allow_html=True)
    if st.button("View", key="btn_completed", use_container_width=True):
        show_completed()
with k4:
    st.markdown(f'<div class="kpi-card kpi-green"><span class="kpi-icon">🟢</span><h2>{on_track}</h2><p>On Track</p></div>', unsafe_allow_html=True)
    if st.button("View", key="btn_ontrack", use_container_width=True):
        show_on_track()
with k5:
    cls = "kpi-orange" if at_risk > 0 else "kpi-green"
    st.markdown(f'<div class="kpi-card {cls}"><span class="kpi-icon">🟡</span><h2>{at_risk}</h2><p>At Risk</p></div>', unsafe_allow_html=True)
    if st.button("View", key="btn_atrisk", use_container_width=True):
        show_at_risk()
with k6:
    cls = "kpi-red" if breached > 0 else "kpi-green"
    st.markdown(f'<div class="kpi-card {cls}"><span class="kpi-icon">🔴</span><h2>{breached}</h2><p>Breached</p></div>', unsafe_allow_html=True)
    if st.button("View", key="btn_breached", use_container_width=True):
        show_breached()
with k7:
    st.markdown(f'<div class="kpi-card kpi-blue"><span class="kpi-icon">⏸️</span><h2>{waiting_info}</h2><p>Waiting for Info</p></div>', unsafe_allow_html=True)
    if st.button("View", key="btn_waiting", use_container_width=True):
        show_waiting()

st.markdown(
    f'<div class="compliance-bar-wrap">'
    f'SLA Compliance: &nbsp;<strong>{sla_pct:.0f}%</strong>&nbsp; of completed tickets met SLA'
    f'<div class="compliance-bar-bg">'
    f'<div class="compliance-bar-fill" style="width:{min(sla_pct, 100):.0f}%"></div>'
    f'</div>'
    f'<span style="color:var(--text-muted);font-size:0.72rem;">'
    f'Live · auto-refreshes every {AUTO_REFRESH_SECONDS // 60} min</span>'
    f'</div>',
    unsafe_allow_html=True,
)
st.markdown("---")

# ---------------------------------------------------------------------------
# Scenario / Team Sections
# ---------------------------------------------------------------------------
# SMB tickets -> single "SMB" section
# Acquisition & Growth tickets -> single "Acquisition & Growth" section
# Others grouped by Scenario
def assign_section(row):
    if row["Team"] == "SMB":
        return "Small-to-Medium Business (SMB)"
    if row["Team"] == "Acquisition & Growth":
        return "Acquisition & Growth"
    if row["Team"] == "Windows Store":
        return "Windows Store"
    return row["Scenario"]

df["DisplaySection"] = df.apply(assign_section, axis=1)

sections_in_data = df["DisplaySection"].unique()

# Sections that should always appear, even with no current tickets
ALWAYS_SHOW_SECTIONS = [
    "Customer Grouping / Hierarchy Management",
    "Personnel Changes",
    "Acquisition & Growth",
    "Small-to-Medium Business (SMB)",
    "Book Assignment Update",
    "Owned-By / Agency / Service Location Override",
    "Windows Store",
    "Quota Moves",
]

# Combine: fixed order + any extra sections from data appended at the end
all_sections = list(ALWAYS_SHOW_SECTIONS)
for s in sections_in_data:
    if s not in all_sections:
        all_sections.append(s)

# Use the fixed order (no dynamic sorting)
section_order = all_sections

SMB_SLA_INFO = {
    "default": 5,
    "description": "5 working days (all SMB scenarios)",
    "info": (
        "**Bad Agency Setup**\n"
        "- Agency / XID info, assignment info, all client info\n\n"
        "**Missing Contacts**\n"
        "- Client info (MAN / Adv Name)\n"
        "- CM team to reach out to Sales leads\n\n"
        "**Unengaged / Inactive Clients**\n"
        "- Client info (MAN / Adv Name)\n"
        "- Number of outreaches\n\n"
        "**Book / Other Requests**\n"
        "- Standard information per scenario"
    ),
}

WS_SLA_INFO = {
    "default": 3,
    "description": "3 working days (all Windows Store scenarios)",
    "info": (
        "All Windows Store change requests have a 3 working day SLA.\n\n"
        "**Weekly Win Processing**\n"
        "- Valid Win submissions\n\n"
        "**Other Requests**\n"
        "- Standard information per scenario"
    ),
}

AG_SLA_INFO = {
    "default": 5,
    "description": "Win Override & Weekly Wins: by next Monday | Growth MPM: 2 working days",
    "info": (
        "**Weekly Win Processing**\n"
        "- Submit by Friday EOD → processed Monday → reflects Wednesday EOD\n\n"
        "**Acquisition Win Override**\n"
        "- Advertiser details of invalid win\n"
        "- MSX Opportunity ID\n"
        "- Evidence & reasoning for override\n"
        "- Must be submitted by Friday EOD\n"
        "- Processed by next Monday\n\n"
        "**Pre/Post Growth MPM Assignment**\n"
        "- Advertiser information\n"
        "- Pre or Post Qualified Win\n"
        "- Growth team assignment\n"
        "- Effective date (month)\n"
        "- 2 working days\n\n"
        "**Other A&G Requests**\n"
        "- Standard information per scenario"
    ),
}

# ---------------------------------------------------------------------------
# Helpers for section rendering
# ---------------------------------------------------------------------------
def _time_pressure(row):
    r = row["Remaining_BDays"]
    status = row["SLA_Status"]
    if "Paused" in status:
        return "Paused"
    if "Completed" in status:
        return "Done" if "Late" not in status else "Late"
    if r < 0:
        return f"{abs(int(r))}d overdue"
    if r == 0:
        return "Due today"
    if r <= 1:
        return "Due tomorrow"
    return f"{int(r)}d buffer"


def _sla_progress(row):
    if row["SLA_Days"] == 0:
        return 100
    return min(int((row["Elapsed_BDays"] / row["SLA_Days"]) * 100), 100)


def _render_section(section, df_filtered):
    sdf = df_filtered[df_filtered["DisplaySection"] == section].copy()
    open_in = int(sdf["IsOpen"].sum()) if not sdf.empty else 0
    total_in = len(sdf)

    if section == "Small-to-Medium Business (SMB)":
        sla_info = SMB_SLA_INFO
    elif section == "Acquisition & Growth":
        sla_info = AG_SLA_INFO
    elif section == "Windows Store":
        sla_info = WS_SLA_INFO
    else:
        sla_info = SCENARIO_SLA.get(section, DEFAULT_SLA)
    sla_desc = sla_info.get("description", "N/A")

    if sdf.empty:
        status_emoji = "🟢"
    else:
        has_breached = sdf["SLA_Status"].str.contains("Breached|Completed Late").any()
        has_at_risk = sdf["SLA_Status"].str.contains("At Risk").any()
        status_emoji = "🔴" if has_breached else ("🟡" if has_at_risk else "🟢")

    st.markdown(
        f'<div class="scenario-header">'
        f'<h3>{status_emoji} {section}</h3>'
        f'<p>SLA: {sla_desc}  •  {open_in} open / {total_in} total</p>'
        f'</div>',
        unsafe_allow_html=True,
    )

    with st.expander("📌 Information Required & SLA Details"):
        st.markdown(sla_info.get("info", "N/A"))

    if sdf.empty:
        st.caption("No current tickets in this section.")
        return

    display = sdf[[
        "ID", "SubType", "Team", "AssignedTo", "State", "RequesterName",
        "CreatedDay", "SLA_Display", "SLA_Days", "Elapsed_BDays", "Remaining_BDays", "SLA_Status",
    ]].copy()

    display["ID"] = display["ID"].apply(
        lambda x: f"https://{ADO_ORG}.visualstudio.com/{ADO_PROJECT}/_workitems/edit/{x}"
    )
    display["Time Left"] = display.apply(_time_pressure, axis=1)
    display["SLA Progress"] = display.apply(_sla_progress, axis=1)

    display = display.rename(columns={
        "SubType": "Change Scenario",
        "AssignedTo": "Assignee",
        "CreatedDay": "Submitted",
        "RequesterName": "Requester",
        "SLA_Display": "SLA Target",
        "SLA_Status": "Status",
    })
    display = display.drop(columns=["Elapsed_BDays", "Remaining_BDays", "SLA_Days"])

    _status_order = {
        "Breached": 0, "At Risk": 1, "On Track": 2,
        "Paused": 2.5, "Completed Late": 3, "Completed": 4,
    }
    display["_sort"] = display["Status"].apply(
        lambda s: next((v for k, v in _status_order.items() if k in s), 5)
    )
    display = display.sort_values(["_sort", "Submitted"], ascending=[True, False]).drop(columns="_sort")

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        height=min(len(display) * 38 + 50, 380),
        column_config={
            "ID": st.column_config.LinkColumn("ID", display_text=r"(\d+)$", help="Click to open in ADO"),
            "SLA Progress": st.column_config.ProgressColumn("SLA %", format="%d%%", min_value=0, max_value=100),
        },
    )


# ---------------------------------------------------------------------------
# Render scenario sections in a 2-column grid
# ---------------------------------------------------------------------------
if not section_order:
    st.info("No tickets match the current filters.")
else:
    for i in range(0, len(section_order), 2):
        pair = section_order[i : i + 2]
        if len(pair) == 1:
            _render_section(pair[0], df)
        else:
            col_a, col_b = st.columns(2)
            with col_a:
                _render_section(pair[0], df)
            with col_b:
                _render_section(pair[1], df)
        st.markdown("")

# ---------------------------------------------------------------------------
# Excel Export
# ---------------------------------------------------------------------------
st.markdown("---")
if not df.empty:
    export_df = df[[
        "ID", "SubType", "Team", "AssignedTo", "State", "RequesterName",
        "CreatedDay", "SLA_Display", "SLA_Days", "Elapsed_BDays",
        "Remaining_BDays", "SLA_Status",
    ]].copy()
    export_df = export_df.rename(columns={
        "SubType": "Scenario", "AssignedTo": "Assignee",
        "CreatedDay": "Submitted", "RequesterName": "Requester",
        "SLA_Display": "SLA target", "SLA_Days": "SLA days",
        "Elapsed_BDays": "Elapsed (bdays)", "Remaining_BDays": "Remaining (bdays)",
        "SLA_Status": "Status",
    })
    export_df = export_df.sort_values("Submitted", ascending=False)

    summary_text = (
        f"Change Management SLA Snapshot \u2014 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        f"Total: {total} | Open: {open_count} | Completed: {completed_count}\n"
        f"At risk: {at_risk} | Breached: {breached} | SLA compliance: {sla_pct:.0f}%\n"
    )

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        pd.DataFrame({"SLA Snapshot": [summary_text]}).to_excel(writer, sheet_name="Summary", index=False)
        export_df.to_excel(writer, sheet_name="Tickets", index=False)
        worksheet = writer.sheets["Tickets"]
        for i, col in enumerate(export_df.columns):
            max_len = max(export_df[col].astype(str).str.len().max(), len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 30))

    st.download_button(
        label="Export SLA snapshot (Excel)",
        data=buffer.getvalue(),
        file_name=f"SLA_Snapshot_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ---------------------------------------------------------------------------
# SLA Notifications — post comment on at-risk tickets (once per ticket)
# ---------------------------------------------------------------------------
check_and_notify_at_risk(df)

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
st.markdown(f"""
<script>
    setTimeout(function() {{ window.location.reload(); }}, {AUTO_REFRESH_SECONDS * 1000});
</script>
""", unsafe_allow_html=True)
