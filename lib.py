"""Shared helpers for 2fresh TikTok analytics.
Zero-cost design:
- Apify clockworks~tiktok-scraper (covered by $5/mo free credit)
- Description-based classifier
- gspread for Sheets (free)
- CallMeBot GET for WhatsApp (free)
"""
import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import gspread
import requests
from google.oauth2.service_account import Credentials

MELBOURNE = ZoneInfo("Australia/Melbourne")

ACCOUNTS = {
    "2fresh_._": {
        "scripted_tab": "2F-TalkingHead",
        "comreply_tab": "2F-ComReply",
        "label": "2F",
    },
    "ryan2fresh_": {
        "scripted_tab": "R2F-TalkingHead",
        "comreply_tab": "R2F-ComReply",
        "label": "R2F",
    },
}

VIDEO_HEADER = [
    "Analysis date", "Post date", "Account", "Type", "Description",
    "URL", "Views", "Likes", "Comments", "Shares", "Saves",
    "Like %", "Comment %", "Share %", "Save %", "Engagement %",
    "Followers at scrape", "Retention % (manual)",
]
SNAPSHOT_HEADER = [
    "Date", "Account", "Followers", "Following", "Total likes", "Video count",
]
ALL_TABS = [
    "2F-TalkingHead", "2F-ComReply",
    "R2F-TalkingHead", "R2F-ComReply",
    "Snapshot",
]


# ---------- Apify scraper ----------
def scrape_profile(username: str, results: int = 75) -> list:
    """Fetch ~`results` most recent videos for a TikTok username via Apify
    clockworks~tiktok-scraper. Returns list in the internal shape daily.py
    expects (field names match the actor's native output).
    """
    token = os.environ["APIFY_TOKEN"]
    actor = "clockworks~tiktok-scraper"
    url = (
        f"https://api.apify.com/v2/acts/{actor}"
        f"/run-sync-get-dataset-items?token={token}"
    )
    payload = {
        "profiles": [username.lstrip("@")],
        "resultsPerPage": results,
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
        "shouldDownloadSubtitles": False,
        "shouldDownloadAvatars": False,
        "shouldDownloadSlideshowImages": False,
    }
    print(f"[scrape] {username} requesting {results} items from Apify")
    try:
        r = requests.post(url, json=payload, timeout=600)
    except requests.Timeout:
        print(f"[scrape] {username} Apify timeout after 10 min")
        return []
    if r.status_code == 402:
        print(f"[scrape] {username} 402 Payment Required — Apify credit exhausted")
        return []
    if r.status_code == 401:
        print(f"[scrape] {username} 401 Unauthorized — check APIFY_TOKEN secret")
        return []
    r.raise_for_status()
    items = r.json()
    if not isinstance(items, list):
        print(f"[scrape] {username} unexpected response shape: {str(items)[:200]}")
        return []
    print(f"[scrape] {username} received {len(items)} items")
    return items


# ---------- Classify ----------
def classify(description: str, item: dict = None) -> str:
    if not description:
        return "scripted"
    d = description.lower()
    if "replying to @" in d[:120] or "replying to " in d[:120]:
        return "comreply"
    return "scripted"


# ---------- Metrics ----------
def compute_ratios(video: dict) -> dict:
    views = video.get("playCount") or 0
    likes = video.get("diggCount") or 0
    comments = video.get("commentCount") or 0
    shares = video.get("shareCount") or 0
    saves = video.get("collectCount") or 0
    denom = max(views, 1)
    return {
        "views": views,
        "likes": likes,
        "comments": comments,
        "shares": shares,
        "saves": saves,
        "like_rate": round(likes / denom * 100, 3),
        "comment_rate": round(comments / denom * 100, 3),
        "share_rate": round(shares / denom * 100, 3),
        "save_rate": round(saves / denom * 100, 3),
        "engagement_rate": round(
            (likes + comments + shares + saves) / denom * 100, 3
        ),
    }


def parse_post_date(video: dict):
    iso = video.get("createTimeISO")
    if not iso:
        ts = video.get("createTime")
        if not ts:
            return None
        dt = datetime.fromtimestamp(int(ts), tz=ZoneInfo("UTC"))
    else:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return dt.astimezone(MELBOURNE)


# ---------- Sheets ----------
def get_sheet():
    sa_json = json.loads(os.environ["GOOGLE_SA_JSON"])
    creds = Credentials.from_service_account_info(
        sa_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(os.environ["SHEET_ID"])


def ensure_tabs_and_headers(sheet):
    existing = {ws.title for ws in sheet.worksheets()}
    for tab in ALL_TABS:
        if tab not in existing:
            sheet.add_worksheet(title=tab, rows=1000, cols=20)
    for tab in ["2F-TalkingHead", "2F-ComReply", "R2F-TalkingHead", "R2F-ComReply"]:
        ws = sheet.worksheet(tab)
        if ws.row_values(1) != VIDEO_HEADER:
            ws.update([VIDEO_HEADER], "A1")
    snap = sheet.worksheet("Snapshot")
    if snap.row_values(1) != SNAPSHOT_HEADER:
        snap.update([SNAPSHOT_HEADER], "A1")


def append_row(sheet, tab_name: str, row: list):
    sheet.worksheet(tab_name).append_row(row, value_input_option="USER_ENTERED")


def already_ran_today(sheet, today_iso: str) -> bool:
    try:
        dates = sheet.worksheet("Snapshot").col_values(1)
    except Exception:
        return False
    return today_iso in dates[1:]


# ---------- WhatsApp ----------
def send_whatsapp(text: str) -> None:
    phone = os.environ["CALLMEBOT_PHONE"]
    apikey = os.environ["CALLMEBOT_APIKEY"]
    url = "https://api.callmebot.com/whatsapp.php"
    chunks, buf = [], ""
    for line in text.split("\n"):
        if len(buf) + len(line) + 1 > 1200:
            chunks.append(buf)
            buf = line
        else:
            buf = f"{buf}\n{line}" if buf else line
    if buf:
        chunks.append(buf)
    for chunk in chunks:
        r = requests.get(
            url,
            params={"phone": phone, "text": chunk, "apikey": apikey},
            timeout=30,
        )
        if r.status_code >= 400:
            print(f"[whatsapp] ERROR {r.status_code}: {r.text[:200]}")
