"""Shared helpers for 2fresh TikTok analytics.
Zero-cost design (runs on Apify $5/mo free credit):
- Daily list (15 videos per account) keeps URL Ledger fresh
- Daily URL-fetch pulls stats for videos that turned exactly 7 days old
- Every video is captured at the same age = fair comparison
- gspread for Sheets (free)
- CallMeBot GET for WhatsApp (free)
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone
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
LEDGER_HEADER = [
    "Post date", "Account", "URL", "First seen",
]
URL_LEDGER_TAB = "URL Ledger"
ALL_TABS = [
    "2F-TalkingHead", "2F-ComReply",
    "R2F-TalkingHead", "R2F-ComReply",
    "Snapshot", URL_LEDGER_TAB,
]

# Ledger pruning: forget entries older than this (days)
LEDGER_RETENTION_DAYS = 30


# ---------- Apify: list latest videos for a profile ----------
def apify_list_profile(username: str, count: int = 15) -> list:
    """Fetch latest `count` videos from a TikTok profile via Apify.
    Returns list of video dicts (actor-native shape). Empty on failure.
    """
    token = os.environ["APIFY_TOKEN"]
    actor = "clockworks~tiktok-scraper"
    endpoint = (
        f"https://api.apify.com/v2/acts/{actor}"
        f"/run-sync-get-dataset-items?token={token}&maxItems={count}"
    )
    payload = {
        "profiles": [username.lstrip("@")],
        "resultsPerPage": count,
        "maxItems": count,
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
        "shouldDownloadSubtitles": False,
        "shouldDownloadAvatars": False,
        "shouldDownloadSlideshowImages": False,
    }
    print(f"[apify.list] {username} requesting {count} items")
    try:
        r = requests.post(endpoint, json=payload, timeout=600)
    except requests.Timeout:
        print(f"[apify.list] {username} timeout")
        return []
    except Exception as e:
        print(f"[apify.list] {username} error: {e}")
        return []
    if r.status_code == 402:
        print(f"[apify.list] {username} 402 Payment Required — credit exhausted")
        return []
    if r.status_code == 401:
        print(f"[apify.list] {username} 401 Unauthorized — check APIFY_TOKEN")
        return []
    # Apify sync API returns 200 or 201 on success
    if not (200 <= r.status_code < 300):
        print(f"[apify.list] {username} HTTP {r.status_code}: {r.text[:200]}")
        return []
    try:
        items = r.json()
    except Exception as e:
        print(f"[apify.list] {username} bad JSON: {e}")
        return []
    if not isinstance(items, list):
        print(f"[apify.list] {username} unexpected shape: {str(items)[:200]}")
        return []
    print(f"[apify.list] {username} got {len(items)} items")
    return items


# ---------- Apify: fetch stats for specific video URLs ----------
def apify_fetch_videos(urls: list) -> list:
    """Fetch fresh stats for a list of TikTok video URLs. Batches in one Apify run.
    Returns list of video dicts. Empty on failure.
    """
    if not urls:
        return []
    token = os.environ["APIFY_TOKEN"]
    actor = "clockworks~tiktok-scraper"
    endpoint = (
        f"https://api.apify.com/v2/acts/{actor}"
        f"/run-sync-get-dataset-items?token={token}"
    )
    payload = {
        "postURLs": urls,
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
        "shouldDownloadSubtitles": False,
        "shouldDownloadAvatars": False,
        "shouldDownloadSlideshowImages": False,
    }
    print(f"[apify.fetch] requesting {len(urls)} URL(s)")
    try:
        r = requests.post(endpoint, json=payload, timeout=600)
    except requests.Timeout:
        print(f"[apify.fetch] timeout")
        return []
    except Exception as e:
        print(f"[apify.fetch] error: {e}")
        return []
    if r.status_code == 402:
        print("[apify.fetch] 402 Payment Required — credit exhausted")
        return []
    if r.status_code == 401:
        print("[apify.fetch] 401 Unauthorized — check APIFY_TOKEN")
        return []
    # Apify sync API returns 200 or 201 on success
    if not (200 <= r.status_code < 300):
        print(f"[apify.fetch] HTTP {r.status_code}: {r.text[:200]}")
        return []
    try:
        items = r.json()
    except Exception as e:
        print(f"[apify.fetch] bad JSON: {e}")
        return []
    if not isinstance(items, list):
        print(f"[apify.fetch] unexpected shape: {str(items)[:200]}")
        return []
    print(f"[apify.fetch] got {len(items)} items")
    return items


# ---------- Classify (deep scan) ----------
def classify(description: str, item: dict = None) -> str:
    """Return 'comreply' if the video is a comment reply, else 'scripted'.
    Checks description first (cheap), then deep-scans all fields of the item
    (handles Apify variations where the 'Replying to @...' prefix lives in a
    different field).
    """
    if description:
        d = description.lower()
        if "replying to @" in d[:200] or "replying to " in d[:200]:
            return "comreply"
    if item:
        try:
            blob = json.dumps(item, default=str).lower()
            if "replying to @" in blob or "replying to " in blob:
                for k, v in item.items():
                    try:
                        if "replying to" in json.dumps(v, default=str).lower():
                            print(f"[classify] comreply matched in field: {k}")
                            break
                    except Exception:
                        pass
                return "comreply"
        except Exception as e:
            print(f"[classify] deep-scan error: {e}")
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
    """Return the post datetime (Melbourne-local) or None if missing."""
    iso = video.get("createTimeISO")
    if iso:
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
            return dt.astimezone(MELBOURNE)
        except Exception:
            pass
    ts = video.get("createTime")
    if ts:
        try:
            dt = datetime.fromtimestamp(int(ts), tz=ZoneInfo("UTC"))
            return dt.astimezone(MELBOURNE)
        except Exception:
            pass
    return None


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
            print(f"[sheet] created missing tab '{tab}'")
    for tab in ["2F-TalkingHead", "2F-ComReply", "R2F-TalkingHead", "R2F-ComReply"]:
        ws = sheet.worksheet(tab)
        if ws.row_values(1) != VIDEO_HEADER:
            ws.update([VIDEO_HEADER], "A1")
    snap = sheet.worksheet("Snapshot")
    if snap.row_values(1) != SNAPSHOT_HEADER:
        snap.update([SNAPSHOT_HEADER], "A1")
    ledger = sheet.worksheet(URL_LEDGER_TAB)
    if ledger.row_values(1) != LEDGER_HEADER:
        ledger.update([LEDGER_HEADER], "A1")


def append_row(sheet, tab_name: str, row: list):
    sheet.worksheet(tab_name).append_row(row, value_input_option="USER_ENTERED")


def already_ran_today(sheet, today_iso: str) -> bool:
    """Return True only if EVERY account has a Snapshot row for today_iso.
    Partial-run safe: if the previous cron crashed after writing 2F but before
    R2F, this returns False so the next cron retries R2F.
    """
    expected = {cfg["label"] for cfg in ACCOUNTS.values()}
    return snapshot_labels_for_date(sheet, today_iso) >= expected


def snapshot_labels_for_date(sheet, date_iso: str) -> set:
    """Return set of account labels that have a Snapshot row for date_iso."""
    try:
        rows = sheet.worksheet("Snapshot").get_all_values()
    except Exception:
        return set()
    out = set()
    for r in rows[1:]:
        if len(r) >= 2 and r[0] == date_iso:
            out.add(r[1])
    return out


def urls_written_for_date(sheet, tab: str, date_iso: str) -> set:
    """Return set of URLs already written to `tab` on analysis_date == date_iso.
    Used by phase 2 to avoid duplicate rows after a partial-run retry.
    """
    try:
        rows = sheet.worksheet(tab).get_all_values()
    except Exception:
        return set()
    out = set()
    for r in rows[1:]:
        if len(r) > 5 and r[0] == date_iso and r[5]:
            out.add(r[5])
    return out


# ---------- URL Ledger helpers ----------
def read_ledger(sheet) -> list:
    """Return all ledger rows as list of dicts: {post_date, account, url, first_seen}.
    Skips the header row. Ignores malformed rows silently.
    """
    try:
        ws = sheet.worksheet(URL_LEDGER_TAB)
    except Exception:
        return []
    rows = ws.get_all_values()
    out = []
    for r in rows[1:]:
        if len(r) < 3:
            continue
        post_date, account, url = r[0], r[1], r[2]
        first_seen = r[3] if len(r) > 3 else ""
        if not url:
            continue
        out.append({
            "post_date": post_date,
            "account": account,
            "url": url,
            "first_seen": first_seen,
        })
    return out


def upsert_ledger(sheet, new_rows: list) -> int:
    """Append new ledger rows for URLs not already present.
    new_rows = list of [post_date, account, url, first_seen].
    Returns count of rows actually appended.
    """
    if not new_rows:
        return 0
    ws = sheet.worksheet(URL_LEDGER_TAB)
    existing_urls = set()
    existing = ws.get_all_values()
    for r in existing[1:]:
        if len(r) >= 3 and r[2]:
            existing_urls.add(r[2])
    to_add = [row for row in new_rows if row[2] not in existing_urls]
    if not to_add:
        return 0
    ws.append_rows(to_add, value_input_option="USER_ENTERED")
    return len(to_add)


def prune_ledger(sheet, today_mel_date) -> int:
    """Remove ledger rows whose post_date is older than LEDGER_RETENTION_DAYS.
    Returns count of rows removed. Rewrites the whole tab in one call.
    """
    ws = sheet.worksheet(URL_LEDGER_TAB)
    rows = ws.get_all_values()
    if len(rows) <= 1:
        return 0
    cutoff = today_mel_date - timedelta(days=LEDGER_RETENTION_DAYS)
    kept = [rows[0]]
    removed = 0
    for r in rows[1:]:
        if len(r) < 3:
            continue
        try:
            pd = datetime.fromisoformat(r[0]).date()
        except Exception:
            kept.append(r)
            continue
        if pd >= cutoff:
            kept.append(r)
        else:
            removed += 1
    if removed > 0:
        ws.clear()
        ws.update(kept, "A1", value_input_option="USER_ENTERED")
    return removed


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
        try:
            r = requests.get(
                url,
                params={"phone": phone, "text": chunk, "apikey": apikey},
                timeout=30,
            )
            if r.status_code >= 400:
                print(f"[whatsapp] ERROR {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"[whatsapp] exception: {e}")
        time.sleep(3)  # CallMeBot rate limit
