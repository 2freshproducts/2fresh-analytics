"""Shared helpers for 2fresh TikTok analytics.

Zero-cost design:
- tikwm.com free public API (no signup, no key)
- Description-based classifier (no Notion / no ML)
- gspread for Sheets (free)
- CallMeBot GET for WhatsApp (free)
"""

import json
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import gspread
import requests
from google.oauth2.service_account import Credentials

MELBOURNE = ZoneInfo("Australia/Melbourne")

ACCOUNTS = {
    # username_on_tiktok : { sheet tab names, label used in messages }
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


# ---------- tikwm scraper (free, no API key) ----------

def scrape_profile(username: str, results: int = 100) -> list:
    """
    Scrape a TikTok profile using tikwm.com's free public API.
    Returns a list of video dicts normalized to match the Apify shape.
    """
    uname = username.lstrip("@")
    base = "https://tikwm.com/api/user/posts"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://tikwm.com/",
    }

    videos = []
    cursor = 0
    page_size = 35  # tikwm caps at ~35 per page
    author_meta = {}

    while len(videos) < results:
        params = {
            "unique_id": uname,
            "count": page_size,
            "cursor": cursor,
        }
        for attempt in range(4):
            r = requests.get(base, params=params, headers=headers, timeout=60)
            if r.status_code == 200:
                break
            time.sleep(2 ** attempt)
        r.raise_for_status()
        data = r.json()

        if data.get("code") != 0:
            raise RuntimeError(f"tikwm error for {uname}: {data.get('msg')}")

        payload = data.get("data") or {}
        items = payload.get("videos") or []
        if not items:
            break

        # Capture author-level stats from first page
        if not author_meta and items:
            author = items[0].get("author") or {}
            stats = payload.get("stats") or {}
            author_meta = {
                "fans": stats.get("followerCount") or author.get("follower_count", 0),
                "following": stats.get("followingCount") or author.get("following_count", 0),
                "heart": stats.get("heartCount") or author.get("heart_count", 0),
                "video": stats.get("videoCount") or author.get("video_count", 0),
            }

        for v in items:
            video_id = v.get("video_id") or v.get("aweme_id") or ""
            create_ts = v.get("create_time") or 0
            create_iso = (
                datetime.fromtimestamp(create_ts, tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
                if create_ts
                else None
            )
            videos.append({
                "text": v.get("title", "") or "",
                "webVideoUrl": f"https://www.tiktok.com/@{uname}/video/{video_id}",
                "playCount": v.get("play_count", 0),
                "diggCount": v.get("digg_count", 0),
                "commentCount": v.get("comment_count", 0),
                "shareCount": v.get("share_count", 0),
                "collectCount": v.get("collect_count", 0),
                "createTime": create_ts,
                "createTimeISO": create_iso,
                "authorMeta": author_meta,
            })
            if len(videos) >= results:
                break

        if not payload.get("hasMore"):
            break
        cursor = payload.get("cursor", cursor + page_size)
        time.sleep(1.2)  # respect rate limit

    return videos


# ---------- Classify ----------

def classify(description: str, item: dict = None) -> str:
    """'comreply' if TikTok comment-reply video, else 'scripted'."""
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
    """Return Melbourne-local date for the video, or None."""
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
        current = ws.row_values(1)
        if current != VIDEO_HEADER:
            ws.update([VIDEO_HEADER], "A1")
    snap = sheet.worksheet("Snapshot")
    if snap.row_values(1) != SNAPSHOT_HEADER:
        snap.update([SNAPSHOT_HEADER], "A1")


def append_row(sheet, tab_name: str, row: list):
    sheet.worksheet(tab_name).append_row(row, value_input_option="USER_ENTERED")


def already_ran_today(sheet, today_iso: str) -> bool:
    """Check Snapshot tab for an entry with today's Melbourne date."""
    try:
        dates = sheet.worksheet("Snapshot").col_values(1)
    except Exception:
        return False
    return today_iso in dates[1:]  # skip header


# ---------- WhatsApp ----------

def send_whatsapp(text: str) -> None:
    """CallMeBot WhatsApp. Splits long messages to stay under URL limits."""
    phone = os.environ["CALLMEBOT_PHONE"]
    apikey = os.environ["CALLMEBOT_APIKEY"]
    url = "https://api.callmebot.com/whatsapp.php"

    chunks = []
    buf = ""
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
