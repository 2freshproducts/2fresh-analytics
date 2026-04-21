"""Shared helpers for 2fresh TikTok analytics.
Zero-cost design:
- ScrapTik via RapidAPI
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


# ---------- ScrapTik scraper (RapidAPI) ----------
def scrape_profile(username: str, results: int = 150) -> list:
    """Fetch `results` most recent videos for a TikTok username via ScrapTik.
    Uses max_cursor pagination (TikTok-style).
    """
    key = os.environ["RAPIDAPI_KEY"]
    host = "scraptik.p.rapidapi.com"
    headers = {"X-RapidAPI-Key": key, "X-RapidAPI-Host": host}
    uname = username.lstrip("@")

    # Step 1: resolve user ID
    r = requests.get(
        f"https://{host}/get-user",
        params={"username": uname},
        headers=headers,
        timeout=60,
    )
    r.raise_for_status()
    user_data = r.json().get("user", {})
    user_id = user_data.get("uid")
    if not user_id:
        raise ValueError(f"[scrape] could not resolve uid for {uname}")
    print(f"[scrape] {uname} uid={user_id}")

    # Step 2: paginate posts using max_cursor (NOT min_cursor)
    videos = []
    cursor = "0"
    seen_cursors = {cursor}
    page = 0
    while len(videos) < results:
        page += 1
        r = requests.get(
            f"https://{host}/user-posts",
            params={
                "user_id": user_id,
                "count": 35,
                "max_cursor": cursor,
                "region": "AU",
            },
            headers=headers,
            timeout=60,
        )
        r.raise_for_status()
        payload = r.json()
        items = payload.get("aweme_list") or []
        has_more = bool(payload.get("has_more"))
        next_cursor = str(payload.get("max_cursor", ""))
        print(
            f"[scrape] {uname} page {page}: {len(items)} items, "
            f"has_more={has_more}, next_cursor={next_cursor}"
        )

        if not items:
            break

        for v in items:
            author = v.get("author") or {}
            stats = v.get("statistics") or {}
            ts = v.get("create_time") or 0
            share_url = (v.get("share_info") or {}).get("share_url", "")
            aweme_id = v.get("aweme_id", "")
            url = share_url or f"https://www.tiktok.com/@{uname}/video/{aweme_id}"
            videos.append({
                "text": v.get("desc", "") or "",
                "webVideoUrl": url,
                "playCount": stats.get("play_count", 0),
                "diggCount": stats.get("digg_count", 0),
                "commentCount": stats.get("comment_count", 0),
                "shareCount": stats.get("share_count", 0),
                "collectCount": stats.get("collect_count", 0),
                "createTime": ts,
                "createTimeISO": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z") if ts else None,
                "authorMeta": {
                    "fans": author.get("follower_count", 0),
                    "following": author.get("following_count", 0),
                    "heart": author.get("total_favorited", 0),
                    "video": author.get("aweme_count", 0),
                },
            })
            if len(videos) >= results:
                break

        if not has_more:
            break
        # Safety: bail if cursor hasn't advanced (prevents infinite loop)
        if not next_cursor or next_cursor in seen_cursors:
            print(f"[scrape] {uname} cursor stuck at {next_cursor!r}, stopping")
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor
        time.sleep(1)

    print(f"[scrape] {uname} total collected: {len(videos)}")
    return videos


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
