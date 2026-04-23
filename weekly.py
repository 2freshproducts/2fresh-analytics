"""Weekly WhatsApp summary — Sunday 8pm Melbourne.
Pure reader: reads the per-account video tabs + Snapshot, computes follower
growth and top videos by views over the last 7 days, sends WhatsApp via
CallMeBot. No scraping — costs nothing.
"""
import sys
import traceback
from datetime import datetime, timedelta

from lib import (
    MELBOURNE, ACCOUNTS, VIDEO_TABS,
    get_sheet, send_whatsapp,
)


def _safe_int(s):
    try:
        return int(float(s))
    except Exception:
        return 0


def _safe_float(s):
    try:
        return float(s)
    except Exception:
        return 0.0


def read_recent_rows(sheet, tabs: list, since_iso: str) -> list:
    """Rows where Analysis date >= since_iso across the given tabs.
    Column layout (18 cols): 0=Analysis date, 1=Post date, 2=Account, 3=Type,
    4=Description, 5=URL, 6=Views, 7=Likes, 8=Comments, 9=Shares, 10=Saves,
    11=Like%, 12=Comment%, 13=Share%, 14=Save%, 15=Engagement%,
    16=Followers at scrape, 17=Retention%
    """
    out = []
    for tab in tabs:
        try:
            ws = sheet.worksheet(tab)
            rows = ws.get_all_values()
        except Exception as e:
            print(f"[weekly] failed reading {tab}: {e}")
            continue
        if len(rows) < 2:
            continue
        for r in rows[1:]:
            if len(r) < 16:
                continue
            if r[0] >= since_iso:
                out.append({
                    "analysis_date": r[0],
                    "post_date": r[1],
                    "account": r[2],
                    "type": r[3],
                    "description": r[4],
                    "url": r[5],
                    "views": _safe_int(r[6]),
                    "likes": _safe_int(r[7]),
                    "comments": _safe_int(r[8]),
                    "shares": _safe_int(r[9]),
                    "saves": _safe_int(r[10]),
                    "engagement_rate": _safe_float(r[15]),
                })
    return out


def follower_growth(sheet, since_iso: str) -> dict:
    """Return {label: (start, end, delta)} follower deltas since since_iso."""
    try:
        ws = sheet.worksheet("Snapshot")
        rows = ws.get_all_values()
    except Exception:
        return {}
    start_per = {}
    end_per = {}
    for r in rows[1:]:
        if len(r) < 3:
            continue
        date, label = r[0], r[1]
        followers = _safe_int(r[2])
        if date >= since_iso:
            if label not in start_per or date < start_per[label][0]:
                start_per[label] = (date, followers)
        if label not in end_per or date > end_per[label][0]:
            end_per[label] = (date, followers)
    out = {}
    for label in {cfg["label"] for cfg in ACCOUNTS.values()}:
        s = start_per.get(label, (None, None))[1]
        e = end_per.get(label, (None, None))[1]
        if s is None or e is None:
            continue
        out[label] = (s, e, e - s)
    return out


def format_summary(video_rows, growth) -> str:
    lines = []
    lines.append("🎯 2Fresh Weekly TikTok Summary")
    lines.append("")

    if growth:
        lines.append("📈 Follower growth (7d):")
        for label, (s, e, d) in sorted(growth.items()):
            sign = "+" if d >= 0 else ""
            lines.append(f"  {label}: {s:,} → {e:,} ({sign}{d:,})")
        lines.append("")

    # Top 6 videos overall (across both accounts) by views
    lines.append("🎬 Top 6 videos by views:")
    if not video_rows:
        lines.append("  (no videos tracked this week)")
    else:
        top = sorted(video_rows, key=lambda x: x["views"], reverse=True)[:6]
        for i, r in enumerate(top, 1):
            desc = (r["description"] or "").replace("\n", " ")[:60]
            lines.append(
                f"  {i}. {r['account']} · {r['views']:,} views · "
                f"{r['engagement_rate']}% eng"
            )
            lines.append(f"     {desc}")
    lines.append("")

    # Per-account stats
    by_account = {}
    for r in video_rows:
        a = r["account"]
        by_account.setdefault(a, []).append(r)
    if by_account:
        lines.append("📊 Per account (7d):")
        for label in sorted(by_account.keys()):
            rows = by_account[label]
            count = len(rows)
            total_views = sum(r["views"] for r in rows)
            avg_eng = (sum(r["engagement_rate"] for r in rows) / count) if count else 0
            lines.append(
                f"  {label}: {count} videos · {total_views:,} views · "
                f"{avg_eng:.2f}% avg eng"
            )

    return "\n".join(lines)


def run():
    today_mel = datetime.now(MELBOURNE).date()
    since = (today_mel - timedelta(days=7)).isoformat()
    print(f"[weekly] reading rows since {since}")

    sheet = get_sheet()
    videos = read_recent_rows(sheet, VIDEO_TABS, since)
    growth = follower_growth(sheet, since)

    print(f"[weekly] video rows: {len(videos)}")
    print(f"[weekly] growth labels: {list(growth.keys())}")

    msg = format_summary(videos, growth)
    print("[weekly] message:\n" + msg)

    send_whatsapp(msg)
    print("[weekly] done.")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"[weekly] FATAL: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
