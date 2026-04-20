"""Weekly run: top 5 videos per account over the last 7 days.
Sends WhatsApp summary via CallMeBot. Runs Sunday ~8pm Melbourne.
"""

import sys
import traceback
from datetime import datetime, timedelta

from lib import (
    ACCOUNTS, MELBOURNE,
    scrape_profile, classify, compute_ratios, parse_post_date,
    send_whatsapp,
)


def fmt_int(n: int) -> str:
    return f"{n:,}"


def run():
    today_mel = datetime.now(MELBOURNE).date()
    week_start = today_mel - timedelta(days=7)

    lines = [
        f"*Weekly TikTok Top 5*",
        f"Week: {week_start} to {today_mel}",
    ]

    for username, cfg in ACCOUNTS.items():
        print(f"[weekly] scraping {username}")
        try:
            videos = scrape_profile(username, results=60)
        except Exception as e:
            print(f"[weekly] apify failed for {username}: {e}")
            lines.append(f"\n*{cfg['label']}*: scrape failed ({e})")
            continue

        recent = []
        for v in videos or []:
            post_local = parse_post_date(v)
            if not post_local:
                continue
            if week_start <= post_local.date() <= today_mel:
                m = compute_ratios(v)
                recent.append({
                    "date": post_local.date().isoformat(),
                    "type": classify(v.get("text", "") or ""),
                    "desc": (v.get("text", "") or "")[:70].replace("\n", " "),
                    "url": v.get("webVideoUrl", ""),
                    **m,
                })

        recent.sort(key=lambda x: x["views"], reverse=True)
        top = recent[:5]

        total_views = sum(r["views"] for r in recent)
        lines.append(f"\n*{cfg['label']}* - {len(recent)} posts, {fmt_int(total_views)} total views")

        if not top:
            lines.append("no posts in range")
            continue

        for i, v in enumerate(top, 1):
            tlabel = "SC" if v["type"] == "scripted" else "CR"
            lines.append(
                f"{i}. [{tlabel}] {fmt_int(v['views'])}v | "
                f"{fmt_int(v['likes'])}L | ER {v['engagement_rate']}% | {v['date']}"
            )
            lines.append(f"   {v['desc']}")
            lines.append(f"   {v['url']}")

    msg = "\n".join(lines)
    print(msg)
    send_whatsapp(msg)
    print("[weekly] WhatsApp sent")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"[weekly] FATAL: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
