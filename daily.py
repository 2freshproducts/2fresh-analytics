"""Daily run: analyse the video posted exactly 7 days ago on each account.

Runs ~6:45am Melbourne (two UTC crons for DST coverage; second run is
no-op'd via Snapshot-tab dedupe).

Writes to 4 tabs: 2F-TalkingHead, 2F-ComReply, R2F-TalkingHead, R2F-ComReply.
Also appends daily follower snapshot to 'Snapshot' tab.
Silent (no WhatsApp). Weekly script handles notifications.
"""

import sys
import traceback
from datetime import datetime, timedelta

from lib import (
    ACCOUNTS, MELBOURNE,
    scrape_profile, classify, compute_ratios, parse_post_date,
    get_sheet, ensure_tabs_and_headers, append_row, already_ran_today,
)


def run():
    today_mel = datetime.now(MELBOURNE).date()
    target_date = today_mel - timedelta(days=7)
    today_iso = today_mel.isoformat()
    print(f"[daily] today_mel={today_iso} target_post_date={target_date}")

    sheet = get_sheet()
    ensure_tabs_and_headers(sheet)

    # DST-safe dedupe: if we already ran today, skip.
    if already_ran_today(sheet, today_iso):
        print("[daily] snapshot already exists for today, skipping")
        return

    total_written = 0

    for username, cfg in ACCOUNTS.items():
        print(f"[daily] scraping {username}")
        try:
            videos = scrape_profile(username, results=100)
        except Exception as e:
            print(f"[daily] apify failed for {username}: {e}")
            continue

        if not videos:
            print(f"[daily] no videos returned for {username}")
            continue

        # Author stats live on each video; pull from first
        author = videos[0].get("authorMeta") or {}
        followers = author.get("fans", 0)
        following = author.get("following", 0)
        heart = author.get("heart", 0)
        video_count = author.get("video", 0)

        append_row(sheet, "Snapshot", [
            today_iso, cfg["label"],
            followers, following, heart, video_count,
        ])
        print(f"[daily] snapshot written {cfg['label']} followers={followers}")

        # Diagnostic: print total items and date histogram
        print(f"[daily] {cfg['label']} total items returned: {len(videos)}")
        date_counts = {}
        for v in videos:
            pl = parse_post_date(v)
            if pl:
                d = pl.date().isoformat()
                date_counts[d] = date_counts.get(d, 0) + 1
            else:
                date_counts["NO_DATE"] = date_counts.get("NO_DATE", 0) + 1
        print(f"[daily] {cfg['label']} date histogram (most recent first):")
        for d, count in sorted(date_counts.items(), reverse=True)[:20]:
            marker = " <-- TARGET" if d == str(target_date) else ""
            print(f"  {d}: {count} video(s){marker}")

        # Find videos posted on the Melbourne-local target_date
        matched = 0
        for v in videos:
            post_local = parse_post_date(v)
            if not post_local or post_local.date() != target_date:
                continue

            desc = v.get("text", "") or ""
            vtype = classify(desc, item=v)
            m = compute_ratios(v)

            row = [
                today_iso,
                post_local.isoformat(timespec="minutes"),
                cfg["label"],
                "Scripted" if vtype == "scripted" else "Comment Reply",
                desc[:500],
                v.get("webVideoUrl", ""),
                m["views"], m["likes"], m["comments"], m["shares"], m["saves"],
                m["like_rate"], m["comment_rate"], m["share_rate"], m["save_rate"],
                m["engagement_rate"],
                followers,
                "",  # retention manual
            ]
            tab = cfg["scripted_tab"] if vtype == "scripted" else cfg["comreply_tab"]
            append_row(sheet, tab, row)
            matched += 1
            total_written += 1
            print(f"[daily] wrote {cfg['label']}/{vtype} -> {v.get('webVideoUrl')}")

        print(f"[daily] {cfg['label']} matched {matched} videos for {target_date}")

    print(f"[daily] done. rows written: {total_written}")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"[daily] FATAL: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
