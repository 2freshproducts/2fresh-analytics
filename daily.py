"""Daily run (~6:45am Melbourne, dual UTC cron for DST).

Two phases:
 1. For each account: scrape latest videos, upsert new URLs to URL Ledger,
    write follower snapshot.
     - Normal mode: 15 per account (cheap, daily maintenance)
     - Bootstrap mode: 60 per account (one-time, when ledger lacks 8-day coverage)
 2. Find ledger entries posted exactly 7 days ago, batch-fetch fresh stats
    by URL, classify, and write rows to the right tab.

Every video is measured at exactly 7 days post-upload => fair comparison.
Silent — weekly.py sends WhatsApp summary on Sundays.
"""
import sys
import traceback
from datetime import datetime, timedelta

from lib import (
    ACCOUNTS, MELBOURNE,
    apify_list_profile, apify_fetch_videos,
    classify, compute_ratios, parse_post_date,
    get_sheet, ensure_tabs_and_headers, append_row, already_ran_today,
    read_ledger, upsert_ledger, prune_ledger,
)


BOOTSTRAP_COUNT = 60      # first-time / recovery scrape size
NORMAL_COUNT = 15         # daily maintenance scrape size
COVERAGE_DAYS = 8         # ledger must have >=1 entry this old or bootstrap triggers


def _needs_bootstrap(ledger, today_mel_date) -> bool:
    """True if ledger lacks an entry older than COVERAGE_DAYS from today."""
    if not ledger:
        return True
    oldest_needed = today_mel_date - timedelta(days=COVERAGE_DAYS)
    for e in ledger:
        pd_str = e.get("post_date", "")
        if not pd_str:
            continue
        try:
            pd = datetime.fromisoformat(pd_str).date()
        except Exception:
            continue
        if pd <= oldest_needed:
            return False
    return True


def phase1_update_ledger_and_snapshot(sheet, today_mel, today_iso, count):
    """For each account: list latest videos, upsert to ledger, write snapshot."""
    for username, cfg in ACCOUNTS.items():
        print(f"[daily.p1] listing {username} count={count}")
        videos = apify_list_profile(username, count=count)
        if not videos:
            print(f"[daily.p1] no videos for {username} — skipping snapshot + ledger")
            continue

        # Snapshot from first video's authorMeta
        author = videos[0].get("authorMeta") or {}
        followers = author.get("fans", 0)
        following = author.get("following", 0)
        heart = author.get("heart", 0)
        video_count = author.get("video", 0)
        append_row(sheet, "Snapshot", [
            today_iso, cfg["label"],
            followers, following, heart, video_count,
        ])
        print(f"[daily.p1] snapshot {cfg['label']} followers={followers}")

        # Build ledger rows from scraped videos
        new_rows = []
        for v in videos:
            post_local = parse_post_date(v)
            if not post_local:
                continue
            url = v.get("webVideoUrl") or ""
            if not url:
                continue
            new_rows.append([
                post_local.date().isoformat(),
                cfg["label"],
                url,
                today_iso,
            ])
        added = upsert_ledger(sheet, new_rows)
        print(f"[daily.p1] ledger {cfg['label']} +{added} new (saw {len(new_rows)})")


def phase2_fetch_and_write(sheet, today_mel):
    """Find ledger entries posted exactly 7 days ago, fetch fresh stats, write rows."""
    target_date = (today_mel - timedelta(days=7)).isoformat()
    today_iso = today_mel.isoformat()
    print(f"[daily.p2] target_post_date={target_date}")

    ledger = read_ledger(sheet)
    target_entries = [e for e in ledger if e["post_date"] == target_date]
    print(f"[daily.p2] ledger has {len(target_entries)} entries for {target_date}")

    if not target_entries:
        print("[daily.p2] nothing to fetch today")
        return 0

    # Map label -> account config so we can find tabs by label
    label_to_cfg = {cfg["label"]: cfg for cfg in ACCOUNTS.values()}

    # Batch-fetch all target URLs in a single Apify run
    urls = [e["url"] for e in target_entries]
    fetched = apify_fetch_videos(urls)
    print(f"[daily.p2] Apify returned {len(fetched)} items for {len(urls)} URLs")

    # Map fetched items by URL for lookup
    by_url = {}
    for v in fetched:
        u = v.get("webVideoUrl") or ""
        if u:
            by_url[u] = v

    # Read latest follower counts from Snapshot for the "Followers at scrape" column
    followers_by_label = _latest_followers(sheet)

    written = 0
    for entry in target_entries:
        url = entry["url"]
        label = entry["account"]
        cfg = label_to_cfg.get(label)
        if not cfg:
            print(f"[daily.p2] unknown label '{label}' in ledger, skipping {url}")
            continue

        v = by_url.get(url)
        if not v:
            print(f"[daily.p2] {label} no fetch result for {url} — skipping")
            continue

        post_local = parse_post_date(v)
        if not post_local:
            print(f"[daily.p2] {label} no post date for {url} — skipping")
            continue

        desc = v.get("text", "") or ""
        vtype = classify(desc, item=v)
        m = compute_ratios(v)
        followers = followers_by_label.get(label, "")
        row = [
            today_iso,
            post_local.isoformat(timespec="minutes"),
            label,
            "Scripted" if vtype == "scripted" else "Comment Reply",
            desc[:500],
            url,
            m["views"], m["likes"], m["comments"], m["shares"], m["saves"],
            m["like_rate"], m["comment_rate"], m["share_rate"], m["save_rate"],
            m["engagement_rate"],
            followers,
            "",  # retention manual
        ]
        tab = cfg["scripted_tab"] if vtype == "scripted" else cfg["comreply_tab"]
        append_row(sheet, tab, row)
        written += 1
        print(f"[daily.p2] wrote {label}/{vtype} -> {url}")

    print(f"[daily.p2] rows written: {written}")
    return written


def _latest_followers(sheet) -> dict:
    """Return {label: followers_int} from the latest Snapshot row per account."""
    try:
        ws = sheet.worksheet("Snapshot")
        rows = ws.get_all_values()
    except Exception:
        return {}
    out = {}
    for r in rows[1:]:
        if len(r) < 3:
            continue
        label = r[1]
        try:
            out[label] = int(r[2])
        except Exception:
            continue
    return out


def run():
    today_mel = datetime.now(MELBOURNE).date()
    today_iso = today_mel.isoformat()
    print(f"[daily] today_mel={today_iso}")

    sheet = get_sheet()
    ensure_tabs_and_headers(sheet)

    # DST-safe dedupe: if Snapshot already has a row for today, skip entirely.
    if already_ran_today(sheet, today_iso):
        print("[daily] snapshot already exists for today, skipping")
        return

    # Decide bootstrap vs normal
    existing_ledger = read_ledger(sheet)
    bootstrap = _needs_bootstrap(existing_ledger, today_mel)
    count = BOOTSTRAP_COUNT if bootstrap else NORMAL_COUNT
    if bootstrap:
        print(f"[daily] BOOTSTRAP MODE — ledger lacks {COVERAGE_DAYS}+ day coverage, fetching {count} per account")
    else:
        print(f"[daily] normal mode — fetching {count} per account")

    # Phase 1: list profiles, upsert ledger, snapshot
    phase1_update_ledger_and_snapshot(sheet, today_mel, today_iso, count)

    # Phase 2: fetch 7-day-old videos by URL, write rows
    phase2_fetch_and_write(sheet, today_mel)

    # House-keeping: prune ledger rows older than retention
    removed = prune_ledger(sheet, today_mel)
    if removed:
        print(f"[daily] pruned {removed} stale ledger rows")

    print("[daily] done.")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"[daily] FATAL: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
