import shlex
import subprocess
import sys
from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path
import json

PUBLISH_PATHS = [
    ".gitignore",
    "build_host_bundle.py",
    "extract_region_civilians.py",
    "fetch_region_pages.py",
    "generate_us_npc_report.py",
    "npc_work_report.html",
    "report.html",
    "update_all_reports.py",
    "docs",
]

OUTPUT_DIR = Path("output")
REPORT_BUILD_META_JSON = OUTPUT_DIR / "report_build_meta.json"


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def write_report_build_meta(args, fetch_failed: bool) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": iso_now(),
        "updated_via": "update_all_reports.py",
        "max_pages": args.max_pages,
        "regions": args.regions,
        "skip_site_build": bool(args.skip_site_build),
        "publish": bool(args.publish),
        "fetch_failed": fetch_failed,
    }
    REPORT_BUILD_META_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_args():
    parser = ArgumentParser(description="Refresh all Eclesiar NPC reports in one run.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=250,
        help="How many U.S. transaction pages to scan while rebuilding the base NPC dataset.",
    )
    parser.add_argument(
        "--regions",
        nargs="*",
        type=int,
        default=[7, 8, 9, 12, 13, 292],
        help="Region detail pages to fetch before rebuilding the workforce report.",
    )
    parser.add_argument(
        "--fetch-delay",
        type=float,
        default=0.5,
        help="Delay between authenticated region-page fetches.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Skip verbose progress output from generate_us_npc_report.py.",
    )
    parser.add_argument(
        "--skip-site-build",
        action="store_true",
        help="Do not rebuild the static hosting bundle in the site folder.",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="After rebuilding reports, pull/rebase, commit publishable changes, and push to GitHub.",
    )
    parser.add_argument(
        "--publish-remote",
        default="origin",
        help="Remote to push to when --publish is used.",
    )
    parser.add_argument(
        "--publish-branch",
        default="main",
        help="Branch to pull/push when --publish is used.",
    )
    parser.add_argument(
        "--publish-message",
        default="Update hosted NPC report",
        help="Commit message to use when --publish creates a commit.",
    )
    parser.add_argument(
        "--allow-fetch-failure",
        action="store_true",
        help="If region-page fetching fails, keep the last good workforce bundle instead of exiting with an error.",
    )
    return parser.parse_args()


def run_step(step_number: int, total_steps: int, command: list[str], label: str) -> None:
    print(f"[{step_number}/{total_steps}] {label}", flush=True)
    print(" ", " ".join(shlex.quote(part) for part in command), flush=True)
    completed = subprocess.run(command)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def run_step_result(step_number: int, total_steps: int, command: list[str], label: str) -> int:
    print(f"[{step_number}/{total_steps}] {label}", flush=True)
    print(" ", " ".join(shlex.quote(part) for part in command), flush=True)
    completed = subprocess.run(command)
    return completed.returncode


def run_capture(command: list[str]) -> str:
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode != 0:
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
        raise SystemExit(completed.returncode)
    return completed.stdout


def publish_changes(args, step_number: int, total_steps: int) -> None:
    remote_ref = f"{args.publish_remote}/{args.publish_branch}"

    run_step(
        step_number,
        total_steps,
        ["git", "pull", "--rebase", "--autostash", args.publish_remote, args.publish_branch],
        f"Sync latest changes from {remote_ref}",
    )
    step_number += 1

    run_step(
        step_number,
        total_steps,
        ["git", "add", "-A", *PUBLISH_PATHS],
        "Stage publishable report files",
    )
    step_number += 1

    status_output = run_capture(["git", "status", "--short", "--", *PUBLISH_PATHS]).strip()
    if status_output:
        run_step(
            step_number,
            total_steps,
            ["git", "commit", "-m", args.publish_message],
            "Commit refreshed hosted report",
        )
    else:
        print(f"[{step_number}/{total_steps}] Commit refreshed hosted report")
        print("  No publishable changes detected; skipping commit.")
    step_number += 1

    run_step(
        step_number,
        total_steps,
        ["git", "push", args.publish_remote, args.publish_branch],
        f"Push hosted report to {remote_ref}",
    )


def main() -> int:
    args = parse_args()

    generate_cmd = [sys.executable, "-u", "generate_us_npc_report.py", "--max-pages", str(args.max_pages)]
    if not args.quiet:
        generate_cmd.append("--verbose")

    fetch_cmd = [sys.executable, "-u", "fetch_region_pages.py", "--delay", str(args.fetch_delay)]
    if args.regions:
        fetch_cmd.append("--regions")
        fetch_cmd.extend(str(region_id) for region_id in args.regions)

    extract_cmd = [sys.executable, "-u", "extract_region_civilians.py"]

    total_steps = 3 if args.skip_site_build else 4
    if args.publish:
        total_steps += 4

    run_step(1, total_steps, generate_cmd, "Generate base U.S. NPC transaction report")
    fetch_failed = False
    fetch_result = run_step_result(2, total_steps, fetch_cmd, "Fetch authenticated U.S. region workforce pages")
    if fetch_result != 0:
        if not args.allow_fetch_failure:
            raise SystemExit(fetch_result)
        fetch_failed = True
        print(
            "  Region-page fetch failed. Keeping the last good workforce bundle and skipping extract/build steps.",
            flush=True,
        )

    if not fetch_failed:
        run_step(3, total_steps, extract_cmd, "Extract civilians, merge workforce data, and rebuild purchase ledger")
    else:
        print(f"[3/{total_steps}] Extract civilians, merge workforce data, and rebuild purchase ledger", flush=True)
        print("  Skipped because region-page fetching failed.", flush=True)

    write_report_build_meta(args, fetch_failed)

    next_step = 4
    if not args.skip_site_build and not fetch_failed:
        site_cmd = [sys.executable, "-u", "build_host_bundle.py"]
        run_step(next_step, total_steps, site_cmd, "Build static hosting bundle")
        next_step += 1
    elif not args.skip_site_build:
        print(f"[{next_step}/{total_steps}] Build static hosting bundle", flush=True)
        print("  Skipped to avoid overwriting the last good hosted workforce report.", flush=True)
        next_step += 1

    if args.publish:
        publish_changes(args, next_step, total_steps)

    if args.publish:
        if fetch_failed:
            print("Done. API data refreshed; hosted workforce report was left unchanged because region fetching failed.")
        else:
            print("Done. Local reports, docs bundle, and GitHub Pages content were all refreshed.")
    else:
        if fetch_failed:
            print("Done. API data refreshed locally; workforce rebuild was skipped because region fetching failed.")
        else:
            print("Done. Open npc_work_report.html to review locally, or publish the docs folder with GitHub Pages.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
