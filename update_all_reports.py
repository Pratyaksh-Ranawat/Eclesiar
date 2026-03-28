import shlex
import subprocess
import sys
from argparse import ArgumentParser


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
    return parser.parse_args()


def run_step(step_number: int, total_steps: int, command: list[str], label: str) -> None:
    print(f"[{step_number}/{total_steps}] {label}")
    print(" ", " ".join(shlex.quote(part) for part in command))
    completed = subprocess.run(command)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def main() -> int:
    args = parse_args()

    generate_cmd = [sys.executable, "generate_us_npc_report.py", "--max-pages", str(args.max_pages)]
    if not args.quiet:
        generate_cmd.append("--verbose")

    fetch_cmd = [sys.executable, "fetch_region_pages.py", "--delay", str(args.fetch_delay)]
    if args.regions:
        fetch_cmd.append("--regions")
        fetch_cmd.extend(str(region_id) for region_id in args.regions)

    extract_cmd = [sys.executable, "extract_region_civilians.py"]

    total_steps = 3 if args.skip_site_build else 4

    run_step(1, total_steps, generate_cmd, "Generate base U.S. NPC transaction report")
    run_step(2, total_steps, fetch_cmd, "Fetch authenticated U.S. region workforce pages")
    run_step(3, total_steps, extract_cmd, "Extract civilians, merge workforce data, and rebuild purchase ledger")

    if not args.skip_site_build:
        site_cmd = [sys.executable, "build_host_bundle.py"]
        run_step(4, total_steps, site_cmd, "Build static hosting bundle")

    print("Done. Open npc_work_report.html to review locally, or publish the docs folder with GitHub Pages.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
