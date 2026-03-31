import shutil
from pathlib import Path


ROOT = Path(".")
SITE_DIR = ROOT / "docs"
OUTPUT_DIR = ROOT / "output"

SOURCE_FILES = [
    ("npc_work_report.html", "index.html"),
    ("report.html", "us_activity_report.html"),
]

OUTPUT_FILES = [
    "report_build_meta.json",
    "region_civilians.json",
    "region_civilians_merged.json",
    "npc_purchase_events.json",
    "npc_purchase_summary.json",
    "us_npcs.json",
    "usa_nationality_npcs.json",
]


def copy_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def main() -> int:
    SITE_DIR.mkdir(parents=True, exist_ok=True)
    (SITE_DIR / "output").mkdir(parents=True, exist_ok=True)
    (SITE_DIR / ".nojekyll").write_text("", encoding="utf-8")

    for source_name, dest_name in SOURCE_FILES:
        source = ROOT / source_name
        if source.exists():
            copy_file(source, SITE_DIR / dest_name)

    for output_name in OUTPUT_FILES:
        source = OUTPUT_DIR / output_name
        if source.exists():
            copy_file(source, SITE_DIR / "output" / output_name)

    readme = """# Hosted Report Bundle

This folder is ready to publish as a static site.

Main report:
- index.html

Supporting data:
- output/

Recommended hosts:
- GitHub Pages
- Cloudflare Pages
- Netlify
"""
    (SITE_DIR / "README.txt").write_text(readme, encoding="utf-8")

    print(f"Built static hosting bundle in {SITE_DIR}")
    print(f"Open {SITE_DIR / 'index.html'} locally or point GitHub Pages at /docs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
