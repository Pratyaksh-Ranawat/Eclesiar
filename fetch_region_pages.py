import os
import sys
import time
from argparse import ArgumentParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


OUTPUT_DIR = Path(".")
DEFAULT_REGION_IDS = [7, 8, 9, 12, 13, 292]


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_args():
    parser = ArgumentParser(description="Fetch authenticated Eclesiar region detail pages.")
    parser.add_argument(
        "--regions",
        nargs="*",
        type=int,
        default=DEFAULT_REGION_IDS,
        help="Region ids to fetch. Defaults to current U.S. region ids.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay in seconds between requests.",
    )
    return parser.parse_args()


def build_headers(cookie: str) -> dict[str, str]:
    return {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        "cookie": cookie,
        "priority": "u=0, i",
        "referer": "https://eclesiar.com/country/3",
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    }


def fetch_region(region_id: int, headers: dict[str, str]) -> str:
    url = f"https://eclesiar.com/region/{region_id}/details"
    request = Request(url, headers=headers)
    with urlopen(request, timeout=45) as response:
        return response.read().decode("utf-8", errors="ignore")


def main() -> int:
    load_dotenv()
    args = parse_args()

    cookie = os.environ.get("ECLESIAR_SESSION_COOKIE")
    if not cookie:
        print(
            "Set ECLESIAR_SESSION_COOKIE in .env with the Cookie header value from your browser request.",
            file=sys.stderr,
        )
        return 1

    headers = build_headers(cookie)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for region_id in args.regions:
        path = OUTPUT_DIR / f"region_{region_id}.html"
        try:
            html = fetch_region(region_id, headers)
        except HTTPError as exc:
            print(f"HTTP {exc.code} while fetching region {region_id}", file=sys.stderr)
            return 1
        except URLError as exc:
            print(f"Network error while fetching region {region_id}: {exc.reason}", file=sys.stderr)
            return 1

        path.write_text(html, encoding="utf-8")
        print(f"Saved {path.name}")
        time.sleep(args.delay)

    print("Done. Run `python extract_region_civilians.py` to parse the saved pages.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
