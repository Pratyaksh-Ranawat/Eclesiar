import json
import os
import re
import time
from html import unescape
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


INPUT_GLOB = "region_*.html"
FALLBACK_INPUT = "doc.html"
OUTPUT_DIR = Path("output")
OUTPUT_JSON = OUTPUT_DIR / "region_civilians.json"
OUTPUT_MERGED_JSON = OUTPUT_DIR / "region_civilians_merged.json"
OUTPUT_PURCHASE_JSON = OUTPUT_DIR / "npc_purchase_events.json"
OUTPUT_PURCHASE_SUMMARY_JSON = OUTPUT_DIR / "npc_purchase_summary.json"
US_NPCS_JSON = OUTPUT_DIR / "us_npcs.json"
API_BASE_URL = "https://api.eclesiar.com"
COUNTRY_ID = 3
PURCHASE_LIMIT = 50
MAX_TRANSACTION_SCAN_PAGES = 400
REQUEST_DELAY_SECONDS = 0.05


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def fetch_json(path: str, api_key: str, params: dict[str, Any] | None = None) -> Any:
    url = f"{API_BASE_URL}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"

    request = Request(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "User-Agent": "eclesiar-region-civilians/1.0",
        },
    )

    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} for {url}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc


def clean_html_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = unescape(value)
    return " ".join(value.split())


def extract_region_id_and_name(html: str, filename: str) -> dict[str, Any]:
    file_match = re.search(r"region_(\d+)\.html$", filename)
    region_id = int(file_match.group(1)) if file_match else None

    id_match = re.search(r"/region/(\d+)/details", html)
    if id_match:
        region_id = int(id_match.group(1))

    name_match = re.search(r"<title>\s*([^<]+?)\s*-\s*Eclesiar\s*</title>", html, re.IGNORECASE)
    region_name = name_match.group(1).strip() if name_match else None

    owner_match = re.search(
        r"Rightfull owner:\s*</p>.*?<p[^>]*>\s*(.*?)\s*</p>",
        html,
        re.IGNORECASE | re.DOTALL,
    )
    rightful_owner = clean_html_text(owner_match.group(1)) if owner_match else None

    return {
        "region_id": region_id,
        "region_name": region_name,
        "rightful_owner": rightful_owner,
    }


def extract_desktop_civilians_table(html: str) -> str | None:
    match = re.search(
        r"<p[^>]*>\s*Civilians:\s*</p>.*?<table class=\"table\s+table-striped\s+mt-2 desktop-only\">(.*?)</table>",
        html,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    return match.group(1)


def parse_civilians(table_html: str) -> list[dict[str, Any]]:
    rows = []
    for row_match in re.finditer(
        r"<tr class=\"npc-row\" data-id=\"(?P<npc_id>\d+)\">(.*?)</tr>",
        table_html,
        re.DOTALL,
    ):
        row_html = row_match.group(0)
        npc_id = int(row_match.group("npc_id"))

        links = list(
            re.finditer(
                r"<a href=\"(?P<href>[^\"]+)\"[^>]*>.*?<img[^>]*alt=\"(?P<alt>[^\"]+)\"[^>]*>(?P<label>.*?)</a>",
                row_html,
                re.DOTALL,
            )
        )

        if len(links) < 2:
            continue

        npc_link = links[0]
        business_link = links[1]

        wage_match = re.search(
            r"<td[^>]*class=\"[^\"]*column-2[^\"]*\"[^>]*>\s*([0-9]+(?:\.[0-9]+)?)\s*</td>",
            row_html,
            re.DOTALL,
        )
        current_wage = float(wage_match.group(1)) if wage_match else None

        business_href = business_link.group("href")
        business_id_match = re.search(r"/business/(\d+)", business_href)

        rows.append(
            {
                "npc_id": npc_id,
                "npc_name": clean_html_text(npc_link.group("label")),
                "npc_profile_path": npc_link.group("href"),
                "working_in": clean_html_text(business_link.group("label")),
                "business_path": business_href,
                "business_id": int(business_id_match.group(1)) if business_id_match else None,
                "current_wage": current_wage,
            }
        )

    return rows


def merge_with_us_npcs(records: list[dict[str, Any]], us_npcs_path: Path) -> list[dict[str, Any]]:
    if not us_npcs_path.exists():
        return records

    payload = json.loads(us_npcs_path.read_text(encoding="utf-8"))
    npc_lookup = {row["npc_id"]: row for row in payload.get("npcs", [])}

    merged = []
    for row in records:
        npc = npc_lookup.get(row["npc_id"], {})
        merged.append(
            {
                **row,
                "nationality_id": npc.get("nationality_id"),
                "nationality_name": npc.get("nationality_name"),
                "region_id": npc.get("region_id"),
                "region_name": npc.get("region_name"),
                "region_country_id": npc.get("region_country_id"),
                "region_country_name": npc.get("region_country_name"),
                "recent_activity_counts": npc.get("recent_activity_counts"),
                "latest_work_tax_paid": npc.get("latest_work_tax_paid"),
                "estimated_wage_from_transactions": npc.get("estimated_wage"),
                "last_seen_at": npc.get("last_seen_at"),
                "transaction_summary": npc.get("summary"),
            }
        )

    return merged


def build_item_lookup(api_key: str, item_ids: set[int]) -> dict[int, dict[str, Any]]:
    if not item_ids:
        return {}

    remaining = set(item_ids)
    item_lookup: dict[int, dict[str, Any]] = {}
    page = 1

    while remaining:
        payload = fetch_json("/server/items", api_key, {"page": page})
        rows = payload.get("data", [])
        if not rows:
            break

        for row in rows:
            item_id = row["id"]
            if item_id in remaining:
                item_lookup[item_id] = row
                remaining.discard(item_id)

        page += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    return item_lookup


def collect_purchase_events(
    api_key: str,
    npc_ids: set[int],
    limit: int = PURCHASE_LIMIT,
) -> tuple[list[dict[str, Any]], dict[int, list[dict[str, Any]]], dict[str, Any]]:
    purchases: list[dict[str, Any]] = []
    pending_item_ids: set[int] = set()
    pages_scanned = 0
    matching_transactions = 0

    for page in range(1, MAX_TRANSACTION_SCAN_PAGES + 1):
        payload = fetch_json("/country/currency-transactions", api_key, {"country_id": COUNTRY_ID, "page": page})
        rows = payload["data"]["data"]
        pages_scanned = page

        for row in rows:
            if row.get("description") != "Items bought in the market":
                continue

            buyer = row.get("from", {})
            if buyer.get("type") != "npc":
                continue

            npc_id = buyer.get("id")
            if npc_id not in npc_ids:
                continue

            matching_transactions += 1

            for complex_row in row.get("complex_transactions", []):
                for item_log in complex_row.get("item_logs", []):
                    if item_log.get("to", {}).get("id") != npc_id:
                        continue

                    item_id = item_log["item_id"]
                    pending_item_ids.add(item_id)
                    purchases.append(
                        {
                            "npc_id": npc_id,
                            "transaction_id": row["id"],
                            "created_at": item_log.get("created_at") or row.get("created_at"),
                            "item_id": item_id,
                            "quantity": item_log.get("quantity"),
                            "country_ledger_value": row.get("value"),
                            "description": row.get("description"),
                        }
                    )

                    if len(purchases) >= limit:
                        break

                if len(purchases) >= limit:
                    break

            if len(purchases) >= limit:
                break

        if len(purchases) >= limit:
            break

        time.sleep(REQUEST_DELAY_SECONDS)

    item_lookup = build_item_lookup(api_key, pending_item_ids)

    enriched_purchases: list[dict[str, Any]] = []
    purchases_by_npc: dict[int, list[dict[str, Any]]] = {}
    for purchase in purchases:
        item = item_lookup.get(purchase["item_id"], {})
        enriched = {
            **purchase,
            "item_name": item.get("name"),
            "item_quality": item.get("quality"),
            "item_type": item.get("type"),
        }
        enriched_purchases.append(enriched)
        purchases_by_npc.setdefault(purchase["npc_id"], []).append(enriched)

    for npc_id in purchases_by_npc:
        purchases_by_npc[npc_id].sort(key=lambda row: row["created_at"], reverse=True)

    enriched_purchases.sort(key=lambda row: row["created_at"], reverse=True)
    summary = {
        "country_id": COUNTRY_ID,
        "target_npc_count": len(npc_ids),
        "purchase_limit": limit,
        "pages_scanned": pages_scanned,
        "matching_transactions_seen": matching_transactions,
        "captured_purchase_events": len(enriched_purchases),
        "workers_with_purchase_events": len(purchases_by_npc),
        "newest_purchase_at": enriched_purchases[0]["created_at"] if enriched_purchases else None,
        "oldest_purchase_at": enriched_purchases[-1]["created_at"] if enriched_purchases else None,
        "note": (
            "This is the most recent recovered slice of matching NPC market-purchase events "
            "from the scanned country transaction pages. It does not prove there were no purchases "
            "outside this recovered window."
        ),
    }
    return enriched_purchases, purchases_by_npc, summary


def main() -> int:
    load_dotenv()
    html_files = sorted(Path(".").glob(INPUT_GLOB))
    if not html_files and Path(FALLBACK_INPUT).exists():
        html_files = [Path(FALLBACK_INPUT)]

    if not html_files:
        print("No region HTML files found. Save files like region_9.html or keep doc.html present.")
        return 1

    extracted = []
    for path in html_files:
        html = path.read_text(encoding="utf-8", errors="ignore")
        region_meta = extract_region_id_and_name(html, path.name)
        table_html = extract_desktop_civilians_table(html)
        if not table_html:
            print(f"Skipping {path.name}: civilians desktop table not found.")
            continue

        civilians = parse_civilians(table_html)
        extracted.append(
            {
                "source_file": path.name,
                **region_meta,
                "civilians": civilians,
            }
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(extracted, indent=2), encoding="utf-8")

    flat_records = []
    for region in extracted:
        for civilian in region["civilians"]:
            flat_records.append(
                {
                    "source_file": region["source_file"],
                    "region_page_id": region["region_id"],
                    "region_page_name": region["region_name"],
                    "rightful_owner": region["rightful_owner"],
                    **civilian,
                }
            )

    merged = merge_with_us_npcs(flat_records, US_NPCS_JSON)

    api_key = os.environ.get("ECLESIAR_API_KEY")
    purchases: list[dict[str, Any]] = []
    purchases_by_npc: dict[int, list[dict[str, Any]]] = {}
    purchase_summary: dict[str, Any] = {
        "country_id": COUNTRY_ID,
        "target_npc_count": len({row["npc_id"] for row in merged}),
        "purchase_limit": PURCHASE_LIMIT,
        "pages_scanned": 0,
        "matching_transactions_seen": 0,
        "captured_purchase_events": 0,
        "workers_with_purchase_events": 0,
        "newest_purchase_at": None,
        "oldest_purchase_at": None,
        "note": "Purchase scan was skipped because ECLESIAR_API_KEY was not available.",
    }
    if api_key:
        purchases, purchases_by_npc, purchase_summary = collect_purchase_events(api_key, {row["npc_id"] for row in merged})

    for row in merged:
        npc_purchases = purchases_by_npc.get(row["npc_id"], [])
        row["recent_market_purchases"] = npc_purchases[:8]
        row["recent_market_purchase_count"] = len(npc_purchases)
        row["purchase_window_newest_at"] = purchase_summary["newest_purchase_at"]
        row["purchase_window_oldest_at"] = purchase_summary["oldest_purchase_at"]

    OUTPUT_MERGED_JSON.write_text(json.dumps(merged, indent=2), encoding="utf-8")
    OUTPUT_PURCHASE_JSON.write_text(json.dumps(purchases, indent=2), encoding="utf-8")
    OUTPUT_PURCHASE_SUMMARY_JSON.write_text(json.dumps(purchase_summary, indent=2), encoding="utf-8")

    total = sum(len(region["civilians"]) for region in extracted)
    print(
        f"Wrote {OUTPUT_JSON} for {len(extracted)} region pages, "
        f"{OUTPUT_MERGED_JSON} with {total} civilian rows, and "
        f"{OUTPUT_PURCHASE_JSON} with {len(purchases)} market-purchase events."
    )
    if purchases:
        print(
            "Recovered purchase window: "
            f"{purchase_summary['newest_purchase_at']} down to {purchase_summary['oldest_purchase_at']} "
            f"after scanning {purchase_summary['pages_scanned']} pages."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
