import json
import os
import sys
import time
from argparse import ArgumentParser
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_BASE_URL = "https://api.eclesiar.com"
COUNTRY_NAME = "United States of America"
OUTPUT_DIR = Path("output")
OUTPUT_JSON = OUTPUT_DIR / "us_npcs.json"
OUTPUT_USA_NATIONALITY_JSON = OUTPUT_DIR / "usa_nationality_npcs.json"
MAX_TRANSACTION_PAGES = 50
REQUEST_DELAY_SECONDS = 0.15


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def fetch_json(path: str, api_key: str, params: dict[str, Any] | None = None) -> Any:
    url = f"{API_BASE_URL}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"

    request = Request(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "User-Agent": "eclesiar-us-npc-report/1.0",
        },
    )

    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} for {url}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc


def normalize_description(text: str) -> str:
    return " ".join(str(text or "").split())


def estimate_wage_from_work_tax(work_tax_paid: float, work_tax_percent: float | int | None) -> float | None:
    if not work_tax_paid or not work_tax_percent:
        return None

    rate = float(work_tax_percent) / 100.0
    if rate <= 0:
        return None

    gross = work_tax_paid / rate
    return round(gross, 3)


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_args() -> Any:
    parser = ArgumentParser(description="Generate a U.S. NPC report from the Eclesiar API.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=MAX_TRANSACTION_PAGES,
        help="How many country transaction pages to scan.",
    )
    parser.add_argument(
        "--country-id",
        type=int,
        default=None,
        help="Override the target country id. Defaults to United States of America lookup.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print scan progress while collecting NPC candidates.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv()

    api_key = os.environ.get("ECLESIAR_API_KEY")
    if not api_key:
        print("Set ECLESIAR_API_KEY in your environment or .env before running this script.", file=sys.stderr)
        return 1

    countries_response = fetch_json("/countries", api_key)
    countries = countries_response["data"]
    country_lookup = {row["id"]: row["name"] for row in countries}
    if args.country_id is not None:
        country = next((row for row in countries if row["id"] == args.country_id), None)
    else:
        country = next((row for row in countries if row["name"] == COUNTRY_NAME), None)
    if not country:
        print("Could not find the target country in /countries.", file=sys.stderr)
        return 1

    country_id = country["id"]
    work_tax_percent = country["laws"]["work_tax"]

    regions_response = fetch_json("/country/regions", api_key, {"country_id": country_id})
    regions = regions_response["data"]
    us_region_ids = {row["id"] for row in regions}
    total_npcs_expected = sum(int(row["nb_npcs"]) for row in regions)
    global_region_lookup: dict[int, dict[str, Any]] = {}
    for country_row in countries:
        country_row_id = country_row["id"]
        if country_row_id >= 10000:
            continue

        try:
            region_rows = fetch_json("/country/regions", api_key, {"country_id": country_row_id}).get("data", [])
        except RuntimeError:
            continue

        if not isinstance(region_rows, list):
            continue

        for region_row in region_rows:
            global_region_lookup[region_row["id"]] = {
                "region_name": region_row["name"],
                "region_country_id": country_row_id,
                "region_country_name": country_row["name"],
            }

        time.sleep(REQUEST_DELAY_SECONDS)

    npc_tx: dict[int, list[dict[str, Any]]] = defaultdict(list)
    observed_us_npcs = set()
    checked_accounts = {}

    for page in range(1, args.max_pages + 1):
        tx_response = fetch_json(
            "/country/currency-transactions",
            api_key,
            {"country_id": country_id, "page": page},
        )
        rows = tx_response["data"]["data"]

        for row in rows:
            for side in ("from", "to"):
                entity = row[side]
                if entity.get("type") != "npc":
                    continue

                npc_id = int(entity["id"])
                npc_tx[npc_id].append(row)

        # Keep every NPC explicitly seen in the U.S. country transaction feed.
        page_npc_ids = [npc_id for npc_id in npc_tx if npc_id not in checked_accounts]
        for npc_id in page_npc_ids:
            account = fetch_json("/account", api_key, {"account_id": npc_id})["data"]
            time.sleep(REQUEST_DELAY_SECONDS)
            checked_accounts[npc_id] = account
            observed_us_npcs.add(npc_id)

        if args.verbose and (page == 1 or page % 10 == 0):
            print(
                f"page={page} scanned_candidates={len(checked_accounts)} "
                f"observed_us_npcs={len(observed_us_npcs)} expected_region_npcs={total_npcs_expected}"
            )

        time.sleep(REQUEST_DELAY_SECONDS)

    records = []
    for npc_id in sorted(observed_us_npcs):
        account = checked_accounts.get(npc_id)
        if account is None:
            account = fetch_json("/account", api_key, {"account_id": npc_id})["data"]
            time.sleep(REQUEST_DELAY_SECONDS)

        transactions = npc_tx.get(npc_id, [])
        transactions.sort(key=lambda row: row["created_at"], reverse=True)

        desc_counter = Counter(normalize_description(row["description"]) for row in transactions)
        latest_work_tax = next(
            (float(row["value"]) for row in transactions if normalize_description(row["description"]) == "Work Taxes"),
            None,
        )
        estimated_wage = estimate_wage_from_work_tax(latest_work_tax, work_tax_percent)
        last_seen = transactions[0]["created_at"] if transactions else None

        global_region = global_region_lookup.get(account["region_id"], {})
        nationality_name = country_lookup.get(account["nationality_id"])
        home_region_in_us = account["region_id"] in us_region_ids
        summary_parts = [
            f"Observed in United States transaction logs",
            f"Account region: {global_region.get('region_name', 'Unknown')}",
            f"Account region country: {global_region.get('region_country_name', 'Unknown')}",
            f"Nationality: {nationality_name or 'Unknown'}",
            f"Recent tax activity: {', '.join(f'{name} x{count}' for name, count in desc_counter.most_common(3)) or 'none seen'}",
        ]
        if estimated_wage is not None:
            summary_parts.append(f"Estimated gross wage from latest Work Taxes: {estimated_wage}")
        else:
            summary_parts.append("Estimated gross wage: unavailable")

        summary_parts.append("Employer/company owner: not exposed by the official API endpoints tested")

        records.append(
            {
                "npc_id": account["id"],
                "name": account["username"],
                "region_id": account["region_id"],
                "region_name": global_region.get("region_name"),
                "region_country_id": global_region.get("region_country_id"),
                "region_country_name": global_region.get("region_country_name"),
                "home_region_is_current_us_region": home_region_in_us,
                "nationality_id": account["nationality_id"],
                "nationality_name": nationality_name,
                "avatar": account["avatar"],
                "day_of_birth": account["day_of_birth"],
                "last_seen_at": last_seen,
                "recent_activity_counts": dict(desc_counter),
                "latest_work_tax_paid": latest_work_tax,
                "estimated_wage": estimated_wage,
                "workplace_name": None,
                "company_owner_name": None,
                "summary": ". ".join(summary_parts),
            }
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": iso_now(),
        "country": {
            "id": country_id,
            "name": country["name"],
            "work_tax_percent": work_tax_percent,
        },
        "counts": {
            "expected_from_current_us_regions": total_npcs_expected,
            "found_in_scanned_transactions": len(records),
            "scanned_transaction_pages": args.max_pages,
            "scanned_npc_candidates": len(checked_accounts),
        },
        "notes": [
            "NPC identities were recovered from official country transaction logs where entities are labeled type=npc.",
            "This report includes NPCs observed inside the scanned United States transaction window, which is broader than current U.S.-region residency.",
            "home_region_is_current_us_region tells you whether the NPC profile region is one of the current U.S. regions returned by /country/regions.",
            "Employer and company owner are null because no tested official endpoint exposed direct NPC->company links.",
            "Estimated wage is derived from latest Work Taxes using the country work tax percentage.",
        ],
        "npcs": records,
    }

    OUTPUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    usa_nationality_payload = {
        "generated_at": payload["generated_at"],
        "source_report": str(OUTPUT_JSON),
        "nationality_id": country_id,
        "nationality_name": country["name"],
        "count": sum(1 for row in records if row.get("nationality_id") == country_id),
        "npcs": [row for row in records if row.get("nationality_id") == country_id],
    }
    OUTPUT_USA_NATIONALITY_JSON.write_text(json.dumps(usa_nationality_payload, indent=2), encoding="utf-8")
    print(
        f"Wrote {OUTPUT_JSON} with {len(records)} NPC records and "
        f"{OUTPUT_USA_NATIONALITY_JSON} with {usa_nationality_payload['count']} USA-nationality NPC records."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
