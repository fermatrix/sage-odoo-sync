import argparse
import csv
import os
from typing import Dict

from sync_customers import DELIMITER, get_env_value, load_env_file, read_csv

try:
    import xmlrpc.client as xmlrpc_client
except Exception:
    xmlrpc_client = None


class OdooClient:
    def __init__(self, url: str, db: str, user: str, apikey: str):
        if xmlrpc_client is None:
            raise RuntimeError("xmlrpc.client unavailable")
        self.url = url.rstrip("/")
        self.db = db
        self.user = user
        self.apikey = apikey
        self.common = xmlrpc_client.ServerProxy(f"{self.url}/xmlrpc/2/common")
        self.models = xmlrpc_client.ServerProxy(f"{self.url}/xmlrpc/2/object")
        self.uid = self.common.authenticate(self.db, self.user, self.apikey, {})
        if not self.uid:
            raise RuntimeError("Odoo authentication failed")

    def search_read(
        self,
        model: str,
        domain: list,
        fields: list,
        limit: int = 2,
        offset: int = 0,
    ):
        return self.models.execute_kw(
            self.db,
            self.uid,
            self.apikey,
            model,
            "search_read",
            [domain],
            {"fields": fields, "limit": limit, "offset": offset},
        )


def export_countries(args: argparse.Namespace) -> int:
    env = load_env_file(args.env_file)
    url = get_env_value(env, "ODOO_STUDIOOPTYX_URL")
    db = get_env_value(env, "ODOO_STUDIOOPTYX_DB")
    user = get_env_value(env, "ODOO_STUDIOOPTYX_USER")
    apikey = get_env_value(env, "ODOO_STUDIOOPTYX_APIKEY")

    if not (url and db and user and apikey):
        print("ERROR: missing Odoo credentials (URL/DB/USER/APIKEY)")
        return 2

    client = OdooClient(url, db, user, apikey)

    customers_master = args.customers_master
    if not os.path.exists(customers_master):
        print(f"ERROR: customers master not found: {customers_master}")
        return 2

    master_root = os.path.dirname(args.customers_sync)
    master_odoo_root = os.path.dirname(args.odoo_customers)
    os.makedirs(master_odoo_root, exist_ok=True)

    countries_out = os.path.join(master_odoo_root, "countries_odoo.csv")
    states_out = os.path.join(master_odoo_root, "states_odoo.csv")
    parity_out = os.path.join(master_root, "country_parity.csv")
    state_parity_out = os.path.join(master_root, "state_parity.csv")

    # Export Odoo countries (id, name, code)
    fields = ["id", "name", "code"]
    offset = 0
    batch = args.batch_size
    countries = []
    while True:
        rows = client.search_read("res.country", [], fields, limit=batch, offset=offset)
        if not rows:
            break
        countries.extend(rows)
        offset += len(rows)

    countries.sort(key=lambda r: (r.get("name") or ""))
    with open(countries_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["OdooId", "OdooName", "OdooCode"], delimiter=DELIMITER)
        writer.writeheader()
        for r in countries:
            writer.writerow({
                "OdooId": r.get("id", ""),
                "OdooName": r.get("name", "") or "",
                "OdooCode": r.get("code", "") or "",
            })

    # Build parity from Sage Address.Country (AddressTypeNumber = 0 only)
    address_master_path = os.path.join(os.path.dirname(customers_master), "address.csv")
    if not os.path.exists(address_master_path):
        print(f"ERROR: address master not found: {address_master_path}")
        return 2

    _, address_rows = read_csv(address_master_path)
    sage_counts: Dict[str, int] = {}
    for r in address_rows:
        addr_type = (r.get("AddressTypeNumber") or "").strip()
        if addr_type != "0":
            continue
        raw = (r.get("Country") or "").strip()
        if not raw:
            continue
        sage_counts[raw] = sage_counts.get(raw, 0) + 1

    # Build quick match suggestions (exact match on ISO2 code or country name)
    by_code = {}
    by_name = {}
    for r in countries:
        code = (r.get("code") or "").strip().upper()
        name = (r.get("name") or "").strip().upper()
        if code:
            by_code[code] = r
        if name:
            by_name[name] = r

    def suggest(raw: str):
        key = raw.strip().upper()
        if key in by_code:
            return by_code[key], "code"
        if key in by_name:
            return by_name[key], "name"
        return None, ""

    with open(parity_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "sage_country_raw",
                "count_address",
                "odoo_country_name",
                "odoo_country_code",
                "match_type",
            ],
            delimiter=DELIMITER,
        )
        writer.writeheader()
        for raw in sorted(sage_counts.keys(), key=lambda k: sage_counts[k], reverse=True):
            match, match_type = suggest(raw)
            writer.writerow({
                "sage_country_raw": raw,
                "count_address": sage_counts[raw],
                "odoo_country_name": (match.get("name") if match else "") or "",
                "odoo_country_code": (match.get("code") if match else "") or "",
                "match_type": match_type,
            })

    print(f"OK: odoo countries exported -> {countries_out}")
    print(f"OK: country parity (address only) -> {parity_out}")

    # Export Odoo states (id, name, code, country)
    state_fields = ["id", "name", "code", "country_id"]
    offset = 0
    states = []
    while True:
        rows = client.search_read("res.country.state", [], state_fields, limit=batch, offset=offset)
        if not rows:
            break
        states.extend(rows)
        offset += len(rows)

    with open(states_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["OdooId", "OdooName", "OdooCode", "OdooCountryId", "OdooCountryName"],
            delimiter=DELIMITER,
        )
        writer.writeheader()
        for r in states:
            country = r.get("country_id") or []
            writer.writerow({
                "OdooId": r.get("id", ""),
                "OdooName": r.get("name", "") or "",
                "OdooCode": r.get("code", "") or "",
                "OdooCountryId": country[0] if isinstance(country, list) and country else "",
                "OdooCountryName": country[1] if isinstance(country, list) and len(country) > 1 else "",
            })

    # Build state parity from Sage Address.State (AddressTypeNumber = 0 only)
    sage_state_counts: Dict[str, int] = {}
    for r in address_rows:
        addr_type = (r.get("AddressTypeNumber") or "").strip()
        if addr_type != "0":
            continue
        raw = (r.get("State") or "").strip()
        if not raw:
            continue
        sage_state_counts[raw] = sage_state_counts.get(raw, 0) + 1

    by_state_code = {}
    for r in states:
        code = (r.get("code") or "").strip().upper()
        if not code:
            continue
        by_state_code.setdefault(code, []).append(r)

    def suggest_state(raw: str):
        key = raw.strip().upper()
        options = by_state_code.get(key, [])
        if not options:
            return None, ""
        # Prefer US/CA if multiple matches
        preferred = None
        for r in options:
            country = r.get("country_id") or []
            cname = country[1] if isinstance(country, list) and len(country) > 1 else ""
            if cname:
                if cname.lower() in {"united states", "united states of america"}:
                    preferred = (r, "code:US")
                    break
                if cname.lower() == "canada":
                    preferred = (r, "code:CA")
        if preferred:
            return preferred
        return options[0], "code"

    with open(state_parity_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "sage_state_raw",
                "count_address",
                "odoo_state_name",
                "odoo_state_code",
                "odoo_country_name",
                "match_type",
            ],
            delimiter=DELIMITER,
        )
        writer.writeheader()
        for raw in sorted(sage_state_counts.keys(), key=lambda k: sage_state_counts[k], reverse=True):
            match, match_type = suggest_state(raw)
            country = match.get("country_id") if match else []
            writer.writerow({
                "sage_state_raw": raw,
                "count_address": sage_state_counts[raw],
                "odoo_state_name": (match.get("name") if match else "") or "",
                "odoo_state_code": (match.get("code") if match else "") or "",
                "odoo_country_name": (country[1] if isinstance(country, list) and len(country) > 1 else "") or "",
                "match_type": match_type,
            })

    print(f"OK: odoo states exported -> {states_out}")
    print(f"OK: state parity (address only) -> {state_parity_out}")
    return 0
